#!/usr/bin/env python3
"""
framework_signal_engine.py
==========================
Top-down framework signal engine (HTF map -> MTF confirmation -> LTF trigger).

Single file, scan-per-run (run it from an external scheduler every 5-15 min),
closed-candle only, read-only decision support (NEVER places orders), alerts
via Telegram.  Data: Hyperliquid public info API.

Usage
-----
  python framework_signal_engine.py                      one full scan
  python framework_signal_engine.py --dry-run            print signals, no Telegram / no state writes
  python framework_signal_engine.py --symbol BTC --profile MID --explain
  python framework_signal_engine.py --mtf-mode gate|bonus|off
  python framework_signal_engine.py --backtest 30
  python framework_signal_engine.py --backtest 30 --compare-mtf
  python framework_signal_engine.py --self-test
  python framework_signal_engine.py --learn-report       show what the engine has learned so far

Self-learning (v2)
------------------
A trade is a WIN when TP1 is hit before the SL (also if the SL is hit after TP1).  The engine learns from
those outcomes, slowly and inside hard bounds:
  * score-component weights  (SHADOW by default: learned, logged, compared, not used until LEARN_WEIGHTS=live)
  * per-variant MIN_SCORE    (only ever raised above your MIN_SCORE, auto-relaxes back)
  * per-symbol throttle      (needs a higher score + longer cooldown; auto-expires after 7 days)
  * circuit breaker          (freezes learning, reverts weights, +5 score for 48h; auto-recovers)
Env: LEARN_MODE=on|off  LEARN_WEIGHTS=shadow|live|off  LEARN_GUARDS=on|off
"""
from __future__ import annotations

import argparse
import bisect
import copy
import fcntl
import json
import logging
import math
import os
import sys
import tempfile
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import requests

# =============================================================================
# CONFIG
# =============================================================================


def _env_str(name: str, default: str) -> str:
    return os.environ.get(name, default).strip()


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.environ.get(name, default)))
    except ValueError:
        return int(default)


# --- infrastructure ---------------------------------------------------------
HL_URL = "https://api.hyperliquid.xyz/info"
HL_TIMEOUT_S = 12
HL_MIN_INTERVAL_S = _env_float("HL_MIN_INTERVAL_S", 0.12)
HL_WEIGHT_PER_MIN = _env_int("HL_WEIGHT_PER_MIN", 1000)  # HL limit is 1200/min/IP
HL_RETRIES = 4
SCAN_WORKERS = max(1, _env_int("SCAN_WORKERS", 4))
STATE_FILE = _env_str("STATE_FILE", "state.json")
CANDLE_CACHE_FILE = _env_str("CANDLE_CACHE_FILE", "candle_cache.json")

# --- modes / switches -------------------------------------------------------
MTF_MODE = _env_str("MTF_MODE", "bonus").lower()  # "gate" | "bonus" | "off"
VALID_MTF_MODES = ("gate", "bonus", "off")
MIN_SCORE = _env_float("MIN_SCORE", 70.0)
ALL_DIRECTIONS = ("LONG", "SHORT")
DEFAULT_ENABLED_VARIANTS = "INTRA-15m,MID,SWING-1w-4h"

# --- self-learning switches ---------------------------------------------------
LEARN_MODE = _env_str("LEARN_MODE", "on").lower()  # "on" | "off"  (master switch)
LEARN_WEIGHTS = _env_str("LEARN_WEIGHTS", "shadow").lower()  # "shadow" | "live" | "off"
LEARN_GUARDS = _env_str("LEARN_GUARDS", "on").lower()  # per-variant min score, symbol throttle, breaker: "on" | "off"

# --- time frames ------------------------------------------------------------
DAY_MS = 86_400_000
TF_MS: dict[str, int] = {
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": DAY_MS,
    "1w": 7 * DAY_MS,
    "1M": 31 * DAY_MS,  # approximation (staleness / history sizing only)
}
CANDLE_COUNTS: dict[str, int] = {"1M": 48, "1w": 156, "1d": 260, "4h": 260, "1h": 300, "15m": 300, "5m": 300}

# --- profiles (HTF -> MTF -> LTF). Two-option slots become VARIANTS. --------
PROFILES: dict[str, dict[str, Any]] = {
    "INTRA": {"htf": ["4h"], "mtf": "1h", "ltf": ["15m", "5m"]},
    "MID": {"htf": ["1d"], "mtf": "4h", "ltf": ["15m"]},
    "SWING": {"htf": ["1M", "1w"], "mtf": "1d", "ltf": ["4h", "1h"]},
}

# --- structure / strategy thresholds ----------------------------------------
MIN_HISTORY = 30  # min candles on a frame, else "insufficient history"
ATR_PERIOD = 14
TREND_SWINGS = 2  # last N swing highs AND N swing lows must rise / fall
SIG_ATR_MULT = 1.5  # significant swing: reaction >= 1.5 x ATR(14) on the HTF
OTE_LO_RET = 0.618
OTE_HI_RET = 0.786
OTE_SWEET_RET = 0.705
HTF_MSB_MAX_AGE = 60  # HTF bars: latest bullish MSB must be this recent
ERL_FRESH_BARS = 20  # HTF bars: ERL "fresh" if the MSB is this recent
STRONG_LOOKBACK = 30  # HTF bars for the "strong trend" (TP3) test
HTF_TAP_BARS = 3  # HTF bars: "just tapped" window
WATCH_NEAR_ATR = 1.5  # HTF ATRs: "near" a PD array (watching digest)
OB_MAX_CANDLES = 3  # consecutive candles combined into one order block
OB_MAX_BREAKS = 3
BREAKER_MAX_OBS = 6
MTF_TOUCH_BARS = 2
MTF_MSS_LOOKBACK = 20
MTF_COUNTER_MSB_BARS = 5
MTF_ARRAY_LOOKBACK = 120
LTF_LOOKBACK = 60  # LTF bars scanned for the sweep structure
LTF_TRIGGER_MAX_AGE = 10  # LTF bars: MSS candle must be this recent
ZONE_TOL_ATR = 0.25  # LTF ATRs tolerance for "inside zone"

# --- risk plan --------------------------------------------------------------
SL_ATR_BUFFER = 0.15  # x ATR(14, LTF)
TP1_MIN_R = 1.5
TP2_MIN_RR = 2.0
ABS_MIN_RR = 1.5
TP3_EXTENSION = 0.272  # fraction of range size beyond the extreme
ENTRY_MAX_DIST_ATR = 1.5

# --- score weights (sum == 100) ---------------------------------------------
W_HTF_TREND = 15.0
W_ZONE = 15.0
W_OTE = 10.0
W_STACKED = 5.0
W_ERL = 5.0
W_LTF_MSS = 15.0
W_RR3 = 10.0
W_MTF_B = 5.0
W_MTF_C = 5.0
W_MTF_D = 10.0
W_MTF_E = 5.0
W_MTF_TOTAL = W_MTF_B + W_MTF_C + W_MTF_D + W_MTF_E  # 25
W_NON_MTF_TOTAL = W_HTF_TREND + W_ZONE + W_OTE + W_STACKED + W_ERL + W_LTF_MSS + W_RR3  # 75
RR_BONUS_THRESHOLD = 3.0

# --- lifecycle --------------------------------------------------------------
FILL_EXPIRY_BARS = 16
COOLDOWN_BARS = 6
CLUSTER_CAP = 3
OPEN_STATES = ("PENDING_FILL", "ACTIVE", "TP1_HIT")
TERMINAL_STATES = ("WIN", "LOSS", "EXPIRED")

# --- state retention / digests ----------------------------------------------
RAW_LOG_MAX_RECORDS = 800
RAW_LOG_MAX_DAYS = 21
WATCH_DIGEST_EVERY_H = 4
DAILY_SUMMARY_HOUR_UTC = _env_int("DAILY_SUMMARY_HOUR_UTC", 21)
TRIGGERS_SEEN_DAYS = 7

# --- learning parameters ------------------------------------------------------
LEARN_LOG_MAX = 3000  # compact, never-time-pruned outcome log (separate from the 21-day raw log)
LEARN_UPDATE_EVERY = 10  # run an update after this many NEW resolved trades
LEARN_WINDOW = 120  # most recent resolved trades used for weight learning
LEARN_MIN_SAMPLE = 30  # no weight change below this many trades in the window
LEARN_MIN_SIDE = 10  # each component needs >= this many trades with it ON and with it OFF
LEARN_PRIOR_K = 8  # shrinkage (pseudo-trades) toward the overall win rate
LEARN_GAIN = 1.5  # 20pp win-rate lift -> target multiplier 1.30
LEARN_STEP = 0.05  # max multiplier move per update
LEARN_MULT_MIN, LEARN_MULT_MAX = 0.75, 1.30  # hard bounds on every weight multiplier
LEARN_VARIANT_WINDOW, LEARN_VARIANT_MIN = 40, 20  # per-variant min-score adaptation
LEARN_MIN_SCORE_MAX_RAISE = 15.0  # variant min score never exceeds MIN_SCORE + this
LEARN_THROTTLE_WINDOW = 15  # symbol throttle: trailing trades ...
LEARN_THROTTLE_MAX_AVG_R = -0.30  # ... with average R below this
LEARN_THROTTLE_DAYS = 7  # throttles auto-expire (no permanent lockout)
LEARN_THROTTLE_SCORE_BONUS = 10.0  # throttled symbol needs MIN_SCORE + this
LEARN_THROTTLE_COOLDOWN_MULT = 3
LEARN_BREAKER_WINDOW = 30  # breaker: last N trades ...
LEARN_BREAKER_MAX_AVG_R = -0.30  # ... average R below this trips it
LEARN_BREAKER_HOURS = 48  # auto-recovers after this long
LEARN_BREAKER_MIN_NEW = 10  # after recovery, need this many new trades before it can re-trip
LEARN_BREAKER_SCORE_BUMP = 5.0
LEARN_PROMOTE_AUC_GAIN = 0.05  # report "ready to promote" when learned AUC beats live AUC by this
LEARN_PROMOTE_MIN_N = 40

# =============================================================================
# INSTRUMENT REGISTRY
# =============================================================================


@dataclass(frozen=True)
class Instrument:
    id: str
    label: str
    asset_class: str
    hl_coin: str
    hl_dex: str
    session: str


def _crypto(coin: str) -> Instrument:
    return Instrument(f"{coin}USDT", f"{coin}USD", "crypto", coin, "", "24x7")


WATCHLIST: list[Instrument] = [
    Instrument("XAUUSD", "GOLD", "metal", "xyz:GOLD", "xyz", "tradfi_ref"),
    *[_crypto(c) for c in ("BTC", "ETH", "SOL", "XRP", "ZEC", "LINK", "BNB", "NEAR", "HYPE")],
]
assert len(WATCHLIST) == 10, "watchlist must contain exactly 10 instruments"
CRYPTO_CLUSTER_IDS = {i.id for i in WATCHLIST if i.asset_class == "crypto"}  # GOLD excluded from cap


@dataclass(frozen=True)
class Variant:
    name: str
    profile: str
    htf: str
    mtf: str
    ltf: str

    @property
    def chain(self) -> str:
        return f"{self.profile} {self.htf}→{self.mtf}→{self.ltf}"


def build_all_variants() -> dict[str, Variant]:
    out: dict[str, Variant] = {}
    for prof, cfg in PROFILES.items():
        for htf in cfg["htf"]:
            for ltf in cfg["ltf"]:
                if len(cfg["htf"]) == 1 and len(cfg["ltf"]) == 1:
                    name = prof
                elif len(cfg["htf"]) == 1:
                    name = f"{prof}-{ltf}"
                else:
                    name = f"{prof}-{htf}-{ltf}"
                out[name] = Variant(name, prof, htf, cfg["mtf"], ltf)
    return out


ALL_VARIANTS = build_all_variants()
_VARIANT_LOOKUP = {k.lower(): v for k, v in ALL_VARIANTS.items()}


def parse_enabled_variants(raw: str) -> list[Variant]:
    out: list[Variant] = []
    for tok in [t.strip() for t in raw.split(",") if t.strip()]:
        v = _VARIANT_LOOKUP.get(tok.lower())
        if v is None:
            raise SystemExit(f"Unknown variant '{tok}'. Valid: {', '.join(ALL_VARIANTS)}")
        if v not in out:
            out.append(v)
    if not out:
        raise SystemExit("ENABLED_VARIANTS is empty")
    return out


def parse_directions(raw: str) -> tuple[str, ...]:
    toks = [t.strip().upper() for t in raw.split(",") if t.strip()]
    bad = [t for t in toks if t not in ALL_DIRECTIONS]
    if bad or not toks:
        raise SystemExit(f"SIGNAL_DIRECTIONS invalid: {raw!r} (use LONG, SHORT or LONG,SHORT)")
    return tuple(d for d in ALL_DIRECTIONS if d in toks)


log = logging.getLogger("framework_engine")

# =============================================================================
# CANDLES / TIME HELPERS
# =============================================================================


@dataclass(frozen=True)
class Candle:
    t: int  # open time ms
    o: float
    h: float
    l: float
    c: float
    v: float
    T: int  # close time ms (as reported by the API)


def candle_from_row(r: list[float]) -> Candle:
    return Candle(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]), int(r[6]))


def candle_to_row(c: Candle) -> list[float]:
    return [c.t, c.o, c.h, c.l, c.c, c.v, c.T]


def sign_of(direction: str) -> int:
    return 1 if direction == "LONG" else -1


def flip(cs: list[Candle], s: int) -> list[Candle]:
    """Map candles into 'long space'. s=+1 identity; s=-1 price inversion (mirror)."""
    if s > 0:
        return cs
    return [Candle(c.t, -c.o, -c.l, -c.h, -c.c, c.v, c.T) for c in cs]


def current_bar_open_ms(tf: str, now_ms: int) -> int:
    if tf == "1M":
        d = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
        return int(datetime(d.year, d.month, 1, tzinfo=timezone.utc).timestamp() * 1000)
    ms = TF_MS[tf]
    if tf == "1w":
        off = 4 * DAY_MS  # epoch is a Thursday; weekly bars assumed to open Monday 00:00 UTC
        return ((now_ms - off) // ms) * ms + off
    return (now_ms // ms) * ms


def is_closed(c: Candle, tf: str, now_ms: int) -> bool:
    """Closed candle: opened before the current bar AND the API close time is in the past."""
    return c.t < current_bar_open_ms(tf, now_ms) and c.T < now_ms


def is_stale(cs: list[Candle], tf: str, now_ms: int) -> bool:
    """Stale feed: last closed candle older than 3 bars of that frame."""
    if not cs:
        return True
    return now_ms - (cs[-1].t + TF_MS[tf]) > 3 * TF_MS[tf]


def fmt_price(p: float) -> str:
    a = abs(p)
    if a >= 100:
        dp = 2
    elif a >= 10:
        dp = 3
    elif a >= 1:
        dp = 4
    elif a >= 0.1:
        dp = 5
    else:
        dp = 6
    return f"{p:,.{dp}f}"


def fmt_ts(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


# =============================================================================
# HL CLIENT
# =============================================================================


class HyperliquidClient:
    """Weighted rate limiter + retry/backoff around the Hyperliquid /info endpoint."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self._lock = threading.Lock()
        self._last_call = 0.0
        self._window: deque[tuple[float, int]] = deque()

    def _throttle(self, weight: int) -> None:
        with self._lock:
            while True:
                now = time.monotonic()
                wait = self._last_call + HL_MIN_INTERVAL_S - now
                if wait > 0:
                    time.sleep(wait)
                    now = time.monotonic()
                while self._window and now - self._window[0][0] > 60:
                    self._window.popleft()
                used = sum(w for _, w in self._window)
                if used + weight <= HL_WEIGHT_PER_MIN or not self._window:
                    break
                time.sleep(max(0.05, 60 - (now - self._window[0][0]) + 0.05))
            self._last_call = time.monotonic()
            self._window.append((self._last_call, weight))

    def post(self, payload: dict, weight: int = 20) -> Any:
        last_err: Optional[Exception] = None
        for attempt in range(HL_RETRIES):
            self._throttle(weight)
            try:
                r = self.session.post(HL_URL, json=payload, timeout=HL_TIMEOUT_S)
                if r.status_code == 429:
                    time.sleep(2.0 * (attempt + 1))
                    last_err = RuntimeError("HTTP 429")
                    continue
                r.raise_for_status()
                return r.json()
            except (requests.RequestException, ValueError) as e:
                last_err = e
                time.sleep(min(8.0, 1.0 * (2 ** attempt)))
        raise RuntimeError(f"HL request failed after {HL_RETRIES} attempts: {last_err}")

    def meta_and_ctxs(self, dex: str) -> tuple[list[str], dict[str, float]]:
        payload: dict[str, Any] = {"type": "metaAndAssetCtxs"}
        if dex:
            payload["dex"] = dex
        data = self.post(payload, weight=20)
        universe = data[0]["universe"]
        ctxs = data[1]
        names: list[str] = []
        marks: dict[str, float] = {}
        for u, ctx in zip(universe, ctxs):
            names.append(u["name"])
            try:
                px = ctx.get("markPx") or ctx.get("midPx")
                if px is not None:
                    marks[u["name"]] = float(px)
            except (TypeError, ValueError):
                pass
        return names, marks

    def candles(self, coin: str, tf: str, start_ms: int, end_ms: int) -> list[list[float]]:
        est = max(1, int((end_ms - start_ms) / TF_MS[tf]) + 1)
        weight = 20 + min(5000, est) // 60
        data = self.post(
            {"type": "candleSnapshot", "req": {"coin": coin, "interval": tf, "startTime": int(start_ms), "endTime": int(end_ms)}},
            weight=weight,
        )
        out = []
        for x in data or []:
            out.append([int(x["t"]), float(x["o"]), float(x["h"]), float(x["l"]), float(x["c"]), float(x["v"]), int(x["T"])])
        out.sort(key=lambda r: r[0])
        return out

    def candles_range(self, coin: str, tf: str, start_ms: int, end_ms: int) -> list[list[float]]:
        """Paginated fetch (candleSnapshot returns at most ~5000 candles per call)."""
        rows: dict[int, list[float]] = {}
        cur = start_ms
        for _ in range(40):
            chunk = self.candles(coin, tf, cur, end_ms)
            if not chunk:
                break
            for r in chunk:
                rows[r[0]] = r
            nxt = chunk[-1][0] + 1
            if nxt <= cur or len(chunk) < 4000:
                break
            cur = nxt
        return [rows[k] for k in sorted(rows)]


class CandleStore:
    """Persistent candle cache (closed candles only), delta-fetch of new candles."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.data: dict[str, list[list[float]]] = {}
        self._lock = threading.Lock()
        self.load()

    def load(self) -> None:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                self.data = {k: v for k, v in raw.items() if isinstance(v, list)}
        except (OSError, ValueError):
            self.data = {}

    def save(self) -> None:
        with self._lock:
            _atomic_write_json(self.path, self.data)

    def refresh(self, client: HyperliquidClient, coin: str, tf: str, now_ms: int) -> None:
        key = f"{coin}|{tf}"
        count = CANDLE_COUNTS[tf]
        cached = self.data.get(key, [])
        cur_open = current_bar_open_ms(tf, now_ms)
        start = int(cached[-1][0]) if cached else cur_open - (count + 2) * TF_MS[tf]
        raw = client.candles(coin, tf, start, now_ms)
        merged = {int(r[0]): r for r in cached}
        for r in raw:
            if is_closed(candle_from_row(r), tf, now_ms):
                merged[int(r[0])] = r
        lst = [merged[k] for k in sorted(merged)][-count:]
        self.data[key] = lst

    def closed(self, coin: str, tf: str) -> list[Candle]:
        return [candle_from_row(r) for r in self.data.get(f"{coin}|{tf}", [])]


# =============================================================================
# STATE
# =============================================================================


def _atomic_write_json(path: str, obj: Any) -> None:
    d = os.path.dirname(os.path.abspath(path))
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, separators=(",", ":"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def new_bucket() -> dict[str, float]:
    return {"signals": 0, "wins": 0, "losses": 0, "expired": 0, "gross_profit_r": 0.0, "gross_loss_r": 0.0, "sum_r": 0.0, "sum_hold_minutes": 0.0}


BUCKET_GROUPS = ("by_symbol", "by_profile", "by_variant", "by_direction", "by_score_bucket", "by_alignment", "by_mtf_mode")

DEFAULT_LEARN: dict[str, Any] = {
    "log": [],  # compact outcome records (WIN/LOSS only)
    "resolved_total": 0,  # monotonic counter (the log is capped, so len() is not usable)
    "n_at_update": 0,
    "weights": {"mult": {}, "ver": 0, "pending": {}, "updated_ms": 0},
    "min_score": {},  # variant -> learned min score
    "throttle": {},  # symbol -> {"until_ms", "avg_r"}
    "throttle_n": {},  # symbol -> its resolved-trade count when last throttled (no re-throttle on stale trades)
    "breaker": {"until_ms": 0, "n_at_trip": 0, "avg_r": 0.0},
}

DEFAULT_STATE: dict[str, Any] = {
    "version": 2,
    "learn": copy.deepcopy(DEFAULT_LEARN),
    "totals": new_bucket(),
    "by_symbol": {},
    "by_profile": {},
    "by_variant": {},
    "by_direction": {},
    "by_score_bucket": {},
    "by_alignment": {},
    "by_mtf_mode": {},
    "signals": [],  # Tier 2 bounded raw log (full records)
    "cooldowns": {},
    "last_eval": {},
    "watching": {},
    "triggers_seen": {},
    "reported_skipped": [],
    "last_digest_ts": 0,
    "last_daily_summary": "",
}

SIGNAL_DEFAULTS: dict[str, Any] = {
    "tp3": None,
    "tp1_hit": False,
    "bars_waited": 0,
    "filled_ms": None,
    "closed_ms": None,
    "result_r": None,
    "tg_message_id": None,
    "mtf_mode": "bonus",
    "win_type": None,  # "TP2" | "TP1_THEN_SL" | "SL" | "EXPIRED"
    "alt_score": None,  # score under the OTHER weight set (learned in shadow mode, defaults in live mode)
    "weights_ver": 0,
    "mtf_aligned": False,
    "mtf_checks": {},
    "direction": "LONG",
    "variant": "",
    "profile": "",
    "asset_class": "crypto",
}


def _merge_defaults(default: Any, loaded: Any) -> Any:
    if isinstance(default, dict) and isinstance(loaded, dict):
        out = copy.deepcopy(default)
        for k, v in loaded.items():
            out[k] = _merge_defaults(default[k], v) if k in default else v
        return out
    return loaded


class StateStore:
    """state.json with atomic writes + non-blocking fcntl file lock."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.lock_path = path + ".lock"
        self._fh = None
        self.state: dict[str, Any] = copy.deepcopy(DEFAULT_STATE)

    def acquire(self) -> bool:
        os.makedirs(os.path.dirname(os.path.abspath(self.lock_path)), exist_ok=True)
        fh = open(self.lock_path, "w")
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            fh.close()
            return False
        self._fh = fh
        return True

    def release(self) -> None:
        if self._fh is not None:
            try:
                fcntl.flock(self._fh, fcntl.LOCK_UN)
            finally:
                self._fh.close()
                self._fh = None

    def load(self) -> dict[str, Any]:
        loaded: dict[str, Any] = {}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
        except (OSError, ValueError):
            loaded = {}
        st = _merge_defaults(DEFAULT_STATE, loaded if isinstance(loaded, dict) else {})
        for g in BUCKET_GROUPS:
            st[g] = {k: _merge_defaults(new_bucket(), b) for k, b in st[g].items()}
        st["signals"] = [{**SIGNAL_DEFAULTS, **r} for r in st["signals"]]
        self.state = st
        return st

    def save(self) -> None:
        prune_state(self.state)
        _atomic_write_json(self.path, self.state)


def score_bucket(score: float) -> str:
    if score >= 90:
        return "90+"
    if score >= 80:
        return "80-89"
    if score >= 70:
        return "70-79"
    return "<70"


def _buckets_for(state: dict, sig: dict) -> list[dict]:
    pairs = [
        ("by_symbol", sig["symbol"]),
        ("by_profile", sig["profile"]),
        ("by_variant", sig["variant"]),
        ("by_direction", sig["direction"]),
        ("by_score_bucket", score_bucket(sig["score"])),
        ("by_alignment", "aligned" if sig.get("mtf_aligned") else "not_aligned"),
        ("by_mtf_mode", sig.get("mtf_mode", "bonus")),
    ]
    out = [state["totals"]]
    for grp, key in pairs:
        out.append(state[grp].setdefault(key, new_bucket()))
    return out


def stats_on_emit(state: dict, sig: dict) -> None:
    for b in _buckets_for(state, sig):
        b["signals"] += 1


def stats_on_resolve(state: dict, sig: dict) -> None:
    res = sig["status"]
    r = float(sig.get("result_r") or 0.0)
    hold = 0.0
    if sig.get("filled_ms") and sig.get("closed_ms"):
        hold = max(0.0, (sig["closed_ms"] - sig["filled_ms"]) / 60000.0)
    for b in _buckets_for(state, sig):
        if res == "EXPIRED":
            b["expired"] += 1
            continue
        if res == "WIN":
            b["wins"] += 1
            b["gross_profit_r"] += r
        elif res == "LOSS":
            b["losses"] += 1
            b["gross_loss_r"] += abs(r)
        b["sum_r"] += r
        b["sum_hold_minutes"] += hold


def prune_state(state: dict) -> None:
    now_ms = int(time.time() * 1000)
    cutoff = now_ms - RAW_LOG_MAX_DAYS * DAY_MS
    sigs = state["signals"]
    keep = [s for s in sigs if s["status"] in OPEN_STATES or (s.get("closed_ms") or s.get("created_ms", now_ms)) >= cutoff]
    if len(keep) > RAW_LOG_MAX_RECORDS:
        excess = len(keep) - RAW_LOG_MAX_RECORDS
        out = []
        for s in keep:  # drop the oldest terminal records first, never open ones
            if excess > 0 and s["status"] in TERMINAL_STATES:
                excess -= 1
                continue
            out.append(s)
        keep = out
    state["signals"] = keep
    lg = state.get("learn", {}).get("log")
    if isinstance(lg, list) and len(lg) > LEARN_LOG_MAX:
        state["learn"]["log"] = lg[-LEARN_LOG_MAX:]
    state["triggers_seen"] = {k: v for k, v in state["triggers_seen"].items() if v >= now_ms - TRIGGERS_SEEN_DAYS * DAY_MS}
    state["cooldowns"] = {k: v for k, v in state["cooldowns"].items() if v >= now_ms - 2 * DAY_MS}
    state["watching"] = {k: v for k, v in state["watching"].items() if v.get("ts", 0) >= now_ms - 12 * 3_600_000}


# =============================================================================
# INDICATORS
# =============================================================================


def atr_series(cs: list[Candle], n: int = ATR_PERIOD) -> list[Optional[float]]:
    out: list[Optional[float]] = [None] * len(cs)
    trs: list[float] = []
    for i, c in enumerate(cs):
        tr = c.h - c.l if i == 0 else max(c.h - c.l, abs(c.h - cs[i - 1].c), abs(c.l - cs[i - 1].c))
        trs.append(tr)
        if i == n - 1:
            out[i] = sum(trs) / n
        elif i >= n:
            out[i] = (out[i - 1] * (n - 1) + tr) / n  # type: ignore[operator]
    return out


def last_atr(cs: list[Candle], n: int = ATR_PERIOD) -> float:
    for v in reversed(atr_series(cs, n)):
        if v is not None:
            return v
    return (sum(c.h - c.l for c in cs) / len(cs)) if cs else 0.0


BASE_WEIGHTS: dict[str, float] = {
    "htf_trend": W_HTF_TREND, "entry_zone": W_ZONE, "ote": W_OTE, "stacked": W_STACKED, "erl_fresh": W_ERL,
    "ltf_mss": W_LTF_MSS, "rr3": W_RR3, "mtf_b": W_MTF_B, "mtf_c": W_MTF_C, "mtf_d": W_MTF_D, "mtf_e": W_MTF_E,
}
MTF_KEYS = ("mtf_b", "mtf_c", "mtf_d", "mtf_e")
FIXED_KEYS = ("htf_trend", "ltf_mss")  # always on -> nothing to learn, never rescaled
LEARNABLE = ("entry_zone", "ote", "stacked", "erl_fresh", "rr3") + MTF_KEYS

# Runtime weight state (set once per scan from state.json by apply_learning_to_runtime; backtests keep the defaults).
WEIGHT_MULT: dict[str, float] = {}  # multipliers used for the LIVE score
ALT_MULT: dict[str, float] = {}  # multipliers for the ALT score (shadow comparison); empty = defaults
MIN_SCORE_BY_VARIANT: dict[str, float] = {}  # learned per-variant min score (>= MIN_SCORE)
SCORE_BUMP: float = 0.0  # temporary global bump while the learning breaker is tripped
THROTTLED: dict[str, float] = {}  # symbol id -> until_ms (runtime copy)
WEIGHTS_VER: int = 0


def eff_weights(mult: Optional[dict[str, float]] = None) -> dict[str, float]:
    m = mult or {}
    return {k: v * m.get(k, 1.0) for k, v in BASE_WEIGHTS.items()}


def normalize_score(raw: float, mtf_mode: str, weights: Optional[dict[str, float]] = None) -> float:
    """Score on 0-100. In MTF 'off' mode the MTF block is excluded: raw / non-MTF total * 100.
    `weights` = effective weights (learned); default = the constants, which sum to 100 (75 without MTF)."""
    w = weights or BASE_WEIGHTS
    if mtf_mode == "off":
        non_mtf = sum(v for k, v in w.items() if k not in MTF_KEYS)
        return max(0.0, min(100.0, raw / non_mtf * 100.0))
    total = sum(w.values())
    return max(0.0, min(100.0, raw / total * 100.0))


def min_score_for(variant_name: str) -> float:
    return MIN_SCORE_BY_VARIANT.get(variant_name, MIN_SCORE) + SCORE_BUMP


# =============================================================================
# STRUCTURE PRIMITIVES  (all work in 'long space'; SHORT = price-inverted mirror)
# =============================================================================


@dataclass(frozen=True)
class Swing:
    idx: int
    price: float
    kind: str  # "H" | "L"


@dataclass(frozen=True)
class Break:
    idx: int  # candle that closed beyond the level
    swing_idx: int
    level: float
    kind: str  # "MSB" (beyond a higher high: continuation) | "MSS" (beyond a lower high: shift)


@dataclass
class PDArray:
    kind: str  # "OB" | "FVG" | "BRK"
    lo: float
    hi: float
    idx: int  # formation index
    t: int
    tapped: bool = False
    valid: bool = True
    ref: int = -1


def find_swings(cs: list[Candle]) -> list[Swing]:
    """3-candle swings; confirmed only once the right-hand candle has closed."""
    out: list[Swing] = []
    for i in range(1, len(cs) - 1):
        if cs[i].h > cs[i - 1].h and cs[i].h > cs[i + 1].h:
            out.append(Swing(i, cs[i].h, "H"))
        if cs[i].l < cs[i - 1].l and cs[i].l < cs[i + 1].l:
            out.append(Swing(i, cs[i].l, "L"))
    return out


def _rising(v: list[float]) -> bool:
    return len(v) >= 2 and all(b > a for a, b in zip(v, v[1:]))


def _falling(v: list[float]) -> bool:
    return len(v) >= 2 and all(b < a for a, b in zip(v, v[1:]))


def is_uptrend(sw: list[Swing], n: int = TREND_SWINGS) -> bool:
    hs = [s.price for s in sw if s.kind == "H"][-n:]
    ls = [s.price for s in sw if s.kind == "L"][-n:]
    return len(hs) >= n and len(ls) >= n and _rising(hs) and _rising(ls)


def is_downtrend(sw: list[Swing], n: int = TREND_SWINGS) -> bool:
    hs = [s.price for s in sw if s.kind == "H"][-n:]
    ls = [s.price for s in sw if s.kind == "L"][-n:]
    return len(hs) >= n and len(ls) >= n and _falling(hs) and _falling(ls)


def trend_of(cs: list[Candle]) -> str:
    sw = find_swings(cs)
    if is_uptrend(sw):
        return "uptrend"
    if is_downtrend(sw):
        return "downtrend"
    return "sideways"


def trend_ok(cs: list[Candle], direction: str) -> bool:
    """Trend gate: LONG needs an uptrend, SHORT a downtrend (one code path via flip)."""
    return is_uptrend(find_swings(flip(cs, sign_of(direction))))


def find_up_breaks(cs: list[Candle], sw: list[Swing]) -> list[Break]:
    """Candle CLOSES (never wicks) above the most recent confirmed swing high; one break per swing.
    kind=MSB when the broken high is a higher high (continuation), MSS when it is a lower high (shift)."""
    highs = [s for s in sw if s.kind == "H"]
    out: list[Break] = []
    broken: set[int] = set()
    hp = 0
    for i in range(len(cs)):
        while hp < len(highs) and highs[hp].idx <= i - 2:
            hp += 1
        if hp == 0:
            continue
        h = highs[hp - 1]
        if h.idx in broken:
            continue
        if cs[i].c > h.price:
            prev = highs[hp - 2] if hp >= 2 else None
            kind = "MSS" if (prev is not None and h.price < prev.price) else "MSB"
            out.append(Break(i, h.idx, h.price, kind))
            broken.add(h.idx)
    return out


def swing_reaction(cs: list[Candle], sw: Swing) -> float:
    """Reaction away from a swing before it is taken out (long space: low -> up-move, high -> down-move)."""
    if sw.kind == "L":
        peak = None
        for j in range(sw.idx + 1, len(cs)):
            if cs[j].l < sw.price:
                break
            peak = cs[j].h if peak is None else max(peak, cs[j].h)
        return (peak - sw.price) if peak is not None else 0.0
    trough = None
    for j in range(sw.idx + 1, len(cs)):
        if cs[j].h > sw.price:
            break
        trough = cs[j].l if trough is None else min(trough, cs[j].l)
    return (sw.price - trough) if trough is not None else 0.0


# --- PD arrays ---------------------------------------------------------------


def eval_mitigation(cs: list[Candle], arr: PDArray, start: int) -> None:
    """Tapped: price trades into the zone. Invalid: a candle CLOSES through the far side (below lo)."""
    for j in range(start, len(cs)):
        if cs[j].l <= arr.hi:
            arr.tapped = True
        if cs[j].c < arr.lo:
            arr.valid = False
            break


def _ob_before_move(cs: list[Candle], origin_idx: int, end_idx: int) -> Optional[PDArray]:
    """Bullish OB: last down candle(s) before the up-move that started at origin_idx."""
    k = next((i for i in range(origin_idx, end_idx + 1) if cs[i].c > cs[i].o), None)
    if k is None:
        return None
    j, steps = k - 1, 0
    while j >= 0 and not (cs[j].c < cs[j].o) and steps < 5:
        j -= 1
        steps += 1
    if j < 0 or not (cs[j].c < cs[j].o):
        return None
    a = j
    while a - 1 >= 0 and cs[a - 1].c < cs[a - 1].o and (j - a) < OB_MAX_CANDLES - 1:
        a -= 1
    lo = min(cs[x].l for x in range(a, j + 1))
    hi = max(cs[x].h for x in range(a, j + 1))
    return PDArray("OB", lo, hi, j, cs[j].t)


def find_obs_long(cs: list[Candle], sw: list[Swing], brks: list[Break], max_n: int) -> list[PDArray]:
    out: list[PDArray] = []
    for b in brks[-max_n:]:
        lows = [s for s in sw if s.kind == "L" and s.idx < b.idx]
        if not lows:
            continue
        ob = _ob_before_move(cs, lows[-1].idx, b.idx)
        if ob is None:
            continue
        ob.ref = b.idx
        eval_mitigation(cs, ob, b.idx + 1)
        out.append(ob)
    return out


def find_fvgs_long(cs: list[Candle], start: int = 0) -> list[PDArray]:
    out: list[PDArray] = []
    for i in range(max(2, start), len(cs)):
        c1, c3 = cs[i - 2], cs[i]
        if c1.h < c3.l:
            arr = PDArray("FVG", c1.h, c3.l, i, c3.t, ref=i)
            eval_mitigation(cs, arr, i + 1)
            out.append(arr)
    return out


def find_breakers_long(cs: list[Candle], start: int = 0) -> list[PDArray]:
    """Bullish breaker = a failed BEARISH order block (price closed up through it)."""
    fl = flip(cs, -1)
    swf = find_swings(fl)
    brf = find_up_breaks(fl, swf)
    out: list[PDArray] = []
    for ob in find_obs_long(fl, swf, brf, BREAKER_MAX_OBS):  # bullish OBs of the inverted series
        hi_r, lo_r = -ob.lo, -ob.hi  # = bearish OB zone in real prices
        f = next((j for j in range(ob.ref + 1, len(cs)) if cs[j].c > hi_r), None)
        if f is None or f < start:
            continue
        arr = PDArray("BRK", lo_r, hi_r, f, cs[f].t, ref=ob.ref)
        eval_mitigation(cs, arr, f + 1)
        out.append(arr)
    return out


def _unflip_arr(a: PDArray, s: int) -> PDArray:
    lo, hi = sorted((s * a.lo, s * a.hi))
    return PDArray(a.kind, lo, hi, a.idx, a.t, a.tapped, a.valid, a.ref)


def detect_fvgs(cs: list[Candle], direction: str) -> list[PDArray]:
    s = sign_of(direction)
    return [_unflip_arr(a, s) for a in find_fvgs_long(flip(cs, s))]


def detect_order_blocks(cs: list[Candle], direction: str) -> list[PDArray]:
    s = sign_of(direction)
    L = flip(cs, s)
    sw = find_swings(L)
    return [_unflip_arr(a, s) for a in find_obs_long(L, sw, find_up_breaks(L, sw), OB_MAX_BREAKS)]


def detect_breakers(cs: list[Candle], direction: str) -> list[PDArray]:
    s = sign_of(direction)
    return [_unflip_arr(a, s) for a in find_breakers_long(flip(cs, s))]


def _overlap(a: PDArray, b: PDArray) -> bool:
    return a.lo <= b.hi and a.hi >= b.lo


# =============================================================================
# STRATEGY  (HTF map / MTF confirmation / LTF trigger)
# =============================================================================


@dataclass
class HTFMap:  # all prices in long space
    range_low: float
    range_high: float
    low_idx: int
    low_t: int
    peak_idx: int
    eq: float
    ote_lo: float
    ote_hi: float
    sweet: float
    msb_idx: int
    msb_age: int
    zone: PDArray
    stacked: int
    in_ote: bool
    atr: float
    strong: bool
    erl_fresh: bool
    irl_levels: list[float]
    guide: str


@dataclass
class MTFResult:
    mode: str
    evaluated: bool = False
    insufficient: bool = False
    a: Optional[bool] = None
    b: Optional[bool] = None
    c: Optional[bool] = None
    d: Optional[bool] = None
    e: Optional[bool] = None
    zone: Optional[PDArray] = None
    points: float = 0.0
    note: str = ""

    @property
    def aligned(self) -> bool:
        return bool(self.evaluated and self.b and self.c)

    def checks(self) -> dict[str, Optional[bool]]:
        return {"A": self.a, "B": self.b, "C": self.c, "D": self.d, "E": self.e}


@dataclass
class LTFTrigger:  # long space
    swept_price: float
    mss_level: float
    sweep_idx: int
    mss_idx: int
    mss_t: int
    ext_low: float
    bk_lo: float
    bk_hi: float
    entry: float
    sl: float


@dataclass
class Plan:  # long space
    entry: float
    sl: float
    tp1: float
    tp2: float
    tp3: Optional[float]
    rr_tp1: float
    rr_tp2: float


@dataclass
class Analysis:
    direction: str
    variant: str
    status: str = "NO_SIGNAL"  # SIGNAL | WATCHING | NO_SIGNAL
    reason: str = ""
    trace: list[str] = field(default_factory=list)
    score: float = 0.0
    signal: Optional[dict] = None
    watch: Optional[dict] = None
    trigger_key: str = ""
    ltf_last_t: int = 0


# --- Stage 1: HTF map --------------------------------------------------------


def build_htf_map(H: list[Candle]) -> tuple[Optional[HTFMap], str, list[str]]:
    """Structural HTF map in long space. Returns (map, failure reason, trace lines)."""
    lines: list[str] = []
    n = len(H)
    sw = find_swings(H)
    atr_s = atr_series(H)
    atr = last_atr(H)
    brks = find_up_breaks(H, sw)
    if not brks:
        return None, "no bullish MSB (close beyond a swing high) on HTF", lines
    m = brks[-1]
    age = n - 1 - m.idx
    lines.append(f"latest bullish break: {m.kind} at {fmt_ts(H[m.idx].t)} (age {age} bars), level {fmt_price(m.level)}")
    if age > HTF_MSB_MAX_AGE:
        return None, f"latest HTF MSB too old ({age} bars > {HTF_MSB_MAX_AGE})", lines
    low = None
    for s_ in reversed([x for x in sw if x.kind == "L" and x.idx < m.idx]):
        a = atr_s[s_.idx] or atr
        if swing_reaction(H, s_) >= SIG_ATR_MULT * a:
            low = s_
            break
    if low is None:
        return None, f"no significant swing low (reaction >= {SIG_ATR_MULT} x ATR) before the MSB", lines
    peak_idx = max(range(m.idx, n), key=lambda j: H[j].h)
    hi = H[peak_idx].h
    low_idx = min(range(low.idx, peak_idx + 1), key=lambda j: H[j].l)
    lo = H[low_idx].l
    size = hi - lo
    if size < SIG_ATR_MULT * atr:
        return None, f"range too small ({size:.6g} < {SIG_ATR_MULT} x ATR {atr:.6g})", lines
    if any(H[j].c < lo for j in range(low_idx + 1, n)):
        return None, "HTF bias invalid: a candle closed beyond the range far end (MSS / macro invalidation)", lines
    if not is_uptrend([x for x in sw if x.idx <= peak_idx]):
        return None, "HTF trend not obvious / against direction (sideways)", lines
    eq = (hi + lo) / 2
    ote_hi = hi - OTE_LO_RET * size
    ote_lo = hi - OTE_HI_RET * size
    sweet = hi - OTE_SWEET_RET * size
    lines.append(f"range {fmt_price(lo)} → {fmt_price(hi)} | EQ {fmt_price(eq)} | OTE {fmt_price(ote_lo)}–{fmt_price(ote_hi)}")

    arrays: list[PDArray] = []
    ob = _ob_before_move(H, low_idx, m.idx)
    if ob is not None:
        ob.ref = m.idx
        eval_mitigation(H, ob, m.idx + 1)
        arrays.append(ob)
    arrays += find_fvgs_long(H, low_idx)
    arrays += find_breakers_long(H, low_idx)
    arrays = [a for a in arrays if a.valid and a.hi > lo and a.lo < hi]
    cands = [a for a in arrays if (a.lo + a.hi) / 2 <= eq]  # discount only (LONG)
    if not cands:
        return None, "no valid PD array in discount (EQ and below)", lines

    since_peak_low = min(H[j].l for j in range(peak_idx, n))
    depth = (hi - since_peak_low) / size
    reversal_ctx = any(b.kind == "MSS" and 0 < m.idx - b.idx <= 30 for b in brks)
    guide = "BRK" if reversal_ctx else ("OB" if depth >= 0.618 else ("FVG" if depth < 0.5 else ""))

    def key(a: PDArray) -> tuple:
        stacked = sum(1 for o in arrays if o is not a and _overlap(a, o))
        in_ote = a.hi >= ote_lo and a.lo <= ote_hi
        return (in_ote, stacked, 1 if a.kind == guide else 0, -(a.lo + a.hi) / 2)

    best = max(cands, key=key)
    stacked = sum(1 for o in arrays if o is not best and _overlap(best, o))
    in_ote = best.hi >= ote_lo and best.lo <= ote_hi
    strong = sum(1 for b in brks if b.kind == "MSB" and n - 1 - b.idx <= STRONG_LOOKBACK) >= 2
    irl = [eq] + [x.price for x in sw if x.kind == "H" and x.idx >= low_idx and lo < x.price < hi]
    irl += [a.lo for a in arrays if a is not best and a.lo > lo]
    lines.append(f"PD array: {best.kind} {fmt_price(best.lo)}–{fmt_price(best.hi)} stacked={stacked} in_ote={in_ote} guide={guide or '-'}")
    hm = HTFMap(lo, hi, low_idx, H[low_idx].t, peak_idx, eq, ote_lo, ote_hi, sweet, m.idx, age, best, stacked, in_ote, atr, strong,
                age <= ERL_FRESH_BARS, sorted(set(irl)), guide)
    return hm, "", lines


# --- Stage 2: MTF confirmation ----------------------------------------------


def eval_mtf(M: Optional[list[Candle]], hm: HTFMap, cur: float, mode: str) -> MTFResult:
    res = MTFResult(mode=mode)
    if mode == "off":
        res.note = "MTF off"
        return res
    if M is None or len(M) < MIN_HISTORY:
        res.insufficient = True
        res.note = "insufficient MTF history"
        return res
    res.evaluated = True
    z = hm.zone
    n = len(M)
    sw = find_swings(M)
    # MTF-A: no MTF close beyond the HTF invalidation level since the range low formed
    res.a = not any(c.c < hm.range_low for c in M if c.t >= hm.low_t)
    # MTF-B: MTF price inside / touching the HTF PD array
    res.b = any(c.l <= z.hi and c.h >= z.lo for c in M[-MTF_TOUCH_BARS:]) or (z.lo <= cur <= z.hi)
    # MTF-D: MTF MSS in trade direction (close above a lower high)
    brks = find_up_breaks(M, sw)
    res.d = any(b.kind == "MSS" and n - 1 - b.idx <= MTF_MSS_LOOKBACK for b in brks)
    # MTF-C: pullback (or MSS already printed) and no fresh counter-direction MSB beyond the array far side
    pullback = is_downtrend(sw)
    fl = flip(M, -1)
    counter = any(n - 1 - b.idx <= MTF_COUNTER_MSB_BARS for b in find_up_breaks(fl, find_swings(fl))) and M[-1].c < z.lo
    res.c = bool((pullback or res.d) and not counter)
    # MTF-E: MTF PD array of the right type inside the HTF zone
    start = max(0, n - MTF_ARRAY_LOOKBACK)
    arrs = find_fvgs_long(M, start) + find_breakers_long(M, start) + find_obs_long(M, sw, brks, 3)
    inside = [a for a in arrs if a.valid and z.lo <= (a.lo + a.hi) / 2 <= z.hi]
    res.e = bool(inside)
    res.zone = max(inside, key=lambda a: a.idx) if inside else None
    res.points = (W_MTF_B if res.b else 0) + (W_MTF_C if res.c else 0) + (W_MTF_D if res.d else 0) + (W_MTF_E if res.e else 0)
    return res


# --- Stage 3: LTF breaker trigger --------------------------------------------


def find_ltf_trigger(L: list[Candle], z_lo: float, z_hi: float, mtf_zone: Optional[PDArray], atr: float) -> tuple[Optional[LTFTrigger], str]:
    n = len(L)
    sw = find_swings(L)
    lows = [s for s in sw if s.kind == "L"]
    highs = [s for s in sw if s.kind == "H"]
    tol = ZONE_TOL_ATR * atr
    best: Optional[LTFTrigger] = None
    progress, why = 0, "no recent LTF swing low"

    def note(p: int, msg: str) -> None:
        nonlocal progress, why
        if p >= progress:
            progress, why = p, msg

    for S in lows:
        if n - 1 - S.idx > LTF_LOOKBACK:
            continue
        t = next((j for j in range(S.idx + 1, n) if L[j].l < S.price), None)
        if t is None:
            note(1, "b) no LTF swing low taken out yet")
            continue
        prior = [h for h in highs if h.idx < S.idx]
        if not prior:
            continue
        A = prior[-1]  # the swing high that created that low
        if any(L[j].c > A.price for j in range(S.idx + 1, t)):
            continue
        if not is_downtrend([x for x in sw if x.idx <= t - 2]):
            note(1, "a) LTF not in a pullback (lower highs/lows) before the sweep")
            continue
        u = next((j for j in range(t, n) if L[j].c > A.price), None)
        if u is None:
            note(2, "c) low swept but no LTF MSS close above the swing high yet")
            continue
        if n - 1 - u > LTF_TRIGGER_MAX_AGE:
            note(2, "c) LTF MSS happened too long ago")
            continue
        ext = min(L[j].l for j in range(t, u + 1))
        if not (z_lo - tol <= ext <= z_hi + tol):
            note(3, "a) LTF sweep is not inside the HTF zone")
            continue
        if mtf_zone is not None and not (mtf_zone.lo - tol <= ext <= mtf_zone.hi + tol):
            note(3, "a) LTF sweep is not inside the MTF zone")
            continue
        b = max(range(S.idx, t), key=lambda j: L[j].h)  # top of the down-move that took the low
        j, k = b, 0
        while j >= 0 and not (L[j].c > L[j].o) and k < 3:
            j -= 1
            k += 1
        if j < 0 or not (L[j].c > L[j].o):
            note(3, "d) no up-candle breaker found before the down-move")
            continue
        a0 = j
        while a0 - 1 >= 0 and L[a0 - 1].c > L[a0 - 1].o and (j - a0) < OB_MAX_CANDLES - 1:
            a0 -= 1
        bk_lo = min(L[x].l for x in range(a0, j + 1))
        bk_hi = max(L[x].h for x in range(a0, j + 1))
        entry = (bk_lo + bk_hi) / 2
        sl = ext - SL_ATR_BUFFER * atr
        if not sl < entry:
            note(3, "d) breaker not above the stop level")
            continue
        if any(L[x].c < sl or L[x].c < bk_lo for x in range(u + 1, n)):
            note(3, "d) breaker failed (closed through) after the MSS")
            continue
        if best is None or u > best.mss_idx:
            best = LTFTrigger(S.price, A.price, t, u, L[u].t, ext, bk_lo, bk_hi, entry, sl)
    if best is not None:
        return best, ""
    return None, why


# --- Risk plan ---------------------------------------------------------------


def build_plan(hm: HTFMap, trig: LTFTrigger) -> tuple[Optional[Plan], str]:
    entry, sl = trig.entry, trig.sl
    risk = entry - sl
    if risk <= 0:
        return None, "non-positive risk"
    tp2 = hm.range_high
    rr2 = (tp2 - entry) / risk
    if rr2 < ABS_MIN_RR or rr2 < TP2_MIN_RR:
        return None, f"R:R to TP2 {rr2:.2f} < {TP2_MIN_RR}"
    tp1_min = entry + TP1_MIN_R * risk
    cands = [x for x in hm.irl_levels if tp1_min <= x < tp2]
    tp1 = min(cands) if cands else tp1_min
    tp3 = tp2 + TP3_EXTENSION * (hm.range_high - hm.range_low) if hm.strong else None
    if not (sl < entry < tp1 < tp2):
        return None, "sanity check failed (SL < entry < TP1 < TP2)"
    return Plan(entry, sl, tp1, tp2, tp3, (tp1 - entry) / risk, rr2), ""


# --- Score -------------------------------------------------------------------


def compute_score(hm: HTFMap, plan: Plan, mtf: MTFResult, mode: str,
                  mult: Optional[dict[str, float]] = None) -> tuple[float, dict[str, float]]:
    """Score 0-100.  `mult` = learned weight multipliers (None / empty = the default weights)."""
    w = eff_weights(mult)
    on: dict[str, bool] = {
        "htf_trend": True,
        "entry_zone": plan.entry <= hm.eq,
        "ote": hm.ote_lo <= plan.entry <= hm.ote_hi,
        "stacked": hm.stacked >= 1,
        "erl_fresh": bool(hm.erl_fresh),
        "ltf_mss": True,
        "rr3": plan.rr_tp2 >= RR_BONUS_THRESHOLD,
    }
    if mode != "off":
        on["mtf_b"], on["mtf_c"], on["mtf_d"], on["mtf_e"] = bool(mtf.b), bool(mtf.c), bool(mtf.d), bool(mtf.e)
    bd = {k: (w[k] if v else 0.0) for k, v in on.items()}
    raw = sum(bd.values())
    return normalize_score(raw, mode, w), bd


# --- Orchestration of the three stages ---------------------------------------


def rz(lo: float, hi: float, s: int) -> tuple[float, float]:
    a, b = s * lo, s * hi
    return (min(a, b), max(a, b))


def analyze_setup(direction: str, variant: Variant, htf_c: list[Candle], mtf_c: Optional[list[Candle]], ltf_c: list[Candle],
                  cur_price: float, mtf_mode: str, htf_cache: Optional[dict] = None) -> Analysis:
    """Pure analysis (no network). Candle lists must contain CLOSED candles only."""
    s = sign_of(direction)
    an = Analysis(direction=direction, variant=variant.name)
    tr = an.trace.append
    tr(f"[{direction}] {variant.chain} | MTF_MODE={mtf_mode}")
    if len(htf_c) < MIN_HISTORY:
        an.reason = "insufficient HTF history"
        tr(an.reason)
        return an
    if len(ltf_c) < MIN_HISTORY:
        an.reason = "insufficient LTF history"
        tr(an.reason)
        return an
    an.ltf_last_t = ltf_c[-1].t
    H, L = flip(htf_c, s), flip(ltf_c, s)
    cur = s * cur_price
    atr_l = last_atr(L)

    # ---- Stage 1 (HARD) ----
    key = (s, htf_c[-1].t, len(htf_c))
    if htf_cache is not None and key in htf_cache:
        hm, why, lines = htf_cache[key]
    else:
        hm, why, lines = build_htf_map(H)
        if htf_cache is not None:
            htf_cache[key] = (hm, why, lines)
    for ln in lines:
        tr("HTF: " + ln)
    if hm is None:
        an.reason = f"Stage 1 (HTF) failed: {why}"
        tr(an.reason)
        return an
    z = hm.zone
    touched = any(c.l <= z.hi and c.h >= z.lo for c in H[-HTF_TAP_BARS:]) and H[-1].c >= z.lo
    at_zone = touched or (z.lo <= cur <= z.hi)
    near = (cur - z.hi) <= WATCH_NEAR_ATR * hm.atr
    zl, zh = rz(z.lo, z.hi, s)

    def mk_watch(stage: str, missing: str) -> dict:
        return {"direction": direction, "variant": variant.name, "profile": variant.profile, "chain": variant.chain, "zone_kind": z.kind,
                "zone_lo": zl, "zone_hi": zh, "alert_level": s * z.hi, "stage": stage, "missing": missing, "at_zone": at_zone, "near": bool(at_zone or near)}

    if not at_zone:
        an.status = "WATCHING"
        an.reason = f"Stage 1: price has not arrived at the HTF {z.kind} (alert level {fmt_price(s * z.hi)})"
        an.watch = mk_watch("1", "price not yet at the HTF PD array")
        tr(an.reason)
        return an
    tr(f"HTF: price at the {z.kind} (touched={touched})")
    if touched:  # the PD array (IRL) has been tapped -> draw is external, target the range extreme
        draw = "external: IRL (PD array) taken → target the range extreme"
    elif hm.erl_fresh:  # ERL just taken, price now arriving in the array -> internal draw / pullback
        draw = "internal: ERL taken → pullback into a PD array in progress"
    else:
        draw = "internal: pullback into a PD array in progress"

    # ---- Stage 2 (MTF) ----
    mtf_res = eval_mtf(flip(mtf_c, s) if mtf_c is not None else None, hm, cur, mtf_mode)
    if mtf_mode == "off":
        tr("MTF: off (Stage 2 skipped)")
    elif mtf_res.insufficient:
        tr("MTF: insufficient history")
        if mtf_mode == "gate":
            an.reason = "Stage 2 (MTF, gate): insufficient MTF history"
            return an
    else:
        tr("MTF: " + " ".join(f"{k}={'-' if v is None else ('PASS' if v else 'fail')}" for k, v in mtf_res.checks().items())
           + f" points={mtf_res.points:g}")
        if mtf_res.a is False:
            an.reason = "Stage 2: MTF-A failed (MTF closed beyond the HTF invalidation level)"
            tr(an.reason)
            return an
        if mtf_mode == "gate" and not (mtf_res.b and mtf_res.c):
            an.status = "WATCHING"
            miss = "MTF-B (not at the HTF zone)" if not mtf_res.b else "MTF-C (pullback not ending / counter MSB)"
            an.reason = f"Stage 2 (gate): {miss}"
            an.watch = mk_watch("2", miss)
            tr(an.reason)
            return an

    # ---- Stage 3 (HARD) ----
    mtf_zone = mtf_res.zone if (mtf_res.evaluated and mtf_res.b and mtf_res.zone is not None) else None
    trig, why3 = find_ltf_trigger(L, z.lo, z.hi, mtf_zone, atr_l)
    if trig is None:
        an.status = "WATCHING"
        an.reason = f"Stage 3 (LTF trigger) incomplete: {why3}"
        an.watch = mk_watch("3", why3)
        tr(an.reason)
        return an
    tr(f"LTF: swept low {fmt_price(s * trig.swept_price)}, MSS close beyond {fmt_price(s * trig.mss_level)} at {fmt_ts(trig.mss_t)}, "
       f"breaker {fmt_price(rz(trig.bk_lo, trig.bk_hi, s)[0])}–{fmt_price(rz(trig.bk_lo, trig.bk_hi, s)[1])}")
    an.trigger_key = f"{variant.name}|{direction}|{trig.mss_t}"

    plan, why_p = build_plan(hm, trig)
    if plan is None:
        an.reason = f"Risk plan rejected: {why_p}"
        tr(an.reason)
        return an
    if abs(plan.entry - cur) > ENTRY_MAX_DIST_ATR * atr_l:
        an.status = "WATCHING"
        an.reason = f"Entry {fmt_price(s * plan.entry)} is > {ENTRY_MAX_DIST_ATR} x ATR(LTF) from price"
        an.watch = mk_watch("3", "entry too far from price (wait for retrace)")
        tr(an.reason)
        return an

    score, bd = compute_score(hm, plan, mtf_res, mtf_mode, WEIGHT_MULT or None)
    an.score = score
    tr(f"Score {score:.1f} breakdown={bd}")
    min_sc = min_score_for(variant.name)
    if score < min_sc:
        an.reason = f"Score {score:.1f} < min score {min_sc:g}" + (f" (learned; base {MIN_SCORE:g})" if abs(min_sc - MIN_SCORE) > 1e-9 else "")
        tr(an.reason)
        return an
    alt_score: Optional[float] = None
    if LEARN_MODE == "on" and (ALT_MULT or WEIGHT_MULT):
        alt_score = compute_score(hm, plan, mtf_res, mtf_mode, ALT_MULT or None)[0]
        tr(f"Alt score (other weight set) {alt_score:.1f}")

    rl, rh = rz(hm.range_low, hm.range_high, s)
    bl, bh = rz(trig.bk_lo, trig.bk_hi, s)
    mz = None
    if mtf_zone is not None:
        mlo, mhi = rz(mtf_zone.lo, mtf_zone.hi, s)
        mz = {"kind": mtf_zone.kind, "lo": mlo, "hi": mhi}
    an.status = "SIGNAL"
    an.signal = {
        "price": cur_price,
        "entry": s * plan.entry, "zone_lo": bl, "zone_hi": bh,
        "sl": s * plan.sl, "tp1": s * plan.tp1, "tp2": s * plan.tp2, "tp3": (s * plan.tp3) if plan.tp3 is not None else None,
        "rr_tp1": plan.rr_tp1, "rr_tp2": plan.rr_tp2,
        "score": score, "score_breakdown": bd, "alt_score": alt_score, "weights_ver": WEIGHTS_VER,
        "mtf_mode": mtf_mode, "mtf_aligned": mtf_res.aligned, "mtf_checks": mtf_res.checks(), "mtf_zone": mz,
        "htf_map": {"trend": "uptrend" if s > 0 else "downtrend", "range_low": rl, "range_high": rh, "eq": s * hm.eq,
                    "ote_lo": rz(hm.ote_lo, hm.ote_hi, s)[0], "ote_hi": rz(hm.ote_lo, hm.ote_hi, s)[1], "msb_age": hm.msb_age},
        "pd_array": {"kind": z.kind, "lo": zl, "hi": zh, "stacked": hm.stacked, "in_ote": hm.in_ote},
        "liquidity": draw,
        "ltf_trigger": {"swept": s * trig.swept_price, "mss_level": s * trig.mss_level, "mss_t": trig.mss_t, "breaker_lo": bl, "breaker_hi": bh},
        "invalidation": s * hm.range_low,
    }
    return an


# =============================================================================
# LIFECYCLE
# =============================================================================


def make_signal_record(inst: Instrument, v: Variant, an: Analysis, mode: str, created_ms: int) -> dict:
    assert an.signal is not None
    rec = {
        **SIGNAL_DEFAULTS,
        **an.signal,
        "id": f"{inst.id}-{v.name}-{an.direction}-{an.ltf_last_t}",
        "symbol": inst.id, "label": inst.label, "asset_class": inst.asset_class, "hl_coin": inst.hl_coin, "hl_dex": inst.hl_dex,
        "direction": an.direction, "profile": v.profile, "variant": v.name, "chain": v.chain, "htf": v.htf, "mtf": v.mtf, "ltf": v.ltf,
        "mtf_mode": mode, "status": "PENDING_FILL", "created_ms": created_ms, "signal_bar_ms": an.ltf_last_t,
        "last_checked_ms": an.ltf_last_t, "trigger_key": an.trigger_key, "tp1_hit": False, "bars_waited": 0,
    }
    return rec


def _close(sig: dict, status: str, r: float, t: int, win_type: Optional[str] = None) -> None:
    sig["status"], sig["result_r"], sig["closed_ms"] = status, r, t
    sig["win_type"] = win_type or ("SL" if status == "LOSS" else "EXPIRED" if status == "EXPIRED" else None)


def step_signal(sig: dict, c: Candle) -> list[str]:
    """Advance one signal by ONE closed LTF candle (shared by live monitor and backtest).
    Returns events: FILLED, TP1, WIN, LOSS, EXPIRED.  If SL and TP share a candle, SL is assumed first."""
    s = sign_of(sig["direction"])
    fc = flip([c], s)[0]
    entry, sl, tp1, tp2 = s * sig["entry"], s * sig["sl"], s * sig["tp1"], s * sig["tp2"]
    ev: list[str] = []
    st = sig["status"]
    if st == "PENDING_FILL":
        sig["bars_waited"] += 1
        if fc.l <= entry:  # a closed LTF candle traded through the limit
            sig["status"], sig["filled_ms"] = "ACTIVE", c.t
            ev.append("FILLED")
            if fc.l <= sl:  # conservative: stopped inside the fill candle
                _close(sig, "LOSS", -1.0, c.t)
                ev.append("LOSS")
            return ev
        if fc.h >= tp1:
            _close(sig, "EXPIRED", 0.0, c.t)
            return ["EXPIRED"]
        if sig["bars_waited"] >= FILL_EXPIRY_BARS:
            _close(sig, "EXPIRED", 0.0, c.t)
            return ["EXPIRED"]
        return ev
    if st in ("ACTIVE", "TP1_HIT"):
        if fc.l <= sl:
            if sig["tp1_hit"]:
                _close(sig, "WIN", float(sig["rr_tp1"]), c.t, "TP1_THEN_SL")  # SL after TP1 -> WIN credited at TP1's R (TP1 before SL = WIN)
                ev.append("WIN")
            else:
                _close(sig, "LOSS", -1.0, c.t)
                ev.append("LOSS")
            return ev
        if fc.h >= tp2:
            if not sig["tp1_hit"]:
                sig["tp1_hit"] = True
                ev.append("TP1")
            _close(sig, "WIN", float(sig["rr_tp2"]), c.t, "TP2")
            ev.append("WIN")
            return ev
        if st == "ACTIVE" and fc.h >= tp1:
            sig["status"], sig["tp1_hit"] = "TP1_HIT", True
            ev.append("TP1")
    return ev


def cooldown_key(sig: dict) -> str:
    return f"{sig['symbol']}|{sig['direction']}|{sig['profile']}"


def monitor_active_signals(state: dict, store: CandleStore, tg: Optional["TelegramClient"]) -> int:
    """Monitor each open signal one closed LTF candle at a time using that signal's own LTF."""
    resolved = 0
    for sig in state["signals"]:
        if sig["status"] not in OPEN_STATES:
            continue
        cs = store.closed(sig["hl_coin"], sig["ltf"])
        for c in [x for x in cs if x.t > sig["last_checked_ms"]]:
            events = step_signal(sig, c)
            sig["last_checked_ms"] = c.t
            for ev in events:
                if ev == "FILLED":
                    log.info("%s filled at %s", sig["id"], fmt_ts(c.t))
                    continue
                log.info("%s -> %s", sig["id"], ev)
                if tg is not None:
                    tg.send(format_lifecycle_message(sig, ev), reply_to=sig.get("tg_message_id"))
            if sig["status"] in TERMINAL_STATES:
                stats_on_resolve(state, sig)
                learn_on_resolve(state, sig)
                cd_mult = LEARN_THROTTLE_COOLDOWN_MULT if sig["symbol"] in THROTTLED else 1
                state["cooldowns"][cooldown_key(sig)] = c.t + COOLDOWN_BARS * cd_mult * TF_MS[sig["ltf"]]
                resolved += 1
                break
    return resolved



# =============================================================================
# SELF-LEARNING
# =============================================================================
# Target: WIN = TP1 reached before the SL (or SL after TP1).  Everything is slow, bounded and reversible.


def learn_on_resolve(state: dict, sig: dict) -> None:
    """Append one compact outcome record (WIN / LOSS only; EXPIRED = never filled = no outcome)."""
    if LEARN_MODE != "on" or sig["status"] not in ("WIN", "LOSS"):
        return
    L = state["learn"]
    bd = sig.get("score_breakdown") or {}
    L["log"].append({
        "id": sig["id"], "t": sig.get("closed_ms"), "sym": sig["symbol"], "var": sig["variant"], "dir": sig["direction"],
        "mode": sig.get("mtf_mode", "bonus"), "score": sig["score"], "alt": sig.get("alt_score"), "ver": sig.get("weights_ver", 0),
        "c": {k: (1 if bd[k] > 0 else 0) for k in LEARNABLE if k in bd},
        "win": 1 if sig["status"] == "WIN" else 0, "r": float(sig.get("result_r") or 0.0), "wt": sig.get("win_type"),
    })
    L["resolved_total"] += 1
    if len(L["log"]) > LEARN_LOG_MAX:
        L["log"] = L["log"][-LEARN_LOG_MAX:]


def auc(scores: list[float], wins: list[int]) -> Optional[float]:
    """P(score of a random winner > score of a random loser); 0.5 = no skill.  None if one class is empty."""
    pos = [s for s, w in zip(scores, wins) if w]
    neg = [s for s, w in zip(scores, wins) if not w]
    if not pos or not neg:
        return None
    gt = sum(1.0 if p > n else 0.5 if p == n else 0.0 for p in pos for n in neg)
    return gt / (len(pos) * len(neg))


def _avg_r(trades: list[dict]) -> float:
    return sum(t["r"] for t in trades) / len(trades) if trades else 0.0


def _rescale(mult: dict[str, float]) -> dict[str, float]:
    """Keep the total score budget at 100: learnable weights are rescaled, fixed ones are untouched; bounds enforced."""
    target = sum(BASE_WEIGHTS[k] for k in LEARNABLE)
    m = {k: mult.get(k, 1.0) for k in LEARNABLE}
    for _ in range(6):
        cur = sum(BASE_WEIGHTS[k] * m[k] for k in LEARNABLE)
        f = target / cur if cur > 0 else 1.0
        m = {k: min(LEARN_MULT_MAX, max(LEARN_MULT_MIN, m[k] * f)) for k in LEARNABLE}
    return {k: round(v, 4) for k, v in m.items()}


def _update_weights(L: dict) -> list[str]:
    win = L["log"][-LEARN_WINDOW:]
    if len(win) < LEARN_MIN_SAMPLE:
        return []
    W = L["weights"]
    mult = {k: W["mult"].get(k, 1.0) for k in LEARNABLE}
    pending: dict[str, int] = W["pending"]
    p0 = sum(t["win"] for t in win) / len(win)
    new = dict(mult)
    for c in LEARNABLE:
        on = [t for t in win if t["c"].get(c) == 1]
        off = [t for t in win if t["c"].get(c) == 0]
        if len(on) < LEARN_MIN_SIDE or len(off) < LEARN_MIN_SIDE:
            pending.pop(c, None)
            continue
        r_on = (sum(t["win"] for t in on) + LEARN_PRIOR_K * p0) / (len(on) + LEARN_PRIOR_K)
        r_off = (sum(t["win"] for t in off) + LEARN_PRIOR_K * p0) / (len(off) + LEARN_PRIOR_K)
        target = min(LEARN_MULT_MAX, max(LEARN_MULT_MIN, 1.0 + LEARN_GAIN * (r_on - r_off)))
        delta = target - mult[c]
        sign = 1 if delta > 0.01 else -1 if delta < -0.01 else 0
        if sign == 0:
            pending.pop(c, None)
            continue
        if pending.get(c) == sign:  # same direction in two consecutive updates -> apply one bounded step
            new[c] = mult[c] + max(-LEARN_STEP, min(LEARN_STEP, delta))
        else:
            pending[c] = sign
    new = _rescale(new)
    if any(abs(new[k] - mult[k]) > 1e-6 for k in LEARNABLE):
        W["mult"], W["ver"] = new, W["ver"] + 1
        moved = ", ".join(f"{k} x{new[k]:.2f}" for k in LEARNABLE if abs(new[k] - mult[k]) > 1e-6)
        return [f"weights v{W['ver']} ({LEARN_WEIGHTS}): {moved}"]
    return []


def _update_min_scores(L: dict) -> list[str]:
    out: list[str] = []
    for v in sorted({t["var"] for t in L["log"]}):
        tr = [t for t in L["log"] if t["var"] == v][-LEARN_VARIANT_WINDOW:]
        if len(tr) < LEARN_VARIANT_MIN:
            continue
        avg = _avg_r(tr)
        cur = L["min_score"].get(v, MIN_SCORE)
        new = cur
        if avg < 0.0:
            new = min(MIN_SCORE + LEARN_MIN_SCORE_MAX_RAISE, cur + 2.0)
        elif avg > 0.15:
            new = max(MIN_SCORE, cur - 1.0)  # never below the configured MIN_SCORE
        if abs(new - cur) > 1e-9:
            if new <= MIN_SCORE + 1e-9:
                L["min_score"].pop(v, None)
            else:
                L["min_score"][v] = new
            out.append(f"min score {v}: {cur:g} -> {new:g} (avgR {avg:+.2f} over {len(tr)})")
    return out


def _update_throttles(L: dict, now_ms: int) -> list[str]:
    out: list[str] = []
    for sym in sorted({t["sym"] for t in L["log"]}):
        if sym in L["throttle"]:
            continue
        all_sym = [t for t in L["log"] if t["sym"] == sym]
        tr = all_sym[-LEARN_THROTTLE_WINDOW:]
        if len(tr) < LEARN_THROTTLE_WINDOW:
            continue
        if len(all_sym) - L["throttle_n"].get(sym, -10**6) < LEARN_THROTTLE_WINDOW // 3:
            continue  # just came off a throttle: judge it on fresh trades, not the same stale ones
        avg = _avg_r(tr)
        if avg < LEARN_THROTTLE_MAX_AVG_R:
            L["throttle_n"][sym] = len(all_sym)
            L["throttle"][sym] = {"until_ms": now_ms + LEARN_THROTTLE_DAYS * DAY_MS, "avg_r": round(avg, 3)}
            out.append(f"throttle {sym}: avgR {avg:+.2f} over last {len(tr)} -> needs score >= {MIN_SCORE + LEARN_THROTTLE_SCORE_BONUS:g} for {LEARN_THROTTLE_DAYS}d")
    return out


def _eval_breaker(L: dict, now_ms: int) -> list[str]:
    b = L["breaker"]
    if b["until_ms"] > now_ms:
        return []
    if b["until_ms"] and not b.get("announced_end"):
        b["announced_end"] = True
        return ["circuit breaker expired: learning resumes"]
    if len(L["log"]) < LEARN_BREAKER_WINDOW:
        return []
    if b["until_ms"] and L["resolved_total"] - b["n_at_trip"] < LEARN_BREAKER_MIN_NEW:
        return []  # recovered recently: wait for fresh trades before judging again
    avg = _avg_r(L["log"][-LEARN_BREAKER_WINDOW:])
    if avg < LEARN_BREAKER_MAX_AVG_R:
        L["breaker"] = {"until_ms": now_ms + LEARN_BREAKER_HOURS * 3_600_000, "n_at_trip": L["resolved_total"], "avg_r": round(avg, 3)}
        return [f"CIRCUIT BREAKER: avgR {avg:+.2f} over last {LEARN_BREAKER_WINDOW} -> learning frozen, default weights, +{LEARN_BREAKER_SCORE_BUMP:g} min score for {LEARN_BREAKER_HOURS}h"]
    return []


def breaker_active(state: dict, now_ms: int) -> bool:
    return LEARN_GUARDS == "on" and state["learn"]["breaker"]["until_ms"] > now_ms


def learning_update(state: dict, now_ms: int) -> list[str]:
    """One learning pass (called once per live scan after trades were resolved).  Returns human-readable change lines."""
    if LEARN_MODE != "on":
        return []
    L, msgs = state["learn"], []
    for sym in [s for s, v in L["throttle"].items() if v["until_ms"] <= now_ms]:
        del L["throttle"][sym]
        msgs.append(f"throttle {sym} expired")
    if LEARN_GUARDS == "on":
        msgs += _eval_breaker(L, now_ms)
    if L["resolved_total"] - L["n_at_update"] < LEARN_UPDATE_EVERY:
        return msgs
    L["n_at_update"] = L["resolved_total"]
    if breaker_active(state, now_ms):
        return msgs  # frozen
    if LEARN_WEIGHTS != "off":
        msgs += _update_weights(L)
    if LEARN_GUARDS == "on":
        msgs += _update_min_scores(L)
        msgs += _update_throttles(L, now_ms)
    return msgs


def apply_learning_to_runtime(state: dict, now_ms: int) -> None:
    """Load what has been learned into the module-level runtime knobs used by analyze_setup / signal_blocked."""
    global WEIGHT_MULT, ALT_MULT, MIN_SCORE_BY_VARIANT, SCORE_BUMP, THROTTLED, WEIGHTS_VER
    WEIGHT_MULT, ALT_MULT, MIN_SCORE_BY_VARIANT, SCORE_BUMP, THROTTLED, WEIGHTS_VER = {}, {}, {}, 0.0, {}, 0
    if LEARN_MODE != "on":
        return
    L = state["learn"]
    mult = dict(L["weights"]["mult"])
    brk = breaker_active(state, now_ms)
    if LEARN_WEIGHTS == "live" and not brk:
        WEIGHT_MULT, ALT_MULT = mult, {}  # live uses learned; ALT = defaults (baseline comparison)
        WEIGHTS_VER = L["weights"]["ver"]
    elif LEARN_WEIGHTS == "shadow":
        WEIGHT_MULT, ALT_MULT = {}, mult  # live uses defaults; ALT = learned (shadow)
        WEIGHTS_VER = L["weights"]["ver"]
    if LEARN_GUARDS == "on":
        MIN_SCORE_BY_VARIANT = dict(L["min_score"])
        THROTTLED = {s: v["until_ms"] for s, v in L["throttle"].items() if v["until_ms"] > now_ms}
        SCORE_BUMP = LEARN_BREAKER_SCORE_BUMP if brk else 0.0


def build_learning_report(state: dict, now_ms: Optional[int] = None) -> list[str]:
    now_ms = now_ms or int(time.time() * 1000)
    L = state["learn"]
    log_ = L["log"]
    out = [f"Learning: mode={LEARN_MODE} weights={LEARN_WEIGHTS} guards={LEARN_GUARDS} | outcomes logged: {L['resolved_total']} (kept {len(log_)})"]
    if log_:
        wins = [t for t in log_ if t["win"]]
        wt: dict[str, int] = {}
        for t in wins:
            wt[t.get("wt") or "?"] = wt.get(t.get("wt") or "?", 0) + 1
        out.append(f"Wins by type: " + ", ".join(f"{k} {v}" for k, v in sorted(wt.items())) + f" | losses {len(log_) - len(wins)} | avgR {_avg_r(log_):+.2f}")
    m = L["weights"]["mult"]
    if m:
        out.append(f"Weight multipliers (v{L['weights']['ver']}): " + ", ".join(f"{k} x{m[k]:.2f}" for k in LEARNABLE if k in m and abs(m[k] - 1) > 1e-6))
    else:
        out.append(f"Weight multipliers: defaults (need {LEARN_MIN_SAMPLE} trades, {LEARN_MIN_SIDE}+ with and without each component)")
    paired = [t for t in log_ if t.get("alt") is not None]
    if len(paired) >= 10:
        a_live = auc([t["score"] for t in paired], [t["win"] for t in paired])
        a_alt = auc([t["alt"] for t in paired], [t["win"] for t in paired])
        if a_live is not None and a_alt is not None:
            line = f"Score skill (AUC, 0.5 = none) over {len(paired)} trades: live {a_live:.2f} vs alternate {a_alt:.2f}"
            if LEARN_WEIGHTS == "shadow" and len(paired) >= LEARN_PROMOTE_MIN_N and a_alt >= a_live + LEARN_PROMOTE_AUC_GAIN:
                line += "  -> learned weights look better: consider LEARN_WEIGHTS=live"
            out.append(line)
    if L["min_score"]:
        out.append("Learned min scores: " + ", ".join(f"{k} {v:g}" for k, v in sorted(L["min_score"].items())))
    thr = {s: v for s, v in L["throttle"].items() if v["until_ms"] > now_ms}
    if thr:
        out.append("Throttled: " + ", ".join(f"{s} (avgR {v['avg_r']:+.2f}, {max(0, (v['until_ms'] - now_ms) // DAY_MS)}d left)" for s, v in sorted(thr.items())))
    if L["breaker"]["until_ms"] > now_ms:
        out.append(f"CIRCUIT BREAKER active (avgR {L['breaker']['avg_r']:+.2f}) until {fmt_ts(L['breaker']['until_ms'])}")
    return out


# =============================================================================
# TELEGRAM
# =============================================================================


def esc(text: Any) -> str:
    t = str(text)
    for ch in ("_", "*", "`", "["):
        t = t.replace(ch, "\\" + ch)
    return t


class TelegramClient:
    def __init__(self, token: str, chat_id: str) -> None:
        self.token, self.chat_id = token, chat_id
        self.session = requests.Session()

    @classmethod
    def from_env(cls) -> "TelegramClient":
        tok, chat = os.environ.get("TG_BOT_TOKEN", "").strip(), os.environ.get("TG_CHAT_ID", "").strip()
        if not tok or not chat:
            raise RuntimeError("TG_BOT_TOKEN and TG_CHAT_ID environment variables are required")
        return cls(tok, chat)

    def send(self, text: str, reply_to: Optional[int] = None) -> Optional[int]:
        payload: dict[str, Any] = {"chat_id": self.chat_id, "text": text[:4000], "parse_mode": "Markdown", "disable_web_page_preview": True}
        if reply_to:
            payload["reply_to_message_id"] = reply_to
            payload["allow_sending_without_reply"] = True
        for attempt in range(3):
            try:
                r = self.session.post(f"https://api.telegram.org/bot{self.token}/sendMessage", json=payload, timeout=15)
                if r.status_code == 400 and payload.get("parse_mode"):
                    payload.pop("parse_mode")  # malformed Markdown: resend as plain text
                    continue
                if r.status_code == 429:
                    time.sleep(float(r.json().get("parameters", {}).get("retry_after", 2)))
                    continue
                r.raise_for_status()
                return int(r.json()["result"]["message_id"])
            except (requests.RequestException, ValueError, KeyError) as e:
                log.warning("telegram send failed (%s), attempt %d", e, attempt + 1)
                time.sleep(1.5 * (attempt + 1))
        return None


def _checks_line(sig: dict) -> str:
    ch = sig.get("mtf_checks") or {}
    names = {"A": "A invalidation", "B": "B at zone", "C": "C pullback end", "D": "D MSS", "E": "E MTF array"}
    parts = []
    for k in "ABCDE":
        v = ch.get(k)
        parts.append(f"{k}{'✅' if v else ('➖' if v is None else '❌')}")
    return " ".join(parts)


def format_signal_message(sig: dict) -> str:
    d = sig["direction"]
    icon = "🟢" if d == "LONG" else "🔴"
    p = fmt_price
    hm, pd, lt = sig["htf_map"], sig["pd_array"], sig["ltf_trigger"]
    L = [f"{icon} *{d}* — *{esc(sig['label'])}* ({esc(sig['symbol'])})", f"*Profile:* {esc(sig['chain'])}  [{esc(sig['variant'])}]"]
    if sig["mtf_mode"] != "off":
        L.append("✅ 3-TF ALIGNED" if sig["mtf_aligned"] else "⚠️ 2-TF (MTF not aligned)")
    L += [f"*Score:* {sig['score']:.0f}/100   *Price:* {p(sig['price'])}", "",
          f"*HTF map ({esc(sig['htf'])}):* {hm['trend']}, range {p(hm['range_low'])}–{p(hm['range_high'])}, EQ {p(hm['eq'])}, "
          f"OTE {p(hm['ote_lo'])}–{p(hm['ote_hi'])}",
          f"*PD array:* {pd['kind']} {p(pd['lo'])}–{p(pd['hi'])} (stacked {pd['stacked']}, OTE {'yes' if pd['in_ote'] else 'no'})"]
    if sig["mtf_mode"] == "off":
        L.append("*MTF:* off")
    else:
        mz = sig.get("mtf_zone")
        L.append(f"*MTF ({esc(sig['mtf'])}, {sig['mtf_mode']}):* {_checks_line(sig)}" + (f" | {mz['kind']} {p(mz['lo'])}–{p(mz['hi'])}" if mz else ""))
    L += [f"*Liquidity:* {esc(sig['liquidity'])}",
          f"*LTF trigger ({esc(sig['ltf'])}):* swept {p(lt['swept'])}, MSS close beyond {p(lt['mss_level'])}, breaker {p(lt['breaker_lo'])}–{p(lt['breaker_hi'])}",
          "",
          f"*ENTRY (limit, 50%):* {p(sig['entry'])}  (zone {p(sig['zone_lo'])}–{p(sig['zone_hi'])})",
          f"*SL:* {p(sig['sl'])}",
          f"*TP1:* {p(sig['tp1'])} ({sig['rr_tp1']:.2f}R, take ~50%)",
          f"*TP2:* {p(sig['tp2'])} ({sig['rr_tp2']:.2f}R)"]
    if sig.get("tp3"):
        L.append(f"*TP3:* {p(sig['tp3'])}")
    word = "below" if d == "LONG" else "above"
    L.append(f"*Invalidation:* HTF close {word} {p(sig['invalidation'])}")
    L.append(f"_Fill expires after {FILL_EXPIRY_BARS} {esc(sig['ltf'])} bars. Decision support only._")
    return "\n".join(L)


def format_lifecycle_message(sig: dict, event: str) -> str:
    p = fmt_price
    head = f"*{esc(sig['label'])}* {sig['direction']} {esc(sig['chain'])}"
    if event == "TP1":
        return f"🎯 {head}\nTP1 hit at {p(sig['tp1'])} (+{sig['rr_tp1']:.2f}R). SL unchanged at {p(sig['sl'])} — consider moving to breakeven manually."
    if event == "WIN":
        wt = sig.get("win_type")
        how = ("TP2 hit" if wt == "TP2" else "stopped after TP1") if wt else (
            "TP2 hit" if sig["result_r"] and abs(sig["result_r"] - sig["rr_tp2"]) < 1e-9 and sig["tp1_hit"] and sig["closed_ms"] else "stopped after TP1")
        return f"✅ {head}\nWIN ({how}) — {sig['result_r']:+.2f}R"
    if event == "LOSS":
        return f"❌ {head}\nLOSS — stopped out at {p(sig['sl'])} (-1.00R)"
    return f"⌛ {head}\nEXPIRED — entry not filled (or TP1/invalid before fill). Excluded from stats."


def _agg(recs: list[dict]) -> dict[str, float]:
    b = new_bucket()
    for r in recs:
        if r["status"] == "WIN":
            b["wins"] += 1
            b["gross_profit_r"] += float(r["result_r"] or 0)
        elif r["status"] == "LOSS":
            b["losses"] += 1
            b["gross_loss_r"] += abs(float(r["result_r"] or 0))
        elif r["status"] == "EXPIRED":
            b["expired"] += 1
            continue
        else:
            continue
        b["sum_r"] += float(r["result_r"] or 0)
        if r.get("filled_ms") and r.get("closed_ms"):
            b["sum_hold_minutes"] += max(0, (r["closed_ms"] - r["filled_ms"]) / 60000)
    return b


def _fmt_stats(b: dict) -> str:
    n = b["wins"] + b["losses"]
    wr = f"{b['wins'] / n * 100:.0f}%" if n else "–"
    pf = "∞" if b["gross_loss_r"] == 0 and b["gross_profit_r"] > 0 else (f"{b['gross_profit_r'] / b['gross_loss_r']:.2f}" if b["gross_loss_r"] else "–")
    avg = f"{b['sum_r'] / n:+.2f}R" if n else "–"
    hold = f"{b['sum_hold_minutes'] / n / 60:.1f}h" if n else "–"
    return f"{b['wins']}W/{b['losses']}L, WR {wr}, PF {pf}, avgR {avg}, hold {hold}"


def build_daily_summary(state: dict, now_ms: int) -> str:
    since = now_ms - DAY_MS
    sigs = state["signals"]
    created = [s for s in sigs if s.get("created_ms", 0) >= since]
    resolved = [s for s in sigs if s["status"] in TERMINAL_STATES and (s.get("closed_ms") or 0) >= since]
    tot = _agg(resolved)
    L = ["📊 *Daily summary (last 24h)*",
         f"Signals: {len(created)} | Wins {int(tot['wins'])} | Losses {int(tot['losses'])} | Expired {int(tot['expired'])}",
         _fmt_stats(tot), ""]

    def group(title: str, keyfn: Callable[[dict], str]) -> None:
        keys = sorted({keyfn(r) for r in resolved})
        if not keys:
            return
        L.append(f"*{title}*")
        for k in keys:
            L.append(f"• {esc(k)}: {_fmt_stats(_agg([r for r in resolved if keyfn(r) == k]))}")

    group("By direction", lambda r: r["direction"])
    group("By profile", lambda r: r["profile"])
    group("By score bucket", lambda r: score_bucket(r["score"]))
    group("By alignment", lambda r: "3-TF aligned" if r.get("mtf_aligned") else "not aligned")
    group("By outcome type", lambda r: r.get("win_type") or r["status"])
    t = state["totals"]
    L += ["", f"_Lifetime:_ {int(t['signals'])} signals — {_fmt_stats(t)}"]
    if LEARN_MODE == "on":
        L += ["", "*Learning*"] + [esc(x) for x in build_learning_report(state, now_ms)]
    return "\n".join(L)


def build_watch_digest(state: dict) -> Optional[str]:
    items = [w for w in state["watching"].values() if w.get("near")]
    if not items:
        return None
    L = ["👀 *Watching digest*"]
    for w in sorted(items, key=lambda x: (x["label"], x["direction"])):
        L.append(f"• *{esc(w['label'])}* {w['direction']} {esc(w['chain'])} — {w['zone_kind']} {fmt_price(w['zone_lo'])}–{fmt_price(w['zone_hi'])} "
                 f"(stage {w['stage']} missing: {esc(w['missing'])})")
    return "\n".join(L)


# =============================================================================
# MAIN  (scan orchestration, explain)
# =============================================================================


def validate_watchlist(client: HyperliquidClient) -> tuple[list[Instrument], dict[str, float], list[str]]:
    """Check each coin exists in the HL universe; skip + report the rest. Returns (valid, marks, skipped msgs)."""
    valid: list[Instrument] = []
    marks: dict[str, float] = {}
    skipped: list[str] = []
    universes: dict[str, Optional[set[str]]] = {}
    for dex in sorted({i.hl_dex for i in WATCHLIST}):
        try:
            names, mk = client.meta_and_ctxs(dex)
            universes[dex] = set(names)
            marks.update(mk)
        except Exception as e:  # noqa: BLE001
            universes[dex] = None
            log.error("universe fetch failed for dex '%s': %s", dex or "core", e)
    for inst in WATCHLIST:
        uni = universes.get(inst.hl_dex)
        if uni is None:
            skipped.append(f"{inst.label}: universe for dex '{inst.hl_dex or 'core'}' unavailable")
        elif inst.hl_coin not in uni:
            skipped.append(f"{inst.label}: coin '{inst.hl_coin}' not found in the Hyperliquid universe")
        else:
            valid.append(inst)
    return valid, marks, skipped


def select_instruments(valid: list[Instrument], symbol: Optional[str]) -> list[Instrument]:
    if not symbol:
        return valid
    q = symbol.strip().upper()
    out = [i for i in valid if q in (i.id.upper(), i.label.upper(), i.hl_coin.upper(), i.hl_coin.split(":")[-1].upper())]
    if not out:
        raise SystemExit(f"Symbol '{symbol}' not on the (valid) watchlist: {[i.label for i in valid]}")
    return out


def needed_frames(variants: list[Variant], mtf_mode: str) -> set[str]:
    tfs: set[str] = set()
    for v in variants:
        tfs.update({v.htf, v.ltf})
        if mtf_mode != "off":
            tfs.add(v.mtf)
    return tfs


def evaluate_symbol(inst: Instrument, variants: list[Variant], store: CandleStore, mtf_mode: str, directions: tuple[str, ...],
                    last_eval: dict, marks: dict[str, float], now_ms: int, force: bool = False) -> list[dict]:
    out: list[dict] = []
    for v in variants:
        try:
            htf = store.closed(inst.hl_coin, v.htf)
            ltf = store.closed(inst.hl_coin, v.ltf)
            mtf = store.closed(inst.hl_coin, v.mtf) if mtf_mode != "off" else None
            if len(htf) < MIN_HISTORY:
                log.info("%s %s: insufficient HTF history (%d %s candles) - variant skipped", inst.label, v.name, len(htf), v.htf)
                continue
            if len(ltf) < MIN_HISTORY:
                log.info("%s %s: insufficient LTF history - variant skipped", inst.label, v.name)
                continue
            if is_stale(ltf, v.ltf, now_ms) or is_stale(htf, v.htf, now_ms):
                log.warning("%s %s: stale feed - skipped", inst.label, v.name)
                continue
            if mtf_mode == "gate" and (mtf is None or len(mtf) < MIN_HISTORY):
                log.info("%s %s: insufficient MTF history in gate mode - skipped", inst.label, v.name)
                continue
            if mtf_mode == "bonus" and (mtf is None or len(mtf) < MIN_HISTORY):
                log.info("%s %s: insufficient MTF history - MTF score 0, mtf_aligned False", inst.label, v.name)
            ek = f"{inst.id}|{v.name}|{mtf_mode}"
            if not force and last_eval.get(ek) == ltf[-1].t:
                continue  # no new closed LTF candle since the last evaluation
            cur = marks.get(inst.hl_coin) or ltf[-1].c
            cache: dict = {}
            ans = {d: analyze_setup(d, v, htf, mtf, ltf, cur, mtf_mode, cache) for d in directions}
            out.append({"inst": inst, "variant": v, "ltf_t": ltf[-1].t, "ek": ek, "analyses": ans})
        except Exception as e:  # noqa: BLE001  never crash the scan for one symbol
            log.exception("%s %s: evaluation error: %s", inst.label, v.name, e)
    return out


def run_scan(args: argparse.Namespace, mtf_mode: str, directions: tuple[str, ...], variants: list[Variant]) -> int:
    dry = args.dry_run
    ss = StateStore(STATE_FILE)
    if not dry and not ss.acquire():
        log.warning("another run holds the state lock - exiting cleanly")
        return 0
    try:
        tg = None if dry else TelegramClient.from_env()  # raises if env vars missing
        state = ss.load()
        now_ms = int(time.time() * 1000)
        apply_learning_to_runtime(state, now_ms)
        client = HyperliquidClient()
        valid, marks, skipped = validate_watchlist(client)
        for msg in skipped:
            log.warning("SKIPPED (not in HL universe): %s", msg)
            if tg and msg not in state["reported_skipped"]:
                tg.send(f"⚠️ Watchlist skip: {esc(msg)}")
                state["reported_skipped"].append(msg)
        insts = select_instruments(valid, args.symbol)
        if args.profile:
            variants = [v for v in variants if v.profile == args.profile.upper()]
            if not variants:
                raise SystemExit(f"No enabled variant for profile {args.profile}")
        log.info("scan: %d symbols, variants=%s, mtf_mode=%s, directions=%s", len(insts), [v.name for v in variants], mtf_mode, directions)

        store = CandleStore(CANDLE_CACHE_FILE)
        tasks: set[tuple[str, str]] = {(i.hl_coin, tf) for i in insts for tf in needed_frames(variants, mtf_mode)}
        for sg in state["signals"]:  # open signals' LTF candles are always fetched (monitoring, incl. orphans)
            if sg["status"] in OPEN_STATES:
                tasks.add((sg["hl_coin"], sg["ltf"]))

        def fetch(task: tuple[str, str]) -> None:
            try:
                store.refresh(client, task[0], task[1], now_ms)
            except Exception as e:  # noqa: BLE001
                log.warning("candle fetch failed %s %s: %s", task[0], task[1], e)

        with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as ex:
            list(ex.map(fetch, sorted(tasks)))
        store.save()

        if dry:
            log.info("dry-run: lifecycle monitoring skipped (no state writes)")
        else:
            monitor_active_signals(state, store, tg)
            changes = learning_update(state, now_ms)
            if changes:
                for ch in changes:
                    log.info("LEARN: %s", ch)
                if tg:
                    tg.send("🧠 *Learning update*\n" + "\n".join("• " + esc(c) for c in changes))
            apply_learning_to_runtime(state, now_ms)  # new weights / thresholds apply to this scan's evaluation

        with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as ex:
            results = [r for sub in ex.map(lambda i: evaluate_symbol(i, variants, store, mtf_mode, directions, state["last_eval"], marks, now_ms), insts)
                       for r in sub]

        candidates: list[tuple[dict, Analysis]] = []
        for r in results:
            inst, v = r["inst"], r["variant"]
            state["last_eval"][r["ek"]] = r["ltf_t"]
            for d, an in r["analyses"].items():
                wk = f"{inst.id}|{v.name}|{d}"
                if an.status == "WATCHING" and an.watch:
                    state["watching"][wk] = {**an.watch, "symbol": inst.id, "label": inst.label, "ts": now_ms}
                else:
                    state["watching"].pop(wk, None)
                if an.status == "SIGNAL":
                    candidates.append((r, an))
                else:
                    log.info("%s %s %s: %s %s", inst.label, v.name, d, an.status, an.reason)

        # dedup across variants: same symbol + direction + profile -> highest score only
        best: dict[tuple, tuple[dict, Analysis]] = {}
        for r, an in candidates:
            k = (r["inst"].id, an.direction, r["variant"].profile)
            if k not in best or an.score > best[k][1].score:
                best[k] = (r, an)
        emitted = 0
        for r, an in sorted(best.values(), key=lambda x: -x[1].score):
            inst, v = r["inst"], r["variant"]
            reason = signal_blocked(state, inst, v, an)
            if reason:
                log.info("%s %s %s blocked: %s", inst.label, v.name, an.direction, reason)
                continue
            rec = make_signal_record(inst, v, an, mtf_mode, now_ms)
            msg = format_signal_message(rec)
            if dry:
                print("\n" + "=" * 70 + "\n" + msg.replace("\\", "") + "\n" + "=" * 70)
            else:
                rec["tg_message_id"] = tg.send(msg) if tg else None
            state["signals"].append(rec)
            state["triggers_seen"][an.trigger_key + "|" + inst.id] = now_ms
            stats_on_emit(state, rec)
            emitted += 1
            log.info("SIGNAL %s %s %s score=%.0f", inst.label, an.direction, v.chain, an.score)

        if not dry and tg:
            if now_ms - state["last_digest_ts"] >= WATCH_DIGEST_EVERY_H * 3_600_000:
                digest = build_watch_digest(state)
                if digest:
                    tg.send(digest)
                state["last_digest_ts"] = now_ms
            today = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
            if today.hour >= DAILY_SUMMARY_HOUR_UTC and state["last_daily_summary"] != today.strftime("%Y-%m-%d"):
                tg.send(build_daily_summary(state, now_ms))
                state["last_daily_summary"] = today.strftime("%Y-%m-%d")
            ss.save()
        log.info("scan done: %d new signals", emitted)
        return 0
    finally:
        ss.release()


def signal_blocked(state: dict, inst: Instrument, v: Variant, an: Analysis) -> str:
    d = an.direction
    if inst.id in THROTTLED and an.score < MIN_SCORE + LEARN_THROTTLE_SCORE_BONUS:
        return f"symbol throttled by learning (needs score >= {MIN_SCORE + LEARN_THROTTLE_SCORE_BONUS:g})"
    ck = f"{inst.id}|{d}|{v.profile}"
    if state["cooldowns"].get(ck, 0) > an.ltf_last_t:
        return "cooldown"
    if (an.trigger_key + "|" + inst.id) in state["triggers_seen"]:
        return "trigger already signalled"
    opens = [s for s in state["signals"] if s["status"] in OPEN_STATES]
    if any(s["symbol"] == inst.id and s["profile"] == v.profile and s["direction"] == d for s in opens):
        return "open signal exists (same symbol/direction/profile)"
    if any(s["symbol"] == inst.id and s["profile"] == v.profile and s["direction"] != d for s in opens):
        return "opposite-direction signal open (same symbol/profile)"
    if inst.id in CRYPTO_CLUSTER_IDS:
        n = sum(1 for s in opens if s["direction"] == d and s["symbol"] in CRYPTO_CLUSTER_IDS)
        if n >= CLUSTER_CAP:
            return f"correlation cap ({n} open {d} in the crypto cluster)"
    return ""


def run_explain(args: argparse.Namespace, mtf_mode: str, directions: tuple[str, ...], variants: list[Variant]) -> int:
    if not args.symbol:
        raise SystemExit("--explain requires --symbol")
    client = HyperliquidClient()
    valid, marks, skipped = validate_watchlist(client)
    insts = select_instruments(valid, args.symbol)
    if args.profile:
        prof = args.profile.upper()
        chosen = [v for v in variants if v.profile == prof] or [v for v in ALL_VARIANTS.values() if v.profile == prof]
        if not chosen:
            raise SystemExit(f"Unknown profile {args.profile}; valid: {list(PROFILES)}")
        variants = chosen
    now_ms = int(time.time() * 1000)
    store = CandleStore(CANDLE_CACHE_FILE)
    for inst in insts:
        for tf in needed_frames(variants, mtf_mode):
            store.refresh(client, inst.hl_coin, tf, now_ms)
        store.save()
        for r in evaluate_symbol(inst, variants, store, mtf_mode, directions, {}, marks, now_ms, force=True):
            print(f"\n##### {inst.label} / {r['variant'].chain} (variant {r['variant'].name}) — active MTF_MODE={mtf_mode}")
            for d, an in r["analyses"].items():
                print("\n".join("  " + t for t in an.trace))
                print(f"  => {an.status}" + (f": {an.reason}" if an.reason else "") + (f" (score {an.score:.1f})" if an.status == "SIGNAL" else ""))
    return 0


# =============================================================================
# BACKTEST
# =============================================================================


def bt_stats(trades: list[dict]) -> dict[str, float]:
    res = [t for t in trades if t["status"] in ("WIN", "LOSS")]
    n = len(res)
    wins = sum(1 for t in res if t["status"] == "WIN")
    gp = sum(t["r"] for t in res if t["r"] > 0)
    gl = abs(sum(t["r"] for t in res if t["r"] < 0))
    eq = peak = dd = 0.0
    for t in sorted(res, key=lambda x: x["t_close"]):
        eq += t["r"]
        peak = max(peak, eq)
        dd = max(dd, peak - eq)
    return {"n": n, "wr": wins / n * 100 if n else 0.0, "pf": (gp / gl) if gl else (float("inf") if gp > 0 else 0.0),
            "avg_r": eq / n if n else 0.0, "total_r": eq, "max_dd": dd}


def _row(label: str, st: dict) -> str:
    pf = "inf" if st["pf"] == float("inf") else f"{st['pf']:.2f}"
    return f"{label:<26}{int(st['n']):>6}{st['wr']:>8.1f}%{pf:>8}{st['avg_r']:>+9.2f}{st['max_dd']:>9.2f}{st['total_r']:>+10.2f}"


HEADER = f"{'':<26}{'n':>6}{'WR':>9}{'PF':>8}{'avgR':>9}{'maxDD':>9}{'totalR':>10}"


def bt_collect(client: HyperliquidClient, insts: list[Instrument], variants: list[Variant], days: int, need_mtf: bool, now_ms: int) -> dict[tuple[str, str], list[Candle]]:
    data: dict[tuple[str, str], list[Candle]] = {}
    tasks: set[tuple[str, str]] = set()
    for i in insts:
        for v in variants:
            tasks.update({(i.hl_coin, v.htf), (i.hl_coin, v.ltf)})
            if need_mtf:
                tasks.add((i.hl_coin, v.mtf))
    start0 = now_ms - days * DAY_MS

    def fetch(task: tuple[str, str]) -> tuple[tuple[str, str], list[Candle]]:
        coin, tf = task
        try:
            rows = client.candles_range(coin, tf, start0 - (CANDLE_COUNTS[tf] + 2) * TF_MS[tf], now_ms)
        except Exception as e:  # noqa: BLE001
            log.warning("backtest fetch failed %s %s: %s", coin, tf, e)
            rows = []
        return task, [c for c in map(candle_from_row, rows) if is_closed(c, tf, now_ms)]

    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as ex:
        for task, cs in ex.map(fetch, sorted(tasks)):
            data[task] = cs
            log.info("backtest data %s %s: %d candles", task[0], task[1], len(cs))
    return data


def bt_run_mode(data: dict[tuple[str, str], list[Candle]], insts: list[Instrument], variants: list[Variant], directions: tuple[str, ...],
                mode: str, days: int, now_ms: int) -> list[dict]:
    """Bar-by-bar replay with the SAME analyze_setup(); only candles closed at each bar are visible."""
    trades: list[dict] = []
    t_start = now_ms - days * DAY_MS
    for inst in insts:
        for v in variants:
            htf, ltf = data.get((inst.hl_coin, v.htf), []), data.get((inst.hl_coin, v.ltf), [])
            mtf = data.get((inst.hl_coin, v.mtf), []) if mode != "off" else None
            if len(htf) < MIN_HISTORY or len(ltf) < MIN_HISTORY:
                log.info("backtest %s %s: insufficient history", inst.label, v.name)
                continue
            if mode == "gate" and (not mtf or len(mtf) < MIN_HISTORY):
                continue
            Th = [c.T for c in htf]
            Tm = [c.T for c in mtf] if mtf else []
            cache: dict = {}
            open_sig: Optional[dict] = None
            cooldown_until = 0
            seen: set[str] = set()
            ltf_ms = TF_MS[v.ltf]
            for i, c in enumerate(ltf):
                if c.t < t_start:
                    continue
                if open_sig is not None:
                    step_signal(open_sig, c)
                    if open_sig["status"] in TERMINAL_STATES:
                        trades.append({"symbol": inst.label, "profile": v.profile, "variant": v.name, "direction": open_sig["direction"],
                                       "aligned": bool(open_sig["mtf_aligned"]), "status": open_sig["status"], "r": float(open_sig["result_r"] or 0),
                                       "t_open": open_sig["created_ms"], "t_close": c.t, "score": open_sig["score"], "win_type": open_sig.get("win_type")})
                        cooldown_until = c.t + COOLDOWN_BARS * ltf_ms
                        open_sig = None
                    continue
                if c.t < cooldown_until:
                    continue
                bar_close = c.T + 1
                nh = bisect.bisect_left(Th, bar_close)
                hs = htf[max(0, nh - CANDLE_COUNTS[v.htf]):nh]
                ms = None
                if mtf is not None:
                    nm = bisect.bisect_left(Tm, bar_close)
                    ms = mtf[max(0, nm - CANDLE_COUNTS[v.mtf]):nm]
                    if mode == "gate" and len(ms) < MIN_HISTORY:
                        continue
                ls = ltf[max(0, i + 1 - CANDLE_COUNTS[v.ltf]):i + 1]
                if len(hs) < MIN_HISTORY or len(ls) < MIN_HISTORY:
                    continue
                best: Optional[Analysis] = None
                for d in directions:
                    an = analyze_setup(d, v, hs, ms, ls, c.c, mode, cache)
                    if an.status == "SIGNAL" and an.trigger_key not in seen and (best is None or an.score > best.score):
                        best = an
                if best is not None:
                    seen.add(best.trigger_key)
                    open_sig = make_signal_record(inst, v, best, mode, c.t)
    # cross-variant dedup: one open signal per (symbol, profile) at a time
    trades.sort(key=lambda t: (t["symbol"], t["profile"], t["t_open"], -t["score"]))
    out: list[dict] = []
    busy: dict[tuple, int] = {}
    for t in trades:
        k = (t["symbol"], t["profile"])
        if t["t_open"] < busy.get(k, 0):
            continue
        busy[k] = t["t_close"]
        out.append(t)
    return out


def print_bt_report(trades: list[dict], mode: str, days: int) -> None:
    print(f"\n=== BACKTEST {days}d | MTF_MODE={mode} ===")
    print(HEADER)
    print(_row("ALL", bt_stats(trades)))
    for title, key in (("symbol", "symbol"), ("profile", "profile"), ("direction", "direction"), ("alignment", "aligned")):
        print(f"--- by {title}")
        for k in sorted({t[key] for t in trades}, key=str):
            lab = ("aligned" if k else "not aligned") if key == "aligned" else str(k)
            print(_row(lab, bt_stats([t for t in trades if t[key] == k])))


def run_backtest(args: argparse.Namespace, mtf_mode: str, directions: tuple[str, ...], variants: list[Variant]) -> int:
    days = int(args.backtest)
    client = HyperliquidClient()
    valid, _, skipped = validate_watchlist(client)
    for m in skipped:
        log.warning("SKIPPED: %s", m)
    insts = select_instruments(valid, args.symbol)
    if args.profile:
        variants = [v for v in variants if v.profile == args.profile.upper()]
    now_ms = int(time.time() * 1000)
    modes = list(VALID_MTF_MODES) if args.compare_mtf else [mtf_mode]
    data = bt_collect(client, insts, variants, days, any(m != "off" for m in modes), now_ms)
    results = {m: bt_run_mode(data, insts, variants, directions, m, days, now_ms) for m in modes}
    if not args.compare_mtf:
        print_bt_report(results[mtf_mode], mtf_mode, days)
        return 0
    for prof in sorted({v.profile for v in variants}):
        print(f"\n=== MTF MODE COMPARISON | profile {prof} | {days}d (identical data) ===")
        print(HEADER)
        for m in modes:
            print(_row(m, bt_stats([t for t in results[m] if t["profile"] == prof])))
    return 0


# =============================================================================
# SELF-TEST
# =============================================================================


def _mk(rows: list[tuple[float, float, float, float]]) -> list[Candle]:
    return [Candle(i * 900_000, o, h, l, c, 1.0, i * 900_000 + 899_999) for i, (o, h, l, c) in enumerate(rows)]


def _inv(cs: list[Candle]) -> list[Candle]:
    return flip(cs, -1)


def self_test() -> int:
    assert abs(W_NON_MTF_TOTAL + W_MTF_TOTAL - 100.0) < 1e-9, "score weights must sum to 100"
    assert abs(W_MTF_TOTAL - 25.0) < 1e-9
    assert normalize_score(75, "off") == 100.0 and normalize_score(0, "off") == 0.0 and abs(normalize_score(37.5, "off") - 50.0) < 1e-9
    assert normalize_score(100, "bonus") == 100.0 and normalize_score(130, "gate") == 100.0
    assert len(WATCHLIST) == 10

    # swings (identical for both directions after inversion)
    sw_rows = [(10, 11, 9, 10), (10, 13, 10, 12), (12, 12, 8, 9), (9, 11, 9, 10)]
    cs = _mk(sw_rows)
    sw = find_swings(cs)
    assert [(s.idx, s.kind) for s in sw] == [(1, "H"), (2, "L")], sw
    swi = find_swings(_inv(cs))
    assert [(s.idx, s.kind) for s in swi] == [(1, "L"), (2, "H")], swi

    # trend
    up = _mk([(10, 11, 9, 10.5), (10.5, 12, 10.4, 11.5), (11.5, 11.8, 10.0, 11), (11, 13, 10.8, 12.8), (12.8, 13.1, 11.5, 12),
              (12, 12.5, 11.4, 12.4), (12.4, 14.5, 12.3, 14.2), (14.2, 14.3, 13, 13.4), (13.4, 13.9, 12.9, 13.7)])
    assert trend_of(up) == "uptrend" and trend_ok(up, "LONG") and not trend_ok(up, "SHORT")
    assert trend_of(_inv(up)) == "downtrend" and trend_ok(_inv(up), "SHORT") and not trend_ok(_inv(up), "LONG")

    # FVG, both directions
    bull = _mk([(10, 11, 9, 10.5), (10.5, 14, 10.5, 13.5), (13.5, 15, 12, 14.5)])
    f = detect_fvgs(bull, "LONG")
    assert [(a.lo, a.hi) for a in f] == [(11, 12)], f
    fs = detect_fvgs(_inv(bull), "SHORT")
    assert [(a.lo, a.hi) for a in fs] == [(-12, -11)], fs
    bear = _mk([(20, 21, 19, 19.5), (19.5, 19.5, 16, 16.5), (16.5, 17, 15, 15.5)])
    assert [(a.lo, a.hi) for a in detect_fvgs(bear, "SHORT")] == [(17, 19)]
    assert detect_fvgs(bear, "LONG") == []

    # order block, both directions
    obs_rows = [(10, 10.5, 9.5, 10.2), (10.2, 12, 10, 11.8), (11.8, 11.9, 10.8, 11), (11, 11.2, 9.5, 9.8), (9.8, 10.5, 9.0, 10.3),
                (10.3, 11.5, 10.1, 11.3), (11.3, 13, 11.2, 12.8), (12.8, 13.2, 12.5, 13)]
    ob_cs = _mk(obs_rows)
    ob = detect_order_blocks(ob_cs, "LONG")
    assert len(ob) == 1 and (ob[0].lo, ob[0].hi) == (9.5, 11.9) and ob[0].valid, ob
    obs = detect_order_blocks(_inv(ob_cs), "SHORT")
    assert len(obs) == 1 and (obs[0].lo, obs[0].hi) == (-11.9, -9.5) and obs[0].valid, obs
    assert detect_order_blocks(_inv(ob_cs), "LONG") == []

    # breaker, both directions (failed bearish OB -> bullish breaker; mirror -> bearish breaker)
    brk_rows = [(20, 20.5, 19.5, 19.8), (19.8, 20, 18, 18.2), (18.2, 19.0, 18.1, 18.9), (18.9, 20.0, 18.8, 19.8), (19.8, 20.8, 19.7, 20.6),
                (20.6, 20.7, 19.0, 19.2), (19.2, 19.3, 17.5, 17.7), (17.7, 18.5, 17.6, 18.4), (18.4, 19.5, 18.3, 19.4),
                (19.4, 21.5, 19.3, 21.2), (21.2, 22, 21, 21.8)]
    bk_cs = _mk(brk_rows)
    bk = detect_breakers(bk_cs, "LONG")
    assert len(bk) == 1 and (bk[0].lo, bk[0].hi) == (18.1, 20.8) and bk[0].valid, bk
    bks = detect_breakers(_inv(bk_cs), "SHORT")
    assert len(bks) == 1 and (bks[0].lo, bks[0].hi) == (-20.8, -18.1) and bks[0].valid, bks
    assert detect_breakers(_inv(bk_cs), "LONG") == []

    # MSB uses CLOSES, never wicks
    wick = _mk([(10, 11, 9, 10), (10, 13, 10, 12), (12, 12.5, 11, 11.5), (11.5, 11.8, 10.5, 11), (11, 14, 10.9, 12.5), (12.5, 12.6, 12, 12.2)])
    assert find_up_breaks(wick, find_swings(wick)) == [], "a wick above a swing high must not count as MSB"

    # SHORT result must be the mirror of LONG on a price-inverted series (full pipeline on synthetic data)
    v = ALL_VARIANTS["MID"]
    assert analyze_setup("LONG", v, up[:5], None, up, 13.0, "off").status in ("NO_SIGNAL", "WATCHING")  # short history path is safe
    # ---- learning layer (synthetic) ----
    import random as _r
    global LEARN_MODE, LEARN_WEIGHTS, LEARN_GUARDS
    saved = (LEARN_MODE, LEARN_WEIGHTS, LEARN_GUARDS)
    LEARN_MODE, LEARN_WEIGHTS, LEARN_GUARDS = "on", "shadow", "on"
    try:
        assert abs(sum(BASE_WEIGHTS.values()) - 100.0) < 1e-9
        m0 = _rescale({k: 1.3 for k in LEARNABLE})
        assert abs(sum(BASE_WEIGHTS[k] * m0[k] for k in LEARNABLE) - sum(BASE_WEIGHTS[k] for k in LEARNABLE)) < 0.2 and all(LEARN_MULT_MIN <= x <= LEARN_MULT_MAX for x in m0.values())
        assert auc([1, 2, 3, 4], [0, 0, 1, 1]) == 1.0 and auc([1, 1], [0, 1]) == 0.5 and auc([1, 2], [1, 1]) is None
        rng = _r.Random(7)
        now = 10 * DAY_MS
        st = {"learn": copy.deepcopy(DEFAULT_LEARN)}

        def add(n: int, sym: str = "BTCUSDT", var: str = "MID", ote_edge: bool = True, base_wr: float = 0.5) -> None:
            for i in range(n):
                c = {k: rng.randint(0, 1) for k in LEARNABLE}
                p = base_wr + (0.25 if (ote_edge and c["ote"]) else -0.15 if ote_edge else 0.0)  # OTE truly predictive
                win = 1 if rng.random() < p else 0
                st["learn"]["log"].append({"id": f"s{len(st['learn']['log'])}", "t": now, "sym": sym, "var": var, "dir": "LONG", "mode": "bonus", "score": 75.0,
                                           "alt": None, "ver": 0, "c": c, "win": win, "r": 1.5 if win else -1.0, "wt": "TP2" if win else "SL"})
                st["learn"]["resolved_total"] += 1
        add(120)
        ch1 = learning_update(st, now)  # first pass: only records pending direction
        add(10)
        ch2 = learning_update(st, now)  # second consecutive pass in the same direction: bounded step applied
        add(10)
        learning_update(st, now)
        mu = st["learn"]["weights"]["mult"]
        assert mu and mu["ote"] > 1.0 and mu["ote"] <= 1.0 + 2 * LEARN_STEP + 1e-6, mu  # learned the real edge, in small steps
        assert all(LEARN_MULT_MIN <= v <= LEARN_MULT_MAX for v in mu.values())
        assert abs(sum(BASE_WEIGHTS[k] * mu[k] for k in LEARNABLE) - sum(BASE_WEIGHTS[k] for k in LEARNABLE)) < 0.5  # budget preserved
        # scores with learned weights stay in 0-100 and 'off' normalisation uses the learned non-MTF total
        wv = eff_weights(mu)
        assert 0.0 <= normalize_score(sum(wv.values()), "bonus", wv) <= 100.0 and abs(normalize_score(sum(v for k, v in wv.items() if k not in MTF_KEYS), "off", wv) - 100.0) < 1e-9
        # per-variant min score rises on negative expectancy, never below MIN_SCORE, bounded above
        st2 = {"learn": copy.deepcopy(DEFAULT_LEARN)}
        st = st2
        add(40, var="INTRA-15m", ote_edge=False, base_wr=0.38)  # mildly negative expectancy (below the breaker)
        for _ in range(12):
            st["learn"]["resolved_total"] += LEARN_UPDATE_EVERY
            learning_update(st, now)
        ms = st["learn"]["min_score"].get("INTRA-15m")
        assert ms is not None and MIN_SCORE < ms <= MIN_SCORE + LEARN_MIN_SCORE_MAX_RAISE, ms
        # throttle: only after a full trailing window of bad trades, and it expires
        st = {"learn": copy.deepcopy(DEFAULT_LEARN)}
        add(14, sym="NEARUSDT", ote_edge=False, base_wr=0.0)
        assert not _update_throttles(st["learn"], now)
        add(1, sym="NEARUSDT", ote_edge=False, base_wr=0.0)
        assert _update_throttles(st["learn"], now) and "NEARUSDT" in st["learn"]["throttle"]
        learning_update(st, now + (LEARN_THROTTLE_DAYS + 1) * DAY_MS)
        assert "NEARUSDT" not in st["learn"]["throttle"], "throttle must auto-expire"
        # breaker trips, freezes learning, auto-recovers, and cannot instantly re-trip
        st = {"learn": copy.deepcopy(DEFAULT_LEARN)}
        add(35, ote_edge=False, base_wr=0.0)
        msgs = learning_update(st, now)
        assert any("CIRCUIT BREAKER" in x for x in msgs) and breaker_active(st, now + 1)
        apply_learning_to_runtime(st, now + 1)
        assert SCORE_BUMP == LEARN_BREAKER_SCORE_BUMP and not WEIGHT_MULT
        later = now + (LEARN_BREAKER_HOURS + 1) * 3_600_000
        assert not breaker_active(st, later)
        learning_update(st, later)
        assert not breaker_active(st, later + 1), "no re-trip before fresh trades arrive"
        apply_learning_to_runtime(st, later)
        assert SCORE_BUMP == 0.0 and not THROTTLED
        # learning must never alter signal outcome logic
        sg = {"direction": "LONG", "status": "ACTIVE", "entry": 10.0, "sl": 9.0, "tp1": 11.5, "tp2": 13.0, "rr_tp1": 1.5, "rr_tp2": 3.0,
              "tp1_hit": False, "bars_waited": 0, "filled_ms": 1, "closed_ms": None, "result_r": None}
        step_signal(sg, Candle(1, 10.0, 11.6, 10.0, 11.5, 1.0, 2))  # TP1 touched
        assert sg["status"] == "TP1_HIT" and sg["tp1_hit"]
        step_signal(sg, Candle(2, 11.0, 11.0, 8.9, 9.0, 1.0, 3))  # then SL: still a WIN at TP1's R
        assert sg["status"] == "WIN" and sg["result_r"] == 1.5 and sg["win_type"] == "TP1_THEN_SL"
        sl2 = {**sg, "status": "ACTIVE", "tp1_hit": False, "win_type": None, "result_r": None}
        step_signal(sl2, Candle(3, 10.0, 10.5, 8.9, 9.0, 1.0, 4))
        assert sl2["status"] == "LOSS" and sl2["result_r"] == -1.0 and sl2["win_type"] == "SL"
    finally:
        LEARN_MODE, LEARN_WEIGHTS, LEARN_GUARDS = saved
        apply_learning_to_runtime({"learn": copy.deepcopy(DEFAULT_LEARN)}, 0)
    print("SELF-TEST OK: swings, trend, FVG, OB, breaker (LONG+SHORT mirror), MSB-by-close, score weights == 100, off-mode normalization, "
          "learning (bounded weights, min-score, throttle expiry, breaker recovery, TP1-before-SL = WIN)")
    return 0


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Top-down framework signal engine (read-only decision support)")
    p.add_argument("--dry-run", action="store_true", help="print signals; no Telegram, no state writes")
    p.add_argument("--symbol", help="limit to one symbol (BTC, ETH, GOLD, XAUUSD ...)")
    p.add_argument("--profile", help="limit to one profile (INTRA, MID, SWING)")
    p.add_argument("--explain", action="store_true", help="step-by-step trace per direction (needs --symbol)")
    p.add_argument("--mtf-mode", choices=VALID_MTF_MODES, help="override MTF_MODE for this run")
    p.add_argument("--backtest", type=int, metavar="DAYS", help="replay closed candles bar by bar")
    p.add_argument("--compare-mtf", action="store_true", help="with --backtest: run gate, bonus, off side by side")
    p.add_argument("--self-test", action="store_true", help="run built-in detector self-tests and exit")
    p.add_argument("--learn-report", action="store_true", help="print what the engine has learned (reads state.json) and exit")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.self_test:
        return self_test()
    if args.learn_report:
        st = StateStore(STATE_FILE).load()
        print("\n".join(build_learning_report(st)))
        return 0
    mode = (args.mtf_mode or MTF_MODE).lower()
    if mode not in VALID_MTF_MODES:
        raise SystemExit(f"MTF_MODE must be one of {VALID_MTF_MODES}, got {mode!r}")
    directions = parse_directions(_env_str("SIGNAL_DIRECTIONS", "LONG,SHORT"))
    variants = parse_enabled_variants(_env_str("ENABLED_VARIANTS", DEFAULT_ENABLED_VARIANTS))
    if args.backtest:
        return run_backtest(args, mode, directions, variants)
    if args.explain:
        return run_explain(args, mode, directions, variants)
    return run_scan(args, mode, directions, variants)


if __name__ == "__main__":
    sys.exit(main())
