"""
OBSIDIAN EDGE  v1.1.0  —  1D -> 4H -> 1H  INSTITUTIONAL CONFLUENCE ENGINE
"Trade the probability, not the noise."

This is a ground-up, next-generation signal engine. It shares only its
*operating model* with prior bots (Hyperliquid market data, Telegram
delivery/reactions, state.json persistence, single-scan-per-run execution
triggered by an external cron). Every piece of trading logic — market
structure, liquidity, order blocks, fair value gaps, regime detection,
confluence scoring, confidence modelling, entry selection and risk
management — has been designed from first principles for this engine.

ARCHITECTURE
------------
1D   — Macro regime & directional bias (trend quality score, not binary)
4H   — Structure, liquidity, zones (order blocks, FVGs, sweeps, premium/
        discount, dealing range) — this is where a *setup* is defined
1H   — Execution trigger (displacement, momentum confirmation, entry
        timing) — this is where a setup becomes a *signal*

DECISION PHILOSOPHY
--------------------
Price action leads. Indicators only ever confirm or veto — they never
originate a signal on their own. Every scoring component is converted
into a probability-weighted confluence score (0-100) via a logistic
aggregation of independent, decorrelated evidence streams, rather than
naive additive point-counting. Only setups whose modelled win
probability clears an adaptive bar (which tightens in poor regimes and
relaxes in favourable ones) are transmitted. The objective is expected
value per signal, not signal count.

OPERATING MODEL (kept compatible with the existing deployment)
-----------------------------------------------------------------
  * Single Python file
  * One scan per execution (no long-running loop)
  * Triggered externally every 15 minutes (cron-job.org / GitHub Actions)
  * Reads state.json at startup, writes it before exit
  * Hyperliquid `info` endpoint for OHLCV + funding + open interest
  * Telegram for delivery, message edits and emoji-reaction lifecycle
    tracking (TP1 / TP2 / SL / missed)

CHANGELOG
---------
v1.1.0 — Win-rate suppression converted from a hard veto to a confidence
         gate. A symbol/direction that trips WIN_RATE_HARD_SUPPRESS_THRESHOLD
         is no longer fully blocked; it must now clear
         WIN_RATE_SUPPRESSION_MIN_GRADE ("High Quality" by default) to still
         fire. This fixes a catch-22 in the prior hard-veto design: a fully
         suppressed pair could never generate new resolved trades (since no
         signal was ever built or recorded for it), so its win rate could
         never recover and suppression was effectively permanent. Letting
         only the best setups through keeps a trickle of fresh outcomes
         flowing into compute_win_rates so suppressed pairs can earn their
         way back via real results instead of staying frozen indefinitely.
v1.0.0 — Initial release.
"""

from __future__ import annotations

__engine_name__ = "OBSIDIAN EDGE"
__version__ = "1.1.0"
__tagline__ = "Trade the probability, not the noise."

import fcntl
import json
import math
import os
import random
import signal as os_signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import requests

# =====================================================================
# 0. ENVIRONMENT / IDENTITY / CONSTANTS
# =====================================================================

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
if not TG_BOT_TOKEN:
    raise RuntimeError("TG_BOT_TOKEN environment variable is required")
if not TG_CHAT_ID:
    raise RuntimeError("TG_CHAT_ID environment variable is required")

_SCRIPT_DIR = Path(__file__).resolve().parent
STATE_FILE = str(_SCRIPT_DIR / "obsidian_edge_state.json")
STATE_VERSION = 1
LOCK_FILE = str(_SCRIPT_DIR / "obsidian_edge.lock")

HL_INFO_URL = "https://api.hyperliquid.xyz/info"
SCAN_WORKERS = int(os.getenv("SCAN_WORKERS", "2"))
HL_TF_WORKERS = int(os.getenv("HL_TF_WORKERS", "2"))
HL_MIN_INTERVAL_S = float(os.getenv("HL_MIN_INTERVAL_S", "0.18"))
HL_MIN_INTERVAL_MAX_S = float(os.getenv("HL_MIN_INTERVAL_MAX_S", "0.60"))

N_1H, N_4H, N_1D = 200, 260, 320
INTERVAL_MS = {"1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}

# Same trading universe as the prior deployment — no quantitative
# justification was found to alter the symbol set, so it is preserved.
WATCHLIST = [
    "BTCUSDT", "ETHUSDT", "HYPEUSDT", "ZECUSDT", "NEARUSDT",
    "ONDOUSDT", "SUIUSDT", "PENGUUSDT", "BNBUSDT", "SOLUSDT",
    "TRXUSDT", "BCHUSDT", "DOGEUSDT", "ADAUSDT", "DOTUSDT",
    "TAOUSDT", "AVAXUSDT", "LINKUSDT", "AAVEUSDT", "XRPUSDT",
    "XLMUSDT", "UNIUSDT", "LTCUSDT", "APTUSDT", "PENDLEUSDT",
]
SMALL_CAP_PAIRS = {"PENGUUSDT", "HYPEUSDT", "ZECUSDT", "PENDLEUSDT"}
SPREAD_EXEMPT = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"}

EMA_FAST, EMA_MID, EMA_SLOW, EMA_TREND = 9, 21, 50, 200
RSI_LEN, ATR_LEN, ADX_LEN, ROC_LEN, BB_LEN = 14, 14, 14, 10, 20

# Indicator math values close to a strict 0-100 confidence read.
CONFIDENCE_FLOOR_STANDARD = 62.0
CONFIDENCE_FLOOR_HIGH = 72.0
CONFIDENCE_FLOOR_PREMIUM = 82.0
CONFIDENCE_FLOOR_ELITE = 90.0

MIN_RR = 1.4
PREFERRED_RR = 2.2
MAX_SIGNALS_PER_SCAN = 3
MAX_CONCURRENT_ACTIVE = 10
SIGNAL_MAX_AGE_1H_BARS = 30
POST_WIN_COOLDOWN_BARS = 1
POST_LOSS_COOLDOWN_S = 5400
BASE_COOLDOWN_BARS = 4

MIN_OI_USD = 500_000.0
MIN_OI_USD_SMALL_CAP = 250_000.0
FUNDING_HEADWIND_PCT = 0.0008
FUNDING_TAILWIND_PCT = 0.0004

ATR_PCT_FLOOR = 0.12
ATR_PCT_CEIL = 11.0
ATR_HISTORY_DEPTH = 180

OI_HISTORY_DEPTH = 30
FUNDING_HISTORY_DEPTH = 6

CORR_LOOKBACK_BARS = 42
CORR_MIN_SAMPLE = 20
CORR_CLUSTER_THRESHOLD = 0.78

WIN_RATE_MIN_SAMPLE = 20
WIN_RATE_HARD_SUPPRESS_THRESHOLD = 0.32
# Once a symbol/direction is suppressed, it is no longer hard-blocked —
# instead it must clear this grade floor to still fire. This keeps a
# trickle of only the best setups flowing for a suppressed pair so new
# resolved trades can accumulate and the win rate has a real chance to
# recover (or confirm staying low), instead of being permanently frozen
# with zero new data ever reaching compute_win_rates for that key.
WIN_RATE_SUPPRESSION_MIN_GRADE = "High Quality"
MAX_SIGNAL_HISTORY = 2000
META_CACHE_TTL_S = 50.0

REACT_TP1, REACT_TP2, REACT_SL, REACT_MISS = "🔥", "🏆", "😭", "😢"

GRADE_FLOOR = {
    "Elite": CONFIDENCE_FLOOR_ELITE,
    "Premium": CONFIDENCE_FLOOR_PREMIUM,
    "High Quality": CONFIDENCE_FLOOR_HIGH,
    "Standard": CONFIDENCE_FLOOR_STANDARD,
}
GRADE_MAX_LEVERAGE = {"Elite": 10.0, "Premium": 8.0, "High Quality": 5.0, "Standard": 3.0}
GRADE_SIZE_PCT = {"Elite": 100, "Premium": 100, "High Quality": 70, "Standard": 45}
GRADE_EMOJI = {"Elite": "💎", "Premium": "⚡", "High Quality": "🔵", "Standard": "⚪"}

_hl_session = requests.Session()
_hl_lock = threading.Lock()
_hl_last_ts = 0.0
_hl_min_interval = HL_MIN_INTERVAL_S
_hl_streak = 0
_state_lock = threading.Lock()
_shutdown = False


def _handle_shutdown(sig, frame):
    global _shutdown
    _shutdown = True
    print(f"\n[SHUTDOWN] signal {sig} received — finishing current scan…")


for _s in (os_signal.SIGINT, os_signal.SIGTERM):
    try:
        os_signal.signal(_s, _handle_shutdown)
    except Exception:
        pass


# =====================================================================
# 1. HYPERLIQUID TRANSPORT LAYER
# =====================================================================

def hl_coin(symbol: str) -> str:
    return symbol.replace("USDT", "")


def hl_post(payload: dict):
    """POST to the Hyperliquid info endpoint with adaptive rate limiting
    and exponential backoff on 429 / transient failures."""
    global _hl_last_ts, _hl_min_interval, _hl_streak
    max_attempts = int(os.getenv("HL_MAX_ATTEMPTS", "6"))
    base_sleep = float(os.getenv("HL_BASE_SLEEP_S", "0.75"))

    for attempt in range(max_attempts):
        try:
            with _hl_lock:
                gap = _hl_min_interval - (time.time() - _hl_last_ts)
                if gap > 0:
                    time.sleep(gap)
                _hl_last_ts = time.time()

            r = _hl_session.post(HL_INFO_URL, json=payload, timeout=15,
                                  headers={"Content-Type": "application/json"})

            if r.status_code == 429:
                with _hl_lock:
                    _hl_min_interval = min(HL_MIN_INTERVAL_MAX_S, _hl_min_interval * 1.25 + 0.02)
                    _hl_streak = 0
                wait = base_sleep * (2 ** attempt) + random.uniform(0.0, 0.4)
                time.sleep(min(20.0, wait))
                continue

            r.raise_for_status()
            with _hl_lock:
                _hl_streak += 1
                if _hl_streak >= 10:
                    _hl_min_interval, _hl_streak = HL_MIN_INTERVAL_S, 0
                else:
                    _hl_min_interval = max(HL_MIN_INTERVAL_S, _hl_min_interval - 0.0025)
            return r.json()
        except Exception:
            if attempt == max_attempts - 1:
                raise
            time.sleep(min(20.0, base_sleep * (2 ** attempt)) + random.uniform(0.0, 0.3))
    raise RuntimeError("hl_post exhausted retries")


def _bar_open(ref_ms: int, interval: str) -> int:
    iv = INTERVAL_MS[interval]
    return (ref_ms // iv) * iv


def get_candles(symbol: str, interval: str, n: int, reference_ms: int | None = None) -> list[dict]:
    coin = hl_coin(symbol)
    iv = INTERVAL_MS[interval]
    ref = reference_ms if reference_ms is not None else int(time.time() * 1000)
    end_ms = _bar_open(ref, interval)
    start_ms = end_ms - iv * (n + 10)

    raw = hl_post({"type": "candleSnapshot",
                    "req": {"coin": coin, "interval": interval, "startTime": start_ms, "endTime": end_ms}})
    if not raw:
        return []
    out = []
    for c in raw:
        if int(c["t"]) >= end_ms:
            continue
        out.append({"t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]),
                     "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"])})
    return out[-n:]


def fetch_all_candles(symbol: str, reference_ms: int | None = None) -> tuple | None:
    out: dict[str, list] = {}
    with ThreadPoolExecutor(max_workers=max(1, HL_TF_WORKERS)) as ex:
        futs = {ex.submit(get_candles, symbol, tf, n, reference_ms): tf
                for tf, n in (("1h", N_1H), ("4h", N_4H), ("1d", N_1D))}
        for fut in as_completed(futs):
            tf = futs[fut]
            try:
                out[tf] = fut.result()
            except Exception as e:
                print(f"  [CANDLES] {symbol} {tf} failed: {e}")
                return None
    if len(out.get("1h", [])) < 60 or len(out.get("4h", [])) < 60 or len(out.get("1d", [])) < 60:
        return None
    return out["1h"], out["4h"], out["1d"]


_meta_cache, _meta_ts = None, 0.0
_meta_lock = threading.Lock()


def get_meta_and_ctx() -> dict | None:
    global _meta_cache, _meta_ts
    with _meta_lock:
        if _meta_cache is not None and time.time() - _meta_ts < META_CACHE_TTL_S:
            return _meta_cache
    try:
        data = hl_post({"type": "metaAndAssetCtxs"})
        if data is None:
            return _meta_cache
        universe, ctxs = data[0].get("universe", []), data[1]
        cache = {}
        for i, a in enumerate(universe):
            name = a.get("name", "")
            if not name:
                continue
            ctx = ctxs[i]
            cache[name] = {
                "funding": float(ctx["funding"]) if ctx.get("funding") is not None else None,
                "oi_coins": float(ctx["openInterest"]) if ctx.get("openInterest") is not None else None,
                "mark": float(ctx["markPx"]) if ctx.get("markPx") is not None else None,
            }
        with _meta_lock:
            _meta_cache, _meta_ts = cache, time.time()
        return cache
    except Exception as e:
        print(f"  [META] fetch failed: {e}")
        return _meta_cache


def get_market_ctx(symbol: str) -> dict | None:
    cache = get_meta_and_ctx()
    if not cache or hl_coin(symbol) not in cache:
        return None
    e = cache[hl_coin(symbol)]
    oi_usd = e["oi_coins"] * e["mark"] if e.get("oi_coins") is not None and e.get("mark") is not None else None
    return {"funding": e.get("funding"), "oi_usd": oi_usd, "mark": e.get("mark")}


def get_mid_price(symbol: str) -> float | None:
    cache = get_meta_and_ctx()
    if cache and hl_coin(symbol) in cache:
        return cache[hl_coin(symbol)].get("mark")
    return None


# =====================================================================
# 2. CORE MATH / INDICATOR PRIMITIVES
# =====================================================================

def safe(v, fb=0.0):
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return fb
        return float(v)
    except (TypeError, ValueError):
        return fb


def ema(vals: list[float], period: int) -> list[float]:
    if len(vals) < period:
        return [float("nan")] * len(vals)
    k = 2.0 / (period + 1)
    out = [float("nan")] * len(vals)
    out[period - 1] = sum(vals[:period]) / period
    for i in range(period, len(vals)):
        out[i] = vals[i] * k + out[i - 1] * (1 - k)
    return out


def sma(vals: list[float], period: int) -> list[float]:
    out = [float("nan")] * len(vals)
    for i in range(period - 1, len(vals)):
        out[i] = sum(vals[i - period + 1:i + 1]) / period
    return out


def stdev(vals: list[float], period: int) -> list[float]:
    out = [float("nan")] * len(vals)
    for i in range(period - 1, len(vals)):
        window = vals[i - period + 1:i + 1]
        m = sum(window) / period
        out[i] = math.sqrt(sum((x - m) ** 2 for x in window) / period)
    return out


def rsi(closes: list[float], period: int) -> list[float]:
    n = len(closes)
    out = [float("nan")] * n
    if n < period + 1:
        return out
    gains = losses = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    avg_g, avg_l = gains / period, losses / period
    out[period] = 100.0 if avg_l == 0 else 100.0 - 100.0 / (1.0 + avg_g / avg_l)
    for i in range(period + 1, n):
        d = closes[i] - closes[i - 1]
        g, l = max(d, 0.0), max(-d, 0.0)
        avg_g = (avg_g * (period - 1) + g) / period
        avg_l = (avg_l * (period - 1) + l) / period
        out[i] = 100.0 if avg_l == 0 else 100.0 - 100.0 / (1.0 + avg_g / avg_l)
    return out


def atr(highs, lows, closes, period: int) -> list[float]:
    n = len(closes)
    trs = [float("nan")] * n
    for i in range(1, n):
        trs[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
    out = [float("nan")] * n
    if n <= period:
        return out
    out[period] = sum(trs[1:period + 1]) / period
    for i in range(period + 1, n):
        out[i] = (out[i - 1] * (period - 1) + trs[i]) / period
    return out


def adx_dmi(highs, lows, closes, period: int):
    n = len(closes)
    plus_dm = [0.0] * n
    minus_dm = [0.0] * n
    trs = [0.0] * n
    for i in range(1, n):
        up, down = highs[i] - highs[i - 1], lows[i - 1] - lows[i]
        plus_dm[i] = up if (up > down and up > 0) else 0.0
        minus_dm[i] = down if (down > up and down > 0) else 0.0
        trs[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))

    def wilder_smooth(series):
        out = [float("nan")] * n
        if n <= period:
            return out
        out[period] = sum(series[1:period + 1])
        for i in range(period + 1, n):
            out[i] = out[i - 1] - out[i - 1] / period + series[i]
        return out

    str_, spd, smd = wilder_smooth(trs), wilder_smooth(plus_dm), wilder_smooth(minus_dm)
    plus_di = [float("nan")] * n
    minus_di = [float("nan")] * n
    dx = [float("nan")] * n
    for i in range(period, n):
        if str_[i] and str_[i] > 0:
            plus_di[i] = 100.0 * spd[i] / str_[i]
            minus_di[i] = 100.0 * smd[i] / str_[i]
            s = plus_di[i] + minus_di[i]
            dx[i] = 100.0 * abs(plus_di[i] - minus_di[i]) / s if s > 0 else 0.0
    adx = [float("nan")] * n
    valid = [x for x in dx if not math.isnan(x)]
    if len(valid) >= period:
        start = next(i for i in range(n) if not math.isnan(dx[i]))
        adx[start + period - 1] = sum(dx[start:start + period]) / period
        for i in range(start + period, n):
            adx[i] = (adx[i - 1] * (period - 1) + dx[i]) / period
    return adx, plus_di, minus_di


def roc(closes: list[float], period: int) -> list[float]:
    n = len(closes)
    out = [float("nan")] * n
    for i in range(period, n):
        if closes[i - period] != 0:
            out[i] = (closes[i] - closes[i - period]) / closes[i - period] * 100.0
    return out


def bollinger_width_pct(closes: list[float], period: int) -> list[float]:
    mid = sma(closes, period)
    sd = stdev(closes, period)
    out = [float("nan")] * len(closes)
    for i in range(len(closes)):
        if not math.isnan(mid[i]) and mid[i] != 0 and not math.isnan(sd[i]):
            out[i] = (4.0 * sd[i]) / mid[i] * 100.0
    return out


_ind_cache: dict[str, dict] = {}
_ind_cache_lock = threading.Lock()


def compute_indicators(candles: list[dict]) -> dict:
    h = [c["h"] for c in candles]
    l = [c["l"] for c in candles]
    c = [c["c"] for c in candles]
    v = [c_["v"] for c_ in candles]
    adx, pdi, mdi = adx_dmi(h, l, c, ADX_LEN)
    return {
        "h": h, "l": l, "c": c, "v": v,
        "ema9": ema(c, EMA_FAST), "ema21": ema(c, EMA_MID),
        "ema50": ema(c, EMA_SLOW), "ema200": ema(c, EMA_TREND),
        "rsi": rsi(c, RSI_LEN), "atr": atr(h, l, c, ATR_LEN),
        "adx": adx, "plus_di": pdi, "minus_di": mdi,
        "roc": roc(c, ROC_LEN), "bbw": bollinger_width_pct(c, BB_LEN),
        "vol_sma": sma(v, 20),
    }


def cached_indicators(key: str, candles: list[dict]) -> dict:
    with _ind_cache_lock:
        if key in _ind_cache:
            return _ind_cache[key]
    ind = compute_indicators(candles)
    with _ind_cache_lock:
        _ind_cache[key] = ind
    return ind


def clear_indicator_cache():
    with _ind_cache_lock:
        _ind_cache.clear()


# =====================================================================
# 3. STATE PERSISTENCE
# =====================================================================

def load_state() -> dict:
    fresh = {
        "_version": STATE_VERSION, "oi_history": {}, "funding_history": {},
        "atr_history": {}, "signal_history": [], "signal_cooldowns": {},
        "post_loss_cooldown": {}, "last_signal_outcome": {}, "active_signals": [],
        "macro_calendar_cache": {},
    }
    for p in (STATE_FILE, STATE_FILE + ".bak"):
        if Path(p).exists():
            try:
                s = json.loads(Path(p).read_text())
                if s.get("_version", 0) != STATE_VERSION:
                    print(f"[STATE] version mismatch in {p} — starting fresh")
                    continue
                for k, v in fresh.items():
                    s.setdefault(k, v)
                return s
            except Exception as e:
                print(f"[STATE] failed to load {p}: {e}")
    print("[STATE] starting fresh")
    return fresh


def save_state(state: dict):
    with _state_lock:
        data = deepcopy(state)
    path, bak = Path(STATE_FILE), Path(STATE_FILE + ".bak")
    try:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, default=str, indent=2))
        if path.exists():
            path.replace(bak)
        tmp.replace(path)
    except Exception as e:
        print(f"[STATE] save failed: {e}")


def update_history(state: dict, bucket: str, symbol: str, value: float, depth: int):
    with _state_lock:
        h = state.setdefault(bucket, {}).setdefault(symbol, [])
        h.append(value)
        if len(h) > depth:
            del h[: len(h) - depth]


def percentile_rank(history: list[float], value: float) -> float | None:
    if len(history) < 10:
        return None
    below = sum(1 for x in history if x <= value)
    return below / len(history)


def record_signal_history(state: dict, symbol: str, direction: str, setup_type: str,
                           confidence: float, grade: str, funding: float | None,
                           atr_pct: float, sent: bool) -> str:
    hist_id = f"{symbol}_{direction}_{int(time.time() * 1000)}_{random.randint(100, 999)}"
    with _state_lock:
        hist = state.setdefault("signal_history", [])
        hist.append({
            "id": hist_id, "symbol": symbol, "direction": direction, "setup_type": setup_type,
            "confidence": confidence, "grade": grade, "funding": funding, "atr_pct": atr_pct,
            "sent": sent, "result": "pending", "ts": int(time.time()),
        })
        if len(hist) > MAX_SIGNAL_HISTORY:
            del hist[: len(hist) - MAX_SIGNAL_HISTORY]
    return hist_id


def update_signal_result(state: dict, hist_id: str, result: str):
    with _state_lock:
        for rec in state.get("signal_history", []):
            if rec.get("id") == hist_id:
                rec["result"] = result
                rec["resolved_ts"] = int(time.time())
                return


_wr_cache: dict | None = None


def reset_win_rate_cache():
    global _wr_cache
    _wr_cache = None


def compute_win_rates(state: dict) -> dict:
    global _wr_cache
    if _wr_cache is not None:
        return _wr_cache
    out: dict[str, dict] = {}
    by_key: dict[str, list[str]] = {}
    for rec in state.get("signal_history", []):
        if rec.get("result") in ("pending",):
            continue
        key = f"{rec['symbol']}_{rec['direction']}"
        by_key.setdefault(key, []).append(rec["result"])
    for key, results in by_key.items():
        wins = sum(1 for r in results if r in ("tp1", "tp2"))
        losses = sum(1 for r in results if r == "sl")
        n = wins + losses
        out[key] = {"win_rate": (wins / n) if n > 0 else None, "n": n}
    _wr_cache = out
    return out


# =====================================================================
# 4. MARKET STRUCTURE  —  SWING PIVOTS, BOS / CHoCH
# =====================================================================

def find_swing_pivots(candles: list[dict], left: int = 2, right: int = 2) -> tuple[list[int], list[int]]:
    """Returns indices of confirmed swing-high and swing-low candles."""
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
    n = len(candles)
    sh, sl = [], []
    for i in range(left, n - right):
        window_h = highs[i - left:i + right + 1]
        window_l = lows[i - left:i + right + 1]
        if highs[i] == max(window_h) and window_h.count(highs[i]) == 1:
            sh.append(i)
        if lows[i] == min(window_l) and window_l.count(lows[i]) == 1:
            sl.append(i)
    return sh, sl


@dataclass
class StructureState:
    bias: str               # "bull" | "bear" | "neutral"
    last_bos_idx: int | None
    last_choch_idx: int | None
    swing_highs: list[int]
    swing_lows: list[int]
    last_major_high: float | None
    last_major_low: float | None
    structure_quality: float  # 0-1, how cleanly stepped the structure is


def analyze_market_structure(candles: list[dict]) -> StructureState:
    """Classifies HH/HL/LH/LL sequencing and detects the most recent
    Break of Structure (trend continuation) vs Change of Character
    (trend reversal) using confirmed swing pivots only — no repaint."""
    sh, sl = find_swing_pivots(candles, left=2, right=2)
    closes = [c["c"] for c in candles]

    if len(sh) < 2 or len(sl) < 2:
        return StructureState("neutral", None, None, sh, sl, None, None, 0.0)

    highs_vals = [(i, candles[i]["h"]) for i in sh]
    lows_vals = [(i, candles[i]["l"]) for i in sl]

    bias = "neutral"
    last_bos, last_choch = None, None
    quality_hits, quality_total = 0, 0

    # Walk the merged pivot sequence chronologically to track HH/HL/LH/LL
    merged = sorted([(i, "H", p) for i, p in highs_vals] + [(i, "L", p) for i, p in lows_vals])
    prev_h = prev_l = None
    trend = "neutral"
    for i, kind, price in merged:
        if kind == "H":
            if prev_h is not None:
                quality_total += 1
                if price > prev_h:           # Higher High
                    if trend == "bear":
                        last_choch = i
                        trend = "bull"
                    elif trend == "bull":
                        last_bos = i
                    else:
                        trend = "bull"
                    quality_hits += 1
                elif price < prev_h:         # Lower High
                    if trend == "bull":
                        last_choch = i
                        trend = "bear"
                    elif trend == "bear":
                        last_bos = i
                    else:
                        trend = "bear"
                    quality_hits += 1
            prev_h = price
        else:
            if prev_l is not None:
                quality_total += 1
                if price < prev_l:           # Lower Low
                    if trend == "bull":
                        last_choch = i
                        trend = "bear"
                    elif trend == "bear":
                        last_bos = i
                    else:
                        trend = "bear"
                    quality_hits += 1
                elif price > prev_l:         # Higher Low
                    if trend == "bear":
                        last_choch = i
                        trend = "bull"
                    elif trend == "bull":
                        last_bos = i
                    else:
                        trend = "bull"
                    quality_hits += 1
            prev_l = price

    bias = trend if trend != "neutral" else "neutral"
    quality = (quality_hits / quality_total) if quality_total else 0.0

    last_major_high = highs_vals[-1][1] if highs_vals else None
    last_major_low = lows_vals[-1][1] if lows_vals else None

    return StructureState(bias, last_bos, last_choch, sh, sl, last_major_high, last_major_low, quality)


# =====================================================================
# 5. LIQUIDITY ENGINE  —  POOLS, SWEEPS, EQUAL HIGHS/LOWS
# =====================================================================

@dataclass
class LiquidityRead:
    swept_high: float | None
    swept_low: float | None
    sweep_reclaimed: bool
    equal_highs: float | None
    equal_lows: float | None
    nearest_pool_above: float | None
    nearest_pool_below: float | None


def detect_liquidity(candles: list[dict], struct: StructureState, lookback: int = 14) -> LiquidityRead:
    """Detects liquidity sweeps (a wick piercing a prior swing extreme
    that is then reclaimed within the same/next bar — the classic
    institutional stop-hunt signature) and equal-high/low liquidity
    pools (resting stops the market is statistically drawn toward)."""
    recent = candles[-lookback:]
    closes = [c["c"] for c in candles]

    swept_high = swept_low = None
    reclaimed = False
    if struct.swing_highs:
        ref_high = candles[struct.swing_highs[-1]]["h"]
        for bar in recent[-6:]:
            if bar["h"] > ref_high and bar["c"] < ref_high:
                swept_high, reclaimed = ref_high, True
                break
            if bar["h"] > ref_high and bar["c"] >= ref_high:
                swept_high = ref_high
    if struct.swing_lows:
        ref_low = candles[struct.swing_lows[-1]]["l"]
        for bar in recent[-6:]:
            if bar["l"] < ref_low and bar["c"] > ref_low:
                swept_low, reclaimed = ref_low, True
                break
            if bar["l"] < ref_low and bar["c"] <= ref_low:
                swept_low = ref_low

    # Equal highs/lows: cluster of pivot extremes within 0.15% of each other.
    eq_high = eq_low = None
    tol = 0.0015
    highs = [candles[i]["h"] for i in struct.swing_highs[-6:]]
    lows = [candles[i]["l"] for i in struct.swing_lows[-6:]]
    for i in range(len(highs)):
        for j in range(i + 1, len(highs)):
            if highs[i] and abs(highs[i] - highs[j]) / highs[i] <= tol:
                eq_high = max(highs[i], highs[j])
    for i in range(len(lows)):
        for j in range(i + 1, len(lows)):
            if lows[i] and abs(lows[i] - lows[j]) / lows[i] <= tol:
                eq_low = min(lows[i], lows[j])

    cur = closes[-1]
    pools_above = [p for p in (struct.last_major_high, eq_high) if p and p > cur]
    pools_below = [p for p in (struct.last_major_low, eq_low) if p and p < cur]

    return LiquidityRead(
        swept_high, swept_low, reclaimed, eq_high, eq_low,
        min(pools_above) if pools_above else None,
        max(pools_below) if pools_below else None,
    )


# =====================================================================
# 6. ORDER BLOCKS & FAIR VALUE GAPS
# =====================================================================

@dataclass
class Zone:
    kind: str          # "OB" | "FVG"
    direction: str      # "bull" | "bear"
    top: float
    bottom: float
    idx: int
    mitigated: bool = False


def detect_order_blocks(candles: list[dict], direction: str, displacement_atr: float,
                         lookback: int = 30) -> list[Zone]:
    """An order block is the last opposing candle immediately preceding
    a displacement move (a body >= 1.2x ATR) that breaks structure. This
    marks the institutional footprint left before price was driven away."""
    h = [c["h"] for c in candles]
    l = [c["l"] for c in candles]
    o = [c["o"] for c in candles]
    c = [c["c"] for c in candles]
    n = len(candles)
    zones: list[Zone] = []
    start = max(2, n - lookback)
    for i in range(start, n - 1):
        body = abs(c[i] - o[i])
        if body < displacement_atr * 1.2:
            continue
        bullish_disp = c[i] > o[i]
        if direction == "long" and bullish_disp:
            j = i - 1
            while j >= max(0, i - 3) and c[j] >= o[j]:
                j -= 1
            if j >= 0:
                zones.append(Zone("OB", "bull", max(o[j], c[j]), l[j], j))
        elif direction == "short" and not bullish_disp:
            j = i - 1
            while j >= max(0, i - 3) and c[j] <= o[j]:
                j -= 1
            if j >= 0:
                zones.append(Zone("OB", "bear", h[j], min(o[j], c[j]), j))
    return zones[-3:]


def detect_fvgs(candles: list[dict], direction: str, lookback: int = 30) -> list[Zone]:
    """A fair value gap is a 3-candle imbalance — candle 1's high/low
    does not overlap candle 3's low/high — representing inefficient,
    unmitigated price delivery that price is statistically drawn back to."""
    h = [c["h"] for c in candles]
    l = [c["l"] for c in candles]
    n = len(candles)
    zones: list[Zone] = []
    start = max(2, n - lookback)
    for i in range(start, n):
        if i < 2:
            continue
        if direction == "long" and l[i] > h[i - 2]:
            zones.append(Zone("FVG", "bull", l[i], h[i - 2], i - 1))
        elif direction == "short" and h[i] < l[i - 2]:
            zones.append(Zone("FVG", "bear", l[i - 2], h[i], i - 1))
    return zones[-3:]


def mark_mitigation(zones: list[Zone], candles: list[dict]) -> list[Zone]:
    for z in zones:
        for bar in candles[z.idx + 1:]:
            if bar["l"] <= z.top and bar["h"] >= z.bottom:
                z.mitigated = True
                break
    return zones


def price_in_zone(price: float, zone: Zone, tolerance_pct: float = 0.0015) -> bool:
    lo, hi = min(zone.top, zone.bottom), max(zone.top, zone.bottom)
    pad = (hi - lo) * tolerance_pct * 10 if hi > lo else price * tolerance_pct
    return (lo - pad) <= price <= (hi + pad)


# =====================================================================
# 7. PREMIUM / DISCOUNT DEALING RANGE
# =====================================================================

def dealing_range_position(candles: list[dict], struct: StructureState, lookback: int = 40) -> dict:
    """Determines where current price sits within the most recent
    significant dealing range (last major swing high/low). Below 0.5
    = discount (favourable for longs), above 0.5 = premium (favourable
    for shorts), per ICT/SMC array theory."""
    window = candles[-lookback:]
    hi = max(b["h"] for b in window)
    lo = min(b["l"] for b in window)
    cur = candles[-1]["c"]
    if hi == lo:
        return {"position": 0.5, "zone": "equilibrium", "range_high": hi, "range_low": lo}
    pos = (cur - lo) / (hi - lo)
    if pos <= 0.30:
        zone = "deep_discount"
    elif pos <= 0.47:
        zone = "discount"
    elif pos <= 0.53:
        zone = "equilibrium"
    elif pos <= 0.70:
        zone = "premium"
    else:
        zone = "deep_premium"
    return {"position": pos, "zone": zone, "range_high": hi, "range_low": lo}


# =====================================================================
# 8. VOLATILITY & REGIME ENGINE
# =====================================================================

def volatility_regime(state: dict, symbol: str, atr_pct: float, bbw_now: float, bbw_hist: list[float]) -> dict:
    """Classifies the current volatility regime adaptively against the
    symbol's own ATR-percent history, and separately flags volatility
    *contraction* (a tightening Bollinger width — energy build-up that
    frequently precedes expansion / breakout)."""
    hist = state.get("atr_history", {}).get(symbol, [])
    pct = percentile_rank(hist, atr_pct)
    if pct is None:
        regime = "unknown"
    elif pct < 0.25:
        regime = "low"
    elif pct < 0.65:
        regime = "normal"
    elif pct < 0.85:
        regime = "elevated"
    else:
        regime = "extreme"

    contraction = False
    valid_bbw = [x for x in bbw_hist[-20:] if not math.isnan(x)]
    if len(valid_bbw) >= 10 and not math.isnan(bbw_now):
        recent_min = min(valid_bbw[-10:])
        contraction = bbw_now <= recent_min * 1.15 and bbw_now < (sum(valid_bbw) / len(valid_bbw)) * 0.75

    return {"regime": regime, "percentile": pct, "contraction": contraction}


# =====================================================================
# 9. MOMENTUM ENGINE
# =====================================================================

def momentum_read(ind: dict, direction: str) -> dict:
    """Confirms (never originates) directional moves using RSI
    positioning, rate-of-change, and a swing-based divergence check
    between price and RSI on the same timeframe."""
    rsi_now = safe(ind["rsi"][-1], 50.0)
    roc_now = safe(ind["roc"][-1], 0.0)

    aligned = (direction == "long" and rsi_now > 50.0 and roc_now > 0) or \
              (direction == "short" and rsi_now < 50.0 and roc_now < 0)
    overheated = (direction == "long" and rsi_now > 78.0) or (direction == "short" and rsi_now < 22.0)

    closes, rsis = ind["c"], ind["rsi"]
    div = False
    if len(closes) > 20:
        c1, c2 = closes[-1], closes[-10]
        r1, r2 = safe(rsis[-1]), safe(rsis[-10])
        if direction == "long" and c1 > c2 and r1 < r2 - 4:
            div = True
        elif direction == "short" and c1 < c2 and r1 > r2 + 4:
            div = True

    return {"rsi": rsi_now, "roc": roc_now, "aligned": aligned, "overheated": overheated, "divergence": div}


# =====================================================================
# 10. ORDERFLOW PROXY (no L2 book available — body/volume based)
# =====================================================================

def orderflow_proxy(candles: list[dict], direction: str, lookback: int = 24) -> dict:
    """Without a live order book, intrabar buy/sell pressure is
    approximated via where each candle closes within its own range
    (close-location value), weighted by volume — a standard, widely
    validated proxy for cumulative delta. Net pressure is then compared
    against the trade direction for alignment."""
    window = candles[-lookback:]
    cvd = 0.0
    cvd_series = []
    buy_vol = sell_vol = 0.0
    for bar in window:
        rng = bar["h"] - bar["l"]
        clv = ((bar["c"] - bar["l"]) - (bar["h"] - bar["c"])) / rng if rng > 0 else 0.0
        delta = clv * bar["v"]
        cvd += delta
        cvd_series.append(cvd)
        if delta >= 0:
            buy_vol += abs(delta)
        else:
            sell_vol += abs(delta)

    total = buy_vol + sell_vol
    buy_ratio = (buy_vol / total) if total > 0 else 0.5
    cvd_slope = (cvd_series[-1] - cvd_series[-6]) if len(cvd_series) >= 6 else 0.0

    aligned = (direction == "long" and buy_ratio > 0.52 and cvd_slope > 0) or \
              (direction == "short" and buy_ratio < 0.48 and cvd_slope < 0)

    return {"buy_ratio": buy_ratio, "cvd_slope": cvd_slope, "aligned": aligned}


# =====================================================================
# 11. DAILY MACRO REGIME / TREND QUALITY (1D)
# =====================================================================

def classify_daily_regime(candles_1d: list[dict]) -> dict:
    """Produces a continuous trend-quality score (0-100) instead of a
    binary bull/bear label, blending EMA-stack alignment, EMA slope,
    ADX/DI strength, and structural bias from the 1D swing sequence."""
    ind = compute_indicators(candles_1d)
    c = ind["c"][-1]
    e9, e21, e50, e200 = (safe(ind["ema9"][-1], c), safe(ind["ema21"][-1], c),
                           safe(ind["ema50"][-1], c), safe(ind["ema200"][-1], c))
    adx_now = safe(ind["adx"][-1], 15.0)
    pdi, mdi = safe(ind["plus_di"][-1], 20.0), safe(ind["minus_di"][-1], 20.0)
    struct = analyze_market_structure(candles_1d)

    bull_stack = e9 > e21 > e50 > e200
    bear_stack = e9 < e21 < e50 < e200
    slope_50 = (e50 - safe(ind["ema50"][-6], e50)) / e50 * 100 if e50 else 0.0

    score = 50.0
    if bull_stack:
        score += 18
    elif e9 > e21 > e50:
        score += 9
    if bear_stack:
        score -= 18
    elif e9 < e21 < e50:
        score -= 9
    score += max(-12, min(12, slope_50 * 6))
    score += max(-10, min(10, (pdi - mdi) * 0.5))
    if adx_now >= 25:
        score += 8 if pdi > mdi else -8
    if struct.bias == "bull":
        score += 10 * struct.structure_quality
    elif struct.bias == "bear":
        score -= 10 * struct.structure_quality

    score = max(0.0, min(100.0, score))
    if score >= 65:
        label = "bullish"
    elif score <= 35:
        label = "bearish"
    else:
        label = "neutral"

    return {"score": score, "label": label, "adx": adx_now, "structure": struct,
            "close": c, "ema200": e200}


# =====================================================================
# 12. 4H STRUCTURE / ZONE SETUP DETECTION
# =====================================================================

@dataclass
class SetupCandidate:
    direction: str
    setup_type: str           # "SWEEP_REVERSAL" | "CONTINUATION" | "BREAKOUT"
    zones: list[Zone]
    liquidity: LiquidityRead
    struct: StructureState
    dealing: dict
    displacement: bool
    notes: list[str] = field(default_factory=list)


def detect_displacement(candles: list[dict], atr_val: float, mult: float = 1.3) -> bool:
    if len(candles) < 3 or atr_val <= 0:
        return False
    last = candles[-1]
    return abs(last["c"] - last["o"]) >= atr_val * mult


def build_4h_setup(candles_4h: list[dict], daily: dict, direction: str) -> SetupCandidate | None:
    ind = cached_indicators(f"4h_{id(candles_4h)}", candles_4h)
    atr_now = safe(ind["atr"][-1], candles_4h[-1]["c"] * 0.01)
    struct = analyze_market_structure(candles_4h)
    liq = detect_liquidity(candles_4h, struct)
    dealing = dealing_range_position(candles_4h, struct)
    displacement = detect_displacement(candles_4h, atr_now)

    obs = mark_mitigation(detect_order_blocks(candles_4h, direction, atr_now), candles_4h)
    fvgs = mark_mitigation(detect_fvgs(candles_4h, direction), candles_4h)
    unmitigated = [z for z in obs + fvgs if not z.mitigated]

    notes = []
    setup_type = None

    sweep_for_long = direction == "long" and liq.swept_low and liq.sweep_reclaimed
    sweep_for_short = direction == "short" and liq.swept_high and liq.sweep_reclaimed
    if sweep_for_long or sweep_for_short:
        setup_type = "SWEEP_REVERSAL"
        notes.append("Liquidity sweep with reclaim detected")
    elif struct.bias == ("bull" if direction == "long" else "bear") and unmitigated:
        setup_type = "CONTINUATION"
        notes.append(f"Trend continuation, {struct.bias} structure intact")
    elif displacement and struct.last_bos_idx is not None and \
            struct.last_bos_idx >= len(candles_4h) - 4:
        setup_type = "BREAKOUT"
        notes.append("Fresh BOS with displacement")

    if setup_type is None:
        return None

    return SetupCandidate(direction, setup_type, unmitigated, liq, struct, dealing, displacement, notes)


# =====================================================================
# 13. 1H EXECUTION TRIGGER
# =====================================================================

def confirm_1h_trigger(candles_1h: list[dict], direction: str, setup: SetupCandidate) -> dict:
    """The 1H timeframe answers one question only: is *now* a high
    quality moment to engage the 4H setup? Requires momentum alignment,
    orderflow alignment, and price being inside (or very near) a
    relevant unmitigated zone or the favourable side of the dealing
    range — never a fresh standalone trigger of its own."""
    ind = cached_indicators(f"1h_{id(candles_1h)}", candles_1h)
    mom = momentum_read(ind, direction)
    of = orderflow_proxy(candles_1h, direction)
    cur = candles_1h[-1]["c"]

    near_zone = any(price_in_zone(cur, z, tolerance_pct=0.002) for z in setup.zones) if setup.zones else False

    favourable_side = (direction == "long" and setup.dealing["zone"] in ("discount", "deep_discount")) or \
                       (direction == "short" and setup.dealing["zone"] in ("premium", "deep_premium"))

    return {"momentum": mom, "orderflow": of, "near_zone": near_zone,
            "favourable_side": favourable_side, "cur": cur, "ind": ind}


# =====================================================================
# 14. CONFLUENCE / CONFIDENCE MODEL  —  WEIGHTED PROBABILITY
# =====================================================================

# Independent evidence streams and their relative weight. These are
# decorrelated by design (structure, liquidity, momentum, orderflow,
# regime, funding/OI, volatility, RR quality) so the aggregate is a
# meaningful probability rather than redundant point-stacking.
WEIGHTS = {
    "daily_alignment": 16.0,
    "structure_quality": 12.0,
    "setup_type": 14.0,
    "zone_confluence": 10.0,
    "momentum": 12.0,
    "orderflow": 10.0,
    "volatility_fit": 8.0,
    "funding_oi": 8.0,
    "win_rate_prior": 6.0,
    "rr_quality": 8.0,
    "divergence_penalty": 6.0,
}


def logistic(x: float) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0


def compute_confidence(daily: dict, setup: SetupCandidate, trig: dict, vol: dict,
                        funding_score: float, wr_prior: float | None, rr: float,
                        direction: str) -> tuple[float, dict]:
    """Aggregates independent evidence into a single 0-100 confidence
    score via a weighted logistic blend (not additive point counting):
    each evidence stream is first scored to [-1, +1], multiplied by its
    weight, summed, then squashed through a logistic function so the
    output behaves like a genuine bounded probability estimate."""
    evid: dict[str, float] = {}

    daily_score_norm = (daily["score"] - 50.0) / 50.0
    evid["daily_alignment"] = daily_score_norm if direction == "long" else -daily_score_norm

    evid["structure_quality"] = (setup.struct.structure_quality * 2 - 1) if setup.struct.bias != "neutral" else -0.2

    setup_type_value = {"SWEEP_REVERSAL": 0.85, "CONTINUATION": 0.65, "BREAKOUT": 0.45}
    evid["setup_type"] = setup_type_value.get(setup.setup_type, 0.0)

    evid["zone_confluence"] = 0.8 if trig["near_zone"] else (-0.3 if not setup.zones else 0.1)

    mom = trig["momentum"]
    mom_val = 0.0
    if mom["aligned"]:
        mom_val += 0.6
    if mom["overheated"]:
        mom_val -= 0.5
    evid["momentum"] = max(-1.0, min(1.0, mom_val))

    evid["orderflow"] = 0.7 if trig["orderflow"]["aligned"] else -0.4

    if vol["regime"] in ("normal", "elevated"):
        vol_val = 0.5
    elif vol["regime"] == "low":
        vol_val = 0.1 if vol["contraction"] else -0.2
    elif vol["regime"] == "extreme":
        vol_val = -0.7
    else:
        vol_val = 0.0
    evid["volatility_fit"] = vol_val

    evid["funding_oi"] = max(-1.0, min(1.0, funding_score))

    if wr_prior is None:
        evid["win_rate_prior"] = 0.0
    else:
        evid["win_rate_prior"] = max(-1.0, min(1.0, (wr_prior - 0.5) * 2.5))

    evid["rr_quality"] = max(-1.0, min(1.0, (rr - MIN_RR) / (PREFERRED_RR - MIN_RR)))

    evid["divergence_penalty"] = -0.8 if mom["divergence"] else 0.15

    weighted_sum = sum(evid[k] * WEIGHTS[k] for k in WEIGHTS)
    max_possible = sum(WEIGHTS.values())
    normalized = weighted_sum / max_possible  # roughly [-1, 1]

    confidence = logistic(normalized * 4.2) * 100.0
    return confidence, evid


def grade_for_confidence(confidence: float) -> str | None:
    if confidence >= CONFIDENCE_FLOOR_ELITE:
        return "Elite"
    if confidence >= CONFIDENCE_FLOOR_PREMIUM:
        return "Premium"
    if confidence >= CONFIDENCE_FLOOR_HIGH:
        return "High Quality"
    if confidence >= CONFIDENCE_FLOOR_STANDARD:
        return "Standard"
    return None


_GRADE_RANK = {"Standard": 0, "High Quality": 1, "Premium": 2, "Elite": 3}


def grade_meets_floor(grade: str | None, floor: str) -> bool:
    """True if `grade` is at least as strong as `floor` in the
    Standard < High Quality < Premium < Elite ordering."""
    if grade is None:
        return False
    return _GRADE_RANK.get(grade, -1) >= _GRADE_RANK.get(floor, 99)


# =====================================================================
# 15. DYNAMIC ENTRY ENGINE
# =====================================================================

@dataclass
class EntryPlan:
    entry_type: str   # "MARKET" | "LIMIT" | "PULLBACK" | "BREAKOUT"
    entry: float
    rationale: str


def plan_entry(direction: str, setup: SetupCandidate, trig: dict, atr_val: float) -> EntryPlan:
    """Chooses whichever execution style maximises realistic fill
    probability for the detected setup, rather than chasing a
    theoretically perfect price. Distance to the nearest valid zone is
    the primary input — entries far from price that routinely go
    unfilled are avoided in favour of a tighter, fillable level."""
    cur = trig["cur"]
    candidate_zones = [z for z in setup.zones if (z.direction == "bull") == (direction == "long")]

    if setup.setup_type == "BREAKOUT" and setup.displacement:
        return EntryPlan("MARKET", cur, "Displacement breakout — immediate market entry to avoid missing the move")

    if candidate_zones:
        nearest = min(candidate_zones, key=lambda z: abs(cur - (z.top + z.bottom) / 2))
        mid = (nearest.top + nearest.bottom) / 2
        dist_atr = abs(cur - mid) / atr_val if atr_val else 0.0
        if dist_atr <= 0.15:
            return EntryPlan("MARKET", cur, f"Price already inside {nearest.kind} zone")
        if dist_atr <= 1.1:
            edge = nearest.top if direction == "long" else nearest.bottom
            limit_px = edge if dist_atr <= 0.5 else mid
            return EntryPlan("LIMIT", limit_px, f"Limit order at {nearest.kind} zone ({dist_atr:.2f} ATR away)")
        # Zone too far to realistically fill — prefer a closer pullback level instead.

    ema_ref = trig["ind"]["ema21"][-1]
    if not math.isnan(ema_ref):
        dist_atr = abs(cur - ema_ref) / atr_val if atr_val else 0.0
        if dist_atr <= 0.6:
            return EntryPlan("PULLBACK", ema_ref, "Shallow pullback to 21EMA, realistically fillable")

    return EntryPlan("MARKET", cur, "No nearby discounted zone — engaging at market to avoid a stale signal")


# =====================================================================
# 16. DYNAMIC RISK ENGINE  —  SL / TP
# =====================================================================

@dataclass
class RiskPlan:
    sl: float
    tp1: float
    tp2: float
    rr1: float
    rr2: float


def plan_risk(direction: str, entry: float, setup: SetupCandidate, atr_val: float,
              liq: LiquidityRead, vol: dict) -> RiskPlan:
    """Stop loss is placed beyond the structural invalidation point
    (the sweep low/high or the defending zone), padded by a volatility
    buffer so normal noise doesn't trigger it, then floored by a
    minimum ATR-based distance for instruments with thin structure.
    Take-profits target the next genuine liquidity pool first (TP1)
    and a volatility-extended objective second (TP2), both subject to
    a minimum risk:reward floor."""
    vol_pad = atr_val * (0.35 if vol["regime"] in ("low", "normal") else 0.55)

    if direction == "long":
        struct_sl = (liq.swept_low or setup.struct.last_major_low or entry - atr_val * 1.2) - vol_pad
        atr_floor_sl = entry - atr_val * 1.0
        sl = min(struct_sl, atr_floor_sl) if struct_sl > entry - atr_val * 2.2 else atr_floor_sl
        risk = entry - sl
        pool_tp = liq.nearest_pool_above
        tp1 = pool_tp if pool_tp and (pool_tp - entry) >= risk * MIN_RR else entry + risk * 1.6
        tp2 = entry + risk * max(PREFERRED_RR, ((pool_tp - entry) / risk * 1.3) if pool_tp and risk > 0 else PREFERRED_RR)
    else:
        struct_sl = (liq.swept_high or setup.struct.last_major_high or entry + atr_val * 1.2) + vol_pad
        atr_floor_sl = entry + atr_val * 1.0
        sl = max(struct_sl, atr_floor_sl) if struct_sl < entry + atr_val * 2.2 else atr_floor_sl
        risk = sl - entry
        pool_tp = liq.nearest_pool_below
        tp1 = pool_tp if pool_tp and (entry - pool_tp) >= risk * MIN_RR else entry - risk * 1.6
        tp2 = entry - risk * max(PREFERRED_RR, ((entry - pool_tp) / risk * 1.3) if pool_tp and risk > 0 else PREFERRED_RR)

    risk = abs(entry - sl)
    rr1 = abs(tp1 - entry) / risk if risk > 0 else 0.0
    rr2 = abs(tp2 - entry) / risk if risk > 0 else 0.0
    return RiskPlan(sl, tp1, tp2, rr1, rr2)


# =====================================================================
# 17. MARKET FILTERS
# =====================================================================

def funding_oi_score(state: dict, symbol: str, direction: str, funding: float | None, oi_usd: float | None) -> tuple[float, list[str]]:
    notes = []
    score = 0.0
    if funding is not None:
        tailwind = (direction == "long" and funding < -FUNDING_TAILWIND_PCT) or \
                   (direction == "short" and funding > FUNDING_TAILWIND_PCT)
        headwind = (direction == "long" and funding > FUNDING_HEADWIND_PCT) or \
                   (direction == "short" and funding < -FUNDING_HEADWIND_PCT)
        if tailwind:
            score += 0.6
            notes.append("Funding favours this direction (crowd positioned opposite)")
        elif headwind:
            score -= 0.8
            notes.append("Funding headwind — crowd already positioned this way")
    if oi_usd is not None:
        hist = state.get("oi_history", {}).get(symbol, [])
        if len(hist) >= 5:
            avg = sum(hist[-5:]) / 5
            chg = (oi_usd - avg) / avg * 100 if avg else 0.0
            if abs(chg) >= 3.0:
                rising_with_move = chg > 0
                score += 0.3 if rising_with_move else -0.2
                notes.append(f"OI {'rising' if rising_with_move else 'falling'} {chg:+.1f}%")
    return max(-1.0, min(1.0, score)), notes


def passes_hard_filters(symbol: str, oi_usd: float | None, atr_pct: float) -> tuple[bool, str]:
    min_oi = MIN_OI_USD_SMALL_CAP if symbol in SMALL_CAP_PAIRS else MIN_OI_USD
    if oi_usd is not None and oi_usd < min_oi:
        return False, f"OI ${oi_usd:,.0f} below floor ${min_oi:,.0f}"
    if atr_pct < ATR_PCT_FLOOR:
        return False, f"ATR% {atr_pct:.3f} below liquidity-quality floor"
    if atr_pct > ATR_PCT_CEIL:
        return False, f"ATR% {atr_pct:.2f} above sane ceiling — likely data anomaly or dislocation"
    return True, ""


def win_rate_suppression(state: dict, symbol: str, direction: str) -> bool:
    wr = compute_win_rates(state).get(f"{symbol}_{direction}")
    if wr and wr["n"] >= WIN_RATE_MIN_SAMPLE and wr["win_rate"] is not None:
        return wr["win_rate"] < WIN_RATE_HARD_SUPPRESS_THRESHOLD
    return False


# =====================================================================
# 18. CORRELATION DECLUSTERING
# =====================================================================

def correlation_matrix(symbols: list[str], bundles: dict[str, tuple]) -> dict[tuple[str, str], float]:
    rets: dict[str, list[float]] = {}
    for sym in symbols:
        b = bundles.get(sym)
        if not b:
            continue
        closes = [c["c"] for c in b[1][-(CORR_LOOKBACK_BARS + 1):]]
        if len(closes) < CORR_MIN_SAMPLE + 1:
            continue
        r = [(closes[i] - closes[i - 1]) / closes[i - 1] for i in range(1, len(closes)) if closes[i - 1]]
        if len(r) >= CORR_MIN_SAMPLE:
            rets[sym] = r
    matrix = {}
    syms = sorted(rets)
    for i, a in enumerate(syms):
        for b in syms[i + 1:]:
            ra, rb = rets[a], rets[b]
            n = min(len(ra), len(rb))
            ra_, rb_ = ra[:n], rb[:n]
            ma, mb = sum(ra_) / n, sum(rb_) / n
            cov = sum((x - ma) * (y - mb) for x, y in zip(ra_, rb_)) / n
            va = sum((x - ma) ** 2 for x in ra_) / n
            vb = sum((y - mb) ** 2 for y in rb_) / n
            if va > 0 and vb > 0:
                matrix[(a, b)] = cov / math.sqrt(va * vb)
    return matrix


def cluster_correlated(symbols: list[str], matrix: dict[tuple[str, str], float]) -> list[set[str]]:
    parent = {s: s for s in symbols}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for (a, b), corr in matrix.items():
        if corr >= CORR_CLUSTER_THRESHOLD:
            union(a, b)
    clusters: dict[str, set[str]] = {}
    for s in symbols:
        clusters.setdefault(find(s), set()).add(s)
    return list(clusters.values())


def deduplicate_correlated(signals: list[tuple], clusters: list[set[str]]) -> list[tuple]:
    cluster_of: dict[str, frozenset] = {}
    for cl in clusters:
        fz = frozenset(cl)
        for s in cl:
            cluster_of[s] = fz
    signals = sorted(signals, key=lambda t: t[2]["confidence"], reverse=True)
    seen_clusters: set = set()
    out = []
    for sym, direction, sig in signals:
        fz = cluster_of.get(sym, frozenset({sym}))
        if fz in seen_clusters:
            continue
        seen_clusters.add(fz)
        out.append((sym, direction, sig))
    return out


# =====================================================================
# 19. SIGNAL OBJECT, COOLDOWNS, LIFECYCLE
# =====================================================================

@dataclass
class Signal:
    symbol: str
    direction: str
    setup_type: str
    grade: str
    confidence: float
    entry_plan: EntryPlan
    risk_plan: RiskPlan
    daily: dict
    setup: SetupCandidate
    trig: dict
    vol: dict
    atr_pct: float
    funding: float | None
    notes: list[str]


def evaluate_symbol(symbol: str, state: dict, candles_1h, candles_4h, candles_1d) -> list[Signal]:
    daily = classify_daily_regime(candles_1d)

    ind_4h = cached_indicators(f"4h_{symbol}", candles_4h)
    cur_price = ind_4h["c"][-1]
    atr_4h = safe(ind_4h["atr"][-1], cur_price * 0.01)
    atr_pct = atr_4h / cur_price * 100 if cur_price else 0.0

    ctx = get_market_ctx(symbol)
    funding = ctx.get("funding") if ctx else None
    oi_usd = ctx.get("oi_usd") if ctx else None

    ok, reason = passes_hard_filters(symbol, oi_usd, atr_pct)
    if not ok:
        print(f"  [FILTER] {hl_coin(symbol)} rejected — {reason}")
        return []

    update_history(state, "atr_history", symbol, atr_pct, ATR_HISTORY_DEPTH)
    if oi_usd is not None:
        update_history(state, "oi_history", symbol, oi_usd, OI_HISTORY_DEPTH)
    if funding is not None:
        update_history(state, "funding_history", symbol, funding, FUNDING_HISTORY_DEPTH)

    bbw_hist = ind_4h["bbw"]
    vol = volatility_regime(state, symbol, atr_pct, safe(bbw_hist[-1], float("nan")), bbw_hist)

    candidate_directions = []
    if daily["label"] in ("bullish", "neutral"):
        candidate_directions.append("long")
    if daily["label"] in ("bearish", "neutral"):
        candidate_directions.append("short")

    signals: list[Signal] = []
    for direction in candidate_directions:
        suppressed = win_rate_suppression(state, symbol, direction)

        setup = build_4h_setup(candles_4h, daily, direction)
        if setup is None:
            continue

        trig = confirm_1h_trigger(candles_1h, direction, setup)
        if not trig["momentum"]["aligned"] and not trig["near_zone"]:
            continue
        if trig["momentum"]["overheated"]:
            continue

        entry_plan = plan_entry(direction, setup, trig, atr_4h)
        risk_plan = plan_risk(direction, entry_plan.entry, setup, atr_4h, setup.liquidity, vol)

        if risk_plan.rr1 < MIN_RR:
            continue

        f_score, f_notes = funding_oi_score(state, symbol, direction, funding, oi_usd)
        wr = compute_win_rates(state).get(f"{symbol}_{direction}")
        wr_prior = wr["win_rate"] if wr and wr["n"] >= WIN_RATE_MIN_SAMPLE else None

        confidence, evidence = compute_confidence(
            daily, setup, trig, vol, f_score, wr_prior, risk_plan.rr1, direction
        )
        grade = grade_for_confidence(confidence)
        if grade is None:
            continue

        # A suppressed symbol/direction is no longer hard-blocked, but it
        # must clear WIN_RATE_SUPPRESSION_MIN_GRADE to still fire. This
        # lets only the best setups through for a known-weak pair, so new
        # resolved trades keep accumulating and the pair can earn its way
        # out of suppression via outcomes rather than staying frozen.
        if suppressed and not grade_meets_floor(grade, WIN_RATE_SUPPRESSION_MIN_GRADE):
            print(f"  [WR SUPPRESS] {hl_coin(symbol)} {direction.upper()} — "
                  f"grade {grade} below {WIN_RATE_SUPPRESSION_MIN_GRADE} floor required while suppressed")
            continue
        if suppressed:
            print(f"  [WR SUPPRESS OVERRIDE] {hl_coin(symbol)} {direction.upper()} — "
                  f"{grade} clears suppression floor, allowing through")

        notes = list(setup.notes) + f_notes
        signals.append(Signal(
            symbol, direction, setup.setup_type, grade, confidence,
            entry_plan, risk_plan, daily, setup, trig, vol, atr_pct, funding, notes,
        ))

    return signals


def check_cooldown(state: dict, symbol: str, direction: str, bar_index: int, confidence: float) -> bool:
    with _state_lock:
        active = list(state.get("active_signals", []))
        last_bar = state.get("signal_cooldowns", {}).get(f"{symbol}_{direction}")
        last_sl_ts = state.get("post_loss_cooldown", {}).get(f"{symbol}_{direction}")
        prev_outcome = state.get("last_signal_outcome", {}).get(f"{symbol}_{direction}", "")

    active_count = sum(1 for s in active if not s.get("resolved"))
    if active_count >= MAX_CONCURRENT_ACTIVE:
        return False

    for sig in active:
        if sig.get("symbol") == symbol and sig.get("direction") == direction and not sig.get("resolved"):
            return False

    if last_bar is not None:
        bars = bar_index - last_bar
        if prev_outcome in ("tp1", "tp2"):
            req = POST_WIN_COOLDOWN_BARS
        elif confidence >= CONFIDENCE_FLOOR_PREMIUM:
            req = 1
        elif confidence >= CONFIDENCE_FLOOR_HIGH:
            req = 2
        else:
            req = BASE_COOLDOWN_BARS
        if bars < req:
            return False

    if last_sl_ts is not None and int(time.time()) - last_sl_ts < POST_LOSS_COOLDOWN_S:
        return False

    return True


def update_cooldown(state: dict, symbol: str, direction: str, bar_index: int):
    with _state_lock:
        state.setdefault("signal_cooldowns", {})[f"{symbol}_{direction}"] = bar_index


def track_signal(state: dict, symbol: str, direction: str, msg_id: int, sig: Signal,
                  bar_index: int, hist_id: str):
    with _state_lock:
        state.setdefault("active_signals", []).append({
            "symbol": symbol, "direction": direction, "msg_id": msg_id, "bar_index": bar_index,
            "signal_bar_time": bar_index * INTERVAL_MS["1h"], "tp1": sig.risk_plan.tp1,
            "tp2": sig.risk_plan.tp2, "sl": sig.risk_plan.sl, "entry": sig.entry_plan.entry,
            "tp1_hit": False, "entry_touched": False, "resolved": False, "hist_id": hist_id,
            "atr_val": sig.atr_pct / 100.0 * sig.entry_plan.entry,
        })


def check_active_signals(state: dict, bar_index_now: int, ref_ms: int):
    with _state_lock:
        signals = list(state.get("active_signals", []))
    if not signals:
        return
    still_active = []
    for sig in signals:
        age = bar_index_now - sig.get("bar_index", bar_index_now)
        if age > SIGNAL_MAX_AGE_1H_BARS:
            hist_id = sig.get("hist_id")
            if hist_id:
                update_signal_result(state, hist_id, "expired")
            continue
        if sig.get("resolved"):
            continue

        symbol, direction, msg_id = sig["symbol"], sig["direction"], sig["msg_id"]
        tp1, tp2, sl_ = sig["tp1"], sig["tp2"], sig["sl"]
        tp1_hit, entry_touched = sig.get("tp1_hit", False), sig.get("entry_touched", False)
        entry_price = sig.get("entry", 0.0)
        atr_val = sig.get("atr_val") or (entry_price * 0.01 if entry_price else 0.01)
        entry_tol = atr_val * 0.55
        last_ts = sig.get("last_processed_ts", sig.get("signal_bar_time", 0))

        try:
            candles = get_candles(symbol, "1h", N_1H, reference_ms=ref_ms)
        except Exception as e:
            print(f"  [TRACK] {symbol} fetch failed: {e}")
            still_active.append(sig)
            continue
        new = [c for c in candles if c["t"] > last_ts]
        if not new:
            if not entry_touched and entry_price:
                mid = get_mid_price(symbol)
                if mid is not None and abs(mid - entry_price) <= entry_tol:
                    sig["entry_touched"] = True
            still_active.append(sig)
            continue
        last_ts = new[-1]["t"]

        def resolve(result_: str):
            sig["resolved"] = True
            with _state_lock:
                if result_ == "sl":
                    state.setdefault("post_loss_cooldown", {})[f"{symbol}_{direction}"] = int(time.time())
                state.setdefault("last_signal_outcome", {})[f"{symbol}_{direction}"] = result_
            hist_id = sig.get("hist_id")
            if hist_id:
                update_signal_result(state, hist_id, result_)

        for bar in new:
            ch, cl = bar["h"], bar["l"]
            if entry_price and not entry_touched:
                if abs((ch + cl) / 2 - entry_price) <= entry_tol:
                    sig["entry_touched"] = entry_touched = True
            if direction == "long":
                if not tp1_hit:
                    if ch >= tp1:
                        if entry_touched:
                            react_to_message(msg_id, REACT_TP1)
                            sig["tp1_hit"] = tp1_hit = True
                        else:
                            react_to_message(msg_id, REACT_MISS)
                            resolve("missed"); break
                    elif cl <= sl_:
                        if entry_touched:
                            react_to_message(msg_id, REACT_SL)
                            resolve("sl")
                        else:
                            resolve("expired")
                        break
                else:
                    if ch >= tp2:
                        react_to_message(msg_id, REACT_TP2); resolve("tp2"); break
                    if cl <= sl_:
                        resolve("tp1"); break
            else:
                if not tp1_hit:
                    if cl <= tp1:
                        if entry_touched:
                            react_to_message(msg_id, REACT_TP1)
                            sig["tp1_hit"] = tp1_hit = True
                        else:
                            react_to_message(msg_id, REACT_MISS)
                            resolve("missed"); break
                    elif ch >= sl_:
                        if entry_touched:
                            react_to_message(msg_id, REACT_SL)
                            resolve("sl")
                        else:
                            resolve("expired")
                        break
                else:
                    if cl <= tp2:
                        react_to_message(msg_id, REACT_TP2); resolve("tp2"); break
                    if ch >= sl_:
                        resolve("tp1"); break

        if not sig.get("resolved"):
            sig["last_processed_ts"] = last_ts
            still_active.append(sig)

    with _state_lock:
        state["active_signals"] = still_active


# =====================================================================
# 20. TELEGRAM
# =====================================================================

def send_telegram(text: str) -> int | None:
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    for attempt in range(3):
        try:
            r = requests.post(url, json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
            r.raise_for_status()
            return r.json()["result"]["message_id"]
        except Exception as e:
            if attempt == 2:
                print(f"[TG ERROR] {e.__class__.__name__}: {str(e)[:200]}")
            time.sleep(2)
    return None


def react_to_message(message_id: int, emoji: str):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/setMessageReaction"
    try:
        r = requests.post(url, json={"chat_id": TG_CHAT_ID, "message_id": message_id,
                                       "reaction": [{"type": "emoji", "emoji": emoji}], "is_big": False}, timeout=10)
        r.raise_for_status()
    except Exception as e:
        print(f"  [REACT ERROR] {e.__class__.__name__}: {str(e)[:200]}")


def confidence_bar(confidence: float) -> str:
    filled = max(0, min(10, round(confidence / 10)))
    return "█" * filled + "░" * (10 - filled)


RANK_MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}


def fmt_px(v: float) -> str:
    if v >= 1000:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:.4f}"
    return f"{v:.6f}"


def format_signal(sig: Signal, rank: int = 0) -> str:
    emoji = GRADE_EMOJI.get(sig.grade, "⚪")
    direction_tag = "▲ LONG" if sig.direction == "long" else "▼ SHORT"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    medal = RANK_MEDALS.get(rank, "")
    rank_tag = f"{medal} <b>Priority #{rank}</b>\n" if rank else ""
    max_lev = GRADE_MAX_LEVERAGE.get(sig.grade, 3.0)
    size_pct = GRADE_SIZE_PCT.get(sig.grade, 45)

    rp = sig.risk_plan
    ep = sig.entry_plan
    notes_block = ("\n" + "\n".join(f"• {n}" for n in sig.notes[:4]) + "\n") if sig.notes else ""

    return (
        f"{rank_tag}"
        f"{emoji} <b>{direction_tag} [{sig.setup_type}]  {sig.grade.upper()}</b>  "
        f"{confidence_bar(sig.confidence)} {sig.confidence:.0f}%\n"
        f"<b>Pair:</b> {sig.symbol}   |   <b>Daily:</b> {sig.daily['label']} "
        f"({sig.daily['score']:.0f}/100)   |   <b>Vol:</b> {sig.vol['regime']}\n"
        f"\n"
        f"<b>Entry:</b> <code>{fmt_px(ep.entry)}</code>  [{ep.entry_type}]\n"
        f"<b>TP1:</b>   <code>{fmt_px(rp.tp1)}</code>  (R:R {rp.rr1:.1f})\n"
        f"<b>TP2:</b>   <code>{fmt_px(rp.tp2)}</code>  (R:R {rp.rr2:.1f})\n"
        f"<b>SL:</b>    <code>{fmt_px(rp.sl)}</code>   (ATR {sig.atr_pct:.2f}%)\n"
        f"\n"
        f"<b>Leverage:</b> Max {max_lev:.0f}x   <b>Size:</b> {size_pct}%\n"
        f"{notes_block}"
        f"\n<i>{__engine_name__} v{__version__} [1D/4H/1H] • Hyperliquid Perps • {ts}</i>"
    )


# =====================================================================
# 21. SCAN ORCHESTRATION
# =====================================================================

def scan_symbol(symbol: str, state: dict, bar_index_now: int, bundle: tuple | None) -> list[tuple]:
    if bundle is None:
        return []
    candles_1h, candles_4h, candles_1d = bundle
    try:
        signals = evaluate_symbol(symbol, state, candles_1h, candles_4h, candles_1d)
    except Exception as e:
        print(f"  [ERROR] {symbol} evaluation failed: {e}")
        return []

    out = []
    for sig in signals:
        if not check_cooldown(state, symbol, sig.direction, bar_index_now, sig.confidence):
            continue
        out.append((symbol, sig.direction, {"confidence": sig.confidence, "sig": sig}))
    return out


def main():
    global _shutdown
    lock_fd = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("[LOCK] another instance is running — exiting")
        sys.exit(1)

    banner = (
        f"\n{'=' * 64}\n"
        f"  {__engine_name__}  v{__version__}\n"
        f"  \"{__tagline__}\"\n"
        f"{'=' * 64}\n"
    )
    print(banner)

    state = load_state()
    reset_win_rate_cache()
    clear_indicator_cache()

    ref_ms = int(time.time() * 1000)
    bar_index_now = ref_ms // INTERVAL_MS["1h"]

    print("[PHASE 0] Checking active signals…")
    check_active_signals(state, bar_index_now, ref_ms)
    save_state(state)

    print(f"[PHASE 1] Fetching candles for {len(WATCHLIST)} symbols…")
    bundles: dict[str, tuple] = {}
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futs = {ex.submit(fetch_all_candles, sym, ref_ms): sym for sym in WATCHLIST}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                b = fut.result()
                if b:
                    bundles[sym] = b
                else:
                    print(f"  [SKIP] {sym} — insufficient candle data")
            except Exception as e:
                print(f"  [ERROR] {sym} candle fetch: {e}")

    if _shutdown:
        save_state(state)
        sys.exit(0)

    print("[PHASE 2] Building correlation clusters…")
    try:
        matrix = correlation_matrix(WATCHLIST, bundles)
        clusters = cluster_correlated(WATCHLIST, matrix)
        multi = [sorted(c) for c in clusters if len(c) > 1]
        if multi:
            print(f"  [CORR] clusters: {multi}")
    except Exception as e:
        print(f"  [CORR] failed: {e}")
        clusters = [{s} for s in WATCHLIST]

    print("[PHASE 3] Scanning for confluence (1D -> 4H -> 1H)…")
    get_meta_and_ctx()

    pending: list[tuple] = []
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futs = {ex.submit(scan_symbol, sym, state, bar_index_now, bundles.get(sym)): sym for sym in WATCHLIST}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                res = fut.result()
                pending.extend(res)
            except Exception as e:
                print(f"    ERROR processing {sym}: {e}")

    flat = [(sym, direction, payload["sig"]) for sym, direction, payload in pending]
    deduped = deduplicate_correlated(
        [(sym, direction, {"confidence": sig.confidence}) for sym, direction, sig in flat], clusters
    )
    deduped_keys = {(s, d) for s, d, _ in deduped}
    deduped_signals = [(s, d, sig) for s, d, sig in flat if (s, d) in deduped_keys]
    deduped_signals.sort(key=lambda t: t[2].confidence, reverse=True)
    top = deduped_signals[:MAX_SIGNALS_PER_SCAN]
    dropped = deduped_signals[MAX_SIGNALS_PER_SCAN:]

    print(f"  [SCAN SUMMARY] {len(deduped_signals)} signal(s) found")
    if dropped:
        print(f"  [RANK] dropped {len(dropped)} lower-priority: "
              f"{[f'{hl_coin(s)} {d.upper()}' for s, d, _ in dropped]}")

    fired = 0
    fired_keys: set[str] = set()
    for rank, (symbol, direction, sig) in enumerate(top, start=1):
        key = f"{symbol}_{direction}"
        if key in fired_keys:
            continue
        fired_keys.add(key)

        msg = format_signal(sig, rank=rank)
        msg_id = send_telegram(msg)

        hist_id = record_signal_history(
            state, symbol, direction, sig.setup_type, sig.confidence, sig.grade,
            sig.funding, sig.atr_pct, sent=bool(msg_id),
        )

        if msg_id:
            update_cooldown(state, symbol, direction, bar_index_now)
            track_signal(state, symbol, direction, msg_id, sig, bar_index_now, hist_id)
            print(f"  [SENT] #{rank} {hl_coin(symbol)} {direction.upper()} "
                  f"grade={sig.grade} conf={sig.confidence:.0f}% "
                  f"TP1={sig.risk_plan.tp1:.4f} TP2={sig.risk_plan.tp2:.4f} SL={sig.risk_plan.sl:.4f}")
        else:
            print(f"  [TG FAIL] #{rank} {hl_coin(symbol)} — Telegram send failed")
        fired += 1
        time.sleep(0.5)

    save_state(state)
    print(f"\nScan complete. {fired} signal(s) fired.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        try:
            send_telegram(f"🚨 {__engine_name__} v{__version__} crashed: {e}")
        except Exception:
            pass
        raise
    finally:
        _hl_session.close()
