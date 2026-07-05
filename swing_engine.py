"""
CRUCIBLE ALPHA  v1.0.0  —  1D -> 4H -> 1H  ADAPTIVE PROBABILITY ENGINE
"Signal is earned, not counted."

A ground-up institutional signal engine. It shares only its *operating
model* with any prior bot on this deployment (Hyperliquid market data,
Telegram delivery + reaction lifecycle, state.json persistence,
single-scan-per-run execution triggered by an external cron). Every
piece of trading logic in this file — market structure classification,
liquidity mapping, order-block / fair-value-gap detection, regime
detection, the confluence engine, the confidence model, entry
selection and risk management — is an original design built around
Smart Money / ICT concepts and standard quantitative primitives.

ARCHITECTURE
------------
1D  — Macro regime layer. Produces a continuous trend-quality score
      (0-100) and a volatility/regime classification. Never a hard
      directional gate — it shapes the confidence prior instead.
4H  — Setup layer. Maps the dealing range (premium/discount), detects
      liquidity sweeps, order blocks and fair value gaps, and defines
      a directional *candidate* with concrete invalidation structure.
5H(1H) — Trigger layer. Requires a displacement + momentum alignment
      event on the 1H before a candidate is promoted to a *signal*.
      This is what separates "a valid idea exists" from "act now".

DECISION PHILOSOPHY
--------------------
Price structure originates every idea. Indicators (RSI, ADX, ROC,
Bollinger width, volume) are never allowed to originate a signal on
their own — they only vote on confirmation/veto inside the confluence
engine. Every vote is converted to a log-odds contribution and combined
via logistic aggregation into a single probability estimate, which is
then mapped to a 0-100 confidence score. This avoids the common trap of
naive additive point-counting, where uncorrelated-looking conditions
that are actually redundant get double-counted.

Setups are only promoted to signals when modelled win probability
clears an *adaptive* bar that tightens automatically in choppy/low
quality regimes and relaxes in clean trending regimes with expanding
volatility — implemented via `regime_confidence_adjustment`.

Volume is a genuine confluence vote, not a documentation claim: a
close-location-value x volume CVD proxy (`orderflow_proxy`) measures
intrabar buy/sell pressure and its short-term slope, and current-bar
volume is compared against its 20-period average, both feeding
`score_confluence` directly. Historical win-rate is both a hard
post-hoc suppression gate *and* a graduated evidence stream once a
symbol/direction has >= WIN_RATE_MIN_SAMPLE resolved trades — realized
edge now moves the confidence number itself, not just a veto. Fresh
BOS/displacement breakouts additionally require confirmation against
the prior 20-period Donchian channel before being rewarded at full
weight.

OPERATING MODEL (kept compatible with the existing deployment)
-----------------------------------------------------------------
  * Single Python file, no packages
  * One scan per execution — no long-running loop
  * Triggered externally every 15 minutes (cron-job.org / GitHub Actions)
  * Reads state.json at startup, writes it before exit
  * Hyperliquid `info` endpoint for OHLCV + funding + open interest
  * Telegram for delivery, message edits and emoji-reaction lifecycle
    tracking (TP1 / TP2 / SL / missed)

CHANGELOG
---------
v1.0.0 — Initial release.
v1.1.0 — Institutional audit fixes:
          * Wired a real orderflow signal: ported a CLV x volume CVD
            proxy (`orderflow_proxy`) plus a volume-vs-SMA20 ratio into
            `score_confluence` as genuine confirmation/veto evidence.
            Docstring no longer overstates what the code does.
          * Promoted historical win-rate from a binary post-hoc
            suppression gate to a graduated evidence stream in
            `score_confluence` (in addition to, not instead of, the
            existing hard suppression floor).
          * Tightened correlation dedup to one signal per correlated
            cluster (was one per (cluster, direction), which allowed
            opposite-direction double exposure within a cluster).
          * Wired the previously dead Donchian(20) computation into a
            breakout-confirmation check for the BREAKOUT setup type.
"""

from __future__ import annotations

__engine_name__ = "CRUCIBLE ALPHA"
__version__ = "1.1.0"
__tagline__ = "Signal is earned, not counted."

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
STATE_FILE = str(_SCRIPT_DIR / "state.json")
STATE_VERSION = 1
LOCK_FILE = str(_SCRIPT_DIR / "crucible_alpha.lock")

HL_INFO_URL = "https://api.hyperliquid.xyz/info"
SCAN_WORKERS = int(os.getenv("SCAN_WORKERS", "2"))
HL_TF_WORKERS = int(os.getenv("HL_TF_WORKERS", "2"))
HL_MIN_INTERVAL_S = float(os.getenv("HL_MIN_INTERVAL_S", "0.18"))
HL_MIN_INTERVAL_MAX_S = float(os.getenv("HL_MIN_INTERVAL_MAX_S", "0.60"))

N_1H, N_4H, N_1D = 220, 280, 340
INTERVAL_MS = {"1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}

WATCHLIST = [
    "BTCUSDT", "ETHUSDT", "HYPEUSDT", "ZECUSDT", "NEARUSDT",
    "ONDOUSDT", "SUIUSDT", "PENGUUSDT", "BNBUSDT", "SOLUSDT",
    "TRXUSDT", "BCHUSDT", "DOGEUSDT", "ADAUSDT", "DOTUSDT",
    "TAOUSDT", "AVAXUSDT", "LINKUSDT", "AAVEUSDT", "XRPUSDT",
    "XLMUSDT", "UNIUSDT", "LTCUSDT", "APTUSDT", "PENDLEUSDT",
]
SMALL_CAP_PAIRS = {"PENGUUSDT", "HYPEUSDT", "ZECUSDT", "PENDLEUSDT"}

EMA_FAST, EMA_MID, EMA_SLOW, EMA_TREND = 8, 20, 50, 200
RSI_LEN, ATR_LEN, ADX_LEN, ROC_LEN, BB_LEN, DONCHIAN_LEN = 14, 14, 14, 12, 20, 20

CONFIDENCE_FLOOR_STANDARD = 60.0
CONFIDENCE_FLOOR_HIGH = 71.0
CONFIDENCE_FLOOR_PREMIUM = 81.0
CONFIDENCE_FLOOR_ELITE = 89.0

MIN_RR = 1.5
PREFERRED_RR = 2.3
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
    first_valid = next((i for i in range(n) if not math.isnan(dx[i])), None)
    if first_valid is not None and n - first_valid >= period:
        start = first_valid + period
        if start < n:
            adx[start] = sum(x for x in dx[first_valid:start] if not math.isnan(x)) / period
            for i in range(start + 1, n):
                adx[i] = (adx[i - 1] * (period - 1) + dx[i]) / period
    return adx, plus_di, minus_di


def roc(vals: list[float], period: int) -> list[float]:
    n = len(vals)
    out = [float("nan")] * n
    for i in range(period, n):
        out[i] = (vals[i] - vals[i - period]) / vals[i - period] * 100.0 if vals[i - period] else 0.0
    return out


def bollinger_width_pct(closes: list[float], period: int) -> list[float]:
    mid = sma(closes, period)
    sd = stdev(closes, period)
    n = len(closes)
    out = [float("nan")] * n
    for i in range(n):
        if not math.isnan(mid[i]) and mid[i]:
            out[i] = (4.0 * sd[i]) / mid[i] * 100.0
    return out


def donchian(highs, lows, period: int):
    n = len(highs)
    up, dn = [float("nan")] * n, [float("nan")] * n
    for i in range(period - 1, n):
        up[i] = max(highs[i - period + 1:i + 1])
        dn[i] = min(lows[i - period + 1:i + 1])
    return up, dn


_ind_cache: dict[str, dict] = {}
_ind_cache_lock = threading.Lock()


def compute_indicators(candles: list[dict]) -> dict:
    h = [c["h"] for c in candles]
    l = [c["l"] for c in candles]
    c = [c["c"] for c in candles]
    v = [c_["v"] for c_ in candles]
    adx, pdi, mdi = adx_dmi(h, l, c, ADX_LEN)
    dc_up, dc_dn = donchian(h, l, DONCHIAN_LEN)
    return {
        "h": h, "l": l, "c": c, "v": v,
        "ema8": ema(c, EMA_FAST), "ema20": ema(c, EMA_MID),
        "ema50": ema(c, EMA_SLOW), "ema200": ema(c, EMA_TREND),
        "rsi": rsi(c, RSI_LEN), "atr": atr(h, l, c, ATR_LEN),
        "adx": adx, "plus_di": pdi, "minus_di": mdi,
        "roc": roc(c, ROC_LEN), "bbw": bollinger_width_pct(c, BB_LEN),
        "vol_sma": sma(v, 20), "dc_up": dc_up, "dc_dn": dc_dn,
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
# 2b. ORDERFLOW PROXY (no L2 book available — body/volume based)
# =====================================================================

def orderflow_proxy(candles: list[dict], direction: str, lookback: int = 24) -> dict:
    """Without a live order book, intrabar buy/sell pressure is
    approximated via where each candle closes within its own range
    (close-location value), weighted by volume — a standard proxy for
    cumulative delta. Net pressure and its short-term slope are then
    compared against the trade direction for alignment. This is the
    real volume vote the module docstring claims; previously `vol_sma`
    was computed and never referenced anywhere in the file."""
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


def volume_confirmation(candles: list[dict], ind: dict) -> dict:
    """Compares the triggering bar's volume against its 20-period SMA
    (`vol_sma`, previously dead-computed and unused). Expansion above
    average supports a genuine participation shift; contraction below
    average is a mild caution flag rather than a veto."""
    vol_now = candles[-1]["v"]
    vol_sma_now = next((v for v in reversed(ind["vol_sma"]) if not math.isnan(v)), None)
    if not vol_sma_now or vol_sma_now <= 0:
        return {"ratio": 1.0, "expanding": False}
    ratio = vol_now / vol_sma_now
    return {"ratio": ratio, "expanding": ratio >= 1.15}


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
# 4. MARKET STRUCTURE — FRACTAL SWINGS, IMPULSE/CORRECTIVE CLASSIFICATION
# =====================================================================
#
# Rather than the common HH/HL/LH/LL walk, structure here is built from
# ATR-normalised "swing legs": a swing is only registered once price has
# travelled at least `SWING_MIN_ATR` average-true-ranges from the prior
# pivot. This filters out mechanically insignificant micro-pivots that
# otherwise pollute BOS/CHoCH detection on noisy crypto price action,
# and lets the *quality* of a break (how many ATRs it clears the prior
# pivot by) feed directly into the structure_quality score used by the
# confluence engine.

SWING_MIN_ATR = 0.55


@dataclass
class StructureState:
    bias: str
    last_bos_idx: int | None
    last_choch_idx: int | None
    swing_highs: list[tuple[int, float]]
    swing_lows: list[tuple[int, float]]
    last_major_high: float | None
    last_major_low: float | None
    structure_quality: float
    break_strength_atr: float


def _raw_fractals(candles: list[dict], left: int = 2, right: int = 2):
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
    n = len(candles)
    sh, sl = [], []
    for i in range(left, n - right):
        wh = highs[i - left:i + right + 1]
        wl = lows[i - left:i + right + 1]
        if highs[i] == max(wh) and wh.count(highs[i]) == 1:
            sh.append(i)
        if lows[i] == min(wl) and wl.count(lows[i]) == 1:
            sl.append(i)
    return sh, sl


def analyze_market_structure(candles: list[dict], atr_series: list[float]) -> StructureState:
    sh, sl = _raw_fractals(candles, left=2, right=2)
    if len(sh) < 2 or len(sl) < 2:
        return StructureState("neutral", None, None, [], [], None, None, 0.0, 0.0)

    avg_atr = next((v for v in reversed(atr_series) if not math.isnan(v)), None) or 0.0
    pts = sorted(
        [(i, "H", candles[i]["h"]) for i in sh] + [(i, "L", candles[i]["l"]) for i in sl]
    )

    filtered: list[tuple[int, str, float]] = []
    for i, kind, price in pts:
        if not filtered:
            filtered.append((i, kind, price))
            continue
        last_i, last_kind, last_price = filtered[-1]
        if kind == last_kind:
            better = (kind == "H" and price > last_price) or (kind == "L" and price < last_price)
            if better:
                filtered[-1] = (i, kind, price)
            continue
        move = abs(price - last_price)
        if avg_atr > 0 and move < avg_atr * SWING_MIN_ATR:
            continue
        filtered.append((i, kind, price))

    trend = "neutral"
    last_bos = last_choch = None
    quality_hits = quality_total = 0
    break_strength = 0.0
    prev_h = prev_l = None
    swing_highs, swing_lows = [], []

    for i, kind, price in filtered:
        if kind == "H":
            swing_highs.append((i, price))
            if prev_h is not None:
                quality_total += 1
                strength = abs(price - prev_h) / avg_atr if avg_atr else 0.0
                if price > prev_h:
                    quality_hits += 1
                    if trend == "bear":
                        last_choch = i
                    elif trend == "bull":
                        last_bos = i
                    trend = "bull"
                    break_strength = strength
                else:
                    if trend == "bull":
                        last_choch = i
                        trend = "bear"
                        break_strength = strength
            prev_h = price
        else:
            swing_lows.append((i, price))
            if prev_l is not None:
                quality_total += 1
                strength = abs(price - prev_l) / avg_atr if avg_atr else 0.0
                if price < prev_l:
                    quality_hits += 1
                    if trend == "bull":
                        last_choch = i
                    elif trend == "bear":
                        last_bos = i
                    trend = "bear"
                    break_strength = strength
                else:
                    if trend == "bear":
                        last_choch = i
                        trend = "bull"
                        break_strength = strength
            prev_l = price

    quality = (quality_hits / quality_total) if quality_total else 0.0
    last_major_high = swing_highs[-1][1] if swing_highs else None
    last_major_low = swing_lows[-1][1] if swing_lows else None
    return StructureState(trend, last_bos, last_choch, swing_highs, swing_lows,
                           last_major_high, last_major_low, quality, break_strength)


# =====================================================================
# 5. LIQUIDITY MAPPING — SWEEPS, EQUAL HIGHS/LOWS, POOLS
# =====================================================================

@dataclass
class LiquidityRead:
    swept_high: float | None
    swept_low: float | None
    sweep_bar_idx: int | None
    sweep_direction: str | None
    nearest_pool_above: float | None
    nearest_pool_below: float | None
    equal_highs: list[float]
    equal_lows: list[float]


def _cluster_levels(levels: list[float], tol_pct: float) -> list[float]:
    if not levels:
        return []
    levels = sorted(levels)
    clusters: list[list[float]] = [[levels[0]]]
    for v in levels[1:]:
        if abs(v - clusters[-1][-1]) / clusters[-1][-1] <= tol_pct:
            clusters[-1].append(v)
        else:
            clusters.append([v])
    return [sum(c) / len(c) for c in clusters if len(c) >= 2]


def read_liquidity(candles: list[dict], struct: StructureState, lookback: int = 40) -> LiquidityRead:
    window = candles[-lookback:]
    highs = [c["h"] for c in window]
    lows = [c["l"] for c in window]
    closes = [c["c"] for c in window]

    equal_highs = _cluster_levels(highs, 0.0018)
    equal_lows = _cluster_levels(lows, 0.0018)

    swept_high = swept_low = None
    sweep_idx = None
    sweep_dir = None

    prior_high = max(highs[:-3]) if len(highs) > 5 else None
    prior_low = min(lows[:-3]) if len(lows) > 5 else None
    for i in range(max(0, len(window) - 4), len(window)):
        h, l, c = highs[i], lows[i], closes[i]
        if prior_high and h > prior_high and c < prior_high:
            swept_high = h
            sweep_idx = i
            sweep_dir = "bear"
        if prior_low and l < prior_low and c > prior_low:
            swept_low = l
            sweep_idx = i
            sweep_dir = "bull"

    cur = closes[-1]
    pools_above = sorted([p for p in equal_highs + ([struct.last_major_high] if struct.last_major_high else []) if p and p > cur])
    pools_below = sorted([p for p in equal_lows + ([struct.last_major_low] if struct.last_major_low else []) if p and p < cur], reverse=True)

    return LiquidityRead(
        swept_high, swept_low, sweep_idx, sweep_dir,
        pools_above[0] if pools_above else None,
        pools_below[0] if pools_below else None,
        equal_highs, equal_lows,
    )


# =====================================================================
# 6. ZONES — ORDER BLOCKS & FAIR VALUE GAPS
# =====================================================================

@dataclass
class Zone:
    kind: str          # "OB" | "FVG"
    direction: str      # "bull" | "bear"
    top: float
    bottom: float
    idx: int
    mitigated: bool
    freshness: float    # 0-1, 1 = most recent


def detect_fair_value_gaps(candles: list[dict], max_zones: int = 6) -> list[Zone]:
    zones: list[Zone] = []
    n = len(candles)
    for i in range(2, n):
        c0, c2 = candles[i - 2], candles[i]
        if c2["l"] > c0["h"]:
            zones.append(Zone("FVG", "bull", c2["l"], c0["h"], i - 1, False, 0.0))
        elif c2["h"] < c0["l"]:
            zones.append(Zone("FVG", "bear", c0["l"], c2["h"], i - 1, False, 0.0))

    cur = candles[-1]["c"]
    for z in zones:
        if z.direction == "bull":
            z.mitigated = any(c["l"] <= z.top for c in candles[z.idx + 1:])
        else:
            z.mitigated = any(c["h"] >= z.bottom for c in candles[z.idx + 1:])

    zones = [z for z in zones if not z.mitigated]
    zones.sort(key=lambda z: z.idx, reverse=True)
    zones = zones[:max_zones]
    for rank, z in enumerate(zones):
        z.freshness = max(0.0, 1.0 - rank / max_zones)
    return zones


def detect_order_blocks(candles: list[dict], struct: StructureState, max_zones: int = 6) -> list[Zone]:
    """An order block is the last opposite-coloured candle immediately
    preceding a displacement leg that produced a structural break
    (BOS/CHoCH). Anchoring to *structural* breaks rather than any local
    displacement filters out low-conviction order blocks that never
    actually reorganised the market."""
    zones: list[Zone] = []
    break_indices = sorted({i for i in (struct.last_bos_idx, struct.last_choch_idx) if i is not None})
    all_break_pts = [i for i, _, _ in _swing_break_points(candles, struct)]
    candidates = sorted(set(break_indices) | set(all_break_pts))

    n = len(candles)
    for bidx in candidates:
        if bidx < 3 or bidx >= n:
            continue
        leg_start = max(0, bidx - 6)
        seg = candles[leg_start:bidx + 1]
        if len(seg) < 2:
            continue
        net_move = seg[-1]["c"] - seg[0]["o"]
        direction = "bull" if net_move > 0 else "bear"
        origin = None
        for j in range(len(seg) - 1, -1, -1):
            is_opposite = (seg[j]["c"] < seg[j]["o"]) if direction == "bull" else (seg[j]["c"] > seg[j]["o"])
            if is_opposite:
                origin = seg[j]
                break
        if origin is None:
            continue
        top, bottom = max(origin["o"], origin["c"]), min(origin["o"], origin["c"])
        idx_abs = leg_start + seg.index(origin)
        mitigated = any(
            (c["l"] <= top if direction == "bull" else c["h"] >= bottom)
            for c in candles[idx_abs + 1:]
        )
        if not mitigated:
            zones.append(Zone("OB", direction, top, bottom, idx_abs, False, 0.0))

    zones.sort(key=lambda z: z.idx, reverse=True)
    zones = zones[:max_zones]
    for rank, z in enumerate(zones):
        z.freshness = max(0.0, 1.0 - rank / max_zones)
    return zones


def _swing_break_points(candles: list[dict], struct: StructureState):
    pts = []
    highs = {i: p for i, p in struct.swing_highs}
    lows = {i: p for i, p in struct.swing_lows}
    idxs = sorted(set(highs) | set(lows))
    prev_h = prev_l = None
    for i in idxs:
        if i in highs:
            if prev_h is not None and highs[i] > prev_h:
                pts.append((i, "H", highs[i]))
            prev_h = highs[i]
        if i in lows:
            if prev_l is not None and lows[i] < prev_l:
                pts.append((i, "L", lows[i]))
            prev_l = lows[i]
    return pts


# =====================================================================
# 7. DEALING RANGE — PREMIUM / DISCOUNT
# =====================================================================

def dealing_range_position(candles: list[dict], struct: StructureState, lookback: int = 50) -> dict:
    window = candles[-lookback:]
    hi = struct.last_major_high or max(c["h"] for c in window)
    lo = struct.last_major_low or min(c["l"] for c in window)
    if hi <= lo:
        hi, lo = max(c["h"] for c in window), min(c["l"] for c in window)
    cur = candles[-1]["c"]
    span = hi - lo
    pos = (cur - lo) / span if span else 0.5
    if pos >= 0.62:
        label = "premium"
    elif pos <= 0.38:
        label = "discount"
    else:
        label = "equilibrium"
    return {"high": hi, "low": lo, "position": pos, "label": label}


# =====================================================================
# 8. REGIME & VOLATILITY CLASSIFICATION
# =====================================================================

def classify_volatility(state: dict, symbol: str, candles: list[dict], ind: dict) -> dict:
    atr_val = next((v for v in reversed(ind["atr"]) if not math.isnan(v)), None)
    cur = candles[-1]["c"]
    atr_pct = (atr_val / cur * 100.0) if atr_val and cur else 0.0
    update_history(state, "atr_history", symbol, atr_pct, ATR_HISTORY_DEPTH)
    hist = state.get("atr_history", {}).get(symbol, [])
    pct = percentile_rank(hist, atr_pct)

    bbw = next((v for v in reversed(ind["bbw"]) if not math.isnan(v)), None) or 0.0
    if pct is None:
        regime = "normal"
    elif pct <= 0.20:
        regime = "low"
    elif pct >= 0.80:
        regime = "high"
    else:
        regime = "normal"

    contracting = bbw > 0 and len([v for v in ind["bbw"][-10:] if not math.isnan(v)]) >= 5 and \
        ind["bbw"][-1] < (sum(v for v in ind["bbw"][-10:] if not math.isnan(v)) / max(1, len([v for v in ind["bbw"][-10:] if not math.isnan(v)])))

    return {"atr_val": atr_val or 0.0, "atr_pct": atr_pct, "percentile": pct,
            "regime": regime, "bbw": bbw, "contracting": contracting}


def daily_trend_quality(candles_1d: list[dict], ind_1d: dict) -> dict:
    """Continuous 0-100 trend-quality score built from EMA stack
    ordering, ADX strength, price position relative to EMA200, and
    the slope consistency of EMA50. This is a *prior*, never a hard
    gate — a low score damps confidence rather than blocking a setup
    outright, since strong 4H/1H setups can precede daily regime
    shifts."""
    e8, e20, e50, e200 = ind_1d["ema8"][-1], ind_1d["ema20"][-1], ind_1d["ema50"][-1], ind_1d["ema200"][-1]
    adx = ind_1d["adx"][-1]
    cur = candles_1d[-1]["c"]

    if any(math.isnan(x) for x in (e8, e20, e50, e200, adx)):
        return {"score": 50.0, "label": "insufficient data", "direction": "neutral"}

    stack_bull = e8 > e20 > e50 > e200
    stack_bear = e8 < e20 < e50 < e200
    slope50 = ind_1d["ema50"][-1] - ind_1d["ema50"][-6] if len(ind_1d["ema50"]) > 6 and not math.isnan(ind_1d["ema50"][-6]) else 0.0

    direction = "bull" if cur > e200 else "bear"
    score = 50.0
    if stack_bull:
        score += 22
        direction = "bull"
    elif stack_bear:
        score += 22
        direction = "bear"
    else:
        score -= 8

    adx_component = max(0.0, min(20.0, (adx - 15.0) * 1.1))
    score += adx_component if (stack_bull or stack_bear) else adx_component * 0.3

    if (direction == "bull" and slope50 > 0) or (direction == "bear" and slope50 < 0):
        score += 8
    else:
        score -= 6

    dist200 = abs(cur - e200) / e200 * 100.0 if e200 else 0.0
    score += max(0.0, min(6.0, dist200 * 0.8))

    score = max(0.0, min(100.0, score))
    if score >= 78:
        label = f"strong {direction}"
    elif score >= 60:
        label = f"moderate {direction}"
    elif score >= 45:
        label = "transitional"
    else:
        label = "choppy"
    return {"score": score, "label": label, "direction": direction}


# =====================================================================
# 9. CONFLUENCE ENGINE — LOGISTIC PROBABILITY AGGREGATION
# =====================================================================
#
# Each independent evidence stream contributes a log-odds delta rather
# than a raw point value. Deltas are summed and passed through a
# logistic function to produce a probability, which keeps the model
# well-calibrated even as more evidence streams are added (unlike
# additive point scoring, where the scale is arbitrary and redundant
# conditions inflate scores). Weights below reflect the *marginal*,
# largely decorrelated edge each factor contributes historically in
# SMC-style research, not an attempt to reward stacking similar
# indicators.

def _logistic(x: float) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0


@dataclass
class SetupCandidate:
    direction: str
    setup_type: str
    struct: StructureState
    zones: list[Zone]
    liq: LiquidityRead
    range_pos: dict
    notes: list[str] = field(default_factory=list)
    displacement: bool = False
    donchian_confirmed: bool = False


def find_setup_candidate(candles_4h: list[dict], ind_4h: dict) -> SetupCandidate | None:
    atr_series = ind_4h["atr"]
    struct = analyze_market_structure(candles_4h, atr_series)
    if struct.bias == "neutral":
        return None

    liq = read_liquidity(candles_4h, struct)
    range_pos = dealing_range_position(candles_4h, struct)
    fvgs = detect_fair_value_gaps(candles_4h)
    obs = detect_order_blocks(candles_4h, struct)
    zones = obs + fvgs

    direction = None
    setup_type = None
    notes: list[str] = []
    displacement = False

    recent_break = struct.last_choch_idx if struct.last_choch_idx and struct.last_choch_idx >= len(candles_4h) - 8 else None

    if liq.sweep_direction == "bull" and range_pos["label"] in ("discount", "equilibrium"):
        direction, setup_type = "long", "LIQUIDITY_SWEEP_REVERSAL"
        notes.append("4H liquidity sweep below prior low, reclaimed — bullish reversal context")
    elif liq.sweep_direction == "bear" and range_pos["label"] in ("premium", "equilibrium"):
        direction, setup_type = "short", "LIQUIDITY_SWEEP_REVERSAL"
        notes.append("4H liquidity sweep above prior high, rejected — bearish reversal context")
    elif recent_break is not None:
        direction = "long" if struct.bias == "bull" else "short"
        setup_type = "CHOCH_REVERSAL"
        notes.append(f"4H change of character confirms {struct.bias} reorganisation")
    elif struct.bias == "bull" and range_pos["label"] == "discount" and any(z.direction == "bull" for z in zones):
        direction, setup_type = "long", "TREND_CONTINUATION"
        notes.append("Uptrend pullback into discount zone — continuation setup")
    elif struct.bias == "bear" and range_pos["label"] == "premium" and any(z.direction == "bear" for z in zones):
        direction, setup_type = "short", "TREND_CONTINUATION"
        notes.append("Downtrend pullback into premium zone — continuation setup")
    elif struct.last_bos_idx is not None and struct.last_bos_idx >= len(candles_4h) - 4 and struct.break_strength_atr >= 1.4:
        direction = "long" if struct.bias == "bull" else "short"
        setup_type = "BREAKOUT"
        displacement = True
        notes.append(f"Fresh {struct.bias} BOS with displacement of {struct.break_strength_atr:.2f}x ATR")
    else:
        return None

    matching_zones = [z for z in zones if (z.direction == "bull") == (direction == "long")]
    if not matching_zones and setup_type != "BREAKOUT":
        return None

    donchian_confirmed = False
    if setup_type == "BREAKOUT":
        # Previously-dead Donchian(20) computation now confirms the
        # breakout: the current close must clear the *prior* 20-period
        # channel extreme (excluding the current bar), not just show a
        # large displacement candle. Distinguishes a real range breakout
        # from a big candle inside an already-wide range.
        dc_up, dc_dn = ind_4h["dc_up"], ind_4h["dc_dn"]
        dc_up_prior = dc_up[-2] if len(dc_up) > 1 else float("nan")
        dc_dn_prior = dc_dn[-2] if len(dc_dn) > 1 else float("nan")
        last_close = candles_4h[-1]["c"]
        if direction == "long" and not math.isnan(dc_up_prior) and last_close > dc_up_prior:
            donchian_confirmed = True
            notes.append("Breakout confirmed above prior 20-period Donchian high")
        elif direction == "short" and not math.isnan(dc_dn_prior) and last_close < dc_dn_prior:
            donchian_confirmed = True
            notes.append("Breakout confirmed below prior 20-period Donchian low")

    return SetupCandidate(direction, setup_type, struct, zones, liq, range_pos, notes, displacement,
                           donchian_confirmed)


def momentum_trigger(candles_1h: list[dict], ind_1h: dict, direction: str) -> dict | None:
    """A signal is only promoted once the 1H shows a genuine
    displacement candle aligned with momentum (RSI slope + ROC sign +
    DI dominance), so the engine reacts to confirmed order flow rather
    than anticipating it."""
    if len(candles_1h) < 20:
        return None
    last = candles_1h[-1]
    body = abs(last["c"] - last["o"])
    rng = last["h"] - last["l"]
    atr_val = next((v for v in reversed(ind_1h["atr"]) if not math.isnan(v)), None)
    if not atr_val or atr_val <= 0:
        return None

    body_atr = body / atr_val
    is_bull_candle = last["c"] > last["o"]
    is_bear_candle = last["c"] < last["o"]

    rsi_now, rsi_prev = ind_1h["rsi"][-1], ind_1h["rsi"][-3] if len(ind_1h["rsi"]) > 3 else float("nan")
    roc_now = ind_1h["roc"][-1]
    pdi, mdi = ind_1h["plus_di"][-1], ind_1h["minus_di"][-1]

    if any(math.isnan(x) for x in (rsi_now, roc_now)):
        return None

    displacement = body_atr >= 0.7 and body / rng >= 0.55 if rng else False

    if direction == "long":
        aligned = is_bull_candle and roc_now > 0 and (math.isnan(rsi_prev) or rsi_now >= rsi_prev) \
            and (math.isnan(pdi) or math.isnan(mdi) or pdi >= mdi)
    else:
        aligned = is_bear_candle and roc_now < 0 and (math.isnan(rsi_prev) or rsi_now <= rsi_prev) \
            and (math.isnan(pdi) or math.isnan(mdi) or mdi >= pdi)

    if not aligned:
        return None

    of = orderflow_proxy(candles_1h, direction)
    vol_conf = volume_confirmation(candles_1h, ind_1h)

    return {"cur": last["c"], "displacement": displacement, "body_atr": body_atr,
            "rsi": rsi_now, "roc": roc_now, "ind": ind_1h,
            "orderflow": of, "vol_confirmation": vol_conf}


def regime_confidence_adjustment(daily: dict, vol: dict) -> float:
    """Adaptive bar: favourable regimes (clean daily trend + expanding
    volatility off a contraction) relax the required probability;
    choppy/low-quality regimes tighten it."""
    adj = 0.0
    if daily["score"] >= 75:
        adj += 4.0
    elif daily["score"] <= 45:
        adj -= 7.0
    if vol["regime"] == "high" and vol.get("contracting") is False:
        adj += 2.0
    elif vol["regime"] == "low":
        adj -= 3.0
    return adj


def score_confluence(direction: str, setup: SetupCandidate, trig: dict, daily: dict,
                      vol: dict, funding_oi: tuple[float, list[str]],
                      wr_prior: float | None = None) -> tuple[float, list[str]]:
    log_odds = -0.55  # base prior: mild skepticism, evidence must earn its way up
    notes: list[str] = list(setup.notes)

    # --- Daily regime alignment ---
    daily_aligned = (direction == "long" and daily["direction"] == "bull") or \
                     (direction == "short" and daily["direction"] == "bear")
    daily_weight = (daily["score"] - 50.0) / 50.0
    log_odds += (0.85 if daily_aligned else -0.55) * abs(daily_weight) * 1.3

    # --- Structural quality ---
    log_odds += (setup.struct.structure_quality - 0.5) * 1.1
    if setup.struct.break_strength_atr > 0:
        log_odds += min(0.5, setup.struct.break_strength_atr * 0.18)

    # --- Zone confluence (order blocks + FVGs stacking) ---
    matching = [z for z in setup.zones if (z.direction == "bull") == (direction == "long")]
    ob_hits = sum(1 for z in matching if z.kind == "OB")
    fvg_hits = sum(1 for z in matching if z.kind == "FVG")
    if ob_hits and fvg_hits:
        log_odds += 0.55
        notes.append("Order block and fair value gap confluence in the same zone")
    elif ob_hits or fvg_hits:
        log_odds += 0.25
    freshness = max([z.freshness for z in matching], default=0.0)
    log_odds += freshness * 0.25

    # --- Premium/discount alignment ---
    favourable_pd = (direction == "long" and setup.range_pos["label"] in ("discount", "equilibrium")) or \
                     (direction == "short" and setup.range_pos["label"] in ("premium", "equilibrium"))
    log_odds += 0.3 if favourable_pd else -0.4

    # --- Liquidity sweep bonus ---
    if setup.setup_type == "LIQUIDITY_SWEEP_REVERSAL":
        log_odds += 0.4
        notes.append("Stop-hunt sweep precedes reversal — classic liquidity-grab structure")

    # --- Donchian breakout confirmation (previously dead computation) ---
    if setup.setup_type == "BREAKOUT":
        if setup.donchian_confirmed:
            log_odds += 0.3
        else:
            log_odds -= 0.25
            notes.append("Breakout lacks prior-range Donchian confirmation — displacement alone")

    # --- Orderflow (CVD proxy) — real volume vote ---
    of = trig["orderflow"]
    if of["aligned"]:
        log_odds += 0.35
        notes.append("Orderflow (CVD proxy) confirms directional pressure")
    else:
        log_odds -= 0.25

    # --- Volume confirmation (vs 20-period SMA) ---
    vc = trig["vol_confirmation"]
    if vc["expanding"]:
        log_odds += min(0.3, (vc["ratio"] - 1.0) * 0.3)
        notes.append("Volume expanding above 20-period average")
    elif vc["ratio"] < 0.7:
        log_odds -= 0.15

    # --- Historical win-rate as graduated evidence (not just a gate) ---
    if wr_prior is not None:
        log_odds += max(-0.6, min(0.6, (wr_prior - 0.5) * 1.8))
        notes.append(f"Historical win-rate {wr_prior * 100:.0f}% factored into confidence")

    # --- 1H trigger quality ---
    log_odds += min(0.5, trig["body_atr"] * 0.35)
    if trig["displacement"]:
        log_odds += 0.35
        notes.append("1H displacement candle confirms order-flow shift")
    rsi_val = trig["rsi"]
    if direction == "long" and 35 <= rsi_val <= 68:
        log_odds += 0.2
    elif direction == "short" and 32 <= rsi_val <= 65:
        log_odds += 0.2
    elif (direction == "long" and rsi_val > 78) or (direction == "short" and rsi_val < 22):
        log_odds -= 0.4
        notes.append("Momentum already stretched on 1H — chasing risk elevated")

    # --- Volatility regime ---
    if vol["regime"] == "low":
        log_odds -= 0.35
        notes.append("Volatility percentile low — reduced follow-through probability")
    elif vol["regime"] == "high":
        log_odds += 0.1

    # --- Funding / OI ---
    f_score, f_notes = funding_oi
    log_odds += f_score * 0.45
    notes.extend(f_notes)

    prob = _logistic(log_odds)
    confidence = max(0.0, min(100.0, prob * 100.0))
    confidence += regime_confidence_adjustment(daily, vol)
    confidence = max(0.0, min(100.0, confidence))
    return confidence, notes


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


# =====================================================================
# 10. GRADE ORDERING (used for win-rate suppression overrides)
# =====================================================================

_GRADE_ORDER = {"Standard": 0, "High Quality": 1, "Premium": 2, "Elite": 3}


def grade_at_least(grade: str, floor_grade: str) -> bool:
    return _GRADE_ORDER.get(grade, -1) >= _GRADE_ORDER.get(floor_grade, 99)


# =====================================================================
# 11. ENTRY ENGINE — REALISTIC EXECUTION SELECTION
# =====================================================================

@dataclass
class EntryPlan:
    entry_type: str
    entry: float
    rationale: str


def plan_entry(direction: str, setup: SetupCandidate, trig: dict, atr_val: float) -> EntryPlan:
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

    ema_ref = trig["ind"]["ema20"][-1]
    if not math.isnan(ema_ref):
        dist_atr = abs(cur - ema_ref) / atr_val if atr_val else 0.0
        if dist_atr <= 0.6:
            return EntryPlan("PULLBACK", ema_ref, "Shallow pullback to 20EMA, realistically fillable")

    return EntryPlan("MARKET", cur, "No nearby discounted zone — engaging at market to avoid a stale signal")


# =====================================================================
# 12. RISK ENGINE — STRUCTURE-JUSTIFIED SL / TP
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
# 13. MARKET FILTERS
# =====================================================================

def funding_oi_score(state: dict, symbol: str, direction: str, funding: float | None,
                      oi_usd: float | None) -> tuple[float, list[str]]:
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


def win_rate_prior(state: dict, symbol: str, direction: str) -> float | None:
    """Graduated evidence companion to win_rate_suppression: returns
    the realized win rate once >= WIN_RATE_MIN_SAMPLE resolved trades
    exist for this symbol/direction, else None (no opinion). This lets
    real historical edge move `score_confluence`'s confidence number
    directly rather than only acting as a binary post-hoc veto."""
    wr = compute_win_rates(state).get(f"{symbol}_{direction}")
    if wr and wr["n"] >= WIN_RATE_MIN_SAMPLE and wr["win_rate"] is not None:
        return wr["win_rate"]
    return None


# =====================================================================
# 14. CORRELATION DECLUSTERING
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
    """One signal per correlated cluster, full stop. Previously the
    dedup key was (cluster, direction), which allowed a long AND a
    short from two different symbols in the same correlated cluster to
    both fire in one scan — a looser risk posture that sat at odds
    with the whole point of correlation declustering. Tightened here
    to match the stricter one-signal-per-cluster policy."""
    sym_to_cluster = {}
    for c in clusters:
        for s in c:
            sym_to_cluster[s] = frozenset(c)

    best_per_cluster: dict[frozenset, tuple] = {}
    for sym, direction, payload in signals:
        key = sym_to_cluster.get(sym, frozenset({sym}))
        conf = payload["confidence"]
        if key not in best_per_cluster or conf > best_per_cluster[key][2]["confidence"]:
            best_per_cluster[key] = (sym, direction, payload)
    return list(best_per_cluster.values())


# =====================================================================
# 15. SIGNAL ASSEMBLY
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
    ind_1h = cached_indicators(f"{symbol}_1h", candles_1h)
    ind_4h = cached_indicators(f"{symbol}_4h", candles_4h)
    ind_1d = cached_indicators(f"{symbol}_1d", candles_1d)

    daily = daily_trend_quality(candles_1d, ind_1d)
    vol = classify_volatility(state, symbol, candles_4h, ind_4h)

    ctx = get_market_ctx(symbol) or {}
    funding, oi_usd = ctx.get("funding"), ctx.get("oi_usd")
    if funding is not None:
        update_history(state, "funding_history", symbol, funding, FUNDING_HISTORY_DEPTH)
    if oi_usd is not None:
        update_history(state, "oi_history", symbol, oi_usd, OI_HISTORY_DEPTH)

    ok, reason = passes_hard_filters(symbol, oi_usd, vol["atr_pct"])
    if not ok:
        print(f"  [FILTER] {hl_coin(symbol)} — {reason}")
        return []

    setup = find_setup_candidate(candles_4h, ind_4h)
    if setup is None:
        return []

    trig = momentum_trigger(candles_1h, ind_1h, setup.direction)
    if trig is None:
        return []

    direction = setup.direction
    atr_val = vol["atr_val"]
    entry_plan = plan_entry(direction, setup, trig, atr_val)
    risk_plan = plan_risk(direction, entry_plan.entry, setup, atr_val, setup.liq, vol)

    if risk_plan.rr1 < MIN_RR:
        return []

    f_score_notes = funding_oi_score(state, symbol, direction, funding, oi_usd)
    wr_prior = win_rate_prior(state, symbol, direction)
    confidence, notes = score_confluence(direction, setup, trig, daily, vol, f_score_notes, wr_prior)
    grade = grade_for_confidence(confidence)
    if grade is None:
        return []

    suppressed = win_rate_suppression(state, symbol, direction)
    if suppressed and not grade_at_least(grade, WIN_RATE_SUPPRESSION_MIN_GRADE):
        print(f"  [WR SUPPRESS] {hl_coin(symbol)} {direction.upper()} — "
              f"grade {grade} below {WIN_RATE_SUPPRESSION_MIN_GRADE} floor required while suppressed")
        return []
    if suppressed:
        print(f"  [WR SUPPRESS OVERRIDE] {hl_coin(symbol)} {direction.upper()} — "
              f"{grade} clears suppression floor, allowing through")

    signal = Signal(symbol, direction, setup.setup_type, grade, confidence, entry_plan,
                     risk_plan, daily, setup, trig, vol, vol["atr_pct"], funding, notes)
    return [signal]


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
# 16. TELEGRAM
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
# 17. SCAN ORCHESTRATION
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
