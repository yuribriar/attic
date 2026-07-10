"""
APEX Adaptive Signal Engine
Version: v1.0.0

Institutional-grade Smart Money Concept signal engine built as an
Adaptive Multi-Engine Ensemble. Runs as a stateless scan-per-run
process (GitHub Actions + cron-job.org, every 15 minutes) against the
Hyperliquid API, persists state to state.json, and publishes signals
and lifecycle updates to Telegram.

Architecture
------------
1. Hyperliquid API layer      - rate-limited, retried, cached candles
2. Indicator library          - EMA/RSI/ATR/ADX/Bollinger/OBV/VWAP
3. Market structure engine    - swings, BOS/CHoCH, trend state
4. SMC zone engine            - order blocks, breaker blocks, FVGs,
                                 liquidity pools/sweeps, premium/discount
5. Regime engine              - trend/volatility/session/breadth/BTC beta
6. Specialized signal engines - 13 independent strategy pathways
7. Decision engine            - adaptive-weighted ensemble scoring,
                                 confidence, expected value, ranking
8. Risk engine                - structure-based adaptive SL/TP
9. Learning engine            - per-engine performance tracking and
                                 adaptive weight / confidence calibration
10. Telegram layer            - formatting, lifecycle updates, daily
                                 summary
11. Orchestration              - scan loop, state persistence, GitHub
                                 Actions safe entrypoint

Dependencies: requests (stdlib otherwise).
"""

from __future__ import annotations

import json
import logging
import math
import os
import random
import signal as os_signal
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

# --------------------------------------------------------------------------
# Identity
# --------------------------------------------------------------------------

ENGINE_NAME = "APEX Adaptive Signal Engine"
ENGINE_VERSION = "v1.0.0"
ENGINE_TAG = f"{ENGINE_NAME} {ENGINE_VERSION}"

# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("apex")

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

def _env_list(name: str, default: list[str]) -> list[str]:
    raw = os.environ.get(name, "")
    if not raw.strip():
        return default
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


HL_API_URL = os.environ.get("HYPERLIQUID_API_URL", "https://api.hyperliquid.xyz/info")

TELEGRAM_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TG_CHAT_ID", "")

WATCHLIST = _env_list(
    "WATCHLIST",
    [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR",
    "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT",
    "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
    ],
)



BASE_ASSET = "BTC"  # used for market regime / correlation beta

# Timeframes. Execution never below 15m per mandate. The AI-selected
# optimal combination: 15m execution, 1h confirmation, 4h HTF bias,
# 1D macro context.
TF_EXEC = "15m"
TF_MID = "1h"
TF_HTF = "4h"
TF_MACRO = "1d"
TIMEFRAMES = [TF_EXEC, TF_MID, TF_HTF, TF_MACRO]
TF_MINUTES = {"15m": 15, "30m": 30, "1h": 60, "2h": 120, "4h": 240,
              "8h": 480, "12h": 720, "1d": 1440}

CANDLES_PER_TF = {TF_EXEC: 300, TF_MID: 300, TF_HTF: 300, TF_MACRO: 200}

STATE_PATH = Path(os.environ.get("STATE_PATH", "state.json"))
CANDLE_CACHE_PATH = Path(os.environ.get("CANDLE_CACHE_PATH", "candle_cache.json"))

STATE_VERSION = 1

# Scan cadence (informational; the actual scheduler is cron-job.org).
SCAN_INTERVAL_MINUTES = 15

# Signal cadence / governor targets.
TARGET_SIGNALS_PER_DAY_MIN = 5
TARGET_SIGNALS_PER_DAY_MAX = 10
MAX_SIGNALS_PER_SCAN = 2
MAX_CONCURRENT_ACTIVE_SIGNALS = 12

# Risk defaults.
MIN_RR_FLOOR = 1.5
DEFAULT_RR_TP1 = 1.5
DEFAULT_RR_TP2 = 3.0
MAX_RR_TP2 = 6.0
ATR_SL_BUFFER_MULT = 0.35
COOLDOWN_BARS = 6  # in execution-timeframe bars
DUPLICATE_ENTRY_TOL_PCT = 0.0025
SIGNAL_EXPIRY_BARS = 16  # cancel unactivated pending signals after N exec bars

# Indicator lengths.
RSI_LEN = 14
ATR_LEN = 14
ADX_LEN = 14
BB_LEN = 20
BB_MULT = 2.0
EMA_FAST = 21
EMA_SLOW = 50
EMA_MACRO = 200

# Correlation / breadth.
CORRELATION_CLUSTER_THRESHOLD = 0.75
CORRELATION_LOOKBACK_BARS = 60

RUN_DEADLINE_SECONDS = _env_int("RUN_DEADLINE_SECONDS", 480)
_RUN_START = time.monotonic()


def time_budget_exceeded() -> bool:
    return (time.monotonic() - _RUN_START) > RUN_DEADLINE_SECONDS


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def utcnow_ms() -> int:
    return int(utcnow().timestamp() * 1000)


# --------------------------------------------------------------------------
# Safe math helpers
# --------------------------------------------------------------------------

def safe(v, fb: float = 0.0) -> float:
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return fb
        return f
    except (TypeError, ValueError):
        return fb


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    if not b:
        return default
    try:
        return a / b
    except ZeroDivisionError:
        return default


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def pct_change(a: float, b: float) -> float:
    return safe_div(b - a, a, 0.0)


# --------------------------------------------------------------------------
# Hyperliquid API layer
# --------------------------------------------------------------------------

class _WeightRateLimiter:
    """Token-bucket limiter honoring Hyperliquid's per-minute weight budget."""

    def __init__(self, budget_per_minute: float):
        self.budget = budget_per_minute
        self.used = 0.0
        self.window_start = time.monotonic()

    def wait(self, weight: float = 20.0):
        now = time.monotonic()
        elapsed = now - self.window_start
        if elapsed >= 60.0:
            self.window_start = now
            self.used = 0.0
            elapsed = 0.0
        if self.used + weight > self.budget:
            sleep_for = max(0.0, 60.0 - elapsed) + 0.05
            time.sleep(min(sleep_for, 12.0))
            self.window_start = time.monotonic()
            self.used = 0.0
        self.used += weight


_RATE_LIMITER = _WeightRateLimiter(budget_per_minute=1150.0)
_SESSION = requests.Session()


def hl_coin(symbol: str) -> str:
    return symbol.upper().replace("-PERP", "").replace("PERP", "").strip()


def hl_post(payload: dict, retries: int = 4, timeout: int = 12) -> dict | list | None:
    weight = 40.0 if payload.get("type") == "candleSnapshot" else 20.0
    backoff = 0.6
    for attempt in range(retries):
        try:
            _RATE_LIMITER.wait(weight)
            resp = _SESSION.post(HL_API_URL, json=payload, timeout=timeout)
            if resp.status_code == 429:
                time.sleep(backoff + random.random() * 0.4)
                backoff = min(backoff * 2, 8.0)
                continue
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            if attempt == retries - 1:
                log.warning("hl_post failed after %d attempts: %s", retries, exc)
                return None
            time.sleep(backoff + random.random() * 0.4)
            backoff = min(backoff * 2, 8.0)
    return None


def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    step = TF_MINUTES[interval] * 60_000
    return (reference_ms // step) * step


def filter_closed_candles(candles: list[dict], interval: str, reference_ms: int) -> list[dict]:
    bar_open = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if safe(c.get("t")) < bar_open]


def _request_candles(symbol: str, interval: str, start_ms: int, end_ms: int) -> list[dict]:
    payload = {
        "type": "candleSnapshot",
        "req": {"coin": hl_coin(symbol), "interval": interval, "startTime": start_ms, "endTime": end_ms},
    }
    data = hl_post(payload)
    if not isinstance(data, list):
        return []
    out = []
    for c in data:
        try:
            out.append({
                "t": int(c["t"]), "o": safe(c["o"]), "h": safe(c["h"]),
                "l": safe(c["l"]), "c": safe(c["c"]), "v": safe(c.get("v", 0)),
            })
        except (KeyError, TypeError, ValueError):
            continue
    out.sort(key=lambda x: x["t"])
    return out


def get_candles(symbol: str, interval: str, n: int, reference_ms: Optional[int] = None,
                 cache: Optional[dict] = None) -> list[dict]:
    reference_ms = reference_ms or utcnow_ms()
    step_ms = TF_MINUTES[interval] * 60_000
    cache_key = f"{hl_coin(symbol)}:{interval}"

    cached = (cache or {}).get(cache_key, {}).get("candles", []) if cache is not None else []
    if cached:
        last_t = cached[-1]["t"]
        gap_bars = (reference_ms - last_t) // step_ms
        if gap_bars <= 1 and len(cached) >= n:
            return filter_closed_candles(cached, interval, reference_ms)[-n:]
        start_ms = last_t + step_ms
    else:
        start_ms = reference_ms - (n + 5) * step_ms

    fresh = _request_candles(symbol, interval, start_ms, reference_ms)
    merged = {c["t"]: c for c in cached}
    for c in fresh:
        merged[c["t"]] = c
    all_sorted = sorted(merged.values(), key=lambda x: x["t"])
    all_sorted = all_sorted[-(n + 10):]

    if cache is not None:
        cache[cache_key] = {"candles": all_sorted, "updated_ms": reference_ms}

    return filter_closed_candles(all_sorted, interval, reference_ms)[-n:]


def fetch_all_candles(symbol: str, cache: Optional[dict] = None,
                       reference_ms: Optional[int] = None) -> Optional[dict[str, list[dict]]]:
    reference_ms = reference_ms or utcnow_ms()
    bundle = {}
    for tf in TIMEFRAMES:
        n = CANDLES_PER_TF[tf]
        candles = get_candles(symbol, tf, n, reference_ms, cache)
        if len(candles) < min(60, n // 2):
            log.debug("insufficient candles for %s %s (%d)", symbol, tf, len(candles))
            return None
        bundle[tf] = candles
    return bundle


_META_CACHE: dict = {"data": None, "ts": 0.0}


def get_meta_and_ctx() -> Optional[tuple[list[str], list[dict]]]:
    if _META_CACHE["data"] is not None and (time.monotonic() - _META_CACHE["ts"]) < 60:
        return _META_CACHE["data"]
    data = hl_post({"type": "metaAndAssetCtxs"})
    if not isinstance(data, list) or len(data) < 2:
        return None
    try:
        universe = [a["name"].upper() for a in data[0]["universe"]]
        ctxs = data[1]
    except (KeyError, TypeError, IndexError):
        return None
    result = (universe, ctxs)
    _META_CACHE["data"] = result
    _META_CACHE["ts"] = time.monotonic()
    return result


def get_market_snapshot() -> dict[str, dict]:
    meta = get_meta_and_ctx()
    if not meta:
        return {}
    universe, ctxs = meta
    snapshot = {}
    for name, ctx in zip(universe, ctxs):
        try:
            snapshot[name] = {
                "mark_px": safe(ctx.get("markPx")),
                "mid_px": safe(ctx.get("midPx", ctx.get("markPx"))),
                "funding": safe(ctx.get("funding")),
                "open_interest": safe(ctx.get("openInterest")),
                "day_volume": safe(ctx.get("dayNtlVlm")),
            }
        except (TypeError, AttributeError):
            continue
    return snapshot


def get_l2_spread_pct(symbol: str) -> Optional[float]:
    data = hl_post({"type": "l2Book", "coin": hl_coin(symbol)})
    if not isinstance(data, dict):
        return None
    try:
        levels = data["levels"]
        best_bid = safe(levels[0][0]["px"])
        best_ask = safe(levels[1][0]["px"])
        if best_bid <= 0 or best_ask <= 0:
            return None
        mid = (best_bid + best_ask) / 2.0
        return safe_div(best_ask - best_bid, mid, 0.0)
    except (KeyError, IndexError, TypeError):
        return None


# --------------------------------------------------------------------------
# Indicator library
# --------------------------------------------------------------------------

def ema(vals: list[float], period: int) -> list[float]:
    if not vals:
        return []
    k = 2.0 / (period + 1.0)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def sma(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        window = vals[max(0, i - period + 1):i + 1]
        out.append(sum(window) / len(window))
    return out


def stdev(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        window = vals[max(0, i - period + 1):i + 1]
        out.append(statistics.pstdev(window) if len(window) > 1 else 0.0)
    return out


def rsi(closes: list[float], period: int = RSI_LEN) -> list[float]:
    if len(closes) < 2:
        return [50.0] * len(closes)
    gains = [0.0]
    losses = [0.0]
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))
    avg_gain = sum(gains[1:period + 1]) / period if len(gains) > period else safe_div(sum(gains), len(gains))
    avg_loss = sum(losses[1:period + 1]) / period if len(losses) > period else safe_div(sum(losses), len(losses))
    out = [50.0] * min(period, len(closes))
    for i in range(period, len(closes)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs = safe_div(avg_gain, avg_loss, default=100.0) if avg_loss > 0 else 100.0
        out.append(100.0 - safe_div(100.0, 1.0 + rs, 0.0) if avg_loss > 0 else 100.0)
    while len(out) < len(closes):
        out.append(out[-1] if out else 50.0)
    return out[:len(closes)]


def atr_series(candles: list[dict], period: int = ATR_LEN) -> list[float]:
    if not candles:
        return []
    trs = []
    prev_close = candles[0]["c"]
    for c in candles:
        tr = max(c["h"] - c["l"], abs(c["h"] - prev_close), abs(c["l"] - prev_close))
        trs.append(tr)
        prev_close = c["c"]
    out = [trs[0]]
    for i in range(1, len(trs)):
        if i < period:
            out.append(sum(trs[:i + 1]) / (i + 1))
        else:
            out.append((out[-1] * (period - 1) + trs[i]) / period)
    return out


def adx_dmi(candles: list[dict], period: int = ADX_LEN) -> tuple[list[float], list[float], list[float]]:
    n = len(candles)
    if n < 2:
        return [0.0] * n, [0.0] * n, [0.0] * n
    plus_dm = [0.0]
    minus_dm = [0.0]
    trs = [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, n):
        up = candles[i]["h"] - candles[i - 1]["h"]
        down = candles[i - 1]["l"] - candles[i]["l"]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        tr = max(candles[i]["h"] - candles[i]["l"],
                  abs(candles[i]["h"] - candles[i - 1]["c"]),
                  abs(candles[i]["l"] - candles[i - 1]["c"]))
        trs.append(tr)

    def wilder(series: list[float]) -> list[float]:
        out = [series[0]]
        for i in range(1, len(series)):
            if i < period:
                out.append(sum(series[:i + 1]))
            else:
                out.append(out[-1] - out[-1] / period + series[i])
        return out

    sm_tr = wilder(trs)
    sm_plus = wilder(plus_dm)
    sm_minus = wilder(minus_dm)
    plus_di = [safe_div(sm_plus[i] * 100.0, sm_tr[i]) for i in range(n)]
    minus_di = [safe_div(sm_minus[i] * 100.0, sm_tr[i]) for i in range(n)]
    dx = [safe_div(abs(plus_di[i] - minus_di[i]) * 100.0, plus_di[i] + minus_di[i]) for i in range(n)]
    adx = sma(dx, period)
    return adx, plus_di, minus_di


def bollinger_width_pct(closes: list[float], period: int = BB_LEN, mult: float = BB_MULT) -> list[float]:
    mid = sma(closes, period)
    sd = stdev(closes, period)
    return [safe_div((sd[i] * mult * 2.0), mid[i]) for i in range(len(closes))]


def obv(closes: list[float], volumes: list[float]) -> list[float]:
    out = [0.0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            out.append(out[-1] + volumes[i])
        elif closes[i] < closes[i - 1]:
            out.append(out[-1] - volumes[i])
        else:
            out.append(out[-1])
    return out


def session_vwap(candles: list[dict], bars_per_session: int = 96) -> float:
    window = candles[-bars_per_session:] if len(candles) >= bars_per_session else candles
    num = sum(((c["h"] + c["l"] + c["c"]) / 3.0) * c["v"] for c in window)
    den = sum(c["v"] for c in window)
    return safe_div(num, den, default=window[-1]["c"] if window else 0.0)


def percentile_rank(vals: list[float], x: float) -> float:
    if not vals:
        return 50.0
    below = sum(1 for v in vals if v <= x)
    return 100.0 * below / len(vals)


def compute_returns(candles: list[dict], lookback: int) -> list[float]:
    closes = [c["c"] for c in candles[-lookback - 1:]]
    return [pct_change(closes[i - 1], closes[i]) for i in range(1, len(closes))]


def pearson(a: list[float], b: list[float]) -> float:
    n = min(len(a), len(b))
    if n < 5:
        return 0.0
    a, b = a[-n:], b[-n:]
    try:
        return statistics.correlation(a, b)
    except (statistics.StatisticsError, ValueError):
        return 0.0


def compute_indicators(candles: list[dict]) -> dict:
    closes = [c["c"] for c in candles]
    volumes = [c["v"] for c in candles]
    ind = {
        "closes": closes,
        "ema_fast": ema(closes, EMA_FAST),
        "ema_slow": ema(closes, EMA_SLOW),
        "ema_macro": ema(closes, EMA_MACRO),
        "rsi": rsi(closes, RSI_LEN),
        "atr": atr_series(candles, ATR_LEN),
        "bb_width_pct": bollinger_width_pct(closes, BB_LEN, BB_MULT),
        "obv": obv(closes, volumes),
        "vol_sma20": sma(volumes, 20),
    }
    adx, plus_di, minus_di = adx_dmi(candles, ADX_LEN)
    ind["adx"], ind["plus_di"], ind["minus_di"] = adx, plus_di, minus_di
    ind["vwap"] = session_vwap(candles)
    return ind


# --------------------------------------------------------------------------
# Market structure engine (swings, BOS / CHoCH)
# --------------------------------------------------------------------------

@dataclass
class Swing:
    index: int
    price: float
    kind: str  # "high" | "low"


def find_swings(candles: list[dict], left: int = 2, right: int = 2) -> list[Swing]:
    swings = []
    n = len(candles)
    for i in range(left, n - right):
        window_h = [candles[j]["h"] for j in range(i - left, i + right + 1)]
        window_l = [candles[j]["l"] for j in range(i - left, i + right + 1)]
        if candles[i]["h"] == max(window_h):
            swings.append(Swing(i, candles[i]["h"], "high"))
        if candles[i]["l"] == min(window_l):
            swings.append(Swing(i, candles[i]["l"], "low"))
    return swings


@dataclass
class StructureState:
    trend: str = "neutral"  # bullish | bearish | neutral
    last_bos_index: Optional[int] = None
    last_choch_index: Optional[int] = None
    last_choch_direction: Optional[str] = None
    last_swing_high: Optional[float] = None
    last_swing_low: Optional[float] = None
    higher_high: bool = False
    higher_low: bool = False
    lower_high: bool = False
    lower_low: bool = False


def analyze_structure(candles: list[dict], swings: list[Swing]) -> StructureState:
    st = StructureState()
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    if not highs or not lows:
        return st

    st.last_swing_high = highs[-1].price
    st.last_swing_low = lows[-1].price
    if len(highs) >= 2:
        st.higher_high = highs[-1].price > highs[-2].price
        st.lower_high = highs[-1].price < highs[-2].price
    if len(lows) >= 2:
        st.higher_low = lows[-1].price > lows[-2].price
        st.lower_low = lows[-1].price < lows[-2].price

    trend = "neutral"
    if st.higher_high and st.higher_low:
        trend = "bullish"
    elif st.lower_high and st.lower_low:
        trend = "bearish"
    st.trend = trend

    ordered = sorted(swings, key=lambda s: s.index)
    closes = [c["c"] for c in candles]
    running_bias = trend
    for k in range(2, len(ordered)):
        cur = ordered[k]
        idx = cur.index
        if idx >= len(closes):
            continue
        close_now = closes[idx]
        prior_highs = [s.price for s in ordered[:k] if s.kind == "high"]
        prior_lows = [s.price for s in ordered[:k] if s.kind == "low"]
        if prior_highs and close_now > max(prior_highs):
            if running_bias != "bullish":
                st.last_choch_index, st.last_choch_direction = idx, "bullish"
                running_bias = "bullish"
            else:
                st.last_bos_index = idx
        elif prior_lows and close_now < min(prior_lows):
            if running_bias != "bearish":
                st.last_choch_index, st.last_choch_direction = idx, "bearish"
                running_bias = "bearish"
            else:
                st.last_bos_index = idx
    return st


def price_zone(price: float, structure: StructureState) -> str:
    if structure.last_swing_high is None or structure.last_swing_low is None:
        return "unknown"
    rng = structure.last_swing_high - structure.last_swing_low
    if rng <= 0:
        return "unknown"
    pos = safe_div(price - structure.last_swing_low, rng)
    if pos >= 0.66:
        return "premium"
    if pos <= 0.33:
        return "discount"
    return "equilibrium"


# --------------------------------------------------------------------------
# SMC zone engine: order blocks, breaker blocks, FVGs
# --------------------------------------------------------------------------

@dataclass
class Zone:
    kind: str          # "order_block" | "breaker" | "fvg"
    direction: str      # "bullish" | "bearish"
    low: float
    high: float
    index: int
    mitigated: bool = False
    quality: float = 1.0

    @property
    def mid(self) -> float:
        return (self.low + self.high) / 2.0

    def contains(self, price: float, buf: float = 0.0) -> bool:
        return (self.low - buf) <= price <= (self.high + buf)


def _avg_volume(candles: list[dict], idx: int, window: int = 20) -> float:
    lo = max(0, idx - window)
    vols = [c["v"] for c in candles[lo:idx]]
    return safe_div(sum(vols), len(vols)) if vols else 0.0


def find_order_blocks(candles: list[dict], atr_vals: list[float], lookback: int = 80) -> list[Zone]:
    zones = []
    n = len(candles)
    start = max(1, n - lookback)
    for i in range(start, n - 1):
        atr_i = atr_vals[i] if i < len(atr_vals) else 0.0
        if atr_i <= 0:
            continue
        c = candles[i]
        body = abs(c["c"] - c["o"])
        avg_vol = _avg_volume(candles, i)

        is_bear_candle = c["c"] < c["o"]
        is_bull_candle = c["c"] > c["o"]
        impulsive_up = (candles[i + 1]["c"] - c["h"]) > 0.5 * atr_i
        impulsive_down = (c["l"] - candles[i + 1]["c"]) > 0.5 * atr_i

        if is_bear_candle and impulsive_up:
            quality = clamp(safe_div(body, atr_i) + safe_div(c["v"], avg_vol + 1e-9) * 0.3, 0.1, 3.0)
            zones.append(Zone("order_block", "bullish", c["l"], c["o"], i, quality=quality))
        if is_bull_candle and impulsive_down:
            quality = clamp(safe_div(body, atr_i) + safe_div(c["v"], avg_vol + 1e-9) * 0.3, 0.1, 3.0)
            zones.append(Zone("order_block", "bearish", c["c"], c["h"], i, quality=quality))
    return zones


def find_fvgs(candles: list[dict], atr_vals: list[float], lookback: int = 80) -> list[Zone]:
    zones = []
    n = len(candles)
    start = max(2, n - lookback)
    for i in range(start, n):
        a, c = candles[i - 2], candles[i]
        atr_i = atr_vals[i] if i < len(atr_vals) else 0.0
        if atr_i <= 0:
            continue
        if a["h"] < c["l"] and (c["l"] - a["h"]) > 0.1 * atr_i:
            zones.append(Zone("fvg", "bullish", a["h"], c["l"], i - 1,
                               quality=clamp(safe_div(c["l"] - a["h"], atr_i), 0.1, 3.0)))
        if a["l"] > c["h"] and (a["l"] - c["h"]) > 0.1 * atr_i:
            zones.append(Zone("fvg", "bearish", c["h"], a["l"], i - 1,
                               quality=clamp(safe_div(a["l"] - c["h"], atr_i), 0.1, 3.0)))
    return zones


def mark_mitigation_and_breakers(zones: list[Zone], candles: list[dict]) -> list[Zone]:
    for z in zones:
        for j in range(z.index + 1, len(candles)):
            c = candles[j]
            if z.direction == "bullish" and c["l"] <= z.low:
                z.mitigated = True
                break
            if z.direction == "bearish" and c["h"] >= z.high:
                z.mitigated = True
                break
    return zones


def derive_breaker_blocks(order_blocks: list[Zone], candles: list[dict]) -> list[Zone]:
    """A mitigated (violated) order block that price closed through becomes a
    breaker block in the opposite direction."""
    breakers = []
    for ob in order_blocks:
        if not ob.mitigated:
            continue
        for j in range(ob.index + 1, len(candles)):
            c = candles[j]
            if ob.direction == "bullish" and c["c"] < ob.low:
                breakers.append(Zone("breaker", "bearish", ob.low, ob.high, j, quality=ob.quality * 0.9))
                break
            if ob.direction == "bearish" and c["c"] > ob.high:
                breakers.append(Zone("breaker", "bullish", ob.low, ob.high, j, quality=ob.quality * 0.9))
                break
    return breakers


def zone_quality(z: Zone) -> float:
    return z.quality * (0.6 if z.mitigated else 1.0)


# --------------------------------------------------------------------------
# Liquidity engine: pools, sweeps, MSS confirmation
# --------------------------------------------------------------------------

def cluster_levels(levels: list[float], tol_pct: float = 0.0015) -> list[tuple[float, int]]:
    if not levels:
        return []
    levels_sorted = sorted(levels)
    clusters = [[levels_sorted[0]]]
    for lv in levels_sorted[1:]:
        if safe_div(lv - clusters[-1][-1], clusters[-1][-1]) <= tol_pct:
            clusters[-1].append(lv)
        else:
            clusters.append([lv])
    return [(sum(cl) / len(cl), len(cl)) for cl in clusters]


def build_liquidity_pools(swings: list[Swing]) -> dict:
    highs = [s.price for s in swings if s.kind == "high"]
    lows = [s.price for s in swings if s.kind == "low"]
    return {
        "buy_side": sorted(cluster_levels(highs), key=lambda x: -x[1]),
        "sell_side": sorted(cluster_levels(lows), key=lambda x: -x[1]),
    }


def detect_sweep(candles: list[dict], pools: dict, direction: str, lookback: int = 10) -> Optional[dict]:
    """A sweep is a wick beyond a liquidity pool that closes back inside it,
    signalling a stop-hunt before reversal / continuation."""
    recent = candles[-lookback:]
    targets = pools["sell_side"] if direction == "bullish" else pools["buy_side"]
    if not targets:
        return None
    for i, c in enumerate(recent):
        for level, weight in targets:
            if direction == "bullish" and c["l"] < level and c["c"] > level:
                return {"level": level, "weight": weight, "index": len(candles) - lookback + i, "wick": level - c["l"]}
            if direction == "bearish" and c["h"] > level and c["c"] < level:
                return {"level": level, "weight": weight, "index": len(candles) - lookback + i, "wick": c["h"] - level}
    return None


def premium_discount_zone(candles: list[dict], lookback: int = 50) -> dict:
    window = candles[-lookback:]
    hi = max(c["h"] for c in window)
    lo = min(c["l"] for c in window)
    eq = (hi + lo) / 2.0
    return {"high": hi, "low": lo, "eq": eq}


@dataclass
class MSSEvent:
    direction: str
    swing_price: float
    confirm_index: int


def detect_mss(candles: list[dict], direction: str, after_index: int, lookback: int = 40) -> Optional[MSSEvent]:
    """Market Structure Shift: after a liquidity sweep, look for a
    displacement close beyond the most recent opposing swing."""
    start = max(0, after_index - lookback)
    segment = candles[start:after_index + 1]
    if len(segment) < 5:
        return None
    swings = find_swings(segment, left=1, right=1)
    for i in range(after_index - start, len(segment)):
        if i >= len(segment):
            break
        c = segment[i]
        if direction == "bullish":
            opp_highs = [s.price for s in swings if s.kind == "high" and s.index < i]
            if opp_highs and c["c"] > max(opp_highs):
                return MSSEvent("bullish", max(opp_highs), start + i)
        else:
            opp_lows = [s.price for s in swings if s.kind == "low" and s.index < i]
            if opp_lows and c["c"] < min(opp_lows):
                return MSSEvent("bearish", min(opp_lows), start + i)
    return None


def clip_tp_to_liquidity(entry: float, tp: float, direction: str, pools: dict,
                          min_rr: float, risk: float) -> float:
    targets = pools["buy_side"] if direction == "bullish" else pools["sell_side"]
    candidates = [lvl for lvl, _ in targets]
    if direction == "bullish":
        ahead = [lvl for lvl in candidates if entry < lvl < tp]
        if ahead:
            best = min(ahead)
            if safe_div(best - entry, risk) >= min_rr:
                return best
    else:
        ahead = [lvl for lvl in candidates if tp < lvl < entry]
        if ahead:
            best = max(ahead)
            if safe_div(entry - best, risk) >= min_rr:
                return best
    return tp


def room_to_next_opposing_level(entry: float, direction: str, zones: list[Zone],
                                 pools: dict) -> Optional[float]:
    opp = "bearish" if direction == "bullish" else "bullish"
    levels = [z.mid for z in zones if z.direction == opp]
    levels += [lvl for lvl, _ in (pools["buy_side"] if direction == "bullish" else pools["sell_side"])]
    if direction == "bullish":
        ahead = [lv for lv in levels if lv > entry]
        return min(ahead) if ahead else None
    ahead = [lv for lv in levels if lv < entry]
    return max(ahead) if ahead else None


# --------------------------------------------------------------------------
# Regime engine
# --------------------------------------------------------------------------

@dataclass
class RegimeVector:
    trend_strength: float = 0.0     # 0-1, from ADX
    volatility_pctile: float = 50.0  # 0-100, ATR% vs own history
    breadth: float = 0.5            # 0-1, fraction of watchlist trending with BTC
    session_weight: float = 1.0     # liquidity-session multiplier
    htf_bias: str = "neutral"
    choppiness: float = 0.5         # 0-1, higher = noisier / more chaotic
    label: str = "neutral"

    def composite_favorability(self) -> float:
        vol_fit = 1.0 - abs(self.volatility_pctile - 55.0) / 55.0
        return clamp(
            0.35 * self.trend_strength + 0.25 * clamp(vol_fit, 0.0, 1.0)
            + 0.2 * self.breadth + 0.1 * self.session_weight
            + 0.1 * (1.0 - self.choppiness),
            0.0, 1.0,
        )


def session_weight_now(reference_ms: Optional[int] = None) -> float:
    hour = utcnow().hour if reference_ms is None else datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc).hour
    # London/NY overlap and NY session carry the deepest liquidity.
    if 12 <= hour <= 20:
        return 1.15
    if 6 <= hour <= 12 or 20 <= hour <= 23:
        return 1.0
    return 0.8


def compute_choppiness(candles: list[dict], atr_vals: list[float], lookback: int = 30) -> float:
    window = candles[-lookback:]
    if len(window) < 5:
        return 0.5
    hi = max(c["h"] for c in window)
    lo = min(c["l"] for c in window)
    rng = hi - lo
    atr_sum = sum(atr_vals[-lookback:]) if len(atr_vals) >= lookback else sum(atr_vals)
    if rng <= 0 or atr_sum <= 0:
        return 0.5
    directional = safe_div(rng, atr_sum)
    return clamp(1.0 - directional, 0.0, 1.0)


def classify_regime(ind_htf: dict, ind_mid: dict, candles_mid: list[dict],
                     breadth: float = 0.5, reference_ms: Optional[int] = None) -> RegimeVector:
    rv = RegimeVector()
    adx_now = ind_htf["adx"][-1] if ind_htf["adx"] else 0.0
    rv.trend_strength = clamp(safe_div(adx_now, 40.0), 0.0, 1.0)

    atr_pct_series = [safe_div(a, c) for a, c in zip(ind_mid["atr"], ind_mid["closes"]) if c]
    if atr_pct_series:
        rv.volatility_pctile = percentile_rank(atr_pct_series[-200:], atr_pct_series[-1])

    rv.breadth = breadth
    rv.session_weight = session_weight_now(reference_ms)
    rv.choppiness = compute_choppiness(candles_mid, ind_mid["atr"])

    ema_f, ema_s = ind_htf["ema_fast"][-1], ind_htf["ema_slow"][-1]
    if ema_f > ema_s and adx_now >= 18:
        rv.htf_bias = "bullish"
    elif ema_f < ema_s and adx_now >= 18:
        rv.htf_bias = "bearish"
    else:
        rv.htf_bias = "neutral"

    if rv.trend_strength > 0.55 and rv.choppiness < 0.45:
        rv.label = "trending"
    elif rv.volatility_pctile > 80:
        rv.label = "expansion"
    elif rv.choppiness > 0.65 and rv.trend_strength < 0.3:
        rv.label = "ranging"
    else:
        rv.label = "neutral"
    return rv


def compute_btc_regime(btc_bundle: dict[str, list[dict]]) -> RegimeVector:
    ind_htf = compute_indicators(btc_bundle[TF_HTF])
    ind_mid = compute_indicators(btc_bundle[TF_MID])
    return classify_regime(ind_htf, ind_mid, btc_bundle[TF_MID])


def compute_breadth(bundles: dict[str, dict[str, list[dict]]], btc_bias: str) -> float:
    if btc_bias == "neutral" or not bundles:
        return 0.5
    aligned = 0
    total = 0
    for sym, bundle in bundles.items():
        closes = bundle[TF_MID][-1 * (EMA_SLOW + 5):]
        if len(closes) < EMA_SLOW:
            continue
        c = [x["c"] for x in closes]
        fast, slow = ema(c, EMA_FAST)[-1], ema(c, EMA_SLOW)[-1]
        total += 1
        if (fast > slow and btc_bias == "bullish") or (fast < slow and btc_bias == "bearish"):
            aligned += 1
    return safe_div(aligned, total, 0.5) if total else 0.5


def adaptive_thresholds(regime: RegimeVector, base_threshold: float) -> float:
    """Tighten filters in chaotic markets, relax in clean trending markets."""
    adj = base_threshold
    if regime.choppiness > 0.65:
        adj += 8.0
    if regime.volatility_pctile > 88 or regime.volatility_pctile < 12:
        adj += 6.0
    if regime.label == "trending" and regime.choppiness < 0.4:
        adj -= 5.0
    return clamp(adj, base_threshold - 10.0, base_threshold + 20.0)


# --------------------------------------------------------------------------
# Correlation clustering (avoid stacking correlated signals)
# --------------------------------------------------------------------------

def compute_pairwise_correlation(symbols: list[str], bundles: dict[str, dict]) -> dict:
    returns = {s: compute_returns(bundles[s][TF_MID], CORRELATION_LOOKBACK_BARS) for s in symbols if s in bundles}
    matrix = {}
    for i, a in enumerate(symbols):
        for b in symbols[i + 1:]:
            if a in returns and b in returns:
                matrix[(a, b)] = pearson(returns[a], returns[b])
    return matrix


def cluster_by_correlation(symbols: list[str], matrix: dict, threshold: float = CORRELATION_CLUSTER_THRESHOLD) -> list[set]:
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
        if corr >= threshold:
            union(a, b)

    groups: dict[str, set] = {}
    for s in symbols:
        groups.setdefault(find(s), set()).add(s)
    return list(groups.values())


# --------------------------------------------------------------------------
# Candidate + risk management
# --------------------------------------------------------------------------

@dataclass
class Candidate:
    symbol: str
    direction: str          # "long" | "short"
    engine: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    confluence: list[str] = field(default_factory=list)
    meta: dict = field(default_factory=dict)
    timeframe: str = TF_EXEC

    @property
    def risk(self) -> float:
        return abs(self.entry - self.sl)

    @property
    def rr1(self) -> float:
        return safe_div(abs(self.tp1 - self.entry), self.risk)

    @property
    def rr2(self) -> float:
        return safe_div(abs(self.tp2 - self.entry), self.risk)


def adaptive_sl_buffer(candles: list[dict], atr_val: float, vol_pctile: float) -> float:
    """Buffer beyond structure to survive common wick-based liquidity sweeps."""
    recent_wicks = []
    for c in candles[-14:]:
        body_top = max(c["o"], c["c"])
        body_bot = min(c["o"], c["c"])
        recent_wicks.append(max(c["h"] - body_top, body_bot - c["l"]))
    avg_wick = safe_div(sum(recent_wicks), len(recent_wicks)) if recent_wicks else atr_val * 0.2
    vol_mult = 1.0 + clamp((vol_pctile - 50.0) / 100.0, -0.3, 0.6)
    return max(avg_wick * 1.1, atr_val * ATR_SL_BUFFER_MULT) * vol_mult


def build_risk_plan(direction: str, entry: float, invalidation_price: float, atr_val: float,
                     candles: list[dict], vol_pctile: float, regime: RegimeVector,
                     pools: dict, min_rr: float = MIN_RR_FLOOR) -> Optional[dict]:
    """Structure-based, liquidity-aware SL/TP construction. SL/TP are always
    validated against candle highs/lows only (never synthetic offsets from
    close)."""
    buffer = adaptive_sl_buffer(candles, atr_val, vol_pctile)

    recent_highs = [c["h"] for c in candles[-40:]]
    recent_lows = [c["l"] for c in candles[-40:]]

    if direction == "long":
        sl = min(invalidation_price - buffer, min(recent_lows[-6:]) - buffer * 0.5)
        if sl >= entry:
            return None
        risk = entry - sl
    else:
        sl = max(invalidation_price + buffer, max(recent_highs[-6:]) + buffer * 0.5)
        if sl <= entry:
            return None
        risk = sl - entry

    if risk <= 0:
        return None

    rr_tp2_base = DEFAULT_RR_TP2
    if regime.label == "trending":
        rr_tp2_base += 1.0
    if regime.label == "ranging":
        rr_tp2_base -= 0.75
    if regime.volatility_pctile > 80:
        rr_tp2_base += 0.5
    rr_tp2_base = clamp(rr_tp2_base, MIN_RR_FLOOR + 0.5, MAX_RR_TP2)

    if direction == "long":
        tp1 = entry + risk * DEFAULT_RR_TP1
        tp2 = entry + risk * rr_tp2_base
        tp1 = clip_tp_to_liquidity(entry, tp1, "bullish", pools, DEFAULT_RR_TP1 * 0.85, risk)
        tp2 = clip_tp_to_liquidity(entry, tp2, "bullish", pools, min_rr, risk)
        tp2 = max(tp2, tp1 + risk * 0.5)
    else:
        tp1 = entry - risk * DEFAULT_RR_TP1
        tp2 = entry - risk * rr_tp2_base
        tp1 = clip_tp_to_liquidity(entry, tp1, "bearish", pools, DEFAULT_RR_TP1 * 0.85, risk)
        tp2 = clip_tp_to_liquidity(entry, tp2, "bearish", pools, min_rr, risk)
        tp2 = min(tp2, tp1 - risk * 0.5)

    rr2 = safe_div(abs(tp2 - entry), risk)
    if rr2 < min_rr:
        return None

    return {"sl": sl, "tp1": tp1, "tp2": tp2, "risk": risk, "buffer": buffer}


def clamp_entry_to_market(cand: Candidate, market_price: float, max_slippage_pct: float = 0.006) -> Optional[Candidate]:
    drift = abs(pct_change(cand.entry, market_price))
    if drift > max_slippage_pct:
        return None
    return cand


# --------------------------------------------------------------------------
# Market context (shared inputs for every specialized engine)
# --------------------------------------------------------------------------

@dataclass
class MarketContext:
    symbol: str
    candles: dict[str, list[dict]]
    ind: dict[str, dict]
    structure: dict[str, StructureState]
    order_blocks: dict[str, list[Zone]]
    breakers: dict[str, list[Zone]]
    fvgs: dict[str, list[Zone]]
    pools: dict
    pd_zone: dict
    regime: RegimeVector
    price: float
    atr_exec: float
    atr_mid: float


def build_market_context(symbol: str, bundle: dict[str, list[dict]], regime: RegimeVector) -> MarketContext:
    ind = {tf: compute_indicators(bundle[tf]) for tf in TIMEFRAMES}
    swings = {tf: find_swings(bundle[tf]) for tf in (TF_EXEC, TF_MID, TF_HTF)}
    structure = {tf: analyze_structure(bundle[tf], swings[tf]) for tf in (TF_EXEC, TF_MID, TF_HTF)}

    order_blocks, breakers, fvgs = {}, {}, {}
    for tf in (TF_EXEC, TF_MID):
        obs = find_order_blocks(bundle[tf], ind[tf]["atr"])
        obs = mark_mitigation_and_breakers(obs, bundle[tf])
        order_blocks[tf] = obs
        breakers[tf] = derive_breaker_blocks(obs, bundle[tf])
        fvg_zones = find_fvgs(bundle[tf], ind[tf]["atr"])
        fvgs[tf] = mark_mitigation_and_breakers(fvg_zones, bundle[tf])

    pools = build_liquidity_pools(swings[TF_MID] + swings[TF_HTF])
    pd_zone = premium_discount_zone(bundle[TF_HTF])

    return MarketContext(
        symbol=symbol, candles=bundle, ind=ind, structure=structure,
        order_blocks=order_blocks, breakers=breakers, fvgs=fvgs, pools=pools,
        pd_zone=pd_zone, regime=regime, price=bundle[TF_EXEC][-1]["c"],
        atr_exec=ind[TF_EXEC]["atr"][-1], atr_mid=ind[TF_MID]["atr"][-1],
    )


def _rr_plan_or_none(direction: str, entry: float, invalidation: float, ctx: MarketContext) -> Optional[dict]:
    return build_risk_plan(
        direction, entry, invalidation, ctx.atr_exec, ctx.candles[TF_EXEC],
        ctx.regime.volatility_pctile, ctx.regime, ctx.pools,
    )


def _fresh_zone(zones: list[Zone], direction: str, exec_price: float, max_age: int, cur_index: int) -> Optional[Zone]:
    dz = [z for z in zones if z.direction == direction and not z.mitigated and (cur_index - z.index) <= max_age]
    if not dz:
        return None
    dz.sort(key=lambda z: (-zone_quality(z), abs(z.mid - exec_price)))
    return dz[0]


# --------------------------------------------------------------------------
# Specialized signal engines (the ensemble)
# --------------------------------------------------------------------------

def engine_smc_confluence(ctx: MarketContext) -> Optional[Candidate]:
    bias = ctx.regime.htf_bias
    directions = [bias] if bias != "neutral" else ["bullish", "bearish"]
    for direction_zone in directions:
        sweep = detect_sweep(ctx.candles[TF_MID], ctx.pools, direction_zone, lookback=14)
        if not sweep:
            continue
        mss = detect_mss(ctx.candles[TF_EXEC], direction_zone, len(ctx.candles[TF_EXEC]) - 1, lookback=40)
        if not mss:
            continue
        pd = price_zone(ctx.price, ctx.structure[TF_HTF])
        direction = "long" if direction_zone == "bullish" else "short"
        if direction == "long" and pd == "premium":
            continue
        if direction == "short" and pd == "discount":
            continue
        sweep_candle = ctx.candles[TF_MID][sweep["index"]]
        invalidation = sweep_candle["l"] if direction == "long" else sweep_candle["h"]
        plan = _rr_plan_or_none(direction, ctx.price, invalidation, ctx)
        if not plan:
            continue
        conf = ["Liquidity sweep", "Market structure shift confirmed", f"{pd.title()} zone entry"]
        zones = ctx.order_blocks[TF_EXEC] + ctx.breakers[TF_EXEC] + ctx.fvgs[TF_EXEC]
        zone = _fresh_zone(zones, direction_zone, ctx.price, 30, len(ctx.candles[TF_EXEC]) - 1)
        if zone and zone.contains(ctx.price, ctx.atr_exec * 0.25):
            conf.append(f"{zone.kind.replace('_', ' ').title()} confluence")
        return Candidate(ctx.symbol, direction, "smc_confluence", ctx.price, plan["sl"], plan["tp1"], plan["tp2"],
                          confluence=conf, meta={"pd_zone": pd, "pool_weight": sweep["weight"]})
    return None


def engine_order_block(ctx: MarketContext) -> Optional[Candidate]:
    bias = ctx.regime.htf_bias
    if bias == "neutral":
        return None
    cur_idx = len(ctx.candles[TF_EXEC]) - 1
    zone = _fresh_zone(ctx.order_blocks[TF_EXEC], bias, ctx.price, 50, cur_idx)
    if zone is None or not zone.contains(ctx.price, ctx.atr_exec * 0.15):
        return None
    pd = price_zone(ctx.price, ctx.structure[TF_HTF])
    direction = "long" if bias == "bullish" else "short"
    if direction == "long" and pd == "premium":
        return None
    if direction == "short" and pd == "discount":
        return None
    invalidation = zone.low if direction == "long" else zone.high
    plan = _rr_plan_or_none(direction, ctx.price, invalidation, ctx)
    if not plan:
        return None
    return Candidate(ctx.symbol, direction, "order_block", ctx.price, plan["sl"], plan["tp1"], plan["tp2"],
                      confluence=["HTF bias aligned", f"Order block retest ({pd})"],
                      meta={"zone_quality": zone_quality(zone)})


def engine_breaker_block(ctx: MarketContext) -> Optional[Candidate]:
    bias = ctx.regime.htf_bias
    if bias == "neutral":
        return None
    cur_idx = len(ctx.candles[TF_EXEC]) - 1
    zone = _fresh_zone(ctx.breakers[TF_EXEC], bias, ctx.price, 50, cur_idx)
    if zone is None or not zone.contains(ctx.price, ctx.atr_exec * 0.15):
        return None
    direction = "long" if bias == "bullish" else "short"
    invalidation = zone.low if direction == "long" else zone.high
    plan = _rr_plan_or_none(direction, ctx.price, invalidation, ctx)
    if not plan:
        return None
    return Candidate(ctx.symbol, direction, "breaker_block", ctx.price, plan["sl"], plan["tp1"], plan["tp2"],
                      confluence=["Breaker block retest", "HTF bias aligned"],
                      meta={"zone_quality": zone_quality(zone)})


def engine_fair_value_gap(ctx: MarketContext) -> Optional[Candidate]:
    bias = ctx.regime.htf_bias
    if bias == "neutral":
        return None
    cur_idx = len(ctx.candles[TF_EXEC]) - 1
    zone = _fresh_zone(ctx.fvgs[TF_EXEC], bias, ctx.price, 40, cur_idx)
    if zone is None or not zone.contains(ctx.price, ctx.atr_exec * 0.1):
        return None
    pd = price_zone(ctx.price, ctx.structure[TF_HTF])
    direction = "long" if bias == "bullish" else "short"
    if direction == "long" and pd == "premium":
        return None
    if direction == "short" and pd == "discount":
        return None
    invalidation = zone.low if direction == "long" else zone.high
    plan = _rr_plan_or_none(direction, ctx.price, invalidation, ctx)
    if not plan:
        return None
    return Candidate(ctx.symbol, direction, "fair_value_gap", ctx.price, plan["sl"], plan["tp1"], plan["tp2"],
                      confluence=["FVG fill", "HTF bias aligned"], meta={"zone_quality": zone_quality(zone)})


def engine_liquidity_sweep(ctx: MarketContext) -> Optional[Candidate]:
    sweep_bull = detect_sweep(ctx.candles[TF_EXEC], ctx.pools, "bullish", lookback=10)
    sweep_bear = detect_sweep(ctx.candles[TF_EXEC], ctx.pools, "bearish", lookback=10)
    options = []
    if sweep_bull:
        invalidation = ctx.candles[TF_EXEC][sweep_bull["index"]]["l"]
        plan = _rr_plan_or_none("long", ctx.price, invalidation, ctx)
        if plan:
            options.append(Candidate(ctx.symbol, "long", "liquidity_sweep", ctx.price, plan["sl"], plan["tp1"],
                                      plan["tp2"], confluence=["Sell-side liquidity swept", "Wick rejection"],
                                      meta={"pool_weight": sweep_bull["weight"]}))
    if sweep_bear:
        invalidation = ctx.candles[TF_EXEC][sweep_bear["index"]]["h"]
        plan = _rr_plan_or_none("short", ctx.price, invalidation, ctx)
        if plan:
            options.append(Candidate(ctx.symbol, "short", "liquidity_sweep", ctx.price, plan["sl"], plan["tp1"],
                                      plan["tp2"], confluence=["Buy-side liquidity swept", "Wick rejection"],
                                      meta={"pool_weight": sweep_bear["weight"]}))
    if not options:
        return None
    options.sort(key=lambda c: -c.meta.get("pool_weight", 0))
    return options[0]


def engine_trend_continuation(ctx: MarketContext) -> Optional[Candidate]:
    bias = ctx.regime.htf_bias
    if bias == "neutral" or ctx.regime.label == "ranging":
        return None
    ind_exec = ctx.ind[TF_EXEC]
    ema_f, ema_s = ind_exec["ema_fast"][-1], ind_exec["ema_slow"][-1]
    aligned = (bias == "bullish" and ema_f > ema_s) or (bias == "bearish" and ema_f < ema_s)
    if not aligned or abs(ctx.price - ema_f) > ctx.atr_exec * 0.6:
        return None
    direction = "long" if bias == "bullish" else "short"
    recent = ctx.candles[TF_EXEC][-10:]
    invalidation = min(c["l"] for c in recent) if direction == "long" else max(c["h"] for c in recent)
    plan = _rr_plan_or_none(direction, ctx.price, invalidation, ctx)
    if not plan:
        return None
    return Candidate(ctx.symbol, direction, "trend_continuation", ctx.price, plan["sl"], plan["tp1"], plan["tp2"],
                      confluence=["EMA trend alignment", "Pullback to dynamic support/resistance"])


def engine_breakout(ctx: MarketContext) -> Optional[Candidate]:
    window = ctx.candles[TF_EXEC][-24:-1]
    if len(window) < 10:
        return None
    hi, lo = max(c["h"] for c in window), min(c["l"] for c in window)
    if safe_div(hi - lo, lo) > 0.05:
        return None
    last = ctx.candles[TF_EXEC][-1]
    vol_avg = ctx.ind[TF_EXEC]["vol_sma20"][-1]
    direction, invalidation = None, None
    if last["c"] > hi and last["v"] > vol_avg * 1.3:
        direction, invalidation = "long", lo
    elif last["c"] < lo and last["v"] > vol_avg * 1.3:
        direction, invalidation = "short", hi
    if not direction:
        return None
    plan = _rr_plan_or_none(direction, ctx.price, invalidation, ctx)
    if not plan:
        return None
    return Candidate(ctx.symbol, direction, "breakout", ctx.price, plan["sl"], plan["tp1"], plan["tp2"],
                      confluence=["Range breakout", "Volume expansion"])


def engine_pullback(ctx: MarketContext) -> Optional[Candidate]:
    st = ctx.structure[TF_MID]
    if st.trend == "neutral" or st.last_swing_high is None or st.last_swing_low is None:
        return None
    rng = st.last_swing_high - st.last_swing_low
    if rng <= 0:
        return None
    rsi_now = ctx.ind[TF_EXEC]["rsi"][-1]
    if st.trend == "bullish":
        direction = "long"
        band_lo, band_hi = st.last_swing_high - rng * 0.618, st.last_swing_high - rng * 0.5
        if not (band_lo <= ctx.price <= band_hi) or rsi_now > 55:
            return None
        invalidation = st.last_swing_low
    else:
        direction = "short"
        band_lo, band_hi = st.last_swing_low + rng * 0.5, st.last_swing_low + rng * 0.618
        if not (band_lo <= ctx.price <= band_hi) or rsi_now < 45:
            return None
        invalidation = st.last_swing_high
    plan = _rr_plan_or_none(direction, ctx.price, invalidation, ctx)
    if not plan:
        return None
    return Candidate(ctx.symbol, direction, "pullback", ctx.price, plan["sl"], plan["tp1"], plan["tp2"],
                      confluence=["Fibonacci 50-61.8% retracement", "Momentum reset"])


def engine_reversal(ctx: MarketContext) -> Optional[Candidate]:
    st = ctx.structure[TF_EXEC]
    if st.last_choch_index is None:
        return None
    cur_idx = len(ctx.candles[TF_EXEC]) - 1
    if cur_idx - st.last_choch_index > 6:
        return None
    pd = price_zone(ctx.price, ctx.structure[TF_HTF])
    direction = "long" if st.last_choch_direction == "bullish" else "short"
    if direction == "long" and pd != "discount":
        return None
    if direction == "short" and pd != "premium":
        return None
    rsi_now = ctx.ind[TF_EXEC]["rsi"][-1]
    if direction == "long" and rsi_now > 45:
        return None
    if direction == "short" and rsi_now < 55:
        return None
    recent = ctx.candles[TF_EXEC][-8:]
    invalidation = min(c["l"] for c in recent) if direction == "long" else max(c["h"] for c in recent)
    plan = _rr_plan_or_none(direction, ctx.price, invalidation, ctx)
    if not plan:
        return None
    return Candidate(ctx.symbol, direction, "reversal", ctx.price, plan["sl"], plan["tp1"], plan["tp2"],
                      confluence=["Fresh CHoCH", f"{pd.title()} zone", "Momentum extreme"])


def engine_momentum(ctx: MarketContext) -> Optional[Candidate]:
    ind = ctx.ind[TF_EXEC]
    if ind["adx"][-1] < 25:
        return None
    direction = "long" if ind["plus_di"][-1] > ind["minus_di"][-1] else "short"
    last3 = ctx.candles[TF_EXEC][-3:]
    consistent = all(c["c"] > c["o"] for c in last3) if direction == "long" else all(c["c"] < c["o"] for c in last3)
    if not consistent or ctx.candles[TF_EXEC][-1]["v"] < ind["vol_sma20"][-1]:
        return None
    invalidation = min(c["l"] for c in last3) if direction == "long" else max(c["h"] for c in last3)
    plan = _rr_plan_or_none(direction, ctx.price, invalidation, ctx)
    if not plan:
        return None
    return Candidate(ctx.symbol, direction, "momentum", ctx.price, plan["sl"], plan["tp1"], plan["tp2"],
                      confluence=["ADX momentum surge", "Directional volume"])


def engine_volatility_expansion(ctx: MarketContext) -> Optional[Candidate]:
    bw = ctx.ind[TF_EXEC]["bb_width_pct"]
    if len(bw) < 30:
        return None
    if percentile_rank(bw[-60:], bw[-6]) > 25:
        return None
    last = ctx.candles[TF_EXEC][-1]
    prior = ctx.candles[TF_EXEC][-6:-1]
    hi, lo = max(c["h"] for c in prior), min(c["l"] for c in prior)
    direction, invalidation = None, None
    if last["c"] > hi:
        direction, invalidation = "long", lo
    elif last["c"] < lo:
        direction, invalidation = "short", hi
    if not direction:
        return None
    plan = _rr_plan_or_none(direction, ctx.price, invalidation, ctx)
    if not plan:
        return None
    return Candidate(ctx.symbol, direction, "volatility_expansion", ctx.price, plan["sl"], plan["tp1"], plan["tp2"],
                      confluence=["Volatility squeeze release"])


def engine_mean_reversion(ctx: MarketContext) -> Optional[Candidate]:
    if ctx.regime.label != "ranging":
        return None
    rsi_now = ctx.ind[TF_EXEC]["rsi"][-1]
    if rsi_now <= 25:
        direction = "long"
        invalidation = min(c["l"] for c in ctx.candles[TF_EXEC][-6:])
    elif rsi_now >= 75:
        direction = "short"
        invalidation = max(c["h"] for c in ctx.candles[TF_EXEC][-6:])
    else:
        return None
    plan = _rr_plan_or_none(direction, ctx.price, invalidation, ctx)
    if not plan:
        return None
    return Candidate(ctx.symbol, direction, "mean_reversion", ctx.price, plan["sl"], plan["tp1"], plan["tp2"],
                      confluence=["RSI extreme", "Range regime fade"])


def engine_range(ctx: MarketContext) -> Optional[Candidate]:
    if ctx.regime.label != "ranging":
        return None
    window = ctx.candles[TF_MID][-40:]
    hi, lo = max(c["h"] for c in window), min(c["l"] for c in window)
    rng = hi - lo
    if rng <= 0:
        return None
    last = ctx.candles[TF_EXEC][-1]
    body = abs(last["c"] - last["o"])
    upper_wick = last["h"] - max(last["o"], last["c"])
    lower_wick = min(last["o"], last["c"]) - last["l"]
    direction, invalidation = None, None
    if (last["l"] - lo) <= rng * 0.08 and lower_wick > body * 1.2 and last["c"] > last["o"]:
        direction, invalidation = "long", lo - ctx.atr_exec * 0.2
    elif (hi - last["h"]) <= rng * 0.08 and upper_wick > body * 1.2 and last["c"] < last["o"]:
        direction, invalidation = "short", hi + ctx.atr_exec * 0.2
    if not direction:
        return None
    plan = _rr_plan_or_none(direction, ctx.price, invalidation, ctx)
    if not plan:
        return None
    return Candidate(ctx.symbol, direction, "range", ctx.price, plan["sl"], plan["tp1"], plan["tp2"],
                      confluence=["Range boundary rejection"])


ENGINE_FUNCS = {
    "smc_confluence": engine_smc_confluence,
    "order_block": engine_order_block,
    "breaker_block": engine_breaker_block,
    "fair_value_gap": engine_fair_value_gap,
    "liquidity_sweep": engine_liquidity_sweep,
    "trend_continuation": engine_trend_continuation,
    "breakout": engine_breakout,
    "pullback": engine_pullback,
    "reversal": engine_reversal,
    "momentum": engine_momentum,
    "volatility_expansion": engine_volatility_expansion,
    "mean_reversion": engine_mean_reversion,
    "range": engine_range,
}
ENGINE_IDS = list(ENGINE_FUNCS.keys())


def run_all_engines(ctx: MarketContext) -> list[Candidate]:
    out = []
    for eng_id, fn in ENGINE_FUNCS.items():
        try:
            cand = fn(ctx)
        except Exception as exc:  # never let one pathway crash the scan
            log.debug("engine %s failed for %s: %s", eng_id, ctx.symbol, exc)
            cand = None
        if cand is not None:
            out.append(cand)
    return out


# --------------------------------------------------------------------------
# State persistence
# --------------------------------------------------------------------------

def _default_state() -> dict:
    return {
        "version": STATE_VERSION,
        "engine_weights": {eid: 1.0 for eid in ENGINE_IDS},
        "engine_stats": {eid: {"trades": 0, "wins": 0, "losses": 0, "sum_r": 0.0} for eid in ENGINE_IDS},
        "calibration": {},          # confidence decile -> {trades, wins}
        "atr_pct_memory": {},       # symbol -> recent atr% history
        "cooldowns": {},            # "symbol:direction" -> bar index
        "active_signals": [],       # list of signal dicts, lifecycle-tracked
        "history": [],              # closed signals, capped
        "signals_fired_log": [],    # timestamps (ms) of fired signals, for governor
        "next_signal_id": 1,
        "last_daily_summary_date": None,
        "bar_index": 0,
        "created_ms": utcnow_ms(),
    }


def _deep_merge_defaults(state: dict, defaults: dict) -> dict:
    for k, v in defaults.items():
        if k not in state:
            state[k] = v
        elif isinstance(v, dict) and isinstance(state.get(k), dict):
            for sub_k, sub_v in v.items():
                state[k].setdefault(sub_k, sub_v)
    return state


def load_state() -> dict:
    if not STATE_PATH.exists():
        return _default_state()
    try:
        state = json.loads(STATE_PATH.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("state load failed (%s), starting fresh", exc)
        return _default_state()
    if state.get("version") != STATE_VERSION:
        log.info("migrating state from version %s to %s", state.get("version"), STATE_VERSION)
    return _deep_merge_defaults(state, _default_state())


def save_state(state: dict):
    tmp = STATE_PATH.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(state, indent=None, default=str))
        tmp.replace(STATE_PATH)
    except OSError as exc:
        log.error("state save failed: %s", exc)


def load_candle_cache() -> dict:
    if not CANDLE_CACHE_PATH.exists():
        return {}
    try:
        return json.loads(CANDLE_CACHE_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def save_candle_cache(cache: dict):
    tmp = CANDLE_CACHE_PATH.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(cache, default=str))
        tmp.replace(CANDLE_CACHE_PATH)
    except OSError as exc:
        log.error("candle cache save failed: %s", exc)


def prune_state(state: dict, max_history: int = 1000, max_days: int = 45):
    cutoff = utcnow_ms() - max_days * 86_400_000
    state["history"] = [h for h in state["history"] if h.get("closed_ms", utcnow_ms()) >= cutoff][-max_history:]
    state["signals_fired_log"] = [t for t in state["signals_fired_log"] if t >= utcnow_ms() - 2 * 86_400_000]
    for sym, mem in list(state["atr_pct_memory"].items()):
        state["atr_pct_memory"][sym] = mem[-300:]


def estimate_signals_last_24h(state: dict) -> int:
    cutoff = utcnow_ms() - 86_400_000
    return sum(1 for t in state["signals_fired_log"] if t >= cutoff)


def check_cooldown(state: dict, symbol: str, direction: str, bar_index: int) -> bool:
    key = f"{symbol}:{direction}"
    last = state["cooldowns"].get(key)
    return last is None or (bar_index - last) >= COOLDOWN_BARS


def update_cooldown(state: dict, symbol: str, direction: str, bar_index: int):
    state["cooldowns"][f"{symbol}:{direction}"] = bar_index


def is_recent_duplicate(state: dict, symbol: str, direction: str, entry: float) -> bool:
    for sig in state["active_signals"]:
        if sig["symbol"] == symbol and sig["direction"] == direction:
            if abs(pct_change(sig["entry"], entry)) <= DUPLICATE_ENTRY_TOL_PCT:
                return True
    return False


def update_atr_pct_memory(state: dict, symbol: str, atr_pct: float) -> float:
    mem = state["atr_pct_memory"].setdefault(symbol, [])
    mem.append(atr_pct)
    if len(mem) > 300:
        del mem[:len(mem) - 300]
    return percentile_rank(mem, atr_pct)


# --------------------------------------------------------------------------
# Decision engine: adaptive-weighted ensemble scoring
# --------------------------------------------------------------------------

def logistic(x: float) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0


def engine_winrate(state: dict, engine_id: str) -> float:
    stats = state["engine_stats"].get(engine_id, {})
    trades = stats.get("trades", 0)
    if trades < 8:
        return 0.5  # insufficient sample, stay neutral
    return clamp(safe_div(stats.get("wins", 0), trades, 0.5), 0.1, 0.9)


def calibration_adjustment(state: dict, raw_confidence: float) -> float:
    bucket = str(int(raw_confidence // 10) * 10)
    cal = state["calibration"].get(bucket)
    if not cal or cal.get("trades", 0) < 10:
        return raw_confidence
    realized = 100.0 * safe_div(cal["wins"], cal["trades"], raw_confidence / 100.0)
    return clamp(0.6 * raw_confidence + 0.4 * realized, 1.0, 99.0)


def score_candidate(cand: Candidate, ctx: MarketContext, state: dict) -> dict:
    weight = state["engine_weights"].get(cand.engine, 1.0)
    winrate = engine_winrate(state, cand.engine)

    confluence_score = clamp(len(cand.confluence) / 4.0, 0.0, 1.0)
    rr_score = clamp(safe_div(cand.rr2 - MIN_RR_FLOOR, 3.0), 0.0, 1.0)
    regime_fit = ctx.regime.composite_favorability()

    mtf_bonus = 0.0
    if ctx.structure[TF_MID].trend != "neutral" and ctx.structure[TF_HTF].trend == ctx.structure[TF_MID].trend:
        mtf_bonus = 0.15
    if cand.direction == "long" and ctx.structure[TF_HTF].trend == "bullish":
        mtf_bonus += 0.1
    if cand.direction == "short" and ctx.structure[TF_HTF].trend == "bearish":
        mtf_bonus += 0.1

    raw = (
        -1.1
        + 2.2 * confluence_score
        + 1.4 * rr_score
        + 1.3 * regime_fit
        + 1.6 * (winrate - 0.5)
        + mtf_bonus
        + 0.5 * (weight - 1.0)
    )
    raw_confidence = 100.0 * logistic(raw)
    confidence = calibration_adjustment(state, raw_confidence)

    p = clamp(confidence / 100.0, 0.05, 0.95)
    ev = p * cand.rr2 - (1 - p) * 1.0

    priority = confidence * 0.65 + clamp(ev, -2, 6) * 8.0 + rr_score * 10.0
    return {"confidence": confidence, "ev": ev, "priority": priority, "winrate": winrate}


def grade_for_confidence(confidence: float) -> str:
    if confidence >= 85:
        return "A+"
    if confidence >= 75:
        return "A"
    if confidence >= 65:
        return "B"
    if confidence >= 55:
        return "C"
    return "D"


def passes_hard_filters(cand: Candidate, ctx: MarketContext, score: dict, min_confidence: float,
                         spread_pct: Optional[float] = None) -> tuple[bool, str]:
    if cand.rr2 < MIN_RR_FLOOR:
        return False, "rr_too_low"
    if score["confidence"] < min_confidence:
        return False, "confidence_below_threshold"
    if score["ev"] <= 0:
        return False, "negative_expected_value"
    if ctx.regime.volatility_pctile > 97:
        return False, "extreme_volatility"
    if spread_pct is not None and spread_pct > 0.004:
        return False, "spread_too_wide"
    return True, "ok"


def governor_min_confidence(state: dict, regime: RegimeVector, base_threshold: float = 62.0) -> float:
    fired_24h = estimate_signals_last_24h(state)
    threshold = adaptive_thresholds(regime, base_threshold)
    if fired_24h >= TARGET_SIGNALS_PER_DAY_MAX:
        threshold += 12.0
    elif fired_24h < TARGET_SIGNALS_PER_DAY_MIN and regime.composite_favorability() > 0.5:
        threshold -= 6.0
    return clamp(threshold, 45.0, 92.0)


def dedup_correlated(ranked: list[dict], clusters: list[set]) -> list[dict]:
    def cluster_of(symbol: str) -> frozenset:
        for cl in clusters:
            if symbol in cl:
                return frozenset(cl)
        return frozenset({symbol})

    seen_clusters = set()
    out = []
    for item in ranked:
        key = (cluster_of(item["candidate"].symbol), item["candidate"].direction)
        if key in seen_clusters:
            continue
        seen_clusters.add(key)
        out.append(item)
    return out


def rank_and_select(scored: list[dict], clusters: list[set], max_new: int = MAX_SIGNALS_PER_SCAN) -> list[dict]:
    scored.sort(key=lambda x: -x["score"]["priority"])
    deduped = dedup_correlated(scored, clusters)
    return deduped[:max_new]


# --------------------------------------------------------------------------
# Continuous learning engine
# --------------------------------------------------------------------------

LEARNING_RATE = 0.06
WEIGHT_BOUNDS = (0.4, 1.8)


def record_trade_outcome(state: dict, sig: dict, result: str, exit_price: float):
    """Analyze a completed trade and feed the learning engine: engine
    win-rate/expectancy, adaptive weighting, and confidence calibration."""
    r_multiple = _r_multiple(sig, exit_price)
    won = r_multiple > 0

    stats = state["engine_stats"].setdefault(sig["engine"], {"trades": 0, "wins": 0, "losses": 0, "sum_r": 0.0})
    stats["trades"] += 1
    stats["wins" if won else "losses"] += 1
    stats["sum_r"] += r_multiple

    expected_r = sig.get("confidence", 60.0) / 100.0 * sig.get("rr2", DEFAULT_RR_TP2) - \
        (1 - sig.get("confidence", 60.0) / 100.0)
    surprise = r_multiple - expected_r
    old_w = state["engine_weights"].get(sig["engine"], 1.0)
    new_w = clamp(old_w + LEARNING_RATE * surprise, *WEIGHT_BOUNDS)
    state["engine_weights"][sig["engine"]] = new_w

    bucket = str(int(sig.get("confidence", 60.0) // 10) * 10)
    cal = state["calibration"].setdefault(bucket, {"trades": 0, "wins": 0})
    cal["trades"] += 1
    cal["wins"] += 1 if won else 0

    entry = {
        "id": sig["id"], "symbol": sig["symbol"], "direction": sig["direction"],
        "engine": sig["engine"], "result": result, "r_multiple": round(r_multiple, 3),
        "confidence": sig.get("confidence"), "regime": sig.get("regime_label"),
        "entry": sig["entry"], "sl": sig["sl"], "tp1": sig["tp1"], "tp2": sig["tp2"],
        "exit_price": exit_price, "duration_bars": state["bar_index"] - sig.get("bar_index", state["bar_index"]),
        "fired_ms": sig.get("fired_ms"), "closed_ms": utcnow_ms(),
        "confluence": sig.get("confluence", []),
    }
    state["history"].append(entry)
    return entry


def tune_engine_weights_decay(state: dict, decay: float = 0.02):
    """Slowly pull weights back toward 1.0 to avoid overfitting to streaks."""
    for eid, w in state["engine_weights"].items():
        state["engine_weights"][eid] = clamp(w + (1.0 - w) * decay, *WEIGHT_BOUNDS)


def learning_insights(state: dict, top_n: int = 3) -> list[str]:
    ranked = sorted(
        state["engine_stats"].items(),
        key=lambda kv: -safe_div(kv[1]["sum_r"], max(kv[1]["trades"], 1)),
    )
    insights = []
    for eid, stats in ranked[:top_n]:
        if stats["trades"] < 5:
            continue
        avg_r = safe_div(stats["sum_r"], stats["trades"])
        insights.append(f"{eid}: {stats['trades']} trades, {avg_r:+.2f}R avg, "
                         f"weight {state['engine_weights'].get(eid, 1.0):.2f}")
    return insights


# --------------------------------------------------------------------------
# Telegram layer
# --------------------------------------------------------------------------

REACTIONS = {
    "ACTIVATED": "👍",
    "TP1": "🔥",
    "TP2": "🏆",
    "SL": "💔",
    "BREAKEVEN": "🤝",
    "CANCELLED": "🗿",
}


def tg_escape(value) -> str:
    text = str(value)
    for ch in "_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


def fmt_px(v: float) -> str:
    if v >= 1000:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:,.4f}"
    return f"{v:.6f}"


def confidence_bar(confidence: float) -> str:
    filled = int(round(confidence / 10.0))
    return "▰" * filled + "▱" * (10 - filled)


def format_signal(cand: Candidate, confidence: float, grade: str, signal_id: int) -> str:
    arrow = "🟢 LONG" if cand.direction == "long" else "🔴 SHORT"
    lines = [
        f"*{tg_escape(ENGINE_TAG)}*",
        f"#{signal_id} · {tg_escape(cand.symbol)}\\-PERP · {arrow}",
        "",
        f"Grade: *{grade}*  Confidence: {confidence:.0f}% {confidence_bar(confidence)}",
        f"Engine: {tg_escape(cand.engine.replace('_', ' ').title())}",
        "",
        "```",
        f"Entry   {fmt_px(cand.entry)}",
        f"SL      {fmt_px(cand.sl)}",
        f"TP1     {fmt_px(cand.tp1)}  (RR {cand.rr1:.2f})",
        f"TP2     {fmt_px(cand.tp2)}  (RR {cand.rr2:.2f})",
        "```",
        "",
        "Confluence: " + tg_escape(", ".join(cand.confluence)) if cand.confluence else "",
    ]
    return "\n".join(l for l in lines if l != "")


def send_telegram(text: str) -> Optional[int]:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.info("telegram not configured, skipping send")
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = _SESSION.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "MarkdownV2",
        }, timeout=10)
        resp.raise_for_status()
        return resp.json().get("result", {}).get("message_id")
    except (requests.RequestException, ValueError) as exc:
        log.warning("send_telegram failed: %s", exc)
        return None


def reply_telegram(text: str, reply_to_message_id: Optional[int]) -> Optional[int]:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or not reply_to_message_id:
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = _SESSION.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "MarkdownV2",
            "reply_to_message_id": reply_to_message_id,
        }, timeout=10)
        resp.raise_for_status()
        return resp.json().get("result", {}).get("message_id")
    except (requests.RequestException, ValueError) as exc:
        log.warning("reply_telegram failed: %s", exc)
        return None


def react_telegram(message_id: Optional[int], emoji: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or not message_id:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setMessageReaction"
    try:
        _SESSION.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID, "message_id": message_id,
            "reaction": [{"type": "emoji", "emoji": emoji}],
        }, timeout=10)
    except requests.RequestException as exc:
        log.debug("react_telegram failed: %s", exc)


def build_daily_summary(state: dict) -> str:
    cutoff = utcnow_ms() - 86_400_000
    recent = [h for h in state["history"] if h.get("closed_ms", 0) >= cutoff]
    total = len(recent)
    wins = sum(1 for h in recent if h["r_multiple"] > 0)
    avg_r = safe_div(sum(h["r_multiple"] for h in recent), total) if total else 0.0
    win_rate = safe_div(wins, total, 0.0) * 100.0

    best = max(recent, key=lambda h: h["r_multiple"], default=None)
    worst = min(recent, key=lambda h: h["r_multiple"], default=None)

    regime_counts: dict[str, int] = {}
    for h in recent:
        regime_counts[h.get("regime", "unknown")] = regime_counts.get(h.get("regime", "unknown"), 0) + 1

    lines = [
        f"*{tg_escape(ENGINE_TAG)} — Daily Summary*",
        f"Window: last 24h · {utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        f"Signals closed: {total}",
        f"Win rate: {win_rate:.1f}%",
        f"Avg R: {avg_r:+.2f}",
        f"Active now: {len(state['active_signals'])}",
    ]
    if best:
        lines.append(f"Best: {tg_escape(best['symbol'])} {best['engine']} {best['r_multiple']:+.2f}R")
    if worst:
        lines.append(f"Worst: {tg_escape(worst['symbol'])} {worst['engine']} {worst['r_multiple']:+.2f}R")
    if regime_counts:
        breakdown = ", ".join(f"{k}: {v}" for k, v in regime_counts.items())
        lines.append(f"Regime mix: {tg_escape(breakdown)}")
    insights = learning_insights(state)
    if insights:
        lines.append("")
        lines.append("Learning insights:")
        lines.extend(f"• {tg_escape(i)}" for i in insights)
    return "\n".join(lines)


def maybe_send_daily_summary(state: dict):
    now = utcnow()
    today_str = now.strftime("%Y-%m-%d")
    if now.hour != 8:
        return
    if state.get("last_daily_summary_date") == today_str:
        return
    send_telegram(build_daily_summary(state))
    state["last_daily_summary_date"] = today_str


# --------------------------------------------------------------------------
# Trade lifecycle monitor
# --------------------------------------------------------------------------

def track_signal(state: dict, cand: Candidate, score: dict, msg_id: Optional[int],
                  regime_label: str) -> dict:
    sig_id = state["next_signal_id"]
    state["next_signal_id"] += 1
    entry_buf = cand.risk * 0.12
    sig = {
        "id": sig_id, "symbol": cand.symbol, "direction": cand.direction, "engine": cand.engine,
        "entry": cand.entry, "sl": cand.sl, "initial_sl": cand.sl, "tp1": cand.tp1, "tp2": cand.tp2,
        "rr1": cand.rr1, "rr2": cand.rr2, "initial_risk": cand.risk, "entry_buf": entry_buf,
        "confidence": round(score["confidence"], 1), "regime_label": regime_label,
        "confluence": cand.confluence, "status": "PENDING", "tp1_hit": False,
        "msg_id": msg_id, "bar_index": state["bar_index"], "fired_ms": utcnow_ms(),
        "last_checked_ms": utcnow_ms(),
    }
    state["active_signals"].append(sig)
    state["signals_fired_log"].append(sig["fired_ms"])
    return sig


def _r_multiple(sig: dict, price: float) -> float:
    risk = sig["initial_risk"]
    if risk <= 0:
        return 0.0
    if sig["direction"] == "long":
        return safe_div(price - sig["entry"], risk)
    return safe_div(sig["entry"] - price, risk)


def _close_signal(state: dict, sig: dict, result: str, price: float):
    if result == "BREAKEVEN" and sig.get("tp1_hit"):
        r_multiple = 0.5 * sig["rr1"]
    else:
        r_multiple = _r_multiple(sig, price)
    entry = record_trade_outcome(state, sig, result, price)
    sig["status"] = "CLOSED"
    state["active_signals"] = [s for s in state["active_signals"] if s["id"] != sig["id"]]

    emoji_key = {"TP2": "TP2", "SL": "SL", "BREAKEVEN": "BREAKEVEN"}.get(result, None)
    label = {"TP2": "TP2 hit 🏆", "SL": "Stopped out 💔", "BREAKEVEN": "Closed at breakeven 🤝"}.get(
        result, f"Closed ({result})")
    text = f"*#{sig['id']} {tg_escape(sig['symbol'])}* — {label}\nResult: {r_multiple:+.2f}R"
    reply_telegram(text, sig.get("msg_id"))
    if emoji_key:
        react_telegram(sig.get("msg_id"), REACTIONS[emoji_key])
    return entry


def _cancel_signal(state: dict, sig: dict, reason: str):
    sig["status"] = "CANCELLED"
    state["active_signals"] = [s for s in state["active_signals"] if s["id"] != sig["id"]]
    text = f"*#{sig['id']} {tg_escape(sig['symbol'])}* — Cancelled ⏹️ ({tg_escape(reason)})"
    reply_telegram(text, sig.get("msg_id"))
    react_telegram(sig.get("msg_id"), REACTIONS["CANCELLED"])


def check_active_signals(state: dict, bundles: dict[str, dict[str, list[dict]]]):
    for sig in list(state["active_signals"]):
        bundle = bundles.get(sig["symbol"])
        if not bundle:
            continue
        candles = [c for c in bundle[TF_EXEC] if c["t"] > sig.get("last_checked_ms", 0) - 1]
        if not candles:
            continue

        long_dir = sig["direction"] == "long"
        for c in candles:
            if sig["status"] == "PENDING":
                lo, hi = sig["entry"] - sig["entry_buf"], sig["entry"] + sig["entry_buf"]
                invalidated = (c["l"] <= sig["sl"]) if long_dir else (c["h"] >= sig["sl"])
                if invalidated:
                    _cancel_signal(state, sig, "invalidated before entry")
                    break
                if c["l"] <= hi and c["h"] >= lo:
                    sig["status"] = "ACTIVE"
                    reply_telegram(f"*#{sig['id']} {tg_escape(sig['symbol'])}* — Activated ✅", sig.get("msg_id"))
                    react_telegram(sig.get("msg_id"), REACTIONS["ACTIVATED"])
                elif (state["bar_index"] - sig["bar_index"]) > SIGNAL_EXPIRY_BARS:
                    _cancel_signal(state, sig, "expired")
                    break

            if sig["status"] == "ACTIVE":
                if long_dir:
                    if c["l"] <= sig["sl"]:
                        result = "BREAKEVEN" if sig["tp1_hit"] and sig["sl"] >= sig["entry"] else "SL"
                        _close_signal(state, sig, result, sig["sl"])
                        break
                    if not sig["tp1_hit"] and c["h"] >= sig["tp1"]:
                        sig["tp1_hit"] = True
                        sig["sl"] = sig["entry"]
                        reply_telegram(f"*#{sig['id']} {tg_escape(sig['symbol'])}* — TP1 hit 🔥 "
                                        f"\\(SL moved to breakeven\\)", sig.get("msg_id"))
                        react_telegram(sig.get("msg_id"), REACTIONS["TP1"])
                    elif sig["tp1_hit"] and c["h"] >= sig["tp2"]:
                        _close_signal(state, sig, "TP2", sig["tp2"])
                        break
                else:
                    if c["h"] >= sig["sl"]:
                        result = "BREAKEVEN" if sig["tp1_hit"] and sig["sl"] <= sig["entry"] else "SL"
                        _close_signal(state, sig, result, sig["sl"])
                        break
                    if not sig["tp1_hit"] and c["l"] <= sig["tp1"]:
                        sig["tp1_hit"] = True
                        sig["sl"] = sig["entry"]
                        reply_telegram(f"*#{sig['id']} {tg_escape(sig['symbol'])}* — TP1 hit 🔥 "
                                        f"\\(SL moved to breakeven\\)", sig.get("msg_id"))
                        react_telegram(sig.get("msg_id"), REACTIONS["TP1"])
                    elif sig["tp1_hit"] and c["l"] <= sig["tp2"]:
                        _close_signal(state, sig, "TP2", sig["tp2"])
                        break

        sig["last_checked_ms"] = bundle[TF_EXEC][-1]["t"] if bundle[TF_EXEC] else sig["last_checked_ms"]


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

_SHUTDOWN = {"flag": False}
_RUNTIME_REFS: dict = {"state": None, "cache": None}


def _handle_sigterm(signum, frame):
    log.warning("received signal %s, saving state and exiting", signum)
    _SHUTDOWN["flag"] = True
    if _RUNTIME_REFS["state"] is not None:
        save_state(_RUNTIME_REFS["state"])
    if _RUNTIME_REFS["cache"] is not None:
        save_candle_cache(_RUNTIME_REFS["cache"])
    sys.exit(0)


def scan_symbol(symbol: str, bundle: dict[str, list[dict]], breadth: float,
                 reference_ms: int, state: dict) -> list[dict]:
    ind_htf = compute_indicators(bundle[TF_HTF])
    ind_mid = compute_indicators(bundle[TF_MID])
    regime = classify_regime(ind_htf, ind_mid, bundle[TF_MID], breadth, reference_ms)

    atr_pct = safe_div(ind_mid["atr"][-1], bundle[TF_MID][-1]["c"])
    update_atr_pct_memory(state, symbol, atr_pct)

    ctx = build_market_context(symbol, bundle, regime)
    min_conf = governor_min_confidence(state, regime)

    results = []
    for cand in run_all_engines(ctx):
        if not check_cooldown(state, symbol, cand.direction, state["bar_index"]):
            continue
        if is_recent_duplicate(state, symbol, cand.direction, cand.entry):
            continue
        score = score_candidate(cand, ctx, state)
        ok, _reason = passes_hard_filters(cand, ctx, score, min_conf)
        if not ok:
            continue
        results.append({"candidate": cand, "score": score, "regime_label": regime.label})
    return results


def main() -> int:
    os_signal.signal(os_signal.SIGTERM, _handle_sigterm)
    try:
        os_signal.signal(os_signal.SIGINT, _handle_sigterm)
    except ValueError:
        pass

    log.info("%s starting scan", ENGINE_TAG)
    state = load_state()
    cache = load_candle_cache()
    _RUNTIME_REFS["state"], _RUNTIME_REFS["cache"] = state, cache

    reference_ms = utcnow_ms()
    symbols = list(dict.fromkeys(WATCHLIST + [BASE_ASSET]))

    bundles: dict[str, dict[str, list[dict]]] = {}
    for symbol in symbols:
        if time_budget_exceeded():
            log.warning("time budget exceeded during candle fetch, stopping early")
            break
        bundle = fetch_all_candles(symbol, cache, reference_ms)
        if bundle:
            bundles[symbol] = bundle
        else:
            log.debug("no data for %s this scan", symbol)

    if BASE_ASSET not in bundles:
        log.error("no BTC data available, aborting scan")
        save_state(state)
        save_candle_cache(cache)
        return 1

    btc_regime = compute_btc_regime(bundles[BASE_ASSET])
    breadth = compute_breadth(bundles, btc_regime.htf_bias)

    tradeable = [s for s in WATCHLIST if s in bundles]
    corr_matrix = compute_pairwise_correlation(tradeable, bundles)
    clusters = cluster_by_correlation(tradeable, corr_matrix)

    all_scored: list[dict] = []
    for symbol in tradeable:
        if time_budget_exceeded():
            log.warning("time budget exceeded during signal scan, stopping early")
            break
        try:
            all_scored.extend(scan_symbol(symbol, bundles[symbol], breadth, reference_ms, state))
        except Exception as exc:
            log.warning("scan_symbol failed for %s: %s", symbol, exc)

    selected = rank_and_select(all_scored, clusters, MAX_SIGNALS_PER_SCAN)
    capacity = max(0, MAX_CONCURRENT_ACTIVE_SIGNALS - len(state["active_signals"]))
    selected = selected[:capacity]

    for item in selected:
        cand: Candidate = item["candidate"]
        score = item["score"]
        spread = get_l2_spread_pct(cand.symbol)
        if spread is not None and spread > 0.004:
            log.info("skipping %s %s: spread too wide (%.4f)", cand.symbol, cand.engine, spread)
            continue
        grade = grade_for_confidence(score["confidence"])
        sig_num = state["next_signal_id"]
        msg = format_signal(cand, score["confidence"], grade, sig_num)
        msg_id = send_telegram(msg)
        track_signal(state, cand, score, msg_id, item["regime_label"])
        update_cooldown(state, cand.symbol, cand.direction, state["bar_index"])
        log.info("signal #%d %s %s via %s conf=%.1f grade=%s", sig_num, cand.symbol,
                  cand.direction, cand.engine, score["confidence"], grade)

    check_active_signals(state, bundles)
    tune_engine_weights_decay(state)
    maybe_send_daily_summary(state)

    state["bar_index"] += 1
    prune_state(state)
    save_state(state)
    save_candle_cache(cache)

    log.info("%s scan complete: %d candidates, %d fired, %d active",
              ENGINE_TAG, len(all_scored), len(selected), len(state["active_signals"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
