#!/usr/bin/env python3
"""
AEGIS-APEX Adaptive Institutional Signal Engine v1.0.0
========================================================

Original synthesis engine for Hyperliquid perpetuals. Scan-per-run model:
an external scheduler (cron-job.org, GitHub Actions cron, etc.) invokes
this script every 15 minutes. All state lives in JSON files next to the
script. No database, no long-running process.

Architecture
------------
1. Hyperliquid data layer: cached multi-timeframe candles, throttled
   requests, exponential backoff, batched meta/snapshot calls.
2. Structure layer: swing detection, BOS/CHoCH, order blocks, breaker
   blocks, fair value gaps, liquidity pools/sweeps, premium/discount,
   session volume profile.
3. Regime layer: BTC macro bias, per-symbol volatility percentile, ADX
   trend strength, session liquidity weight, noise index, cross-
   sectional breadth -> a single RegimeVector per symbol per scan.
4. Specialized Engine layer: eleven independent candidate generators
   (Order Block, Breaker Block, FVG Fill, Liquidity Sweep Reversal,
   Trend Continuation, Pullback, Breakout Expansion, Momentum,
   Mean Reversion, Range, CHoCH Reversal). Each only fires inside the
   regime conditions it is suited for.
5. Decision Engine: merges every candidate through one calibrated
   scoring function using adaptive, self-tuning per-engine weights,
   regime fit, MTF alignment, liquidity/volatility/volume confluence,
   expected value and RR. Applies correlation-cluster deduplication and
   an adaptive frequency governor that steers output toward a 5-10
   signal/day band without ever forcing low-quality trades.
6. Risk layer: structure-based SL beyond real invalidation with an ATR
   floor, TP clipped to genuine opposing liquidity / order block /
   value-area edges, adaptive RR.
7. Learning layer: every closed trade updates per-engine expectancy,
   confidence calibration buckets and regime-fit weighting via slow,
   regularized EMAs - never a hard overfit to the latest sample.
8. Telegram layer: entry/SL/TP1/TP2 messages, lifecycle reply updates
   (activated / TP1 / TP2 / SL / break-even / closed / cancelled),
   daily 08:00 UTC summary.

Configure via environment variables (see CONFIGURATION) and run:

    python3 aegis_apex_engine_v1_0_0.py
"""

from __future__ import annotations

import json
import logging
import math
import os
import signal as signal_module
import statistics
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional

# ============================================================================
# CONFIGURATION
# ============================================================================

ENGINE_NAME = "AEGIS-APEX"
ENGINE_VERSION = "1.0.0"

HL_API_URL = "https://api.hyperliquid.xyz/info"
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")

STATE_PATH = os.environ.get("AEGIS_STATE_PATH", "state.json")
CACHE_PATH = os.environ.get("AEGIS_CANDLE_CACHE_PATH", "candle_cache.json")
LOG_PATH = os.environ.get("AEGIS_LOG_PATH", "aegis_apex.log")

CANDLE_OVERLAP_BARS = 3  # re-fetched closed bars past cache watermark

WATCHLIST = [
    "BTC", "ETH", "HYPE", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK",
    "SUI", "NEAR", "TRX", "BCH", "LTC", "DOT", "AAVE", "UNI", "APT", "TAO",
    "ONDO", "PENDLE", "ZEC", "PENGU", "XLM",
]

# Never below 15m. Bias (HTF context) / Structure (zone building) /
# Execution (entry trigger) triplets, chosen per-symbol by regime fit.
COMBOS = {
    "intraday": {"bias": "4h", "struct": "1h", "exec": "15m", "hold": "4-20h"},
    "swing":    {"bias": "1d", "struct": "4h", "exec": "1h",  "hold": "1-5d"},
    "position": {"bias": "1d", "struct": "12h", "exec": "4h", "hold": "3-10d"},
}
TF_MS = {
    "15m": 15 * 60_000, "1h": 60 * 60_000, "4h": 4 * 60 * 60_000,
    "12h": 12 * 60 * 60_000, "1d": 24 * 60 * 60_000,
}
CANDLE_COUNT = {"15m": 300, "1h": 300, "4h": 260, "12h": 220, "1d": 200}

ATR_LEN = 14
RSI_LEN = 14
ADX_LEN = 14
BB_LEN = 20
BB_MULT = 2.0
VOL_PROFILE_BINS = 24

BASE_ACCEPT_THRESHOLD = 0.62
TARGET_SIGNALS_PER_DAY = (5.0, 10.0)
MAX_CONCURRENT_SAME_DIRECTION = 6
MAX_CONCURRENT_PER_SYMBOL = 1
COOLDOWN_BARS_15M = 6
DUPLICATE_ENTRY_TOL_PCT = 0.0025
CORRELATION_TOL_PCT = 0.0015
DAILY_SUMMARY_HOUR_UTC = 8

logging.basicConfig(
    filename=LOG_PATH, level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOG = logging.getLogger(ENGINE_NAME)

_SHUTDOWN = {"flag": False}


def _handle_shutdown(sig_num, frame):
    _SHUTDOWN["flag"] = True


signal_module.signal(signal_module.SIGTERM, _handle_shutdown)
signal_module.signal(signal_module.SIGINT, _handle_shutdown)


# ============================================================================
# HYPERLIQUID DATA LAYER
# ============================================================================

class _RateLimiter:
    def __init__(self, budget_per_minute: float = 1000.0):
        self.budget = budget_per_minute
        self.used = 0.0
        self.window_start = time.time()

    def wait(self, weight: float = 2.0):
        now = time.time()
        if now - self.window_start >= 60:
            self.window_start = now
            self.used = 0.0
        if self.used + weight > self.budget:
            sleep_for = 60 - (now - self.window_start)
            if sleep_for > 0:
                time.sleep(sleep_for)
            self.window_start = time.time()
            self.used = 0.0
        self.used += weight


_LIMITER = _RateLimiter()


def hl_post(payload: dict, retries: int = 4, timeout: int = 12) -> Optional[dict]:
    body = json.dumps(payload).encode()
    for attempt in range(retries):
        _LIMITER.wait(2.0 if payload.get("type") == "candleSnapshot" else 1.0)
        try:
            req = urllib.request.Request(
                HL_API_URL, data=body, method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
                json.JSONDecodeError) as exc:
            LOG.warning("hl_post attempt %d failed: %s", attempt, exc)
            time.sleep(min(1.5 * (2 ** attempt), 20))
    return None


def current_bar_open_ms(ref_ms: int, interval: str) -> int:
    step = TF_MS[interval]
    return (ref_ms // step) * step


def filter_closed(candles: list, interval: str, ref_ms: int) -> list:
    open_ms = current_bar_open_ms(ref_ms, interval)
    return [c for c in candles if c["t"] < open_ms]


def _request_candles(symbol: str, interval: str, start_ms: int, end_ms: int) -> list:
    resp = hl_post({
        "type": "candleSnapshot",
        "req": {"coin": symbol, "interval": interval, "startTime": start_ms, "endTime": end_ms},
    })
    return resp if isinstance(resp, list) else []


def get_candles(symbol: str, interval: str, n: int, cache: dict, ref_ms: Optional[int] = None) -> list:
    ref_ms = ref_ms or int(time.time() * 1000)
    key = f"{symbol}:{interval}"
    step = TF_MS[interval]
    entry = cache.get(key)
    if entry and entry.get("candles"):
        existing = entry["candles"]
        watermark = existing[-1]["t"] - CANDLE_OVERLAP_BARS * step
        fresh = _request_candles(symbol, interval, watermark, ref_ms)
        merged = {c["t"]: c for c in existing}
        for c in fresh:
            merged[c["t"]] = c
        candles = sorted(merged.values(), key=lambda c: c["t"])[-n * 2:]
    else:
        start_ms = ref_ms - step * (n + 5)
        candles = _request_candles(symbol, interval, start_ms, ref_ms)
    candles = filter_closed(candles, interval, ref_ms)[-n:]
    cache[key] = {"candles": candles, "updated": ref_ms}
    return candles


def get_meta_and_ctx() -> Optional[tuple]:
    resp = hl_post({"type": "metaAndAssetCtxs"})
    if not resp or len(resp) < 2:
        return None
    return resp[0].get("universe", []), resp[1]


def get_market_snapshot() -> dict:
    meta = get_meta_and_ctx()
    out = {}
    if not meta:
        return out
    universe, ctxs = meta
    for u, c in zip(universe, ctxs):
        try:
            out[u["name"]] = {
                "mark": float(c.get("markPx", 0) or 0),
                "oracle": float(c.get("oraclePx", 0) or 0),
                "funding": float(c.get("funding", 0) or 0),
                "volume24h": float(c.get("dayNtlVlm", 0) or 0),
                "openInterest": float(c.get("openInterest", 0) or 0),
            }
        except (TypeError, ValueError):
            continue
    return out


# ============================================================================
# INDICATORS
# ============================================================================

def safe(v, fb=0.0):
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return fb
        return v
    except TypeError:
        return fb


def ema(vals: list, period: int) -> list:
    if not vals:
        return []
    k = 2.0 / (period + 1)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def sma(vals: list, period: int) -> list:
    out = []
    for i in range(len(vals)):
        if i + 1 < period:
            out.append(vals[i])
        else:
            out.append(sum(vals[i + 1 - period:i + 1]) / period)
    return out


def stdev(vals: list, period: int) -> list:
    out = []
    for i in range(len(vals)):
        if i + 1 < period:
            out.append(0.0)
        else:
            window = vals[i + 1 - period:i + 1]
            out.append(statistics.pstdev(window))
    return out


def rsi(closes: list, period: int = RSI_LEN) -> list:
    if len(closes) < 2:
        return [50.0] * len(closes)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g, avg_l = gains[0], losses[0]
    out = [50.0]
    for i in range(1, len(closes)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs = avg_g / avg_l if avg_l > 1e-12 else 100.0
        out.append(100 - 100 / (1 + rs))
    return out


def atr(highs, lows, closes, period: int = ATR_LEN) -> list:
    trs = [highs[0] - lows[0]]
    for i in range(1, len(closes)):
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    out, a = [trs[0]], trs[0]
    for i in range(1, len(trs)):
        a = (a * (period - 1) + trs[i]) / period
        out.append(a)
    return out


def adx_dmi(highs, lows, closes, period: int = ADX_LEN) -> tuple:
    n = len(closes)
    plus_dm, minus_dm, trs = [0.0], [0.0], [highs[0] - lows[0]]
    for i in range(1, n):
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))

    def wilder(series):
        out, a = [series[0]], series[0]
        for i in range(1, len(series)):
            a = a - a / period + series[i]
            out.append(a)
        return out

    atr_w = wilder(trs)
    pdi = [100 * (p / t) if t > 1e-12 else 0.0 for p, t in zip(wilder(plus_dm), atr_w)]
    mdi = [100 * (m / t) if t > 1e-12 else 0.0 for m, t in zip(wilder(minus_dm), atr_w)]
    dx = [100 * abs(p - m) / (p + m) if (p + m) > 1e-12 else 0.0 for p, m in zip(pdi, mdi)]
    adx = ema(dx, period)
    return adx, pdi, mdi


def bollinger_width_pct(closes: list, period: int = BB_LEN, mult: float = BB_MULT) -> list:
    mid = sma(closes, period)
    sd = stdev(closes, period)
    return [(2 * mult * s) / m if m > 1e-9 else 0.0 for s, m in zip(sd, mid)]


def compute_indicators(candles: list) -> dict:
    closes = [c["c"] for c in candles]
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
    vols = [c.get("v", 0.0) for c in candles]
    if len(closes) < 20:
        return {"insufficient": True}
    atr_vals = atr(highs, lows, closes)
    adx_vals, pdi, mdi = adx_dmi(highs, lows, closes)
    return {
        "closes": closes, "highs": highs, "lows": lows, "vols": vols,
        "ema20": ema(closes, 20), "ema50": ema(closes, 50),
        "ema200": ema(closes, 200) if len(closes) >= 200 else ema(closes, len(closes)),
        "rsi": rsi(closes), "atr": atr_vals, "adx": adx_vals, "pdi": pdi, "mdi": mdi,
        "bb_width": bollinger_width_pct(closes),
        "vol_sma20": sma(vols, 20),
        "insufficient": False,
    }


# ============================================================================
# STATE / CACHE PERSISTENCE
# ============================================================================

def _default_state() -> dict:
    return {
        "signals": [],
        "history": [],
        "engine_weights": {},
        "confidence_calibration": {},
        "regime_memory": {},
        "cooldowns": {},
        "accept_threshold": BASE_ACCEPT_THRESHOLD,
        "signal_timestamps": [],
        "last_daily_summary_date": None,
        "bar_index": 0,
    }


def load_state() -> dict:
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH) as f:
                st = json.load(f)
            base = _default_state()
            base.update(st)
            return base
        except (json.JSONDecodeError, OSError):
            LOG.warning("state.json unreadable, reinitializing")
    return _default_state()


def save_state(state: dict):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, STATE_PATH)


def load_candle_cache() -> dict:
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def save_candle_cache(cache: dict):
    tmp = CACHE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cache, f)
    os.replace(tmp, CACHE_PATH)


def prune_state(state: dict, max_signals: int = 500, max_history: int = 1500):
    state["signals"] = state["signals"][-max_signals:]
    state["history"] = state["history"][-max_history:]
    cutoff = time.time() - 86400
    state["signal_timestamps"] = [t for t in state["signal_timestamps"] if t > cutoff]


# ============================================================================
# REGIME LAYER
# ============================================================================

@dataclass
class RegimeVector:
    btc_bias: str
    btc_strength: float
    vol_percentile: float
    adx_strength: float
    session_weight: float
    noise_index: float
    breadth: float
    trend_dir: str

    def favorability(self, direction: str) -> float:
        align = 1.0 if direction == self.trend_dir else (0.4 if self.trend_dir == "neutral" else -0.3)
        return max(0.0, min(1.0, 0.5 + 0.2 * align + 0.15 * (self.adx_strength / 40)
                             + 0.1 * self.session_weight - 0.15 * self.noise_index))


def session_weight_now() -> float:
    hour = time.gmtime().tm_hour
    if 12 <= hour <= 20:
        return 1.0
    if 6 <= hour < 12 or 20 < hour <= 23:
        return 0.75
    return 0.5


def compute_noise_index(candles: list, lookback: int = 30) -> float:
    window = candles[-lookback:]
    if len(window) < 5:
        return 0.5
    body = sum(abs(c["c"] - c["o"]) for c in window)
    rng = sum(c["h"] - c["l"] for c in window) or 1e-9
    return max(0.0, min(1.0, 1.0 - body / rng))


def compute_btc_regime(btc_ind: dict) -> tuple:
    if btc_ind.get("insufficient"):
        return "neutral", 0.0
    e20, e50, e200 = btc_ind["ema20"][-1], btc_ind["ema50"][-1], btc_ind["ema200"][-1]
    adx = btc_ind["adx"][-1]
    if e20 > e50 > e200:
        return "long", min(1.0, adx / 40)
    if e20 < e50 < e200:
        return "short", min(1.0, adx / 40)
    return "neutral", min(1.0, adx / 40) * 0.5


def compute_breadth(bundles: dict, btc_bias: str) -> float:
    if btc_bias == "neutral" or not bundles:
        return 0.5
    agree = 0
    total = 0
    for bundle in bundles.values():
        ind = bundle.get("struct_ind", {})
        if ind.get("insufficient"):
            continue
        total += 1
        e20, e50 = ind["ema20"][-1], ind["ema50"][-1]
        sym_bias = "long" if e20 > e50 else "short"
        if sym_bias == btc_bias:
            agree += 1
    return agree / total if total else 0.5


def volatility_percentile(atr_pct: float, state: dict, symbol: str) -> float:
    mem = state["regime_memory"].setdefault(symbol, {"atr_pct_history": []})
    hist = mem["atr_pct_history"]
    hist.append(atr_pct)
    mem["atr_pct_history"] = hist[-120:]
    if len(hist) < 10:
        return 0.5
    sorted_hist = sorted(hist)
    rank = sum(1 for h in sorted_hist if h <= atr_pct)
    return rank / len(sorted_hist)


def build_regime_vector(state: dict, symbol: str, struct_ind: dict, exec_candles: list,
                         btc_bias: str, btc_strength: float, breadth: float) -> RegimeVector:
    atr_pct = struct_ind["atr"][-1] / struct_ind["closes"][-1] if struct_ind["closes"][-1] else 0.0
    vol_pct = volatility_percentile(atr_pct, state, symbol)
    adx_strength = struct_ind["adx"][-1]
    noise = compute_noise_index(exec_candles)
    e20, e50 = struct_ind["ema20"][-1], struct_ind["ema50"][-1]
    trend_dir = "long" if e20 > e50 else ("short" if e20 < e50 else "neutral")
    return RegimeVector(
        btc_bias=btc_bias, btc_strength=btc_strength, vol_percentile=vol_pct,
        adx_strength=adx_strength, session_weight=session_weight_now(),
        noise_index=noise, breadth=breadth, trend_dir=trend_dir,
    )


def select_combo(regime: RegimeVector) -> str:
    if regime.vol_percentile > 0.7 and regime.adx_strength < 18:
        return "position"
    if regime.adx_strength >= 22:
        return "intraday"
    return "swing"


# ============================================================================
# STRUCTURE / SMC LAYER
# ============================================================================

@dataclass
class Swing:
    idx: int
    price: float
    kind: str  # "high" | "low"


def find_swings(candles: list, left: int = 2, right: int = 2) -> list:
    swings = []
    for i in range(left, len(candles) - right):
        window_h = [candles[j]["h"] for j in range(i - left, i + right + 1)]
        window_l = [candles[j]["l"] for j in range(i - left, i + right + 1)]
        if candles[i]["h"] == max(window_h):
            swings.append(Swing(i, candles[i]["h"], "high"))
        if candles[i]["l"] == min(window_l):
            swings.append(Swing(i, candles[i]["l"], "low"))
    return swings


@dataclass
class StructureState:
    trend: str
    last_bos_idx: Optional[int]
    last_choch_idx: Optional[int]
    last_swing_high: Optional[float]
    last_swing_low: Optional[float]


def analyze_structure(candles: list, swings: list) -> StructureState:
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    if len(highs) < 2 or len(lows) < 2:
        return StructureState("neutral", None, None,
                               highs[-1].price if highs else None,
                               lows[-1].price if lows else None)
    trend = "neutral"
    last_bos, last_choch = None, None
    if highs[-1].price > highs[-2].price and lows[-1].price > lows[-2].price:
        trend = "long"
        last_bos = highs[-1].idx
    elif highs[-1].price < highs[-2].price and lows[-1].price < lows[-2].price:
        trend = "short"
        last_bos = lows[-1].idx
    if len(highs) >= 2 and len(lows) >= 2:
        # CHoCH: break of the most recent opposite-trend swing
        if trend != "long" and lows[-1].price > highs[-2].price:
            last_choch = lows[-1].idx
        if trend != "short" and highs[-1].price < lows[-2].price:
            last_choch = highs[-1].idx
    return StructureState(trend, last_bos, last_choch, highs[-1].price, lows[-1].price)


@dataclass
class Zone:
    kind: str  # order_block | breaker_block | fvg
    direction: str  # long | short
    top: float
    bottom: float
    idx: int
    tested: bool = False

    def mid(self) -> float:
        return (self.top + self.bottom) / 2

    def contains(self, price: float) -> bool:
        return self.bottom <= price <= self.top


def find_order_blocks(candles: list, atr_vals: list, lookback: int = 60) -> list:
    zones = []
    start = max(1, len(candles) - lookback)
    for i in range(start, len(candles) - 1):
        c, nxt = candles[i], candles[i + 1]
        body = abs(nxt["c"] - nxt["o"])
        if body < 0.6 * atr_vals[i]:
            continue
        if nxt["c"] > nxt["o"] and c["c"] < c["o"]:
            zones.append(Zone("order_block", "long", c["o"], c["l"], i))
        elif nxt["c"] < nxt["o"] and c["c"] > c["o"]:
            zones.append(Zone("order_block", "short", c["h"], c["o"], i))
    return zones


def find_breaker_blocks(candles: list, structure_swings: list, atr_vals: list, lookback: int = 60) -> list:
    zones = []
    highs = [s for s in structure_swings if s.kind == "high" and s.idx >= len(candles) - lookback]
    lows = [s for s in structure_swings if s.kind == "low" and s.idx >= len(candles) - lookback]
    for h in highs:
        for i in range(h.idx + 1, len(candles)):
            if candles[i]["c"] > h.price:
                pre = candles[max(h.idx - 2, 0):h.idx + 1]
                if pre:
                    top = max(c["h"] for c in pre)
                    bottom = min(c["l"] for c in pre)
                    zones.append(Zone("breaker_block", "long", top, bottom, h.idx))
                break
    for l_ in lows:
        for i in range(l_.idx + 1, len(candles)):
            if candles[i]["c"] < l_.price:
                pre = candles[max(l_.idx - 2, 0):l_.idx + 1]
                if pre:
                    top = max(c["h"] for c in pre)
                    bottom = min(c["l"] for c in pre)
                    zones.append(Zone("breaker_block", "short", top, bottom, l_.idx))
                break
    return zones


def find_fvgs(candles: list, lookback: int = 60) -> list:
    zones = []
    start = max(2, len(candles) - lookback)
    for i in range(start, len(candles)):
        a, b, c = candles[i - 2], candles[i - 1], candles[i]
        if c["l"] > a["h"]:
            zones.append(Zone("fvg", "long", c["l"], a["h"], i))
        elif c["h"] < a["l"]:
            zones.append(Zone("fvg", "short", a["l"], c["h"], i))
    return zones


def mark_untested(zones: list, candles: list) -> list:
    out = []
    for z in zones:
        tested = any(z.contains(c["l"]) or z.contains(c["h"]) for c in candles[z.idx + 2:])
        z.tested = tested
        out.append(z)
    return out


def cluster_levels(levels: list, tol_pct: float = 0.0015) -> list:
    if not levels:
        return []
    levels = sorted(levels)
    clusters = [[levels[0]]]
    for lv in levels[1:]:
        if abs(lv - clusters[-1][-1]) / clusters[-1][-1] <= tol_pct:
            clusters[-1].append(lv)
        else:
            clusters.append([lv])
    return [(sum(c) / len(c), len(c)) for c in clusters]


def build_liquidity_pools(swings: list) -> dict:
    highs = [s.price for s in swings if s.kind == "high"]
    lows = [s.price for s in swings if s.kind == "low"]
    return {"resistance": cluster_levels(highs), "support": cluster_levels(lows)}


def detect_sweep(candles: list, pools: dict, direction: str, lookback: int = 10) -> Optional[dict]:
    recent = candles[-lookback:]
    targets = pools["support"] if direction == "long" else pools["resistance"]
    for level, weight in targets:
        for c in recent:
            wick = c["l"] if direction == "long" else c["h"]
            close_ok = c["c"] > level if direction == "long" else c["c"] < level
            pierced = wick < level if direction == "long" else wick > level
            if pierced and close_ok:
                return {"level": level, "weight": weight, "candle": c}
    return None


def premium_discount_zone(candles: list, lookback: int = 50) -> dict:
    window = candles[-lookback:]
    hi = max(c["h"] for c in window)
    lo = min(c["l"] for c in window)
    mid = (hi + lo) / 2
    price = candles[-1]["c"]
    zone = "premium" if price > mid else "discount"
    depth = abs(price - mid) / (hi - lo) if hi > lo else 0.0
    return {"zone": zone, "mid": mid, "high": hi, "low": lo, "depth": depth}


def volume_profile(candles: list, bins: int = VOL_PROFILE_BINS) -> dict:
    hi = max(c["h"] for c in candles)
    lo = min(c["l"] for c in candles)
    if hi <= lo:
        return {"poc": candles[-1]["c"], "vah": hi, "val": lo}
    width = (hi - lo) / bins
    buckets = [0.0] * bins
    for c in candles:
        mid = (c["h"] + c["l"]) / 2
        b = min(bins - 1, max(0, int((mid - lo) / width)))
        buckets[b] += c.get("v", 0.0)
    poc_idx = buckets.index(max(buckets))
    poc = lo + width * (poc_idx + 0.5)
    total = sum(buckets) or 1.0
    order = sorted(range(bins), key=lambda i: -buckets[i])
    covered, sel = 0.0, []
    for i in order:
        covered += buckets[i]
        sel.append(i)
        if covered / total >= 0.68:
            break
    vah = lo + width * (max(sel) + 1)
    val = lo + width * min(sel)
    return {"poc": poc, "vah": vah, "val": val}


@dataclass
class SMCBundle:
    swings: list
    structure: StructureState
    order_blocks: list
    breaker_blocks: list
    fvgs: list
    pools: dict
    pd_zone: dict
    vol_profile: dict


def build_smc_bundle(candles: list, atr_vals: list) -> SMCBundle:
    swings = find_swings(candles)
    structure = analyze_structure(candles, swings)
    obs = mark_untested(find_order_blocks(candles, atr_vals), candles)
    bbs = mark_untested(find_breaker_blocks(candles, swings, atr_vals), candles)
    fvgs = mark_untested(find_fvgs(candles), candles)
    pools = build_liquidity_pools(swings)
    pd_zone = premium_discount_zone(candles)
    vp = volume_profile(candles[-100:] if len(candles) >= 100 else candles)
    return SMCBundle(swings, structure, obs, bbs, fvgs, pools, pd_zone, vp)


# ============================================================================
# CANDIDATE / RISK MANAGEMENT
# ============================================================================

@dataclass
class Candidate:
    symbol: str
    engine: str
    direction: str
    combo: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    raw_confidence: float
    reasons: list = field(default_factory=list)

    def rr1(self) -> float:
        risk = abs(self.entry - self.sl)
        return abs(self.tp1 - self.entry) / risk if risk > 1e-9 else 0.0

    def rr2(self) -> float:
        risk = abs(self.entry - self.sl)
        return abs(self.tp2 - self.entry) / risk if risk > 1e-9 else 0.0


def adaptive_sl_buffer(atr_val: float, vol_percentile: float) -> float:
    base = 0.35 * atr_val
    widen = 1.0 + 0.5 * vol_percentile
    return base * widen


def structure_stop(direction: str, entry: float, invalidation: float, atr_val: float, vol_pct: float) -> float:
    buf = adaptive_sl_buffer(atr_val, vol_pct)
    if direction == "long":
        return min(invalidation - buf, entry - 0.5 * atr_val)
    return max(invalidation + buf, entry + 0.5 * atr_val)


def clip_tp_to_liquidity(entry: float, raw_tp: float, direction: str, pools: dict, vp: dict) -> float:
    candidates = [raw_tp]
    targets = pools["resistance"] if direction == "long" else pools["support"]
    for level, _weight in targets:
        beyond = level > entry if direction == "long" else level < entry
        if beyond:
            candidates.append(level)
    vp_edge = vp["vah"] if direction == "long" else vp["val"]
    if (vp_edge > entry) == (direction == "long"):
        candidates.append(vp_edge)
    if direction == "long":
        viable = [c for c in candidates if c > entry]
        return min(viable) if viable else raw_tp
    viable = [c for c in candidates if c < entry]
    return max(viable) if viable else raw_tp


def build_candidate(symbol: str, engine: str, direction: str, combo: str, entry: float,
                     invalidation: float, atr_val: float, vol_pct: float,
                     pools: dict, vp: dict, target_rr: float,
                     raw_confidence: float, reasons: list) -> Candidate:
    sl = structure_stop(direction, entry, invalidation, atr_val, vol_pct)
    risk = abs(entry - sl)
    raw_tp1 = entry + risk * target_rr * (1 if direction == "long" else -1)
    raw_tp2 = entry + risk * (target_rr * 1.8) * (1 if direction == "long" else -1)
    tp1 = clip_tp_to_liquidity(entry, raw_tp1, direction, pools, vp)
    tp2 = clip_tp_to_liquidity(entry, raw_tp2, direction, pools, vp)
    if direction == "long" and tp1 <= entry:
        tp1 = raw_tp1
    if direction == "short" and tp1 >= entry:
        tp1 = raw_tp1
    return Candidate(symbol, engine, direction, combo, entry, sl, tp1, tp2, raw_confidence, reasons)


def clamp_to_market(cand: Candidate, market_price: float) -> Optional[Candidate]:
    drift = abs(cand.entry - market_price) / market_price if market_price else 1.0
    if drift > 0.01:
        return None
    cand.entry = market_price
    return cand


# ============================================================================
# SPECIALIZED ENGINES
# ============================================================================
# Each engine returns 0 or 1 Candidate. All operate on a shared "ctx" dict
# built once per symbol per scan (see evaluate_symbol) so no engine repeats
# indicator or SMC computation.

def engine_order_block(ctx: dict) -> Optional[Candidate]:
    smc, ind, price = ctx["smc"], ctx["struct_ind"], ctx["price"]
    regime = ctx["regime"]
    fresh_obs = [z for z in smc.order_blocks if not z.tested]
    for z in sorted(fresh_obs, key=lambda z: -z.idx)[:6]:
        if z.direction != regime.trend_dir:
            continue
        if not z.contains(price):
            continue
        conf = 0.55 + 0.15 * (1 - smc.pd_zone["depth"] if z.direction == "long" else smc.pd_zone["depth"])
        conf += 0.1 if smc.structure.trend == z.direction else 0.0
        return build_candidate(
            ctx["symbol"], "order_block", z.direction, ctx["combo_name"], price, z.bottom if z.direction == "long" else z.top,
            ind["atr"][-1], regime.vol_percentile, smc.pools, smc.vol_profile, 1.8, conf,
            [f"untested {z.direction} order block reclaim"],
        )
    return None


def engine_breaker_block(ctx: dict) -> Optional[Candidate]:
    smc, ind, price = ctx["smc"], ctx["struct_ind"], ctx["price"]
    regime = ctx["regime"]
    for z in sorted(smc.breaker_blocks, key=lambda z: -z.idx)[:4]:
        if z.direction != regime.trend_dir or not z.contains(price):
            continue
        conf = 0.6 + 0.1 * min(1.0, regime.adx_strength / 30)
        return build_candidate(
            ctx["symbol"], "breaker_block", z.direction, ctx["combo_name"], price,
            z.bottom if z.direction == "long" else z.top, ind["atr"][-1], regime.vol_percentile,
            smc.pools, smc.vol_profile, 2.0, conf, [f"{z.direction} breaker block retest post structure flip"],
        )
    return None


def engine_fvg_fill(ctx: dict) -> Optional[Candidate]:
    smc, ind, price = ctx["smc"], ctx["struct_ind"], ctx["price"]
    regime = ctx["regime"]
    for z in sorted(smc.fvgs, key=lambda z: -z.idx)[:6]:
        if z.tested or z.direction != regime.trend_dir or not z.contains(price):
            continue
        conf = 0.52 + 0.12 * (1 - regime.noise_index)
        return build_candidate(
            ctx["symbol"], "fvg_fill", z.direction, ctx["combo_name"], price,
            z.bottom if z.direction == "long" else z.top, ind["atr"][-1], regime.vol_percentile,
            smc.pools, smc.vol_profile, 1.6, conf, [f"unfilled {z.direction} fair value gap partial fill"],
        )
    return None


def engine_liquidity_sweep(ctx: dict) -> Optional[Candidate]:
    smc, ind, price = ctx["smc"], ctx["struct_ind"], ctx["price"]
    regime = ctx["regime"]
    for direction in ("long", "short"):
        sweep = detect_sweep(ctx["exec_candles"], smc.pools, direction)
        if not sweep:
            continue
        conf = 0.6 + 0.15 * min(1.0, sweep["weight"] / 3) + 0.1 * (1 - regime.noise_index)
        invalidation = sweep["candle"]["l"] if direction == "long" else sweep["candle"]["h"]
        return build_candidate(
            ctx["symbol"], "liquidity_sweep", direction, ctx["combo_name"], price, invalidation,
            ind["atr"][-1], regime.vol_percentile, smc.pools, smc.vol_profile, 2.2, conf,
            [f"{direction} liquidity sweep + reclaim of {sweep['level']:.4f}"],
        )
    return None


def engine_choch_reversal(ctx: dict) -> Optional[Candidate]:
    smc, ind, price = ctx["smc"], ctx["struct_ind"], ctx["price"]
    regime = ctx["regime"]
    structure = smc.structure
    if structure.last_choch_idx is None:
        return None
    recency = len(ctx["exec_candles"]) - structure.last_choch_idx
    if recency > 8:
        return None
    direction = "long" if structure.trend != "short" else "short"
    invalidation = structure.last_swing_low if direction == "long" else structure.last_swing_high
    if invalidation is None:
        return None
    conf = 0.58 + 0.1 * (1 - recency / 8)
    return build_candidate(
        ctx["symbol"], "choch_reversal", direction, ctx["combo_name"], price, invalidation,
        ind["atr"][-1], regime.vol_percentile, smc.pools, smc.vol_profile, 2.0, conf,
        ["fresh change of character against prior trend"],
    )


def engine_trend_continuation(ctx: dict) -> Optional[Candidate]:
    ind, price, regime = ctx["struct_ind"], ctx["price"], ctx["regime"]
    if regime.adx_strength < 20 or regime.trend_dir == "neutral":
        return None
    e20, e50 = ind["ema20"][-1], ind["ema50"][-1]
    pullback_ok = (abs(price - e20) / price < 0.006)
    if not pullback_ok:
        return None
    r = ind["rsi"][-1]
    if regime.trend_dir == "long" and not (40 <= r <= 60):
        return None
    if regime.trend_dir == "short" and not (40 <= r <= 60):
        return None
    conf = 0.55 + 0.15 * min(1.0, regime.adx_strength / 35)
    invalidation = e50 if regime.trend_dir == "long" else e50
    return build_candidate(
        ctx["symbol"], "trend_continuation", regime.trend_dir, ctx["combo_name"], price, invalidation,
        ind["atr"][-1], regime.vol_percentile, ctx["smc"].pools, ctx["smc"].vol_profile, 2.2, conf,
        ["trend pullback to EMA20 in established directional move"],
    )


def engine_pullback(ctx: dict) -> Optional[Candidate]:
    smc, ind, price, regime = ctx["smc"], ctx["struct_ind"], ctx["price"], ctx["regime"]
    if regime.trend_dir == "neutral":
        return None
    zone = smc.pd_zone
    favorable = (regime.trend_dir == "long" and zone["zone"] == "discount") or \
                (regime.trend_dir == "short" and zone["zone"] == "premium")
    if not favorable or zone["depth"] < 0.15:
        return None
    conf = 0.5 + 0.2 * zone["depth"]
    invalidation = zone["low"] if regime.trend_dir == "long" else zone["high"]
    return build_candidate(
        ctx["symbol"], "pullback", regime.trend_dir, ctx["combo_name"], price, invalidation,
        ind["atr"][-1], regime.vol_percentile, smc.pools, smc.vol_profile, 1.8, conf,
        [f"pullback into {zone['zone']} array with trend"],
    )


def engine_breakout_expansion(ctx: dict) -> Optional[Candidate]:
    ind, price, regime, candles = ctx["struct_ind"], ctx["price"], ctx["regime"], ctx["exec_candles"]
    bb_now, bb_prev = ind["bb_width"][-1], ind["bb_width"][-10] if len(ind["bb_width"]) > 10 else ind["bb_width"][0]
    if bb_prev <= 1e-9 or bb_now / bb_prev < 1.3:
        return None
    vol_now, vol_avg = ind["vols"][-1], ind["vol_sma20"][-1]
    if vol_avg <= 0 or vol_now < 1.4 * vol_avg:
        return None
    recent_high = max(c["h"] for c in candles[-20:-1])
    recent_low = min(c["l"] for c in candles[-20:-1])
    if price > recent_high:
        direction = "long"
        invalidation = recent_high - 0.3 * ind["atr"][-1]
    elif price < recent_low:
        direction = "short"
        invalidation = recent_low + 0.3 * ind["atr"][-1]
    else:
        return None
    conf = 0.55 + 0.15 * min(1.0, (vol_now / vol_avg - 1))
    return build_candidate(
        ctx["symbol"], "breakout_expansion", direction, ctx["combo_name"], price, invalidation,
        ind["atr"][-1], regime.vol_percentile, ctx["smc"].pools, ctx["smc"].vol_profile, 2.4, conf,
        ["volatility expansion breakout with volume confirmation"],
    )


def engine_momentum(ctx: dict) -> Optional[Candidate]:
    ind, price, regime = ctx["struct_ind"], ctx["price"], ctx["regime"]
    r = ind["rsi"]
    if len(r) < 5:
        return None
    slope = r[-1] - r[-4]
    if r[-1] > 60 and slope > 8 and regime.trend_dir == "long":
        direction = "long"
    elif r[-1] < 40 and slope < -8 and regime.trend_dir == "short":
        direction = "short"
    else:
        return None
    invalidation = ind["ema20"][-1]
    conf = 0.52 + 0.1 * min(1.0, abs(slope) / 20)
    return build_candidate(
        ctx["symbol"], "momentum", direction, ctx["combo_name"], price, invalidation,
        ind["atr"][-1], regime.vol_percentile, ctx["smc"].pools, ctx["smc"].vol_profile, 1.8, conf,
        ["RSI momentum surge aligned with trend"],
    )


def engine_mean_reversion(ctx: dict) -> Optional[Candidate]:
    ind, price, regime = ctx["struct_ind"], ctx["price"], ctx["regime"]
    if regime.adx_strength > 18:
        return None
    r = ind["rsi"][-1]
    e20 = ind["ema20"][-1]
    dist = abs(price - e20) / price
    if r > 72 and dist > 0.012:
        direction = "short"
        invalidation = price + 1.2 * ind["atr"][-1]
    elif r < 28 and dist > 0.012:
        direction = "long"
        invalidation = price - 1.2 * ind["atr"][-1]
    else:
        return None
    conf = 0.5 + 0.15 * min(1.0, dist / 0.03)
    return build_candidate(
        ctx["symbol"], "mean_reversion", direction, ctx["combo_name"], price, invalidation,
        ind["atr"][-1], regime.vol_percentile, ctx["smc"].pools, ctx["smc"].vol_profile, 1.5, conf,
        ["overextension from mean in low-ADX range regime"],
    )


def engine_range(ctx: dict) -> Optional[Candidate]:
    ind, price, regime, candles = ctx["struct_ind"], ctx["price"], ctx["regime"], ctx["exec_candles"]
    if regime.adx_strength > 16:
        return None
    window = candles[-30:]
    hi = max(c["h"] for c in window)
    lo = min(c["l"] for c in window)
    if hi <= lo:
        return None
    pos = (price - lo) / (hi - lo)
    if pos < 0.1:
        direction, invalidation = "long", lo - 0.4 * ind["atr"][-1]
    elif pos > 0.9:
        direction, invalidation = "short", hi + 0.4 * ind["atr"][-1]
    else:
        return None
    conf = 0.5 + 0.15 * (1 - regime.noise_index)
    return build_candidate(
        ctx["symbol"], "range", direction, ctx["combo_name"], price, invalidation,
        ind["atr"][-1], regime.vol_percentile, ctx["smc"].pools, ctx["smc"].vol_profile, 1.4, conf,
        ["range boundary fade in low-volatility consolidation"],
    )


ENGINES = [
    engine_order_block, engine_breaker_block, engine_fvg_fill, engine_liquidity_sweep,
    engine_choch_reversal, engine_trend_continuation, engine_pullback,
    engine_breakout_expansion, engine_momentum, engine_mean_reversion, engine_range,
]


# ============================================================================
# DECISION ENGINE (SCORING, WEIGHTING, DEDUPLICATION, GOVERNOR)
# ============================================================================

def get_engine_weight(state: dict, engine: str) -> float:
    return state["engine_weights"].get(engine, {}).get("weight", 1.0)


def logistic(x: float) -> float:
    try:
        return 1 / (1 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0


def calibrate_confidence(state: dict, engine: str, raw_conf: float) -> float:
    bucket = state["confidence_calibration"].get(engine)
    if not bucket or bucket.get("n", 0) < 8:
        return raw_conf
    observed_wr = bucket["win_rate"]
    return 0.6 * raw_conf + 0.4 * observed_wr


def score_candidate(cand: Candidate, regime: RegimeVector, state: dict, breadth: float) -> float:
    engine_w = get_engine_weight(state, cand.engine)
    calibrated = calibrate_confidence(state, cand.engine, cand.raw_confidence)
    regime_fit = regime.favorability(cand.direction)
    mtf_align = 1.0 if regime.trend_dir == cand.direction else 0.5
    rr_score = min(1.0, cand.rr2() / 3.0)
    breadth_score = breadth if cand.direction == regime.btc_bias else (1 - breadth)
    x = (
        2.2 * (calibrated - 0.5)
        + 1.4 * (engine_w - 1.0)
        + 1.1 * (regime_fit - 0.5)
        + 0.7 * (mtf_align - 0.5)
        + 0.9 * (rr_score - 0.4)
        + 0.5 * (breadth_score - 0.5)
        - 0.8 * regime.noise_index
    )
    return logistic(x)


def compute_returns(closes: list, lookback: int) -> list:
    window = closes[-lookback:]
    return [(window[i] / window[i - 1] - 1) for i in range(1, len(window))] if len(window) > 1 else []


def pearson(a: list, b: list) -> float:
    n = min(len(a), len(b))
    if n < 5:
        return 0.0
    a, b = a[-n:], b[-n:]
    try:
        return statistics.correlation(a, b)
    except (statistics.StatisticsError, ZeroDivisionError):
        return 0.0


def build_correlation_clusters(returns_by_symbol: dict) -> list:
    symbols = list(returns_by_symbol.keys())
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

    for i, s1 in enumerate(symbols):
        for s2 in symbols[i + 1:]:
            if abs(pearson(returns_by_symbol[s1], returns_by_symbol[s2])) > 0.75:
                union(s1, s2)
    clusters = {}
    for s in symbols:
        clusters.setdefault(find(s), set()).add(s)
    return list(clusters.values())


def dedup_correlated(ranked: list, clusters: list) -> list:
    def cluster_of(sym):
        for cl in clusters:
            if sym in cl:
                return frozenset(cl)
        return frozenset([sym])

    seen = set()
    out = []
    for cand, score in ranked:
        key = (cluster_of(cand.symbol), cand.direction)
        if key in seen:
            continue
        seen.add(key)
        out.append((cand, score))
    return out


def passes_hard_filters(cand: Candidate, regime: RegimeVector) -> tuple:
    if cand.rr1() < 1.2:
        return False, "rr1_too_low"
    if regime.noise_index > 0.85:
        return False, "excess_noise"
    if regime.vol_percentile > 0.97:
        return False, "volatility_extreme"
    return True, "ok"


def check_cooldown(state: dict, symbol: str, direction: str, bar_index: int) -> bool:
    key = f"{symbol}:{direction}"
    last = state["cooldowns"].get(key)
    return last is None or (bar_index - last) >= COOLDOWN_BARS_15M


def update_cooldown(state: dict, symbol: str, direction: str, bar_index: int):
    state["cooldowns"][f"{symbol}:{direction}"] = bar_index


def is_recent_duplicate(state: dict, symbol: str, direction: str, entry: float) -> bool:
    for sig in state["signals"][-40:]:
        if sig["symbol"] == symbol and sig["direction"] == direction and sig["status"] in ("active", "activated"):
            if abs(sig["entry"] - entry) / entry <= DUPLICATE_ENTRY_TOL_PCT:
                return True
    return False


def estimate_signals_last_24h(state: dict) -> int:
    cutoff = time.time() - 86400
    return sum(1 for t in state["signal_timestamps"] if t > cutoff)


def governor_adjust_threshold(state: dict):
    lo, hi = TARGET_SIGNALS_PER_DAY
    count = estimate_signals_last_24h(state)
    thr = state.get("accept_threshold", BASE_ACCEPT_THRESHOLD)
    if count < lo:
        thr = max(0.45, thr - 0.01)
    elif count > hi:
        thr = min(0.85, thr + 0.015)
    else:
        thr = thr + (BASE_ACCEPT_THRESHOLD - thr) * 0.1
    state["accept_threshold"] = thr
    return thr


def count_open_same_direction(state: dict, direction: str) -> int:
    return sum(1 for s in state["signals"] if s["status"] in ("active", "activated") and s["direction"] == direction)


def count_open_for_symbol(state: dict, symbol: str) -> int:
    return sum(1 for s in state["signals"] if s["status"] in ("active", "activated") and s["symbol"] == symbol)


def decision_engine(all_candidates: list, regimes: dict, state: dict, breadth: float,
                     returns_by_symbol: dict) -> list:
    scored = []
    for cand in all_candidates:
        regime = regimes[cand.symbol]
        ok, _reason = passes_hard_filters(cand, regime)
        if not ok:
            continue
        if not check_cooldown(state, cand.symbol, cand.direction, state["bar_index"]):
            continue
        if is_recent_duplicate(state, cand.symbol, cand.direction, cand.entry):
            continue
        if count_open_for_symbol(state, cand.symbol) >= MAX_CONCURRENT_PER_SYMBOL:
            continue
        if count_open_same_direction(state, cand.direction) >= MAX_CONCURRENT_SAME_DIRECTION:
            continue
        score = score_candidate(cand, regime, state, breadth)
        scored.append((cand, score))

    scored.sort(key=lambda pair: -pair[1])
    best_per_symbol = {}
    for cand, score in scored:
        if cand.symbol not in best_per_symbol or score > best_per_symbol[cand.symbol][1]:
            best_per_symbol[cand.symbol] = (cand, score)
    ranked = sorted(best_per_symbol.values(), key=lambda pair: -pair[1])

    clusters = build_correlation_clusters(returns_by_symbol)
    ranked = dedup_correlated(ranked, clusters)

    threshold = governor_adjust_threshold(state)
    accepted = [(c, s) for c, s in ranked if s >= threshold]
    return accepted


# ============================================================================
# TELEGRAM LAYER
# ============================================================================

def tg_escape(value) -> str:
    text = str(value)
    for ch in "_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


def fmt_px(v: float) -> str:
    if v >= 100:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:,.4f}"
    return f"{v:.6f}"


def confidence_bar(conf: float) -> str:
    filled = round(conf * 10)
    return "\u2588" * filled + "\u2591" * (10 - filled)


def grade_for_confidence(conf: float) -> str:
    if conf >= 0.85:
        return "A+"
    if conf >= 0.75:
        return "A"
    if conf >= 0.68:
        return "B"
    return "C"


def format_signal(cand: Candidate, score: float) -> str:
    grade = grade_for_confidence(score)
    arrow = "\U0001F7E2 LONG" if cand.direction == "long" else "\U0001F534 SHORT"
    reasons = "\n".join(f"  \u2022 {tg_escape(r)}" for r in cand.reasons)
    return (
        f"*{tg_escape(ENGINE_NAME)} v{ENGINE_VERSION}*\n"
        f"{arrow}  `{tg_escape(cand.symbol)}`  \\[{tg_escape(cand.engine)}\\]\n"
        f"Grade: *{grade}*  Confidence: {confidence_bar(score)} {score*100:.0f}%\n"
        f"Combo: {tg_escape(cand.combo)}\n\n"
        f"`Entry: {fmt_px(cand.entry)}`\n"
        f"`SL:    {fmt_px(cand.sl)}`\n"
        f"`TP1:   {fmt_px(cand.tp1)}`  \\(RR {cand.rr1():.2f}\\)\n"
        f"`TP2:   {fmt_px(cand.tp2)}`  \\(RR {cand.rr2():.2f}\\)\n\n"
        f"{reasons}"
    )


def send_telegram(text: str) -> Optional[int]:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        LOG.info("Telegram not configured, skipping send")
        return None
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = json.dumps({"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "MarkdownV2"}).encode()
    try:
        req = urllib.request.Request(url, data=payload, method="POST",
                                      headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            return data.get("result", {}).get("message_id")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
        LOG.warning("telegram send failed: %s", exc)
        return None


def reply_telegram(text: str, reply_to: Optional[int]) -> Optional[int]:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return None
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    body = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "MarkdownV2"}
    if reply_to:
        body["reply_to_message_id"] = reply_to
    try:
        req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                      headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            return data.get("result", {}).get("message_id")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
        LOG.warning("telegram reply failed: %s", exc)
        return None


def react_telegram(message_id: Optional[int], emoji: str):
    if not TG_BOT_TOKEN or not TG_CHAT_ID or not message_id:
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/setMessageReaction"
    body = {"chat_id": TG_CHAT_ID, "message_id": message_id, "reaction": [{"type": "emoji", "emoji": emoji}]}
    try:
        req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                      headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=8)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError):
        pass


# ============================================================================
# SIGNAL LIFECYCLE
# ============================================================================

def record_signal(state: dict, cand: Candidate, score: float, msg_id: Optional[int]):
    sig = {
        "id": f"{cand.symbol}-{int(time.time())}",
        "symbol": cand.symbol, "engine": cand.engine, "direction": cand.direction,
        "combo": cand.combo, "entry": cand.entry, "sl": cand.sl, "tp1": cand.tp1, "tp2": cand.tp2,
        "confidence": score, "opened_at": time.time(), "status": "active",
        "message_id": msg_id, "tp1_hit": False, "reasons": cand.reasons,
        "bar_index_opened": state["bar_index"],
    }
    state["signals"].append(sig)
    state["signal_timestamps"].append(time.time())
    update_cooldown(state, cand.symbol, cand.direction, state["bar_index"])


def _r_multiple(sig: dict, price: float) -> float:
    risk = abs(sig["entry"] - sig["sl"])
    if risk < 1e-9:
        return 0.0
    move = (price - sig["entry"]) if sig["direction"] == "long" else (sig["entry"] - price)
    return move / risk


def _close_out(state: dict, sig: dict, result: str, price: float):
    sig["status"] = "closed"
    sig["closed_at"] = time.time()
    sig["close_price"] = price
    sig["result"] = result
    sig["r_multiple"] = _r_multiple(sig, price)
    state["history"].append(dict(sig))
    reply_telegram(f"*{tg_escape(sig['symbol'])}* closed: *{tg_escape(result)}* @ `{fmt_px(price)}` "
                    f"\\({sig['r_multiple']:.2f}R\\)", sig.get("message_id"))
    react_telegram(sig.get("message_id"), "\U0001F44D" if sig["r_multiple"] > 0 else "\U0001F44E")


def check_active_signals(state: dict, snapshot: dict):
    for sig in state["signals"]:
        if sig["status"] not in ("active", "activated"):
            continue
        info = snapshot.get(sig["symbol"])
        if not info:
            continue
        price = info["mark"]
        direction = sig["direction"]
        if sig["status"] == "active":
            touched = (price <= sig["entry"]) if direction == "long" and price <= sig["entry"] else \
                      (price >= sig["entry"]) if direction == "short" and price >= sig["entry"] else False
            if abs(price - sig["entry"]) / sig["entry"] < 0.001 or touched:
                sig["status"] = "activated"
                reply_telegram(f"\u2705 *{tg_escape(sig['symbol'])}* activated @ `{fmt_px(price)}`",
                               sig.get("message_id"))
                continue
        hit_sl = price <= sig["sl"] if direction == "long" else price >= sig["sl"]
        hit_tp1 = price >= sig["tp1"] if direction == "long" else price <= sig["tp1"]
        hit_tp2 = price >= sig["tp2"] if direction == "long" else price <= sig["tp2"]
        if hit_sl:
            _close_out(state, sig, "SL" if not sig["tp1_hit"] else "BE_STOP", price)
        elif hit_tp2:
            _close_out(state, sig, "TP2", price)
        elif hit_tp1 and not sig["tp1_hit"]:
            sig["tp1_hit"] = True
            sig["sl"] = sig["entry"]
            reply_telegram(f"\U0001F3AF *{tg_escape(sig['symbol'])}* TP1 hit @ `{fmt_px(price)}` \u2014 SL moved to breakeven",
                           sig.get("message_id"))
            react_telegram(sig.get("message_id"), "\U0001F525")


# ============================================================================
# LEARNING LAYER
# ============================================================================

def update_engine_weights(state: dict):
    by_engine = {}
    for sig in state["history"][-400:]:
        by_engine.setdefault(sig["engine"], []).append(sig["r_multiple"])
    for engine, rs in by_engine.items():
        if len(rs) < 5:
            continue
        expectancy = sum(rs) / len(rs)
        win_rate = sum(1 for r in rs if r > 0) / len(rs)
        target_weight = max(0.4, min(1.8, 1.0 + 0.35 * expectancy + 0.3 * (win_rate - 0.5)))
        prior = state["engine_weights"].get(engine, {"weight": 1.0})
        smoothed = prior["weight"] * 0.85 + target_weight * 0.15
        state["engine_weights"][engine] = {"weight": smoothed, "n": len(rs), "expectancy": expectancy, "win_rate": win_rate}


def update_confidence_calibration(state: dict):
    by_engine = {}
    for sig in state["history"][-400:]:
        by_engine.setdefault(sig["engine"], []).append(sig)
    for engine, sigs in by_engine.items():
        if len(sigs) < 5:
            continue
        wins = sum(1 for s in sigs if s["r_multiple"] > 0)
        win_rate = wins / len(sigs)
        prior = state["confidence_calibration"].get(engine, {"win_rate": win_rate, "n": 0})
        smoothed = prior["win_rate"] * 0.8 + win_rate * 0.2
        state["confidence_calibration"][engine] = {"win_rate": smoothed, "n": len(sigs)}


def run_learning_cycle(state: dict):
    update_engine_weights(state)
    update_confidence_calibration(state)


# ============================================================================
# DAILY SUMMARY
# ============================================================================

def generate_daily_summary(state: dict) -> str:
    today_cutoff = time.time() - 86400
    recent = [s for s in state["history"] if s.get("closed_at", 0) > today_cutoff]
    if not recent:
        return f"*{tg_escape(ENGINE_NAME)} Daily Summary*\nNo closed trades in the last 24h\\."
    wins = sum(1 for s in recent if s["r_multiple"] > 0)
    total_r = sum(s["r_multiple"] for s in recent)
    win_rate = wins / len(recent) * 100
    by_engine = {}
    for s in recent:
        by_engine.setdefault(s["engine"], []).append(s["r_multiple"])
    best = max(by_engine.items(), key=lambda kv: sum(kv[1]) / len(kv[1])) if by_engine else None
    worst = min(by_engine.items(), key=lambda kv: sum(kv[1]) / len(kv[1])) if by_engine else None
    lines = [
        f"*{tg_escape(ENGINE_NAME)} v{ENGINE_VERSION} Daily Summary*",
        f"Trades closed: {len(recent)}  Win rate: {win_rate:.0f}%  Total: {total_r:.2f}R",
        f"Accept threshold: {state.get('accept_threshold', BASE_ACCEPT_THRESHOLD):.2f}",
    ]
    if best:
        lines.append(f"Best engine: {tg_escape(best[0])} \\({sum(best[1])/len(best[1]):.2f}R avg\\)")
    if worst:
        lines.append(f"Worst engine: {tg_escape(worst[0])} \\({sum(worst[1])/len(worst[1]):.2f}R avg\\)")
    return "\n".join(lines)


def maybe_send_daily_summary(state: dict):
    now = time.gmtime()
    today_str = time.strftime("%Y-%m-%d", now)
    if now.tm_hour == DAILY_SUMMARY_HOUR_UTC and state.get("last_daily_summary_date") != today_str:
        send_telegram(generate_daily_summary(state))
        state["last_daily_summary_date"] = today_str


# ============================================================================
# ORCHESTRATION
# ============================================================================

def _prefetch(symbol: str, cache: dict) -> tuple:
    try:
        bundle = {}
        for tf_key in ("bias", "struct", "exec"):
            pass
        needed_tfs = {"15m", "1h", "4h", "12h", "1d"}
        bundle["candles"] = {tf: get_candles(symbol, tf, CANDLE_COUNT[tf], cache) for tf in needed_tfs}
        return symbol, bundle
    except Exception as exc:  # noqa: BLE001
        LOG.warning("prefetch failed for %s: %s", symbol, exc)
        return symbol, None


def evaluate_symbol(symbol: str, bundle: dict, state: dict, btc_bias: str, btc_strength: float,
                     breadth: float) -> list:
    candles_by_tf = bundle["candles"]
    struct_candles = candles_by_tf.get("1h", [])
    exec_candles = candles_by_tf.get("15m", [])
    if len(struct_candles) < 60 or len(exec_candles) < 60:
        return []
    struct_ind = compute_indicators(struct_candles)
    exec_ind = compute_indicators(exec_candles)
    if struct_ind.get("insufficient") or exec_ind.get("insufficient"):
        return []
    regime = build_regime_vector(state, symbol, struct_ind, exec_candles, btc_bias, btc_strength, breadth)
    combo_name = select_combo(regime)
    smc = build_smc_bundle(exec_candles, exec_ind["atr"])
    price = exec_candles[-1]["c"]
    ctx = {
        "symbol": symbol, "struct_ind": exec_ind, "exec_candles": exec_candles,
        "smc": smc, "price": price, "regime": regime, "combo_name": combo_name,
    }
    candidates = []
    for engine_fn in ENGINES:
        try:
            cand = engine_fn(ctx)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("engine %s failed for %s: %s", engine_fn.__name__, symbol, exc)
            continue
        if cand:
            candidates.append(cand)
    return candidates


def run_scan():
    state = load_state()
    cache = load_candle_cache()
    state["bar_index"] = state.get("bar_index", 0) + 1

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_prefetch, sym, cache): sym for sym in WATCHLIST}
        bundles = {}
        for fut in as_completed(futures):
            symbol, bundle = fut.result()
            if bundle:
                bundles[symbol] = bundle

    save_candle_cache(cache)

    btc_bundle = bundles.get("BTC")
    btc_bias, btc_strength = "neutral", 0.0
    if btc_bundle:
        btc_ind = compute_indicators(btc_bundle["candles"].get("1h", []))
        btc_bias, btc_strength = compute_btc_regime(btc_ind)

    struct_ind_cache = {}
    for symbol, bundle in bundles.items():
        ind = compute_indicators(bundle["candles"].get("1h", []))
        struct_ind_cache[symbol] = {"struct_ind": ind}
    breadth = compute_breadth(struct_ind_cache, btc_bias)

    all_candidates = []
    regimes = {}
    returns_by_symbol = {}
    for symbol, bundle in bundles.items():
        struct_candles = bundle["candles"].get("1h", [])
        if len(struct_candles) < 30:
            continue
        closes = [c["c"] for c in struct_candles]
        returns_by_symbol[symbol] = compute_returns(closes, 60)
        cands = evaluate_symbol(symbol, bundle, state, btc_bias, btc_strength, breadth)
        for c in cands:
            all_candidates.append(c)
        exec_ind = compute_indicators(bundle["candles"].get("1h", []))
        if not exec_ind.get("insufficient"):
            regimes[symbol] = build_regime_vector(state, symbol, exec_ind, bundle["candles"].get("15m", []),
                                                   btc_bias, btc_strength, breadth)

    accepted = decision_engine(all_candidates, regimes, state, breadth, returns_by_symbol)

    snapshot = get_market_snapshot()
    for cand, score in accepted:
        info = snapshot.get(cand.symbol)
        market_price = info["mark"] if info else cand.entry
        clamped = clamp_to_market(cand, market_price)
        if not clamped:
            continue
        text = format_signal(clamped, score)
        msg_id = send_telegram(text)
        record_signal(state, clamped, score, msg_id)
        LOG.info("signal emitted %s %s %s score=%.2f", clamped.symbol, clamped.engine, clamped.direction, score)

    if snapshot:
        check_active_signals(state, snapshot)

    run_learning_cycle(state)
    maybe_send_daily_summary(state)
    prune_state(state)
    save_state(state)


def main():
    try:
        run_scan()
    except Exception as exc:  # noqa: BLE001
        LOG.exception("run_scan failed: %s", exc)
        raise


if __name__ == "__main__":
    main()
