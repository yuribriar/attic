#!/usr/bin/env python3
"""
VANTAGE ADAPTIVE SIGNAL ENGINE
================================
Version: 1.0.0

Institutional-style, multi-engine adaptive signal generator for Hyperliquid
perpetuals. Built from a comparative gap analysis of two prior reference
engines (KESTREL v1.0.1, AXIS v2.1.0) -- see GAP_ANALYSIS.md for the full
writeup. No code from either reference is reused; this is an independent
implementation.

Design summary (full rationale in ARCHITECTURE.md / STRATEGY.md):
  - 14 independent specialized engines (SMC, Trend Continuation, Breakout,
    Pullback, Liquidity Sweep, Order Block, Breaker Block, Fair Value Gap,
    Momentum, Reversal, Mean Reversion, Range, Volatility Expansion, plus
    a 15th "VWAP Reversion" engine added during gap analysis) each emit
    zero or more independent candidate Signals with their own direction,
    entry/SL/TP, confidence, expected RR, confluences and regime fit.
  - A single Decision Engine ranks all candidates using adaptively-weighted
    per-engine historical performance (stored in state.json), regime fit,
    MTF alignment, EV and RR, then de-duplicates by symbol/direction and by
    return-correlation cluster before emitting the top N per scan.
  - An adaptive frequency governor nudges the acceptance threshold toward a
    5-10 signal/day band using a slow EMA of realized daily signal count,
    never forcing trades to fill a quota.
  - A learning system updates per (engine, regime, asset, timeframe)
    statistics after every trade resolution and feeds those stats back into
    engine weighting and confidence calibration.
  - Structure-based, candle-verified risk management: SL/TP validated only
    against candle highs/lows, never mid-price or live price.
  - Telegram integration with reply-threaded status updates and an 08:00 UTC
    daily summary.

Runs as a single scan per invocation (designed for a 15-minute external
scheduler, e.g. GitHub Actions cron). All state persists in state.json
(operational/learning state) and candle_cache.json (shared OHLCV cache),
both read and written atomically on every run.

Secrets (HYPERLIQUID is public read-only so no key is required for market
data; Telegram credentials are required) are read exclusively from
environment variables -- never hardcoded:
  TG_BOT_TOKEN   Telegram bot token
  TG_CHAT_ID     Telegram chat/channel id to post to
Optional:
  HL_API_URL, STATE_PATH, CANDLE_CACHE_PATH, SCAN_WORKERS, LOG_LEVEL,
  MAX_SIGNALS_PER_SCAN, WATCHLIST (comma separated overrides default list)
"""

from __future__ import annotations

import json
import logging
import math
import os
import statistics
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

ENGINE_NAME = "VANTAGE"
ENGINE_VERSION = "1.0.0"

# ============================================================================
# SECTION 1: CONFIGURATION
# ============================================================================
# Engineering decision: all secrets come from environment variables so the
# script is safe to commit and run unattended under GitHub Actions secrets.

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")

HL_API_URL = os.environ.get("HL_API_URL", "https://api.hyperliquid.xyz/info")
STATE_PATH = os.environ.get("STATE_PATH", "state.json")
CANDLE_CACHE_PATH = os.environ.get("CANDLE_CACHE_PATH", "candle_cache.json")
SCAN_WORKERS = int(os.environ.get("SCAN_WORKERS", "4"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
MAX_SIGNALS_PER_SCAN = int(os.environ.get("MAX_SIGNALS_PER_SCAN", "3"))

# Engineering decision: default watchlist mirrors both references' liquid
# Hyperliquid perpetual majors/alts; overridable via WATCHLIST env var so
# the same file works for any account without code changes.
_DEFAULT_WATCHLIST = [
    "BTC", "ETH", "HYPE", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX",
    "LINK", "SUI", "NEAR", "DOT", "AAVE", "LTC", "APT", "ONDO", "TAO",
    "UNI", "TRX", "BCH", "XLM", "PENDLE",
]
WATCHLIST = [s.strip().upper() for s in os.environ.get(
    "WATCHLIST", ",".join(_DEFAULT_WATCHLIST)).split(",") if s.strip()]

# Engineering decision: 15m minimum per spec. LTF=15m for execution timing,
# MTF=1h for confirmation, HTF=4h for structure/bias, D1 for macro context.
TF_LTF = "15m"
TF_MTF = "1h"
TF_HTF = "4h"
TF_D1 = "1d"
TF_STACK = [TF_LTF, TF_MTF, TF_HTF, TF_D1]
TF_BAR_MS = {"15m": 15 * 60_000, "30m": 30 * 60_000, "1h": 3_600_000,
             "2h": 7_200_000, "4h": 14_400_000, "8h": 28_800_000,
             "12h": 43_200_000, "1d": 86_400_000}
CANDLES_PER_TF = 200  # bars fetched per timeframe; bounded for GH Actions runtime

RSI_LEN = 14
ATR_LEN = 14
ADX_LEN = 14
BB_LEN = 20
BB_MULT = 2.0
SWING_LEFT_RIGHT = 2
LIQUIDITY_EQ_TOL_PCT = 0.0015
CORRELATION_DEDUP_THRESHOLD = 0.80

ENGINE_LIST = [
    "smc", "trend_continuation", "breakout", "pullback", "liquidity_sweep",
    "order_block", "breaker_block", "fair_value_gap", "momentum",
    "reversal", "mean_reversion", "range_trading", "volatility_expansion",
    "vwap_reversion",
]

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(ENGINE_NAME)


# ============================================================================
# SECTION 2: GENERIC MATH / INDICATOR UTILITIES (shared by every engine)
# ============================================================================

def safe(v, fb=0.0):
    try:
        f = float(v)
        return f if math.isfinite(f) else fb
    except (TypeError, ValueError):
        return fb


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b not in (0, 0.0) else default


def ema(vals: list[float], period: int) -> list[float]:
    if not vals:
        return []
    k = 2 / (period + 1)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def sma(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        lo = max(0, i - period + 1)
        window = vals[lo:i + 1]
        out.append(sum(window) / len(window))
    return out


def stdev(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        lo = max(0, i - period + 1)
        window = vals[lo:i + 1]
        out.append(statistics.pstdev(window) if len(window) > 1 else 0.0)
    return out


def rsi(closes: list[float], period: int = RSI_LEN) -> list[float]:
    if len(closes) < 2:
        return [50.0] * len(closes)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g, avg_l = gains[1], losses[1]
    out = [50.0, 50.0]
    for i in range(2, len(closes)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs = safe_div(avg_g, avg_l, default=100.0)
        out.append(100 - 100 / (1 + rs) if avg_l > 0 else 100.0)
    while len(out) < len(closes):
        out.append(50.0)
    return out


def true_range(c: list[dict], i: int) -> float:
    if i == 0:
        return c[i]["h"] - c[i]["l"]
    return max(c[i]["h"] - c[i]["l"], abs(c[i]["h"] - c[i - 1]["c"]),
               abs(c[i]["l"] - c[i - 1]["c"]))


def atr_series(candles: list[dict], period: int = ATR_LEN) -> list[float]:
    if not candles:
        return []
    trs = [true_range(candles, i) for i in range(len(candles))]
    out = [trs[0]]
    for i in range(1, len(trs)):
        out.append((out[-1] * (period - 1) + trs[i]) / period)
    return out


def adx_series(candles: list[dict], period: int = ADX_LEN) -> list[float]:
    n = len(candles)
    if n < 2:
        return [0.0] * n
    plus_dm, minus_dm, trs = [0.0], [0.0], [true_range(candles, 0)]
    for i in range(1, n):
        up = candles[i]["h"] - candles[i - 1]["h"]
        down = candles[i - 1]["l"] - candles[i]["l"]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        trs.append(true_range(candles, i))

    def wilder(vals):
        out = [vals[0]]
        for v in vals[1:]:
            out.append(out[-1] - out[-1] / period + v)
        return out

    atr_w = wilder(trs)
    pdi = [100 * safe_div(p, a) for p, a in zip(wilder(plus_dm), atr_w)]
    mdi = [100 * safe_div(m, a) for m, a in zip(wilder(minus_dm), atr_w)]
    dx = [100 * safe_div(abs(p - m), (p + m)) for p, m in zip(pdi, mdi)]
    out = [dx[0]]
    for v in dx[1:]:
        out.append((out[-1] * (period - 1) + v) / period)
    return out


def bb_width_pct(closes: list[float], period: int = BB_LEN, mult: float = BB_MULT) -> list[float]:
    mid = sma(closes, period)
    sd = stdev(closes, period)
    return [safe_div(2 * mult * s, m) for m, s in zip(mid, sd)]


def percentile_rank(vals: list[float], x: float) -> float:
    if not vals:
        return 50.0
    below = sum(1 for v in vals if v <= x)
    return 100 * below / len(vals)


def compute_returns(closes: list[float], lookback: int) -> list[float]:
    window = closes[-lookback:] if len(closes) >= lookback else closes
    return [safe_div(window[i] - window[i - 1], window[i - 1]) for i in range(1, len(window))]


def pearson(a: list[float], b: list[float]) -> float:
    n = min(len(a), len(b))
    if n < 5:
        return 0.0
    a, b = a[-n:], b[-n:]
    try:
        return statistics.correlation(a, b)
    except (statistics.StatisticsError, ValueError):
        return 0.0


def vwap_series(candles: list[dict]) -> list[float]:
    cum_pv, cum_v, out = 0.0, 0.0, []
    for c in candles:
        typical = (c["h"] + c["l"] + c["c"]) / 3
        cum_pv += typical * c["v"]
        cum_v += c["v"]
        out.append(safe_div(cum_pv, cum_v, default=typical))
    return out


def compute_indicators(candles: list[dict]) -> dict:
    closes = [c["c"] for c in candles]
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
    return {
        "closes": closes, "highs": highs, "lows": lows,
        "ema20": ema(closes, 20), "ema50": ema(closes, 50), "ema200": ema(closes, 200),
        "rsi": rsi(closes), "atr": atr_series(candles), "adx": adx_series(candles),
        "bb_width": bb_width_pct(closes), "vwap": vwap_series(candles),
        "vol_sma20": sma([c["v"] for c in candles], 20),
    }


# ============================================================================
# SECTION 3: DATA LAYER (Hyperliquid client, shared candle cache)
# ============================================================================

class RateLimiter:
    """Simple fixed-interval throttle; keeps us comfortably under Hyperliquid's
    public info-endpoint rate limits without needing per-weight accounting."""

    def __init__(self, min_interval_s: float = 0.20):
        self.min_interval_s = min_interval_s
        self._last = 0.0

    def wait(self):
        now = time.monotonic()
        delta = now - self._last
        if delta < self.min_interval_s:
            time.sleep(self.min_interval_s - delta)
        self._last = time.monotonic()


_rate_limiter = RateLimiter()


def hl_post(payload: dict, retries: int = 4, timeout: int = 12) -> Optional[dict | list]:
    backoff = 0.75
    for attempt in range(retries):
        _rate_limiter.wait()
        try:
            resp = requests.post(HL_API_URL, json=payload, timeout=timeout,
                                  headers={"Content-Type": "application/json"})
            if resp.status_code == 429:
                time.sleep(backoff)
                backoff *= 2
                continue
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            log.warning("hl_post attempt %d failed: %s", attempt + 1, exc)
            time.sleep(backoff)
            backoff *= 2
    log.error("hl_post exhausted retries for payload type=%s", payload.get("type"))
    return None


def get_meta_universe() -> list[str]:
    data = hl_post({"type": "meta"})
    if not data or "universe" not in data:
        return []
    return [a["name"] for a in data["universe"]]


def _normalize_candles(raw: list[dict]) -> list[dict]:
    out = []
    for r in raw:
        try:
            out.append({
                "t": int(r["t"]), "o": float(r["o"]), "h": float(r["h"]),
                "l": float(r["l"]), "c": float(r["c"]), "v": float(r["v"]),
            })
        except (KeyError, TypeError, ValueError):
            continue
    out.sort(key=lambda x: x["t"])
    return out


def filter_closed(candles: list[dict], interval: str, reference_ms: int) -> list[dict]:
    bar_ms = TF_BAR_MS.get(interval, 900_000)
    current_open = (reference_ms // bar_ms) * bar_ms
    return [c for c in candles if c["t"] < current_open]


def fetch_candles(symbol: str, interval: str, n: int, reference_ms: int,
                   cache: dict) -> list[dict]:
    """Delta-fetches only new bars past the cached watermark, merges, trims."""
    bar_ms = TF_BAR_MS.get(interval, 900_000)
    key = f"{symbol}:{interval}"
    cached = cache.get(key, {"candles": []})
    existing = cached.get("candles", [])
    end_ms = reference_ms
    if existing:
        start_ms = existing[-1]["t"] - 3 * bar_ms  # small overlap re-fetch for revision safety
    else:
        start_ms = end_ms - n * bar_ms
    payload = {"type": "candleSnapshot",
               "req": {"coin": symbol, "interval": interval, "startTime": max(0, start_ms), "endTime": end_ms}}
    raw = hl_post(payload)
    fresh = _normalize_candles(raw) if isinstance(raw, list) else []
    merged = {c["t"]: c for c in existing}
    for c in fresh:
        merged[c["t"]] = c
    all_sorted = sorted(merged.values(), key=lambda x: x["t"])
    trimmed = all_sorted[-(n + 5):]
    cache[key] = {"candles": trimmed}
    closed = filter_closed(trimmed, interval, reference_ms)
    return closed[-n:]


def fetch_symbol_bundle(symbol: str, reference_ms: int, cache: dict) -> Optional[dict]:
    bundle = {}
    for tf in TF_STACK:
        candles = fetch_candles(symbol, tf, CANDLES_PER_TF, reference_ms, cache)
        if len(candles) < 60:
            log.info("Insufficient %s candles for %s (%d bars) -- skipping symbol", tf, symbol, len(candles))
            return None
        bundle[tf] = {"candles": candles, "ind": compute_indicators(candles)}
    return bundle


# ============================================================================
# SECTION 4: STRUCTURE & SMART MONEY PRIMITIVES (shared across engines)
# ============================================================================

@dataclass
class Swing:
    idx: int
    price: float
    kind: str  # "high" | "low"


def find_swings(candles: list[dict], lr: int = SWING_LEFT_RIGHT) -> list[Swing]:
    out = []
    for i in range(lr, len(candles) - lr):
        window = candles[i - lr:i + lr + 1]
        h, l = candles[i]["h"], candles[i]["l"]
        if h == max(c["h"] for c in window):
            out.append(Swing(i, h, "high"))
        if l == min(c["l"] for c in window):
            out.append(Swing(i, l, "low"))
    return out


@dataclass
class StructureState:
    bias: str  # "bull" | "bear" | "neutral"
    last_bos_idx: int
    last_choch_idx: int
    last_swing_high: float
    last_swing_low: float


def analyze_structure(candles: list[dict], swings: list[Swing]) -> StructureState:
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    if len(highs) < 2 or len(lows) < 2:
        last_h = highs[-1].price if highs else candles[-1]["h"]
        last_l = lows[-1].price if lows else candles[-1]["l"]
        return StructureState("neutral", -1, -1, last_h, last_l)

    bias, bos_idx, choch_idx = "neutral", -1, -1
    hh_hl = highs[-1].price > highs[-2].price and lows[-1].price > lows[-2].price
    ll_lh = lows[-1].price < lows[-2].price and highs[-1].price < highs[-2].price
    if hh_hl:
        bias = "bull"
    elif ll_lh:
        bias = "bear"

    # BOS: close beyond the most recent opposite swing extreme in trend direction.
    for i in range(len(candles) - 1, max(0, len(candles) - 40), -1):
        c = candles[i]["c"]
        if bias == "bull" and c > highs[-1].price:
            bos_idx = i
            break
        if bias == "bear" and c < lows[-1].price:
            bos_idx = i
            break
    # CHoCH: close beyond the extreme that would flip bias.
    for i in range(len(candles) - 1, max(0, len(candles) - 40), -1):
        c = candles[i]["c"]
        if bias == "bull" and c < lows[-1].price:
            choch_idx = i
            break
        if bias == "bear" and c > highs[-1].price:
            choch_idx = i
            break

    return StructureState(bias, bos_idx, choch_idx, highs[-1].price, lows[-1].price)


@dataclass
class Zone:
    kind: str  # "bull_ob" | "bear_ob" | "bull_fvg" | "bear_fvg" | "bull_breaker" | "bear_breaker"
    top: float
    bottom: float
    idx: int
    mitigated: bool = False

    @property
    def mid(self) -> float:
        return (self.top + self.bottom) / 2


def find_order_blocks(candles: list[dict], atr_vals: list[float], lookback: int = 80) -> list[Zone]:
    out = []
    start = max(1, len(candles) - lookback)
    for i in range(start, len(candles) - 1):
        body = abs(candles[i]["c"] - candles[i]["o"])
        if body < 0.6 * max(atr_vals[i], 1e-9):
            continue
        # Bullish OB: last down-close candle before a strong up impulse that
        # breaks the prior swing high within the next 3 bars.
        if candles[i]["c"] < candles[i]["o"]:
            for j in range(i + 1, min(i + 4, len(candles))):
                if candles[j]["c"] > candles[i]["h"] and (candles[j]["c"] - candles[j]["o"]) > 0.8 * max(atr_vals[j], 1e-9):
                    out.append(Zone("bull_ob", candles[i]["h"], candles[i]["l"], i))
                    break
        if candles[i]["c"] > candles[i]["o"]:
            for j in range(i + 1, min(i + 4, len(candles))):
                if candles[j]["c"] < candles[i]["l"] and (candles[j]["o"] - candles[j]["c"]) > 0.8 * max(atr_vals[j], 1e-9):
                    out.append(Zone("bear_ob", candles[i]["h"], candles[i]["l"], i))
                    break
    return out


def find_fvgs(candles: list[dict], lookback: int = 80) -> list[Zone]:
    out = []
    start = max(2, len(candles) - lookback)
    for i in range(start, len(candles)):
        a, c = candles[i - 2], candles[i]
        if c["l"] > a["h"]:
            out.append(Zone("bull_fvg", c["l"], a["h"], i))
        if c["h"] < a["l"]:
            out.append(Zone("bear_fvg", a["l"], c["h"], i))
    return out


def mark_mitigation_and_breakers(zones: list[Zone], candles: list[dict]) -> list[Zone]:
    """Marks zones touched by later price, and reclassifies mitigated order
    blocks whose zone price is later closed through as breaker blocks."""
    out = []
    for z in zones:
        mitigated = False
        breaker = False
        for c in candles[z.idx + 1:]:
            if c["l"] <= z.top and c["h"] >= z.bottom:
                mitigated = True
            if mitigated and z.kind == "bull_ob" and c["c"] < z.bottom:
                breaker = True
            if mitigated and z.kind == "bear_ob" and c["c"] > z.top:
                breaker = True
        if breaker:
            new_kind = "bear_breaker" if z.kind == "bull_ob" else "bull_breaker"
            out.append(Zone(new_kind, z.top, z.bottom, z.idx, mitigated=True))
        else:
            out.append(Zone(z.kind, z.top, z.bottom, z.idx, mitigated=mitigated))
    return out


def cluster_levels(levels: list[float], tol_pct: float = LIQUIDITY_EQ_TOL_PCT) -> list[tuple[float, int]]:
    if not levels:
        return []
    levels = sorted(levels)
    clusters, cur = [], [levels[0]]
    for p in levels[1:]:
        if abs(p - cur[-1]) / max(cur[-1], 1e-9) <= tol_pct:
            cur.append(p)
        else:
            clusters.append(cur)
            cur = [p]
    clusters.append(cur)
    return [(sum(c) / len(c), len(c)) for c in clusters]


def build_liquidity_pools(swings: list[Swing]) -> dict:
    highs = cluster_levels([s.price for s in swings if s.kind == "high"])
    lows = cluster_levels([s.price for s in swings if s.kind == "low"])
    return {
        "buy_side": sorted([h for h in highs if h[1] >= 2], key=lambda x: -x[1]),
        "sell_side": sorted([l for l in lows if l[1] >= 2], key=lambda x: -x[1]),
    }


def detect_sweep(candles: list[dict], pools: dict, direction: str, lookback: int = 10) -> Optional[dict]:
    """A sweep = a wick pierces a liquidity pool level, but the candle closes
    back on the other side of it within the lookback window."""
    recent = candles[-lookback:]
    targets = pools["sell_side"] if direction == "long" else pools["buy_side"]
    for level, count in targets:
        for c in recent:
            if direction == "long" and c["l"] < level and c["c"] > level:
                return {"level": level, "strength": count}
            if direction == "short" and c["h"] > level and c["c"] < level:
                return {"level": level, "strength": count}
    return None


def premium_discount_zone(candles: list[dict], lookback: int = 50) -> dict:
    window = candles[-lookback:]
    hi, lo = max(c["h"] for c in window), min(c["l"] for c in window)
    mid = (hi + lo) / 2
    return {"high": hi, "low": lo, "mid": mid}


def zone_price_in_range(price: float, pd_zone: dict) -> str:
    if price >= pd_zone["mid"]:
        return "premium"
    return "discount"


# ============================================================================
# SECTION 5: MARKET REGIME DETECTION
# ============================================================================

@dataclass
class Regime:
    trend: str        # "trending" | "ranging" | "consolidation"
    direction: str     # "bull" | "bear" | "neutral"
    volatility: str    # "high" | "normal" | "low" | "expansion"
    label: str


def classify_regime(mtf_ind: dict, htf_structure: StructureState) -> Regime:
    adx = mtf_ind["adx"][-1]
    bbw = mtf_ind["bb_width"][-20:]
    bbw_now = mtf_ind["bb_width"][-1]
    bbw_pctile = percentile_rank(bbw, bbw_now)

    if bbw_pctile <= 20:
        vol = "low"
    elif bbw_pctile >= 90:
        vol = "expansion"
    elif bbw_pctile >= 70:
        vol = "high"
    else:
        vol = "normal"

    if adx >= 25:
        trend = "trending"
    elif adx <= 15:
        trend = "ranging"
    else:
        trend = "consolidation"

    direction = {"bull": "bull", "bear": "bear"}.get(htf_structure.bias, "neutral")
    label = f"{trend}_{direction}_{vol}"
    return Regime(trend, direction, vol, label)


# ============================================================================
# SECTION 6: SIGNAL CONTRACT (common output of every specialized engine)
# ============================================================================

@dataclass
class Signal:
    engine: str
    symbol: str
    timeframe: str
    direction: str  # "long" | "short"
    entry: float
    sl: float
    tp1: float
    tp2: float
    confidence: float          # 0-100, engine's own local confidence
    expected_rr: float
    confluences: list[str] = field(default_factory=list)
    regime_fit: list[str] = field(default_factory=list)
    generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def rr1(self) -> float:
        risk = abs(self.entry - self.sl)
        return safe_div(abs(self.tp1 - self.entry), risk)

    def rr2(self) -> float:
        risk = abs(self.entry - self.sl)
        return safe_div(abs(self.tp2 - self.entry), risk)


# ============================================================================
# SECTION 7: RISK MANAGEMENT (structure-based, candle-verified)
# ============================================================================

def build_risk_plan(direction: str, entry: float, invalidation: float, atr_val: float,
                     ltf_candles: list[dict], liquidity: dict,
                     rr_target: float = 2.0) -> Optional[tuple[float, float, float]]:
    """SL sits beyond structural invalidation plus an ATR buffer so a normal
    wick sweep of the level doesn't stop us out; validated only against
    candle highs/lows from the actual data, never live/mid price. TP is
    clipped to the nearest real opposing liquidity pool when one exists
    inside the projected RR path, else a plain R-multiple."""
    buf = 0.25 * atr_val
    recent_extreme_low = min(c["l"] for c in ltf_candles[-30:])
    recent_extreme_high = max(c["h"] for c in ltf_candles[-30:])

    if direction == "long":
        sl = min(invalidation - buf, recent_extreme_low - buf * 0.25)
        if sl >= entry:
            return None
        risk = entry - sl
        tp2 = entry + risk * rr_target
        tp1 = entry + risk * (rr_target / 2)
        for level, _cnt in liquidity.get("buy_side", []):
            if entry < level < tp2:
                tp2 = min(tp2, level * 0.999)
        tp1 = min(tp1, tp2 - risk * 0.1) if tp2 > entry else tp1
    else:
        sl = max(invalidation + buf, recent_extreme_high + buf * 0.25)
        if sl <= entry:
            return None
        risk = sl - entry
        tp2 = entry - risk * rr_target
        tp1 = entry - risk * (rr_target / 2)
        for level, _cnt in liquidity.get("sell_side", []):
            if tp2 < level < entry:
                tp2 = max(tp2, level * 1.001)
        tp1 = max(tp1, tp2 + risk * 0.1) if tp2 < entry else tp1

    if risk <= 0:
        return None
    return round(sl, 6), round(tp1, 6), round(tp2, 6)


def validate_against_candles(direction: str, entry: float, sl: float, ltf_candles: list[dict]) -> bool:
    """Confirms SL/TP logic is grounded in actual traded highs/lows, not an
    arbitrary or unreachable level -- guards against zones far outside any
    recently observed price action."""
    lo = min(c["l"] for c in ltf_candles[-100:])
    hi = max(c["h"] for c in ltf_candles[-100:])
    span = hi - lo
    if span <= 0:
        return False
    if direction == "long":
        return lo - span * 0.5 <= sl < entry
    return entry < sl <= hi + span * 0.5


# ============================================================================
# SECTION 8: SPECIALIZED ENGINES
# ============================================================================
# Each function receives the multi-timeframe `bundle` for one symbol plus
# shared structure/regime context, and returns zero or more Signal objects.
# Engineering decision: engines share the structure/zone/liquidity primitives
# above (computed once per symbol in the orchestrator) rather than
# recomputing them, per the API/CPU-efficiency requirements in the spec.

def _base_confluences(regime: Regime, mtf_bias: str) -> list[str]:
    c = [f"regime:{regime.label}"]
    if mtf_bias != "neutral":
        c.append(f"mtf_bias:{mtf_bias}")
    return c


def eng_smc(ctx: dict) -> list[Signal]:
    """Order Block + BOS retest with liquidity sweep confirmation -- the
    flagship composite Smart Money Concept pathway."""
    out = []
    ltf, structure, zones, liq, regime = ctx["ltf"], ctx["htf_structure"], ctx["ltf_zones"], ctx["liq"], ctx["regime"]
    candles, ind = ltf["candles"], ltf["ind"]
    price = candles[-1]["c"]
    atr_val = ind["atr"][-1]
    if structure.bias not in ("bull", "bear"):
        return out
    direction = "long" if structure.bias == "bull" else "short"
    want_kind = "bull_ob" if direction == "long" else "bear_ob"
    candidates = [z for z in zones if z.kind == want_kind and z.mitigated is False]
    for z in sorted(candidates, key=lambda z: -z.idx)[:3]:
        if not (z.bottom <= price <= z.top or abs(price - z.mid) / max(price, 1e-9) < 0.01):
            continue
        sweep = detect_sweep(candles, liq, direction)
        plan = build_risk_plan(direction, price, z.bottom if direction == "long" else z.top,
                                atr_val, candles, liq, rr_target=2.5)
        if not plan:
            continue
        sl, tp1, tp2 = plan
        if not validate_against_candles(direction, price, sl, candles):
            continue
        confl = _base_confluences(regime, structure.bias) + ["order_block_retest"]
        if sweep:
            confl.append("liquidity_sweep_confirmation")
        conf = 55 + (10 if sweep else 0) + (10 if regime.trend == "trending" else 0)
        sig = Signal("smc", ctx["symbol"], TF_LTF, direction, price, sl, tp1, tp2,
                      min(conf, 92), 0.0, confl, [regime.label])
        sig.expected_rr = sig.rr2()
        out.append(sig)
        break
    return out


def eng_trend_continuation(ctx: dict) -> list[Signal]:
    ltf, regime = ctx["ltf"], ctx["regime"]
    candles, ind = ltf["candles"], ltf["ind"]
    price = candles[-1]["c"]
    if regime.trend != "trending" or regime.direction == "neutral":
        return []
    direction = "long" if regime.direction == "bull" else "short"
    ema20, ema50 = ind["ema20"][-1], ind["ema50"][-1]
    near_ema20 = abs(price - ema20) / max(price, 1e-9) < 0.006
    aligned = (ema20 > ema50) if direction == "long" else (ema20 < ema50)
    if not (near_ema20 and aligned):
        return []
    atr_val = ind["atr"][-1]
    invalidation = ema50 if direction == "long" else ema50
    plan = build_risk_plan(direction, price, invalidation, atr_val, candles, ctx["liq"], rr_target=2.0)
    if not plan:
        return []
    sl, tp1, tp2 = plan
    if not validate_against_candles(direction, price, sl, candles):
        return []
    sig = Signal("trend_continuation", ctx["symbol"], TF_LTF, direction, price, sl, tp1, tp2,
                 62 + (8 if ind["adx"][-1] > 30 else 0), 0.0,
                 _base_confluences(regime, ctx["htf_structure"].bias) + ["ema20_pullback", "ema_stack_aligned"],
                 [regime.label])
    sig.expected_rr = sig.rr2()
    return [sig]


def eng_breakout(ctx: dict) -> list[Signal]:
    ltf, regime = ctx["ltf"], ctx["regime"]
    candles, ind = ltf["candles"], ltf["ind"]
    window = candles[-24:-1]
    hi, lo = max(c["h"] for c in window), min(c["l"] for c in window)
    price = candles[-1]["c"]
    vol_avg = ind["vol_sma20"][-2]
    vol_now = candles[-1]["v"]
    if regime.trend == "ranging" and vol_now > 1.5 * max(vol_avg, 1e-9):
        if price > hi:
            direction = "long"
        elif price < lo:
            direction = "short"
        else:
            return []
    else:
        return []
    atr_val = ind["atr"][-1]
    invalidation = lo if direction == "long" else hi
    plan = build_risk_plan(direction, price, invalidation, atr_val, candles, ctx["liq"], rr_target=2.2)
    if not plan:
        return []
    sl, tp1, tp2 = plan
    if not validate_against_candles(direction, price, sl, candles):
        return []
    sig = Signal("breakout", ctx["symbol"], TF_LTF, direction, price, sl, tp1, tp2, 58, 0.0,
                 _base_confluences(regime, ctx["htf_structure"].bias) + ["range_breakout", "volume_expansion"],
                 [regime.label])
    sig.expected_rr = sig.rr2()
    return [sig]


def eng_pullback(ctx: dict) -> list[Signal]:
    ltf, regime = ctx["ltf"], ctx["regime"]
    candles, ind = ltf["candles"], ltf["ind"]
    price = candles[-1]["c"]
    if ctx["htf_structure"].bias not in ("bull", "bear"):
        return []
    direction = "long" if ctx["htf_structure"].bias == "bull" else "short"
    fib_hi, fib_lo = ctx["pd_zone"]["high"], ctx["pd_zone"]["low"]
    rng = fib_hi - fib_lo
    if rng <= 0:
        return []
    retr = (fib_hi - price) / rng if direction == "long" else (price - fib_lo) / rng
    if not (0.38 <= retr <= 0.66):
        return []
    atr_val = ind["atr"][-1]
    invalidation = fib_lo if direction == "long" else fib_hi
    plan = build_risk_plan(direction, price, invalidation, atr_val, candles, ctx["liq"], rr_target=2.0)
    if not plan:
        return []
    sl, tp1, tp2 = plan
    if not validate_against_candles(direction, price, sl, candles):
        return []
    sig = Signal("pullback", ctx["symbol"], TF_LTF, direction, price, sl, tp1, tp2, 56, 0.0,
                 _base_confluences(regime, ctx["htf_structure"].bias) + ["fib_retracement_zone"], [regime.label])
    sig.expected_rr = sig.rr2()
    return [sig]


def eng_liquidity_sweep(ctx: dict) -> list[Signal]:
    ltf, regime = ctx["ltf"], ctx["regime"]
    candles, ind = ltf["candles"], ltf["ind"]
    price = candles[-1]["c"]
    out = []
    for direction in ("long", "short"):
        sweep = detect_sweep(candles, ctx["liq"], direction, lookback=6)
        if not sweep:
            continue
        atr_val = ind["atr"][-1]
        invalidation = sweep["level"] - atr_val * 0.15 if direction == "long" else sweep["level"] + atr_val * 0.15
        plan = build_risk_plan(direction, price, invalidation, atr_val, candles, ctx["liq"], rr_target=2.3)
        if not plan:
            continue
        sl, tp1, tp2 = plan
        if not validate_against_candles(direction, price, sl, candles):
            continue
        conf = 60 + min(sweep["strength"] * 5, 15)
        sig = Signal("liquidity_sweep", ctx["symbol"], TF_LTF, direction, price, sl, tp1, tp2,
                     min(conf, 90), 0.0,
                     _base_confluences(regime, ctx["htf_structure"].bias) + ["liquidity_pool_sweep"], [regime.label])
        sig.expected_rr = sig.rr2()
        out.append(sig)
    return out


def eng_order_block(ctx: dict) -> list[Signal]:
    """Standalone fresh, unmitigated Order Block retest -- does not require a
    prior BOS the way the composite SMC engine does, so it can fire earlier
    in a developing move."""
    ltf, regime = ctx["ltf"], ctx["regime"]
    candles, ind = ltf["candles"], ltf["ind"]
    price = candles[-1]["c"]
    out = []
    for z in ctx["ltf_zones"]:
        if z.mitigated or z.kind not in ("bull_ob", "bear_ob"):
            continue
        direction = "long" if z.kind == "bull_ob" else "short"
        if not (z.bottom <= price <= z.top):
            continue
        atr_val = ind["atr"][-1]
        invalidation = z.bottom if direction == "long" else z.top
        plan = build_risk_plan(direction, price, invalidation, atr_val, candles, ctx["liq"], rr_target=2.0)
        if not plan:
            continue
        sl, tp1, tp2 = plan
        if not validate_against_candles(direction, price, sl, candles):
            continue
        sig = Signal("order_block", ctx["symbol"], TF_LTF, direction, price, sl, tp1, tp2, 52, 0.0,
                     _base_confluences(regime, ctx["htf_structure"].bias) + ["fresh_order_block"], [regime.label])
        sig.expected_rr = sig.rr2()
        out.append(sig)
        break
    return out


def eng_breaker_block(ctx: dict) -> list[Signal]:
    ltf, regime = ctx["ltf"], ctx["regime"]
    candles, ind = ltf["candles"], ltf["ind"]
    price = candles[-1]["c"]
    out = []
    for z in ctx["ltf_zones"]:
        if z.kind not in ("bull_breaker", "bear_breaker"):
            continue
        direction = "long" if z.kind == "bull_breaker" else "short"
        if not (z.bottom <= price <= z.top):
            continue
        atr_val = ind["atr"][-1]
        invalidation = z.bottom if direction == "long" else z.top
        plan = build_risk_plan(direction, price, invalidation, atr_val, candles, ctx["liq"], rr_target=2.2)
        if not plan:
            continue
        sl, tp1, tp2 = plan
        if not validate_against_candles(direction, price, sl, candles):
            continue
        sig = Signal("breaker_block", ctx["symbol"], TF_LTF, direction, price, sl, tp1, tp2, 57, 0.0,
                     _base_confluences(regime, ctx["htf_structure"].bias) + ["breaker_block_retest"], [regime.label])
        sig.expected_rr = sig.rr2()
        out.append(sig)
        break
    return out


def eng_fair_value_gap(ctx: dict) -> list[Signal]:
    ltf, regime = ctx["ltf"], ctx["regime"]
    candles, ind = ltf["candles"], ltf["ind"]
    price = candles[-1]["c"]
    out = []
    for z in ctx["ltf_zones"]:
        if z.kind not in ("bull_fvg", "bear_fvg") or z.mitigated:
            continue
        direction = "long" if z.kind == "bull_fvg" else "short"
        if not (z.bottom <= price <= z.top):
            continue
        atr_val = ind["atr"][-1]
        invalidation = z.bottom if direction == "long" else z.top
        plan = build_risk_plan(direction, price, invalidation, atr_val, candles, ctx["liq"], rr_target=1.8)
        if not plan:
            continue
        sl, tp1, tp2 = plan
        if not validate_against_candles(direction, price, sl, candles):
            continue
        sig = Signal("fair_value_gap", ctx["symbol"], TF_LTF, direction, price, sl, tp1, tp2, 50, 0.0,
                     _base_confluences(regime, ctx["htf_structure"].bias) + ["fvg_fill_entry"], [regime.label])
        sig.expected_rr = sig.rr2()
        out.append(sig)
        break
    return out


def eng_momentum(ctx: dict) -> list[Signal]:
    ltf, regime = ctx["ltf"], ctx["regime"]
    candles, ind = ltf["candles"], ltf["ind"]
    price = candles[-1]["c"]
    r = ind["rsi"]
    roc = safe_div(price - candles[-6]["c"], candles[-6]["c"])
    direction = None
    if r[-1] > 58 and r[-1] > r[-3] and roc > 0.01:
        direction = "long"
    elif r[-1] < 42 and r[-1] < r[-3] and roc < -0.01:
        direction = "short"
    if not direction:
        return []
    atr_val = ind["atr"][-1]
    invalidation = price - 1.5 * atr_val if direction == "long" else price + 1.5 * atr_val
    plan = build_risk_plan(direction, price, invalidation, atr_val, candles, ctx["liq"], rr_target=1.8)
    if not plan:
        return []
    sl, tp1, tp2 = plan
    if not validate_against_candles(direction, price, sl, candles):
        return []
    sig = Signal("momentum", ctx["symbol"], TF_LTF, direction, price, sl, tp1, tp2, 54, 0.0,
                 _base_confluences(regime, ctx["htf_structure"].bias) + ["rsi_momentum", "rate_of_change"], [regime.label])
    sig.expected_rr = sig.rr2()
    return [sig]


def eng_reversal(ctx: dict) -> list[Signal]:
    ltf, regime = ctx["ltf"], ctx["regime"]
    candles, ind = ltf["candles"], ltf["ind"]
    price = candles[-1]["c"]
    r = ind["rsi"]
    closes = ind["closes"]
    n = len(closes)
    if n < 20:
        return []
    lo_idx = n - 1 - closes[-20:][::-1].index(min(closes[-20:]))
    hi_idx = n - 1 - closes[-20:][::-1].index(max(closes[-20:]))
    direction = None
    if closes[-1] <= min(closes[-20:]) * 1.002 and r[-1] > r[lo_idx] and r[-1] < 40:
        direction = "long"
    elif closes[-1] >= max(closes[-20:]) * 0.998 and r[-1] < r[hi_idx] and r[-1] > 60:
        direction = "short"
    if not direction:
        return []
    atr_val = ind["atr"][-1]
    extreme = min(c["l"] for c in candles[-20:]) if direction == "long" else max(c["h"] for c in candles[-20:])
    plan = build_risk_plan(direction, price, extreme, atr_val, candles, ctx["liq"], rr_target=2.4)
    if not plan:
        return []
    sl, tp1, tp2 = plan
    if not validate_against_candles(direction, price, sl, candles):
        return []
    sig = Signal("reversal", ctx["symbol"], TF_LTF, direction, price, sl, tp1, tp2, 53, 0.0,
                 _base_confluences(regime, ctx["htf_structure"].bias) + ["rsi_divergence"], [regime.label])
    sig.expected_rr = sig.rr2()
    return [sig]


def eng_mean_reversion(ctx: dict) -> list[Signal]:
    ltf, regime = ctx["ltf"], ctx["regime"]
    candles, ind = ltf["candles"], ltf["ind"]
    price = candles[-1]["c"]
    if regime.trend != "ranging":
        return []
    closes = ind["closes"]
    mid = sma(closes, BB_LEN)[-1]
    sd = stdev(closes, BB_LEN)[-1]
    upper, lower = mid + BB_MULT * sd, mid - BB_MULT * sd
    direction = None
    if price <= lower:
        direction = "long"
    elif price >= upper:
        direction = "short"
    if not direction:
        return []
    atr_val = ind["atr"][-1]
    invalidation = lower - atr_val if direction == "long" else upper + atr_val
    plan = build_risk_plan(direction, price, invalidation, atr_val, candles, ctx["liq"], rr_target=1.6)
    if not plan:
        return []
    sl, tp1, tp2 = plan
    if not validate_against_candles(direction, price, sl, candles):
        return []
    sig = Signal("mean_reversion", ctx["symbol"], TF_LTF, direction, price, sl, tp1, tp2, 51, 0.0,
                 _base_confluences(regime, ctx["htf_structure"].bias) + ["bollinger_band_extreme"], [regime.label])
    sig.expected_rr = sig.rr2()
    return [sig]


def eng_range_trading(ctx: dict) -> list[Signal]:
    ltf, regime = ctx["ltf"], ctx["regime"]
    candles, ind = ltf["candles"], ltf["ind"]
    price = candles[-1]["c"]
    if regime.trend != "ranging":
        return []
    window = candles[-40:]
    hi, lo = max(c["h"] for c in window), min(c["l"] for c in window)
    rng = hi - lo
    if rng <= 0:
        return []
    direction = None
    if price <= lo + 0.12 * rng:
        direction = "long"
    elif price >= hi - 0.12 * rng:
        direction = "short"
    if not direction:
        return []
    atr_val = ind["atr"][-1]
    invalidation = lo - atr_val * 0.3 if direction == "long" else hi + atr_val * 0.3
    plan = build_risk_plan(direction, price, invalidation, atr_val, candles, ctx["liq"], rr_target=1.7)
    if not plan:
        return []
    sl, tp1, tp2 = plan
    if not validate_against_candles(direction, price, sl, candles):
        return []
    sig = Signal("range_trading", ctx["symbol"], TF_LTF, direction, price, sl, tp1, tp2, 50, 0.0,
                 _base_confluences(regime, ctx["htf_structure"].bias) + ["range_boundary_bounce"], [regime.label])
    sig.expected_rr = sig.rr2()
    return [sig]


def eng_volatility_expansion(ctx: dict) -> list[Signal]:
    ltf, regime = ctx["ltf"], ctx["regime"]
    candles, ind = ltf["candles"], ltf["ind"]
    price = candles[-1]["c"]
    if regime.volatility != "expansion":
        return []
    bbw = ind["bb_width"]
    was_squeeze = percentile_rank(bbw[-40:-5], min(bbw[-20:-5]) if len(bbw) > 25 else bbw[-1]) <= 20
    if not was_squeeze:
        return []
    direction = "long" if price > ind["ema20"][-1] else "short"
    atr_val = ind["atr"][-1]
    invalidation = price - 1.8 * atr_val if direction == "long" else price + 1.8 * atr_val
    plan = build_risk_plan(direction, price, invalidation, atr_val, candles, ctx["liq"], rr_target=2.1)
    if not plan:
        return []
    sl, tp1, tp2 = plan
    if not validate_against_candles(direction, price, sl, candles):
        return []
    sig = Signal("volatility_expansion", ctx["symbol"], TF_LTF, direction, price, sl, tp1, tp2, 55, 0.0,
                 _base_confluences(regime, ctx["htf_structure"].bias) + ["volatility_squeeze_release"], [regime.label])
    sig.expected_rr = sig.rr2()
    return [sig]


def eng_vwap_reversion(ctx: dict) -> list[Signal]:
    """Added during gap analysis: neither reference used session VWAP as an
    independent entry engine (only as a scoring confluence); it is a
    well-established institutional mean-reversion reference level."""
    ltf, regime = ctx["ltf"], ctx["regime"]
    candles, ind = ltf["candles"], ltf["ind"]
    price = candles[-1]["c"]
    vwap = ind["vwap"][-1]
    atr_val = ind["atr"][-1]
    dist = (price - vwap) / max(atr_val, 1e-9)
    direction = None
    if dist <= -1.8 and regime.trend != "trending":
        direction = "long"
    elif dist >= 1.8 and regime.trend != "trending":
        direction = "short"
    if not direction:
        return []
    invalidation = price - 1.2 * atr_val if direction == "long" else price + 1.2 * atr_val
    plan = build_risk_plan(direction, price, invalidation, atr_val, candles, ctx["liq"], rr_target=1.5)
    if not plan:
        return []
    sl, tp1, tp2 = plan
    if not validate_against_candles(direction, price, sl, candles):
        return []
    sig = Signal("vwap_reversion", ctx["symbol"], TF_LTF, direction, price, sl, tp1, tp2, 48, 0.0,
                 _base_confluences(regime, ctx["htf_structure"].bias) + ["vwap_extension"], [regime.label])
    sig.expected_rr = sig.rr2()
    return [sig]


ENGINE_FUNCS = {
    "smc": eng_smc, "trend_continuation": eng_trend_continuation, "breakout": eng_breakout,
    "pullback": eng_pullback, "liquidity_sweep": eng_liquidity_sweep, "order_block": eng_order_block,
    "breaker_block": eng_breaker_block, "fair_value_gap": eng_fair_value_gap, "momentum": eng_momentum,
    "reversal": eng_reversal, "mean_reversion": eng_mean_reversion, "range_trading": eng_range_trading,
    "volatility_expansion": eng_volatility_expansion, "vwap_reversion": eng_vwap_reversion,
}


# ============================================================================
# SECTION 9: DECISION ENGINE (adaptive ranking, scoring, de-duplication)
# ============================================================================

def engine_weight(state: dict, engine: str) -> float:
    """Adaptive per-engine weight derived from realized win rate, shrunk
    toward a neutral prior until enough samples exist (Bayesian-style
    shrinkage prevents overfitting to a handful of early trades)."""
    stats = state.get("engine_stats", {}).get(engine, {})
    wins, losses = stats.get("wins", 0), stats.get("losses", 0)
    n = wins + losses
    prior_wr, prior_n = 0.5, 8
    wr = (wins + prior_wr * prior_n) / (n + prior_n)
    return 0.6 + 0.8 * wr  # bounded roughly [0.6, 1.4]


def compute_ev(win_rate: float, rr: float) -> float:
    return win_rate * rr - (1 - win_rate)


def score_signal(sig: Signal, state: dict, mtf_alignment: float) -> float:
    stats = state.get("engine_stats", {}).get(sig.engine, {})
    wins, losses = stats.get("wins", 0), stats.get("losses", 0)
    n = wins + losses
    prior_wr, prior_n = 0.5, 8
    est_wr = (wins + prior_wr * prior_n) / (n + prior_n)
    ev = compute_ev(est_wr, sig.expected_rr)
    w = engine_weight(state, sig.engine)
    confluence_bonus = min(len(sig.confluences) * 3, 15)
    score = (sig.confidence * 0.4 + ev * 20 * 0.25 + sig.expected_rr * 5 * 0.15
             + mtf_alignment * 20 * 0.15 + confluence_bonus * 0.05) * w
    return score


def mtf_alignment_score(bundle: dict, structure_by_tf: dict, direction: str) -> float:
    agree = 0
    total = 0
    for tf in (TF_MTF, TF_HTF, TF_D1):
        st = structure_by_tf.get(tf)
        if not st or st.bias == "neutral":
            continue
        total += 1
        want = "bull" if direction == "long" else "bear"
        if st.bias == want:
            agree += 1
    return safe_div(agree, total, default=0.5)


def correlation_clusters(returns_by_symbol: dict[str, list[float]]) -> list[set]:
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

    for i in range(len(symbols)):
        for j in range(i + 1, len(symbols)):
            a, b = symbols[i], symbols[j]
            if pearson(returns_by_symbol[a], returns_by_symbol[b]) >= CORRELATION_DEDUP_THRESHOLD:
                union(a, b)

    groups: dict[str, set] = {}
    for s in symbols:
        groups.setdefault(find(s), set()).add(s)
    return list(groups.values())


def rank_and_select(candidates: list[tuple[Signal, float]], clusters: list[set],
                     max_signals: int, threshold: float) -> list[Signal]:
    ranked = sorted(candidates, key=lambda t: -t[1])
    chosen: list[Signal] = []
    used_symbols: set[str] = set()
    used_clusters: set[frozenset] = set()

    def cluster_of(symbol: str) -> frozenset:
        for c in clusters:
            if symbol in c:
                return frozenset(c)
        return frozenset({symbol})

    for sig, score in ranked:
        if score < threshold:
            continue
        if sig.symbol in used_symbols:
            continue
        cl = cluster_of(sig.symbol)
        if cl in used_clusters:
            continue
        chosen.append(sig)
        used_symbols.add(sig.symbol)
        used_clusters.add(cl)
        if len(chosen) >= max_signals:
            break
    return chosen


def governor_threshold(state: dict, base_threshold: float = 45.0) -> float:
    """Adaptive frequency governor: nudges the acceptance threshold toward a
    5-10 signal/day band using a slow EMA of daily signal counts. Bounded to
    prevent runaway drift; never forces trades to hit the quota."""
    hist = state.get("daily_signal_counts", [])
    if not hist:
        return base_threshold
    avg = sum(hist[-7:]) / len(hist[-7:])
    if avg < 5:
        adj = -6.0
    elif avg > 10:
        adj = 6.0
    else:
        adj = 0.0
    prev_adj = state.get("governor_adjustment", 0.0)
    smoothed = prev_adj * 0.7 + adj * 0.3
    state["governor_adjustment"] = smoothed
    return max(25.0, min(70.0, base_threshold + smoothed))


# ============================================================================
# SECTION 10: STATE PERSISTENCE (atomic, versioned)
# ============================================================================

def _default_state() -> dict:
    return {
        "engine_version": ENGINE_VERSION,
        "open_signals": [],
        "signal_history": [],
        "engine_stats": {e: {"wins": 0, "losses": 0, "rr_sum": 0.0, "hold_time_sum_s": 0.0,
                              "count": 0} for e in ENGINE_LIST},
        "regime_stats": {},
        "asset_stats": {},
        "timeframe_stats": {},
        "confidence_calibration": {},
        "daily_signal_counts": [],
        "governor_adjustment": 0.0,
        "last_daily_summary_date": None,
        "cooldowns": {},
        "learning_log": [],
    }


def load_state() -> dict:
    p = Path(STATE_PATH)
    if not p.exists():
        return _default_state()
    try:
        data = json.loads(p.read_text())
        merged = _default_state()
        merged.update(data)
        return merged
    except (json.JSONDecodeError, OSError) as exc:
        log.error("Failed to load state.json (%s); starting from default state", exc)
        return _default_state()


def save_state(state: dict):
    p = Path(STATE_PATH)
    tmp = p.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(state, indent=2, default=str))
        tmp.replace(p)  # atomic on POSIX
    except OSError as exc:
        log.error("Failed to persist state.json: %s", exc)


def load_candle_cache() -> dict:
    p = Path(CANDLE_CACHE_PATH)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def save_candle_cache(cache: dict):
    p = Path(CANDLE_CACHE_PATH)
    tmp = p.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(cache))
        tmp.replace(p)
    except OSError as exc:
        log.error("Failed to persist candle_cache.json: %s", exc)


def prune_state(state: dict, max_days: int = 30):
    cutoff = time.time() - max_days * 86400
    state["signal_history"] = [
        h for h in state["signal_history"]
        if h.get("closed_at_ts", time.time()) >= cutoff
    ][-2000:]
    state["daily_signal_counts"] = state["daily_signal_counts"][-30:]
    state["learning_log"] = state["learning_log"][-500:]


# ============================================================================
# SECTION 11: LEARNING SYSTEM
# ============================================================================

def resolve_open_signals(state: dict, live_prices: dict[str, float],
                          live_candles: dict[str, list[dict]]) -> list[dict]:
    """Checks each open signal's LTF candles since entry for SL/TP1/TP2
    touches (validated against candle highs/lows only) and resolves it,
    updating learning statistics. Returns resolution events for Telegram."""
    events = []
    still_open = []
    for os_ in state["open_signals"]:
        symbol = os_["symbol"]
        candles = live_candles.get(symbol)
        if not candles:
            still_open.append(os_)
            continue
        relevant = [c for c in candles if c["t"] > os_["opened_at_ms"]]
        direction = os_["direction"]
        status = os_.get("status", "activated")
        hit_tp1 = os_.get("hit_tp1", False)
        resolved = False
        for c in relevant:
            if direction == "long":
                if c["l"] <= os_["sl"]:
                    status, resolved = ("breakeven" if hit_tp1 else "stopped_out"), True
                    break
                if not hit_tp1 and c["h"] >= os_["tp1"]:
                    hit_tp1, status = True, "tp1_hit"
                    os_["sl"] = os_["entry"]  # move to break-even after TP1
                if c["h"] >= os_["tp2"]:
                    status, resolved = "tp2_hit", True
                    break
            else:
                if c["h"] >= os_["sl"]:
                    status, resolved = ("breakeven" if hit_tp1 else "stopped_out"), True
                    break
                if not hit_tp1 and c["l"] <= os_["tp1"]:
                    hit_tp1, status = True, "tp1_hit"
                    os_["sl"] = os_["entry"]
                if c["l"] <= os_["tp2"]:
                    status, resolved = "tp2_hit", True
                    break
        os_["hit_tp1"] = hit_tp1
        os_["status"] = status
        if resolved:
            win = status in ("tp1_hit", "tp2_hit", "breakeven") and status != "stopped_out"
            realized_rr = os_["expected_rr"] if status == "tp2_hit" else (
                0.0 if status == "breakeven" else (os_["expected_rr"] / 2 if status == "tp1_hit" else -1.0))
            hold_s = max(0, (relevant[-1]["t"] - os_["opened_at_ms"]) / 1000)
            update_learning(state, os_, win=(status != "stopped_out"), realized_rr=realized_rr, hold_s=hold_s)
            os_["closed_at_ts"] = time.time()
            state["signal_history"].append(os_)
            events.append({"signal": os_, "status": status})
        else:
            still_open.append(os_)
    state["open_signals"] = still_open
    return events


def update_learning(state: dict, sig: dict, win: bool, realized_rr: float, hold_s: float):
    es = state["engine_stats"].setdefault(sig["engine"], {"wins": 0, "losses": 0, "rr_sum": 0.0,
                                                            "hold_time_sum_s": 0.0, "count": 0})
    es["wins"] += 1 if win else 0
    es["losses"] += 0 if win else 1
    es["rr_sum"] += realized_rr
    es["hold_time_sum_s"] += hold_s
    es["count"] += 1

    for bucket_key, bucket_store in (
        (sig.get("regime", "unknown"), state["regime_stats"]),
        (sig["symbol"], state["asset_stats"]),
        (sig["timeframe"], state["timeframe_stats"]),
    ):
        b = bucket_store.setdefault(bucket_key, {"wins": 0, "losses": 0, "rr_sum": 0.0, "count": 0})
        b["wins"] += 1 if win else 0
        b["losses"] += 0 if win else 1
        b["rr_sum"] += realized_rr
        b["count"] += 1

    conf_bucket = str(int(sig["confidence"] // 10) * 10)
    cc = state["confidence_calibration"].setdefault(conf_bucket, {"predicted_n": 0, "wins": 0, "count": 0})
    cc["count"] += 1
    cc["wins"] += 1 if win else 0

    state["learning_log"].append({
        "ts": datetime.now(timezone.utc).isoformat(), "symbol": sig["symbol"], "engine": sig["engine"],
        "outcome": "win" if win else "loss", "realized_rr": round(realized_rr, 3),
        "regime": sig.get("regime", "unknown"), "hold_s": round(hold_s, 1),
    })


# ============================================================================
# SECTION 12: TELEGRAM INTEGRATION
# ============================================================================

TG_API = "https://api.telegram.org/bot{token}/{method}"
ALLOWED_REACTIONS = ["\U0001F44D", "\U0001F44E", "\u2705", "\u274C", "\U0001F525", "\U0001F440"]


def _tg_call(method: str, payload: dict) -> Optional[dict]:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.warning("Telegram credentials missing; skipping %s", method)
        return None
    url = TG_API.format(token=TG_BOT_TOKEN, method=method)
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        log.error("Telegram %s failed: %s", method, exc)
        return None


_CONFLUENCE_LABELS = {
    "order_block_retest": "Order block retest",
    "ema20_pullback": "EMA20 pullback",
    "ema_stack_aligned": "EMA stack aligned",
    "range_breakout": "Range breakout",
    "volume_expansion": "Volume expansion",
    "fib_retracement_zone": "Fib retracement zone",
    "liquidity_pool_sweep": "Liquidity sweep",
    "fresh_order_block": "Fresh order block",
    "breaker_block_retest": "Breaker block retest",
    "fvg_fill_entry": "FVG fill",
    "rsi_momentum": "RSI momentum",
    "rate_of_change": "Rate of change",
    "rsi_divergence": "RSI divergence",
    "bollinger_band_extreme": "Bollinger Band extreme",
    "range_boundary_bounce": "Range boundary bounce",
    "volatility_squeeze_release": "Volatility squeeze release",
    "vwap_extension": "VWAP extension",
}
_REGIME_TREND = {"trending": "Trending", "ranging": "Ranging", "consolidation": "Consolidating"}
_REGIME_DIR = {"bull": "bullish", "bear": "bearish", "neutral": "neutral"}
_REGIME_VOL = {"high": "high vol", "low": "low vol", "normal": "normal vol", "expansion": "vol expansion"}


def humanize_confluence(tag: str) -> str:
    if tag.startswith("regime:"):
        parts = tag.split(":", 1)[1].split("_")
        trend, direction, vol = (parts + ["", "", ""])[:3]
        return f"{_REGIME_TREND.get(trend, trend.title())} {_REGIME_DIR.get(direction, direction)} ({_REGIME_VOL.get(vol, vol)})"
    if tag.startswith("mtf_bias:"):
        return f"{tag.split(':', 1)[1].title()} MTF bias"
    return _CONFLUENCE_LABELS.get(tag, tag.replace("_", " ").capitalize())


def format_signal_message(sig: Signal, grade: str) -> str:
    arrow = "\U0001F7E2 LONG" if sig.direction == "long" else "\U0001F534 SHORT"
    confl_lines = "\n".join(f"\u2022 {humanize_confluence(c)}" for c in sig.confluences)
    return (
        f"<b>{ENGINE_NAME} v{ENGINE_VERSION}</b>\n"
        f"{arrow}  <b>{sig.symbol}</b>  ({sig.engine})\n"
        f"Grade: {grade}  |  Confidence: {sig.confidence:.0f}%  |  RR: {sig.expected_rr:.2f}\n"
        f"Entry : <code>{sig.entry}</code>\n"
        f"SL    : <code>{sig.sl}</code>\n"
        f"TP1   : <code>{sig.tp1}</code>\n"
        f"TP2   : <code>{sig.tp2}</code>\n"
        f"{confl_lines}\n"
        f"<i>Signal only -- not financial advice. Verify independently before acting.</i>"
    )


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


def send_signal_telegram(sig: Signal) -> Optional[int]:
    grade = grade_for_confidence(sig.confidence)
    text = format_signal_message(sig, grade)
    resp = _tg_call("sendMessage", {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"})
    if resp and resp.get("ok"):
        return resp["result"]["message_id"]
    return None


STATUS_EMOJI = {
    "activated": "\U0001F440", "tp1_hit": "\u2705", "tp2_hit": "\U0001F525",
    "stopped_out": "\u274C", "breakeven": "\U0001F44D", "closed": "\u2705", "cancelled": "\u274C",
}
STATUS_TEXT = {
    "activated": "Activated", "tp1_hit": "TP1 Hit", "tp2_hit": "TP2 Hit",
    "stopped_out": "Stop Loss Hit", "breakeven": "Closed at Break-even",
    "closed": "Closed", "cancelled": "Cancelled",
}


def send_status_update(sig_record: dict, status: str):
    msg_id = sig_record.get("tg_message_id")
    if not msg_id:
        return
    text = f"<b>{sig_record['symbol']}</b> update: {STATUS_TEXT.get(status, status)}"
    _tg_call("sendMessage", {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML",
                              "reply_to_message_id": msg_id})
    emoji = STATUS_EMOJI.get(status)
    if emoji:
        _tg_call("setMessageReaction", {"chat_id": TG_CHAT_ID, "message_id": msg_id,
                                         "reaction": [{"type": "emoji", "emoji": emoji}]})


def build_daily_summary(state: dict) -> str:
    today = datetime.now(timezone.utc).date().isoformat()
    todays = [h for h in state["signal_history"]
              if h.get("closed_at_ts") and datetime.fromtimestamp(h["closed_at_ts"], tz=timezone.utc).date().isoformat() == today]
    wins = sum(1 for h in todays if h["status"] != "stopped_out")
    losses = sum(1 for h in todays if h["status"] == "stopped_out")
    total = wins + losses
    win_rate = safe_div(wins, total, default=0.0) * 100
    rr_vals = [h.get("expected_rr", 0) for h in todays]
    avg_rr = safe_div(sum(rr_vals), len(rr_vals))
    gains = sum(h.get("expected_rr", 0) for h in todays if h["status"] != "stopped_out")
    losses_sum = sum(1 for h in todays if h["status"] == "stopped_out")
    profit_factor = safe_div(gains, losses_sum, default=gains)

    by_engine = {}
    for h in todays:
        e = by_engine.setdefault(h["engine"], {"w": 0, "l": 0})
        e["w" if h["status"] != "stopped_out" else "l"] += 1
    by_regime = {}
    for h in todays:
        r = by_regime.setdefault(h.get("regime", "unknown"), {"w": 0, "l": 0})
        r["w" if h["status"] != "stopped_out" else "l"] += 1

    best = max(todays, key=lambda h: h.get("expected_rr", 0), default=None)
    worst = min(todays, key=lambda h: h.get("expected_rr", 0), default=None)

    calib_lines = []
    for bucket, c in sorted(state.get("confidence_calibration", {}).items(), key=lambda x: int(x[0])):
        if c["count"] > 0:
            calib_lines.append(f"  {bucket}%: predicted~{bucket}% actual={100*c['wins']/c['count']:.0f}% (n={c['count']})")

    lines = [
        f"<b>{ENGINE_NAME} v{ENGINE_VERSION} -- Daily Summary ({today})</b>",
        f"Signals: {total}  Wins: {wins}  Losses: {losses}  Win rate: {win_rate:.1f}%",
        f"Profit factor: {profit_factor:.2f}  Avg RR: {avg_rr:.2f}",
        "By engine: " + ", ".join(f"{k}({v['w']}W/{v['l']}L)" for k, v in by_engine.items()) if by_engine else "By engine: none",
        "By regime: " + ", ".join(f"{k}({v['w']}W/{v['l']}L)" for k, v in by_regime.items()) if by_regime else "By regime: none",
        f"Best: {best['symbol']} ({best['engine']}, RR {best.get('expected_rr', 0):.2f})" if best else "Best: n/a",
        f"Worst: {worst['symbol']} ({worst['engine']}, RR {worst.get('expected_rr', 0):.2f})" if worst else "Worst: n/a",
        "Confidence calibration:\n" + "\n".join(calib_lines) if calib_lines else "Confidence calibration: insufficient data",
        f"Governor threshold adjustment: {state.get('governor_adjustment', 0.0):+.1f}",
    ]
    return "\n".join(lines)


def maybe_send_daily_summary(state: dict):
    now = datetime.now(timezone.utc)
    today_str = now.date().isoformat()
    if now.hour == 8 and state.get("last_daily_summary_date") != today_str:
        text = build_daily_summary(state)
        _tg_call("sendMessage", {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"})
        state["last_daily_summary_date"] = today_str


# ============================================================================
# SECTION 13: PER-SYMBOL ANALYSIS PIPELINE
# ============================================================================

def build_symbol_context(symbol: str, bundle: dict) -> Optional[dict]:
    ltf, mtf, htf, d1 = bundle[TF_LTF], bundle[TF_MTF], bundle[TF_HTF], bundle[TF_D1]
    ltf_swings = find_swings(ltf["candles"])
    htf_swings = find_swings(htf["candles"])
    ltf_structure = analyze_structure(ltf["candles"], ltf_swings)
    htf_structure = analyze_structure(htf["candles"], htf_swings)
    d1_structure = analyze_structure(d1["candles"], find_swings(d1["candles"]))
    mtf_structure = analyze_structure(mtf["candles"], find_swings(mtf["candles"]))

    ob = find_order_blocks(ltf["candles"], ltf["ind"]["atr"])
    fvg = find_fvgs(ltf["candles"])
    zones = mark_mitigation_and_breakers(ob + fvg, ltf["candles"])
    liq = build_liquidity_pools(htf_swings)
    pd_zone = premium_discount_zone(htf["candles"])
    regime = classify_regime(mtf["ind"], htf_structure)

    return {
        "symbol": symbol, "ltf": ltf, "mtf": mtf, "htf": htf, "d1": d1,
        "ltf_zones": zones, "liq": liq, "pd_zone": pd_zone, "regime": regime,
        "htf_structure": htf_structure,
        "structure_by_tf": {TF_LTF: ltf_structure, TF_MTF: mtf_structure, TF_HTF: htf_structure, TF_D1: d1_structure},
    }


def run_all_engines(ctx: dict) -> list[Signal]:
    out = []
    for name in ENGINE_LIST:
        try:
            out.extend(ENGINE_FUNCS[name](ctx))
        except Exception as exc:  # an isolated engine failure must not kill the scan
            log.exception("Engine %s failed for %s: %s", name, ctx["symbol"], exc)
    return out


# ============================================================================
# SECTION 14: MAIN ORCHESTRATION
# ============================================================================

def process_symbol(symbol: str, reference_ms: int, cache: dict) -> Optional[dict]:
    bundle = fetch_symbol_bundle(symbol, reference_ms, cache)
    if not bundle:
        return None
    ctx = build_symbol_context(symbol, bundle)
    ctx["signals"] = run_all_engines(ctx)
    ctx["returns"] = compute_returns(bundle[TF_MTF]["ind"]["closes"], 60)
    return ctx


def main():
    started = time.time()
    reference_ms = int(started * 1000)
    state = load_state()
    cache = load_candle_cache()

    log.info("%s v%s scan starting -- %d symbols", ENGINE_NAME, ENGINE_VERSION, len(WATCHLIST))

    contexts: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as pool:
        futures = {pool.submit(process_symbol, s, reference_ms, cache): s for s in WATCHLIST}
        for fut in as_completed(futures):
            symbol = futures[fut]
            try:
                ctx = fut.result()
                if ctx:
                    contexts[symbol] = ctx
            except Exception as exc:
                log.exception("Symbol %s processing failed: %s", symbol, exc)

    save_candle_cache(cache)

    # --- resolve open signals against fresh candles (learning + status updates) ---
    live_candles = {s: c["ltf"]["candles"] for s, c in contexts.items()}
    live_prices = {s: c["ltf"]["candles"][-1]["c"] for s, c in contexts.items()}
    events = resolve_open_signals(state, live_prices, live_candles)
    for ev in events:
        send_status_update(ev["signal"], ev["status"])

    # --- rank and select new candidates ---
    all_candidates: list[Signal] = []
    returns_by_symbol = {}
    for symbol, ctx in contexts.items():
        all_candidates.extend(ctx["signals"])
        returns_by_symbol[symbol] = ctx["returns"]

    clusters = correlation_clusters(returns_by_symbol) if len(returns_by_symbol) > 1 else []
    scored = []
    open_symbols_dirs = {(o["symbol"], o["direction"]) for o in state["open_signals"]}
    for sig in all_candidates:
        if (sig.symbol, sig.direction) in open_symbols_dirs:
            continue
        st = contexts[sig.symbol]["structure_by_tf"]
        align = mtf_alignment_score(contexts[sig.symbol], st, sig.direction)
        score = score_signal(sig, state, align)
        scored.append((sig, score))

    threshold = governor_threshold(state)
    selected = rank_and_select(scored, clusters, MAX_SIGNALS_PER_SCAN, threshold)

    today_str = datetime.now(timezone.utc).date().isoformat()
    if not state["daily_signal_counts"] or state.get("_last_count_date") != today_str:
        state["daily_signal_counts"].append(0)
        state["_last_count_date"] = today_str

    for sig in selected:
        grade = grade_for_confidence(sig.confidence)
        msg_id = send_signal_telegram(sig)
        record = asdict(sig)
        record.update({
            "opened_at_ms": reference_ms, "status": "activated", "hit_tp1": False,
            "tg_message_id": msg_id, "grade": grade,
            "regime": contexts[sig.symbol]["regime"].label,
        })
        state["open_signals"].append(record)
        state["daily_signal_counts"][-1] += 1
        log.info("Signal emitted: %s %s via %s conf=%.0f rr=%.2f", sig.symbol, sig.direction,
                  sig.engine, sig.confidence, sig.expected_rr)

    maybe_send_daily_summary(state)
    prune_state(state)
    save_state(state)

    elapsed = time.time() - started
    log.info("Scan complete in %.1fs -- %d symbols processed, %d candidates, %d signals emitted",
              elapsed, len(contexts), len(all_candidates), len(selected))


if __name__ == "__main__":
    main()
