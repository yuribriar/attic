# ══════════════════════════════════════════════════════════════════════════
#  VANTAGE — Adaptive Multi-Engine Institutional Signal System
#  v1.0.0
#
#  Original architecture. Built from a gap analysis of four reference
#  engines (Kestrel, Axis, Kairos, Meridian) — no code merged from any of
#  them. Runs a panel of thirteen independent specialist engines against
#  a shared HTF->LTF market model, then arbitrates their output through a
#  single Decision Engine that ranks opportunities by expected value,
#  regime fit, and live historical performance, publishing only the setups
#  that clear an adaptive quality bar. A persistent learning store tracks
#  every trade to calibrate confidence and engine weighting over time.
# ══════════════════════════════════════════════════════════════════════════

from __future__ import annotations

import os
import sys
import json
import math
import time
import random
import logging
import statistics
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

ENGINE_NAME = "VANTAGE"
__version__ = "1.0.0"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(ENGINE_NAME)

# ══════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")
if not TG_BOT_TOKEN:
    raise RuntimeError("TG_BOT_TOKEN environment variable is required")
if not TG_CHAT_ID:
    raise RuntimeError("TG_CHAT_ID environment variable is required")

STATE_PATH = Path(os.environ.get("VANTAGE_STATE_PATH", "state.json"))
HL_BASE_URL = "https://api.hyperliquid.xyz/info"
HL_MIN_INTERVAL_S = float(os.environ.get("HL_MIN_INTERVAL_S", "0.20"))
HL_MAX_RETRIES = int(os.environ.get("HL_MAX_RETRIES", "4"))
SCAN_WORKERS = int(os.environ.get("SCAN_WORKERS", "4"))
SCAN_INTERVAL_MIN = 15

WATCHLIST = [
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK",
    "SUI", "NEAR", "DOT", "TRX", "BCH", "LTC", "APT", "AAVE", "ONDO",
    "TAO", "UNI", "XLM", "HYPE", "PENDLE", "ZEC", "PENGU",
]
MAJORS = {"BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOGE"}

# ── TIMEFRAME STACK (15m floor per mandate; no sub-15m timeframes) ─────────
TF_MACRO, TF_HTF, TF_MID, TF_LTF = "1d", "4h", "1h", "15m"
TF_BARS = {TF_MACRO: 150, TF_HTF: 300, TF_MID: 300, TF_LTF: 320}

# ── INDICATOR LENGTHS ───────────────────────────────────────────────────────
EMA_FAST, EMA_SLOW, EMA_TREND = 21, 50, 200
RSI_LEN, ATR_LEN, ADX_LEN, BB_LEN = 14, 14, 14, 20
BB_MULT = 2.0

# ── ZONE / STRUCTURE THRESHOLDS ─────────────────────────────────────────────
DISPLACEMENT_ATR_MULT = 1.1
STRUCTURE_LOOKBACK = 30
FVG_MIN_ATR_MULT = 0.10
ZONE_MAX_WIDTH_ATR_MULT = 1.8
SWING_LEFT_RIGHT = 3

# ── QUALITY / RISK THRESHOLDS ───────────────────────────────────────────────
MIN_CONFIDENCE_BASE = 62.0
MIN_RR_BASE = 1.5
MAX_SIGNALS_PER_RUN = 6
MAX_OPEN_PER_SYMBOL = 1
MAX_OPEN_TOTAL = 12
CORRELATION_GROUPS = {
    "majors": {"BTC", "ETH"},
    "l1": {"SOL", "AVAX", "NEAR", "APT", "SUI", "ADA", "DOT", "TRX"},
    "defi": {"AAVE", "UNI", "LINK", "PENDLE", "ONDO"},
    "meme": {"DOGE", "PENGU"},
    "legacy": {"LTC", "BCH", "XLM"},
}

DAILY_SUMMARY_HOUR_UTC = 8

# ══════════════════════════════════════════════════════════════════════════
#  GENERIC MATH / INDICATOR UTILITIES
# ══════════════════════════════════════════════════════════════════════════

def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    k = 2.0 / (period + 1.0)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def sma(values: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(values)):
        lo = max(0, i - period + 1)
        window = values[lo:i + 1]
        out.append(sum(window) / len(window))
    return out


def stdev(values: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(values)):
        lo = max(0, i - period + 1)
        window = values[lo:i + 1]
        if len(window) < 2:
            out.append(0.0)
            continue
        out.append(statistics.pstdev(window))
    return out


def true_range(candles: list[dict]) -> list[float]:
    out = []
    prev_close = None
    for c in candles:
        h, l, cl = c["h"], c["l"], c["c"]
        if prev_close is None:
            out.append(h - l)
        else:
            out.append(max(h - l, abs(h - prev_close), abs(l - prev_close)))
        prev_close = cl
    return out


def atr_series(candles: list[dict], period: int = ATR_LEN) -> list[float]:
    tr = true_range(candles)
    if not tr:
        return []
    out = [tr[0]]
    for i in range(1, len(tr)):
        if i < period:
            out.append(sum(tr[:i + 1]) / (i + 1))
        else:
            out.append((out[-1] * (period - 1) + tr[i]) / period)
    return out


def rsi_series(closes: list[float], period: int = RSI_LEN) -> list[float]:
    if len(closes) < 2:
        return [50.0] * len(closes)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_gain = sum(gains[1:period + 1]) / period if len(gains) > period else sum(gains) / max(len(gains), 1)
    avg_loss = sum(losses[1:period + 1]) / period if len(losses) > period else sum(losses) / max(len(losses), 1)
    out = [50.0] * min(period, len(closes))
    for i in range(period, len(closes)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs = avg_gain / avg_loss if avg_loss > 1e-12 else 100.0
        out.append(100.0 - (100.0 / (1.0 + rs)))
    while len(out) < len(closes):
        out.append(out[-1] if out else 50.0)
    return out[:len(closes)]


def adx_series(candles: list[dict], period: int = ADX_LEN) -> list[float]:
    n = len(candles)
    if n < 2:
        return [0.0] * n
    plus_dm, minus_dm, tr = [0.0], [0.0], [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, n):
        up = candles[i]["h"] - candles[i - 1]["h"]
        down = candles[i - 1]["l"] - candles[i]["l"]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        tr.append(max(
            candles[i]["h"] - candles[i]["l"],
            abs(candles[i]["h"] - candles[i - 1]["c"]),
            abs(candles[i]["l"] - candles[i - 1]["c"]),
        ))
    atr = atr_series(candles, period)
    pdi, mdi, dx = [], [], []
    sm_plus = sm_minus = 0.0
    for i in range(n):
        if i < period:
            sm_plus = sum(plus_dm[:i + 1])
            sm_minus = sum(minus_dm[:i + 1])
        else:
            sm_plus = sm_plus - (sm_plus / period) + plus_dm[i]
            sm_minus = sm_minus - (sm_minus / period) + minus_dm[i]
        a = atr[i] if atr[i] > 1e-12 else 1e-12
        pd = 100 * (sm_plus / period) / a
        md = 100 * (sm_minus / period) / a
        pdi.append(pd)
        mdi.append(md)
        denom = pd + md if (pd + md) > 1e-12 else 1e-12
        dx.append(100 * abs(pd - md) / denom)
    out = [dx[0]]
    for i in range(1, n):
        if i < period:
            out.append(sum(dx[:i + 1]) / (i + 1))
        else:
            out.append((out[-1] * (period - 1) + dx[i]) / period)
    return out


def bollinger_width(closes: list[float], period: int = BB_LEN, mult: float = BB_MULT) -> list[float]:
    mid = sma(closes, period)
    sd = stdev(closes, period)
    out = []
    for i in range(len(closes)):
        upper = mid[i] + mult * sd[i]
        lower = mid[i] - mult * sd[i]
        out.append((upper - lower) / mid[i] if mid[i] > 1e-12 else 0.0)
    return out


def percentile_rank(values: list[float], target: float) -> float:
    if not values:
        return 50.0
    below = sum(1 for v in values if v <= target)
    return 100.0 * below / len(values)


# ══════════════════════════════════════════════════════════════════════════
#  DATA MODEL
# ══════════════════════════════════════════════════════════════════════════

@dataclass
class MarketFrame:
    """One timeframe's candles plus derived indicator series for a symbol."""
    tf: str
    candles: list[dict]
    closes: list[float] = field(default_factory=list)
    ema_fast: list[float] = field(default_factory=list)
    ema_slow: list[float] = field(default_factory=list)
    ema_trend: list[float] = field(default_factory=list)
    rsi: list[float] = field(default_factory=list)
    atr: list[float] = field(default_factory=list)
    adx: list[float] = field(default_factory=list)
    bb_width: list[float] = field(default_factory=list)

    @classmethod
    def build(cls, tf: str, candles: list[dict]) -> "MarketFrame":
        closes = [c["c"] for c in candles]
        return cls(
            tf=tf, candles=candles, closes=closes,
            ema_fast=ema(closes, EMA_FAST),
            ema_slow=ema(closes, EMA_SLOW),
            ema_trend=ema(closes, EMA_TREND),
            rsi=rsi_series(closes, RSI_LEN),
            atr=atr_series(candles, ATR_LEN),
            adx=adx_series(candles, ADX_LEN),
            bb_width=bollinger_width(closes, BB_LEN, BB_MULT),
        )

    def last(self, series: list[float], back: int = 0) -> float:
        if not series:
            return 0.0
        idx = len(series) - 1 - back
        return series[max(idx, 0)]


@dataclass
class Zone:
    kind: str            # "ob_bull" | "ob_bear" | "breaker_bull" | "breaker_bear" | "fvg_bull" | "fvg_bear"
    top: float
    bottom: float
    origin_idx: int
    mitigated: bool = False


@dataclass
class StructureMap:
    bias: str                       # "bullish" | "bearish" | "neutral"
    bos: bool
    choch: bool
    zones: list[Zone]
    swing_high: float
    swing_low: float
    premium_discount: float         # 0 = at swing low, 1 = at swing high
    liquidity_high: float
    liquidity_low: float


@dataclass
class EngineSignal:
    engine: str
    symbol: str
    direction: str                  # "long" | "short"
    entry: float
    stop: float
    tp1: float
    tp2: float
    confidence: float
    expected_rr: float
    confluences: list[str]
    regime: str


@dataclass
class Signal:
    id: str
    symbol: str
    engine: str
    direction: str
    entry: float
    stop: float
    tp1: float
    tp2: float
    confidence: float
    expected_rr: float
    confluences: list[str]
    regime: str
    quality_score: float
    created_at: str
    status: str = "activated"       # activated -> tp1 -> tp2/closed/sl/be/cancelled
    tp1_hit: bool = False
    message_id: Optional[int] = None


# ══════════════════════════════════════════════════════════════════════════
#  HYPERLIQUID CLIENT — rate limited, cached, retried
# ══════════════════════════════════════════════════════════════════════════

class HyperliquidClient:
    _INTERVAL_MAP = {"15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d"}

    def __init__(self):
        self._last_call = 0.0
        self._cache: dict[tuple, list[dict]] = {}
        self._session = requests.Session()

    def _throttle(self):
        elapsed = time.time() - self._last_call
        if elapsed < HL_MIN_INTERVAL_S:
            time.sleep(HL_MIN_INTERVAL_S - elapsed)
        self._last_call = time.time()

    def _post(self, payload: dict) -> Any:
        backoff = 0.5
        last_exc = None
        for attempt in range(HL_MAX_RETRIES):
            self._throttle()
            try:
                r = self._session.post(HL_BASE_URL, json=payload, timeout=10)
                if r.status_code == 429:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                r.raise_for_status()
                return r.json()
            except requests.RequestException as e:
                last_exc = e
                time.sleep(backoff + random.uniform(0, 0.25))
                backoff *= 2
        log.warning(f"Hyperliquid request failed after retries: {last_exc}")
        return None

    def candles(self, symbol: str, tf: str, bars: int) -> list[dict]:
        cache_key = (symbol, tf, bars)
        if cache_key in self._cache:
            return self._cache[cache_key]
        interval = self._INTERVAL_MAP[tf]
        now_ms = int(time.time() * 1000)
        tf_ms = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}[tf]
        start_ms = now_ms - bars * tf_ms
        payload = {
            "type": "candleSnapshot",
            "req": {"coin": symbol, "interval": interval, "startTime": start_ms, "endTime": now_ms},
        }
        raw = self._post(payload)
        if not raw:
            self._cache[cache_key] = []
            return []
        candles = [
            {"t": row["t"], "o": float(row["o"]), "h": float(row["h"]),
             "l": float(row["l"]), "c": float(row["c"]), "v": float(row.get("v", 0.0))}
            for row in raw
        ]
        self._cache[cache_key] = candles
        return candles

    def mid_price(self, symbol: str) -> Optional[float]:
        raw = self._post({"type": "allMids"})
        if not raw or symbol not in raw:
            return None
        try:
            return float(raw[symbol])
        except (TypeError, ValueError):
            return None

    def load_symbol(self, symbol: str) -> dict[str, MarketFrame]:
        frames = {}
        for tf in (TF_MACRO, TF_HTF, TF_MID, TF_LTF):
            candles = self.candles(symbol, tf, TF_BARS[tf])
            if len(candles) < 30:
                continue
            frames[tf] = MarketFrame.build(tf, candles)
        return frames


# ══════════════════════════════════════════════════════════════════════════
#  STRUCTURE / SMART-MONEY MODEL
# ══════════════════════════════════════════════════════════════════════════

def swing_points(candles: list[dict], left: int = SWING_LEFT_RIGHT, right: int = SWING_LEFT_RIGHT):
    highs, lows = [], []
    for i in range(left, len(candles) - right):
        window = candles[i - left:i + right + 1]
        h = candles[i]["h"]
        l = candles[i]["l"]
        if h == max(c["h"] for c in window):
            highs.append((i, h))
        if l == min(c["l"] for c in window):
            lows.append((i, l))
    return highs, lows


def detect_structure(candles: list[dict], atr: list[float]) -> StructureMap:
    n = len(candles)
    lookback = candles[-STRUCTURE_LOOKBACK:] if n > STRUCTURE_LOOKBACK else candles
    highs, lows = swing_points(candles)
    recent_highs = [h for i, h in highs if i >= n - STRUCTURE_LOOKBACK]
    recent_lows = [l for i, l in lows if i >= n - STRUCTURE_LOOKBACK]
    swing_high = max(recent_highs) if recent_highs else max(c["h"] for c in lookback)
    swing_low = min(recent_lows) if recent_lows else min(c["l"] for c in lookback)

    closes = [c["c"] for c in candles]
    bos, choch, bias = False, False, "neutral"
    if len(highs) >= 2 and len(lows) >= 2:
        last_close = closes[-1]
        prior_high = highs[-2][1] if len(highs) >= 2 else swing_high
        prior_low = lows[-2][1] if len(lows) >= 2 else swing_low
        if last_close > prior_high:
            bos, bias = True, "bullish"
        elif last_close < prior_low:
            bos, bias = True, "bearish"
        ema_t = ema(closes, EMA_TREND)
        if bias == "neutral":
            bias = "bullish" if closes[-1] > ema_t[-1] else "bearish"
        # CHoCH: opposite-direction break of the most recent minor swing
        if len(highs) >= 3 and len(lows) >= 3:
            if bias == "bullish" and closes[-1] < lows[-2][1]:
                choch = True
            elif bias == "bearish" and closes[-1] > highs[-2][1]:
                choch = True

    zones = detect_zones(candles, atr, bias)
    rng = swing_high - swing_low if swing_high > swing_low else 1e-9
    pd = (closes[-1] - swing_low) / rng
    pd = min(max(pd, 0.0), 1.0)

    return StructureMap(
        bias=bias, bos=bos, choch=choch, zones=zones,
        swing_high=swing_high, swing_low=swing_low,
        premium_discount=pd, liquidity_high=swing_high, liquidity_low=swing_low,
    )


def detect_zones(candles: list[dict], atr: list[float], bias: str) -> list[Zone]:
    zones: list[Zone] = []
    n = len(candles)
    lo_bound = max(0, n - STRUCTURE_LOOKBACK * 2)

    # Order blocks: last opposite candle before a displacement move that
    # breaks recent structure.
    for i in range(lo_bound + 2, n - 1):
        body = abs(candles[i]["c"] - candles[i]["o"])
        a = atr[i] if atr[i] > 1e-12 else 1e-9
        if body / a < DISPLACEMENT_ATR_MULT:
            continue
        bullish_disp = candles[i]["c"] > candles[i]["o"]
        prior = candles[i - 1]
        if bullish_disp and prior["c"] < prior["o"]:
            zones.append(Zone(kind="ob_bull", top=prior["h"], bottom=prior["l"], origin_idx=i - 1))
        elif (not bullish_disp) and prior["c"] > prior["o"]:
            zones.append(Zone(kind="ob_bear", top=prior["h"], bottom=prior["l"], origin_idx=i - 1))

    # Fair value gaps: 3-candle imbalance.
    for i in range(lo_bound + 2, n):
        a = atr[i] if atr[i] > 1e-12 else 1e-9
        c0, c2 = candles[i - 2], candles[i]
        if c2["l"] > c0["h"] and (c2["l"] - c0["h"]) / a > FVG_MIN_ATR_MULT:
            zones.append(Zone(kind="fvg_bull", top=c2["l"], bottom=c0["h"], origin_idx=i - 1))
        elif c0["l"] > c2["h"] and (c0["l"] - c2["h"]) / a > FVG_MIN_ATR_MULT:
            zones.append(Zone(kind="fvg_bear", top=c0["l"], bottom=c2["h"], origin_idx=i - 1))

    # Mark mitigation and promote mitigated OBs to breakers.
    last_close = candles[-1]["c"]
    promoted = []
    for z in zones:
        touched = any(c["l"] <= z.top and c["h"] >= z.bottom for c in candles[z.origin_idx + 2:])
        z.mitigated = touched
        if z.kind == "ob_bull" and touched and last_close < z.bottom:
            promoted.append(Zone(kind="breaker_bear", top=z.top, bottom=z.bottom, origin_idx=z.origin_idx, mitigated=False))
        elif z.kind == "ob_bear" and touched and last_close > z.top:
            promoted.append(Zone(kind="breaker_bull", top=z.top, bottom=z.bottom, origin_idx=z.origin_idx, mitigated=False))

    zones.extend(promoted)
    # Filter zones too wide to be tradable POIs.
    a_last = atr[-1] if atr and atr[-1] > 1e-12 else 1e-9
    zones = [z for z in zones if (z.top - z.bottom) / a_last <= ZONE_MAX_WIDTH_ATR_MULT]
    return zones[-40:]


def classify_regime(mid: MarketFrame) -> str:
    adx_now = mid.last(mid.adx)
    bbw_now = mid.last(mid.bb_width)
    bbw_hist = mid.bb_width[-100:] if len(mid.bb_width) >= 20 else mid.bb_width
    bbw_pct = percentile_rank(bbw_hist, bbw_now)
    if adx_now >= 27 and bbw_pct >= 55:
        return "trending"
    if adx_now < 18 and bbw_pct <= 40:
        return "ranging"
    if bbw_pct >= 80:
        return "expansion"
    if bbw_pct <= 15:
        return "consolidation"
    return "neutral"


def nearest_zone(zones: list[Zone], price: float, kinds: tuple[str, ...], max_dist_pct: float = 0.06) -> Optional[Zone]:
    candidates = [z for z in zones if z.kind in kinds and not z.mitigated]
    best, best_dist = None, float("inf")
    for z in candidates:
        mid_z = (z.top + z.bottom) / 2
        dist = abs(price - mid_z) / price
        if dist < best_dist and dist <= max_dist_pct:
            best, best_dist = z, dist
    return best


# ══════════════════════════════════════════════════════════════════════════
#  SPECIALIST ENGINES
#  Each engine reads the shared MarketFrame stack + StructureMap and either
#  returns an EngineSignal or None. All SL/TP anchoring uses candle
#  highs/lows only, never midpoint or live price.
# ══════════════════════════════════════════════════════════════════════════

class BaseEngine:
    name = "base"

    def generate(self, symbol: str, frames: dict[str, MarketFrame],
                 structure: dict[str, StructureMap], regime: str) -> Optional[EngineSignal]:
        raise NotImplementedError

    def _rr(self, entry, stop, tp) -> float:
        risk = abs(entry - stop)
        if risk < 1e-12:
            return 0.0
        return abs(tp - entry) / risk


class SmartMoneyConceptEngine(BaseEngine):
    name = "SMC"

    def generate(self, symbol, frames, structure, regime):
        if TF_LTF not in frames or TF_HTF not in frames:
            return None
        htf_s = structure[TF_HTF]
        ltf = frames[TF_LTF]
        price = ltf.last(ltf.closes)
        atr = ltf.last(ltf.atr)
        if htf_s.bias == "bullish":
            zone = nearest_zone(htf_s.zones, price, ("ob_bull", "breaker_bull"))
            if not zone or htf_s.premium_discount > 0.55:
                return None
            stop = zone.bottom - 0.15 * atr
            entry = price
            tp1 = entry + 1.5 * (entry - stop)
            tp2 = htf_s.liquidity_high
            if tp2 <= entry:
                tp2 = entry + 2.5 * (entry - stop)
            conf = 60 + (10 if htf_s.bos else 0) + (8 if not zone.mitigated else 0)
            return EngineSignal(self.name, symbol, "long", entry, stop, tp1, tp2, conf,
                                 self._rr(entry, stop, tp2), ["HTF bullish OB/Breaker", "discount zone"], regime)
        if htf_s.bias == "bearish":
            zone = nearest_zone(htf_s.zones, price, ("ob_bear", "breaker_bear"))
            if not zone or htf_s.premium_discount < 0.45:
                return None
            stop = zone.top + 0.15 * atr
            entry = price
            tp1 = entry - 1.5 * (stop - entry)
            tp2 = htf_s.liquidity_low
            if tp2 >= entry:
                tp2 = entry - 2.5 * (stop - entry)
            conf = 60 + (10 if htf_s.bos else 0) + (8 if not zone.mitigated else 0)
            return EngineSignal(self.name, symbol, "short", entry, stop, tp1, tp2, conf,
                                 self._rr(entry, stop, tp2), ["HTF bearish OB/Breaker", "premium zone"], regime)
        return None


class TrendContinuationEngine(BaseEngine):
    name = "TrendContinuation"

    def generate(self, symbol, frames, structure, regime):
        if regime not in ("trending", "expansion") or TF_MID not in frames:
            return None
        mid = frames[TF_MID]
        price = mid.last(mid.closes)
        atr = mid.last(mid.atr)
        f, s, t = mid.last(mid.ema_fast), mid.last(mid.ema_slow), mid.last(mid.ema_trend)
        if f > s > t and price > f:
            recent_low = min(c["l"] for c in mid.candles[-10:])
            stop = recent_low - 0.2 * atr
            entry = price
            tp1 = entry + 1.5 * (entry - stop)
            tp2 = entry + 3.0 * (entry - stop)
            conf = 58 + min(mid.last(mid.adx) - 20, 15)
            return EngineSignal(self.name, symbol, "long", entry, stop, tp1, tp2, conf,
                                 self._rr(entry, stop, tp2), ["EMA stack aligned bullish", "trending regime"], regime)
        if f < s < t and price < f:
            recent_high = max(c["h"] for c in mid.candles[-10:])
            stop = recent_high + 0.2 * atr
            entry = price
            tp1 = entry - 1.5 * (stop - entry)
            tp2 = entry - 3.0 * (stop - entry)
            conf = 58 + min(mid.last(mid.adx) - 20, 15)
            return EngineSignal(self.name, symbol, "short", entry, stop, tp1, tp2, conf,
                                 self._rr(entry, stop, tp2), ["EMA stack aligned bearish", "trending regime"], regime)
        return None


class BreakoutEngine(BaseEngine):
    name = "Breakout"

    def generate(self, symbol, frames, structure, regime):
        if TF_MID not in frames:
            return None
        mid = frames[TF_MID]
        candles = mid.candles
        atr = mid.last(mid.atr)
        window = candles[-21:-1]
        hi, lo = max(c["h"] for c in window), min(c["l"] for c in window)
        last = candles[-1]
        body = abs(last["c"] - last["o"])
        if last["c"] > hi and body / max(atr, 1e-9) > 0.9:
            stop = hi - 0.25 * atr
            entry = last["c"]
            tp1 = entry + 1.5 * (entry - stop)
            tp2 = entry + 2.8 * (entry - stop)
            conf = 55 + (10 if regime in ("expansion", "trending") else 0)
            return EngineSignal(self.name, symbol, "long", entry, stop, tp1, tp2, conf,
                                 self._rr(entry, stop, tp2), ["Range high breakout", "displacement candle"], regime)
        if last["c"] < lo and body / max(atr, 1e-9) > 0.9:
            stop = lo + 0.25 * atr
            entry = last["c"]
            tp1 = entry - 1.5 * (stop - entry)
            tp2 = entry - 2.8 * (stop - entry)
            conf = 55 + (10 if regime in ("expansion", "trending") else 0)
            return EngineSignal(self.name, symbol, "short", entry, stop, tp1, tp2, conf,
                                 self._rr(entry, stop, tp2), ["Range low breakdown", "displacement candle"], regime)
        return None


class PullbackEngine(BaseEngine):
    name = "Pullback"

    def generate(self, symbol, frames, structure, regime):
        if TF_MID not in frames:
            return None
        mid = frames[TF_MID]
        price = mid.last(mid.closes)
        atr = mid.last(mid.atr)
        f, s, t = mid.last(mid.ema_fast), mid.last(mid.ema_slow), mid.last(mid.ema_trend)
        rsi_now = mid.last(mid.rsi)
        if t < f and s > 0 and price <= s * 1.01 and price >= s * 0.985 and rsi_now < 55 and f > t:
            stop = min(c["l"] for c in mid.candles[-6:]) - 0.2 * atr
            entry = price
            tp1 = entry + 1.4 * (entry - stop)
            tp2 = entry + 2.5 * (entry - stop)
            return EngineSignal(self.name, symbol, "long", entry, stop, tp1, tp2, 57.0,
                                 self._rr(entry, stop, tp2), ["Pullback to EMA50 in uptrend"], regime)
        if f < t and price >= s * 0.99 and price <= s * 1.015 and rsi_now > 45 and f < t:
            stop = max(c["h"] for c in mid.candles[-6:]) + 0.2 * atr
            entry = price
            tp1 = entry - 1.4 * (stop - entry)
            tp2 = entry - 2.5 * (stop - entry)
            return EngineSignal(self.name, symbol, "short", entry, stop, tp1, tp2, 57.0,
                                 self._rr(entry, stop, tp2), ["Pullback to EMA50 in downtrend"], regime)
        return None


class LiquiditySweepEngine(BaseEngine):
    name = "LiquiditySweep"

    def generate(self, symbol, frames, structure, regime):
        if TF_LTF not in frames:
            return None
        ltf = frames[TF_LTF]
        candles = ltf.candles
        atr = ltf.last(ltf.atr)
        s = structure[TF_LTF]
        last, prev = candles[-1], candles[-2]
        wick_low = min(last["o"], last["c"]) - last["l"]
        wick_high = last["h"] - max(last["o"], last["c"])
        if last["l"] < s.liquidity_low and last["c"] > s.liquidity_low and wick_low > 0.6 * atr:
            stop = last["l"] - 0.1 * atr
            entry = last["c"]
            tp1 = entry + 1.5 * (entry - stop)
            tp2 = s.liquidity_high if s.liquidity_high > entry else entry + 2.5 * (entry - stop)
            return EngineSignal(self.name, symbol, "long", entry, stop, tp1, tp2, 63.0,
                                 self._rr(entry, stop, tp2), ["Wick sweep below liquidity low", "reclaim close"], regime)
        if last["h"] > s.liquidity_high and last["c"] < s.liquidity_high and wick_high > 0.6 * atr:
            stop = last["h"] + 0.1 * atr
            entry = last["c"]
            tp1 = entry - 1.5 * (stop - entry)
            tp2 = s.liquidity_low if s.liquidity_low < entry else entry - 2.5 * (stop - entry)
            return EngineSignal(self.name, symbol, "short", entry, stop, tp1, tp2, 63.0,
                                 self._rr(entry, stop, tp2), ["Wick sweep above liquidity high", "reclaim close"], regime)
        return None


class OrderBlockEngine(BaseEngine):
    name = "OrderBlock"

    def generate(self, symbol, frames, structure, regime):
        if TF_LTF not in frames:
            return None
        ltf = frames[TF_LTF]
        s = structure[TF_LTF]
        price = ltf.last(ltf.closes)
        atr = ltf.last(ltf.atr)
        zone = nearest_zone(s.zones, price, ("ob_bull",), max_dist_pct=0.02)
        if zone and price >= zone.bottom and price <= zone.top * 1.01:
            stop = zone.bottom - 0.15 * atr
            entry = price
            tp1 = entry + 1.5 * (entry - stop)
            tp2 = entry + 2.5 * (entry - stop)
            return EngineSignal(self.name, symbol, "long", entry, stop, tp1, tp2, 58.0,
                                 self._rr(entry, stop, tp2), ["Reaction at LTF bullish order block"], regime)
        zone = nearest_zone(s.zones, price, ("ob_bear",), max_dist_pct=0.02)
        if zone and price <= zone.top and price >= zone.bottom * 0.99:
            stop = zone.top + 0.15 * atr
            entry = price
            tp1 = entry - 1.5 * (stop - entry)
            tp2 = entry - 2.5 * (stop - entry)
            return EngineSignal(self.name, symbol, "short", entry, stop, tp1, tp2, 58.0,
                                 self._rr(entry, stop, tp2), ["Reaction at LTF bearish order block"], regime)
        return None


class BreakerBlockEngine(BaseEngine):
    name = "BreakerBlock"

    def generate(self, symbol, frames, structure, regime):
        if TF_LTF not in frames:
            return None
        ltf = frames[TF_LTF]
        s = structure[TF_LTF]
        price = ltf.last(ltf.closes)
        atr = ltf.last(ltf.atr)
        zone = nearest_zone(s.zones, price, ("breaker_bull",), max_dist_pct=0.02)
        if zone and zone.bottom <= price <= zone.top * 1.01:
            stop = zone.bottom - 0.15 * atr
            entry = price
            tp1 = entry + 1.6 * (entry - stop)
            tp2 = entry + 2.8 * (entry - stop)
            return EngineSignal(self.name, symbol, "long", entry, stop, tp1, tp2, 61.0,
                                 self._rr(entry, stop, tp2), ["Retest of bullish breaker block"], regime)
        zone = nearest_zone(s.zones, price, ("breaker_bear",), max_dist_pct=0.02)
        if zone and zone.bottom * 0.99 <= price <= zone.top:
            stop = zone.top + 0.15 * atr
            entry = price
            tp1 = entry - 1.6 * (stop - entry)
            tp2 = entry - 2.8 * (stop - entry)
            return EngineSignal(self.name, symbol, "short", entry, stop, tp1, tp2, 61.0,
                                 self._rr(entry, stop, tp2), ["Retest of bearish breaker block"], regime)
        return None


class FairValueGapEngine(BaseEngine):
    name = "FVG"

    def generate(self, symbol, frames, structure, regime):
        if TF_LTF not in frames:
            return None
        ltf = frames[TF_LTF]
        s = structure[TF_LTF]
        price = ltf.last(ltf.closes)
        atr = ltf.last(ltf.atr)
        zone = nearest_zone(s.zones, price, ("fvg_bull",), max_dist_pct=0.015)
        if zone and zone.bottom <= price <= zone.top:
            stop = zone.bottom - 0.2 * atr
            entry = price
            tp1 = entry + 1.4 * (entry - stop)
            tp2 = entry + 2.4 * (entry - stop)
            return EngineSignal(self.name, symbol, "long", entry, stop, tp1, tp2, 55.0,
                                 self._rr(entry, stop, tp2), ["Fill of bullish fair value gap"], regime)
        zone = nearest_zone(s.zones, price, ("fvg_bear",), max_dist_pct=0.015)
        if zone and zone.bottom <= price <= zone.top:
            stop = zone.top + 0.2 * atr
            entry = price
            tp1 = entry - 1.4 * (stop - entry)
            tp2 = entry - 2.4 * (stop - entry)
            return EngineSignal(self.name, symbol, "short", entry, stop, tp1, tp2, 55.0,
                                 self._rr(entry, stop, tp2), ["Fill of bearish fair value gap"], regime)
        return None


class MeanReversionEngine(BaseEngine):
    name = "MeanReversion"

    def generate(self, symbol, frames, structure, regime):
        if regime != "ranging" or TF_MID not in frames:
            return None
        mid = frames[TF_MID]
        price = mid.last(mid.closes)
        atr = mid.last(mid.atr)
        rsi_now = mid.last(mid.rsi)
        s = structure[TF_MID]
        rng = s.swing_high - s.swing_low
        if rng <= 0:
            return None
        if s.premium_discount < 0.20 and rsi_now < 35:
            stop = s.swing_low - 0.3 * atr
            entry = price
            tp1 = entry + 1.3 * (entry - stop)
            tp2 = s.swing_low + rng * 0.5
            return EngineSignal(self.name, symbol, "long", entry, stop, tp1, tp2, 56.0,
                                 self._rr(entry, stop, tp2), ["Deep discount in range", "RSI oversold"], regime)
        if s.premium_discount > 0.80 and rsi_now > 65:
            stop = s.swing_high + 0.3 * atr
            entry = price
            tp1 = entry - 1.3 * (stop - entry)
            tp2 = s.swing_low + rng * 0.5
            return EngineSignal(self.name, symbol, "short", entry, stop, tp1, tp2, 56.0,
                                 self._rr(entry, stop, tp2), ["Deep premium in range", "RSI overbought"], regime)
        return None


class ReversalEngine(BaseEngine):
    name = "Reversal"

    def generate(self, symbol, frames, structure, regime):
        if TF_MID not in frames:
            return None
        mid = frames[TF_MID]
        s = structure[TF_MID]
        price = mid.last(mid.closes)
        atr = mid.last(mid.atr)
        if not s.choch:
            return None
        if s.bias == "bullish":
            stop = s.swing_low - 0.25 * atr
            entry = price
            tp1 = entry + 1.5 * (entry - stop)
            tp2 = s.swing_high
            return EngineSignal(self.name, symbol, "long", entry, stop, tp1, tp2, 60.0,
                                 self._rr(entry, stop, tp2), ["Change of character confirmed bullish"], regime)
        if s.bias == "bearish":
            stop = s.swing_high + 0.25 * atr
            entry = price
            tp1 = entry - 1.5 * (stop - entry)
            tp2 = s.swing_low
            return EngineSignal(self.name, symbol, "short", entry, stop, tp1, tp2, 60.0,
                                 self._rr(entry, stop, tp2), ["Change of character confirmed bearish"], regime)
        return None


class MomentumEngine(BaseEngine):
    name = "Momentum"

    def generate(self, symbol, frames, structure, regime):
        if TF_MID not in frames:
            return None
        mid = frames[TF_MID]
        price = mid.last(mid.closes)
        atr = mid.last(mid.atr)
        rsi_now, rsi_prev = mid.last(mid.rsi), mid.last(mid.rsi, back=3)
        if rsi_now > 58 and rsi_now > rsi_prev and price > mid.last(mid.ema_fast):
            stop = min(c["l"] for c in mid.candles[-8:]) - 0.15 * atr
            entry = price
            tp1 = entry + 1.4 * (entry - stop)
            tp2 = entry + 2.4 * (entry - stop)
            return EngineSignal(self.name, symbol, "long", entry, stop, tp1, tp2, 54.0,
                                 self._rr(entry, stop, tp2), ["Rising momentum, RSI accelerating"], regime)
        if rsi_now < 42 and rsi_now < rsi_prev and price < mid.last(mid.ema_fast):
            stop = max(c["h"] for c in mid.candles[-8:]) + 0.15 * atr
            entry = price
            tp1 = entry - 1.4 * (stop - entry)
            tp2 = entry - 2.4 * (stop - entry)
            return EngineSignal(self.name, symbol, "short", entry, stop, tp1, tp2, 54.0,
                                 self._rr(entry, stop, tp2), ["Falling momentum, RSI accelerating"], regime)
        return None


class VolatilityExpansionEngine(BaseEngine):
    name = "VolatilityExpansion"

    def generate(self, symbol, frames, structure, regime):
        if regime != "expansion" or TF_MID not in frames:
            return None
        mid = frames[TF_MID]
        candles = mid.candles
        atr = mid.last(mid.atr)
        last = candles[-1]
        prior_range_avg = sum(c["h"] - c["l"] for c in candles[-11:-1]) / 10
        if (last["h"] - last["l"]) > 1.8 * prior_range_avg and last["c"] > last["o"]:
            stop = last["l"] - 0.2 * atr
            entry = last["c"]
            tp1 = entry + 1.5 * (entry - stop)
            tp2 = entry + 3.0 * (entry - stop)
            return EngineSignal(self.name, symbol, "long", entry, stop, tp1, tp2, 56.0,
                                 self._rr(entry, stop, tp2), ["Volatility expansion candle, bullish"], regime)
        if (last["h"] - last["l"]) > 1.8 * prior_range_avg and last["c"] < last["o"]:
            stop = last["h"] + 0.2 * atr
            entry = last["c"]
            tp1 = entry - 1.5 * (stop - entry)
            tp2 = entry - 3.0 * (stop - entry)
            return EngineSignal(self.name, symbol, "short", entry, stop, tp1, tp2, 56.0,
                                 self._rr(entry, stop, tp2), ["Volatility expansion candle, bearish"], regime)
        return None


class RangeTradingEngine(BaseEngine):
    name = "RangeTrading"

    def generate(self, symbol, frames, structure, regime):
        if regime != "ranging" or TF_LTF not in frames:
            return None
        ltf = frames[TF_LTF]
        s = structure[TF_LTF]
        price = ltf.last(ltf.closes)
        atr = ltf.last(ltf.atr)
        rng = s.swing_high - s.swing_low
        if rng <= 0:
            return None
        band = rng * 0.12
        if price <= s.swing_low + band:
            stop = s.swing_low - 0.3 * atr
            entry = price
            tp1 = s.swing_low + rng * 0.45
            tp2 = s.swing_high - band
            return EngineSignal(self.name, symbol, "long", entry, stop, tp1, tp2, 53.0,
                                 self._rr(entry, stop, tp2), ["Long at range support"], regime)
        if price >= s.swing_high - band:
            stop = s.swing_high + 0.3 * atr
            entry = price
            tp1 = s.swing_high - rng * 0.45
            tp2 = s.swing_low + band
            return EngineSignal(self.name, symbol, "short", entry, stop, tp1, tp2, 53.0,
                                 self._rr(entry, stop, tp2), ["Short at range resistance"], regime)
        return None


ALL_ENGINES: list[BaseEngine] = [
    SmartMoneyConceptEngine(), TrendContinuationEngine(), BreakoutEngine(),
    PullbackEngine(), LiquiditySweepEngine(), OrderBlockEngine(),
    BreakerBlockEngine(), FairValueGapEngine(), MeanReversionEngine(),
    ReversalEngine(), MomentumEngine(), VolatilityExpansionEngine(),
    RangeTradingEngine(),
]


# ══════════════════════════════════════════════════════════════════════════
#  LEARNING STORE — persistent stats, adaptive weighting, self-tuning bar
# ══════════════════════════════════════════════════════════════════════════

class LearningStore:
    MIN_SAMPLE_FOR_TRUST = 12

    def __init__(self, state: dict):
        self.state = state
        self.state.setdefault("engine_stats", {})
        self.state.setdefault("regime_stats", {})
        self.state.setdefault("global_stats", {"wins": 0, "losses": 0, "sum_rr": 0.0, "count": 0})
        self.state.setdefault("confidence_calibration", {})

    def engine_weight(self, engine: str) -> float:
        stats = self.state["engine_stats"].get(engine)
        if not stats or stats["wins"] + stats["losses"] < self.MIN_SAMPLE_FOR_TRUST:
            return 1.0
        wr = stats["wins"] / max(stats["wins"] + stats["losses"], 1)
        # shrink toward 1.0 for small samples, bounded [0.6, 1.4] to avoid overfitting
        n = stats["wins"] + stats["losses"]
        shrink = min(n / (n + 30), 0.85)
        raw_weight = 0.6 + 1.6 * wr
        return 1.0 + shrink * (raw_weight - 1.0)

    def regime_weight(self, engine: str, regime: str) -> float:
        key = f"{engine}::{regime}"
        stats = self.state["regime_stats"].get(key)
        if not stats or stats["wins"] + stats["losses"] < self.MIN_SAMPLE_FOR_TRUST:
            return 1.0
        wr = stats["wins"] / max(stats["wins"] + stats["losses"], 1)
        return 0.85 + 0.3 * wr

    def adaptive_confidence_floor(self) -> float:
        g = self.state["global_stats"]
        total = g["wins"] + g["losses"]
        if total < self.MIN_SAMPLE_FOR_TRUST:
            return MIN_CONFIDENCE_BASE
        wr = g["wins"] / max(total, 1)
        if wr < 0.40:
            return min(MIN_CONFIDENCE_BASE + 8, 82.0)
        if wr > 0.60:
            return max(MIN_CONFIDENCE_BASE - 5, 55.0)
        return MIN_CONFIDENCE_BASE

    def record_close(self, signal: Signal, outcome: str, realized_rr: float):
        win = outcome in ("tp1", "tp2")
        es = self.state["engine_stats"].setdefault(signal.engine, {"wins": 0, "losses": 0, "sum_rr": 0.0, "count": 0})
        es["wins"] += 1 if win else 0
        es["losses"] += 0 if win else 1
        es["sum_rr"] += realized_rr
        es["count"] += 1

        rk = f"{signal.engine}::{signal.regime}"
        rs = self.state["regime_stats"].setdefault(rk, {"wins": 0, "losses": 0, "sum_rr": 0.0, "count": 0})
        rs["wins"] += 1 if win else 0
        rs["losses"] += 0 if win else 1
        rs["sum_rr"] += realized_rr
        rs["count"] += 1

        g = self.state["global_stats"]
        g["wins"] += 1 if win else 0
        g["losses"] += 0 if win else 1
        g["sum_rr"] += realized_rr
        g["count"] += 1

        bucket = str(int(signal.confidence // 10) * 10)
        cal = self.state["confidence_calibration"].setdefault(bucket, {"wins": 0, "total": 0})
        cal["wins"] += 1 if win else 0
        cal["total"] += 1


# ══════════════════════════════════════════════════════════════════════════
#  DECISION ENGINE — arbitrates all specialist output into a ranked shortlist
# ══════════════════════════════════════════════════════════════════════════

def correlation_group(symbol: str) -> str:
    for group, members in CORRELATION_GROUPS.items():
        if symbol in members:
            return group
    return "other"


class DecisionEngine:
    def __init__(self, learning: LearningStore):
        self.learning = learning

    def score(self, sig: EngineSignal, mtf_confluence: float) -> float:
        w_engine = self.learning.engine_weight(sig.engine)
        w_regime = self.learning.regime_weight(sig.engine, sig.regime)
        rr_bonus = min(max(sig.expected_rr - MIN_RR_BASE, 0) * 4, 12)
        confluence_bonus = len(sig.confluences) * 2.0
        base = sig.confidence * w_engine * w_regime
        return base + rr_bonus + confluence_bonus + mtf_confluence

    def rank(self, candidates: list[tuple[EngineSignal, float]]) -> list[tuple[EngineSignal, float]]:
        floor = self.learning.adaptive_confidence_floor()
        filtered = [(s, q) for s, q in candidates if s.confidence >= floor and s.expected_rr >= MIN_RR_BASE]
        filtered.sort(key=lambda x: x[1], reverse=True)

        chosen: list[tuple[EngineSignal, float]] = []
        used_symbol_dir = set()
        used_groups = set()
        for sig, q in filtered:
            key = (sig.symbol, sig.direction)
            group = correlation_group(sig.symbol)
            group_dir = (group, sig.direction)
            if key in used_symbol_dir:
                continue
            if group_dir in used_groups and sig.symbol not in MAJORS:
                continue
            chosen.append((sig, q))
            used_symbol_dir.add(key)
            used_groups.add(group_dir)
            if len(chosen) >= MAX_SIGNALS_PER_RUN:
                break
        return chosen


def mtf_confluence_score(structure: dict[str, StructureMap], direction: str) -> float:
    aligned = 0
    total = 0
    for tf in (TF_MACRO, TF_HTF, TF_MID):
        s = structure.get(tf)
        if not s:
            continue
        total += 1
        want = "bullish" if direction == "long" else "bearish"
        if s.bias == want:
            aligned += 1
    if total == 0:
        return 0.0
    return 6.0 * (aligned / total)


# ══════════════════════════════════════════════════════════════════════════
#  STATE / TRADE LIFECYCLE MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════

def load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text())
        except (json.JSONDecodeError, OSError) as e:
            log.warning(f"Failed reading state file, starting fresh: {e}")
    return {"open_signals": [], "daily": {}, "last_summary_date": None}


def save_state(state: dict):
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, default=str))
    tmp.replace(STATE_PATH)


def update_open_signals(state: dict, client: HyperliquidClient, learning: LearningStore) -> list[dict]:
    """Check every open signal against fresh 15m candles and evolve its status."""
    updates = []
    still_open = []
    for rec in state["open_signals"]:
        symbol = rec["symbol"]
        candles = client.candles(symbol, TF_LTF, 6)
        if not candles:
            still_open.append(rec)
            continue
        hi = max(c["h"] for c in candles)
        lo = min(c["l"] for c in candles)
        sig = Signal(**rec)
        closed = False
        if sig.direction == "long":
            if lo <= sig.stop:
                closed, outcome, rr = True, "sl", -1.0
            elif not sig.tp1_hit and hi >= sig.tp1:
                sig.tp1_hit = True
                sig.status = "tp1"
                sig.stop = sig.entry
                updates.append({"signal": asdict(sig), "event": "tp1"})
            if sig.tp1_hit and hi >= sig.tp2:
                closed, outcome, rr = True, "tp2", sig.expected_rr
        else:
            if hi >= sig.stop:
                closed, outcome, rr = True, "sl", -1.0
            elif not sig.tp1_hit and lo <= sig.tp1:
                sig.tp1_hit = True
                sig.status = "tp1"
                sig.stop = sig.entry
                updates.append({"signal": asdict(sig), "event": "tp1"})
            if sig.tp1_hit and lo <= sig.tp2:
                closed, outcome, rr = True, "tp2", sig.expected_rr

        if closed:
            sig.status = outcome
            learning.record_close(sig, outcome, rr)
            updates.append({"signal": asdict(sig), "event": outcome})
        else:
            still_open.append(asdict(sig))
    state["open_signals"] = still_open
    return updates


# ══════════════════════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════════════════════

def _fmt(v: float) -> str:
    if v >= 100:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:.4f}"
    return f"{v:.6f}"


def build_signal_message(sig: Signal) -> str:
    arrow = "LONG" if sig.direction == "long" else "SHORT"
    lines = [
        f"*{ENGINE_NAME}* v{__version__} — New Signal",
        f"*{sig.symbol}* — *{arrow}* ({sig.engine})",
        "",
        f"Entry: `{_fmt(sig.entry)}`",
        f"Stop: `{_fmt(sig.stop)}`",
        f"TP1: `{_fmt(sig.tp1)}`",
        f"TP2: `{_fmt(sig.tp2)}`",
        "",
        f"Confidence: {sig.confidence:.0f}%  |  Expected RR: {sig.expected_rr:.2f}",
        f"Regime: {sig.regime}",
        f"Confluences: {', '.join(sig.confluences)}",
        f"ID: `{sig.id}`",
    ]
    return "\n".join(lines)


def build_update_message(sig_dict: dict, event: str) -> str:
    label = {
        "tp1": "TP1 hit — stop moved to break-even",
        "tp2": "TP2 hit — trade closed",
        "sl": "Stop loss hit — trade closed",
        "be": "Closed at break-even",
        "cancelled": "Signal cancelled",
    }.get(event, event.upper())
    return f"*{ENGINE_NAME}* update — *{sig_dict['symbol']}* ({sig_dict['engine']})\n{label}\nID: `{sig_dict['id']}`"


def send_telegram(text: str) -> Optional[int]:
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    for attempt in range(3):
        try:
            r = requests.post(url, json=payload, timeout=10)
            if r.status_code == 200:
                return r.json().get("result", {}).get("message_id")
            time.sleep(1 + attempt)
        except requests.RequestException as e:
            log.warning(f"Telegram send failed (attempt {attempt}): {e}")
            time.sleep(1 + attempt)
    return None


def build_daily_summary(state: dict, learning: LearningStore) -> str:
    g = learning.state["global_stats"]
    total = g["wins"] + g["losses"]
    wr = (g["wins"] / total * 100) if total else 0.0
    pf_num = sum(max(0, learning.state["engine_stats"][e]["sum_rr"]) for e in learning.state["engine_stats"])
    pf_den = max(total - g["wins"], 1)
    avg_rr = (g["sum_rr"] / total) if total else 0.0

    best, worst = None, None
    for e, s in learning.state["engine_stats"].items():
        n = s["wins"] + s["losses"]
        if n == 0:
            continue
        e_wr = s["wins"] / n
        if best is None or e_wr > best[1]:
            best = (e, e_wr)
        if worst is None or e_wr < worst[1]:
            worst = (e, e_wr)

    lines = [
        f"*{ENGINE_NAME}* v{__version__} — Daily Summary",
        f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
        "",
        f"Total signals tracked: {total}",
        f"Wins: {g['wins']}  Losses: {g['losses']}",
        f"Win rate: {wr:.1f}%",
        f"Avg RR: {avg_rr:.2f}",
        "",
        "Performance by engine:",
    ]
    for e, s in learning.state["engine_stats"].items():
        n = s["wins"] + s["losses"]
        if n == 0:
            continue
        lines.append(f"  {e}: {s['wins']}/{n} ({s['wins']/n*100:.0f}%)")
    if best:
        lines.append(f"\nBest setup: {best[0]} ({best[1]*100:.0f}% WR)")
    if worst:
        lines.append(f"Worst setup: {worst[0]} ({worst[1]*100:.0f}% WR)")
    lines.append(f"\nOpen signals: {len(state['open_signals'])}")
    return "\n".join(lines)


def maybe_send_daily_summary(state: dict, learning: LearningStore):
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    if now.hour >= DAILY_SUMMARY_HOUR_UTC and state.get("last_summary_date") != today:
        send_telegram(build_daily_summary(state, learning))
        state["last_summary_date"] = today


# ══════════════════════════════════════════════════════════════════════════
#  SCAN PIPELINE
# ══════════════════════════════════════════════════════════════════════════

def scan_symbol(client: HyperliquidClient, symbol: str, regime_override: Optional[str] = None):
    frames = client.load_symbol(symbol)
    if TF_MID not in frames or TF_LTF not in frames:
        return []
    structure = {tf: detect_structure(f.candles, f.atr) for tf, f in frames.items()}
    regime = regime_override or classify_regime(frames[TF_MID])

    results = []
    for engine in ALL_ENGINES:
        try:
            sig = engine.generate(symbol, frames, structure, regime)
        except Exception as e:
            log.warning(f"{engine.name} failed on {symbol}: {e}")
            continue
        if sig:
            results.append(sig)
    return results


def already_open(state: dict, symbol: str) -> int:
    return sum(1 for r in state["open_signals"] if r["symbol"] == symbol)


def run_once():
    client = HyperliquidClient()
    state = load_state()
    learning = LearningStore(state)

    updates = update_open_signals(state, client, learning)
    for u in updates:
        send_telegram(build_update_message(u["signal"], u["event"]))

    if len(state["open_signals"]) >= MAX_OPEN_TOTAL:
        log.info("Max open signals reached; skipping new scan this run.")
        save_state(state)
        maybe_send_daily_summary(state, learning)
        return

    all_candidates: list[tuple[EngineSignal, float]] = []
    decision = DecisionEngine(learning)

    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as pool:
        futures = {pool.submit(scan_symbol, client, sym): sym for sym in WATCHLIST}
        for fut in as_completed(futures):
            symbol = futures[fut]
            try:
                engine_signals = fut.result()
            except Exception as e:
                log.warning(f"Scan failed for {symbol}: {e}")
                continue
            if already_open(state, symbol) >= MAX_OPEN_PER_SYMBOL:
                continue
            frames = client.load_symbol(symbol)
            structure = {tf: detect_structure(f.candles, f.atr) for tf, f in frames.items()} if frames else {}
            for sig in engine_signals:
                mtf = mtf_confluence_score(structure, sig.direction) if structure else 0.0
                quality = decision.score(sig, mtf)
                all_candidates.append((sig, quality))

    ranked = decision.rank(all_candidates)
    now_iso = datetime.now(timezone.utc).isoformat()

    for sig, quality in ranked:
        sig_id = f"{sig.symbol}-{sig.engine}-{int(time.time())}-{random.randint(100,999)}"
        signal = Signal(
            id=sig_id, symbol=sig.symbol, engine=sig.engine, direction=sig.direction,
            entry=sig.entry, stop=sig.stop, tp1=sig.tp1, tp2=sig.tp2,
            confidence=sig.confidence, expected_rr=sig.expected_rr,
            confluences=sig.confluences, regime=sig.regime, quality_score=quality,
            created_at=now_iso,
        )
        message_id = send_telegram(build_signal_message(signal))
        signal.message_id = message_id
        state["open_signals"].append(asdict(signal))
        log.info(f"Published {signal.symbol} {signal.direction} via {signal.engine} (score={quality:.1f})")

    if not ranked:
        log.info("No setups cleared the adaptive quality bar this run.")

    maybe_send_daily_summary(state, learning)
    save_state(state)


def main():
    try:
        run_once()
    except Exception:
        log.exception(f"{ENGINE_NAME} run failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
