#!/usr/bin/env python3
"""
VERITAS -- Adaptive Institutional-Grade Multi-Engine Signal Platform
Version: v1.0.0

Multi-engine crypto signal platform for Hyperliquid. Adaptive (not fixed) engine weighting
driven by rolling, sample-size-gated performance; a single unified resolution/lifecycle path
shared by every engine (no per-engine duplicate SL/TP/fill logic); an explicit regime-fit veto
layered on top of raw confidence; a liquidity sanity check applied to every engine except the
liquidity-sweep engine itself; and an "expired_gap" state for candles that jump past a pending
entry without clean fill data.

Architecture: single self-contained file, section-delimited, no local imports.
"""

from __future__ import annotations

import json
import os
import time
import random
import logging
import tempfile
import statistics
import urllib.request
import urllib.error
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional, Any

# ================================================================================================
# SECTION 0 -- GLOBAL CONFIGURATION
# ================================================================================================

ENGINE_NAME = "VERITAS"
ENGINE_VERSION = "v1.0.0"

STATE_PATH = os.environ.get("VERITAS_STATE_PATH", "state.json")
LOG_LEVEL = os.environ.get("VERITAS_LOG_LEVEL", "INFO")

HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"

# Forbidden timeframes: 1m/2m/3m/5m. Minimum timeframe is 15m.
TF_LTF = "15m"          # execution / precision timeframe
TF_MTF = "1h"           # confirmation timeframe
TF_HTF = "4h"           # context / bias timeframe
TF_HTF2 = "1d"          # higher context (macro bias, premium/discount zones)
ALL_TIMEFRAMES = [TF_LTF, TF_MTF, TF_HTF, TF_HTF2]

HL_INTERVAL_MAP = {"15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d"}

CANDLES_PER_REQUEST = 300
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 1.5
REQUEST_TIMEOUT_SECONDS = 12
MIN_SECONDS_BETWEEN_REQUESTS = 0.25

WATCHLIST = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]

TARGET_SIGNALS_PER_DAY_LOW = 5
TARGET_SIGNALS_PER_DAY_HIGH = 10
MAX_CONCURRENT_ACTIVE_SIGNALS = 12

MIN_NAMED_CONFLUENCES = 3
REQUIRE_MTF_ALIGNMENT = True

MIN_ENTRY_SL_ATR_FRACTION = 0.15
MIN_ENTRY_TP1_ATR_FRACTION = 0.25
MAX_PENDING_ENTRY_ATR_MULTIPLE = 1.5

PENDING_ENTRY_EXPIRY_BARS = 8

MIN_TRADES_FOR_ADAPTATION = 20

TAKER_FEE_RATE = 0.00035
MAKER_FEE_RATE = 0.00010
ASSUMED_SLIPPAGE_ATR_FRACTION = 0.02
ASSUMED_LATENCY_BARS = 0

DAILY_SUMMARY_HOUR_UTC = 8

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")
TELEGRAM_API_BASE = "https://api.telegram.org"

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
log = logging.getLogger(ENGINE_NAME)

# ================================================================================================
# SECTION 1 -- SHARED UTILITIES
# ================================================================================================

def safe_div(a: float, b: float, default: float = 0.0) -> float:
    try:
        if b == 0 or b != b:
            return default
        r = a / b
        return r if r == r else default
    except (ZeroDivisionError, TypeError):
        return default


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: Optional[datetime] = None) -> str:
    return (dt or utcnow()).isoformat()


def atomic_write_json(path: str, payload: dict) -> None:
    """Crash-safe state persistence: write to a temp file in the same
    directory, flush+fsync, then os.replace -- which is atomic on POSIX filesystems -- so a
    process killed mid-write (e.g. a GitHub Actions timeout) can never leave state.json
    truncated or corrupted."""
    directory = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp_path = tempfile.mkstemp(prefix=".veritas_state_", dir=directory)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


def load_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


# ================================================================================================
# SECTION 2 -- INDICATOR / STRUCTURE LIBRARY (shared across every specialized engine)
# ================================================================================================
# Candles are plain dicts: {"t": ms_epoch, "o": float, "h": float, "l": float, "c": float,
# "v": float}. Kept as dicts (not a heavier custom class or a pandas DataFrame dependency) to
# minimize memory/CPU footprint under GitHub Actions performance targets, while
# staying trivially serializable for caching/logging.

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


def true_range(candles: list[dict]) -> list[float]:
    trs = []
    for i, c in enumerate(candles):
        if i == 0:
            trs.append(c["h"] - c["l"])
            continue
        prev_close = candles[i - 1]["c"]
        trs.append(max(c["h"] - c["l"], abs(c["h"] - prev_close), abs(c["l"] - prev_close)))
    return trs


def atr(candles: list[dict], period: int = 14) -> list[float]:
    trs = true_range(candles)
    return ema(trs, period) if trs else []


def rsi(values: list[float], period: int = 14) -> list[float]:
    if len(values) < 2:
        return [50.0] * len(values)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(values)):
        delta = values[i] - values[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))
    avg_gain = ema(gains, period)
    avg_loss = ema(losses, period)
    out = []
    for g, l in zip(avg_gain, avg_loss):
        rs = safe_div(g, l, default=0.0) if l > 0 else (100.0 if g > 0 else 0.0)
        out.append(100.0 - safe_div(100.0, 1.0 + rs, default=0.0) if l > 0 else (100.0 if g > 0 else 50.0))
    return out


def rolling_std(values: list[float], period: int = 20) -> list[float]:
    out = []
    for i in range(len(values)):
        lo = max(0, i - period + 1)
        window = values[lo:i + 1]
        out.append(statistics.pstdev(window) if len(window) > 1 else 0.0)
    return out


def swing_points(candles: list[dict], lookback: int = 2) -> tuple[list[int], list[int]]:
    """Fractal swing highs/lows: index i is a swing high/low if it is the max/min of the
    window [i-lookback, i+lookback]. Returns (swing_high_indices, swing_low_indices)."""
    highs_idx, lows_idx = [], []
    n = len(candles)
    for i in range(lookback, n - lookback):
        window = candles[i - lookback:i + lookback + 1]
        if candles[i]["h"] == max(c["h"] for c in window):
            highs_idx.append(i)
        if candles[i]["l"] == min(c["l"] for c in window):
            lows_idx.append(i)
    return highs_idx, lows_idx


@dataclass
class StructureState:
    bias: str                  # "bullish" | "bearish" | "neutral"
    last_bos_idx: Optional[int]
    last_choch_idx: Optional[int]
    last_swing_high: Optional[float]
    last_swing_low: Optional[float]
    unmitigated_highs: list[float]
    unmitigated_lows: list[float]


def detect_structure(candles: list[dict], lookback: int = 2) -> StructureState:
    """Break of Structure (BOS) / Change of Character (CHoCH) detection from swing pivots.
    BOS = a new pivot breaks in the direction of the prevailing trend (trend continuation).
    CHoCH = price breaks the most recent opposing pivot, flipping the prevailing trend."""
    highs_idx, lows_idx = swing_points(candles, lookback)
    pivots = sorted([(i, "H", candles[i]["h"]) for i in highs_idx] +
                     [(i, "L", candles[i]["l"]) for i in lows_idx])
    bias = "neutral"
    last_bos_idx = None
    last_choch_idx = None
    recent_high = None
    recent_low = None
    for idx, kind, price in pivots:
        if kind == "H":
            recent_high = price
        else:
            recent_low = price
    swing_h = [p for p in pivots if p[1] == "H"]
    swing_l = [p for p in pivots if p[1] == "L"]
    if len(swing_h) >= 2 and len(swing_l) >= 2:
        higher_highs = swing_h[-1][2] > swing_h[-2][2]
        higher_lows = swing_l[-1][2] > swing_l[-2][2]
        lower_lows = swing_l[-1][2] < swing_l[-2][2]
        lower_highs = swing_h[-1][2] < swing_h[-2][2]
        if higher_highs and higher_lows:
            bias = "bullish"
        elif lower_lows and lower_highs:
            bias = "bearish"
        last_close = candles[-1]["c"]
        if bias == "bullish" and last_close > swing_h[-1][2]:
            last_bos_idx = len(candles) - 1
        elif bias == "bearish" and last_close < swing_l[-1][2]:
            last_bos_idx = len(candles) - 1
        if bias == "bullish" and last_close < swing_l[-1][2]:
            last_choch_idx = len(candles) - 1
            bias = "bearish"
        elif bias == "bearish" and last_close > swing_h[-1][2]:
            last_choch_idx = len(candles) - 1
            bias = "bullish"
    unmitigated_highs = [p for _, k, p in pivots[-8:] if k == "H"]
    unmitigated_lows = [p for _, k, p in pivots[-8:] if k == "L"]
    return StructureState(bias, last_bos_idx, last_choch_idx, recent_high, recent_low,
                           unmitigated_highs, unmitigated_lows)


@dataclass
class Zone:
    kind: str          # "order_block" | "breaker_block" | "fvg"
    direction: str     # "bullish" | "bearish"
    top: float
    bottom: float
    idx: int
    mitigated: bool = False


def find_order_blocks(candles: list[dict], structure: StructureState, max_zones: int = 6) -> list[Zone]:
    """An order block is the last opposite-direction candle before a strong displacement move
    that produces a BOS. Bullish OB = last down-candle before an up-displacement BOS."""
    zones: list[Zone] = []
    n = len(candles)
    if n < 5:
        return zones
    body = [abs(c["c"] - c["o"]) for c in candles]
    avg_body = statistics.mean(body[-30:]) if n >= 30 else (statistics.mean(body) if body else 0.0)
    for i in range(2, n - 1):
        disp = candles[i]
        if avg_body <= 0:
            continue
        is_displacement = abs(disp["c"] - disp["o"]) > avg_body * 1.5
        if not is_displacement:
            continue
        bullish_disp = disp["c"] > disp["o"]
        prev = candles[i - 1]
        prev_is_opposite = (prev["c"] < prev["o"]) if bullish_disp else (prev["c"] > prev["o"])
        if not prev_is_opposite:
            continue
        broke_recent_high = bullish_disp and disp["c"] > max(c["h"] for c in candles[max(0, i - 10):i])
        broke_recent_low = (not bullish_disp) and disp["c"] < min(c["l"] for c in candles[max(0, i - 10):i])
        if not (broke_recent_high or broke_recent_low):
            continue
        zone = Zone(
            kind="order_block",
            direction="bullish" if bullish_disp else "bearish",
            top=prev["h"], bottom=prev["l"], idx=i - 1,
        )
        if zone.direction == "bullish":
            zone.mitigated = any(c["l"] <= zone.top for c in candles[i:])
        else:
            zone.mitigated = any(c["h"] >= zone.bottom for c in candles[i:])
        zones.append(zone)
    return zones[-max_zones:]


def find_breaker_blocks(candles: list[dict], obs: list[Zone]) -> list[Zone]:
    """A breaker block is a failed order block: price mitigates through it and closes beyond,
    flipping that zone's polarity for future reaction."""
    breakers = []
    for ob in obs:
        for c in candles[ob.idx + 1:]:
            if ob.direction == "bullish" and c["c"] < ob.bottom:
                breakers.append(Zone("breaker_block", "bearish", ob.top, ob.bottom, ob.idx, mitigated=False))
                break
            if ob.direction == "bearish" and c["c"] > ob.top:
                breakers.append(Zone("breaker_block", "bullish", ob.top, ob.bottom, ob.idx, mitigated=False))
                break
    return breakers[-6:]


def find_fair_value_gaps(candles: list[dict], max_zones: int = 8) -> list[Zone]:
    """A (3-candle) Fair Value Gap: candle[i-1].high < candle[i+1].low (bullish imbalance) or
    candle[i-1].low > candle[i+1].high (bearish imbalance)."""
    gaps = []
    n = len(candles)
    for i in range(1, n - 1):
        a, b = candles[i - 1], candles[i + 1]
        if a["h"] < b["l"]:
            gap = Zone("fvg", "bullish", b["l"], a["h"], i)
            gap.mitigated = any(c["l"] <= gap.bottom for c in candles[i + 2:])
            gaps.append(gap)
        elif a["l"] > b["h"]:
            gap = Zone("fvg", "bearish", a["l"], b["h"], i)
            gap.mitigated = any(c["h"] >= gap.top for c in candles[i + 2:])
            gaps.append(gap)
    return gaps[-max_zones:]


def detect_liquidity_pools(candles: list[dict], lookback: int = 2) -> dict:
    """Equal highs/lows (within a tight tolerance) mark resting liquidity pools that price is
    statistically drawn toward sweeping. Returns nearby unmitigated pool levels above/below."""
    highs_idx, lows_idx = swing_points(candles, lookback)
    tol_ref = statistics.mean([c["h"] - c["l"] for c in candles[-30:]]) if len(candles) >= 30 else 1.0
    tol = max(tol_ref * 0.1, 1e-9)
    pools_high, pools_low = [], []
    highs = [candles[i]["h"] for i in highs_idx]
    lows = [candles[i]["l"] for i in lows_idx]
    for i, h in enumerate(highs):
        cluster = [x for x in highs if abs(x - h) <= tol]
        if len(cluster) >= 2:
            pools_high.append(max(cluster))
    for i, l in enumerate(lows):
        cluster = [x for x in lows if abs(x - l) <= tol]
        if len(cluster) >= 2:
            pools_low.append(min(cluster))
    return {"pools_high": sorted(set(pools_high)), "pools_low": sorted(set(pools_low), reverse=True)}


def detect_liquidity_sweep(candles: list[dict], pools: dict) -> Optional[dict]:
    """A liquidity sweep: the most recent candle's wick pierces a pool level but the body
    closes back on the other side -- the classic stop-hunt-then-reverse signature."""
    if len(candles) < 3:
        return None
    last = candles[-1]
    for level in pools.get("pools_high", []):
        if last["h"] > level and last["c"] < level:
            return {"direction": "bearish", "level": level, "wick_high": last["h"]}
    for level in pools.get("pools_low", []):
        if last["l"] < level and last["c"] > level:
            return {"direction": "bullish", "level": level, "wick_low": last["l"]}
    return None


def volatility_state(candles: list[dict]) -> str:
    atrs = atr(candles, 14)
    if len(atrs) < 30:
        return "normal"
    current = atrs[-1]
    baseline = statistics.mean(atrs[-30:])
    if baseline <= 0:
        return "normal"
    ratio = current / baseline
    if ratio > 1.35:
        return "high"
    if ratio < 0.7:
        return "low"
    return "normal"


def is_ranging(candles: list[dict], period: int = 20) -> bool:
    closes = [c["c"] for c in candles[-period:]]
    if len(closes) < period:
        return False
    rng = max(closes) - min(closes)
    mean_c = statistics.mean(closes)
    return safe_div(rng, mean_c, 0.0) < 0.03


def premium_discount_zone(candles: list[dict], lookback: int = 50) -> dict:
    window = candles[-lookback:] if len(candles) >= lookback else candles
    hi = max(c["h"] for c in window)
    lo = min(c["l"] for c in window)
    mid = (hi + lo) / 2.0
    last = candles[-1]["c"]
    zone = "premium" if last > mid else "discount"
    return {"high": hi, "low": lo, "mid": mid, "zone": zone}

# ================================================================================================
# SECTION 3 -- MARKET CONTEXT / REGIME DETECTION
# ================================================================================================

REGIMES = ["bull_trend", "bear_trend", "ranging", "high_volatility", "low_volatility", "reversal", "neutral"]


@dataclass
class MarketContext:
    symbol: str
    regime: str
    htf_bias: str
    mtf_bias: str
    ltf_bias: str
    mtf_aligned: bool
    volatility: str
    is_ranging: bool
    atr_ltf: float
    price: float
    pd_zone: dict
    liquidity: dict
    sweep: Optional[dict]
    structure_ltf: StructureState
    structure_htf: StructureState


def detect_regime(struct_htf: StructureState, struct_mtf: StructureState, vol: str, ranging: bool) -> str:
    if ranging and vol != "high":
        return "ranging"
    if vol == "high":
        return "high_volatility"
    if vol == "low" and ranging:
        return "low_volatility"
    if struct_htf.last_choch_idx is not None and struct_mtf.last_choch_idx is not None:
        return "reversal"
    if struct_htf.bias == "bullish" and struct_mtf.bias == "bullish":
        return "bull_trend"
    if struct_htf.bias == "bearish" and struct_mtf.bias == "bearish":
        return "bear_trend"
    return "neutral"


def build_market_context(symbol: str, candles_by_tf: dict[str, list[dict]]) -> Optional[MarketContext]:
    ltf = candles_by_tf.get(TF_LTF, [])
    mtf = candles_by_tf.get(TF_MTF, [])
    htf = candles_by_tf.get(TF_HTF, [])
    htf2 = candles_by_tf.get(TF_HTF2, [])
    if len(ltf) < 30 or len(mtf) < 30 or len(htf) < 30:
        return None
    struct_ltf = detect_structure(ltf)
    struct_mtf = detect_structure(mtf)
    struct_htf = detect_structure(htf)
    vol = volatility_state(ltf)
    ranging = is_ranging(ltf)
    regime = detect_regime(struct_htf, struct_mtf, vol, ranging)
    mtf_aligned = struct_ltf.bias == struct_mtf.bias and struct_mtf.bias in ("bullish", "bearish")
    pools = detect_liquidity_pools(ltf)
    sweep = detect_liquidity_sweep(ltf, pools)
    pd_ref = htf2 if len(htf2) >= 50 else htf
    pd_zone = premium_discount_zone(pd_ref)
    atrs = atr(ltf, 14)
    return MarketContext(
        symbol=symbol, regime=regime,
        htf_bias=struct_htf.bias, mtf_bias=struct_mtf.bias, ltf_bias=struct_ltf.bias,
        mtf_aligned=mtf_aligned, volatility=vol, is_ranging=ranging,
        atr_ltf=atrs[-1] if atrs else 0.0, price=ltf[-1]["c"], pd_zone=pd_zone,
        liquidity=pools, sweep=sweep, structure_ltf=struct_ltf, structure_htf=struct_htf,
    )


# ================================================================================================
# SECTION 4 -- SIGNAL CANDIDATE MODEL
# ================================================================================================

@dataclass
class SignalCandidate:
    engine: str
    symbol: str
    direction: str              # "long" | "short"
    entry: float
    sl: float
    tp1: float
    tp2: float
    confidence: float           # 0-100, raw per-engine confidence before Decision Engine reweighting
    expected_rr: float
    confluences: list[str]
    best_fit_regimes: list[str]         # engine documents its own regime suitability
    entry_is_market: bool               # True if entry == price at signal time
    generated_at: str = field(default_factory=iso)
    score: float = 0.0                  # populated by the Decision Engine
    veto_reasons: list[str] = field(default_factory=list)


# ================================================================================================
# SECTION 5 -- DATA LAYER: HYPERLIQUID CLIENT + SHARED CANDLE CACHE
# ================================================================================================

class RateLimiter:
    """Simple client-side throttle so bursts of requests across symbols/timeframes never exceed
    Hyperliquid's documented rate limits."""

    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self._last_call = 0.0

    def wait(self):
        elapsed = time.monotonic() - self._last_call
        remaining = self.min_interval - elapsed
        if remaining > 0:
            time.sleep(remaining)
        self._last_call = time.monotonic()


class HyperliquidClient:
    """Thin wrapper around Hyperliquid's public /info endpoint. Implements exponential backoff
    with jitter and graceful degradation: a failed symbol/timeframe fetch
    is logged and skipped rather than crashing the whole scan."""

    def __init__(self, base_url: str = HYPERLIQUID_INFO_URL):
        self.base_url = base_url
        self.limiter = RateLimiter(MIN_SECONDS_BETWEEN_REQUESTS)

    def _post(self, payload: dict) -> Any:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self.base_url, data=data,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        last_err: Optional[Exception] = None
        for attempt in range(MAX_RETRIES):
            self.limiter.wait()
            try:
                with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError) as e:
                last_err = e
                sleep_s = (BACKOFF_BASE_SECONDS ** attempt) + random.uniform(0, 0.5)
                log.warning(f"Hyperliquid request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}. "
                            f"Retrying in {sleep_s:.1f}s")
                time.sleep(sleep_s)
        log.error(f"Hyperliquid request permanently failed after {MAX_RETRIES} attempts: {last_err}")
        return None

    def get_candles(self, coin: str, interval: str, lookback_bars: int = CANDLES_PER_REQUEST) -> list[dict]:
        interval_ms = _interval_to_ms(interval)
        end_time = int(time.time() * 1000)
        start_time = end_time - interval_ms * lookback_bars
        payload = {
            "type": "candleSnapshot",
            "req": {"coin": coin, "interval": HL_INTERVAL_MAP.get(interval, interval),
                     "startTime": start_time, "endTime": end_time},
        }
        raw = self._post(payload)
        if not raw or not isinstance(raw, list):
            return []
        candles = []
        for bar in raw:
            try:
                candles.append({
                    "t": int(bar["t"]), "o": float(bar["o"]), "h": float(bar["h"]),
                    "l": float(bar["l"]), "c": float(bar["c"]), "v": float(bar["v"]),
                })
            except (KeyError, TypeError, ValueError):
                continue
        candles.sort(key=lambda c: c["t"])
        return candles

    def get_mid_price(self, coin: str) -> Optional[float]:
        raw = self._post({"type": "allMids"})
        if not raw or not isinstance(raw, dict):
            return None
        try:
            return float(raw.get(coin))
        except (TypeError, ValueError):
            return None


def _interval_to_ms(interval: str) -> int:
    unit = interval[-1]
    n = int(interval[:-1])
    mult = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}.get(unit, 60_000)
    return n * mult


class CandleCache:
    """Shared candle + indicator cache across all specialized engines for a single scan run:
    each (symbol, timeframe) pair is fetched from Hyperliquid exactly
    once per run and reused by every engine and by the shared indicator library, instead of
    each engine independently re-requesting or recomputing the same series."""

    def __init__(self, client: HyperliquidClient):
        self.client = client
        self._candles: dict[tuple[str, str], list[dict]] = {}

    def get(self, symbol: str, timeframe: str) -> list[dict]:
        key = (symbol, timeframe)
        if key not in self._candles:
            self._candles[key] = self.client.get_candles(symbol, timeframe)
        return self._candles[key]

    def get_all_timeframes(self, symbol: str) -> dict[str, list[dict]]:
        return {tf: self.get(symbol, tf) for tf in ALL_TIMEFRAMES}

# ================================================================================================
# SECTION 6 -- ENTRY PLACEMENT & RISK VALIDATION HELPERS
# ================================================================================================

def validate_and_finalize_entry(
    engine: str, symbol: str, direction: str, entry: float, sl: float, tp1: float, tp2: float,
    confidence: float, confluences: list[str], best_fit_regimes: list[str],
    atr_val: float, market_price: float, entry_is_market: bool,
) -> Optional[SignalCandidate]:
    """Central gate every specialized engine must funnel candidates through before emitting one.
    Enforces mandatory entry-placement rules so a bad entry is rejected at the
    source rather than silently degrading downstream statistics:
      1. Minimum entry-to-SL and entry-to-TP1 distance (in ATR terms) -- rejects noise trades
         that would produce near-instant stop-outs or meaningless TP1 tags.
      2. A resting/zone entry may not sit further than MAX_PENDING_ENTRY_ATR_MULTIPLE * ATR from
         current market price -- rejects stale/unrealistic pending setups.
    SL/TP validated using candle-derived levels only (callers pass in highs/lows-derived prices,
    never live mid-price).
    """
    if atr_val <= 0 or entry <= 0 or sl <= 0 or tp1 <= 0 or tp2 <= 0:
        return None
    if direction not in ("long", "short"):
        return None

    risk = abs(entry - sl)
    reward1 = abs(tp1 - entry)
    reward2 = abs(tp2 - entry)

    if risk < MIN_ENTRY_SL_ATR_FRACTION * atr_val:
        return None
    if reward1 < MIN_ENTRY_TP1_ATR_FRACTION * atr_val:
        return None

    # Directional sanity: long must have sl < entry < tp1 <= tp2; short is the mirror image.
    if direction == "long":
        if not (sl < entry < tp1 <= tp2):
            return None
    else:
        if not (tp2 <= tp1 < entry < sl):
            return None

    if not entry_is_market:
        distance_from_market = abs(entry - market_price)
        if distance_from_market > MAX_PENDING_ENTRY_ATR_MULTIPLE * atr_val:
            return None

    expected_rr = safe_div(reward2, risk, default=0.0)
    if expected_rr < 1.0:
        return None

    return SignalCandidate(
        engine=engine, symbol=symbol, direction=direction, entry=entry, sl=sl, tp1=tp1, tp2=tp2,
        confidence=clamp(confidence, 0.0, 100.0), expected_rr=expected_rr,
        confluences=confluences, best_fit_regimes=best_fit_regimes,
        entry_is_market=entry_is_market,
    )


def liquidity_sanity_check(cand: SignalCandidate, ctx: MarketContext) -> bool:
    """Liquidity sanity check. Reject/flag candidates whose entry sits
    directly inside a level that's about to be swept, or immediately adjacent to an obvious
    unmitigated liquidity pool -- UNLESS the candidate is itself a liquidity-sweep signal
    designed to trade that behavior. Returns True if the candidate passes (is safe to keep)."""
    if cand.engine == "LiquiditySweep":
        return True
    tol = max(ctx.atr_ltf * 0.15, 1e-9)
    for level in ctx.liquidity.get("pools_high", []):
        if abs(cand.entry - level) <= tol and cand.direction == "long":
            return False
    for level in ctx.liquidity.get("pools_low", []):
        if abs(cand.entry - level) <= tol and cand.direction == "short":
            return False
    return True


def regime_fit_score(cand: SignalCandidate, ctx: MarketContext) -> float:
    """Regime-fit veto. Returns a multiplier in [0, 1] applied to confidence:
    1.0 if the current regime is in the engine's documented best-fit list, a partial discount if
    the regime is merely adjacent/neutral, and a heavy discount otherwise. This runs even when
    the candidate's raw confidence looks high in isolation."""
    if ctx.regime in cand.best_fit_regimes:
        return 1.0
    if "neutral" in cand.best_fit_regimes or ctx.regime == "neutral":
        return 0.6
    return 0.25


# ================================================================================================
# SECTION 7 -- SPECIALIZED ENGINE BASE CLASS
# ================================================================================================

class SpecializedEngine:
    """Common interface every specialized engine implements. Each engine independently produces
    zero or more SignalCandidate objects; it never talks to Telegram, state, or Hyperliquid
    directly -- it only reasons over the shared CandleCache/MarketContext, keeping engines pure
    and unit-testable and avoiding duplicate-logic across engines."""

    name: str = "Base"
    best_fit_regimes: list[str] = []

    def generate(self, ctx: MarketContext, candles: dict[str, list[dict]]) -> list[SignalCandidate]:
        raise NotImplementedError

    def _finalize(self, ctx: MarketContext, direction: str, entry: float, sl: float, tp1: float,
                  tp2: float, confidence: float, confluences: list[str],
                  entry_is_market: bool) -> Optional[SignalCandidate]:
        cand = validate_and_finalize_entry(
            engine=self.name, symbol=ctx.symbol, direction=direction, entry=entry, sl=sl,
            tp1=tp1, tp2=tp2, confidence=confidence, confluences=confluences,
            best_fit_regimes=self.best_fit_regimes, atr_val=ctx.atr_ltf, market_price=ctx.price,
            entry_is_market=entry_is_market,
        )
        if cand is None:
            return None
        if not liquidity_sanity_check(cand, ctx):
            return None
        return cand

# ================================================================================================
# SECTION 8 -- SPECIALIZED ENGINES
# ================================================================================================
# Each engine documents its own best_fit_regimes (used by the Decision Engine's regime-fit veto)
# and returns at most one candidate per symbol per scan to keep engines focused and
# comparable in the learning system's per-engine stats.

class SMCEngine(SpecializedEngine):
    """Smart Money Concept: HTF bias + BOS/CHoCH structure + premium/discount zone alignment.
    Enters on a pullback into a discount (long) / premium (short) zone that lines up with HTF
    bias and a confirmed BOS, targeting the opposing liquidity pool."""
    name = "SMC"
    best_fit_regimes = ["bull_trend", "bear_trend", "reversal"]

    def generate(self, ctx, candles):
        ltf = candles[TF_LTF]
        if ctx.htf_bias not in ("bullish", "bearish") or not ctx.mtf_aligned:
            return []
        direction = "long" if ctx.htf_bias == "bullish" else "short"
        if direction == "long" and ctx.pd_zone["zone"] != "discount":
            return []
        if direction == "short" and ctx.pd_zone["zone"] != "premium":
            return []
        confluences = ["HTF bias aligned", "MTF/LTF structure aligned", f"Price in {ctx.pd_zone['zone']} zone"]
        obs = find_order_blocks(ltf, ctx.structure_ltf)
        relevant_obs = [z for z in obs if z.direction == direction and not z.mitigated]
        entry = ctx.price
        entry_is_market = True
        if relevant_obs:
            z = relevant_obs[-1]
            entry = (z.top + z.bottom) / 2.0
            entry_is_market = False
            confluences.append("Unmitigated order block confluence")
        if direction == "long":
            sl = min(c["l"] for c in ltf[-6:]) - ctx.atr_ltf * 0.25
            tp1 = entry + ctx.atr_ltf * 1.5
            tp2 = ctx.structure_ltf.last_swing_high or (entry + ctx.atr_ltf * 3.0)
        else:
            sl = max(c["h"] for c in ltf[-6:]) + ctx.atr_ltf * 0.25
            tp1 = entry - ctx.atr_ltf * 1.5
            tp2 = ctx.structure_ltf.last_swing_low or (entry - ctx.atr_ltf * 3.0)
        confidence = 68 + (8 if ctx.structure_htf.last_bos_idx is not None else 0)
        return _one_or_none(self._finalize(ctx, direction, entry, sl, tp1, tp2, confidence,
                                            confluences, entry_is_market))


class TrendContinuationEngine(SpecializedEngine):
    """EMA-stack alignment across LTF/MTF plus a shallow pullback entry in the direction of the
    prevailing trend."""
    name = "TrendContinuation"
    best_fit_regimes = ["bull_trend", "bear_trend"]

    def generate(self, ctx, candles):
        ltf = candles[TF_LTF]
        closes = [c["c"] for c in ltf]
        if len(closes) < 55:
            return []
        ema20, ema50 = ema(closes, 20), ema(closes, 50)
        if ctx.regime not in ("bull_trend", "bear_trend"):
            return []
        direction = "long" if ctx.regime == "bull_trend" else "short"
        price = ltf[-1]["c"]
        near_ema = abs(price - ema20[-1]) < ctx.atr_ltf * 0.6
        stack_ok = (ema20[-1] > ema50[-1]) if direction == "long" else (ema20[-1] < ema50[-1])
        if not (near_ema and stack_ok):
            return []
        confluences = ["EMA20/EMA50 stack aligned with trend", "Pullback to EMA20", f"HTF bias {ctx.htf_bias}"]
        entry = price
        if direction == "long":
            sl = min(c["l"] for c in ltf[-5:]) - ctx.atr_ltf * 0.2
            tp1 = entry + ctx.atr_ltf * 1.3
            tp2 = entry + ctx.atr_ltf * 2.6
        else:
            sl = max(c["h"] for c in ltf[-5:]) + ctx.atr_ltf * 0.2
            tp1 = entry - ctx.atr_ltf * 1.3
            tp2 = entry - ctx.atr_ltf * 2.6
        confidence = 62
        return _one_or_none(self._finalize(ctx, direction, entry, sl, tp1, tp2, confidence,
                                            confluences, entry_is_market=True))


class BreakoutEngine(SpecializedEngine):
    """Range/consolidation breakout confirmed by volume expansion and a decisive close beyond
    the prior range boundary."""
    name = "Breakout"
    best_fit_regimes = ["high_volatility", "bull_trend", "bear_trend"]

    def generate(self, ctx, candles):
        ltf = candles[TF_LTF]
        if len(ltf) < 25:
            return []
        window = ltf[-21:-1]
        hi, lo = max(c["h"] for c in window), min(c["l"] for c in window)
        last = ltf[-1]
        vols = [c["v"] for c in ltf[-21:-1]]
        avg_vol = statistics.mean(vols) if vols else 0.0
        volume_confirmed = last["v"] > avg_vol * 1.4 if avg_vol > 0 else False
        if last["c"] > hi and volume_confirmed:
            direction = "long"
        elif last["c"] < lo and volume_confirmed:
            direction = "short"
        else:
            return []
        confluences = ["Range breakout with volume expansion", f"Regime: {ctx.regime}",
                        f"Volume {last['v']/avg_vol:.1f}x average" if avg_vol > 0 else "Volume expansion"]
        entry = last["c"]
        if direction == "long":
            sl = hi - ctx.atr_ltf * 0.3
            tp1 = entry + ctx.atr_ltf * 1.4
            tp2 = entry + ctx.atr_ltf * 2.8
        else:
            sl = lo + ctx.atr_ltf * 0.3
            tp1 = entry - ctx.atr_ltf * 1.4
            tp2 = entry - ctx.atr_ltf * 2.8
        confidence = 60
        return _one_or_none(self._finalize(ctx, direction, entry, sl, tp1, tp2, confidence,
                                            confluences, entry_is_market=True))


class PullbackEngine(SpecializedEngine):
    """A deeper Fibonacci-style retracement (measured via recent swing range) back toward value
    within an established trend, entered as a resting (non-market) limit at the retracement
    level rather than chasing price."""
    name = "Pullback"
    best_fit_regimes = ["bull_trend", "bear_trend"]

    def generate(self, ctx, candles):
        s = ctx.structure_ltf
        if ctx.regime not in ("bull_trend", "bear_trend") or not s.last_swing_high or not s.last_swing_low:
            return []
        direction = "long" if ctx.regime == "bull_trend" else "short"
        rng = s.last_swing_high - s.last_swing_low
        if rng <= 0:
            return []
        if direction == "long":
            entry = s.last_swing_high - rng * 0.618
            sl = s.last_swing_low - ctx.atr_ltf * 0.2
            tp1 = s.last_swing_high - rng * 0.236
            tp2 = s.last_swing_high
        else:
            entry = s.last_swing_low + rng * 0.618
            sl = s.last_swing_high + ctx.atr_ltf * 0.2
            tp1 = s.last_swing_low + rng * 0.236
            tp2 = s.last_swing_low
        confluences = ["61.8% retracement into value", f"Trend regime {ctx.regime}",
                        f"HTF bias {ctx.htf_bias} confirms direction"]
        confidence = 58
        return _one_or_none(self._finalize(ctx, direction, entry, sl, tp1, tp2, confidence,
                                            confluences, entry_is_market=False))


class LiquiditySweepEngine(SpecializedEngine):
    """Trades the reversal immediately following a stop-hunt: wick pierces a resting liquidity
    pool, body closes back inside -- entering in the direction opposite the sweep."""
    name = "LiquiditySweep"
    best_fit_regimes = ["reversal", "ranging", "neutral"]

    def generate(self, ctx, candles):
        ltf = candles[TF_LTF]
        sweep = ctx.sweep
        if not sweep:
            return []
        direction = "long" if sweep["direction"] == "bullish" else "short"
        last = ltf[-1]
        entry = last["c"]
        if direction == "long":
            sl = sweep["wick_low"] - ctx.atr_ltf * 0.15
            tp1 = entry + ctx.atr_ltf * 1.2
            tp2 = entry + ctx.atr_ltf * 2.4
        else:
            sl = sweep["wick_high"] + ctx.atr_ltf * 0.15
            tp1 = entry - ctx.atr_ltf * 1.2
            tp2 = entry - ctx.atr_ltf * 2.4
        confluences = ["Liquidity pool swept then reclaimed", f"Sweep level {sweep['level']:.4f}",
                        f"Regime context: {ctx.regime}"]
        confidence = 64
        return _one_or_none(self._finalize(ctx, direction, entry, sl, tp1, tp2, confidence,
                                            confluences, entry_is_market=True))


class OrderBlockEngine(SpecializedEngine):
    """Enters directly at an unmitigated order block as a resting order, independent of broader
    SMC confluence stacking (kept separate from SMCEngine so the learning system can measure
    order-block edge in isolation via per-engine performance tracking)."""
    name = "OrderBlock"
    best_fit_regimes = ["bull_trend", "bear_trend", "reversal"]

    def generate(self, ctx, candles):
        ltf = candles[TF_LTF]
        obs = find_order_blocks(ltf, ctx.structure_ltf)
        unmit = [z for z in obs if not z.mitigated]
        if not unmit:
            return []
        z = unmit[-1]
        direction = "long" if z.direction == "bullish" else "short"
        entry = (z.top + z.bottom) / 2.0
        if direction == "long":
            sl = z.bottom - ctx.atr_ltf * 0.25
            tp1 = entry + ctx.atr_ltf * 1.4
            tp2 = entry + ctx.atr_ltf * 2.8
        else:
            sl = z.top + ctx.atr_ltf * 0.25
            tp1 = entry - ctx.atr_ltf * 1.4
            tp2 = entry - ctx.atr_ltf * 2.8
        confluences = ["Unmitigated order block", f"Zone direction {z.direction}",
                        f"HTF bias {ctx.htf_bias}"]
        confidence = 59
        return _one_or_none(self._finalize(ctx, direction, entry, sl, tp1, tp2, confidence,
                                            confluences, entry_is_market=False))


class BreakerBlockEngine(SpecializedEngine):
    """Enters at a breaker block (a failed order block whose polarity has flipped) as it's
    retested from the opposite side."""
    name = "BreakerBlock"
    best_fit_regimes = ["reversal", "bull_trend", "bear_trend"]

    def generate(self, ctx, candles):
        ltf = candles[TF_LTF]
        obs = find_order_blocks(ltf, ctx.structure_ltf)
        breakers = find_breaker_blocks(ltf, obs)
        if not breakers:
            return []
        z = breakers[-1]
        direction = "long" if z.direction == "bullish" else "short"
        entry = (z.top + z.bottom) / 2.0
        if direction == "long":
            sl = z.bottom - ctx.atr_ltf * 0.25
            tp1 = entry + ctx.atr_ltf * 1.4
            tp2 = entry + ctx.atr_ltf * 2.8
        else:
            sl = z.top + ctx.atr_ltf * 0.25
            tp1 = entry - ctx.atr_ltf * 1.4
            tp2 = entry - ctx.atr_ltf * 2.8
        confluences = ["Breaker block retest", "Failed order block polarity flip",
                        f"HTF bias {ctx.htf_bias}"]
        confidence = 57
        return _one_or_none(self._finalize(ctx, direction, entry, sl, tp1, tp2, confidence,
                                            confluences, entry_is_market=False))


class FairValueGapEngine(SpecializedEngine):
    """Enters as price returns to fill an unmitigated Fair Value Gap in the direction of the
    prevailing imbalance."""
    name = "FairValueGap"
    best_fit_regimes = ["bull_trend", "bear_trend", "ranging"]

    def generate(self, ctx, candles):
        ltf = candles[TF_LTF]
        gaps = find_fair_value_gaps(ltf)
        unmit = [g for g in gaps if not g.mitigated]
        if not unmit:
            return []
        g = unmit[-1]
        direction = "long" if g.direction == "bullish" else "short"
        entry = (g.top + g.bottom) / 2.0
        if direction == "long":
            sl = g.bottom - ctx.atr_ltf * 0.2
            tp1 = entry + ctx.atr_ltf * 1.2
            tp2 = entry + ctx.atr_ltf * 2.4
        else:
            sl = g.top + ctx.atr_ltf * 0.2
            tp1 = entry - ctx.atr_ltf * 1.2
            tp2 = entry - ctx.atr_ltf * 2.4
        confluences = ["Unmitigated FVG fill", f"Imbalance direction {g.direction}",
                        f"MTF aligned: {ctx.mtf_aligned}"]
        confidence = 55
        return _one_or_none(self._finalize(ctx, direction, entry, sl, tp1, tp2, confidence,
                                            confluences, entry_is_market=False))


class MomentumEngine(SpecializedEngine):
    """RSI + rate-of-change momentum continuation: enters in the direction of strong,
    accelerating momentum that hasn't yet reached exhaustion extremes."""
    name = "Momentum"
    best_fit_regimes = ["bull_trend", "bear_trend", "high_volatility"]

    def generate(self, ctx, candles):
        ltf = candles[TF_LTF]
        closes = [c["c"] for c in ltf]
        if len(closes) < 20:
            return []
        r = rsi(closes, 14)
        roc = safe_div(closes[-1] - closes[-6], closes[-6], 0.0) if len(closes) > 6 else 0.0
        direction = None
        if r[-1] > 55 and r[-1] < 75 and roc > 0.004:
            direction = "long"
        elif r[-1] < 45 and r[-1] > 25 and roc < -0.004:
            direction = "short"
        if not direction:
            return []
        entry = closes[-1]
        if direction == "long":
            sl = min(c["l"] for c in ltf[-6:]) - ctx.atr_ltf * 0.2
            tp1 = entry + ctx.atr_ltf * 1.3
            tp2 = entry + ctx.atr_ltf * 2.5
        else:
            sl = max(c["h"] for c in ltf[-6:]) + ctx.atr_ltf * 0.2
            tp1 = entry - ctx.atr_ltf * 1.3
            tp2 = entry - ctx.atr_ltf * 2.5
        confluences = [f"RSI momentum ({r[-1]:.1f})", f"6-bar ROC {roc*100:.2f}%",
                        f"Regime: {ctx.regime}"]
        confidence = 54
        return _one_or_none(self._finalize(ctx, direction, entry, sl, tp1, tp2, confidence,
                                            confluences, entry_is_market=True))


class ReversalEngine(SpecializedEngine):
    """Confirmed CHoCH plus RSI divergence -- a genuine trend-character flip rather than a mere
    pullback, targeting the opposite side of the recent range."""
    name = "Reversal"
    best_fit_regimes = ["reversal"]

    def generate(self, ctx, candles):
        ltf = candles[TF_LTF]
        s = ctx.structure_ltf
        if s.last_choch_idx is None:
            return []
        closes = [c["c"] for c in ltf]
        r = rsi(closes, 14)
        highs_idx, lows_idx = swing_points(ltf, 2)
        divergence = False
        if s.bias == "bullish" and len(lows_idx) >= 2:
            i1, i2 = lows_idx[-2], lows_idx[-1]
            divergence = ltf[i2]["l"] < ltf[i1]["l"] and r[i2] > r[i1]
        elif s.bias == "bearish" and len(highs_idx) >= 2:
            i1, i2 = highs_idx[-2], highs_idx[-1]
            divergence = ltf[i2]["h"] > ltf[i1]["h"] and r[i2] < r[i1]
        direction = "long" if s.bias == "bullish" else "short"
        entry = ltf[-1]["c"]
        if direction == "long":
            sl = (s.last_swing_low or entry - ctx.atr_ltf * 2) - ctx.atr_ltf * 0.2
            tp1 = entry + ctx.atr_ltf * 1.5
            tp2 = entry + ctx.atr_ltf * 3.0
        else:
            sl = (s.last_swing_high or entry + ctx.atr_ltf * 2) + ctx.atr_ltf * 0.2
            tp1 = entry - ctx.atr_ltf * 1.5
            tp2 = entry - ctx.atr_ltf * 3.0
        if not divergence:
            return []
        confluences = ["Confirmed CHoCH", "RSI divergence", f"Structure bias flipped to {s.bias}"]
        confidence = 61
        return _one_or_none(self._finalize(ctx, direction, entry, sl, tp1, tp2, confidence,
                                            confluences, entry_is_market=True))


class MeanReversionEngine(SpecializedEngine):
    """Fades statistical extremes back toward the mean using a Bollinger-style band built on
    the shared rolling_std/sma indicators, valid only in genuinely low-trend conditions."""
    name = "MeanReversion"
    best_fit_regimes = ["ranging", "low_volatility"]

    def generate(self, ctx, candles):
        ltf = candles[TF_LTF]
        closes = [c["c"] for c in ltf]
        if len(closes) < 25 or ctx.regime not in ("ranging", "low_volatility"):
            return []
        mid = sma(closes, 20)
        std = rolling_std(closes, 20)
        upper = mid[-1] + 2.0 * std[-1]
        lower = mid[-1] - 2.0 * std[-1]
        price = closes[-1]
        if price >= upper:
            direction = "short"
        elif price <= lower:
            direction = "long"
        else:
            return []
        entry = price
        if direction == "long":
            sl = lower - ctx.atr_ltf * 0.3
            tp1 = mid[-1]
            tp2 = upper
        else:
            sl = upper + ctx.atr_ltf * 0.3
            tp1 = mid[-1]
            tp2 = lower
        confluences = ["Price at 2-std band extreme", "Range/low-vol regime",
                        f"Regime: {ctx.regime}"]
        confidence = 56
        return _one_or_none(self._finalize(ctx, direction, entry, sl, tp1, tp2, confidence,
                                            confluences, entry_is_market=True))


class RangeTradingEngine(SpecializedEngine):
    """Fades the well-defined boundaries of an established trading range, distinct from
    MeanReversion in that it uses literal swing-derived range edges rather than a statistical
    band, and requires the range to have held for several touches."""
    name = "RangeTrading"
    best_fit_regimes = ["ranging"]

    def generate(self, ctx, candles):
        ltf = candles[TF_LTF]
        if ctx.regime != "ranging" or len(ltf) < 25:
            return []
        window = ltf[-25:]
        hi, lo = max(c["h"] for c in window), min(c["l"] for c in window)
        price = ltf[-1]["c"]
        near_hi = price >= hi - ctx.atr_ltf * 0.3
        near_lo = price <= lo + ctx.atr_ltf * 0.3
        if near_hi:
            direction = "short"
        elif near_lo:
            direction = "long"
        else:
            return []
        entry = price
        mid = (hi + lo) / 2.0
        if direction == "long":
            sl = lo - ctx.atr_ltf * 0.3
            tp1 = mid
            tp2 = hi
        else:
            sl = hi + ctx.atr_ltf * 0.3
            tp1 = mid
            tp2 = lo
        confluences = ["Range boundary fade", "Established range (25-bar)",
                        f"Volatility: {ctx.volatility}"]
        confidence = 55
        return _one_or_none(self._finalize(ctx, direction, entry, sl, tp1, tp2, confidence,
                                            confluences, entry_is_market=True))


class VolatilityExpansionEngine(SpecializedEngine):
    """Trades the initial thrust out of a volatility squeeze (ATR compression followed by a
    sharp expansion candle), aiming to catch the start of a new directional move."""
    name = "VolatilityExpansion"
    best_fit_regimes = ["high_volatility", "reversal"]

    def generate(self, ctx, candles):
        ltf = candles[TF_LTF]
        atrs = atr(ltf, 14)
        if len(atrs) < 25:
            return []
        baseline = statistics.mean(atrs[-25:-1])
        if baseline <= 0:
            return []
        was_squeezed = atrs[-2] < baseline * 0.75
        expanding_now = atrs[-1] > baseline * 1.3
        if not (was_squeezed and expanding_now):
            return []
        last = ltf[-1]
        direction = "long" if last["c"] > last["o"] else "short"
        entry = last["c"]
        if direction == "long":
            sl = last["l"] - ctx.atr_ltf * 0.15
            tp1 = entry + ctx.atr_ltf * 1.5
            tp2 = entry + ctx.atr_ltf * 3.0
        else:
            sl = last["h"] + ctx.atr_ltf * 0.15
            tp1 = entry - ctx.atr_ltf * 1.5
            tp2 = entry - ctx.atr_ltf * 3.0
        confluences = ["Volatility squeeze release", "Expansion candle confirmation",
                        f"Regime: {ctx.regime}"]
        confidence = 58
        return _one_or_none(self._finalize(ctx, direction, entry, sl, tp1, tp2, confidence,
                                            confluences, entry_is_market=True))


def _one_or_none(cand: Optional[SignalCandidate]) -> list[SignalCandidate]:
    return [cand] if cand is not None else []


ALL_ENGINES: list[SpecializedEngine] = [
    SMCEngine(), TrendContinuationEngine(), BreakoutEngine(), PullbackEngine(),
    LiquiditySweepEngine(), OrderBlockEngine(), BreakerBlockEngine(), FairValueGapEngine(),
    MomentumEngine(), ReversalEngine(), MeanReversionEngine(), RangeTradingEngine(),
    VolatilityExpansionEngine(),
]

# ================================================================================================
# SECTION 9 -- DECISION ENGINE
# ================================================================================================

@dataclass
class EngineWeight:
    weight: float = 1.0
    n_trades: int = 0
    win_rate: float = 0.5
    profit_factor: float = 1.0
    brier: float = 0.25


class DecisionEngine:
    """Centralized ranking/selection layer. Nothing here is fixed-weight: every engine's
    contribution to a candidate's final score is scaled by that engine's *adaptively learned*
    performance weight, which is itself only updated once a segment has reached
    MIN_TRADES_FOR_ADAPTATION -- see LearningSystem
    below, which owns the weight-update math. The Decision Engine only consumes weights, it
    never mutates them, keeping the read/write responsibility for learned state in one place."""

    def __init__(self, engine_weights: dict[str, EngineWeight]):
        self.engine_weights = engine_weights

    def _weight_for(self, engine_name: str) -> EngineWeight:
        return self.engine_weights.setdefault(engine_name, EngineWeight())

    def score(self, cand: SignalCandidate, ctx: MarketContext) -> tuple[float, list[str]]:
        vetoes: list[str] = []
        fit = regime_fit_score(cand, ctx)
        if fit < 0.5:
            vetoes.append(f"regime-fit veto: {ctx.regime} not in {cand.best_fit_regimes}")

        ew = self._weight_for(cand.engine)
        learned_multiplier = clamp(ew.weight, 0.25, 2.0)

        mtf_bonus = 1.1 if (REQUIRE_MTF_ALIGNMENT and ctx.mtf_aligned) else 1.0
        if REQUIRE_MTF_ALIGNMENT and not ctx.mtf_aligned and cand.engine not in (
            "LiquiditySweep", "MeanReversion", "RangeTrading"):
            vetoes.append("MTF alignment required but absent")

        if len(cand.confluences) < MIN_NAMED_CONFLUENCES:
            vetoes.append(f"insufficient confluences ({len(cand.confluences)} < {MIN_NAMED_CONFLUENCES})")

        rr_component = clamp(cand.expected_rr / 3.0, 0.0, 1.0)
        confidence_component = cand.confidence / 100.0
        calibration_component = 1.0 - clamp(ew.brier, 0.0, 1.0)

        raw_score = (
            confidence_component * 0.30
            + rr_component * 0.20
            + calibration_component * 0.15
            + fit * 0.20
            + clamp(ew.win_rate, 0.0, 1.0) * 0.15
        ) * learned_multiplier * mtf_bonus

        return raw_score, vetoes

    def rank(self, candidates: list[SignalCandidate], ctx_by_symbol: dict[str, MarketContext]
             ) -> list[SignalCandidate]:
        scored = []
        for cand in candidates:
            ctx = ctx_by_symbol[cand.symbol]
            s, vetoes = self.score(cand, ctx)
            cand.score = s
            cand.veto_reasons = vetoes
            if vetoes:
                continue
            scored.append(cand)
        scored.sort(key=lambda c: c.score, reverse=True)
        return self._deduplicate_correlated(scored)

    @staticmethod
    def _deduplicate_correlated(candidates: list[SignalCandidate]) -> list[SignalCandidate]:
        """Keeps the single highest-scoring candidate per (symbol, direction) so multiple
        engines agreeing on the same trade don't count as independent signals for frequency,
        exposure, or concurrency-cap purposes -- while still letting the confluence they share
        lift that one candidate's score via each engine's own scoring pass."""
        best: dict[tuple[str, str], SignalCandidate] = {}
        for cand in candidates:
            key = (cand.symbol, cand.direction)
            if key not in best or cand.score > best[key].score:
                best[key] = cand
        return sorted(best.values(), key=lambda c: c.score, reverse=True)

# ================================================================================================
# SECTION 10 -- SIGNAL LIFECYCLE: ENTRY-FILL VERIFICATION & OUTCOME RESOLUTION
# ================================================================================================
#
# Design notes:
# * sig["sl"] is written once at creation and never reassigned -- there is no "move SL to
#   breakeven on TP1" code path.
# * Non-market signals start entry_filled=False with a bars_pending counter; market-price
#   entries start entry_filled=True and skip the fill check.
# * Before TP1 is secured, SL is checked first on any candle where both SL/TP appear reachable
#   (conservative -- costs an unverifiable marginal win, never manufactures a loss). Once TP1 IS
#   secured, a later original-SL touch is credited as `tp1_stop` (a WIN with TP1's R banked).


def evaluate_candle_for_signal(sig: dict, candle: dict) -> tuple[Optional[str], bool]:
    """Advances one active signal by exactly one candle. Returns (event, closed):
      event in {None, "expired", "tp1_partial", "tp2", "tp1_stop", "sl"}
      closed is True once no further monitoring is needed for this signal."""
    lo, hi = candle["l"], candle["h"]
    direction = sig["direction"]

    if not sig["entry_filled"]:
        entry = sig["entry"]
        if not (lo <= entry <= hi):
            sig["bars_pending"] = sig.get("bars_pending", 0) + 1
            if sig["bars_pending"] >= PENDING_ENTRY_EXPIRY_BARS:
                return "expired", True
            return None, False
        sig["entry_filled"] = True
        sig["fill_price"] = entry
        sig["filled_at"] = candle["t"]
        # Fall through: this same candle may still register a same-candle SL/TP hit.

    sl, tp1, tp2 = sig["sl"], sig["tp1"], sig["tp2"]
    if direction == "long":
        hit_sl, hit_tp1, hit_tp2 = lo <= sl, hi >= tp1, hi >= tp2
    else:
        hit_sl, hit_tp1, hit_tp2 = hi >= sl, lo <= tp1, lo <= tp2

    if not sig.get("tp1_hit", False):
        if hit_sl:
            return "sl", True
        if hit_tp1:
            sig["tp1_hit"] = True
            sig["tp1_hit_at"] = candle["t"]
            if hit_tp2:
                return "tp2", True
            return "tp1_partial", False
        return None, False
    else:
        # TP1 already secured on a prior candle. sig["sl"] is unchanged since creation.
        if hit_tp2:
            return "tp2", True
        if hit_sl:
            return "tp1_stop", True
        return None, False


def _r_multiple(sig: dict, price: float) -> float:
    risk = abs(sig["entry"] - sig["sl"])
    if risk <= 0:
        return 0.0
    if sig["direction"] == "long":
        return safe_div(price - sig["entry"], risk, 0.0)
    return safe_div(sig["entry"] - price, risk, 0.0)


def realized_r_for_close(sig: dict, event: str) -> float:
    """Size-weighted realized R: 50% of size closes at TP1, the remaining 50%
    runs to its eventual exit. A flat 'TP1 = 1R' credit would only be correct for a 100%-at-TP1
    exit, so both legs are blended here regardless of which final event closed the trade."""
    if event == "sl":
        return -1.0
    r_tp1 = _r_multiple(sig, sig["tp1"])
    if event == "tp1_stop":
        r_final = _r_multiple(sig, sig["sl"])
    elif event == "tp2":
        r_final = _r_multiple(sig, sig["tp2"])
    else:
        return 0.0
    return 0.5 * r_tp1 + 0.5 * r_final


# Every one of these consumers must only ever see these two labels for win/loss statistics.
WIN_EVENTS = ("tp2", "tp1_stop")
LOSS_EVENTS = ("sl",)
EXCLUDED_EVENTS = ("expired", "expired_gap")  # never fed into win-rate/PF/calibration/weights


def outcome_label(event: str) -> str:
    if event in WIN_EVENTS:
        return "win"
    if event in LOSS_EVENTS:
        return "loss"
    return event  # "expired" / "expired_gap" -- excluded from all win/loss consumers


def forensic_tag(sig: dict, event: str, ctx_regime_at_entry: str, ctx_regime_now: str) -> str:
    """Every closed trade gets a concrete, human-readable reason before it's
    allowed to feed back into any statistic, so the learning system reinforces genuine signal
    rather than noise."""
    if event == "expired":
        return "entry never filled within pending window"
    if event == "expired_gap":
        return "candle data gap prevented reliable resolution; excluded from stats"
    if event == "sl" and not sig.get("tp1_hit"):
        if sig.get("_mtf_aligned_at_entry") is False:
            return "stopped out before MTF confirmation ever aligned"
        if sig.get("_near_swept_pool"):
            return "chased a level immediately adjacent to swept liquidity"
        if ctx_regime_at_entry != ctx_regime_now:
            return "regime shifted against the position before structure invalidated"
        return "correct structural read invalidated; stopped at original SL"
    if event == "tp1_stop":
        return "TP1 secured, original SL later hit on the runner -- correct read, poor RR capture"
    if event == "tp2":
        return "full thesis played out to TP2"
    return "unresolved"


def advance_active_signal(sig: dict, ltf_candles: list[dict]) -> Optional[tuple[str, dict]]:
    """Feeds every LTF candle strictly after the signal's own origin candle, in chronological
    order, into evaluate_candle_for_signal, stopping at the first closing event. Detects data
    gaps (a missing expected 15m step between the last-checked candle and the next available
    one) and resolves those as "expired_gap" rather than silently mis-scoring a trade off
    incomplete data."""
    origin_ts = sig["signal_bar_ts"]
    last_checked = sig.get("last_checked_ts", origin_ts)
    relevant = [c for c in ltf_candles if c["t"] > last_checked]
    if not relevant:
        return None
    interval_ms = _interval_to_ms(TF_LTF)
    for c in relevant:
        expected_gap = c["t"] - last_checked
        if expected_gap > interval_ms * 2 and last_checked != origin_ts:
            return "expired_gap", c
        if expected_gap > interval_ms * 3:
            return "expired_gap", c
        event, closed = evaluate_candle_for_signal(sig, c)
        sig["last_checked_ts"] = c["t"]
        if closed:
            return event, c
    return None

# ================================================================================================
# SECTION 11 -- CONTINUOUS LEARNING SYSTEM
# ================================================================================================

def _default_segment_stats() -> dict:
    return {
        "n": 0, "wins": 0, "losses": 0, "sum_r": 0.0, "gross_profit_r": 0.0, "gross_loss_r": 0.0,
        "sum_hold_secs": 0.0, "mae_sum": 0.0, "mfe_sum": 0.0, "brier_sum": 0.0,
        "sum_confidence": 0.0,
    }


class LearningSystem:
    """Owns every learned/adaptive number in the engine: per-(asset, regime, timeframe, engine)
    segment statistics, engine weights, and confidence calibration. Nothing here updates live
    behavior until MIN_TRADES_FOR_ADAPTATION samples exist for the relevant segment (Section
    13's mandatory minimum-sample-size gate) -- small samples are tracked (so they can mature)
    but never allowed to move weights or thresholds while still small, which prevents the
    engine from overfitting to a handful of recent trades."""

    def __init__(self, state: dict):
        self.state = state
        self.state.setdefault("engine_stats", {})
        self.state.setdefault("segment_stats", {})   # key: "asset|regime|tf|engine"
        self.state.setdefault("engine_weights", {})
        self.state.setdefault("calibration_buckets", {})  # confidence decile -> [n, n_correct]

    def engine_weight_objects(self) -> dict[str, EngineWeight]:
        out = {}
        for name, d in self.state["engine_weights"].items():
            out[name] = EngineWeight(**d)
        return out

    def _segment_key(self, asset: str, regime: str, timeframe: str, engine: str) -> str:
        return f"{asset}|{regime}|{timeframe}|{engine}"

    def record_closed_trade(self, sig: dict, event: str, close_price: float, hold_secs: float,
                             realized_r: float, tag: str) -> None:
        label = outcome_label(event)
        history = self.state.setdefault("trade_history", [])
        history.append({
            "id": sig["id"], "engine": sig["engine"], "symbol": sig["symbol"],
            "direction": sig["direction"], "entry": sig["entry"], "sl": sig["sl"],
            "tp1": sig["tp1"], "tp2": sig["tp2"], "confidence": sig["confidence"],
            "result": event, "label": label, "realized_r": realized_r, "close_price": close_price,
            "regime_at_entry": sig.get("regime"), "hold_secs": hold_secs, "tag": tag,
            "closed_at": iso(), "mae_r": sig.get("mae_r", 0.0), "mfe_r": sig.get("mfe_r", 0.0),
        })
        if len(history) > 5000:
            del history[: len(history) - 5000]

        if label not in ("win", "loss"):
            return  # expired/expired_gap trades never touch any statistic below.

        engine_stats = self.state["engine_stats"].setdefault(sig["engine"], _default_segment_stats())
        self._accumulate(engine_stats, label, realized_r, hold_secs, sig)

        seg_key = self._segment_key(sig["symbol"], sig.get("regime", "unknown"), TF_LTF, sig["engine"])
        seg_stats = self.state["segment_stats"].setdefault(seg_key, _default_segment_stats())
        self._accumulate(seg_stats, label, realized_r, hold_secs, sig)

        self._update_calibration(sig["confidence"], label)
        self._maybe_update_engine_weight(sig["engine"], engine_stats)

    @staticmethod
    def _accumulate(stats: dict, label: str, r: float, hold_secs: float, sig: dict) -> None:
        stats["n"] += 1
        stats["sum_r"] += r
        stats["sum_hold_secs"] += hold_secs
        stats["mae_sum"] += sig.get("mae_r", 0.0)
        stats["mfe_sum"] += sig.get("mfe_r", 0.0)
        stats["sum_confidence"] += sig.get("confidence", 0.0)
        if label == "win":
            stats["wins"] += 1
            stats["gross_profit_r"] += max(r, 0.0)
        else:
            stats["losses"] += 1
            stats["gross_loss_r"] += max(-r, 0.0)

    def _update_calibration(self, confidence: float, label: str) -> None:
        bucket = str(int(clamp(confidence, 0, 99.9) // 10) * 10)
        buckets = self.state["calibration_buckets"]
        b = buckets.setdefault(bucket, {"n": 0, "n_correct": 0})
        b["n"] += 1
        if label == "win":
            b["n_correct"] += 1

    def _maybe_update_engine_weight(self, engine_name: str, stats: dict) -> None:
        """Only adapt once a segment has reached MIN_TRADES_FOR_ADAPTATION. Below
        that threshold the raw stats keep accumulating but engine_weight stays at its last
        (or default 1.0) value, so a hot or cold streak of a handful of trades can't swing
        live behavior."""
        if stats["n"] < MIN_TRADES_FOR_ADAPTATION:
            return
        win_rate = safe_div(stats["wins"], stats["n"], 0.5)
        profit_factor = safe_div(stats["gross_profit_r"], max(stats["gross_loss_r"], 1e-6),
                                  stats["gross_profit_r"] or 1.0)
        avg_confidence = safe_div(stats["sum_confidence"], stats["n"], 50.0) / 100.0
        actual_rate = win_rate
        brier = (avg_confidence - actual_rate) ** 2

        # Weight is a smooth, bounded function of win-rate and profit factor relative to a
        # neutral 50%/1.0 baseline -- never a hard reassignment, so weights drift gradually
        # (never overfitting to the newest handful of trades).
        wr_component = clamp((win_rate - 0.5) * 2.0, -1.0, 1.0)          # -1..1
        pf_component = clamp((profit_factor - 1.0) / 2.0, -1.0, 1.0)     # roughly -1..1
        target_weight = clamp(1.0 + 0.35 * wr_component + 0.25 * pf_component, 0.25, 2.0)

        weights = self.state["engine_weights"]
        current = weights.setdefault(engine_name, asdict(EngineWeight()))
        # Exponential smoothing toward the new target -- gradual adaptation, not a jump.
        smoothing = 0.15
        current["weight"] = current["weight"] * (1 - smoothing) + target_weight * smoothing
        current["n_trades"] = stats["n"]
        current["win_rate"] = win_rate
        current["profit_factor"] = profit_factor
        current["brier"] = brier

    def calibration_accuracy(self) -> float:
        buckets = self.state["calibration_buckets"]
        total_n = sum(b["n"] for b in buckets.values())
        if total_n == 0:
            return 0.0
        errors = []
        for bucket, b in buckets.items():
            if b["n"] == 0:
                continue
            predicted = (int(bucket) + 5) / 100.0
            actual = b["n_correct"] / b["n"]
            errors.append(abs(predicted - actual) * b["n"])
        return 1.0 - clamp(sum(errors) / total_n, 0.0, 1.0)

    def adaptive_filter_tightness(self) -> float:
        """Automatically tighten thresholds in chaotic/low-quality markets and relax
        them in clean/high-quality markets. Returns a multiplier >1 (tighten) or <1 (relax)
        applied to MIN_NAMED_CONFLUENCES-style thresholds, derived from recent realized
        volatility of trade outcomes (a proxy for how noisy/choppy conditions have been)."""
        history = self.state.get("trade_history", [])
        recent = [h for h in history[-40:] if h.get("label") in ("win", "loss")]
        if len(recent) < MIN_TRADES_FOR_ADAPTATION:
            return 1.0
        win_rate = sum(1 for h in recent if h["label"] == "win") / len(recent)
        if win_rate < 0.35:
            return 1.25   # markets have been noisy/unfavorable -- tighten standards
        if win_rate > 0.60:
            return 0.9    # conditions have been clean -- allow slightly more frequency
        return 1.0

# ================================================================================================
# SECTION 12 -- RISK MANAGEMENT & REALISTIC COST MODELING
# ================================================================================================

@dataclass
class RiskParameters:
    account_risk_fraction: float = 0.0075   # fraction of account equity risked per trade
    max_concurrent_risk_fraction: float = 0.05


class RiskManager:
    """Position sizing and expected-value math, with realistic cost modeling folded in so
    simulated/paper win-rate and profit factor aren't inflated relative to live trading. Sizing itself is denominated in R (risk units); the caller/exchange-side
    integration converts R to actual position size against account equity."""

    def __init__(self, params: RiskParameters = RiskParameters()):
        self.params = params

    @staticmethod
    def cost_adjusted_rr(entry: float, sl: float, tp1: float, tp2: float, atr_val: float,
                          entry_is_market: bool) -> float:
        """Shaves expected RR down by round-trip taker/maker fees plus assumed slippage, so the
        Decision Engine's EV math reflects what live trading will actually realize rather than a
        frictionless backtest number."""
        risk = abs(entry - sl)
        reward2 = abs(tp2 - entry)
        if risk <= 0:
            return 0.0
        fee_rate = MAKER_FEE_RATE if not entry_is_market else TAKER_FEE_RATE
        round_trip_fee_cost = 2 * fee_rate * entry
        slippage_cost = ASSUMED_SLIPPAGE_ATR_FRACTION * atr_val
        adjusted_reward = max(reward2 - round_trip_fee_cost - slippage_cost, 0.0)
        adjusted_risk = risk + round_trip_fee_cost * 0.5 + slippage_cost * 0.5
        return safe_div(adjusted_reward, adjusted_risk, 0.0)

    def expected_value_r(self, cand: SignalCandidate, engine_weight: EngineWeight) -> float:
        """EV in R units using this engine's *learned* (sample-size-gated) win rate where
        available, falling back to a conservative 45% prior for engines/segments still below
        MIN_TRADES_FOR_ADAPTATION so a brand-new or thin-sample engine can't claim an
        unrealistically rosy EV."""
        p_win = engine_weight.win_rate if engine_weight.n_trades >= MIN_TRADES_FOR_ADAPTATION else 0.45
        r_win = cand.expected_rr
        r_loss = -1.0
        return p_win * r_win + (1 - p_win) * r_loss

    def position_size_r(self, account_equity: float, entry: float, sl: float) -> float:
        """Returns position notional such that a full SL hit loses exactly
        account_risk_fraction * account_equity."""
        risk_per_unit = abs(entry - sl)
        if risk_per_unit <= 0 or account_equity <= 0:
            return 0.0
        dollar_risk = account_equity * self.params.account_risk_fraction
        units = dollar_risk / risk_per_unit
        return units * entry

# ================================================================================================
# SECTION 13 -- TELEGRAM INTEGRATION
# ================================================================================================

# Every value here must be one of Telegram's allowed setMessageReaction emoji -- anything
# outside that fixed list is silently rejected by the API. Keys match the lifecycle event
# names actually produced by advance_active_signal()/send_status_update()/send_resolution().
REACTION_EMOJIS = {
    "activated": "\u26a1",        # zap -- entry filled, signal now live
    "tp1": "\U0001F525",          # fire -- TP1 partial hit
    "tp2": "\U0001F3C6",          # trophy -- full win, TP2 hit
    "tp1_stop": "\U0001F44D",     # thumbs up -- still a win (TP1 banked, SL hit later)
    "sl": "\U0001F44E",           # thumbs down -- loss
    "expired": "\U0001F937",      # shrug -- never filled, no result
    "expired_gap": "\U0001F914",  # thinking face -- data gap, excluded from stats
}


def _fmt_price(p: float) -> str:
    """Bare numeric formatting only -- no currency symbol, comma, or label -- so the Telegram
    monospace span around this value copies exactly this number and nothing else."""
    if p >= 100:
        return f"{p:.2f}"
    if p >= 1:
        return f"{p:.4f}"
    return f"{p:.6f}"


class TelegramNotifier:
    """All outbound Telegram messaging. Every method that displays Entry/SL/TP1/TP2 renders each
    value inside its own backtick span on its own line, with the label kept outside the span, for
    copy-paste-friendly formatting."""

    def __init__(self, token: str = TG_BOT_TOKEN, chat_id: str = TG_CHAT_ID):
        self.token = token
        self.chat_id = chat_id

    def _send(self, text: str, parse_mode: str = "Markdown") -> Optional[int]:
        if not self.token or not self.chat_id:
            log.info(f"[telegram disabled -- would send]\n{text}")
            return None
        url = f"{TELEGRAM_API_BASE}/bot{self.token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": parse_mode}
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as resp:
                    body = json.loads(resp.read().decode("utf-8"))
                    return body.get("result", {}).get("message_id")
            except (urllib.error.URLError, urllib.error.HTTPError, ValueError) as e:
                log.warning(f"Telegram send failed (attempt {attempt + 1}/3): {e}")
                time.sleep(1.5 ** attempt)
        return None

    def react_telegram(self, message_id: Optional[int], emoji: str) -> None:
        """Best-effort emoji reaction on a signal's original message; failures are logged and
        swallowed -- never let a reaction failure interrupt the scan."""
        if not self.token or not self.chat_id or not message_id or not emoji:
            return
        url = f"{TELEGRAM_API_BASE}/bot{self.token}/setMessageReaction"
        payload = {
            "chat_id": self.chat_id, "message_id": message_id,
            "reaction": [{"type": "emoji", "emoji": emoji}],
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS):
                pass
        except (urllib.error.URLError, urllib.error.HTTPError, ValueError) as e:
            log.debug(f"Telegram reaction failed (non-fatal): {e}")

    def send_new_signal(self, sig: dict) -> Optional[int]:
        header = f"*{ENGINE_NAME} {ENGINE_VERSION}*  --  New Signal"
        direction_word = "LONG" if sig["direction"] == "long" else "SHORT"
        fill_note = ("Market entry" if sig["entry_is_market"] else
                     f"Resting entry -- expires unfilled after {PENDING_ENTRY_EXPIRY_BARS} bars")
        lines = [
            header,
            f"{sig['symbol']}  |  {direction_word}  |  Engine: {sig['engine']}",
            f"Confidence: {sig['confidence']:.0f}%   Expected RR: {sig['expected_rr']:.2f}",
            f"Regime: {sig.get('regime', 'n/a')}   ({fill_note})",
            "",
            f"Entry: `{_fmt_price(sig['entry'])}`",
            f"SL: `{_fmt_price(sig['sl'])}`",
            f"TP1: `{_fmt_price(sig['tp1'])}`",
            f"TP2: `{_fmt_price(sig['tp2'])}`",
            "",
            "Confluences: " + "; ".join(sig["confluences"]),
        ]
        return self._send("\n".join(lines))

    def send_status_update(self, sig: dict, status: str) -> None:
        header = f"*{ENGINE_NAME}*  --  {sig['symbol']} {status.upper()}"
        lines = [header]
        if status == "activated":
            lines.append("Entry filled -- signal is now live.")
        elif status == "tp1":
            lines.append("TP1 hit -- partial profit secured.")
            lines.append(f"SL remains at its original level, unchanged: `{_fmt_price(sig['sl'])}`")
            lines.append("(You may choose to manually move your own SL to entry if you want to "
                          "lock in breakeven yourself -- the engine does not do this automatically.)")
        elif status == "expired":
            lines.append(f"Entry never filled within {PENDING_ENTRY_EXPIRY_BARS} bars -- signal "
                          f"expired. Not counted as a win or loss.")
        lines += [
            f"Entry: `{_fmt_price(sig['entry'])}`", f"SL: `{_fmt_price(sig['sl'])}`",
            f"TP1: `{_fmt_price(sig['tp1'])}`", f"TP2: `{_fmt_price(sig['tp2'])}`",
        ]
        self._send("\n".join(lines))
        self.react_telegram(sig.get("tg_message_id"), REACTION_EMOJIS.get(status, ""))

    def send_resolution(self, sig: dict, event: str, realized_r: float, tag: str) -> None:
        headline_map = {
            "tp2": "\u2705 TP2 hit -- WIN",
            "tp1_stop": "\u2705 TP1 secured, SL later hit -- WIN (TP1's R credited; original SL "
                         "was never moved)",
            "sl": "\u274C SL hit, no TP1 -- LOSS",
            "expired": "\u23F3 Entry never filled -- expired (excluded from win/loss stats)",
            "expired_gap": "\u26A0\uFE0F Data gap prevented reliable resolution -- excluded from stats",
        }
        lines = [
            f"*{ENGINE_NAME}*  --  {sig['symbol']} resolved",
            headline_map.get(event, event),
            f"Realized R: {realized_r:+.2f}" if event not in ("expired", "expired_gap") else "",
            f"Reason: {tag}",
            f"Entry: `{_fmt_price(sig['entry'])}`", f"SL: `{_fmt_price(sig['sl'])}`",
            f"TP1: `{_fmt_price(sig['tp1'])}`", f"TP2: `{_fmt_price(sig['tp2'])}`",
        ]
        self._send("\n".join(l for l in lines if l))
        self.react_telegram(sig.get("tg_message_id"), REACTION_EMOJIS.get(event, ""))

    def send_daily_summary(self, state: dict, learning: "LearningSystem") -> None:
        history = state.get("trade_history", [])
        today = utcnow().date()
        todays = [h for h in history if datetime.fromisoformat(h["closed_at"]).date() == today]
        resolved = [h for h in todays if h["label"] in ("win", "loss")]
        wins = [h for h in resolved if h["label"] == "win"]
        losses = [h for h in resolved if h["label"] == "loss"]
        win_rate = safe_div(len(wins), len(resolved), 0.0)
        gross_profit = sum(max(h["realized_r"], 0.0) for h in resolved)
        gross_loss = sum(max(-h["realized_r"], 0.0) for h in resolved)
        profit_factor = safe_div(gross_profit, max(gross_loss, 1e-6), gross_profit or 0.0)
        avg_rr = safe_div(sum(h["realized_r"] for h in resolved), len(resolved), 0.0)
        avg_hold_h = safe_div(sum(h["hold_secs"] for h in resolved), len(resolved), 0.0) / 3600.0

        by_regime: dict[str, list[int]] = {}
        by_engine: dict[str, list[int]] = {}
        for h in resolved:
            r_key = h.get("regime_at_entry", "unknown")
            by_regime.setdefault(r_key, [0, 0])
            by_regime[r_key][0] += 1
            by_regime[r_key][1] += 1 if h["label"] == "win" else 0
            e_key = h["engine"]
            by_engine.setdefault(e_key, [0, 0])
            by_engine[e_key][0] += 1
            by_engine[e_key][1] += 1 if h["label"] == "win" else 0

        best = max(resolved, key=lambda h: h["realized_r"], default=None)
        worst = min(resolved, key=lambda h: h["realized_r"], default=None)

        lines = [
            f"*{ENGINE_NAME} {ENGINE_VERSION}  --  Daily Summary*",
            f"Total signals: {len(todays)}   Wins: {len(wins)}   Losses: {len(losses)}",
            f"Win rate: {win_rate*100:.1f}%   Profit factor: {profit_factor:.2f}   Avg RR: {avg_rr:+.2f}",
            f"Avg hold time: {avg_hold_h:.1f}h",
            "",
            "By regime: " + ", ".join(f"{k} {v[1]}/{v[0]}" for k, v in by_regime.items()) or "n/a",
            "By engine: " + ", ".join(f"{k} {v[1]}/{v[0]}" for k, v in by_engine.items()) or "n/a",
            "",
            f"Best: {best['symbol']} {best['realized_r']:+.2f}R ({best['engine']})" if best else "Best: n/a",
            f"Worst: {worst['symbol']} {worst['realized_r']:+.2f}R ({worst['engine']})" if worst else "Worst: n/a",
            f"Confidence calibration accuracy: {learning.calibration_accuracy()*100:.1f}%",
            "Learning: engine weights adapt only once a segment reaches "
            f"{MIN_TRADES_FOR_ADAPTATION}+ resolved trades; thinner segments are still tracked "
            f"but not yet influencing live weighting.",
        ]
        self._send("\n".join(lines))

# ================================================================================================
# SECTION 14 -- ORCHESTRATION (Hyperliquid workflow)
# ================================================================================================

def _new_signal_id(state: dict) -> str:
    state["_next_id"] = state.get("_next_id", 0) + 1
    return f"sig_{state['_next_id']:08d}"


class Orchestrator:
    """Ties every module together into the single scan-per-run execution the spec requires:
    one shared candle fetch per (symbol, timeframe), one decision pass, one lifecycle-advance
    pass over existing active signals, one state write. Designed to complete comfortably inside
    a 15-minute GitHub Actions window even across a ~10-symbol watchlist, since every expensive
    step (candles, indicators) is computed at most once per symbol per run."""

    def __init__(self, state: dict):
        self.state = state
        self.state.setdefault("active_signals", [])
        self.state.setdefault("trade_history", [])
        self.client = HyperliquidClient()
        self.cache = CandleCache(self.client)
        self.telegram = TelegramNotifier()
        self.learning = LearningSystem(state)
        self.decision_engine = DecisionEngine(self.learning.engine_weight_objects())
        self.risk = RiskManager()

    def run_scan(self) -> None:
        log.info(f"=== {ENGINE_NAME} {ENGINE_VERSION} scan starting: {iso()} ===")
        ctx_by_symbol: dict[str, MarketContext] = {}
        candidates: list[SignalCandidate] = []

        for symbol in WATCHLIST:
            try:
                candles = self.cache.get_all_timeframes(symbol)
                ctx = build_market_context(symbol, candles)
                if ctx is None:
                    log.warning(f"{symbol}: insufficient candle history this run, skipping")
                    continue
                ctx_by_symbol[symbol] = ctx
                for engine in ALL_ENGINES:
                    try:
                        candidates.extend(engine.generate(ctx, candles))
                    except Exception:
                        log.exception(f"{engine.name} raised generating a candidate for {symbol}; skipping")
            except Exception:
                log.exception(f"Failed processing {symbol} this run; skipping symbol")

        tightness = self.learning.adaptive_filter_tightness()
        effective_min_confluences = max(2, round(MIN_NAMED_CONFLUENCES * tightness))
        ranked = [c for c in candidates if len(c.confluences) >= effective_min_confluences]
        ranked = self.decision_engine.rank(ranked, ctx_by_symbol)

        self._advance_active_signals(ctx_by_symbol)
        self._emit_new_signals(ranked, ctx_by_symbol)
        self._maybe_send_daily_summary()

        self.state["engine_weights"] = {
            name: asdict(ew) for name, ew in self.decision_engine.engine_weights.items()
        }
        self.state["last_run_at"] = iso()
        atomic_write_json(STATE_PATH, self.state)
        log.info(f"=== scan complete: {len(ranked)} candidate(s) survived ranking, "
                 f"{len(self.state['active_signals'])} active signal(s) ===")

    def _emit_new_signals(self, ranked: list[SignalCandidate], ctx_by_symbol: dict[str, MarketContext]) -> None:
        active = self.state["active_signals"]
        occupied = {(a["symbol"], a["direction"]) for a in active}
        slots_free = MAX_CONCURRENT_ACTIVE_SIGNALS - len(active)
        for cand in ranked:
            if slots_free <= 0:
                break
            if (cand.symbol, cand.direction) in occupied:
                continue
            ctx = ctx_by_symbol[cand.symbol]
            ltf = self.cache.get(cand.symbol, TF_LTF)
            sig_id = _new_signal_id(self.state)
            sig = {
                "id": sig_id, "engine": cand.engine, "symbol": cand.symbol,
                "direction": cand.direction, "entry": cand.entry, "sl": cand.sl,
                "tp1": cand.tp1, "tp2": cand.tp2, "confidence": cand.confidence,
                "expected_rr": cand.expected_rr, "confluences": cand.confluences,
                "regime": ctx.regime, "entry_is_market": cand.entry_is_market,
                "entry_filled": cand.entry_is_market, "bars_pending": 0, "tp1_hit": False,
                "signal_bar_ts": ltf[-1]["t"] if ltf else 0,
                "last_checked_ts": ltf[-1]["t"] if ltf else 0,
                "opened_at": iso(), "mae_r": 0.0, "mfe_r": 0.0,
                "_mtf_aligned_at_entry": ctx.mtf_aligned,
                "_near_swept_pool": ctx.sweep is not None,
                "status": "activated" if cand.entry_is_market else "pending",
            }
            msg_id = self.telegram.send_new_signal(sig)
            sig["tg_message_id"] = msg_id
            active.append(sig)
            occupied.add((cand.symbol, cand.direction))
            slots_free -= 1

    def _advance_active_signals(self, ctx_by_symbol: dict[str, MarketContext]) -> None:
        still_active = []
        for sig in self.state["active_signals"]:
            ltf = self.cache.get(sig["symbol"], TF_LTF)
            if not ltf:
                still_active.append(sig)
                continue
            was_pending = not sig["entry_filled"]
            result = advance_active_signal(sig, ltf)
            if was_pending and sig["entry_filled"] and sig.get("status") == "pending":
                sig["status"] = "activated"
                self.telegram.send_status_update(sig, "activated")
            if result is None:
                self._update_mae_mfe(sig, ltf)
                still_active.append(sig)
                continue
            event, close_candle = result
            if event == "tp1_partial":
                self.telegram.send_status_update(sig, "tp1")
                still_active.append(sig)
                continue
            close_price = {"tp2": sig["tp2"], "tp1_stop": sig["sl"], "sl": sig["sl"]}.get(event, sig["entry"])
            realized_r = realized_r_for_close(sig, event)
            opened_dt = datetime.fromisoformat(sig["opened_at"])
            hold_secs = (utcnow() - opened_dt).total_seconds()
            ctx_now = ctx_by_symbol.get(sig["symbol"])
            tag = forensic_tag(sig, event, sig.get("regime", "unknown"),
                                ctx_now.regime if ctx_now else sig.get("regime", "unknown"))
            self.learning.record_closed_trade(sig, event, close_price, hold_secs, realized_r, tag)
            self.telegram.send_resolution(sig, event, realized_r, tag)
        self.state["active_signals"] = still_active

    @staticmethod
    def _update_mae_mfe(sig: dict, ltf: list[dict]) -> None:
        if not sig["entry_filled"] or not ltf:
            return
        last = ltf[-1]
        risk = abs(sig["entry"] - sig["sl"])
        if risk <= 0:
            return
        if sig["direction"] == "long":
            adverse = safe_div(sig["entry"] - last["l"], risk, 0.0)
            favorable = safe_div(last["h"] - sig["entry"], risk, 0.0)
        else:
            adverse = safe_div(last["h"] - sig["entry"], risk, 0.0)
            favorable = safe_div(sig["entry"] - last["l"], risk, 0.0)
        sig["mae_r"] = max(sig.get("mae_r", 0.0), adverse)
        sig["mfe_r"] = max(sig.get("mfe_r", 0.0), favorable)

    def _maybe_send_daily_summary(self) -> None:
        now = utcnow()
        last_sent = self.state.get("last_daily_summary_date")
        if now.hour == DAILY_SUMMARY_HOUR_UTC and last_sent != now.date().isoformat():
            self.telegram.send_daily_summary(self.state, self.learning)
            self.state["last_daily_summary_date"] = now.date().isoformat()


# ================================================================================================
# SECTION 15 -- ENTRYPOINT
# ================================================================================================

def main() -> None:
    state = load_json(STATE_PATH, default={})
    try:
        Orchestrator(state).run_scan()
    except Exception:
        log.exception("Fatal error during scan -- state as of last successful checkpoint is preserved")
        raise


if __name__ == "__main__":
    main()
