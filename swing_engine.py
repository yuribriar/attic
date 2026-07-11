#!/usr/bin/env python3
"""
OBSIDIAN v1.0.0
Adaptive multi-engine signal system for Hyperliquid perpetuals. Single
self-contained file. Scan-per-run model, driven by an external scheduler
(e.g. GitHub Actions cron); state persists to state.json between runs.
"""

from __future__ import annotations

import os
import sys
import json
import time
import logging
import statistics
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Any

try:
    import requests
except ImportError:  # pragma: no cover
    print("Missing dependency 'requests'. Install via requirements.txt", file=sys.stderr)
    raise

# ══════════════════════════════════════════════════════════════════════════
# SECTION 0 — ENGINE IDENTITY & GLOBAL CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════

ENGINE_NAME = "OBSIDIAN"
ENGINE_VERSION = "1.0.0"

def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v not in (None, "") else default

def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default

def _env_list(name: str, default: list[str]) -> list[str]:
    v = os.getenv(name)
    if not v:
        return default
    return [s.strip().upper() for s in v.split(",") if s.strip()]

# Secrets — read from environment only (GitHub Actions secrets), never hardcoded.
HL_API_URL = _env_str("HYPERLIQUID_API_URL", "https://api.hyperliquid.xyz/info")
HL_WALLET_ADDRESS = _env_str("HYPERLIQUID_WALLET_ADDRESS")   # optional, read-only info endpoint
HL_API_KEY = _env_str("HYPERLIQUID_API_KEY")                  # optional; info endpoint is public
TG_BOT_TOKEN = _env_str("TG_BOT_TOKEN")
TG_CHAT_ID = _env_str("TG_CHAT_ID")

STATE_PATH = Path(_env_str("STATE_FILE", "state.json"))
WATCHLIST = _env_list("WATCHLIST", [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
])

# Timeframes — 15M is the hard entry-timeframe floor (enforced by the assert below).
TF_HTF_BIAS = "1D"
TF_HTF_STRUCT = "4H"
TF_CONFIRM = "1H"
TF_ENTRY = "15M"
ALL_TIMEFRAMES = [TF_HTF_BIAS, TF_HTF_STRUCT, TF_CONFIRM, TF_ENTRY]
_FORBIDDEN_TFS = {"1M", "2M", "3M", "5M"}
assert not (set(ALL_TIMEFRAMES) & _FORBIDDEN_TFS), "Forbidden timeframe configured"

TARGET_SIGNALS_PER_DAY_MIN = _env_float("TARGET_SIGNALS_MIN", 5.0)
TARGET_SIGNALS_PER_DAY_MAX = _env_float("TARGET_SIGNALS_MAX", 10.0)
MAX_CANDIDATES_PER_SCAN = _env_int("MAX_CANDIDATES_PER_SCAN", 2)
MIN_RR = _env_float("MIN_RR", 1.5)
SCAN_INTERVAL_MINUTES = 15

LOG_LEVEL = _env_str("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s UTC %(levelname)s [%(name)s] %(message)s",
)
logging.Formatter.converter = time.gmtime
log = logging.getLogger(ENGINE_NAME)

# ══════════════════════════════════════════════════════════════════════════
# SECTION 1 — DATA MODELS
# ══════════════════════════════════════════════════════════════════════════

@dataclass
class Candle:
    ts: int      # epoch ms, candle open time
    o: float
    h: float
    l: float
    c: float
    v: float


@dataclass
class SwingPoint:
    index: int
    ts: int
    price: float
    kind: str          # "high" | "low"


@dataclass
class StructureEvent:
    kind: str           # "BOS" | "CHoCH"
    direction: str       # "long" | "short"
    ts: int
    price: float


@dataclass
class Zone:
    kind: str            # "order_block" | "breaker_block" | "fvg" | "liquidity_pool"
    direction: str        # "long" | "short"
    top: float
    bottom: float
    ts: int
    mitigated: bool = False


@dataclass
class RegimeSnapshot:
    trend_strength: float     # 0..1, ADX-style normalized
    trend_direction: str       # "up" | "down" | "flat"
    volatility_pct: float       # ATR / price, 0..1
    volatility_percentile: float  # 0..1 percentile vs recent history
    classification: str          # "trending" | "ranging" | "expansion" | "reversal" | "consolidation"
    regime_vector: dict[str, float] = field(default_factory=dict)


@dataclass
class Signal:
    engine: str
    symbol: str
    direction: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    confidence: float
    expected_rr: float
    regime_suitability: list[str]
    confluences: list[str]
    timeframe: str
    ts: int
    ev: float = 0.0
    score: float = 0.0


# ══════════════════════════════════════════════════════════════════════════
# SECTION 2 — STATE PERSISTENCE (atomic, crash-safe)
# ══════════════════════════════════════════════════════════════════════════

DEFAULT_STATE: dict[str, Any] = {
    "schema_version": 1,
    "engine_name": ENGINE_NAME,
    "engine_version": ENGINE_VERSION,
    "active_signals": {},        # signal_id -> signal dict + tracking fields
    "signal_history": [],        # completed signals (bounded)
    "engine_weights": {},        # engine_name -> adaptive weight (init 1.0)
    "engine_stats": {},          # engine_name -> {wins, losses, sum_rr, ...}
    "daily_counts": {},          # "YYYY-MM-DD" -> count
    "ev_threshold": 0.35,        # adaptive acceptance threshold
    "last_daily_summary_date": None,
    "confidence_calibration": {},  # bucket -> {predicted, realized, n}
    "cooldowns": {},             # "SYMBOL:direction" -> ts until which new signal is blocked
    "run_stats": {"total_runs": 0, "total_signals": 0, "last_run_ts": None},
}


class StateStore:
    def __init__(self, path: Path):
        self.path = path
        self.data: dict[str, Any] = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            log.info("No existing state file at %s; initializing default state.", self.path)
            return json.loads(json.dumps(DEFAULT_STATE))
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            merged = json.loads(json.dumps(DEFAULT_STATE))
            merged.update(loaded)
            return merged
        except (json.JSONDecodeError, OSError) as e:
            log.error("State file corrupt or unreadable (%s); reinitializing default state.", e)
            return json.loads(json.dumps(DEFAULT_STATE))

    def save(self) -> None:
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(self.data, f, indent=2, sort_keys=True, default=str)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self.path)
        except OSError as e:
            log.error("Failed to persist state atomically: %s", e)
            raise
        finally:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass


# ══════════════════════════════════════════════════════════════════════════
# SECTION 3 — HYPERLIQUID DATA LAYER (shared cache, throttling, backoff)
# ══════════════════════════════════════════════════════════════════════════

_TF_TO_HL_INTERVAL = {
    "15M": "15m", "30M": "30m", "1H": "1h", "2H": "2h",
    "4H": "4h", "8H": "8h", "12H": "12h", "1D": "1d",
}


class HyperliquidClient:
    """Shared, rate-limited, retrying, cached client for Hyperliquid's public
    `info` endpoint. All engines route candle requests through this single
    client so a scan never issues duplicate requests."""

    def __init__(self, base_url: str, min_interval_s: float = 0.15, timeout_s: float = 10.0):
        self.base_url = base_url
        self.min_interval_s = min_interval_s
        self.timeout_s = timeout_s
        self._lock = threading.Lock()
        self._last_request_ts = 0.0
        self._session = requests.Session()
        self._cache: dict[tuple[str, str], list[Candle]] = {}

    def _throttle(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self.min_interval_s - (now - self._last_request_ts)
            if wait > 0:
                time.sleep(wait)
            self._last_request_ts = time.monotonic()

    def _post(self, payload: dict, max_retries: int = 4) -> Optional[Any]:
        backoff = 0.5
        for attempt in range(1, max_retries + 1):
            self._throttle()
            try:
                resp = self._session.post(self.base_url, json=payload, timeout=self.timeout_s)
                if resp.status_code == 429:
                    log.warning("Hyperliquid rate limited (429); backing off %.1fs", backoff)
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 8.0)
                    continue
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError) as e:
                log.warning("Hyperliquid request failed (attempt %d/%d): %s", attempt, max_retries, e)
                time.sleep(backoff)
                backoff = min(backoff * 2, 8.0)
        log.error("Hyperliquid request permanently failed after %d attempts.", max_retries)
        return None

    def get_candles(self, symbol: str, timeframe: str, lookback: int = 300) -> list[Candle]:
        """Fetch candles with an in-run cache keyed by (symbol, timeframe).
        Returns [] on failure (graceful degradation)."""
        cache_key = (symbol, timeframe)
        if cache_key in self._cache:
            return self._cache[cache_key]

        interval = _TF_TO_HL_INTERVAL.get(timeframe)
        if interval is None:
            log.error("Unsupported timeframe requested: %s", timeframe)
            return []

        interval_ms = _interval_to_ms(timeframe)
        end_time = int(time.time() * 1000)
        start_time = end_time - interval_ms * lookback

        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": interval,
                "startTime": start_time,
                "endTime": end_time,
            },
        }
        raw = self._post(payload)
        candles: list[Candle] = []
        if isinstance(raw, list):
            for item in raw:
                try:
                    candles.append(Candle(
                        ts=int(item["t"]),
                        o=float(item["o"]),
                        h=float(item["h"]),
                        l=float(item["l"]),
                        c=float(item["c"]),
                        v=float(item.get("v", 0.0)),
                    ))
                except (KeyError, TypeError, ValueError):
                    continue
        candles.sort(key=lambda c: c.ts)
        self._cache[cache_key] = candles
        if not candles:
            log.warning("No candle data retrieved for %s %s (degraded gracefully).", symbol, timeframe)
        return candles


def _interval_to_ms(timeframe: str) -> int:
    unit = timeframe[-1]
    n = int(timeframe[:-1])
    if unit == "M":
        return n * 60 * 1000
    if unit == "H":
        return n * 60 * 60 * 1000
    if unit == "D":
        return n * 24 * 60 * 60 * 1000
    raise ValueError(f"Bad timeframe: {timeframe}")


# ══════════════════════════════════════════════════════════════════════════
# SECTION 4 — SHARED INDICATORS
# ══════════════════════════════════════════════════════════════════════════

def sma(values: list[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def ema_series(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    k = 2 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def true_range(prev_close: float, high: float, low: float) -> float:
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def atr(candles: list[Candle], period: int = 14) -> Optional[float]:
    if len(candles) < period + 1:
        return None
    trs = [true_range(candles[i - 1].c, candles[i].h, candles[i].l) for i in range(1, len(candles))]
    return sma(trs, period)


def adx(candles: list[Candle], period: int = 14) -> Optional[float]:
    if len(candles) < period * 2:
        return None
    plus_dm, minus_dm, trs = [], [], []
    for i in range(1, len(candles)):
        up = candles[i].h - candles[i - 1].h
        down = candles[i - 1].l - candles[i].l
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        trs.append(true_range(candles[i - 1].c, candles[i].h, candles[i].l))
    atr_v = sma(trs, period)
    if not atr_v or atr_v == 0:
        return None
    plus_di = 100 * (sma(plus_dm, period) or 0) / atr_v
    minus_di = 100 * (sma(minus_dm, period) or 0) / atr_v
    denom = plus_di + minus_di
    if denom == 0:
        return 0.0
    dx = 100 * abs(plus_di - minus_di) / denom
    return dx


def rsi(values: list[float], period: int = 14) -> Optional[float]:
    if len(values) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    avg_gain = sma(gains, period)
    avg_loss = sma(losses, period)
    if avg_gain is None or avg_loss is None:
        return None
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def volume_zscore(candles: list[Candle], lookback: int = 20) -> float:
    if len(candles) < lookback + 1:
        return 0.0
    vols = [c.v for c in candles[-(lookback + 1):-1]]
    if len(vols) < 2:
        return 0.0
    mean_v = statistics.mean(vols)
    stdev_v = statistics.pstdev(vols) or 1e-9
    return (candles[-1].v - mean_v) / stdev_v


# ══════════════════════════════════════════════════════════════════════════
# SECTION 5 — MARKET STRUCTURE / SMC PRIMITIVES (shared across engines)
# ══════════════════════════════════════════════════════════════════════════

def find_swing_points(candles: list[Candle], left: int = 3, right: int = 3) -> list[SwingPoint]:
    swings: list[SwingPoint] = []
    n = len(candles)
    for i in range(left, n - right):
        window_h = [candles[j].h for j in range(i - left, i + right + 1)]
        window_l = [candles[j].l for j in range(i - left, i + right + 1)]
        if candles[i].h == max(window_h) and window_h.count(candles[i].h) == 1:
            swings.append(SwingPoint(i, candles[i].ts, candles[i].h, "high"))
        if candles[i].l == min(window_l) and window_l.count(candles[i].l) == 1:
            swings.append(SwingPoint(i, candles[i].ts, candles[i].l, "low"))
    return swings


def detect_structure_events(candles: list[Candle], swings: list[SwingPoint]) -> list[StructureEvent]:
    """BOS = close beyond the most recent same-direction structural swing in
    the direction of the prevailing trend; CHoCH = the first opposite-direction
    break after a run of same-direction structure (candle-close confirmed)."""
    events: list[StructureEvent] = []
    if len(swings) < 2:
        return events

    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    trend = None  # "up" | "down"

    for i, c in enumerate(candles):
        recent_highs = [s for s in highs if s.index < i]
        recent_lows = [s for s in lows if s.index < i]
        if recent_highs and c.c > recent_highs[-1].price:
            kind = "CHoCH" if trend == "down" else "BOS"
            events.append(StructureEvent(kind, "long", c.ts, c.c))
            trend = "up"
        elif recent_lows and c.c < recent_lows[-1].price:
            kind = "CHoCH" if trend == "up" else "BOS"
            events.append(StructureEvent(kind, "short", c.ts, c.c))
            trend = "down"
    return events


def find_order_blocks(candles: list[Candle], events: list[StructureEvent], lookback: int = 12) -> list[Zone]:
    """Last opposite-colored candle preceding an impulsive structural break."""
    zones: list[Zone] = []
    ts_to_idx = {c.ts: i for i, c in enumerate(candles)}
    for ev in events:
        idx = ts_to_idx.get(ev.ts)
        if idx is None:
            continue
        start = max(0, idx - lookback)
        segment = candles[start:idx]
        if not segment:
            continue
        if ev.direction == "long":
            bearish = [c for c in segment if c.c < c.o]
            if not bearish:
                continue
            base = bearish[-1]
            zones.append(Zone("order_block", "long", top=base.h, bottom=base.l, ts=base.ts))
        else:
            bullish = [c for c in segment if c.c > c.o]
            if not bullish:
                continue
            base = bullish[-1]
            zones.append(Zone("order_block", "short", top=base.h, bottom=base.l, ts=base.ts))
    return zones


def promote_breaker_blocks(order_blocks: list[Zone], candles: list[Candle]) -> list[Zone]:
    """An order block that price closes fully through becomes a breaker
    block in the opposite direction."""
    breakers: list[Zone] = []
    for ob in order_blocks:
        for c in candles:
            if c.ts <= ob.ts:
                continue
            if ob.direction == "long" and c.c < ob.bottom:
                ob.mitigated = True
                breakers.append(Zone("breaker_block", "short", top=ob.top, bottom=ob.bottom, ts=c.ts))
                break
            if ob.direction == "short" and c.c > ob.top:
                ob.mitigated = True
                breakers.append(Zone("breaker_block", "long", top=ob.top, bottom=ob.bottom, ts=c.ts))
                break
    return breakers


def find_fair_value_gaps(candles: list[Candle]) -> list[Zone]:
    """Three-candle imbalance: gap between candle[i-1] and candle[i+1]."""
    gaps: list[Zone] = []
    for i in range(1, len(candles) - 1):
        c0, c2 = candles[i - 1], candles[i + 1]
        if c2.l > c0.h:
            gaps.append(Zone("fvg", "long", top=c2.l, bottom=c0.h, ts=candles[i].ts))
        elif c2.h < c0.l:
            gaps.append(Zone("fvg", "short", top=c0.l, bottom=c2.h, ts=candles[i].ts))
    unmitigated = []
    for g in gaps:
        later = [c for c in candles if c.ts > g.ts]
        filled = any(c.l <= g.bottom for c in later) if g.direction == "long" else any(c.h >= g.top for c in later)
        g.mitigated = filled
        unmitigated.append(g)
    return unmitigated


def find_liquidity_pools(swings: list[SwingPoint], tolerance_pct: float = 0.0015) -> list[Zone]:
    """Equal highs / equal lows cluster into resting liquidity pools."""
    pools: list[Zone] = []
    for kind, direction in (("high", "short"), ("low", "long")):
        pts = sorted([s for s in swings if s.kind == kind], key=lambda s: s.price)
        cluster: list[SwingPoint] = []
        for p in pts:
            if cluster and abs(p.price - cluster[-1].price) / max(cluster[-1].price, 1e-9) <= tolerance_pct:
                cluster.append(p)
            else:
                if len(cluster) >= 2:
                    avg = statistics.mean(pt.price for pt in cluster)
                    pools.append(Zone("liquidity_pool", direction, top=avg, bottom=avg, ts=cluster[-1].ts))
                cluster = [p]
        if len(cluster) >= 2:
            avg = statistics.mean(pt.price for pt in cluster)
            pools.append(Zone("liquidity_pool", direction, top=avg, bottom=avg, ts=cluster[-1].ts))
    return pools


def premium_discount_zone(candles: list[Candle], lookback: int = 50) -> tuple[float, float, float]:
    """Returns (equilibrium, range_high, range_low) over lookback window."""
    seg = candles[-lookback:] if len(candles) >= lookback else candles
    if not seg:
        return (0.0, 0.0, 0.0)
    hi, lo = max(c.h for c in seg), min(c.l for c in seg)
    return ((hi + lo) / 2, hi, lo)


def detect_liquidity_sweep(candles: list[Candle], pools: list[Zone], wick_ratio: float = 1.5) -> Optional[Zone]:
    """Recent candle wicks through a liquidity pool then closes back inside —
    classic stop hunt."""
    if not candles:
        return None
    last = candles[-1]
    body = abs(last.c - last.o) or 1e-9
    for pool in pools:
        level = pool.top
        if pool.direction == "short" and last.h > level and last.c < level:
            upper_wick = last.h - max(last.c, last.o)
            if upper_wick / body >= wick_ratio:
                return pool
        if pool.direction == "long" and last.l < level and last.c > level:
            lower_wick = min(last.c, last.o) - last.l
            if lower_wick / body >= wick_ratio:
                return pool
    return None


@dataclass
class StructuralContext:
    swings: list[SwingPoint]
    events: list[StructureEvent]
    order_blocks: list[Zone]
    breaker_blocks: list[Zone]
    fvgs: list[Zone]
    liquidity_pools: list[Zone]
    equilibrium: float
    range_high: float
    range_low: float
    sweep: Optional[Zone]


def build_structural_context(candles: list[Candle]) -> Optional[StructuralContext]:
    if len(candles) < 30:
        return None
    swings = find_swing_points(candles)
    events = detect_structure_events(candles, swings)
    obs = find_order_blocks(candles, events)
    breakers = promote_breaker_blocks(obs, candles)
    fvgs = find_fair_value_gaps(candles)
    pools = find_liquidity_pools(swings)
    eq, hi, lo = premium_discount_zone(candles)
    sweep = detect_liquidity_sweep(candles, pools)
    return StructuralContext(swings, events, obs, breakers, fvgs, pools, eq, hi, lo, sweep)


# ══════════════════════════════════════════════════════════════════════════
# SECTION 6 — REGIME DETECTION
# ══════════════════════════════════════════════════════════════════════════

def detect_regime(candles: list[Candle]) -> Optional[RegimeSnapshot]:
    if len(candles) < 40:
        return None
    closes = [c.c for c in candles]
    adx_v = adx(candles) or 0.0
    atr_v = atr(candles) or 0.0
    price = closes[-1] or 1e-9
    vol_pct = atr_v / price if price else 0.0

    atr_series = []
    for i in range(20, len(candles)):
        seg = candles[max(0, i - 14):i + 1]
        a = atr(seg)
        if a:
            atr_series.append(a / (candles[i].c or 1e-9))
    vol_percentile = 0.5
    if len(atr_series) >= 5:
        rank = sum(1 for v in atr_series if v <= vol_pct)
        vol_percentile = rank / len(atr_series)

    ema_fast = ema_series(closes, 20)
    ema_slow = ema_series(closes, 50) if len(closes) >= 50 else ema_series(closes, len(closes))
    trend_dir = "flat"
    if ema_fast and ema_slow:
        diff = ema_fast[-1] - ema_slow[-1]
        if abs(diff) / price > 0.001:
            trend_dir = "up" if diff > 0 else "down"

    trend_strength = min(adx_v / 50.0, 1.0)

    if trend_strength > 0.5 and vol_percentile > 0.6:
        classification = "expansion"
    elif trend_strength > 0.4:
        classification = "trending"
    elif vol_percentile < 0.25:
        classification = "consolidation"
    elif trend_strength < 0.2 and vol_percentile < 0.6:
        classification = "ranging"
    else:
        classification = "reversal"

    regime_vector = {
        "trend_strength": trend_strength,
        "volatility_percentile": vol_percentile,
        "adx": adx_v,
        "atr_pct": vol_pct,
    }
    return RegimeSnapshot(trend_strength, trend_dir, vol_pct, vol_percentile, classification, regime_vector)


# ══════════════════════════════════════════════════════════════════════════
# SECTION 7 — RISK MANAGEMENT (structure-based, candle-verified only)
# ══════════════════════════════════════════════════════════════════════════

def structure_based_stop(direction: str, ctx: StructuralContext, entry: float, candles: list[Candle],
                          atr_floor: float) -> float:
    """SL beyond the invalidating structural swing, floored by ATR so stops
    are never unrealistically tight. Uses candle highs/lows only."""
    recent_lows = [s.price for s in ctx.swings if s.kind == "low"]
    recent_highs = [s.price for s in ctx.swings if s.kind == "high"]
    if direction == "long":
        structural = min([p for p in recent_lows if p < entry], default=entry - atr_floor * 1.5)
        candidate = structural - atr_floor * 0.25
        return min(candidate, entry - atr_floor * 0.5)
    else:
        structural = max([p for p in recent_highs if p > entry], default=entry + atr_floor * 1.5)
        candidate = structural + atr_floor * 0.25
        return max(candidate, entry + atr_floor * 0.5)


def clip_target_to_liquidity(direction: str, entry: float, raw_target: float, ctx: StructuralContext) -> float:
    """Clip a raw R-multiple target to the nearest real opposing liquidity
    pool / order block edge if one sits inside the raw target's path,
    otherwise keep the raw target."""
    candidates = [z.top if direction == "long" else z.bottom for z in
                  (ctx.liquidity_pools + ctx.order_blocks + ctx.breaker_blocks)]
    if direction == "long":
        in_path = [p for p in candidates if entry < p <= raw_target]
        return min(in_path) if in_path else raw_target
    else:
        in_path = [p for p in candidates if raw_target <= p < entry]
        return max(in_path) if in_path else raw_target


def build_trade_plan(direction: str, entry: float, ctx: StructuralContext, candles: list[Candle],
                      min_rr: float) -> Optional[tuple[float, float, float]]:
    atr_v = atr(candles) or (entry * 0.01)
    sl = structure_based_stop(direction, ctx, entry, candles, atr_v)
    risk = abs(entry - sl)
    if risk <= 0:
        return None
    raw_tp1 = entry + risk * 2.0 if direction == "long" else entry - risk * 2.0
    raw_tp2 = entry + risk * 3.5 if direction == "long" else entry - risk * 3.5
    tp1 = clip_target_to_liquidity(direction, entry, raw_tp1, ctx)
    tp2 = clip_target_to_liquidity(direction, entry, raw_tp2, ctx)
    rr1 = abs(tp1 - entry) / risk
    if rr1 < min_rr:
        return None
    return round(sl, 6), round(tp1, 6), round(tp2, 6)


# ══════════════════════════════════════════════════════════════════════════
# SECTION 8 — SPECIALIZED ENGINES
# ══════════════════════════════════════════════════════════════════════════
# Each engine receives shared context and returns 0 or 1 Signal candidate.

def _confidence(components: dict[str, float]) -> float:
    """Weighted-average confidence in [0,1] from named component scores."""
    if not components:
        return 0.0
    return max(0.0, min(1.0, sum(components.values()) / len(components)))


def _make_signal(engine: str, symbol: str, direction: str, entry: float, plan: tuple[float, float, float],
                  confidence: float, regimes: list[str], confluences: list[str], tf: str, ts: int) -> Signal:
    sl, tp1, tp2 = plan
    risk = abs(entry - sl)
    rr = abs(tp1 - entry) / risk if risk else 0.0
    return Signal(engine, symbol, direction, round(entry, 6), sl, tp1, tp2, round(confidence, 3),
                  round(rr, 2), regimes, confluences, tf, ts)


def engine_smc(sym, candles, ctx, regime, min_rr):
    if not ctx.events:
        return None
    last_event = ctx.events[-1]
    direction = last_event.direction
    entry = candles[-1].c
    ob_align = any(z.direction == direction for z in ctx.order_blocks[-5:])
    conf = _confidence({
        "structure": 0.9 if last_event.kind == "CHoCH" else 0.6,
        "ob_confluence": 0.8 if ob_align else 0.3,
        "regime": 0.7 if regime.classification in ("trending", "reversal") else 0.4,
    })
    plan = build_trade_plan(direction, entry, ctx, candles, min_rr)
    if not plan or conf < 0.4:
        return None
    confl = ["CHoCH" if last_event.kind == "CHoCH" else "BOS"] + (["Order Block confluence"] if ob_align else [])
    return _make_signal("SMC", sym, direction, entry, plan, conf, ["trending", "reversal"], confl, TF_ENTRY, candles[-1].ts)


def engine_trend_continuation(sym, candles, ctx, regime, min_rr):
    if regime.classification != "trending" or regime.trend_direction == "flat":
        return None
    direction = "long" if regime.trend_direction == "up" else "short"
    entry = candles[-1].c
    aligned_events = [e for e in ctx.events[-3:] if e.direction == direction]
    conf = _confidence({
        "trend_strength": regime.trend_strength,
        "structure_alignment": 0.8 if aligned_events else 0.35,
    })
    plan = build_trade_plan(direction, entry, ctx, candles, min_rr)
    if not plan or conf < 0.45:
        return None
    return _make_signal("TrendContinuation", sym, direction, entry, plan, conf, ["trending", "expansion"],
                         ["Trend-aligned structure"], TF_ENTRY, candles[-1].ts)


def engine_breakout(sym, candles, ctx, regime, min_rr):
    if len(candles) < 25:
        return None
    recent = candles[-20:-1]
    hi, lo = max(c.h for c in recent), min(c.l for c in recent)
    last = candles[-1]
    vz = volume_zscore(candles)
    direction = None
    if last.c > hi and vz > 1.0:
        direction = "long"
    elif last.c < lo and vz > 1.0:
        direction = "short"
    if not direction:
        return None
    conf = _confidence({"range_break": 0.75, "volume": min(vz / 3, 1.0), "volatility_expansion":
                         0.7 if regime.classification == "expansion" else 0.4})
    plan = build_trade_plan(direction, last.c, ctx, candles, min_rr)
    if not plan or conf < 0.45:
        return None
    return _make_signal("Breakout", sym, direction, last.c, plan, conf, ["expansion", "trending"],
                         ["Range breakout", "Volume surge"], TF_ENTRY, last.ts)


def engine_pullback(sym, candles, ctx, regime, min_rr):
    if regime.classification not in ("trending", "expansion") or regime.trend_direction == "flat":
        return None
    direction = "long" if regime.trend_direction == "up" else "short"
    eq = ctx.equilibrium
    entry = candles[-1].c
    in_discount = entry < eq if direction == "long" else entry > eq
    if not in_discount:
        return None
    conf = _confidence({"trend": regime.trend_strength, "discount_zone": 0.75})
    plan = build_trade_plan(direction, entry, ctx, candles, min_rr)
    if not plan or conf < 0.4:
        return None
    zone_label = "Discount zone" if direction == "long" else "Premium zone"
    return _make_signal("Pullback", sym, direction, entry, plan, conf, ["trending"], [zone_label], TF_ENTRY, candles[-1].ts)


def engine_liquidity_sweep(sym, candles, ctx, regime, min_rr):
    if not ctx.sweep:
        return None
    direction = ctx.sweep.direction
    entry = candles[-1].c
    conf = _confidence({"sweep_quality": 0.8, "regime": 0.6 if regime.classification == "reversal" else 0.45})
    plan = build_trade_plan(direction, entry, ctx, candles, min_rr)
    if not plan:
        return None
    return _make_signal("LiquiditySweep", sym, direction, entry, plan, conf, ["reversal", "ranging"],
                         ["Liquidity pool swept"], TF_ENTRY, candles[-1].ts)


def engine_order_block(sym, candles, ctx, regime, min_rr):
    last = candles[-1]
    active_obs = [z for z in ctx.order_blocks if not z.mitigated]
    for z in active_obs[-6:]:
        if z.direction == "long" and z.bottom <= last.l <= z.top:
            plan = build_trade_plan("long", last.c, ctx, candles, min_rr)
            if plan:
                conf = _confidence({"ob_reaction": 0.75, "regime": 0.6})
                return _make_signal("OrderBlock", sym, "long", last.c, plan, conf, ["trending", "reversal"],
                                     ["Price reacted at bullish OB"], TF_ENTRY, last.ts)
        if z.direction == "short" and z.bottom <= last.h <= z.top:
            plan = build_trade_plan("short", last.c, ctx, candles, min_rr)
            if plan:
                conf = _confidence({"ob_reaction": 0.75, "regime": 0.6})
                return _make_signal("OrderBlock", sym, "short", last.c, plan, conf, ["trending", "reversal"],
                                     ["Price reacted at bearish OB"], TF_ENTRY, last.ts)
    return None


def engine_breaker_block(sym, candles, ctx, regime, min_rr):
    last = candles[-1]
    for z in ctx.breaker_blocks[-6:]:
        if z.direction == "long" and z.bottom <= last.l <= z.top:
            plan = build_trade_plan("long", last.c, ctx, candles, min_rr)
            if plan:
                conf = _confidence({"breaker_reaction": 0.8, "regime": 0.6})
                return _make_signal("BreakerBlock", sym, "long", last.c, plan, conf, ["trending", "reversal"],
                                     ["Bullish breaker retest"], TF_ENTRY, last.ts)
        if z.direction == "short" and z.bottom <= last.h <= z.top:
            plan = build_trade_plan("short", last.c, ctx, candles, min_rr)
            if plan:
                conf = _confidence({"breaker_reaction": 0.8, "regime": 0.6})
                return _make_signal("BreakerBlock", sym, "short", last.c, plan, conf, ["trending", "reversal"],
                                     ["Bearish breaker retest"], TF_ENTRY, last.ts)
    return None


def engine_fair_value_gap(sym, candles, ctx, regime, min_rr):
    last = candles[-1]
    for z in ctx.fvgs[-6:]:
        if z.mitigated:
            continue
        if z.direction == "long" and z.bottom <= last.l <= z.top:
            plan = build_trade_plan("long", last.c, ctx, candles, min_rr)
            if plan:
                conf = _confidence({"fvg_fill": 0.7, "regime": 0.55})
                return _make_signal("FairValueGap", sym, "long", last.c, plan, conf, ["trending"],
                                     ["Bullish FVG fill"], TF_ENTRY, last.ts)
        if z.direction == "short" and z.bottom <= last.h <= z.top:
            plan = build_trade_plan("short", last.c, ctx, candles, min_rr)
            if plan:
                conf = _confidence({"fvg_fill": 0.7, "regime": 0.55})
                return _make_signal("FairValueGap", sym, "short", last.c, plan, conf, ["trending"],
                                     ["Bearish FVG fill"], TF_ENTRY, last.ts)
    return None


def engine_momentum(sym, candles, ctx, regime, min_rr):
    closes = [c.c for c in candles]
    r = rsi(closes)
    if r is None:
        return None
    vz = volume_zscore(candles)
    direction = None
    if r > 58 and regime.trend_direction == "up" and vz > 0.3:
        direction = "long"
    elif r < 42 and regime.trend_direction == "down" and vz > 0.3:
        direction = "short"
    if not direction:
        return None
    conf = _confidence({"rsi_momentum": min(abs(r - 50) / 30, 1.0), "volume": min(max(vz, 0) / 2, 1.0)})
    plan = build_trade_plan(direction, candles[-1].c, ctx, candles, min_rr)
    if not plan or conf < 0.4:
        return None
    return _make_signal("Momentum", sym, direction, candles[-1].c, plan, conf, ["trending", "expansion"],
                         ["RSI momentum + volume"], TF_ENTRY, candles[-1].ts)


def engine_reversal(sym, candles, ctx, regime, min_rr):
    closes = [c.c for c in candles]
    r = rsi(closes)
    if r is None or regime.classification not in ("reversal", "ranging"):
        return None
    direction = None
    if r < 30 and ctx.sweep and ctx.sweep.direction == "long":
        direction = "long"
    elif r > 70 and ctx.sweep and ctx.sweep.direction == "short":
        direction = "short"
    if not direction:
        return None
    conf = _confidence({"extreme_rsi": min(abs(r - 50) / 30, 1.0), "sweep_confluence": 0.75})
    plan = build_trade_plan(direction, candles[-1].c, ctx, candles, min_rr)
    if not plan:
        return None
    return _make_signal("Reversal", sym, direction, candles[-1].c, plan, conf, ["reversal"],
                         ["RSI extreme + liquidity sweep"], TF_ENTRY, candles[-1].ts)


def engine_mean_reversion(sym, candles, ctx, regime, min_rr):
    if regime.classification != "ranging":
        return None
    closes = [c.c for c in candles]
    m = sma(closes, 20)
    if m is None:
        return None
    last = candles[-1].c
    dev = (last - m) / m
    direction = None
    if dev < -0.015:
        direction = "long"
    elif dev > 0.015:
        direction = "short"
    if not direction:
        return None
    conf = _confidence({"mean_deviation": min(abs(dev) / 0.03, 1.0), "range_regime": 0.7})
    plan = build_trade_plan(direction, last, ctx, candles, min_rr)
    if not plan:
        return None
    return _make_signal("MeanReversion", sym, direction, last, plan, conf, ["ranging", "consolidation"],
                         ["Deviation from SMA20"], TF_ENTRY, candles[-1].ts)


def engine_range_trading(sym, candles, ctx, regime, min_rr):
    if regime.classification not in ("ranging", "consolidation") or len(candles) < 25:
        return None
    seg = candles[-25:]
    hi, lo = max(c.h for c in seg), min(c.l for c in seg)
    last = candles[-1]
    direction = None
    if last.l <= lo * 1.002:
        direction = "long"
    elif last.h >= hi * 0.998:
        direction = "short"
    if not direction:
        return None
    conf = _confidence({"range_edge": 0.65, "regime": 0.6})
    plan = build_trade_plan(direction, last.c, ctx, candles, min_rr)
    if not plan:
        return None
    return _make_signal("RangeTrading", sym, direction, last.c, plan, conf, ["ranging"],
                         ["Range boundary reaction"], TF_ENTRY, last.ts)


def engine_volatility_expansion(sym, candles, ctx, regime, min_rr):
    if regime.classification != "expansion":
        return None
    last = candles[-1]
    direction = "long" if last.c > last.o else "short"
    vz = volume_zscore(candles)
    conf = _confidence({"volatility": min(regime.volatility_percentile, 1.0), "volume": min(max(vz, 0) / 2, 1.0)})
    plan = build_trade_plan(direction, last.c, ctx, candles, min_rr)
    if not plan or conf < 0.4:
        return None
    return _make_signal("VolatilityExpansion", sym, direction, last.c, plan, conf, ["expansion"],
                         ["Volatility expansion breakout candle"], TF_ENTRY, last.ts)


ENGINES = {
    "SMC": engine_smc,
    "TrendContinuation": engine_trend_continuation,
    "Breakout": engine_breakout,
    "Pullback": engine_pullback,
    "LiquiditySweep": engine_liquidity_sweep,
    "OrderBlock": engine_order_block,
    "BreakerBlock": engine_breaker_block,
    "FairValueGap": engine_fair_value_gap,
    "Momentum": engine_momentum,
    "Reversal": engine_reversal,
    "MeanReversion": engine_mean_reversion,
    "RangeTrading": engine_range_trading,
    "VolatilityExpansion": engine_volatility_expansion,
}


# ══════════════════════════════════════════════════════════════════════════
# SECTION 9 — CENTRAL DECISION ENGINE (adaptive weighting, EV ranking)
# ══════════════════════════════════════════════════════════════════════════

def compute_ev(signal: Signal, engine_weight: float, mtf_alignment: float, regime_fit: float) -> float:
    base_ev = signal.confidence * signal.expected_rr - (1 - signal.confidence) * 1.0
    return base_ev * engine_weight * (0.5 + 0.5 * mtf_alignment) * (0.5 + 0.5 * regime_fit)


def mtf_alignment_score(direction: str, htf_regime: Optional[RegimeSnapshot]) -> float:
    if htf_regime is None:
        return 0.5
    if htf_regime.trend_direction == "flat":
        return 0.5
    aligned = (direction == "long" and htf_regime.trend_direction == "up") or \
              (direction == "short" and htf_regime.trend_direction == "down")
    return 0.9 if aligned else 0.25


def regime_fit_score(signal: Signal, regime: RegimeSnapshot) -> float:
    return 1.0 if regime.classification in signal.regime_suitability else 0.5


def rank_candidates(candidates: list[Signal], htf_regime: Optional[RegimeSnapshot], regime: RegimeSnapshot,
                     engine_weights: dict[str, float]) -> list[Signal]:
    for s in candidates:
        w = engine_weights.get(s.engine, 1.0)
        mtf = mtf_alignment_score(s.direction, htf_regime)
        fit = regime_fit_score(s, regime)
        s.ev = compute_ev(s, w, mtf, fit)
        s.score = s.ev
    return sorted(candidates, key=lambda s: s.score, reverse=True)


def deduplicate_by_symbol(ranked: list[Signal]) -> list[Signal]:
    seen: set[str] = set()
    out: list[Signal] = []
    for s in ranked:
        if s.symbol in seen:
            continue
        seen.add(s.symbol)
        out.append(s)
    return out


def adaptive_ev_threshold(state: StateStore) -> float:
    """Frequency governor: nudge threshold toward the 5-10/day band using a
    slow EMA of realized daily counts."""
    counts = state.data.get("daily_counts", {})
    recent = list(counts.values())[-7:]
    threshold = state.data.get("ev_threshold", 0.35)
    if not recent:
        return threshold
    avg = statistics.mean(recent)
    if avg < TARGET_SIGNALS_PER_DAY_MIN:
        threshold = max(0.05, threshold - 0.01)
    elif avg > TARGET_SIGNALS_PER_DAY_MAX:
        threshold = min(1.2, threshold + 0.01)
    state.data["ev_threshold"] = round(threshold, 4)
    return threshold


# ══════════════════════════════════════════════════════════════════════════
# SECTION 10 — CONTINUOUS LEARNING SYSTEM
# ══════════════════════════════════════════════════════════════════════════

def _bounded_ema(old: float, new_obs: float, alpha: float, lo: float, hi: float) -> float:
    val = old * (1 - alpha) + new_obs * alpha
    return max(lo, min(hi, val))


def update_learning_from_closed_trade(state: StateStore, trade: dict) -> None:
    engine = trade["engine"]
    stats = state.data["engine_stats"].setdefault(engine, {
        "wins": 0, "losses": 0, "sum_rr": 0.0, "n": 0, "sum_conf_error": 0.0
    })
    won = trade["outcome"] in ("tp1", "tp2")
    stats["n"] += 1
    if won:
        stats["wins"] += 1
        stats["sum_rr"] += trade.get("realized_rr", 0.0)
    else:
        stats["losses"] += 1

    predicted_conf = trade.get("confidence", 0.5)
    realized = 1.0 if won else 0.0
    stats["sum_conf_error"] += abs(predicted_conf - realized)

    win_rate = stats["wins"] / stats["n"] if stats["n"] else 0.5
    weights = state.data["engine_weights"]
    old_w = weights.get(engine, 1.0)
    target_w = 0.5 + win_rate  # win_rate 0..1 -> target weight 0.5..1.5
    weights[engine] = round(_bounded_ema(old_w, target_w, alpha=0.08, lo=0.3, hi=2.0), 4)

    bucket = f"{int(predicted_conf * 10) * 10}-{int(predicted_conf * 10) * 10 + 10}"
    cal = state.data["confidence_calibration"].setdefault(bucket, {"predicted_sum": 0.0, "realized_sum": 0.0, "n": 0})
    cal["predicted_sum"] += predicted_conf
    cal["realized_sum"] += realized
    cal["n"] += 1


def daily_stats_summary(state: StateStore) -> dict[str, Any]:
    history = state.data.get("signal_history", [])
    today = _utcnow().strftime("%Y-%m-%d")
    todays = [t for t in history if str(t.get("closed_at", "")).startswith(today)]
    wins = sum(1 for t in todays if t.get("outcome") in ("tp1", "tp2"))
    losses = sum(1 for t in todays if t.get("outcome") == "sl")
    total = wins + losses
    win_rate = (wins / total * 100) if total else 0.0
    gross_win = sum(t.get("realized_rr", 0) for t in todays if t.get("outcome") in ("tp1", "tp2"))
    gross_loss = sum(1 for t in todays if t.get("outcome") == "sl")
    profit_factor = (gross_win / gross_loss) if gross_loss else (gross_win if gross_win else 0.0)
    avg_rr = statistics.mean([t.get("realized_rr", 0) for t in todays]) if todays else 0.0
    hold_times = [t.get("hold_minutes", 0) for t in todays if t.get("hold_minutes")]
    avg_hold = statistics.mean(hold_times) if hold_times else 0.0

    by_regime: dict[str, dict] = {}
    by_engine: dict[str, dict] = {}
    for t in todays:
        r = t.get("regime", "unknown")
        e = t.get("engine", "unknown")
        by_regime.setdefault(r, {"wins": 0, "losses": 0})
        by_engine.setdefault(e, {"wins": 0, "losses": 0})
        key = "wins" if t.get("outcome") in ("tp1", "tp2") else "losses"
        by_regime[r][key] += 1
        by_engine[e][key] += 1

    best = max(todays, key=lambda t: t.get("realized_rr", -999), default=None)
    worst = min(todays, key=lambda t: t.get("realized_rr", 999), default=None)

    cal = state.data.get("confidence_calibration", {})
    cal_errors = []
    for b, v in cal.items():
        if v["n"]:
            cal_errors.append(abs(v["predicted_sum"] / v["n"] - v["realized_sum"] / v["n"]))
    cal_accuracy = round(100 * (1 - statistics.mean(cal_errors)), 1) if cal_errors else None

    return {
        "total_signals": total, "wins": wins, "losses": losses, "win_rate": round(win_rate, 1),
        "profit_factor": round(profit_factor, 2), "avg_rr": round(avg_rr, 2), "avg_hold_minutes": round(avg_hold, 1),
        "by_regime": by_regime, "by_engine": by_engine, "best": best, "worst": worst,
        "confidence_calibration_accuracy": cal_accuracy,
    }


# ══════════════════════════════════════════════════════════════════════════
# SECTION 11 — TELEGRAM INTEGRATION
# ══════════════════════════════════════════════════════════════════════════

TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"
# The actual set of emoji Telegram allows as message reactions (transcribed
# from the reaction picker, not guessed) -- Telegram silently rejects
# setMessageReaction calls for anything outside this fixed list, so
# react() below validates against it before ever hitting the API.
VALID_REACTIONS = {
    "❤️", "👍", "👎", "🔥", "🥰", "👏", "😁", "🤔",
    "🤯", "😱", "🤬", "😢", "🎉", "🤩", "🤮", "💩",
    "🙏", "👌", "🕊️", "🤡", "🤲", "🤭", "😏", "❤️‍🔥",
    "🌚", "🌭", "💯", "😆", "⚡", "🍌", "🏆", "💔",
    "😑", "😐", "🍓", "🍾", "💋", "🖕", "😈", "😴",
    "😭", "🤓", "👻", "🧑‍💻", "👀", "🎃", "🙈", "😇",
    "😧", "🤝", "✍️", "🫰", "🎅", "🎄", "⛄", "💅",
    "😜", "🗿", "🆒", "💘", "🙉", "🦄", "😘", "💊",
    "🙊", "😎", "👾", "🤷‍♂️", "🤷‍♀️", "😡",
}


class TelegramNotifier:
    def __init__(self, token: Optional[str], chat_id: Optional[str]):
        self.token = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id)
        self._session = requests.Session()
        if not self.enabled:
            log.warning("Telegram credentials not configured; notifications disabled.")

    def _call(self, method: str, payload: dict) -> Optional[dict]:
        if not self.enabled:
            return None
        url = TELEGRAM_API.format(token=self.token, method=method)
        try:
            resp = self._session.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            log.error("Telegram API call '%s' failed: %s", method, e)
            return None

    def send_message(self, text: str, reply_to: Optional[int] = None) -> Optional[int]:
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
        if reply_to:
            payload["reply_to_message_id"] = reply_to
        result = self._call("sendMessage", payload)
        if result and result.get("ok"):
            return result["result"]["message_id"]
        return None

    def react(self, message_id: int, emoji: str) -> None:
        if emoji not in VALID_REACTIONS:
            emoji = "👍"
        self._call("setMessageReaction", {
            "chat_id": self.chat_id, "message_id": message_id,
            "reaction": [{"type": "emoji", "emoji": emoji}],
        })

    def format_signal(self, s: Signal) -> str:
        arrow = "🟢 LONG" if s.direction == "long" else "🔴 SHORT"
        confl = ", ".join(s.confluences) if s.confluences else "—"
        return (
            f"<b>{ENGINE_NAME} v{ENGINE_VERSION}</b>\n"
            f"<b>{s.symbol}-PERP</b> | {arrow} | {s.engine}\n\n"
            f"<b>Entry:</b> <code>{s.entry}</code>\n"
            f"<b>SL:</b> <code>{s.sl}</code>\n"
            f"<b>TP1:</b> <code>{s.tp1}</code>\n"
            f"<b>TP2:</b> <code>{s.tp2}</code>\n\n"
            f"Confidence: {s.confidence * 100:.0f}% | Expected RR: {s.expected_rr:.2f}\n"
            f"Timeframe: {s.timeframe}\n"
            f"Confluences: {confl}\n"
            f"Status: <b>Activated</b>"
        )

    def send_signal(self, s: Signal) -> Optional[int]:
        # No reaction on the original post itself -- reactions mark *outcomes*
        # (TP1 / win / breakeven / loss), same as AXIS ENGINE's react_telegram.
        return self.send_message(self.format_signal(s))

    def send_status_update(self, original_message_id: int, symbol: str, status: str) -> None:
        # Mirrors AXIS ENGINE's react_telegram emoji scheme: 🔥 on TP1,
        # 👍 on a win or a breakeven stop-out (tp1 already banked), 👎 on a
        # straight loss. "Closed"/"Cancelled" are kept for statuses this
        # engine may use elsewhere.
        emoji_map = {"TP1": "🔥", "TP2": "👍", "SL": "👎", "Break-even": "👍",
                     "Closed": "🏆", "Cancelled": "🤷‍♂️"}
        text = f"<b>{symbol}-PERP</b> update: <b>{status}</b>"
        mid = self.send_message(text, reply_to=original_message_id)
        if mid and status in emoji_map:
            self.react(mid, emoji_map[status])

    def send_daily_summary(self, stats: dict) -> None:
        best = stats.get("best")
        worst = stats.get("worst")
        best_str = f"{best['symbol']} ({best.get('realized_rr', 0):.2f}R)" if best else "—"
        worst_str = f"{worst['symbol']} ({worst.get('realized_rr', 0):.2f}R)" if worst else "—"
        engine_lines = "\n".join(
            f"  • {e}: {v['wins']}W / {v['losses']}L" for e, v in stats.get("by_engine", {}).items()
        ) or "  • No closed trades"
        regime_lines = "\n".join(
            f"  • {r}: {v['wins']}W / {v['losses']}L" for r, v in stats.get("by_regime", {}).items()
        ) or "  • No closed trades"
        text = (
            f"<b>{ENGINE_NAME} v{ENGINE_VERSION} — Daily Summary</b>\n\n"
            f"Total Signals: {stats['total_signals']}\n"
            f"Wins/Losses: {stats['wins']}/{stats['losses']}\n"
            f"Win Rate: {stats['win_rate']}%\n"
            f"Profit Factor: {stats['profit_factor']}\n"
            f"Avg RR: {stats['avg_rr']}\n"
            f"Avg Hold: {stats['avg_hold_minutes']} min\n\n"
            f"<b>By Regime:</b>\n{regime_lines}\n\n"
            f"<b>By Engine:</b>\n{engine_lines}\n\n"
            f"Best Setup: {best_str}\n"
            f"Worst Setup: {worst_str}\n"
            f"Confidence Calibration Accuracy: {stats.get('confidence_calibration_accuracy', 'N/A')}%\n"
        )
        self.send_message(text)


# ══════════════════════════════════════════════════════════════════════════
# SECTION 12 — ORCHESTRATION
# ══════════════════════════════════════════════════════════════════════════

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def evaluate_open_signals(state: StateStore, client: HyperliquidClient, notifier: TelegramNotifier) -> None:
    active = state.data.get("active_signals", {})
    to_close = []
    for sig_id, rec in active.items():
        symbol = rec["symbol"]
        candles = client.get_candles(symbol, TF_ENTRY, lookback=20)
        if not candles:
            continue
        last = candles[-1]
        direction = rec["direction"]
        outcome = None
        if direction == "long":
            if last.l <= rec["sl"]:
                outcome = "sl"
            elif last.h >= rec["tp2"]:
                outcome = "tp2"
            elif last.h >= rec["tp1"] and not rec.get("tp1_hit"):
                rec["tp1_hit"] = True
                if notifier.enabled and rec.get("message_id"):
                    notifier.send_status_update(rec["message_id"], symbol, "TP1")
        else:
            if last.h >= rec["sl"]:
                outcome = "sl"
            elif last.l <= rec["tp2"]:
                outcome = "tp2"
            elif last.l <= rec["tp1"] and not rec.get("tp1_hit"):
                rec["tp1_hit"] = True
                if notifier.enabled and rec.get("message_id"):
                    notifier.send_status_update(rec["message_id"], symbol, "TP1")

        if outcome:
            entry = rec["entry"]
            risk = abs(entry - rec["sl"]) or 1e-9
            realized_rr = (abs(rec["tp1" if outcome == "tp1" else "tp2"] - entry) / risk) if outcome != "sl" else -1.0
            opened_at = datetime.fromisoformat(rec["opened_at"])
            hold_minutes = (_utcnow() - opened_at).total_seconds() / 60.0
            closed_trade = {**rec, "outcome": outcome, "realized_rr": realized_rr,
                             "closed_at": _utcnow().isoformat(), "hold_minutes": round(hold_minutes, 1)}
            state.data["signal_history"].append(closed_trade)
            state.data["signal_history"] = state.data["signal_history"][-2000:]
            update_learning_from_closed_trade(state, closed_trade)
            if notifier.enabled and rec.get("message_id"):
                if outcome == "tp2":
                    close_status = "TP2"
                elif rec.get("tp1_hit"):
                    close_status = "Break-even"  # TP1 already banked before SL was hit
                else:
                    close_status = "SL"
                notifier.send_status_update(rec["message_id"], symbol, close_status)
            to_close.append(sig_id)

    for sig_id in to_close:
        del state.data["active_signals"][sig_id]


def scan_once(state: StateStore, client: HyperliquidClient, notifier: TelegramNotifier) -> list[Signal]:
    evaluate_open_signals(state, client, notifier)

    all_candidates: list[Signal] = []
    per_symbol_regime: dict[str, RegimeSnapshot] = {}
    per_symbol_htf_regime: dict[str, Optional[RegimeSnapshot]] = {}

    for symbol in WATCHLIST:
        cooldown_until = state.data["cooldowns"].get(symbol)
        if cooldown_until and _utcnow().timestamp() < cooldown_until:
            continue

        entry_candles = client.get_candles(symbol, TF_ENTRY, lookback=200)
        confirm_candles = client.get_candles(symbol, TF_CONFIRM, lookback=200)
        htf_candles = client.get_candles(symbol, TF_HTF_STRUCT, lookback=200)

        if len(entry_candles) < 40:
            continue

        ctx = build_structural_context(entry_candles)
        regime = detect_regime(confirm_candles or entry_candles)
        htf_regime = detect_regime(htf_candles) if htf_candles else None
        if ctx is None or regime is None:
            continue

        per_symbol_regime[symbol] = regime
        per_symbol_htf_regime[symbol] = htf_regime

        for engine_fn in ENGINES.values():
            try:
                candidate = engine_fn(symbol, entry_candles, ctx, regime, MIN_RR)
            except Exception as e:  # noqa: BLE001 — never let one engine crash the scan
                log.error("Engine %s raised on %s: %s", engine_fn.__name__, symbol, e)
                continue
            if candidate:
                all_candidates.append(candidate)

    if not all_candidates:
        return []

    threshold = adaptive_ev_threshold(state)
    ranked_all: list[Signal] = []
    for symbol in {c.symbol for c in all_candidates}:
        sym_candidates = [c for c in all_candidates if c.symbol == symbol]
        ranked = rank_candidates(sym_candidates, per_symbol_htf_regime.get(symbol), per_symbol_regime[symbol],
                                  state.data["engine_weights"])
        ranked_all.extend(ranked)

    ranked_all.sort(key=lambda s: s.score, reverse=True)
    ranked_all = deduplicate_by_symbol(ranked_all)
    qualified = [s for s in ranked_all if s.score >= threshold]
    accepted = qualified[:MAX_CANDIDATES_PER_SCAN]
    return accepted


def persist_and_notify(state: StateStore, notifier: TelegramNotifier, accepted: list[Signal]) -> None:
    today = _utcnow().strftime("%Y-%m-%d")
    for s in accepted:
        message_id = notifier.send_signal(s) if notifier.enabled else None
        sig_id = f"{s.symbol}-{s.engine}-{s.ts}"
        state.data["active_signals"][sig_id] = {
            **asdict(s), "message_id": message_id, "opened_at": _utcnow().isoformat(), "tp1_hit": False,
        }
        state.data["cooldowns"][s.symbol] = (_utcnow() + timedelta(hours=2)).timestamp()
        state.data["daily_counts"][today] = state.data["daily_counts"].get(today, 0) + 1
        state.data["run_stats"]["total_signals"] += 1

    state.data["run_stats"]["total_runs"] += 1
    state.data["run_stats"]["last_run_ts"] = _utcnow().isoformat()

    old_days = sorted(state.data["daily_counts"].keys())
    if len(old_days) > 30:
        for d in old_days[:-30]:
            del state.data["daily_counts"][d]


def maybe_send_daily_summary(state: StateStore, notifier: TelegramNotifier) -> None:
    now = _utcnow()
    if now.hour != 8:
        return
    today_str = now.strftime("%Y-%m-%d")
    if state.data.get("last_daily_summary_date") == today_str:
        return
    stats = daily_stats_summary(state)
    if notifier.enabled:
        notifier.send_daily_summary(stats)
    state.data["last_daily_summary_date"] = today_str


def run() -> int:
    log.info("%s v%s starting scan.", ENGINE_NAME, ENGINE_VERSION)
    state = StateStore(STATE_PATH)
    client = HyperliquidClient(HL_API_URL)
    notifier = TelegramNotifier(TG_BOT_TOKEN, TG_CHAT_ID)

    try:
        accepted = scan_once(state, client, notifier)
        persist_and_notify(state, notifier, accepted)
        maybe_send_daily_summary(state, notifier)
        log.info("Scan complete. %d new signal(s) accepted.", len(accepted))
        return 0
    except Exception as e:  # noqa: BLE001 — top-level guard for unattended runs
        log.exception("Unhandled error during scan: %s", e)
        return 1
    finally:
        state.save()


if __name__ == "__main__":
    sys.exit(run())
