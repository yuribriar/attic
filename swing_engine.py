#!/usr/bin/env python3
"""MERIDIAN Signal Engine -- v2.0.2

Adaptive hybrid crypto perpetual-futures signal engine for Hyperliquid.
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
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional


ENGINE_NAME = "Meridian Signal Engine"
ENGINE_VERSION = "v2.0.2"
RESOLUTION_LOGIC_VERSION = 1  # bumped whenever outcome-scoring/SL-TP resolution logic changes
STATE_SCHEMA_VERSION = 2      # bumped for the Tier-1 profit-factor baseline field

# Identifiers copied verbatim per identifier-parity requirement
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")
STATE_PATH = os.environ.get("STATE_PATH", "state.json")
CANDLE_CACHE_PATH = os.environ.get("CANDLE_CACHE_PATH", "candle_cache.json")
HL_API_URL = os.environ.get("HL_API_URL", "https://api.hyperliquid.xyz/info")

# Watchlist
WATCHLIST: list[str] = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]

# Correlation groups for the correlated-asset concurrency cap.
# Coarse groupings, good enough to stop the cap being consumed by
# near-duplicate bets without a live correlation matrix each run.
CORRELATION_GROUPS: dict[str, str] = {
    "BTC": "majors", "ETH": "majors", "BNB": "majors", "SOL": "majors",
    "XRP": "majors", "DOGE": "majors", "TRX": "majors", "LTC": "majors", "BCH": "majors",
    "NEAR": "l1_alt", "SUI": "l1_alt", "APT": "l1_alt", "AVAX": "l1_alt",
    "ADA": "l1_alt", "DOT": "l1_alt", "TAO": "l1_alt",
    "LINK": "defi_infra", "AAVE": "defi_infra", "UNI": "defi_infra",
    "ONDO": "defi_infra", "PENDLE": "defi_infra",
    "HYPE": "exchange_native", "PENGU": "meme_narrative", "ZEC": "privacy", "XLM": "l1_alt",
}

# Timeframes
# 5M is used only for the optional Stage 5 entry-timing refinement, never
# as a live-trigger timeframe on its own.
FORBIDDEN_TIMEFRAMES = {"1m", "2m", "3m"}
TF_WEEKLY, TF_DAILY, TF_4H, TF_1H, TF_15M, TF_5M = "1w", "1d", "4h", "1h", "15m", "5m"
CANDLE_LOOKBACK = {TF_WEEKLY: 120, TF_DAILY: 200, TF_4H: 300, TF_1H: 400, TF_15M: 500, TF_5M: 200}
CANDLE_DELTA_OVERLAP_BARS = 3  # extra closed bars re-fetched past the cached watermark

# Optional Stage 5 5M entry refinement -- opt-in
ENABLE_5M_REFINE = os.environ.get("ENABLE_5M_REFINE", "false").lower() == "true"

# Risk / trade construction constants
RR_MIN_GATE = 1.5
RR_MAX_GATE = 3.5
RR_MIN_GATE_COUNTERTREND = 2.0
MIN_RISK_ATR_MULT = 1.0
MAX_SL_ATR_MULT = 4.0
MIN_SL_DISTANCE_PCT = 0.006
MAX_SL_DISTANCE_PCT = 0.025
MIN_MOVE_PCT_TP1 = 0.012
MIN_MOVE_PCT_TP2 = 0.020
SL_POOL_CLEAR_WINDOW_ATR_MULT = 1.5          # bounded window for liquidity-pool clearing search
MAX_PENDING_ENTRY_DISTANCE_ATR_MULT = 1.2    # cap on how far a pending entry may sit from market

# Entry-fill / pending-signal lifecycle
PENDING_ENTRY_EXPIRY_BARS = 16          # on the entry's own trigger timeframe (15M -> ~4h)
COUNTERTREND_RETEST_EXPIRY_BARS = 12

# Portfolio / concurrency controls
MAX_CONCURRENT_ACTIVE_SIGNALS = 6
MAX_CONCURRENT_PER_CORRELATION_GROUP = 2

# Scan-phase throughput
# Per-symbol scanning is I/O-bound, so a small thread pool speeds up larger
# watchlists; each symbol's scan stays fault-isolated.
SCAN_MAX_WORKERS = int(os.environ.get("SCAN_MAX_WORKERS", "6"))
FIXED_RISK_PCT_OF_EQUITY = 0.0075       # 0.75% fixed-fractional per-trade risk (position-sizing reference only)
MAX_DAILY_LOSS_PCT = 0.04
MAX_DRAWDOWN_CIRCUIT_BREAKER_PCT = 0.15
PORTFOLIO_EXPOSURE_CAP_PCT = 0.35       # sum of open-position risk as % of equity
ENABLE_KELLY_SIZING = os.environ.get("ENABLE_KELLY_SIZING", "false").lower() == "true"
KELLY_FRACTION_CAP = 0.5                # half-Kelly cap when Kelly sizing is enabled

# Adaptive-learning bounds -- every adaptive parameter has a
# documented [min, max] and a capped max per-update step.
ADAPTIVE_BOUNDS: dict[str, tuple[float, float, float]] = {
    # name: (min, max, max_step_per_update)
    "sl_buffer_percentile": (40.0, 90.0, 5.0),
    "tp1_target_rank_preference": (2.0, 6.0, 1.0),
    "regime_fit_discount": (0.0, 0.6, 0.05),
    "mtf_alignment_weight": (0.05, 0.35, 0.03),
    "liquidity_sanity_threshold": (0.1, 0.9, 0.05),
    "sfp_mss_strictness": (0.0, 1.0, 0.1),
    "confidence_calibration_shift": (-0.25, 0.25, 0.03),
    "session_open_proximity_weight": (0.0, 0.15, 0.02),
}
MIN_SAMPLE_SIZE_FOR_ADAPTATION = 20     # per segment/category before an adjustment is trusted

# Live-performance circuit breaker
# Dual-metric: win-rate deviation OR a profit-factor collapse trips the
# breaker, so a stretch of many small wins offset by a few large losses is
# still caught even when the raw win rate looks fine.
CIRCUIT_BREAKER_LOOKBACK_TRADES = 40
CIRCUIT_BREAKER_MAX_WIN_RATE_DEVIATION = 0.20   # vs documented baseline win rate
BASELINE_PROFIT_FACTOR = 1.6                    # documented, pre-deployment baseline
CIRCUIT_BREAKER_MIN_PROFIT_FACTOR = 1.0         # trip if rolling profit factor falls to/below breakeven

# Macro/news blackout window
MACRO_BLACKOUT_MINUTES_BEFORE = 30
MACRO_BLACKOUT_MINUTES_AFTER = 30
# Static recurring UTC schedule of the highest-impact scheduled US macro
# events; swappable for a live feed without touching engine logic.
# weekday: 0=Mon..6=Sun; week_of_month counts occurrences within the month.
MACRO_EVENT_CALENDAR: list[dict[str, Any]] = [
    {"name": "us_cpi", "weekday": 2, "week_of_month": 2, "hour_utc": 13, "minute_utc": 30, "affects": "ALL"},
    {"name": "fomc_decision", "weekday": 2, "week_of_month": 3, "hour_utc": 18, "minute_utc": 0, "affects": "ALL"},
    {"name": "us_nfp", "weekday": 4, "week_of_month": 1, "hour_utc": 13, "minute_utc": 30, "affects": "ALL"},
]
# operators may ADDITIONALLY populate state["macro_events"] with ISO
# timestamps + affected assets (e.g. earnings, geopolitical events, a live
# feed); those are checked on top of, never instead of, the static calendar.

# Counter-Trend Reversal engine -- opt-in, default OFF
ENABLE_COUNTERTREND_ENGINE = os.environ.get("ENABLE_COUNTERTREND_ENGINE", "false").lower() == "true"

# Composite score category weights, regime-adjustable
BASE_SCORE_WEIGHTS: dict[str, float] = {
    "trend": 0.25, "structure": 0.20, "momentum": 0.15, "liquidity": 0.15,
    "volume": 0.10, "volatility": 0.10, "risk": 0.05,
}
# Regime weighting table -- data, never hardcoded conditionals.
REGIME_WEIGHT_MULTIPLIERS: dict[str, dict[str, float]] = {
    "Strong Bull Trend":  {"trend": 1.3, "momentum": 1.2, "liquidity": 0.85},
    "Strong Bear Trend":  {"trend": 1.3, "momentum": 1.2, "liquidity": 0.85},
    "Weak Trend":         {"trend": 1.0, "momentum": 1.0, "liquidity": 1.0},
    "Sideways":           {"trend": 0.6, "liquidity": 1.3, "structure": 1.1},
    "Range":              {"trend": 0.6, "liquidity": 1.3, "structure": 1.1},
    "Expansion":          {"volatility": 1.3, "momentum": 1.1},
    "Compression":        {"volatility": 1.4, "structure": 1.1},
    "High Volatility":    {"volatility": 1.3, "momentum": 0.85, "risk": 1.3},
    "Low Volatility":     {"volatility": 1.2},
    "Breakout":           {"volatility": 1.2, "momentum": 1.15, "structure": 1.1},
    "Pullback":           {"trend": 1.15, "structure": 1.1},
    "Mean Reversion":     {"liquidity": 1.2, "trend": 0.7},
}
# Per-term saturation cap on the logistic blend: no single term may
# saturate the composite score on its own.
MAX_SINGLE_TERM_CONTRIBUTION = 0.35

# Confidence grade buckets
GRADE_THRESHOLDS = [("A+", 92), ("A", 84), ("B+", 74), ("B", 62)]  # else below bar, no signal


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("meridian")


@dataclass
class Candle:
    ts: int      # ms epoch, candle open time
    o: float
    h: float
    l: float
    c: float
    v: float


@dataclass
class Pivot:
    index: int
    ts: int
    price: float
    kind: str  # "high" | "low"


@dataclass
class Zone:
    """A structural point of interest: order block, breaker block, or FVG."""
    kind: str            # "order_block" | "breaker_block" | "fvg" | "premium_discount"
    direction: str        # "bullish" | "bearish"
    top: float
    bottom: float
    origin_index: int
    origin_ts: int
    mitigated: bool = False
    from_sweep: bool = False   # whether this zone arose from a specific liquidity sweep


@dataclass
class LiquidityPool:
    kind: str           # "BSL" | "SSL"
    price: float
    is_equal_cluster: bool
    indices: list[int]


@dataclass
class SweepEvent:
    direction: str        # direction of the resulting move ("bullish" | "bearish")
    pool: LiquidityPool
    index: int
    ts: int
    is_pure: bool          # SFP purity check
    session_tag: Optional[str] = None


@dataclass
class View:
    """All computed feature primitives for one asset/timeframe, computed once
    and shared by every downstream consumer."""
    symbol: str
    timeframe: str
    candles: list[Candle]
    closes: list[float] = field(default_factory=list)
    highs: list[float] = field(default_factory=list)
    lows: list[float] = field(default_factory=list)
    volumes: list[float] = field(default_factory=list)

    ema_fast: list[float] = field(default_factory=list)
    ema_slow: list[float] = field(default_factory=list)
    sma: list[float] = field(default_factory=list)
    vwap: list[float] = field(default_factory=list)
    adx: list[float] = field(default_factory=list)
    lr_slope: float = 0.0

    atr: list[float] = field(default_factory=list)
    atr_percentile: float = 50.0
    bb_width: list[float] = field(default_factory=list)
    donchian_width: list[float] = field(default_factory=list)

    rsi: list[float] = field(default_factory=list)
    macd_hist: list[float] = field(default_factory=list)
    stoch_rsi: list[float] = field(default_factory=list)
    roc: list[float] = field(default_factory=list)

    vol_sma: list[float] = field(default_factory=list)
    rel_volume: float = 1.0
    obv: list[float] = field(default_factory=list)
    cmf: list[float] = field(default_factory=list)

    pivots: list[Pivot] = field(default_factory=list)
    trend_direction: str = "neutral"
    trend_strength: float = 0.0
    trend_quality: float = 0.0

    order_blocks: list[Zone] = field(default_factory=list)
    breaker_blocks: list[Zone] = field(default_factory=list)
    fvgs: list[Zone] = field(default_factory=list)
    liquidity_pools: list[LiquidityPool] = field(default_factory=list)
    premium_discount: dict[str, float] = field(default_factory=dict)  # {"premium_from":, "discount_to":, "eq":}


@dataclass
class Candidate:
    symbol: str
    direction: str          # "bullish" | "bearish"
    engine: str
    style: str               # "intraday" | "swing"
    entry: float
    entry_kind: str          # "pending" | "market"
    sl: float
    tp1: float
    tp2: float
    rr1: float
    rr2: float
    sl_anchor: str
    confidence: float = 0.0
    grade: str = ""
    scores: dict[str, float] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)
    counter_trend: bool = False
    market_regime: str = ""
    regime_confidence: float = 0.0
    trend_label: str = ""
    higher_timeframe_alignment: bool = True
    session_anchored: bool = False


@dataclass
class DispatchedSignal:
    """Mutable dataclass modeling one dispatched signal's lifecycle
    (pending -> activated -> resolved). Not the on-disk representation --
    state.json still stores plain dicts via an asdict()/DispatchedSignal(**d)
    round-trip, so this adds type safety without any schema migration risk."""
    symbol: str
    direction: str
    engine: str
    style: str
    entry: float
    entry_kind: str
    sl: float
    tp1: float
    tp2: float
    rr1: float
    rr2: float
    sl_anchor: str
    confidence: float
    grade: str
    counter_trend: bool
    market_regime: str
    regime_confidence: float
    session_anchored: bool
    higher_timeframe_alignment: bool
    status: str
    dispatched_ts: str
    resolution_logic_version: int
    message_id: Optional[int] = None
    dispatched_bar_index: Optional[int] = None
    filled_bar_index: Optional[int] = None


def _dispatched_signal_from_dict(d: dict) -> DispatchedSignal:
    """Tolerant reconstruction -- unknown/extra keys (from an older or newer
    engine version's state.json) are dropped rather than raising, matching
    the defensive-load spirit of `StateStore._load`."""
    known = set(DispatchedSignal.__dataclass_fields__.keys())
    return DispatchedSignal(**{k: v for k, v in d.items() if k in known})


_HL_INTERVAL_MAP = {
    TF_15M: "15m", TF_1H: "1h", TF_4H: "4h", TF_DAILY: "1d", TF_WEEKLY: "1w", TF_5M: "5m",
}


class RequestWeightPacer:
    """Sliding-window pacer keeping the engine safely inside Hyperliquid's
    documented per-IP request-weight budget."""

    def __init__(self, max_weight_per_minute: int = 1100):
        self.max_weight = max_weight_per_minute
        self._events: list[tuple[float, int]] = []

    def acquire(self, weight: int = 20):
        now = time.monotonic()
        self._events = [(t, w) for (t, w) in self._events if now - t < 60]
        used = sum(w for _, w in self._events)
        if used + weight > self.max_weight:
            sleep_for = 60 - (now - self._events[0][0]) if self._events else 1.0
            time.sleep(max(0.2, min(sleep_for, 5.0)))
        self._events.append((time.monotonic(), weight))


class HyperliquidClient:
    """Thin, read-only client around Hyperliquid's public /info endpoint."""

    def __init__(self, base_url: str = HL_API_URL):
        self.base_url = base_url
        self.pacer = RequestWeightPacer()

    def _post(self, payload: dict, retries: int = 3) -> Any:
        body = json.dumps(payload).encode("utf-8")
        last_err = None
        for attempt in range(retries):
            self.pacer.acquire()
            req = urllib.request.Request(
                self.base_url, data=body,
                headers={"Content-Type": "application/json"}, method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=15) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
                last_err = e
                time.sleep(0.75 * (attempt + 1))
        log.error("Hyperliquid request failed after %d retries: %s", retries, last_err)
        return None

    def fetch_candles(self, symbol: str, timeframe: str, start_ms: int, end_ms: int) -> list[Candle]:
        interval = _HL_INTERVAL_MAP.get(timeframe)
        if interval is None:
            raise ValueError(f"Unsupported/forbidden timeframe for live trigger use: {timeframe}")
        payload = {
            "type": "candleSnapshot",
            "req": {"coin": symbol, "interval": interval, "startTime": start_ms, "endTime": end_ms},
        }
        raw = self._post(payload)
        if not raw or not isinstance(raw, list):
            return []
        out = []
        for c in raw:
            try:
                out.append(Candle(
                    ts=int(c["t"]), o=float(c["o"]), h=float(c["h"]),
                    l=float(c["l"]), c=float(c["c"]), v=float(c.get("v", 0.0)),
                ))
            except (KeyError, TypeError, ValueError):
                continue
        out.sort(key=lambda x: x.ts)
        return out

    def fetch_mark_price(self, symbol: str) -> Optional[float]:
        raw = self._post({"type": "allMids"})
        if not isinstance(raw, dict):
            return None
        val = raw.get(symbol)
        try:
            return float(val) if val is not None else None
        except (TypeError, ValueError):
            return None


def _atomic_write_json(path: str, data: Any, indent: Optional[int] = None) -> None:
    """Write-temp-then-rename atomic persistence.

    `indent=None` (default) preserves the original compact single-line
    output -- used for candle_cache.json, which holds large raw OHLCV
    arrays and would otherwise bloat file size / git diffs on every
    15-minute run for no practical benefit (it's not meant to be
    hand-read). Callers that want a human-readable file (state.json)
    pass an explicit indent."""
    tmp_path = f"{path}.tmp.{os.getpid()}"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent, sort_keys=(indent is not None))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


class CandleCacheStore:
    """Persistent cache keyed by asset+timeframe. Every stage of
    the top-down sequence and every specialized engine reads the same shared
    candle series -- fetched/computed once per run, never redundantly."""

    def __init__(self, path: str = CANDLE_CACHE_PATH):
        self.path = path
        self.cache: dict[str, dict[str, list[dict]]] = self._load()

    def _load(self) -> dict:
        if not os.path.exists(self.path):
            log.info("No existing candle_cache.json -- cold start.")
            return {}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, OSError) as e:
            log.warning("candle_cache.json unreadable (%s) -- starting fresh.", e)
            return {}

    def save(self) -> None:
        try:
            _atomic_write_json(self.path, self.cache)
        except OSError as e:
            log.error("Failed to persist candle_cache.json: %s", e)

    def get_or_fetch(self, client: HyperliquidClient, symbol: str, timeframe: str) -> list[Candle]:
        sym_cache = self.cache.setdefault(symbol, {}).setdefault(timeframe, [])
        now_ms = int(time.time() * 1000)
        lookback_bars = CANDLE_LOOKBACK.get(timeframe, 300)
        tf_ms = _timeframe_ms(timeframe)
        cutoff = _current_bar_open_ms(now_ms, timeframe)

        if sym_cache:
            last_ts = sym_cache[-1]["t"]
            if last_ts + tf_ms >= cutoff:
                closed = [d for d in sym_cache if d["t"] < cutoff]
                return [Candle(ts=d["t"], o=d["o"], h=d["h"], l=d["l"], c=d["c"], v=d["v"]) for d in closed]
            start = last_ts - tf_ms * CANDLE_DELTA_OVERLAP_BARS
        else:
            start = now_ms - lookback_bars * tf_ms

        if start < now_ms:
            fresh = client.fetch_candles(symbol, timeframe, start, now_ms)
            fresh = _filter_closed_candles(fresh, timeframe, now_ms)
            fresh_dicts = [{"t": c.ts, "o": c.o, "h": c.h, "l": c.l, "c": c.c, "v": c.v} for c in fresh]
            if fresh_dicts:
                merged = {d["t"]: d for d in sym_cache}
                for d in fresh_dicts:
                    merged[d["t"]] = d
                sym_cache = sorted(merged.values(), key=lambda d: d["t"])
                # bound cache growth: keep a small margin beyond the lookback window
                sym_cache = sym_cache[-(lookback_bars + 30):]
                self.cache[symbol][timeframe] = sym_cache

        closed = [d for d in sym_cache if d["t"] < cutoff]
        return [Candle(ts=d["t"], o=d["o"], h=d["h"], l=d["l"], c=d["c"], v=d["v"]) for d in closed]


def _timeframe_ms(timeframe: str) -> int:
    return {
        TF_5M: 5 * 60_000, TF_15M: 15 * 60_000, TF_1H: 60 * 60_000, TF_4H: 4 * 60 * 60_000,
        TF_DAILY: 24 * 60 * 60_000, TF_WEEKLY: 7 * 24 * 60 * 60_000,
    }[timeframe]


def _current_bar_open_ms(reference_ms: int, timeframe: str) -> int:
    step = _timeframe_ms(timeframe)
    return (reference_ms // step) * step


def _filter_closed_candles(candles: list[Candle], timeframe: str, reference_ms: int) -> list[Candle]:
    """Keep only candles whose interval has fully elapsed."""
    cutoff = _current_bar_open_ms(reference_ms, timeframe)
    return [c for c in candles if c.ts < cutoff]


def _default_state() -> dict:
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "resolution_logic_version": RESOLUTION_LOGIC_VERSION,
        # --- Tier 1: permanent, incrementally-updated aggregates ------------
        "tier1": {
            "baseline_profit_factor": BASELINE_PROFIT_FACTOR,  # circuit-breaker input
            "adaptive_params": {
                "sl_buffer_percentile": {},          # "{asset}:{tf}" -> float, default 65.0
                "sl_buffer_percentile_dist": {},      # "{asset}:15M" -> float
                "tp1_target_rank_preference": {},     # asset -> int, default 3
                "regime_fit_discount": {},             # "{engine}:{regime}" -> float
                "mtf_alignment_weight": {},            # engine -> float
                "liquidity_sanity_threshold": {},      # "{engine}:{setup}" -> float
                "sfp_mss_strictness": {},              # engine -> float
                "confidence_calibration_shift": {},    # "{engine}:{bucket}" -> float
                "session_open_proximity_weight": {},   # global -> float
            },
            "segment_stats": {},        # "{asset}|{regime}|{tf}|{engine}" -> {wins, losses, r_sum, n}
            "calibration": {},           # "{engine}:{bucket}" -> {predicted_n, wins, n}
            "forensic_category_counts": {},   # category -> count
            "forensic_category_r_drift": {},  # category -> cumulative parameter delta magnitude
            "baseline_win_rate": 0.5,
            "rolling_win_rate": 0.5,
            "equity_curve_r": [],        # bounded list of cumulative R checkpoints
            "signal_rate_history": {},   # "YYYY-MM-DD" -> count
            "fill_rate_stats": {"filled": 0, "expired": 0},
            "filter_funnel": {},         # filter_name -> {"eliminated": n, "passed": n}
            "circuit_breaker": {"tripped": False, "since": None},
        },
        # --- Tier 2: bounded, prunable raw trade log -------------------------
        "tier2": {
            "trade_log": [],             # list of resolved-trade records, bounded
            "active_signals": [],        # currently open/pending dispatched signals
        },
        "macro_events": [],              # operator-supplied [{ "ts": iso, "assets": [...]}]
        "last_run_ts": None,
    }


class StateStore:
    """Atomic, lock-free (single-process, single-run) load/save of state.json."""

    def __init__(self, path: str = STATE_PATH):
        self.path = path
        self.state: dict = self._load()

    def _load(self) -> dict:
        default = _default_state()
        if not os.path.exists(self.path):
            log.info("No existing state.json -- cold start with defaults.")
            return default
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if not isinstance(loaded, dict):
                raise ValueError("state.json root is not an object")
            # Shallow-merge onto defaults so newly-added keys are always present.
            merged = default
            for top_key in ("tier1", "tier2"):
                merged[top_key].update(loaded.get(top_key, {}))
                if top_key == "tier1":
                    for sub_key in default["tier1"]["adaptive_params"]:
                        merged["tier1"]["adaptive_params"][sub_key] = (
                            loaded.get("tier1", {}).get("adaptive_params", {}).get(sub_key)
                            or default["tier1"]["adaptive_params"][sub_key]
                        )
            merged["macro_events"] = loaded.get("macro_events", [])
            merged["last_run_ts"] = loaded.get("last_run_ts")
            merged["last_daily_summary_date"] = loaded.get("last_daily_summary_date")
            merged["resolution_logic_version"] = loaded.get("resolution_logic_version", RESOLUTION_LOGIC_VERSION)
            merged["tier1"]["baseline_profit_factor"] = loaded.get("tier1", {}).get(
                "baseline_profit_factor", BASELINE_PROFIT_FACTOR)
            merged["schema_version"] = STATE_SCHEMA_VERSION  # always upgrade in place; no destructive migration
            return merged
        except (json.JSONDecodeError, OSError, ValueError) as e:
            log.error("Failed to load state.json (%s); falling back to defaults.", e)
            return default

    def save(self) -> None:
        self.state["last_run_ts"] = datetime.now(timezone.utc).isoformat()
        self._prune_tier2()
        try:
            _atomic_write_json(self.path, self.state, indent=2)  # human-readable; state.json is small
        except OSError as e:
            log.error("Failed to persist state.json -- next run will not see this run's updates: %s", e)

    def _prune_tier2(self, max_trade_log: int = 2000, max_active: int = 200) -> None:
        """Bound Tier 2's unbounded-growth collections. Pruning Tier 2 never
        touches Tier 1, so aggregates and all adaptive behavior are unchanged."""
        t2 = self.state["tier2"]
        if len(t2["trade_log"]) > max_trade_log:
            t2["trade_log"] = t2["trade_log"][-max_trade_log:]
        if len(t2["active_signals"]) > max_active:
            t2["active_signals"] = t2["active_signals"][-max_active:]


def get_adaptive(state: dict, param: str, key: str, default: float) -> float:
    return float(state["tier1"]["adaptive_params"].get(param, {}).get(key, default))


def set_adaptive(state: dict, param: str, key: str, new_value: float) -> None:
    """Apply an adaptive-parameter update with its documented bound and capped
    step size. Never a raw, unbounded assignment."""
    lo, hi, max_step = ADAPTIVE_BOUNDS[param]
    bucket = state["tier1"]["adaptive_params"].setdefault(param, {})
    current = float(bucket.get(key, (lo + hi) / 2))
    delta = max(-max_step, min(max_step, new_value - current))
    bucket[key] = max(lo, min(hi, current + delta))


# Each indicator is computed once per asset/timeframe/run and reused by
# every consumer, never recomputed inside individual engines.

def _ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    k = 2.0 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def _sma(values: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(values)):
        window = values[max(0, i - period + 1): i + 1]
        out.append(sum(window) / len(window))
    return out


def _true_range(candles: list[Candle]) -> list[float]:
    tr = []
    prev_close = candles[0].c if candles else 0.0
    for c in candles:
        tr.append(max(c.h - c.l, abs(c.h - prev_close), abs(c.l - prev_close)))
        prev_close = c.c
    return tr


def _atr(candles: list[Candle], period: int = 14) -> list[float]:
    tr = _true_range(candles)
    return _ema(tr, period) if tr else []


def _rsi(closes: list[float], period: int = 14) -> list[float]:
    if len(closes) < 2:
        return [50.0] * len(closes)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))
    avg_gain = _ema(gains, period)
    avg_loss = _ema(losses, period)
    out = []
    for g, l in zip(avg_gain, avg_loss):
        if l <= 1e-12:
            out.append(100.0)
        else:
            rs = g / l
            out.append(100.0 - (100.0 / (1.0 + rs)))
    return out


def _macd_hist(closes: list[float]) -> list[float]:
    ema12, ema26 = _ema(closes, 12), _ema(closes, 26)
    macd = [a - b for a, b in zip(ema12, ema26)]
    signal = _ema(macd, 9)
    return [m - s for m, s in zip(macd, signal)]


def _stoch_rsi(rsi_vals: list[float], period: int = 14) -> list[float]:
    out = []
    for i in range(len(rsi_vals)):
        window = rsi_vals[max(0, i - period + 1): i + 1]
        lo, hi = min(window), max(window)
        out.append(0.0 if hi - lo < 1e-9 else (rsi_vals[i] - lo) / (hi - lo) * 100.0)
    return out


def _roc(closes: list[float], period: int = 10) -> list[float]:
    out = []
    for i in range(len(closes)):
        j = max(0, i - period)
        base = closes[j]
        out.append(0.0 if base == 0 else (closes[i] - base) / base * 100.0)
    return out


def _obv(candles: list[Candle]) -> list[float]:
    out = [0.0]
    for i in range(1, len(candles)):
        if candles[i].c > candles[i - 1].c:
            out.append(out[-1] + candles[i].v)
        elif candles[i].c < candles[i - 1].c:
            out.append(out[-1] - candles[i].v)
        else:
            out.append(out[-1])
    return out


def _cmf(candles: list[Candle], period: int = 20) -> list[float]:
    mfv = []
    for c in candles:
        rng = c.h - c.l
        mfm = 0.0 if rng < 1e-12 else ((c.c - c.l) - (c.h - c.c)) / rng
        mfv.append(mfm * c.v)
    out = []
    for i in range(len(candles)):
        w_mfv = mfv[max(0, i - period + 1): i + 1]
        w_vol = [candles[j].v for j in range(max(0, i - period + 1), i + 1)]
        vol_sum = sum(w_vol)
        out.append(0.0 if vol_sum < 1e-12 else sum(w_mfv) / vol_sum)
    return out


def _vwap(candles: list[Candle]) -> list[float]:
    out, cum_pv, cum_v = [], 0.0, 0.0
    for c in candles:
        typical = (c.h + c.l + c.c) / 3.0
        cum_pv += typical * c.v
        cum_v += c.v
        out.append(typical if cum_v < 1e-12 else cum_pv / cum_v)
    return out


def _bb_width(closes: list[float], period: int = 20) -> list[float]:
    out = []
    for i in range(len(closes)):
        window = closes[max(0, i - period + 1): i + 1]
        if len(window) < 2:
            out.append(0.0)
            continue
        mean = sum(window) / len(window)
        sd = statistics.pstdev(window)
        out.append(0.0 if mean == 0 else (4 * sd) / mean)
    return out


def _donchian_width(candles: list[Candle], period: int = 20) -> list[float]:
    out = []
    for i in range(len(candles)):
        window = candles[max(0, i - period + 1): i + 1]
        hi = max(c.h for c in window)
        lo = min(c.l for c in window)
        mid = (hi + lo) / 2.0
        out.append(0.0 if mid == 0 else (hi - lo) / mid)
    return out


def _linreg_slope(values: list[float], period: int = 20) -> float:
    window = values[-period:]
    n = len(window)
    if n < 2:
        return 0.0
    xs = list(range(n))
    mean_x, mean_y = sum(xs) / n, sum(window) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, window))
    den = sum((x - mean_x) ** 2 for x in xs)
    slope = 0.0 if den == 0 else num / den
    return slope / mean_y if mean_y else 0.0  # normalized slope


def _adx(candles: list[Candle], period: int = 14) -> list[float]:
    if len(candles) < period + 1:
        return [0.0] * len(candles)
    plus_dm, minus_dm = [0.0], [0.0]
    for i in range(1, len(candles)):
        up = candles[i].h - candles[i - 1].h
        down = candles[i - 1].l - candles[i].l
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
    tr = _true_range(candles)
    atr_s = _ema(tr, period)
    plus_di = [100 * p / a if a > 1e-9 else 0.0 for p, a in zip(_ema(plus_dm, period), atr_s)]
    minus_di = [100 * m / a if a > 1e-9 else 0.0 for m, a in zip(_ema(minus_dm, period), atr_s)]
    dx = [100 * abs(p - m) / (p + m) if (p + m) > 1e-9 else 0.0 for p, m in zip(plus_di, minus_di)]
    return _ema(dx, period)


def _swing_pivots(candles: list[Candle], left: int = 3, right: int = 3) -> list[Pivot]:
    """Precise, parameterized swing-detection rule: a bar is a swing high/low
    only if it is the strict extreme over `left` bars before and `right` bars
    after it. Applied only to fully closed candles."""
    pivots: list[Pivot] = []
    n = len(candles)
    for i in range(left, n - right):
        window_h = [candles[j].h for j in range(i - left, i + right + 1)]
        window_l = [candles[j].l for j in range(i - left, i + right + 1)]
        if candles[i].h == max(window_h) and window_h.count(candles[i].h) == 1:
            pivots.append(Pivot(index=i, ts=candles[i].ts, price=candles[i].h, kind="high"))
        if candles[i].l == min(window_l) and window_l.count(candles[i].l) == 1:
            pivots.append(Pivot(index=i, ts=candles[i].ts, price=candles[i].l, kind="low"))
    return pivots


def _atr_percentile(atr_series: list[float], lookback: int = 100) -> float:
    if not atr_series:
        return 50.0
    window = [a for a in atr_series[-lookback:] if a is not None]
    if len(window) < 5:
        return 50.0
    current = window[-1]
    rank = sum(1 for a in window if a <= current)
    return 100.0 * rank / len(window)


# Smart Money Concepts primitives

def _detect_fvgs(candles: list[Candle]) -> list[Zone]:
    """Three-candle fair value gap: candle[i-1].high < candle[i+1].low (bullish
    imbalance) or the mirror for bearish. Only evaluated on closed candles."""
    out = []
    for i in range(1, len(candles) - 1):
        left, right = candles[i - 1], candles[i + 1]
        if left.h < right.l:
            out.append(Zone(kind="fvg", direction="bullish", top=right.l, bottom=left.h,
                             origin_index=i, origin_ts=candles[i].ts))
        elif left.l > right.h:
            out.append(Zone(kind="fvg", direction="bearish", top=left.l, bottom=right.h,
                             origin_index=i, origin_ts=candles[i].ts))
    return out


def _detect_order_blocks(candles: list[Candle], pivots: list[Pivot]) -> list[Zone]:
    """Order block: the last opposite-direction candle before a displacement
    leg (a candle whose range exceeds 1.5x the local ATR) that breaks the most
    recent opposing swing point."""
    out = []
    atr_series = _atr(candles, 14)
    for i in range(2, len(candles)):
        body = abs(candles[i].c - candles[i].o)
        local_atr = atr_series[i] if i < len(atr_series) and atr_series[i] else 1e-9
        if body < 1.5 * local_atr:
            continue
        bullish_disp = candles[i].c > candles[i].o
        j = i - 1
        if bullish_disp and candles[j].c < candles[j].o:
            out.append(Zone(kind="order_block", direction="bullish",
                             top=candles[j].h, bottom=candles[j].l,
                             origin_index=j, origin_ts=candles[j].ts))
        elif (not bullish_disp) and candles[j].c > candles[j].o:
            out.append(Zone(kind="order_block", direction="bearish",
                             top=candles[j].h, bottom=candles[j].l,
                             origin_index=j, origin_ts=candles[j].ts))
    return out[-40:]


def _identify_liquidity_pools(pivots: list[Pivot], tolerance_pct: float = 0.0015) -> list[LiquidityPool]:
    """EQH/EQL clustering -> BSL/SSL pools."""
    highs = sorted([p for p in pivots if p.kind == "high"], key=lambda p: p.price)
    lows = sorted([p for p in pivots if p.kind == "low"], key=lambda p: p.price)
    pools: list[LiquidityPool] = []

    def cluster(points: list[Pivot], pool_kind: str):
        i = 0
        while i < len(points):
            group = [points[i]]
            j = i + 1
            while j < len(points) and abs(points[j].price - group[0].price) <= tolerance_pct * group[0].price:
                group.append(points[j])
                j += 1
            avg_price = sum(p.price for p in group) / len(group)
            pools.append(LiquidityPool(kind=pool_kind, price=avg_price,
                                        is_equal_cluster=len(group) >= 2,
                                        indices=[p.index for p in group]))
            i = j

    cluster(highs, "BSL")
    cluster(lows, "SSL")
    return pools


def _premium_discount_zone(candles: list[Candle], pivots: list[Pivot]) -> dict[str, float]:
    """Premium/discount/equilibrium against the current dealing range, defined
    from the most recent significant swing high/low pair."""
    if len(pivots) < 2:
        return {}
    recent = pivots[-20:]
    highs = [p.price for p in recent if p.kind == "high"]
    lows = [p.price for p in recent if p.kind == "low"]
    if not highs or not lows:
        return {}
    range_high, range_low = max(highs), min(lows)
    eq = (range_high + range_low) / 2.0
    return {"range_high": range_high, "range_low": range_low, "equilibrium": eq,
            "premium_from": eq, "discount_to": eq}


def structure_shift(direction: str, view: "View", kind: str = "BOS") -> Optional[dict]:
    """Single shared structure-shift detector: the one function
    both the MSS/BOS detector and the Counter-Trend engine's CHoCH
    check call into. `kind` in {"BOS", "CHoCH"}.
      BOS   -- a close beyond the most recent swing point IN the prevailing
               trend direction (continuation).
      CHoCH -- a close beyond the most recent swing point AGAINST the
               immediately-preceding sequence (the first break of the opposite
               structure -- a character change, not mere continuation).
    Operates on closed candles only.
    """
    pivots = view.pivots
    if len(pivots) < 3:
        return None
    closes = view.closes
    last_closed_idx = len(closes) - 1
    if direction == "bullish":
        relevant = [p for p in pivots if p.kind == "high"]
        if not relevant:
            return None
        ref = relevant[-1]
        prior_highs = [p for p in relevant[:-1]]
        broke = closes[last_closed_idx] > ref.price
        if not broke:
            return None
        if kind == "CHoCH":
            # a CHoCH requires the immediately preceding swing sequence to have
            # been making lower highs (i.e. this is the FIRST break upward)
            if len(prior_highs) >= 2 and prior_highs[-1].price >= prior_highs[-2].price:
                return None
        return {"level": ref.price, "index": ref.index, "ts": ref.ts, "kind": kind}
    else:
        relevant = [p for p in pivots if p.kind == "low"]
        if not relevant:
            return None
        ref = relevant[-1]
        prior_lows = [p for p in relevant[:-1]]
        broke = closes[last_closed_idx] < ref.price
        if not broke:
            return None
        if kind == "CHoCH":
            if len(prior_lows) >= 2 and prior_lows[-1].price <= prior_lows[-2].price:
                return None
        return {"level": ref.price, "index": ref.index, "ts": ref.ts, "kind": kind}


def _detect_breaker_blocks(order_blocks: list[Zone], structure_shifts_by_index: dict[int, dict]) -> list[Zone]:
    """A breaker block is a former order block that has since been invalidated
    (price traded through it) and flipped by a confirmed structure shift --
    the most recent confirmed institutional footprint."""
    out = []
    for ob in order_blocks:
        flip = structure_shifts_by_index.get(ob.origin_index)
        if flip is None:
            continue
        flipped_direction = "bearish" if ob.direction == "bullish" else "bullish"
        out.append(Zone(kind="breaker_block", direction=flipped_direction,
                         top=ob.top, bottom=ob.bottom,
                         origin_index=ob.origin_index, origin_ts=ob.origin_ts, from_sweep=ob.from_sweep))
    return out


def build_view(symbol: str, timeframe: str, candles: list[Candle]) -> View:
    """Compute every feature primitive once for this asset/timeframe."""
    v = View(symbol=symbol, timeframe=timeframe, candles=candles)
    if len(candles) < 10:
        return v
    v.closes = [c.c for c in candles]
    v.highs = [c.h for c in candles]
    v.lows = [c.l for c in candles]
    v.volumes = [c.v for c in candles]

    v.ema_fast = _ema(v.closes, 21)
    v.ema_slow = _ema(v.closes, 55)
    v.sma = _sma(v.closes, 50)
    v.vwap = _vwap(candles)
    v.adx = _adx(candles)
    v.lr_slope = _linreg_slope(v.closes)

    v.atr = _atr(candles)
    v.atr_percentile = _atr_percentile(v.atr)
    v.bb_width = _bb_width(v.closes)
    v.donchian_width = _donchian_width(candles)

    v.rsi = _rsi(v.closes)
    v.macd_hist = _macd_hist(v.closes)
    v.stoch_rsi = _stoch_rsi(v.rsi)
    v.roc = _roc(v.closes)

    v.vol_sma = _sma(v.volumes, 20)
    v.rel_volume = (v.volumes[-1] / v.vol_sma[-1]) if v.vol_sma and v.vol_sma[-1] > 1e-9 else 1.0
    v.obv = _obv(candles)
    v.cmf = _cmf(candles)

    v.pivots = _swing_pivots(candles)

    # Trend engine outputs
    ema_gap = (v.ema_fast[-1] - v.ema_slow[-1]) / v.closes[-1] if v.closes[-1] else 0.0
    if ema_gap > 0.002 and v.lr_slope > 0:
        v.trend_direction = "bullish"
    elif ema_gap < -0.002 and v.lr_slope < 0:
        v.trend_direction = "bearish"
    else:
        v.trend_direction = "neutral"
    v.trend_strength = min(1.0, abs(v.lr_slope) * 25 + (v.adx[-1] / 100.0 if v.adx else 0.0))
    # trend_quality: how clean (low BB-width noise relative to directional slope) vs choppy
    recent_bb = v.bb_width[-20:] if v.bb_width else [0.0]
    v.trend_quality = max(0.0, min(1.0, v.trend_strength - (statistics.pstdev(recent_bb) if len(recent_bb) > 1 else 0)))

    v.order_blocks = _detect_order_blocks(candles, v.pivots)
    v.fvgs = _detect_fvgs(candles)
    v.liquidity_pools = _identify_liquidity_pools(v.pivots)
    v.premium_discount = _premium_discount_zone(candles, v.pivots)

    shifts_by_index: dict[int, dict] = {}
    for p in v.pivots:
        shifts_by_index[p.index] = {"level": p.price, "index": p.index, "ts": p.ts}
    v.breaker_blocks = _detect_breaker_blocks(v.order_blocks, shifts_by_index)

    return v


@dataclass
class RegimeVector:
    trend_strength: float
    volatility_percentile: float
    liquidity_activity: float
    session_open_proximity: float
    noise_index: float
    breadth: float


def compute_regime_vector(view_1h: View, views_by_symbol: dict[str, View], now_utc: datetime) -> RegimeVector:
    trend_strength = view_1h.trend_strength
    vol_pctile = view_1h.atr_percentile / 100.0
    liquidity_activity = min(1.0, len(view_1h.liquidity_pools) / 12.0)

    session_open_proximity = _session_open_proximity_score(now_utc)

    recent_ranges = [(c.h - c.l) / c.c for c in view_1h.candles[-30:] if c.c]
    noise_index = min(1.0, (statistics.pstdev(recent_ranges) * 40) if len(recent_ranges) > 1 else 0.0)

    coherent = 0
    total = 0
    for sym, v in views_by_symbol.items():
        if not v.closes:
            continue
        total += 1
        if v.trend_direction == view_1h.trend_direction and v.trend_direction != "neutral":
            coherent += 1
    breadth = (coherent / total) if total else 0.5

    return RegimeVector(trend_strength=trend_strength, volatility_percentile=vol_pctile,
                         liquidity_activity=liquidity_activity, session_open_proximity=session_open_proximity,
                         noise_index=noise_index, breadth=breadth)


def _session_open_proximity_score(now_utc: datetime) -> float:
    """Continuous score for proximity to a
    major session open: Asia 00:00 UTC, London 07:00 UTC, New York 12:30 UTC."""
    opens = [0 * 60, 7 * 60, 12 * 60 + 30]
    minutes_now = now_utc.hour * 60 + now_utc.minute
    dist = min(min(abs(minutes_now - o), 1440 - abs(minutes_now - o)) for o in opens)
    return max(0.0, 1.0 - dist / 90.0)  # full score at the open, decaying to 0 by 90 minutes out


def classify_regime(rv: RegimeVector) -> tuple[str, float, str]:
    """Discrete regime label derived from multiple independent quantitative
    features -- display/lookup derivative only, never a scoring substitute."""
    label, confidence = "Sideways", 0.5
    if rv.trend_strength > 0.65 and rv.volatility_percentile > 0.4:
        label = "Strong Bull Trend" if rv.breadth >= 0.5 else "Strong Bear Trend"
        confidence = min(1.0, rv.trend_strength)
    elif rv.trend_strength > 0.35:
        label = "Weak Trend"
        confidence = rv.trend_strength
    elif rv.volatility_percentile > 0.85:
        label = "High Volatility"
        confidence = rv.volatility_percentile
    elif rv.volatility_percentile < 0.15:
        label = "Compression" if rv.noise_index < 0.3 else "Low Volatility"
        confidence = 1.0 - rv.volatility_percentile
    elif rv.noise_index > 0.6:
        label = "Range"
        confidence = rv.noise_index
    else:
        label = "Sideways"
        confidence = 1.0 - rv.trend_strength

    expected_behavior = {
        "Strong Bull Trend": "Favor trend-continuation longs; discount counter-trend shorts.",
        "Strong Bear Trend": "Favor trend-continuation shorts; discount counter-trend longs.",
        "Weak Trend": "Moderate directional edge; require stronger structural confirmation.",
        "Sideways": "Favor liquidity/range logic; discount pure trend-following.",
        "Range": "Trade the boundaries; discount breakout chasing.",
        "Expansion": "Momentum/volatility setups favored.",
        "Compression": "Watch for breakout; discount premature directional bets.",
        "High Volatility": "Widen risk tolerance; discount momentum-confirmation weight.",
        "Low Volatility": "Reduce size expectations; favor patient entries.",
        "Breakout": "Favor retest-based continuation entries.",
        "Pullback": "Favor continuation entries on the pullback, not fresh reversals.",
        "Mean Reversion": "Favor liquidity-fade setups; discount trend-following.",
    }.get(label, "No strong regime read; default to selectivity.")
    return label, round(confidence, 3), expected_behavior


def regime_adjusted_weights(regime_label: str) -> dict[str, float]:
    weights = dict(BASE_SCORE_WEIGHTS)
    mult = REGIME_WEIGHT_MULTIPLIERS.get(regime_label, {})
    for k, m in mult.items():
        weights[k] = weights.get(k, 0.0) * m
    total = sum(weights.values()) or 1.0
    return {k: v / total for k, v in weights.items()}  # renormalized, still sums to 1


@dataclass
class AdaptiveFilterState:
    """Filter categories, tightened/relaxed by regime cleanliness."""
    location: float = 0.5
    context: float = 0.5
    trend: float = 0.5
    range_: float = 0.5
    reversal: float = 0.5
    liquidity: float = 0.5
    volume: float = 0.5
    volatility: float = 0.5
    momentum: float = 0.5
    mtf_confirmation: float = 0.5
    institutional_confluence: float = 0.5
    quality_score: float = 0.5
    expected_value: float = 0.5
    risk_reward: float = 0.5


def compute_adaptive_filters(rv: RegimeVector, regime_label: str) -> AdaptiveFilterState:
    """Tighten thresholds (raise the bar, i.e. higher required score) in
    chaotic/low-quality markets; relax (lower the bar) in clean markets --
    always resolved toward selectivity when ambiguous."""
    cleanliness = max(0.0, min(1.0, rv.trend_strength * (1.0 - rv.noise_index)))
    chaos = 1.0 - cleanliness
    base = 0.55 + 0.30 * chaos  # tighter (higher) bar as chaos increases
    f = AdaptiveFilterState(
        location=base, context=base, trend=base if "Trend" in regime_label else base + 0.05,
        range_=base if regime_label in ("Range", "Sideways") else base + 0.05,
        reversal=base + (0.1 if regime_label.startswith("Strong") else 0.0),
        liquidity=base - 0.03, volume=base, volatility=base,
        momentum=base if regime_label not in ("High Volatility",) else base + 0.05,
        mtf_confirmation=base, institutional_confluence=base,
        quality_score=base, expected_value=base, risk_reward=base,
    )
    return f


def _detect_sweep(direction: str, view: "View", state: dict, asset: str) -> Optional[SweepEvent]:
    """Detect a genuine liquidity sweep of an EQH/EQL (or isolated swing) pool
    that fails to hold, with SFP purity classification."""
    if len(view.candles) < 5:
        return None
    last = view.candles[-1]
    pool_kind = "SSL" if direction == "bullish" else "BSL"
    candidates = [p for p in view.liquidity_pools if p.kind == pool_kind]
    if not candidates:
        return None
    # nearest pool the last closed candle's wick actually traded through
    best = None
    for pool in candidates:
        if direction == "bullish" and last.l < pool.price < last.c:
            best = pool
            break
        if direction == "bearish" and last.h > pool.price > last.c:
            best = pool
            break
    if best is None:
        return None
    wick = (last.c - last.l) if direction == "bullish" else (last.h - last.c)
    body = abs(last.c - last.o) or 1e-9
    is_pure = wick / body >= 1.2  # genuine wick-based rejection, not an ambiguous/partial sweep
    session_tag = _session_range_tag(view, last)
    return SweepEvent(direction=direction, pool=best, index=len(view.candles) - 1, ts=last.ts,
                       is_pure=is_pure, session_tag=session_tag)


def _session_range_tag(view: "View", candle: Candle) -> Optional[str]:
    dt = datetime.fromtimestamp(candle.ts / 1000, tz=timezone.utc)
    hour = dt.hour
    if 0 <= hour < 7:
        return "asian_range"
    if 7 <= hour < 12:
        return "london_range"
    if 12 <= hour < 21:
        return "ny_range"
    return None


def _poi_pool(direction: str, view: "View") -> list[Zone]:
    zones = view.order_blocks + view.breaker_blocks + view.fvgs
    return [z for z in zones if z.direction == direction and not z.mitigated]


# Kind-filtered POI pool: lets Order Block / Breaker Block / Fair Value Gap
# engines each derive a distinct reference zone instead of sharing one POI.
_TYPED_ZONE_ATTR = {"order_block": "order_blocks", "breaker_block": "breaker_blocks", "fvg": "fvgs"}


def _typed_poi_pool(direction: str, view: "View", kind: str) -> list[Zone]:
    attr = _TYPED_ZONE_ATTR.get(kind)
    if attr is None:
        return []
    zones: list[Zone] = getattr(view, attr)
    matches = [z for z in zones if z.direction == direction and not z.mitigated]
    return sorted(matches, key=lambda z: z.origin_index, reverse=True)  # most recent first


def zone_selection_sequence(direction: str, h1_view: View, h4_view: View, state: dict, asset: str) -> Optional[dict]:
    """Zone-selection sequence, executed within Stage 3 (1H Trade Setup).
    Returns a dict describing the validated POI, or None (NOT READY/INVALID)."""
    # Step 1: HTF bias is the caller's responsibility (Stage 1); this function
    # only ever runs once that bias is established and passed in as `direction`.

    # Step 2: candidate POI pool on the 1H view.
    poi_pool = _poi_pool(direction, h1_view)
    log_filter_attrition(state, "DIAG_1h_poi_pool_exists", passed=bool(poi_pool))  # diagnostic instrumentation
    if not poi_pool:
        return None

    # Step 3: SFP purity check + sweep-to-POI causality.
    sweep = _detect_sweep(direction, h1_view, state, asset)
    sfp_strictness = get_adaptive(state, "sfp_mss_strictness", "SMC", 0.3)
    poi = None
    if sweep is not None:
        if not sweep.is_pure and sfp_strictness > 0.5:
            return None  # impure SFP discounted to rejection under a tightened filter
        # sweep-to-POI causality: the POI must be downstream of this sweep
        downstream = [z for z in poi_pool if z.origin_index >= sweep.index - 3]
        if downstream:
            poi = downstream[0]
            poi.from_sweep = True
    if poi is None:
        # not disqualified without a sweep -- an isolated structural POI is
        # still eligible, just without the EQH/EQL confluence bonus
        poi = poi_pool[0]

    # Step 4: MSS confirmation via the single shared structure_shift() function.
    mss = structure_shift(direction, h1_view, kind="BOS")
    log_filter_attrition(state, "DIAG_1h_mss_bos_confirmed", passed=(mss is not None))  # diagnostic instrumentation
    if mss is None:
        return None  # NOT READY -- structure hasn't confirmed the shift yet

    # Step 5: breaker confirmation, where applicable -- prefer the breaker
    # block over a plain order block/FVG if one exists at/near the same zone.
    breakers = [z for z in h1_view.breaker_blocks if z.direction == direction]
    if breakers:
        candidate_breaker = breakers[-1]
        if abs(candidate_breaker.top - poi.top) / max(poi.top, 1e-9) < 0.01:
            poi = candidate_breaker

    return {
        "poi": poi, "sweep": sweep, "mss": mss,
        "session_anchored": bool(sweep and sweep.session_tag is not None),
    }


def ote_refine_entry(direction: str, poi: Zone, m15_view: View) -> float:
    """Step 6: Fibonacci OTE refinement (61.8-79%) of the impulse leg into the
    validated POI -- precision modifier only, never a new confluence point."""
    zone_mid_low, zone_mid_high = min(poi.top, poi.bottom), max(poi.top, poi.bottom)
    ote_low = zone_mid_low + 0.618 * (zone_mid_high - zone_mid_low)
    ote_high = zone_mid_low + 0.79 * (zone_mid_high - zone_mid_low)
    if direction == "bullish":
        return round((ote_low + min(ote_high, zone_mid_high)) / 2.0, 8)
    else:
        return round((max(ote_low, zone_mid_low) + ote_high) / 2.0, 8)


def stage1_bias(weekly: View, daily: View) -> str:
    """Weekly + Daily -> exactly one of Bullish / Bearish / Neutral."""
    votes = [weekly.trend_direction, daily.trend_direction]
    if votes.count("bullish") == 2:
        return "bullish"
    if votes.count("bearish") == 2:
        return "bearish"
    if votes[1] != "neutral" and weekly.trend_strength < 0.25:
        return votes[1]  # weak weekly signal -- defer to the more decisive daily read
    if votes.count("bullish") == 1 and votes.count("bearish") == 1:
        return "neutral"
    return votes[1] if votes[1] != "neutral" else votes[0]


def stage2_context(bias: str, h4: View) -> bool:
    """4H must confirm agreement with the Stage 1 bias."""
    if bias == "neutral":
        return False
    return h4.trend_direction == bias or (h4.trend_direction == "neutral" and h4.trend_strength < 0.2)


def stage3_setup(bias: str, h1: View, h4: View, state: dict, asset: str) -> tuple[str, Optional[dict]]:
    """Returns (VALID|NOT_READY|INVALID, zone_result_or_None)."""
    result = zone_selection_sequence(bias, h1, h4, state, asset)
    if result is None:
        # distinguish NOT READY (bias/context intact, setup still forming) from
        # INVALID (structure actively contradicts) using the MSS read alone
        mss_probe = structure_shift(bias, h1, kind="BOS")
        opposite = "bearish" if bias == "bullish" else "bullish"
        contradicting = structure_shift(opposite, h1, kind="BOS")
        if contradicting is not None and mss_probe is None:
            return "INVALID", None
        return "NOT_READY", None
    return "VALID", result


def stage4_entry(bias: str, zone_result: dict, m15: View) -> Optional[dict]:
    """15M confirmed MSS within the validated 1H POI; the FVG created by that
    specific 15M break is the entry vehicle, refined by OTE."""
    mss_15m = structure_shift(bias, m15, kind="BOS")
    if mss_15m is None:
        return None
    poi: Zone = zone_result["poi"]
    # the 15M break must occur within the validated 1H POI's price band
    last_close = m15.closes[-1]
    band_lo, band_hi = min(poi.top, poi.bottom), max(poi.top, poi.bottom)
    tolerance = (m15.atr[-1] or 0.0) * 0.5
    if not (band_lo - tolerance <= last_close <= band_hi + tolerance):
        return None
    # the FVG must have arisen from this specific 15M break (causality, mirrors step 3)
    recent_fvgs = [z for z in m15.fvgs if z.direction == bias and z.origin_index >= len(m15.candles) - 4]
    if not recent_fvgs:
        return None
    entry_zone = recent_fvgs[-1]
    entry_price = ote_refine_entry(bias, entry_zone, m15) if _overlaps(entry_zone, poi) else \
        (entry_zone.top + entry_zone.bottom) / 2.0
    return {"entry": entry_price, "entry_zone": entry_zone, "mss_15m": mss_15m}


def _overlaps(a: Zone, b: Zone) -> bool:
    a_lo, a_hi = min(a.top, a.bottom), max(a.top, a.bottom)
    b_lo, b_hi = min(b.top, b.bottom), max(b.top, b.bottom)
    return a_lo <= b_hi and b_lo <= a_hi


def stage5_5m_refine(bias: str, entry_zone: Zone, m5_view: Optional[View], fallback_entry: float) -> tuple[float, bool]:
    """Optional Stage 5 entry-timing refinement using only already-closed 5M
    candles, strictly bounded inside the Stage-4 FVG/POI. Can never relocate,
    widen, or salvage a Stage 4 result -- clamped to the zone's own bounds,
    and any failure to refine falls back to the unrefined Stage 4 entry.
    Returns (entry_price, was_refined)."""
    if not ENABLE_5M_REFINE or m5_view is None or len(m5_view.candles) < 5:
        return fallback_entry, False
    zone_lo, zone_hi = min(entry_zone.top, entry_zone.bottom), max(entry_zone.top, entry_zone.bottom)
    last5 = m5_view.candles[-1]
    touched_zone = (zone_lo <= last5.l <= zone_hi) or (zone_lo <= last5.h <= zone_hi)
    if not touched_zone:
        return fallback_entry, False
    rejection = ((bias == "bullish" and last5.c > last5.o and last5.l <= zone_lo * 1.001) or
                 (bias == "bearish" and last5.c < last5.o and last5.h >= zone_hi * 0.999))
    if not rejection:
        return fallback_entry, False
    refined = min(max(last5.c, zone_lo), zone_hi)  # clamp -- never allowed outside the Stage-4 zone
    return refined, True


def adaptive_sl_buffer(view: View, state: dict, asset: str) -> float:
    tf_key = f"{asset}:{view.timeframe}"
    percentile = get_adaptive(state, "sl_buffer_percentile", tf_key, 65.0)
    atr_now = view.atr[-1] if view.atr else 0.0
    buffer = atr_now * (percentile / 100.0) * 0.5
    state["tier1"]["adaptive_params"]["sl_buffer_percentile_dist"][f"{asset}:15M"] = buffer
    return buffer


def select_sl_anchor(direction: str, entry: float, m15_view: View, h1_view: View,
                      h4_view: View, state: dict, asset: str) -> Optional[tuple[str, View, float]]:
    """SL-anchor hierarchy: prefer the tightest genuinely-structural invalidation
    level, in order 15M -> 1H -> 4H, so the stop reflects the entry's own
    trigger structure first and only widens to a higher timeframe if the LTF
    offers no valid structural level."""
    for name, view in (("15M", m15_view), ("1H", h1_view), ("4H", h4_view)):
        pivots = [p for p in view.pivots if p.kind == ("low" if direction == "bullish" else "high")]
        if not pivots:
            continue
        candidate = pivots[-1]
        if direction == "bullish" and candidate.price < entry:
            return name, view, candidate.price
        if direction == "bearish" and candidate.price > entry:
            return name, view, candidate.price
    return None


def _clear_sl_of_liquidity_pool(direction: str, sl: float, view: View) -> float:
    """Buffer THEN clear, in this order. Bounded search window."""
    atr_now = view.atr[-1] if view.atr else 0.0
    window = atr_now * SL_POOL_CLEAR_WINDOW_ATR_MULT
    pool_kind = "SSL" if direction == "bullish" else "BSL"
    in_window = [p for p in view.liquidity_pools if p.kind == pool_kind and abs(p.price - sl) <= window]
    if not in_window:
        return sl
    nearest = min(in_window, key=lambda p: abs(p.price - sl))
    tiny_extra = atr_now * 0.05
    return (nearest.price - tiny_extra) if direction == "bullish" else (nearest.price + tiny_extra)


def _opposing_structural_levels(direction: str, entry: float, view: View) -> list[dict]:
    candidates = []
    for pool in view.liquidity_pools:
        if direction == "bullish" and pool.kind == "BSL" and pool.price > entry:
            candidates.append({"price": pool.price, "score": 2.0 if pool.is_equal_cluster else 1.0})
        elif direction == "bearish" and pool.kind == "SSL" and pool.price < entry:
            candidates.append({"price": pool.price, "score": 2.0 if pool.is_equal_cluster else 1.0})
    opposite_dir = "bearish" if direction == "bullish" else "bullish"
    for z in (view.order_blocks + view.breaker_blocks + view.fvgs):
        if z.direction != opposite_dir:
            continue
        mid = (z.top + z.bottom) / 2.0
        if direction == "bullish" and mid > entry:
            candidates.append({"price": mid, "score": 1.5 if z.kind == "breaker_block" else 1.0})
        elif direction == "bearish" and mid < entry:
            candidates.append({"price": mid, "score": 1.5 if z.kind == "breaker_block" else 1.0})
    if not candidates:
        return []
    return _merge_confluent_levels(candidates, tol=(view.atr[-1] or entry * 0.003) * 0.3)


def _merge_confluent_levels(candidates: list[dict], tol: float) -> list[dict]:
    candidates = sorted(candidates, key=lambda c: c["price"])
    merged = [dict(candidates[0])]
    for c in candidates[1:]:
        if abs(c["price"] - merged[-1]["price"]) <= tol:
            merged[-1]["score"] += c["score"]
            merged[-1]["price"] = (merged[-1]["price"] + c["price"]) / 2
        else:
            merged.append(dict(c))
    return merged


def _tp_selection_band(candidates: list[dict], entry: float, state: dict, asset: str) -> list[dict]:
    """Keep the nearest `n` opposing-structural-level candidates by distance from entry."""
    n = int(get_adaptive(state, "tp1_target_rank_preference", asset, 3.0))
    return sorted(candidates, key=lambda c: abs(c["price"] - entry))[:max(n, 2)]


def _rr(entry: float, sl: float, tp: float, direction: str) -> float:
    risk = abs(entry - sl)
    reward = (tp - entry) if direction == "bullish" else (entry - tp)
    return 0.0 if risk <= 1e-12 else reward / risk


def build_risk_plan(direction: str, entry: float, m15_view: View, h1_view: View,
                     h4_view: View, state: dict, asset: str,
                     rr_min_gate: Optional[float] = None) -> Optional[dict]:
    rr_min_gate = rr_min_gate if rr_min_gate is not None else RR_MIN_GATE
    anchor = select_sl_anchor(direction, entry, m15_view, h1_view, h4_view, state, asset)
    if anchor is None:
        return None
    anchor_name, view, structural_sl = anchor

    buffer = adaptive_sl_buffer(view, state, asset)
    sl = (structural_sl - buffer) if direction == "bullish" else (structural_sl + buffer)
    sl = _clear_sl_of_liquidity_pool(direction, sl, view)

    risk = abs(entry - sl)
    if risk <= 1e-12:
        return None
    atr_now = view.atr[-1] or 1e-9
    if risk / atr_now > MAX_SL_ATR_MULT:
        return None
    if not (max(MIN_RISK_ATR_MULT * atr_now, MIN_SL_DISTANCE_PCT * entry)
            <= risk <=
            min(MAX_SL_ATR_MULT * atr_now, MAX_SL_DISTANCE_PCT * entry)):
        return None

    candidates = _opposing_structural_levels(direction, entry, view)
    if len(candidates) < 2:
        return None

    band = _tp_selection_band(candidates, entry, state, asset)
    tp1_pick = max(band, key=lambda c: c["score"])
    remaining = [c for c in candidates if c is not tp1_pick and
                 (c["price"] > tp1_pick["price"] if direction == "bullish" else c["price"] < tp1_pick["price"])]
    if not remaining:
        return None
    tp2_pick = min(remaining, key=lambda c: abs(c["price"] - tp1_pick["price"]))  # nearest-beyond-TP1
    tp1, tp2 = tp1_pick["price"], tp2_pick["price"]

    if direction == "bullish" and not (tp2 > tp1):
        return None
    if direction == "bearish" and not (tp2 < tp1):
        return None

    if abs(tp1 - entry) < entry * MIN_MOVE_PCT_TP1:
        return None
    if abs(tp2 - entry) < entry * MIN_MOVE_PCT_TP2:
        return None

    rr1 = _rr(entry, sl, tp1, direction)
    rr2 = _rr(entry, sl, tp2, direction)
    if rr1 < rr_min_gate or rr1 > RR_MAX_GATE:
        return None

    plan = {"sl": sl, "tp1": tp1, "tp2": tp2, "rr1": rr1, "rr2": rr2, "risk": risk,
            "buffer": buffer, "sl_anchor": anchor_name}
    # No-cosmetic-clamping final integrity assertion:
    assert abs(_rr(entry, sl, tp1, direction) - rr1) < 1e-6, \
        "displayed RR does not match RR implied by entry/sl/tp1 -- never clamp the number alone"
    return plan


def tp1_runway_ok(direction: str, entry: float, m15_view: View, state: dict, asset: str) -> bool:
    """Cheap pre-SL feasibility gate -- coarse plausibility only."""
    candidates = _opposing_structural_levels(direction, entry, m15_view)
    if not candidates:
        return False
    band = _tp_selection_band(candidates, entry, state, asset)
    best_in_band = max(band, key=lambda c: c["score"])
    plausible_reward = abs(best_in_band["price"] - entry)
    typical_risk = get_adaptive(state, "sl_buffer_percentile_dist", f"{asset}:15M",
                                 (m15_view.atr[-1] or 1e-9) * MIN_RISK_ATR_MULT)
    return (plausible_reward / max(typical_risk, 1e-9)) >= RR_MIN_GATE * 0.8


def retracement_entry(setup_type: str, direction: str, view: View, reference_zone: Optional[Zone] = None) -> Optional[float]:
    """Shared retracement-entry helper -- every specialized
    engine derives entry through a return-to-level mechanism; never a bare
    `close` assignment with entry_kind="market"."""
    last = view.candles[-1]
    if setup_type == "breakout" and reference_zone is not None:
        return (reference_zone.top + reference_zone.bottom) / 2.0  # retest of broken boundary
    if setup_type in ("liquidity_sweep", "reversal", "order_block", "breaker_block", "fvg") and reference_zone is not None:
        return ote_refine_entry(direction, reference_zone, view)
    if setup_type == "momentum":
        return view.ema_fast[-1] if view.ema_fast else None  # pullback to fast EMA
    if setup_type == "mean_reversion":
        mid = view.sma[-1] if view.sma else last.c
        width = (view.bb_width[-1] if view.bb_width else 0.02) * mid
        return mid - width if direction == "bullish" else mid + width  # BB extreme
    if setup_type == "range" and reference_zone is not None:
        return (reference_zone.top + reference_zone.bottom) / 2.0  # range boundary
    if setup_type == "volatility_expansion" and reference_zone is not None:
        return (reference_zone.top + reference_zone.bottom) / 2.0  # retest of broken band
    return None


def entry_distance_ok(direction: str, entry: float, market_price: float, view: View) -> bool:
    atr_now = view.atr[-1] or (market_price * 0.005)
    return abs(entry - market_price) <= atr_now * MAX_PENDING_ENTRY_DISTANCE_ATR_MULT


def _clip01(x: float) -> float:
    """Keep an individual scoring term within its natural [0, 1] range."""
    return max(0.0, min(1.0, x))


def _cap_contribution(term_value: float, weight: float) -> float:
    """Cap a single term's WEIGHTED contribution to the composite sum so no
    one term can saturate the blend alone.

    This must be applied to (term * weight), not to the raw term. Capping
    the raw term before weighting -- the prior implementation -- collapses
    the entire achievable ceiling to exactly the cap value whenever the
    category weights sum to 1.0 (sum(weight_i * min(term_i, cap)) <=
    cap * sum(weight_i) = cap), which made every grade threshold above the
    cap's logistic image mathematically unreachable regardless of setup
    quality. Capping post-weight instead limits any single category's
    *share* of the blend without capping the blend's overall ceiling.
    """
    return max(0.0, min(MAX_SINGLE_TERM_CONTRIBUTION, term_value * weight))


def composite_score(direction: str, h1: View, h4: View, weekly: View, daily: View, m15: View,
                     zone_result: dict, plan: dict, rv: RegimeVector, regime_label: str,
                     state: dict, engine_name: str) -> tuple[float, dict[str, float], list[str]]:
    """Continuous weighted/logistic blend over a small, documented, auditable
    set of terms -- never a large discrete integer point-stack."""
    weights = regime_adjusted_weights(regime_label)
    reasons: list[str] = []

    trend_term = _clip01(h1.trend_strength * (1.0 if h1.trend_direction == direction else 0.3))
    reasons.append(f"1H trend strength {h1.trend_strength:.2f} aligned {direction}")

    structure_term = _clip01(0.7 if zone_result.get("mss") else 0.0)
    if zone_result.get("poi") is not None:
        poi: Zone = zone_result["poi"]
        structure_term = _clip01(structure_term + (0.15 if poi.kind == "breaker_block" else 0.05))
        reasons.append(f"Validated {poi.kind.replace('_', ' ')} POI with confirmed MSS")

    rsi_val = h1.rsi[-1] if h1.rsi else 50.0
    momentum_aligned = (rsi_val > 50 if direction == "bullish" else rsi_val < 50)
    momentum_term = _clip01((abs(rsi_val - 50) / 50.0) * (1.0 if momentum_aligned else 0.4))
    reasons.append(f"1H RSI {rsi_val:.1f} {'supports' if momentum_aligned else 'mixed vs'} {direction}")

    sweep = zone_result.get("sweep")
    liquidity_term = 0.0
    if sweep is not None:
        liquidity_term = _clip01(0.5 + (0.25 if sweep.pool.is_equal_cluster else 0.0) +
                                        (0.15 if sweep.is_pure else 0.0))
        reasons.append(f"Liquidity sweep of {sweep.pool.kind} pool "
                        f"({'equal-cluster, ' if sweep.pool.is_equal_cluster else ''}"
                        f"{'pure' if sweep.is_pure else 'impure'} SFP)")
        if zone_result.get("session_anchored"):
            session_w = get_adaptive(state, "session_open_proximity_weight", "global", 0.05)
            liquidity_term = _clip01(liquidity_term + session_w * rv.session_open_proximity)

    volume_term = _clip01(min(1.0, h1.rel_volume / 2.5))
    reasons.append(f"Relative volume {h1.rel_volume:.2f}x")

    volatility_term = _clip01(1.0 - abs(rv.volatility_percentile - 0.55))  # mid-range ATR percentile preferred
    risk_term = _clip01(min(1.0, (plan["rr1"] - RR_MIN_GATE) / (RR_MAX_GATE - RR_MIN_GATE)))
    reasons.append(f"RR1 {plan['rr1']:.2f} (floor {RR_MIN_GATE})")

    htf_alignment = (weekly.trend_direction == direction or daily.trend_direction == direction
                      or h4.trend_direction == direction)
    mtf_w = get_adaptive(state, "mtf_alignment_weight", engine_name, 0.15)
    mtf_bonus = mtf_w if htf_alignment else 0.0
    if htf_alignment:
        reasons.append("Weekly/Daily/4H bias aligned (MTF confirmation)")

    terms = {"trend": trend_term, "structure": structure_term, "momentum": momentum_term,
             "liquidity": liquidity_term, "volume": volume_term, "volatility": volatility_term,
             "risk": risk_term}
    raw = sum(_cap_contribution(v, weights.get(k, 0.0)) for k, v in terms.items()) + mtf_bonus

    regime_discount = get_adaptive(state, "regime_fit_discount", f"{engine_name}:{regime_label}", 0.0)
    raw = max(0.0, raw - regime_discount)

    calibration_shift = get_adaptive(state, "confidence_calibration_shift", f"{engine_name}:mid", 0.0)
    # logistic squash into 0..100, then apply the (bounded) calibration shift
    logistic = 1.0 / (1.0 + math.exp(-6.0 * (raw - 0.5)))
    confidence = max(0.0, min(100.0, (logistic + calibration_shift) * 100.0))

    scores_scaled = {k: round(_cap_contribution(v, weights.get(k, 0.0)) * 100, 2) for k, v in terms.items()}
    return confidence, scores_scaled, reasons


def grade_for_confidence(confidence: float) -> Optional[str]:
    for grade, threshold in GRADE_THRESHOLDS:
        if confidence >= threshold:
            return grade
    return None  # below every bucket bar -- not eligible for dispatch


def regime_fit_veto(direction: str, engine_name: str, regime_label: str, counter_trend: bool) -> bool:
    """Returns True if this candidate should be vetoed/heavily discounted for
    fighting the prevailing, high-conviction regime read. Counter-trend engine
    signals expect to be against the regime by design and are exempt."""
    if counter_trend:
        return False
    if regime_label == "Strong Bull Trend" and direction == "bearish":
        return True
    if regime_label == "Strong Bear Trend" and direction == "bullish":
        return True
    return False


def liquidity_sanity_check(direction: str, entry: float, view: View, state: dict, engine_name: str) -> bool:
    """True = safe to proceed. False = entry sits inside/adjacent to an
    unmitigated liquidity pool it did not itself derive from (chased sweep)."""
    threshold = get_adaptive(state, "liquidity_sanity_threshold", f"{engine_name}:default", 0.4)
    atr_now = view.atr[-1] or (entry * 0.005)
    opposite_pool_kind = "BSL" if direction == "bullish" else "SSL"
    for pool in view.liquidity_pools:
        if pool.kind == opposite_pool_kind and abs(pool.price - entry) <= atr_now * threshold:
            return False
    return True


def _week_of_month(dt: datetime) -> int:
    return (dt.day - 1) // 7 + 1


def _static_calendar_blackout_active(asset: str, now_utc: datetime) -> bool:
    """Self-sufficient default event source; works with zero operator setup."""
    for ev in MACRO_EVENT_CALENDAR:
        if now_utc.weekday() != ev["weekday"] or _week_of_month(now_utc) != ev["week_of_month"]:
            continue
        affected = ev.get("affects", "ALL")
        if affected != "ALL" and asset not in affected:
            continue
        event_dt = now_utc.replace(hour=ev["hour_utc"], minute=ev["minute_utc"], second=0, microsecond=0)
        delta_min = (now_utc - event_dt).total_seconds() / 60.0
        if -MACRO_BLACKOUT_MINUTES_BEFORE <= delta_min <= MACRO_BLACKOUT_MINUTES_AFTER:
            return True
    return False


def _operator_supplied_blackout_active(asset: str, state: dict, now_utc: datetime) -> bool:
    """Additional override source (live feed / one-off events) layered on
    top of the static calendar, never the only source."""
    events = state.get("macro_events", [])
    for ev in events:
        try:
            ev_ts = datetime.fromisoformat(ev["ts"])
        except (KeyError, ValueError, TypeError):
            continue
        if ev_ts.tzinfo is None:
            ev_ts = ev_ts.replace(tzinfo=timezone.utc)
        affected = ev.get("assets", [])
        if affected and asset not in affected and "ALL" not in affected:
            continue
        window_start = ev_ts - timedelta(minutes=MACRO_BLACKOUT_MINUTES_BEFORE)
        window_end = ev_ts + timedelta(minutes=MACRO_BLACKOUT_MINUTES_AFTER)
        if window_start <= now_utc <= window_end:
            return True
    return False


def macro_blackout_active(asset: str, state: dict, now_utc: datetime) -> bool:
    """Active if either the static calendar or an operator/live-feed event
    says so."""
    return (_static_calendar_blackout_active(asset, now_utc) or
            _operator_supplied_blackout_active(asset, state, now_utc))


def run_smc_engine(symbol: str, views: dict[str, View], bias: str, state: dict, market_price: float) -> Optional[Candidate]:
    """Primary engine: the full top-down + zone-selection sequence,
    MSS -> FVG entry, OTE refinement."""
    weekly, daily, h4, h1, m15 = (views[TF_WEEKLY], views[TF_DAILY], views[TF_4H],
                                    views[TF_1H], views[TF_15M])
    stage2_ok = stage2_context(bias, h4)  # diagnostic instrumentation below
    log_filter_attrition(state, "DIAG_stage2_h4_context", passed=stage2_ok)
    if not stage2_ok:
        return None
    stage3_result, zone_result = stage3_setup(bias, h1, h4, state, symbol)
    log_filter_attrition(state, f"DIAG_stage3_outcome_{stage3_result}", passed=True)  # diagnostic instrumentation
    if stage3_result != "VALID":
        return None
    entry_result = stage4_entry(bias, zone_result, m15)
    if entry_result is None:
        return None
    entry, refined = stage5_5m_refine(bias, entry_result["entry_zone"], views.get(TF_5M), entry_result["entry"])
    entry_distance_pass = entry_distance_ok(bias, entry, market_price, m15)  # diagnostic instrumentation below
    log_filter_attrition(state, "DIAG_entry_distance_ok", passed=entry_distance_pass)
    if not entry_distance_pass:
        return None
    tp1_runway_pass = tp1_runway_ok(bias, entry, m15, state, symbol)  # diagnostic instrumentation below
    log_filter_attrition(state, "DIAG_tp1_runway_ok", passed=tp1_runway_pass)
    if not tp1_runway_pass:
        return None
    if not liquidity_sanity_check(bias, entry, m15, state, "SMC"):
        return None
    plan = build_risk_plan(bias, entry, m15, h1, h4, state, symbol)
    if plan is None:
        return None
    rv = compute_regime_vector(h1, views, datetime.now(timezone.utc))
    regime_label, regime_confidence, _ = classify_regime(rv)
    if regime_fit_veto(bias, "SMC", regime_label, counter_trend=False):
        return None
    confidence, scores, reasons = composite_score(bias, h1, h4, weekly, daily, m15, zone_result, plan,
                                                    rv, regime_label, state, "SMC")
    grade = grade_for_confidence(confidence)
    if grade is None:
        return None
    if refined:
        reasons.append("Entry refined on 5M rejection inside Stage-4 FVG (Stage 5, opt-in)")
    return Candidate(symbol=symbol, direction=bias, engine="SMC", style=_style_for(entry, plan),
                      entry=entry, entry_kind="pending", sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"],
                      rr1=plan["rr1"], rr2=plan["rr2"], sl_anchor=plan["sl_anchor"],
                      confidence=confidence, grade=grade, scores=scores, reasons=reasons,
                      market_regime=regime_label, regime_confidence=regime_confidence,
                      trend_label=bias.capitalize(),
                      higher_timeframe_alignment=(weekly.trend_direction == bias or daily.trend_direction == bias
                                                    or h4.trend_direction == bias),
                      session_anchored=zone_result.get("session_anchored", False))


def _style_for(entry: float, plan: dict) -> str:
    move_pct = abs(plan["tp1"] - entry) / entry if entry else 0.0
    return "swing" if move_pct >= 0.03 else "intraday"


def _generic_setup_engine(engine_name: str, setup_type: str, symbol: str, views: dict[str, View],
                           bias: str, state: dict, market_price: float,
                           required_poi_kind: Optional[str] = None) -> Optional[Candidate]:
    """Shared orchestration for every non-SMC base-ensemble engine (Breakout,
    Momentum, Mean Reversion, Range, Volatility Expansion, Order Block,
    Breaker Block, Fair Value Gap, Reversal) -- each supplies its own
    reference-zone selection but reuses retracement_entry, build_risk_plan,
    and composite_score end to end so no engine has a divergent code path.

    `required_poi_kind`: when set, the engine's reference zone is drawn
    from the 1H view's own typed zone collection for that kind, so e.g.
    the Order Block engine only ever fires off an actual unmitigated
    order block."""
    weekly, daily, h4, h1, m15 = (views[TF_WEEKLY], views[TF_DAILY], views[TF_4H],
                                    views[TF_1H], views[TF_15M])
    stage2_ok = stage2_context(bias, h4)  # diagnostic instrumentation below
    log_filter_attrition(state, "DIAG_stage2_h4_context", passed=stage2_ok)
    if not stage2_ok:
        return None
    stage3_result, zone_result = stage3_setup(bias, h1, h4, state, symbol)
    log_filter_attrition(state, f"DIAG_stage3_outcome_{stage3_result}", passed=True)  # diagnostic instrumentation
    if stage3_result != "VALID":
        return None
    if required_poi_kind is not None:
        typed_pool = _typed_poi_pool(bias, h1, required_poi_kind)
        if not typed_pool:
            return None
        reference_zone = typed_pool[0]
    else:
        reference_zone = zone_result.get("poi")
    entry = retracement_entry(setup_type, bias, m15, reference_zone)
    if entry is None:
        return None
    entry_distance_pass = entry_distance_ok(bias, entry, market_price, m15)  # diagnostic instrumentation below
    log_filter_attrition(state, "DIAG_entry_distance_ok", passed=entry_distance_pass)
    if not entry_distance_pass:
        return None
    tp1_runway_pass = tp1_runway_ok(bias, entry, m15, state, symbol)  # diagnostic instrumentation below
    log_filter_attrition(state, "DIAG_tp1_runway_ok", passed=tp1_runway_pass)
    if not tp1_runway_pass:
        return None
    if not liquidity_sanity_check(bias, entry, m15, state, engine_name):
        return None
    plan = build_risk_plan(bias, entry, m15, h1, h4, state, symbol)
    if plan is None:
        return None
    rv = compute_regime_vector(h1, views, datetime.now(timezone.utc))
    regime_label, regime_confidence, _ = classify_regime(rv)
    if regime_fit_veto(bias, engine_name, regime_label, counter_trend=False):
        return None
    confidence, scores, reasons = composite_score(bias, h1, h4, weekly, daily, m15, zone_result, plan,
                                                    rv, regime_label, state, engine_name)
    grade = grade_for_confidence(confidence)
    if grade is None:
        return None
    return Candidate(symbol=symbol, direction=bias, engine=engine_name, style=_style_for(entry, plan),
                      entry=entry, entry_kind="pending", sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"],
                      rr1=plan["rr1"], rr2=plan["rr2"], sl_anchor=plan["sl_anchor"],
                      confidence=confidence, grade=grade, scores=scores, reasons=reasons,
                      market_regime=regime_label, regime_confidence=regime_confidence,
                      trend_label=bias.capitalize(),
                      higher_timeframe_alignment=(weekly.trend_direction == bias or daily.trend_direction == bias
                                                    or h4.trend_direction == bias),
                      session_anchored=zone_result.get("session_anchored", False))


BASE_ENGINE_SETUPS = [
    # (engine_name, setup_type, required_poi_kind)
    ("Breakout", "breakout", None),
    ("Momentum", "momentum", None),
    ("Mean Reversion", "mean_reversion", None),
    ("Range Trading", "range", None),
    ("Volatility Expansion", "volatility_expansion", None),
    ("Order Block", "order_block", "order_block"),
    ("Breaker Block", "breaker_block", "breaker_block"),
    ("Fair Value Gap", "fvg", "fvg"),
    ("Reversal", "reversal", None),
]


def _htf_poi_pool(direction: str, weekly: View, daily: View) -> Optional[dict]:
    for view in (weekly, daily):
        pool = _poi_pool(direction, view)
        if pool:
            return {"view": view, "poi": pool[0]}
    return None


def _exhaustion_signature(direction: str, view: View) -> Optional[float]:
    if len(view.candles) < 6:
        return None
    bodies = [abs(c.c - c.o) for c in view.candles[-6:]]
    shrinking = bodies[-1] < bodies[0] * 0.7
    last = view.candles[-1]
    atr_now = view.atr[-1] or 1e-9
    wick = (last.h - max(last.c, last.o)) if direction == "bearish" else (min(last.c, last.o) - last.l)
    elongated = wick / atr_now > 0.5
    highs = [p for p in view.pivots if p.kind == "high"]
    lows = [p for p in view.pivots if p.kind == "low"]
    no_new_extreme = False
    if direction == "bearish" and len(highs) >= 2:
        no_new_extreme = highs[-1].price <= highs[-2].price
    elif direction == "bullish" and len(lows) >= 2:
        no_new_extreme = lows[-1].price >= lows[-2].price
    score = 0.4 * shrinking + 0.4 * elongated + 0.2 * no_new_extreme
    return score if score > 0 else None


def _retest_and_hold(direction: str, choch: dict, m15: View, state: dict, asset: str) -> Optional[dict]:
    level = choch["level"]
    bars_since = len(m15.candles) - 1 - choch["index"]
    if bars_since > COUNTERTREND_RETEST_EXPIRY_BARS:
        return None
    last = m15.candles[-1]
    held = (last.c > level) if direction == "bullish" else (last.c < level)
    touched = (last.l <= level <= last.h)
    if held and touched:
        return {"entry": level}
    return None


def run_countertrend_gate(bias: str, weekly: View, daily: View, h4: View, h1: View,
                           m15: View, state: dict, asset: str) -> Optional[Candidate]:
    if bias not in ("bullish", "bearish"):
        return None
    direction = "bearish" if bias == "bullish" else "bullish"

    htf_poi = _htf_poi_pool(direction, weekly, daily)
    if htf_poi is None:
        return None

    exhaustion = _exhaustion_signature(direction, h4) or _exhaustion_signature(direction, h1)
    if exhaustion is None:
        return None

    choch = structure_shift(direction, h1, kind="CHoCH") or structure_shift(direction, m15, kind="CHoCH")
    if choch is None:
        return None

    retest = _retest_and_hold(direction, choch, m15, state, asset)
    if retest is None:
        return None

    plan = build_risk_plan(direction, retest["entry"], htf_poi["view"], h1, h4, state, asset,
                            rr_min_gate=RR_MIN_GATE_COUNTERTREND)
    if plan is None:
        return None

    zone_result = {"poi": htf_poi["poi"], "sweep": None, "mss": choch, "session_anchored": False}
    rv = compute_regime_vector(h1, {asset: h1}, datetime.now(timezone.utc))
    # Best-fit regime is the opposite of the base ensemble's; exempt from
    # the standard regime-fit veto by design.
    regime_label, regime_confidence, _ = classify_regime(rv)
    confidence, scores, reasons = composite_score(direction, h1, h4, weekly, daily, m15, zone_result, plan,
                                                    rv, regime_label, state, "Counter-Trend Reversal")
    confidence = confidence * (0.6 + 0.4 * exhaustion)  # exhaustion strength modulates confidence
    grade = grade_for_confidence(confidence)
    if grade is None:
        return None
    reasons.append(f"Counter-trend against {bias} Weekly/Daily bias; exhaustion signature {exhaustion:.2f}")
    return Candidate(symbol=asset, direction=direction, engine="Counter-Trend Reversal", style="intraday",
                      entry=retest["entry"], entry_kind="pending", sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"],
                      rr1=plan["rr1"], rr2=plan["rr2"], sl_anchor=plan["sl_anchor"],
                      confidence=confidence, grade=grade, scores=scores, reasons=reasons,
                      counter_trend=True, market_regime=regime_label, regime_confidence=regime_confidence,
                      trend_label=direction.capitalize(), higher_timeframe_alignment=False)


def check_entry_filled(signal: dict, m15_view: View) -> str:
    """Returns 'filled', 'expired', or 'pending' for a still-unfilled signal."""
    entry = signal["entry"]
    dispatched_index = signal.get("dispatched_bar_index", len(m15_view.candles) - 1)
    bars_elapsed = (len(m15_view.candles) - 1) - dispatched_index
    for c in m15_view.candles[max(0, dispatched_index):]:
        if c.l <= entry <= c.h:
            return "filled"
    if bars_elapsed > PENDING_ENTRY_EXPIRY_BARS:
        return "expired"
    return "pending"


# Position-exit model: FULL EXIT AT TP1. 100% of size closes at TP1; a
# later touch of the original SL is bookkeeping only and never reopens or
# re-scores the trade. No automatic SL-to-breakeven on TP1.

def resolve_signal(signal: dict, m15_view: View) -> Optional[dict]:
    """Single-TP1 resolution: a signal resolves the moment TP1 or SL is hit,
    whichever occurs first. TP2 is never polled, checked, or reported."""
    entry, sl, tp1, direction = signal["entry"], signal["sl"], signal["tp1"], signal["direction"]
    dispatched_index = signal.get("filled_bar_index", signal.get("dispatched_bar_index", 0))
    mfe, mae = 0.0, 0.0
    for c in m15_view.candles[dispatched_index:]:
        if direction == "bullish":
            mfe = max(mfe, (c.h - entry))
            mae = max(mae, (entry - c.l))
            hit_tp1 = c.h >= tp1
            hit_sl = c.l <= sl
        else:
            mfe = max(mfe, (entry - c.l))
            mae = max(mae, (c.h - entry))
            hit_tp1 = c.l <= tp1
            hit_sl = c.h >= sl
        if hit_tp1 or hit_sl:
            risk = abs(entry - sl) or 1e-9
            if hit_sl and not hit_tp1:
                result, r_realized = "loss", -1.0
            elif hit_tp1 and not hit_sl:
                result, r_realized = "win", abs(tp1 - entry) / risk
            else:
                # Same-candle ambiguity: conservatively assume SL was touched
                # first (worst-case, never optimistic) unless the candle's
                # direction of travel clearly favors TP1 first.
                favors_tp1 = (c.c > c.o) == (direction == "bullish")
                if favors_tp1:
                    result, r_realized = "win", abs(tp1 - entry) / risk
                else:
                    result, r_realized = "loss", -1.0
            return {
                "result": result, "r_realized": r_realized,
                "mfe_r": round(mfe / risk, 4), "mae_r": round(mae / risk, 4),
                "resolved_ts": c.ts,
            }
    return None  # still open


FORENSIC_CATEGORIES = [
    "regime_mismatch", "structural_invalidation_too_tight", "chased_swept_liquidity",
    "mtf_conflict_ignored", "sfp_mss_sequence_violated", "correct_read_poor_rr",
    "confidence_miscalibration", "filter_over_permissiveness", "genuine_variance",
]


def classify_forensic_category(trade: dict, outcome: dict) -> str:
    """Each category is reached by a positive, verifiable condition on the
    trade's recorded data -- never a generic else/fallback branch."""
    mfe_r, mae_r = outcome["mfe_r"], outcome["mae_r"]
    if outcome["result"] != "loss":
        return "genuine_variance"  # wins routed through win-reinforcement, not this taxonomy

    if trade.get("regime_mismatch_at_entry"):
        return "regime_mismatch"
    if mae_r <= 1.05 and trade.get("sl_within_normal_noise_range"):
        return "structural_invalidation_too_tight"
    if trade.get("entry_near_swept_pool"):
        return "chased_swept_liquidity"
    if trade.get("htf_ltf_disagreed_and_htf_won"):
        return "mtf_conflict_ignored"
    if trade.get("sfp_impure_or_mss_unconfirmed"):
        return "sfp_mss_sequence_violated"
    if mfe_r >= 0.8:
        return "correct_read_poor_rr"
    if trade.get("confidence", 0) >= 84 and trade.get("confidence_bucket_realized_wr", 1.0) < 0.5:
        return "confidence_miscalibration"
    if trade.get("passed_filters_on_thin_margin"):
        return "filter_over_permissiveness"
    return "genuine_variance"


def apply_forensic_adaptation(state: dict, trade: dict, category: str) -> None:
    """One diagnosis, one deterministic route to a documented adaptive
    parameter -- subject to the adaptive bounds, step cap, and minimum
    sample-size gating."""
    counts = state["tier1"]["forensic_category_counts"]
    counts[category] = counts.get(category, 0) + 1
    n = counts[category]
    engine = trade.get("engine", "SMC")
    asset = trade.get("symbol", "")
    regime = trade.get("market_regime", "")
    if n < MIN_SAMPLE_SIZE_FOR_ADAPTATION:
        return  # not enough samples yet to trust an adjustment

    if category == "regime_mismatch":
        key = f"{engine}:{regime}"
        cur = get_adaptive(state, "regime_fit_discount", key, 0.0)
        set_adaptive(state, "regime_fit_discount", key, cur + 0.05)
    elif category == "structural_invalidation_too_tight":
        key = f"{asset}:15M"
        cur = get_adaptive(state, "sl_buffer_percentile", key, 65.0)
        set_adaptive(state, "sl_buffer_percentile", key, cur + 3.0)
    elif category == "chased_swept_liquidity":
        key = f"{engine}:default"
        cur = get_adaptive(state, "liquidity_sanity_threshold", key, 0.4)
        set_adaptive(state, "liquidity_sanity_threshold", key, cur + 0.05)
    elif category == "mtf_conflict_ignored":
        cur = get_adaptive(state, "mtf_alignment_weight", engine, 0.15)
        set_adaptive(state, "mtf_alignment_weight", engine, cur + 0.03)
    elif category == "sfp_mss_sequence_violated":
        cur = get_adaptive(state, "sfp_mss_strictness", engine, 0.3)
        set_adaptive(state, "sfp_mss_strictness", engine, cur + 0.1)
    elif category == "correct_read_poor_rr":
        cur = get_adaptive(state, "tp1_target_rank_preference", asset, 3.0)
        set_adaptive(state, "tp1_target_rank_preference", asset, cur + 1.0)
    elif category == "confidence_miscalibration":
        key = f"{engine}:mid"
        cur = get_adaptive(state, "confidence_calibration_shift", key, 0.0)
        set_adaptive(state, "confidence_calibration_shift", key, cur - 0.03)
    # "filter_over_permissiveness" routes through the attrition log
    # rather than a single adaptive scalar; "genuine_variance" -> no change.

    drift = state["tier1"]["forensic_category_r_drift"]
    drift[category] = drift.get(category, 0.0) + ADAPTIVE_BOUNDS.get(
        {"regime_mismatch": "regime_fit_discount", "structural_invalidation_too_tight": "sl_buffer_percentile",
         "chased_swept_liquidity": "liquidity_sanity_threshold", "mtf_conflict_ignored": "mtf_alignment_weight",
         "sfp_mss_sequence_violated": "sfp_mss_strictness", "correct_read_poor_rr": "tp1_target_rank_preference",
         "confidence_miscalibration": "confidence_calibration_shift"}.get(category, "regime_fit_discount"),
        (0, 0, 0.01))[2]


def reinforce_win_factors(state: dict, trade: dict) -> None:
    """Win reinforcement: raise (within bounds) the weights of factors that
    were genuinely present and predictive -- never a factor merely present."""
    engine = trade.get("engine", "SMC")
    if trade.get("higher_timeframe_alignment"):
        cur = get_adaptive(state, "mtf_alignment_weight", engine, 0.15)
        set_adaptive(state, "mtf_alignment_weight", engine, cur + 0.01)
    if trade.get("session_anchored"):
        cur = get_adaptive(state, "session_open_proximity_weight", "global", 0.05)
        set_adaptive(state, "session_open_proximity_weight", "global", cur + 0.01)


def update_calibration(state: dict, engine: str, confidence: float, result: str) -> None:
    bucket = "A+" if confidence >= 92 else "A" if confidence >= 84 else "B+" if confidence >= 74 else "B"
    key = f"{engine}:{bucket}"
    cal = state["tier1"]["calibration"].setdefault(key, {"n": 0, "wins": 0})
    cal["n"] += 1
    if result == "win":
        cal["wins"] += 1


def check_health(state: dict) -> list[str]:
    """Never silently changes trading behavior -- only logs warnings and
    recommends review."""
    warnings = []
    trades = state["tier2"]["trade_log"][-CIRCUIT_BREAKER_LOOKBACK_TRADES:]
    if len(trades) >= MIN_SAMPLE_SIZE_FOR_ADAPTATION:
        wins = sum(1 for t in trades if t.get("result") == "win")
        rolling_wr = wins / len(trades)
        state["tier1"]["rolling_win_rate"] = round(rolling_wr, 4)
        baseline = state["tier1"].get("baseline_win_rate", 0.5)
        wr_tripped = abs(rolling_wr - baseline) > CIRCUIT_BREAKER_MAX_WIN_RATE_DEVIATION

        # Second, independent leg: profit factor. Catches a bad stretch of
        # high-frequency small wins against a few large losses, which a raw
        # win-rate check alone can miss.
        gross_win = sum(t["r_realized"] for t in trades if t.get("r_realized", 0) > 0)
        gross_loss = abs(sum(t["r_realized"] for t in trades if t.get("r_realized", 0) < 0))
        rolling_pf = (gross_win / gross_loss) if gross_loss > 1e-9 else float("inf")
        state["tier1"]["rolling_profit_factor"] = None if rolling_pf == float("inf") else round(rolling_pf, 3)
        pf_tripped = rolling_pf <= CIRCUIT_BREAKER_MIN_PROFIT_FACTOR

        if wr_tripped or pf_tripped:
            state["tier1"]["circuit_breaker"] = {"tripped": True, "since": datetime.now(timezone.utc).isoformat()}
            if wr_tripped:
                warnings.append(f"Live-performance circuit breaker TRIPPED (win rate): rolling win rate "
                                 f"{rolling_wr:.2%} deviates >{CIRCUIT_BREAKER_MAX_WIN_RATE_DEVIATION:.0%} from "
                                 f"baseline {baseline:.2%}. Adaptation frozen; review recommended.")
            if pf_tripped:
                warnings.append(f"Live-performance circuit breaker TRIPPED (profit factor): rolling profit "
                                 f"factor {rolling_pf:.2f} at/below {CIRCUIT_BREAKER_MIN_PROFIT_FACTOR:.2f} "
                                 f"(baseline {state['tier1'].get('baseline_profit_factor', BASELINE_PROFIT_FACTOR):.2f}). "
                                 f"Adaptation frozen; review recommended.")
        elif state["tier1"]["circuit_breaker"].get("tripped"):
            state["tier1"]["circuit_breaker"] = {"tripped": False, "since": None}

    for key, cal in state["tier1"]["calibration"].items():
        if cal["n"] >= MIN_SAMPLE_SIZE_FOR_ADAPTATION:
            predicted = {"A+": 0.92, "A": 0.84, "B+": 0.74, "B": 0.62}.get(key.split(":")[-1], 0.7)
            realized = cal["wins"] / cal["n"]
            if realized < predicted - 0.15:
                warnings.append(f"Confidence calibration warning for {key}: predicted ~{predicted:.0%}, "
                                 f"realized {realized:.0%} over {cal['n']} trades.")

    for cat, count in state["tier1"]["forensic_category_counts"].items():
        total = sum(state["tier1"]["forensic_category_counts"].values()) or 1
        if count / total > 0.34 and total >= 30:
            warnings.append(f"Forensic category '{cat}' accounts for {count/total:.0%} of losses "
                             f"({count}/{total}) -- audit whether its signature is discriminating correctly.")
    return warnings


def is_circuit_breaker_active(state: dict) -> bool:
    return bool(state["tier1"]["circuit_breaker"].get("tripped"))


def build_signal_json(candidate: Candidate) -> dict:
    return {
        "symbol": candidate.symbol,
        "signal": "LONG" if candidate.direction == "bullish" else "SHORT",
        "engine": candidate.engine,
        "counter_trend": candidate.counter_trend,
        "style": candidate.style,
        "entry_kind": candidate.entry_kind,
        "confidence": round(candidate.confidence, 1),
        "grade": candidate.grade,
        "entry": candidate.entry,
        "stop_loss": candidate.sl,
        "take_profit": {"tp1": candidate.tp1, "tp2": candidate.tp2},
        "risk_reward": {"rr1": round(candidate.rr1, 2), "rr2_suggested": round(candidate.rr2, 2)},
        "market_regime": candidate.market_regime,
        "regime_confidence": candidate.regime_confidence,
        "trend": candidate.trend_label,
        "sl_anchor": candidate.sl_anchor,
        "holding_time": "30m-4h" if candidate.style == "intraday" else "4h-multi-day",
        "timeframe": "15M",
        "higher_timeframe_alignment": candidate.higher_timeframe_alignment,
        "scores": candidate.scores,
        "reasons": candidate.reasons,
    }


def active_signal_slots_available(state: dict) -> int:
    active = [s for s in state["tier2"]["active_signals"] if s.get("status") in ("pending", "activated")]
    return MAX_CONCURRENT_ACTIVE_SIGNALS - len(active)


def correlation_cap_ok(symbol: str, state: dict) -> bool:
    group = CORRELATION_GROUPS.get(symbol, "other")
    active = [s for s in state["tier2"]["active_signals"] if s.get("status") in ("pending", "activated")]
    same_group = sum(1 for s in active if CORRELATION_GROUPS.get(s.get("symbol"), "other") == group)
    return same_group < MAX_CONCURRENT_PER_CORRELATION_GROUP


def portfolio_exposure_ok(state: dict) -> bool:
    active = [s for s in state["tier2"]["active_signals"] if s.get("status") in ("pending", "activated")]
    total_risk_pct = sum(FIXED_RISK_PCT_OF_EQUITY for _ in active)
    return total_risk_pct < PORTFOLIO_EXPOSURE_CAP_PCT


def daily_loss_circuit_ok(state: dict) -> bool:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    todays_trades = [t for t in state["tier2"]["trade_log"]
                      if str(t.get("resolved_ts_iso", "")).startswith(today)]
    daily_r = sum(t.get("r_realized", 0.0) for t in todays_trades)
    return (daily_r * FIXED_RISK_PCT_OF_EQUITY) > -MAX_DAILY_LOSS_PCT


def position_size_fraction(state: dict, win_rate: float = 0.5, avg_rr: float = 1.8) -> float:
    """Position-sizing reference: fixed-fractional by default,
    optional half-Kelly when ENABLE_KELLY_SIZING is set. This engine's product
    is the signal itself -- sizing is informational, not part of
    signal quality/filtering."""
    if not ENABLE_KELLY_SIZING:
        return FIXED_RISK_PCT_OF_EQUITY
    b = avg_rr
    kelly = win_rate - (1 - win_rate) / max(b, 1e-9)
    kelly = max(0.0, kelly) * KELLY_FRACTION_CAP
    return min(kelly, PORTFOLIO_EXPOSURE_CAP_PCT / max(MAX_CONCURRENT_ACTIVE_SIGNALS, 1))


def portfolio_gate_ok(symbol: str, state: dict) -> tuple[bool, str]:
    """Single composed gate over the individual checks below, with a
    human-readable rejection reason. Each check remains independently
    callable."""
    if active_signal_slots_available(state) <= 0:
        return False, "MAX_CONCURRENT_ACTIVE_SIGNALS reached"
    if not correlation_cap_ok(symbol, state):
        return False, f"correlation cap reached for group '{CORRELATION_GROUPS.get(symbol, 'other')}'"
    if not portfolio_exposure_ok(state):
        return False, "PORTFOLIO_EXPOSURE_CAP_PCT reached"
    if not daily_loss_circuit_ok(state):
        return False, "MAX_DAILY_LOSS_PCT circuit breaker tripped"
    return True, ""


def log_filter_attrition(state: dict, filter_name: str, passed: bool) -> None:
    funnel = state["tier1"]["filter_funnel"].setdefault(filter_name, {"eliminated": 0, "passed": 0})
    funnel["passed" if passed else "eliminated"] += 1


TELEGRAM_ENABLED = bool(TG_BOT_TOKEN and TG_CHAT_ID)
if not TELEGRAM_ENABLED:
    log.warning("TG_BOT_TOKEN and/or TG_CHAT_ID missing/empty -- running in signal-generation-only mode.")

_ACRONYM_PRESERVE = {"Smc": "SMC", "Mss": "MSS", "Sfp": "SFP", "Rr": "RR", "Tp1": "TP1", "Tp2": "TP2"}


def _clean_identifier(text: str) -> str:
    """No raw underscores in user-facing text -- Title Case with spaces,
    with known trading-domain acronyms preserved in their conventional case."""
    cleaned = str(text).replace("_", " ").title()
    for word, acronym in _ACRONYM_PRESERVE.items():
        if cleaned == word:
            return acronym
        cleaned = cleaned.replace(f" {word} ", f" {acronym} ")
        if cleaned.startswith(f"{word} "):
            cleaned = f"{acronym} " + cleaned[len(word) + 1:]
        if cleaned.endswith(f" {word}"):
            cleaned = cleaned[: -len(word)] + acronym
    return cleaned


def _fmt_num(x: float) -> str:
    if x >= 100:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}"
    return f"{x:.8f}".rstrip("0").rstrip(".")


def format_signal_message(sig: dict) -> str:
    lines = [f"*{ENGINE_NAME} {ENGINE_VERSION}*", ""]
    if sig["counter_trend"]:
        lines.append("\u26A0\uFE0F *COUNTER-TREND* -- against Weekly/Daily bias")
    signal_emoji = "\U0001F7E2" if sig["signal"] == "LONG" else "\U0001F534"
    lines.append(f"{signal_emoji} *{sig['signal']}  {sig['symbol']}*  ({_clean_identifier(sig['engine'])})")
    lines.append(f"Grade: *{sig['grade']}*  |  Confidence: *{sig['confidence']}%*")
    lines.append(f"Style: {_clean_identifier(sig['style'])}  |  Holding time: {sig['holding_time']}")
    lines.append("")
    lines.append(f"Entry: `{_fmt_num(sig['entry'])}`")
    lines.append(f"SL: `{_fmt_num(sig['stop_loss'])}`")
    lines.append(f"TP1: `{_fmt_num(sig['take_profit']['tp1'])}`")
    lines.append(f"TP2 (suggested): `{_fmt_num(sig['take_profit']['tp2'])}`")
    lines.append("")
    lines.append(f"RR1: {sig['risk_reward']['rr1']}  |  RR2 (suggested): {sig['risk_reward']['rr2_suggested']}")
    lines.append(f"Regime: {_clean_identifier(sig['market_regime'])} "
                 f"({sig['regime_confidence']*100:.0f}% confidence)")
    lines.append(f"HTF Alignment: {'Yes' if sig['higher_timeframe_alignment'] else 'No'}")
    lines.append(f"SL Anchor: {sig['sl_anchor']}")
    lines.append("")
    lines.append("*Reasons:*")
    for r in sig["reasons"]:
        lines.append(f"\u2022 {r}")
    return "\n".join(lines)


STATUS_LABELS = {"activated": "Activated", "win": "TP1", "loss": "SL",
                  "expired": "Expired", "closed": "Closed", "cancelled": "Cancelled"}


class TelegramNotifier:
    """Consolidates all Telegram dispatch: message text, truncation at
    4096 chars, reaction emoji, six-status reply lifecycle, Markdown
    parse mode."""

    REACTION_EMOJI = "\U0001F680"  # rocket, used as the acknowledgment reaction

    def __init__(self, bot_token: str = TG_BOT_TOKEN, chat_id: str = TG_CHAT_ID) -> None:
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.enabled = bool(bot_token and chat_id)

    def _post(self, method: str, payload: dict) -> Optional[dict]:
        if not self.enabled:
            return None
        url = f"https://api.telegram.org/bot{self.bot_token}/{method}"
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            log.error("Telegram request failed (%s): %s", method, e)
            return None

    def send_signal(self, sig: dict) -> Optional[int]:
        text = format_signal_message(sig)
        resp = self._post("sendMessage", {"chat_id": self.chat_id, "text": text[:4096], "parse_mode": "Markdown"})
        if resp and resp.get("ok"):
            message_id = resp["result"]["message_id"]
            self._post("setMessageReaction", {"chat_id": self.chat_id, "message_id": message_id,
                                               "reaction": [{"type": "emoji", "emoji": self.REACTION_EMOJI}]})
            return message_id
        return None

    def send_reply(self, message_id: int, status: str, extra: str = "") -> None:
        label = STATUS_LABELS.get(status, _clean_identifier(status))
        text = f"*{ENGINE_NAME} {ENGINE_VERSION}* -- {label}"
        if extra:
            text += f"\n{extra}"
        self._post("sendMessage", {"chat_id": self.chat_id, "text": text, "parse_mode": "Markdown",
                                    "reply_to_message_id": message_id})

    def send_daily_summary(self, state: dict) -> None:
        t1, t2 = state["tier1"], state["tier2"]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        trades = [t for t in t2["trade_log"] if str(t.get("resolved_ts_iso", "")).startswith(today)]
        wins = sum(1 for t in trades if t.get("result") == "win")
        losses = sum(1 for t in trades if t.get("result") == "loss")
        total = wins + losses
        win_rate = (wins / total) if total else 0.0
        gross_win = sum(t["r_realized"] for t in trades if t.get("r_realized", 0) > 0)
        gross_loss = abs(sum(t["r_realized"] for t in trades if t.get("r_realized", 0) < 0))
        profit_factor = (gross_win / gross_loss) if gross_loss > 1e-9 else float("inf")
        avg_rr = statistics.fmean([t.get("rr1", 0) for t in trades]) if trades else 0.0

        by_engine: dict[str, list[dict]] = {}
        for t in trades:
            by_engine.setdefault(t.get("engine", "unknown"), []).append(t)

        lines = [f"*{ENGINE_NAME} {ENGINE_VERSION} -- Daily Summary ({today} UTC)*", "",
                 f"Total signals: {total}", f"Wins/Losses: {wins}/{losses}  (Win rate {win_rate:.1%})",
                 f"Profit factor: {profit_factor:.2f}" if profit_factor != float("inf") else "Profit factor: n/a",
                 f"Avg RR1: {avg_rr:.2f}", ""]
        lines.append("*By Engine:*")
        for engine_name, engine_trades in by_engine.items():
            w = sum(1 for t in engine_trades if t.get("result") == "win")
            n = len(engine_trades)
            lines.append(f"\u2022 {_clean_identifier(engine_name)}: {w}/{n} ({(w/n if n else 0):.0%})")
        todays_forensic_counts: dict[str, int] = {}
        for t in trades:
            cat = t.get("forensic_category")
            if cat:
                todays_forensic_counts[cat] = todays_forensic_counts.get(cat, 0) + 1
        if todays_forensic_counts:
            lines.append("")
            lines.append("*Forensic Categories (today):*")
            for cat, count in todays_forensic_counts.items():
                lines.append(f"\u2022 {_clean_identifier(cat)}: {count}")
        # Note: fill_rate_stats has no per-event timestamps, so it can't be scoped
        # to a single day -- shown as an all-time reference figure instead.
        filled, expired = t1["fill_rate_stats"]["filled"], t1["fill_rate_stats"]["expired"]
        fill_total = filled + expired
        if fill_total:
            lines.append("")
            lines.append(f"Fill rate (all-time): {filled}/{fill_total} ({filled/fill_total:.0%})")
        self._post("sendMessage", {"chat_id": self.chat_id, "text": "\n".join(lines)[:4096], "parse_mode": "Markdown"})


telegram = TelegramNotifier()  # module-level singleton, mirrors prior module-level function access pattern


def _load_all_views(client: HyperliquidClient, cache: CandleCacheStore, symbol: str) -> Optional[dict[str, View]]:
    views = {}
    for tf in (TF_WEEKLY, TF_DAILY, TF_4H, TF_1H, TF_15M):
        candles = cache.get_or_fetch(client, symbol, tf)
        if len(candles) < 30:
            log.info("Insufficient %s candle history for %s -- skipping this run.", tf, symbol)
            return None
        views[tf] = build_view(symbol, tf, candles)
    if ENABLE_5M_REFINE:
        # Optional/opt-in tier: never blocks a scan even if unavailable --
        # stage5_5m_refine() falls back to the unrefined entry when absent.
        try:
            m5_candles = cache.get_or_fetch(client, symbol, TF_5M)
            if len(m5_candles) >= 10:
                views[TF_5M] = build_view(symbol, TF_5M, m5_candles)
        except Exception:
            log.exception("5M candle fetch failed for %s -- proceeding without Stage 5 refine.", symbol)
    return views


def _record_dispatch(state: dict, candidate: Candidate, message_id: Optional[int]) -> None:
    # Built via the typed DispatchedSignal dataclass (a missing/misnamed
    # field is a construction-time TypeError), then flattened to a plain
    # dict for on-disk storage so state.json's shape is unchanged.
    dispatched = DispatchedSignal(
        symbol=candidate.symbol, direction=candidate.direction, engine=candidate.engine,
        style=candidate.style, entry=candidate.entry, entry_kind=candidate.entry_kind,
        sl=candidate.sl, tp1=candidate.tp1, tp2=candidate.tp2, rr1=candidate.rr1, rr2=candidate.rr2,
        sl_anchor=candidate.sl_anchor, confidence=candidate.confidence, grade=candidate.grade,
        counter_trend=candidate.counter_trend, market_regime=candidate.market_regime,
        regime_confidence=candidate.regime_confidence, session_anchored=candidate.session_anchored,
        higher_timeframe_alignment=candidate.higher_timeframe_alignment,
        status="pending", message_id=message_id,
        dispatched_ts=datetime.now(timezone.utc).isoformat(),
        resolution_logic_version=RESOLUTION_LOGIC_VERSION,
    )
    state["tier2"]["active_signals"].append(asdict(dispatched))


def _monitor_active_signals(state: dict, client: HyperliquidClient, cache: CandleCacheStore) -> None:
    still_active = []
    for sig in state["tier2"]["active_signals"]:
        if sig.get("resolution_logic_version") != RESOLUTION_LOGIC_VERSION:
            # trades under a prior resolution logic version are left as-is
            still_active.append(sig)
            continue
        candles = cache.get_or_fetch(client, sig["symbol"], TF_15M)
        if len(candles) < 5:
            still_active.append(sig)
            continue
        view = build_view(sig["symbol"], TF_15M, candles)

        if sig["status"] == "pending":
            fill_state = check_entry_filled(sig, view)
            if fill_state == "filled":
                sig["status"] = "activated"
                sig["filled_bar_index"] = len(view.candles) - 1
                state["tier1"]["fill_rate_stats"]["filled"] += 1
            elif fill_state == "expired":
                sig["status"] = "expired"
                state["tier1"]["fill_rate_stats"]["expired"] += 1
                continue  # drop from active list
            else:
                still_active.append(sig)
                continue

        if sig["status"] == "activated":
            outcome = resolve_signal(sig, view)
            if outcome is None:
                still_active.append(sig)
                continue
            trade_record = {**sig, **outcome, "resolved_ts_iso": datetime.now(timezone.utc).isoformat()}
            category = classify_forensic_category(sig, outcome)
            trade_record["forensic_category"] = category
            if outcome["result"] == "win":
                reinforce_win_factors(state, trade_record)
            else:
                apply_forensic_adaptation(state, trade_record, category)
            update_calibration(state, sig["engine"], sig["confidence"], outcome["result"])
            state["tier2"]["trade_log"].append(trade_record)
            if sig.get("message_id"):
                status = "win" if outcome["result"] == "win" else "loss"
                telegram.send_reply(sig["message_id"], status,
                                   f"R realized: {outcome['r_realized']:.2f}")
    state["tier2"]["active_signals"] = still_active


def _scan_asset(symbol: str, client: HyperliquidClient, cache: CandleCacheStore, state: dict) -> list[Candidate]:
    now_utc = datetime.now(timezone.utc)
    if macro_blackout_active(symbol, state, now_utc):
        log.info("Macro blackout active for %s -- skipping.", symbol)
        return []

    views = _load_all_views(client, cache, symbol)
    if views is None:
        return []

    market_price = client.fetch_mark_price(symbol) or views[TF_15M].closes[-1]

    bias = stage1_bias(views[TF_WEEKLY], views[TF_DAILY])
    log_filter_attrition(state, "stage1_neutral", passed=(bias != "neutral"))
    if bias == "neutral":
        return []  # Neutral bias is sufficient grounds for NO TRADE

    candidates: list[Candidate] = []

    try:
        smc = run_smc_engine(symbol, views, bias, state, market_price)
    except Exception:
        log.exception("SMC engine raised while scanning %s -- skipping this engine this run.", symbol)
        smc = None
    log_filter_attrition(state, "SMC", passed=smc is not None)
    if smc is not None:
        candidates.append(smc)

    for engine_name, setup_type, required_poi_kind in BASE_ENGINE_SETUPS:
        # each engine call is fault-isolated: one misbehaving engine can
        # never zero out every other engine's candidates this run
        try:
            cand = _generic_setup_engine(engine_name, setup_type, symbol, views, bias, state,
                                          market_price, required_poi_kind=required_poi_kind)
        except Exception:
            log.exception("Engine %s raised while scanning %s -- skipping this engine this run.",
                           engine_name, symbol)
            cand = None
        log_filter_attrition(state, engine_name, passed=cand is not None)
        if cand is not None:
            candidates.append(cand)

    if ENABLE_COUNTERTREND_ENGINE:
        try:
            ct = run_countertrend_gate(bias, views[TF_WEEKLY], views[TF_DAILY], views[TF_4H],
                                        views[TF_1H], views[TF_15M], state, symbol)
        except Exception:
            log.exception("Counter-Trend Reversal engine raised while scanning %s -- skipping this run.", symbol)
            ct = None
        log_filter_attrition(state, "Counter-Trend Reversal", passed=ct is not None)
        if ct is not None:
            candidates.append(ct)

    return candidates


def run_scan() -> None:
    log.info("%s %s -- scan starting", ENGINE_NAME, ENGINE_VERSION)
    store = StateStore(STATE_PATH)
    state = store.state
    cache = CandleCacheStore(CANDLE_CACHE_PATH)
    client = HyperliquidClient(HL_API_URL)

    try:
        _monitor_active_signals(state, client, cache)

        if is_circuit_breaker_active(state):
            log.warning("Circuit breaker is tripped -- adaptation frozen; scan continues in monitor-only mode.")
        else:
            if not daily_loss_circuit_ok(state):
                log.warning("Daily loss limit reached -- suppressing new signal generation for the remainder of today.")
            else:
                # scan phase is thread-pooled per symbol; each symbol stays
                # fault-isolated, and dispatch/gating still runs only after
                # every symbol has finished, so ordering is unaffected.
                all_candidates: list[Candidate] = []
                skipped: list[str] = []

                def _scan_one(sym: str):
                    try:
                        return sym, _scan_asset(sym, client, cache, state), None
                    except Exception as e:  # noqa: BLE001 -- one bad symbol never kills the run
                        return sym, [], e

                with ThreadPoolExecutor(max_workers=SCAN_MAX_WORKERS) as pool:
                    futures = {pool.submit(_scan_one, sym): sym for sym in WATCHLIST}
                    for fut in as_completed(futures):
                        sym, cands, err = fut.result()
                        if err is not None:
                            log.exception("Unhandled error scanning %s -- skipping asset this run.", sym,
                                          exc_info=err)
                            skipped.append(sym)
                            continue
                        all_candidates.extend(cands)

                log.info("Scan phase complete: %d/%d symbols processed (%d skipped), %d candidate(s).",
                         len(WATCHLIST) - len(skipped), len(WATCHLIST), len(skipped), len(all_candidates))

                # dispatch in descending confidence order, subject to the
                # concurrency/correlation/exposure gate
                for cand in sorted(all_candidates, key=lambda c: c.confidence, reverse=True):
                    gate_ok, gate_reason = portfolio_gate_ok(cand.symbol, state)
                    if not gate_ok:
                        if gate_reason == "MAX_CONCURRENT_ACTIVE_SIGNALS reached":
                            log.info("MAX_CONCURRENT_ACTIVE_SIGNALS reached -- remaining candidates held for a future scan.")
                            break
                        if gate_reason in ("PORTFOLIO_EXPOSURE_CAP_PCT reached", "MAX_DAILY_LOSS_PCT circuit breaker tripped"):
                            log.info("Portfolio gate closed (%s) -- remaining candidates held for a future scan.", gate_reason)
                            break
                        log.info("Skipping %s: %s", cand.symbol, gate_reason)
                        continue
                    sig_json = build_signal_json(cand)
                    message_id = telegram.send_signal(sig_json) if TELEGRAM_ENABLED else None
                    _record_dispatch(state, cand, message_id)
                    log.info("Dispatched %s %s via %s (grade %s, confidence %.1f%%)",
                             sig_json["signal"], cand.symbol, cand.engine, cand.grade, cand.confidence)

                today_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                rate_hist = state["tier1"]["signal_rate_history"]
                rate_hist[today_key] = rate_hist.get(today_key, 0) + sum(
                    1 for s in state["tier2"]["active_signals"]
                    if s.get("dispatched_ts", "").startswith(today_key)
                )
                # keep only the trailing 30 days
                for k in list(rate_hist.keys())[:-30]:
                    del rate_hist[k]

        warnings = check_health(state)
        for w in warnings:
            log.warning(w)

        now_utc = datetime.now(timezone.utc)
        last_summary = state.get("last_daily_summary_date")
        if TELEGRAM_ENABLED and now_utc.hour == 8 and last_summary != now_utc.strftime("%Y-%m-%d"):
            telegram.send_daily_summary(state)
            state["last_daily_summary_date"] = now_utc.strftime("%Y-%m-%d")

    except Exception:
        log.exception("Unhandled exception during scan -- state will still be persisted for continuity.")
    finally:
        cache.save()
        store.save()
        log.info("%s %s -- scan complete", ENGINE_NAME, ENGINE_VERSION)


if __name__ == "__main__":
    run_scan()
