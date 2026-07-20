#!/usr/bin/env python3
"""
ODYSSEY ADAPTIVE SIGNAL ENGINE — v1.2.0
========================================
Institutional-grade, self-learning, multi-strategy crypto signal engine for
Hyperliquid perpetuals. Single-file, dependency-light, GitHub-Actions-ready.

Key components:
  - 3-tier Macro/Structure/Execution multi-timeframe stack.
  - 13 specialized setup engines (SMC, order blocks, breakers, FVGs,
    liquidity sweeps, momentum, reversal, mean reversion, range, etc.)
    scored through a shared Decision Engine with regime-fit and
    correlation-cap logic.
  - PendingEntryTracker enforces real fill verification for every
    non-market entry; SL is never auto-repositioned to breakeven.
  - Tier 1 (permanent aggregates) / Tier 2 (bounded raw log) state split
    driving fully automatic, bounded, incremental learning.
  - Live-performance circuit breaker against a documented pre-deployment
    baseline.

v1.1.0 changes (signal-quality learning):
  - active_baseline now actually updates from live results once enough
    trades resolve (previously frozen at BASELINE_NOTE forever — the old
    docstring claimed it self-updated but no code path ever did it).
  - New confluence-level learning: each confluence tag (e.g.
    "liquidity_sweep", "fvg_reclaim") gets its own tracked win rate / avg-R
    and a learned quality multiplier, gated by the same MIN_SAMPLE_SIZE and
    ADAPT_MAX_STEP damping as everything else. The old confluence_bonus was
    purely a count of tags with zero notion of which tags actually predict
    wins — this replaces that with real per-tag learning.
  - New per-asset quality multiplier, same treatment, using the by_asset
    segment stats that were already being tracked but never fed back into
    scoring.
  - final_score now multiplies in engine_weight * confluence_quality_mult *
    asset_quality_mult, so signal quality is shaped by everything the
    engine has actually learned about what wins, not just its type.

v1.2.0 changes (signal-quality corrections, driven by 60-trade forensic
review showing 26.7% blended win rate, momentum at 20.6% WR / 57% of
volume, and a 16.0% long vs 34.3% short split):
  - update_engine_weights() now applies an accelerated (but still bounded
    and damped) step when a segment's trailing win rate is far below the
    live baseline, instead of always using the same ADAPT_MAX_STEP as a
    segment performing near baseline. Previously a segment could sit at a
    materially bad win rate for dozens of trades and only crawl a few
    points off its starting weight, because the fixed per-step cap
    shrinks geometrically as the weight approaches its target.
  - New setup-type-specific filter overrides
    (tier1.filter_thresholds["setup_overrides"]): a setup type whose
    realized win rate is materially below baseline gets its own, stricter
    min_confidence/min_score gate layered on top of (never below) the
    global thresholds, damped/bounded the same way as every other
    adaptive parameter via update_setup_overrides(). This lets a single
    bad-performing setup type get filtered harder without penalizing
    every other engine the way a global threshold bump would.
  - New setup_type+direction segment tracking (by_setup_direction) and a
    matching learned quality multiplier
    (tier1.setup_direction_quality), applied in final_score. This is what
    lets the engine learn that e.g. "momentum + long" specifically has
    been a worse combination than either factor alone would predict, and
    suppress that combination without touching momentum shorts or other
    engines' longs.
  - New optional per-signal confidence_components on Signal/RankedSignal
    records: each engine now records the individual terms that summed
    into its stated `confidence` (base rate, ADX bonus, alignment bonus,
    etc). These are persisted to the Tier-2 log for later analysis and
    are NOT fed back into scoring — confidence itself is not treated as
    reliable enough yet to be recalibrated from a components model this
    small; this just makes that recalibration possible once there's
    enough data, without materially changing filtering behavior today.
  - forensic_tag() now classifies wins using r_realized vs rr_tp1/rr_tp2
    instead of mfe_r. mfe_r is never updated on the same-candle
    tp1_tp2_same_candle resolution path, so trades that hit TP2 in the
    same candle as TP1 were being mislabeled as "TP1 secured" rather than
    a full extension. r_realized is always correct at resolution time and
    doesn't depend on that intra-candle bookkeeping.
  - One-time state migration (schema_version 2 -> 3) raises any
    filter_thresholds below the new tightened floor and seeds the
    momentum setup_override, so an existing deployment picks up the
    tightened gates immediately rather than waiting for the adaptive
    loop to slowly get there.

v1.2.1 changes (signal-flow fixes):
  - New symbol cooldown (symbol_cooldowns in state, COOLDOWN_BARS_AFTER_LOSS
    / COOLDOWN_BARS_AFTER_WIN): previously run_scan() computed which symbols
    were "active" *after* monitor_all() had already resolved and removed
    this scan's closed trades, so a symbol stopped out this scan was
    immediately re-scanned and could receive a brand-new signal in the same
    pass. A resolved trade now blocks new signals on that symbol for a
    bars-based cooldown (longer after a loss than a win).
  - New SETUP_PENDING_EXPIRY_BARS replacing the flat
    DEFAULT_PENDING_EXPIRY_BARS[TF_LTF] (12 bars/3h) for every pending
    setup. Near-market ATR-fraction entries (liquidity sweep, mean
    reversion, range, reversal) keep the 3h window. Structural zone entries
    -- especially SMC/order-block/breaker-block, drawn from 4H structure --
    get a longer window (up to 12h), since giving a 4H-structure retest only
    3h to happen was the main driver of "never filled within 12 bars"
    expiries.

v1.2.2 changes (notification quieting):
  - The "TP1 secured, SL later hit -> WIN" resolution message is no longer
    sent: TP1 was already reported when it hit, and SL later returning to
    the (never-repositioned) original stop doesn't change the outcome or
    require action, so the follow-up ping was pure noise. Still fully
    resolved, still fully counted in stats/cooldown -- just not messaged.
  - The "EXPIRED (no fill)" message is no longer sent. Expired pending
    signals still resolve (removed from active_signals, logged to tier2 for
    forensic review, excluded from win/loss stats as before) -- just
    silently, since there's no position and nothing actionable to report.

Scan-per-run model: an external scheduler (GitHub Actions, cron, etc.)
invokes this script every 15 minutes. All persistence lives in state.json
next to the script.
"""

from __future__ import annotations

import json
import os
import time
import random
import logging
import hashlib
import traceback
import urllib.request
import urllib.error
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Dict, Tuple

import numpy as np

# ==============================================================================
# SECTION A — GLOBAL CONFIGURATION
# ==============================================================================

ENGINE_NAME = "Odyssey Adaptive Signal Engine"
ENGINE_VERSION = "v1.2.2"

# Same watchlist across all six reference engines -> reused verbatim.
WATCHLIST: List[str] = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]

# Rough sector map for the correlation cap; unknown symbols default to
# their own singleton sector.
SECTOR_MAP: Dict[str, str] = {
    "BTC": "btc", "ETH": "eth",
    "SOL": "l1_alt", "AVAX": "l1_alt", "SUI": "l1_alt", "APT": "l1_alt",
    "NEAR": "l1_alt", "TAO": "l1_alt", "DOT": "l1_alt",
    "BNB": "bnb",
    "XRP": "payments", "XLM": "payments", "TRX": "payments", "LTC": "payments", "BCH": "payments",
    "DOGE": "meme", "PENGU": "meme",
    "LINK": "oracle_infra", "ONDO": "oracle_infra", "PENDLE": "oracle_infra",
    "AAVE": "defi", "UNI": "defi",
    "ADA": "l1_alt", "HYPE": "exchange", "ZEC": "privacy",
}

# Timeframes: 15M is the hard floor. 1M/2M/3M/5M are forbidden.
FORBIDDEN_TIMEFRAMES = {"1m", "2m", "3m", "5m"}
TF_MACRO = "1d"     # regime / macro bias
TF_HTF = "4h"       # structure, order blocks, breaker blocks
TF_MID = "1h"       # confirmation / MTF alignment
TF_LTF = "15m"      # execution / precision entry timing
ALL_TIMEFRAMES = [TF_MACRO, TF_HTF, TF_MID, TF_LTF]
assert not (set(ALL_TIMEFRAMES) & FORBIDDEN_TIMEFRAMES), "Forbidden timeframe configured"

CANDLE_LOOKBACK = {TF_MACRO: 200, TF_HTF: 300, TF_MID: 300, TF_LTF: 300}
TF_MS = {"15m": 15 * 60_000, "1h": 60 * 60_000, "4h": 4 * 60 * 60_000, "1d": 24 * 60 * 60_000}

SCAN_INTERVAL_MINUTES = 15

# --- Risk / signal-shape hard floors ---
TP1_RR_MIN = 1.5                 # hard floor, never relaxed
TP1_RR_SOFT_CEILING = 2.0        # natural upper end of TP1's honest range
MIN_ENTRY_SL_ATR_FRAC = 0.15     # min |entry-SL| as a fraction of ATR(LTF)
MIN_ENTRY_TP1_ATR_FRAC = 0.30    # min |entry-TP1| as a fraction of ATR(LTF)
MAX_PENDING_ENTRY_ATR_MULT = 2.5 # max distance a pending/zone entry may sit from market, in ATRs
LIQUIDITY_SWEEP_ENTRY_OFFSET_ATR_FRAC = 0.15  # limit offset from market for sweep-reclaim entries;
                                               # small enough that normal candle noise fills it within
                                               # a bar or two, instead of waiting for a full retest of
                                               # the (already-left-behind) reclaim price
MOMENTUM_STYLE_ENTRY_OFFSET_ATR_FRAC = 0.08   # tighter offset for momentum/breakout/trend/vol-expansion
                                               # entries. These theses are directional, not mean-reverting
                                               # -- a wide offset would only get filled on the weak setups
                                               # that stall and pull back, while the clean, strong moves
                                               # (the ones actually worth taking) run away unfilled. Kept
                                               # small enough to just need ordinary intrabar noise, not a
                                               # real retracement.

# --- Concurrency / breadth ---
MAX_CONCURRENT_ACTIVE_SIGNALS = 8
MAX_CONCURRENT_PER_SECTOR = 2
TARGET_SIGNALS_PER_DAY_MIN = 5
TARGET_SIGNALS_PER_DAY_MAX = 10

# --- Pending-entry lifecycle ---
DEFAULT_PENDING_EXPIRY_BARS = {TF_LTF: 12}   # ~3h on 15m bars for zone/limit-style entries
# v1.2.1: SETUP_PENDING_EXPIRY_BARS (per-setup override of the above) is
# defined right after the SetupType enum below, since it needs that type.

# v1.2.1: cooldown, in LTF (15m) bars, before a symbol becomes eligible for a
# new signal again after any active trade on it resolves. Previously there
# was no cooldown at all -- run_scan() computed "which symbols are active"
# *after* monitor_all() had already resolved and removed that scan's closed
# trades, so a symbol stopped out this scan was immediately re-scanned and
# could receive a brand-new signal in the same pass (visible as a loss
# notification and a fresh signal on the same symbol at the same timestamp).
# Longer after a loss than a win: a thesis that just got invalidated deserves
# more room before being re-tried on the same symbol.
COOLDOWN_BARS_AFTER_LOSS = 8   # ~2h on 15m bars
COOLDOWN_BARS_AFTER_WIN = 4    # ~1h on 15m bars

# --- Learning / adaptation bounds ---
MIN_SAMPLE_SIZE = 20             # per-segment trades required before adapting
ADAPT_MAX_STEP = 0.08            # max fractional change to any adaptive param per run
ENGINE_WEIGHT_MIN, ENGINE_WEIGHT_MAX = 0.35, 1.75   # relative to baseline 1.0
CONF_CALIBRATION_MIN, CONF_CALIBRATION_MAX = -0.20, 0.20
FILTER_THRESHOLD_MIN, FILTER_THRESHOLD_MAX = 0.30, 0.95

# Secondary, more conservative quality multipliers (v1.1.0). Kept tighter
# than ENGINE_WEIGHT bounds since these compound multiplicatively with
# engine_weight in final_score — wide bounds on every layer would let a
# signal's score swing far more than any single layer's own trustworthiness
# justifies at small sample sizes.
CONFLUENCE_QUALITY_MIN, CONFLUENCE_QUALITY_MAX = 0.70, 1.30
ASSET_QUALITY_MIN, ASSET_QUALITY_MAX = 0.70, 1.30

# Setup-type + direction combination quality (v1.2.0). Wider downside bound
# than confluence/asset quality on purpose: a specific setup+direction
# combination that's badly underperforming (e.g. momentum longs) should be
# suppressible much harder than a single confluence tag or asset, without
# needing a separate hard-coded gate for it. Upside bound stays modest —
# this system should be conservative about ever rewarding a combination
# more than the setup type's own engine_weight already does.
SETUP_DIRECTION_QUALITY_MIN, SETUP_DIRECTION_QUALITY_MAX = 0.40, 1.20

# --- Accelerated decay for severely underperforming segments (v1.2.0) ---
# The standard ADAPT_MAX_STEP is deliberately conservative so a handful of
# trades can't yank a weight around. But that same conservatism means a
# segment sitting far below baseline for many resolved trades in a row
# converges to its target very slowly (each step is a fraction of the
# *current* value, so it's a geometric, not linear, approach). When a
# segment's edge is this bad, "don't overfit to noise" no longer applies —
# there's enough signal to move faster while still remaining a damped,
# bounded step rather than a jump straight to target.
SEVERE_UNDERPERFORM_WR_GAP = 0.15     # win-rate gap below baseline to trigger acceleration
ACCELERATED_ADAPT_STEP_MULT = 3.0     # multiplier applied to ADAPT_MAX_STEP when triggered
ACCELERATED_ADAPT_STEP_CAP = 0.30     # hard ceiling on the accelerated step fraction itself

# --- Setup-type-specific filter overrides (v1.2.0) ---
SETUP_OVERRIDE_WR_GAP = 0.15          # win-rate gap below baseline to warrant its own gate
SETUP_OVERRIDE_MAX_STEP = 0.05        # damped step size for tightening/relaxing an override
SETUP_OVERRIDE_MAX_BUMP = 0.20        # max amount an override may sit above the global threshold

# --- Live baseline auto-update (v1.1.0) ---
# The circuit breaker and engine-weight expectancy math compare live results
# against this baseline. It starts at BASELINE_NOTE (a pre-deployment prior)
# and blends toward realized live performance once there's enough data to
# trust, using the same damped-step machinery as everything else so it can't
# jump on a handful of trades.
BASELINE_MIN_LIVE_TRADES = 40        # total resolved trades before blending starts
BASELINE_ADAPT_MAX_STEP = 0.05       # slower than parameter adaptation on purpose

# --- Live-performance circuit breaker ---
CIRCUIT_BREAKER_WINDOW = 30                  # rolling trades
CIRCUIT_BREAKER_WR_DROP = 0.15               # absolute win-rate drop vs baseline
CIRCUIT_BREAKER_PF_DROP = 0.35               # relative profit-factor drop vs baseline

# --- Tier-2 raw log retention ---
TIER2_RETENTION_DAYS = 15
TIER2_MAX_RECORDS = 1500

# --- Pre-deployment baseline for the circuit breaker ---
# Conservative institutional-SMC prior; replaced by live stats once enough
# trades resolve (see StateStore.get_effective_baseline).
BASELINE_NOTE = {
    "win_rate": 0.46,        # expected win rate at TP1-RR ~1.5-2.0 floor
    "profit_factor": 1.35,
    "avg_rr": 1.7,
}

STATE_PATH = os.environ.get("ODYSSEY_STATE_PATH", "state.json")
HL_INFO_URL = os.environ.get("HL_INFO_URL", "https://api.hyperliquid.xyz/info")
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("odyssey")

# ==============================================================================
# SECTION B — CORE DATA STRUCTURES
# ==============================================================================

class Regime(str, Enum):
    BULL_TREND = "bull_trend"
    BEAR_TREND = "bear_trend"
    RANGING = "ranging"
    CONSOLIDATION = "consolidation"
    EXPANSION = "expansion"
    HIGH_VOL_CHOP = "high_vol_chop"


class SetupType(str, Enum):
    SMC = "smc"
    TREND_CONTINUATION = "trend_continuation"
    BREAKOUT = "breakout"
    PULLBACK = "pullback"
    LIQUIDITY_SWEEP = "liquidity_sweep"
    ORDER_BLOCK = "order_block"
    BREAKER_BLOCK = "breaker_block"
    FAIR_VALUE_GAP = "fair_value_gap"
    MOMENTUM = "momentum"
    REVERSAL = "reversal"
    MEAN_REVERSION = "mean_reversion"
    RANGE_TRADING = "range_trading"
    VOLATILITY_EXPANSION = "volatility_expansion"


# Regimes each engine is best suited for; feeds the Decision Engine's
# regime-fit multiplier (never a hard gate).
ENGINE_REGIME_FIT: Dict[SetupType, List[Regime]] = {
    SetupType.SMC: [Regime.BULL_TREND, Regime.BEAR_TREND, Regime.EXPANSION],
    SetupType.TREND_CONTINUATION: [Regime.BULL_TREND, Regime.BEAR_TREND],
    SetupType.BREAKOUT: [Regime.CONSOLIDATION, Regime.EXPANSION],
    SetupType.PULLBACK: [Regime.BULL_TREND, Regime.BEAR_TREND],
    SetupType.LIQUIDITY_SWEEP: [Regime.RANGING, Regime.HIGH_VOL_CHOP, Regime.EXPANSION],
    SetupType.ORDER_BLOCK: [Regime.BULL_TREND, Regime.BEAR_TREND, Regime.EXPANSION],
    SetupType.BREAKER_BLOCK: [Regime.BULL_TREND, Regime.BEAR_TREND, Regime.RANGING],
    SetupType.FAIR_VALUE_GAP: [Regime.BULL_TREND, Regime.BEAR_TREND, Regime.EXPANSION],
    SetupType.MOMENTUM: [Regime.EXPANSION, Regime.BULL_TREND, Regime.BEAR_TREND],
    SetupType.REVERSAL: [Regime.HIGH_VOL_CHOP, Regime.RANGING, Regime.CONSOLIDATION],
    SetupType.MEAN_REVERSION: [Regime.RANGING, Regime.CONSOLIDATION],
    SetupType.RANGE_TRADING: [Regime.RANGING, Regime.CONSOLIDATION],
    SetupType.VOLATILITY_EXPANSION: [Regime.CONSOLIDATION, Regime.EXPANSION],
}

# Entry mechanism per engine: fade/reversal setups use "pending" limit entries
# near market (the small pullback IS the thesis, so no downside to waiting for
# it). Directional/momentum setups use "market" -- the cleanest, strongest
# moves in this category don't give back a pullback before running, so a
# limit offset would systematically filter out the best setups in exchange
# for a small maker-fee saving. "market" entries fill immediately (skip
# PendingEntryTracker); "pending" entries wait for price to touch a limit.
ENGINE_ENTRY_KIND: Dict[SetupType, str] = {
    SetupType.SMC: "pending",
    SetupType.TREND_CONTINUATION: "market",
    SetupType.BREAKOUT: "market",
    SetupType.PULLBACK: "pending",
    SetupType.LIQUIDITY_SWEEP: "pending",
    SetupType.ORDER_BLOCK: "pending",
    SetupType.BREAKER_BLOCK: "pending",
    SetupType.FAIR_VALUE_GAP: "pending",
    SetupType.MOMENTUM: "market",
    SetupType.REVERSAL: "pending",
    SetupType.MEAN_REVERSION: "pending",
    SetupType.RANGE_TRADING: "pending",
    SetupType.VOLATILITY_EXPANSION: "market",
}

# v1.2.1: per-setup pending expiry, in LTF (15m) bars. Replaces the old flat
# DEFAULT_PENDING_EXPIRY_BARS[TF_LTF] (12 bars / 3h) for every pending setup.
# That flat window was fine for near-market ATR-fraction offsets but starved
# structural zone entries -- especially SMC/order-block/breaker-block, whose
# zones are drawn from 4H (TF_HTF) structure and can legitimately take much
# longer than 3h to be retested. This only changes how long an entry is given
# to fill, not how far it's allowed to sit from market (still governed by
# MAX_PENDING_ENTRY_ATR_MULT).
SETUP_PENDING_EXPIRY_BARS: Dict[SetupType, int] = {
    # Near-market ATR-fraction offsets -- ordinary intrabar noise fills these
    # within a bar or two, so the original 3h window is kept as a generous
    # ceiling rather than a tight one.
    SetupType.LIQUIDITY_SWEEP: 12,   # ~3h
    SetupType.MEAN_REVERSION: 12,    # ~3h
    SetupType.RANGE_TRADING: 12,     # ~3h
    SetupType.REVERSAL: 12,          # ~3h
    # LTF-derived structural zone -- moderate allowance.
    SetupType.FAIR_VALUE_GAP: 16,    # ~4h
    # Mid-TF (1h) fib retrace -- needs more room than a near-market offset.
    SetupType.PULLBACK: 32,          # ~8h
    # HTF (4h) structural zones -- these are the ones that were expiring
    # unfilled most often under the old flat 3h window.
    SetupType.ORDER_BLOCK: 48,       # ~12h
    SetupType.BREAKER_BLOCK: 48,     # ~12h
    SetupType.SMC: 48,               # ~12h
}


@dataclass
class Candle:
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class Signal:
    """Independent output contract every specialized engine must produce."""
    setup_type: SetupType
    symbol: str
    direction: str                 # "long" | "short"
    entry: float
    sl: float
    tp1: float
    tp2: float
    confidence: float              # 0-1 raw, pre-calibration
    rr_tp1: float
    rr_tp2: float
    confluences: List[str]
    regime_at_signal: Regime
    entry_kind: str                # "market" | "pending"
    timeframe: str = TF_LTF
    created_ts: int = 0
    entry_filled: bool = False
    pending_bars: int = 0
    pending_expiry_bars: int = 0
    signal_id: str = ""
    # v1.2.0: the individual terms each engine summed to produce `confidence`
    # (e.g. {"base": 0.45, "adx_bonus": 0.12}). Logged for later recalibration
    # of the confidence field against realized outcomes — not read by any
    # scoring or filtering path today.
    confidence_components: Dict[str, float] = field(default_factory=dict)

    def finalize_id(self):
        raw = f"{self.symbol}|{self.setup_type}|{self.created_ts}|{self.entry}|{self.direction}"
        self.signal_id = hashlib.sha1(raw.encode()).hexdigest()[:12]


@dataclass
class RankedSignal:
    signal: Signal
    score: float
    tier: str                      # "A+" | "A" | "B"
    ev: float
    engine_weight: float
    regime_fit_mult: float
    confluence_quality_mult: float = 1.0   # v1.1.0
    asset_quality_mult: float = 1.0        # v1.1.0
    setup_direction_quality_mult: float = 1.0  # v1.2.0

# ==============================================================================
# SECTION C — HTTP / RETRY UTILITIES
# ==============================================================================

def http_post_json(url: str, payload: dict, timeout: float = 10.0,
                    max_retries: int = 4) -> Optional[dict]:
    """POST JSON with exponential backoff + jitter. Used for Hyperliquid /info
    and Telegram calls. Never raises — returns None on total failure so callers
    can degrade gracefully."""
    data = json.dumps(payload).encode("utf-8")
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(
                url, data=data, headers={"Content-Type": "application/json"}, method="POST"
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read()
                return json.loads(body)
        except urllib.error.HTTPError as e:
            if e.code == 429 or e.code >= 500:
                sleep_s = min(2 ** attempt + random.random(), 20)
                log.warning("HTTP %s from %s, retry in %.1fs", e.code, url, sleep_s)
                time.sleep(sleep_s)
                continue
            log.error("Non-retryable HTTP %s from %s: %s", e.code, url, e)
            return None
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            sleep_s = min(2 ** attempt + random.random(), 20)
            log.warning("Request error %s (%s), retry in %.1fs", e, url, sleep_s)
            time.sleep(sleep_s)
            continue
    log.error("Exhausted retries for %s", url)
    return None


def http_get_form(url: str, params: dict, timeout: float = 10.0,
                   max_retries: int = 3) -> Optional[dict]:
    """GET with querystring + retry, used for Telegram sendMessage."""
    import urllib.parse
    qs = urllib.parse.urlencode(params)
    full_url = f"{url}?{qs}"
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(full_url, timeout=timeout) as resp:
                return json.loads(resp.read())
        except Exception as e:
            sleep_s = min(2 ** attempt + random.random(), 15)
            log.warning("Telegram GET failed (%s), retry in %.1fs", e, sleep_s)
            time.sleep(sleep_s)
    return None


# ==============================================================================
# SECTION D — HYPERLIQUID DATA LAYER
# ==============================================================================

class HyperliquidClient:
    """Thin client around Hyperliquid's public /info endpoint. Read-only market
    data is all this engine needs. Implements a shared in-memory candle cache across all engines
    within a single run plus request throttling."""

    def __init__(self, base_url: str = HL_INFO_URL, min_interval_s: float = 0.15):
        self.base_url = base_url
        self.min_interval_s = min_interval_s
        self._last_call_ts = 0.0
        self._cache: Dict[Tuple[str, str], List[Candle]] = {}

    def _throttle(self):
        elapsed = time.time() - self._last_call_ts
        if elapsed < self.min_interval_s:
            time.sleep(self.min_interval_s - elapsed)
        self._last_call_ts = time.time()

    def get_candles(self, symbol: str, timeframe: str, lookback: int) -> List[Candle]:
        """Fetch OHLCV candles once per (symbol, timeframe) per run and share
        the result across every specialized engine that needs it."""
        key = (symbol, timeframe)
        if key in self._cache:
            return self._cache[key]

        interval_ms = TF_MS[timeframe]
        end_time = int(time.time() * 1000)
        start_time = end_time - interval_ms * (lookback + 5)
        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": timeframe,
                "startTime": start_time,
                "endTime": end_time,
            },
        }
        self._throttle()
        raw = http_post_json(self.base_url, payload)
        candles: List[Candle] = []
        if raw:
            try:
                for row in raw:
                    candles.append(Candle(
                        ts=int(row["t"]),
                        open=float(row["o"]),
                        high=float(row["h"]),
                        low=float(row["l"]),
                        close=float(row["c"]),
                        volume=float(row.get("v", 0.0)),
                    ))
            except (KeyError, TypeError, ValueError) as e:
                log.error("Malformed candle payload for %s/%s: %s", symbol, timeframe, e)
                candles = []
        candles = candles[-lookback:] if len(candles) > lookback else candles
        self._cache[key] = candles
        return candles

    def get_mark_price(self, symbol: str) -> Optional[float]:
        self._throttle()
        raw = http_post_json(self.base_url, {"type": "allMids"})
        if not raw:
            return None
        try:
            return float(raw.get(symbol))
        except (TypeError, ValueError):
            return None

# ==============================================================================
# SECTION E — SHARED INDICATOR LIBRARY (computed once, reused by every engine)
# ==============================================================================

def to_arrays(candles: List[Candle]) -> Dict[str, np.ndarray]:
    return {
        "open": np.array([c.open for c in candles], dtype=float),
        "high": np.array([c.high for c in candles], dtype=float),
        "low": np.array([c.low for c in candles], dtype=float),
        "close": np.array([c.close for c in candles], dtype=float),
        "volume": np.array([c.volume for c in candles], dtype=float),
        "ts": np.array([c.ts for c in candles], dtype=np.int64),
    }


def ema(values: np.ndarray, length: int) -> np.ndarray:
    if len(values) == 0:
        return values
    alpha = 2.0 / (length + 1.0)
    out = np.empty_like(values, dtype=float)
    out[0] = values[0]
    for i in range(1, len(values)):
        out[i] = alpha * values[i] + (1 - alpha) * out[i - 1]
    return out


def true_range(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    return np.maximum.reduce([high - low, np.abs(high - prev_close), np.abs(low - prev_close)])


def atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, length: int = 14) -> np.ndarray:
    tr = true_range(high, low, close)
    return ema(tr, length)


def rsi(close: np.ndarray, length: int = 14) -> np.ndarray:
    if len(close) < 2:
        return np.full_like(close, 50.0)
    delta = np.diff(close, prepend=close[0])
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    avg_gain = ema(gain, length)
    avg_loss = ema(loss, length)
    rs = np.divide(avg_gain, avg_loss, out=np.full_like(avg_gain, np.nan), where=avg_loss != 0)
    out = 100 - (100 / (1 + rs))
    out = np.where(avg_loss == 0, 100.0, out)
    return out


def adx(high: np.ndarray, low: np.ndarray, close: np.ndarray, length: int = 14) -> np.ndarray:
    up_move = np.diff(high, prepend=high[0])
    down_move = -np.diff(low, prepend=low[0])
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    tr = true_range(high, low, close)
    atr_s = ema(tr, length)
    atr_s_safe = np.where(atr_s == 0, 1e-9, atr_s)
    plus_di = 100 * ema(plus_dm, length) / atr_s_safe
    minus_di = 100 * ema(minus_dm, length) / atr_s_safe
    denom = np.where((plus_di + minus_di) == 0, 1e-9, plus_di + minus_di)
    dx = 100 * np.abs(plus_di - minus_di) / denom
    return ema(dx, length)


def bollinger(close: np.ndarray, length: int = 20, mult: float = 2.0):
    mid = np.array([close[max(0, i - length + 1):i + 1].mean() for i in range(len(close))])
    std = np.array([close[max(0, i - length + 1):i + 1].std() for i in range(len(close))])
    return mid - mult * std, mid, mid + mult * std


def swing_points(high: np.ndarray, low: np.ndarray, window: int = 3):
    """Fractal swing highs/lows: index i is a swing high/low if it's the max/min
    within +/- window bars. Used for structure (BOS/CHoCH), liquidity pools, and
    structural SL placement."""
    n = len(high)
    is_high = np.zeros(n, dtype=bool)
    is_low = np.zeros(n, dtype=bool)
    for i in range(window, n - window):
        seg_h = high[i - window:i + window + 1]
        seg_l = low[i - window:i + window + 1]
        if high[i] == seg_h.max() and np.argmax(seg_h) == window:
            is_high[i] = True
        if low[i] == seg_l.min() and np.argmin(seg_l) == window:
            is_low[i] = True
    return is_high, is_low


# ==============================================================================
# SECTION F — SMC PRIMITIVES (structure, order blocks, breakers, FVGs, liquidity)
# ==============================================================================

@dataclass
class StructureState:
    bias: str                     # "bullish" | "bearish" | "neutral"
    last_bos_idx: int
    last_choch_idx: int
    last_swing_high: float
    last_swing_low: float


def detect_structure(arr: Dict[str, np.ndarray], window: int = 3) -> StructureState:
    """Break of Structure (BOS) = trend-confirming break of the last swing.
    Change of Character (CHoCH) = counter-trend break signaling reversal."""
    high, low, close = arr["high"], arr["low"], arr["close"]
    is_high, is_low = swing_points(high, low, window)
    swing_high_idxs = np.where(is_high)[0]
    swing_low_idxs = np.where(is_low)[0]

    bias = "neutral"
    last_bos_idx, last_choch_idx = -1, -1
    last_sh = float(high[-1])
    last_sl = float(low[-1])

    if len(swing_high_idxs) >= 2 and len(swing_low_idxs) >= 2:
        recent_highs = high[swing_high_idxs[-3:]] if len(swing_high_idxs) >= 3 else high[swing_high_idxs]
        recent_lows = low[swing_low_idxs[-3:]] if len(swing_low_idxs) >= 3 else low[swing_low_idxs]
        last_sh = float(high[swing_high_idxs[-1]])
        last_sl = float(low[swing_low_idxs[-1]])
        higher_highs = np.all(np.diff(recent_highs) > 0) if len(recent_highs) > 1 else False
        higher_lows = np.all(np.diff(recent_lows) > 0) if len(recent_lows) > 1 else False
        lower_highs = np.all(np.diff(recent_highs) < 0) if len(recent_highs) > 1 else False
        lower_lows = np.all(np.diff(recent_lows) < 0) if len(recent_lows) > 1 else False

        if higher_highs and higher_lows:
            bias = "bullish"
        elif lower_highs and lower_lows:
            bias = "bearish"

        # BOS: close breaks beyond the last confirmed swing in trend direction.
        # CHoCH: close breaks the last swing against the prevailing bias.
        for i in range(swing_high_idxs[-1], len(close)):
            if close[i] > last_sh:
                if bias == "bullish":
                    last_bos_idx = i
                elif bias == "bearish":
                    last_choch_idx = i
                break
        for i in range(swing_low_idxs[-1], len(close)):
            if close[i] < last_sl:
                if bias == "bearish":
                    last_bos_idx = max(last_bos_idx, i)
                elif bias == "bullish":
                    last_choch_idx = max(last_choch_idx, i)
                break

    return StructureState(bias, last_bos_idx, last_choch_idx, last_sh, last_sl)


@dataclass
class Zone:
    kind: str            # "order_block" | "breaker_block" | "fvg"
    direction: str        # "bullish" | "bearish"
    top: float
    bottom: float
    idx: int
    mitigated: bool = False


def detect_order_blocks(arr: Dict[str, np.ndarray], structure: StructureState, lookback: int = 60) -> List[Zone]:
    """An order block is the last opposite-direction candle before a strong
    displacement move that produced a BOS. Bullish OB = last down-candle before
    an up-impulse; bearish OB = last up-candle before a down-impulse."""
    open_, high, low, close = arr["open"], arr["high"], arr["low"], arr["close"]
    n = len(close)
    start = max(1, n - lookback)
    zones: List[Zone] = []
    atr_s = atr(high, low, close, 14)
    for i in range(start, n - 1):
        body = close[i] - open_[i]
        impulse = close[i + 1] - open_[i + 1] if i + 1 < n else 0
        avg_atr = atr_s[i] if atr_s[i] > 0 else 1e-9
        is_impulse = abs(impulse) > 1.3 * avg_atr
        if not is_impulse:
            continue
        if impulse > 0 and body < 0:
            zones.append(Zone("order_block", "bullish", top=high[i], bottom=low[i], idx=i))
        elif impulse < 0 and body > 0:
            zones.append(Zone("order_block", "bearish", top=high[i], bottom=low[i], idx=i))
    _mark_mitigation(zones, close, start_idx=start)
    return zones


def detect_breaker_blocks(order_blocks: List[Zone], arr: Dict[str, np.ndarray]) -> List[Zone]:
    """A breaker block is a failed order block: price closed back through it
    (mitigating it), flipping its polarity — a former bullish OB that failed
    becomes bearish resistance, and vice versa."""
    close = arr["close"]
    breakers: List[Zone] = []
    for z in order_blocks:
        if not z.mitigated:
            continue
        flipped_dir = "bearish" if z.direction == "bullish" else "bullish"
        breakers.append(Zone("breaker_block", flipped_dir, top=z.top, bottom=z.bottom, idx=z.idx))
    _mark_mitigation(breakers, close, start_idx=0)
    return breakers


def detect_fvgs(arr: Dict[str, np.ndarray], lookback: int = 60) -> List[Zone]:
    """Fair Value Gap: a 3-candle imbalance where candle[i-1].high < candle[i+1].low
    (bullish FVG) or candle[i-1].low > candle[i+1].high (bearish FVG)."""
    high, low, close = arr["high"], arr["low"], arr["close"]
    n = len(close)
    start = max(1, n - lookback)
    zones: List[Zone] = []
    for i in range(start, n - 1):
        if i < 1:
            continue
        if high[i - 1] < low[i + 1] if i + 1 < n else False:
            zones.append(Zone("fvg", "bullish", top=low[i + 1], bottom=high[i - 1], idx=i))
        if i + 1 < n and low[i - 1] > high[i + 1]:
            zones.append(Zone("fvg", "bearish", top=low[i - 1], bottom=high[i + 1], idx=i))
    _mark_mitigation(zones, close, start_idx=start)
    return zones


def _mark_mitigation(zones: List[Zone], close: np.ndarray, start_idx: int):
    for z in zones:
        for j in range(z.idx + 1, len(close)):
            if z.bottom <= close[j] <= z.top:
                z.mitigated = True
                break


@dataclass
class LiquidityPool:
    level: float
    kind: str   # "buy_side" (above, resting sell-stops/shorts) | "sell_side" (below)
    idx: int
    swept: bool = False


def detect_liquidity_pools(arr: Dict[str, np.ndarray], window: int = 3, lookback: int = 80) -> List[LiquidityPool]:
    """Equal highs/lows and swing extremes act as resting liquidity. A pool is
    'swept' if a later wick pierces it but closes back inside (classic stop hunt)."""
    high, low, close = arr["high"], arr["low"], arr["close"]
    is_high, is_low = swing_points(high, low, window)
    n = len(close)
    start = max(0, n - lookback)
    pools: List[LiquidityPool] = []
    for i in range(start, n):
        if is_high[i]:
            pools.append(LiquidityPool(level=float(high[i]), kind="buy_side", idx=i))
        if is_low[i]:
            pools.append(LiquidityPool(level=float(low[i]), kind="sell_side", idx=i))
    for p in pools:
        for j in range(p.idx + 1, n):
            if p.kind == "buy_side" and high[j] > p.level and close[j] < p.level:
                p.swept = True
                break
            if p.kind == "sell_side" and low[j] < p.level and close[j] > p.level:
                p.swept = True
                break
    return pools


def premium_discount_zone(high: np.ndarray, low: np.ndarray, lookback: int = 50) -> Tuple[float, float, float]:
    """Returns (range_low, equilibrium, range_high) over the lookback window.
    Below equilibrium = discount (favor longs); above = premium (favor shorts)."""
    seg_h = high[-lookback:]
    seg_l = low[-lookback:]
    rng_high, rng_low = float(seg_h.max()), float(seg_l.min())
    eq = (rng_high + rng_low) / 2.0
    return rng_low, eq, rng_high

# ==============================================================================
# SECTION G — REGIME DETECTION
# ==============================================================================

def detect_regime(macro_arr: Dict[str, np.ndarray], htf_arr: Dict[str, np.ndarray]) -> Tuple[Regime, Dict[str, float]]:
    close_h, high_h, low_h = htf_arr["close"], htf_arr["high"], htf_arr["low"]
    adx_h = adx(high_h, low_h, close_h, 14)
    atr_h = atr(high_h, low_h, close_h, 14)
    ema_fast = ema(close_h, 21)
    ema_slow = ema(close_h, 50)

    last_adx = float(adx_h[-1])
    atr_pct = float(atr_h[-1] / close_h[-1]) if close_h[-1] else 0.0
    atr_hist_pct = atr_h[-40:] / np.where(close_h[-40:] == 0, 1e-9, close_h[-40:])
    vol_percentile = float((atr_hist_pct < atr_pct).mean()) if len(atr_hist_pct) else 0.5
    trending_up = ema_fast[-1] > ema_slow[-1]
    trend_slope = float((ema_fast[-1] - ema_fast[-10]) / ema_fast[-10]) if len(ema_fast) > 10 and ema_fast[-10] else 0.0

    metrics = {"adx": last_adx, "atr_pct": atr_pct, "vol_percentile": vol_percentile, "trend_slope": trend_slope}

    if vol_percentile > 0.85 and last_adx < 20:
        return Regime.HIGH_VOL_CHOP, metrics
    if last_adx >= 25 and trending_up and trend_slope > 0:
        return Regime.BULL_TREND, metrics
    if last_adx >= 25 and not trending_up and trend_slope < 0:
        return Regime.BEAR_TREND, metrics
    if vol_percentile > 0.70 and last_adx >= 20:
        return Regime.EXPANSION, metrics
    if last_adx < 18 and vol_percentile < 0.35:
        return Regime.CONSOLIDATION, metrics
    return Regime.RANGING, metrics


def session_window(ts_ms: int) -> str:
    """Asia/London/NY liquidity-rhythm tagging purely for
    session-aware breadth logging; never gates signal generation on its own."""
    hour = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).hour
    if 0 <= hour < 8:
        return "asia"
    if 7 <= hour < 16:
        return "london"
    return "ny"


# ==============================================================================
# SECTION H — RISK MANAGEMENT / ENTRY VALIDATION
# ==============================================================================

class RejectReason(str, Enum):
    RR_BELOW_FLOOR = "rr_below_floor"
    ENTRY_TOO_CLOSE_TO_SL = "entry_too_close_to_sl"
    ENTRY_TOO_CLOSE_TO_TP1 = "entry_too_close_to_tp1"
    PENDING_ENTRY_TOO_FAR = "pending_entry_too_far"
    STRUCTURE_INVALID = "structure_invalid"
    LIQUIDITY_TRAP = "liquidity_trap"
    NONE = "none"


@dataclass
class ValidationResult:
    ok: bool
    reason: RejectReason = RejectReason.NONE


def validate_signal_shape(sig: Signal, current_price: float, atr_ltf: float) -> ValidationResult:
    """Structurally enforces hard floors. Any candidate failing
    this check is rejected before it ever reaches the Decision Engine — these
    are genuine invalidation conditions, not soft confluences."""
    if atr_ltf <= 0:
        return ValidationResult(False, RejectReason.STRUCTURE_INVALID)

    if sig.rr_tp1 < TP1_RR_MIN:
        return ValidationResult(False, RejectReason.RR_BELOW_FLOOR)

    dist_sl = abs(sig.entry - sig.sl)
    dist_tp1 = abs(sig.tp1 - sig.entry)
    if dist_sl < MIN_ENTRY_SL_ATR_FRAC * atr_ltf:
        return ValidationResult(False, RejectReason.ENTRY_TOO_CLOSE_TO_SL)
    if dist_tp1 < MIN_ENTRY_TP1_ATR_FRAC * atr_ltf:
        return ValidationResult(False, RejectReason.ENTRY_TOO_CLOSE_TO_TP1)

    if sig.entry_kind == "pending":
        dist_from_market = abs(sig.entry - current_price)
        if dist_from_market > MAX_PENDING_ENTRY_ATR_MULT * atr_ltf:
            return ValidationResult(False, RejectReason.PENDING_ENTRY_TOO_FAR)

    return ValidationResult(True)


def liquidity_sanity_check(sig: Signal, pools: List[LiquidityPool], atr_ltf: float) -> float:
    """reject/discount candidates whose entry sits directly inside a
    level about to be swept, or immediately adjacent to an obvious unmitigated
    pool — unless the setup IS a liquidity-sweep engine trading that behavior.
    Returns a multiplier in (0, 1]; 0 for the hard-reject case."""
    if sig.setup_type == SetupType.LIQUIDITY_SWEEP:
        return 1.0
    near_thresh = 0.25 * atr_ltf
    for p in pools:
        if p.swept:
            continue
        if abs(sig.entry - p.level) < near_thresh:
            # Entry stacked into unmitigated opposing liquidity is a likely
            # trap; heavy discount rather than an automatic zero.
            if (sig.direction == "long" and p.kind == "sell_side") or \
               (sig.direction == "short" and p.kind == "buy_side"):
                return 0.35
            return 0.75
    return 1.0


def build_sl_tp_from_structure(direction: str, entry: float, structural_stop: float,
                                target_pool: Optional[float], atr_val: float) -> Tuple[float, float, float]:
    """Builds SL/TP1/TP2 strictly from candle highs/lows (never midpoint or live
    price — stated three times in the spec for emphasis). TP1 sits
    at the honest nearest structural target within the 1.5-2.0 RR band; TP2
    extends toward the next liquidity pool/structure with no RR ceiling."""
    risk = abs(entry - structural_stop)
    if risk <= 0:
        risk = max(atr_val * 0.5, 1e-9)

    if direction == "long":
        sl = structural_stop
        tp1_floor = entry + TP1_RR_MIN * risk
        if target_pool is not None and target_pool > entry:
            # Buffer applied to the risk-distance (not the absolute price) so a
            # 15% allowance stays a 15% allowance regardless of how large entry
            # is relative to risk. Previously `tp1_ceiling * 1.15` scaled the
            # whole price, letting TP1 run out to whatever pool existed with
            # almost no effective cap -> could end up past TP2.
            tp1_cap = entry + TP1_RR_SOFT_CEILING * risk * 1.15
            tp1 = min(max(target_pool, tp1_floor), tp1_cap)
            tp1 = max(tp1, tp1_floor)
        else:
            tp1 = tp1_floor
        tp2_base = target_pool if (target_pool is not None and target_pool > tp1) else entry + 3.0 * risk
        tp2 = max(tp2_base, entry + TP1_RR_SOFT_CEILING * risk * 1.2)
    else:
        sl = structural_stop
        tp1_floor = entry - TP1_RR_MIN * risk
        if target_pool is not None and target_pool < entry:
            # Mirrors the long-side fix: buffer applied to the risk-distance,
            # not the absolute price. Previously `tp1_ceiling * 1.15` scaled
            # the whole price, which for short signals pushed the "cap" far
            # above entry and caused the final `min(tp1, tp1_floor)` to almost
            # always clamp TP1 back to the bare 1.5R floor -- silently
            # discarding a valid, farther liquidity-pool target.
            tp1_cap = entry - TP1_RR_SOFT_CEILING * risk * 1.15
            tp1 = max(min(target_pool, tp1_floor), tp1_cap)
            tp1 = min(tp1, tp1_floor)
        else:
            tp1 = tp1_floor
        tp2_base = target_pool if (target_pool is not None and target_pool < tp1) else entry - 3.0 * risk
        tp2 = min(tp2_base, entry - TP1_RR_SOFT_CEILING * risk * 1.2)

    return sl, tp1, tp2


def compute_rr(direction: str, entry: float, sl: float, tp1: float, tp2: float) -> Tuple[float, float]:
    risk = abs(entry - sl)
    if risk <= 0:
        return 0.0, 0.0
    rr1 = abs(tp1 - entry) / risk
    rr2 = abs(tp2 - entry) / risk
    return rr1, rr2

# ==============================================================================
# SECTION I — MARKET CONTEXT (shared computation across all specialized engines)
# ==============================================================================

@dataclass
class MarketContext:
    symbol: str
    price: float
    regime: Regime
    regime_metrics: Dict[str, float]
    ltf: Dict[str, np.ndarray]
    mid: Dict[str, np.ndarray]
    htf: Dict[str, np.ndarray]
    macro: Dict[str, np.ndarray]
    atr_ltf: np.ndarray
    rsi_ltf: np.ndarray
    adx_ltf: np.ndarray
    structure_htf: StructureState
    structure_mid: StructureState
    order_blocks: List[Zone]
    breaker_blocks: List[Zone]
    fvgs: List[Zone]
    pools: List[LiquidityPool]
    pd_zone: Tuple[float, float, float]
    now_ts: int


def build_context(client: HyperliquidClient, symbol: str) -> Optional[MarketContext]:
    candles = {tf: client.get_candles(symbol, tf, CANDLE_LOOKBACK[tf]) for tf in ALL_TIMEFRAMES}
    if any(len(candles[tf]) < 30 for tf in ALL_TIMEFRAMES):
        log.info("Skipping %s: insufficient candle history", symbol)
        return None

    arrs = {tf: to_arrays(candles[tf]) for tf in ALL_TIMEFRAMES}
    price = client.get_mark_price(symbol) or float(arrs[TF_LTF]["close"][-1])

    regime, regime_metrics = detect_regime(arrs[TF_MACRO], arrs[TF_HTF])
    atr_ltf = atr(arrs[TF_LTF]["high"], arrs[TF_LTF]["low"], arrs[TF_LTF]["close"], 14)
    rsi_ltf = rsi(arrs[TF_LTF]["close"], 14)
    adx_ltf = adx(arrs[TF_LTF]["high"], arrs[TF_LTF]["low"], arrs[TF_LTF]["close"], 14)

    structure_htf = detect_structure(arrs[TF_HTF])
    structure_mid = detect_structure(arrs[TF_MID])
    order_blocks = detect_order_blocks(arrs[TF_HTF], structure_htf)
    breaker_blocks = detect_breaker_blocks(order_blocks, arrs[TF_HTF])
    fvgs = detect_fvgs(arrs[TF_LTF])
    pools = detect_liquidity_pools(arrs[TF_MID])
    pd_zone = premium_discount_zone(arrs[TF_HTF]["high"], arrs[TF_HTF]["low"])

    return MarketContext(
        symbol=symbol, price=price, regime=regime, regime_metrics=regime_metrics,
        ltf=arrs[TF_LTF], mid=arrs[TF_MID], htf=arrs[TF_HTF], macro=arrs[TF_MACRO],
        atr_ltf=atr_ltf, rsi_ltf=rsi_ltf, adx_ltf=adx_ltf,
        structure_htf=structure_htf, structure_mid=structure_mid,
        order_blocks=order_blocks, breaker_blocks=breaker_blocks, fvgs=fvgs,
        pools=pools, pd_zone=pd_zone, now_ts=int(time.time() * 1000),
    )


def _nearest_pool_target(pools: List[LiquidityPool], direction: str, entry: float) -> Optional[float]:
    candidates = []
    for p in pools:
        if p.swept:
            continue
        if direction == "long" and p.kind == "buy_side" and p.level > entry:
            candidates.append(p.level)
        if direction == "short" and p.kind == "sell_side" and p.level < entry:
            candidates.append(p.level)
    if not candidates:
        return None
    return min(candidates, key=lambda lv: abs(lv - entry)) if direction == "long" else max(candidates, key=lambda lv: abs(lv - entry))


def _mk_signal(setup: SetupType, ctx: MarketContext, direction: str, entry: float,
               structural_stop: float, confidence: float, confluences: List[str],
               confidence_components: Optional[Dict[str, float]] = None) -> Optional[Signal]:
    target = _nearest_pool_target(ctx.pools, direction, entry)
    sl, tp1, tp2 = build_sl_tp_from_structure(direction, entry, structural_stop, target, float(ctx.atr_ltf[-1]))
    rr1, rr2 = compute_rr(direction, entry, sl, tp1, tp2)
    entry_kind = ENGINE_ENTRY_KIND[setup]
    sig = Signal(
        setup_type=setup, symbol=ctx.symbol, direction=direction, entry=entry, sl=sl,
        tp1=tp1, tp2=tp2, confidence=confidence, rr_tp1=rr1, rr_tp2=rr2,
        confluences=confluences, regime_at_signal=ctx.regime.value, entry_kind=entry_kind,
        timeframe=TF_LTF, created_ts=ctx.now_ts,
        pending_expiry_bars=SETUP_PENDING_EXPIRY_BARS.get(setup, DEFAULT_PENDING_EXPIRY_BARS[TF_LTF]) if entry_kind == "pending" else 0,
        confidence_components=dict(confidence_components) if confidence_components else {},
    )
    sig.finalize_id()
    return sig


# ==============================================================================
# SECTION J — SPECIALIZED ENGINES
# ==============================================================================

class BaseEngine:
    setup_type: SetupType = None  # type: ignore

    def generate(self, ctx: MarketContext) -> List[Signal]:
        raise NotImplementedError


class SMCEngine(BaseEngine):
    """Composite institutional read: HTF bias + discount/premium zone + BOS
    confirmation on mid TF -> entry on nearest unmitigated HTF order block in
    the direction of bias. This is the flagship engine synthesizing structure,
    zones, and premium/discount."""
    setup_type = SetupType.SMC

    def generate(self, ctx: MarketContext) -> List[Signal]:
        out = []
        bias = ctx.structure_htf.bias
        if bias == "neutral":
            return out
        rng_low, eq, rng_high = ctx.pd_zone
        direction = "long" if bias == "bullish" else "short"
        in_discount = ctx.price < eq
        in_premium = ctx.price > eq
        if direction == "long" and not in_discount:
            return out
        if direction == "short" and not in_premium:
            return out

        unmitigated_obs = [z for z in ctx.order_blocks if not z.mitigated and
                            z.direction == ("bullish" if direction == "long" else "bearish")]
        if not unmitigated_obs:
            return out
        zone = min(unmitigated_obs, key=lambda z: abs((z.top + z.bottom) / 2 - ctx.price))
        entry = (zone.top + zone.bottom) / 2
        stop = zone.bottom * 0.999 if direction == "long" else zone.top * 1.001

        confluences = ["htf_bias_" + bias, "premium_discount_alignment", "unmitigated_order_block"]
        components = {"base": 0.55}
        confidence = 0.55
        if ctx.structure_mid.bias == bias:
            confidence += 0.15
            components["mtf_structure_alignment"] = 0.15
            confluences.append("mtf_structure_alignment")
        if ctx.structure_htf.last_bos_idx > 0:
            confidence += 0.1
            components["htf_bos_confirmed"] = 0.1
            confluences.append("htf_bos_confirmed")
        sig = _mk_signal(self.setup_type, ctx, direction, entry, stop, min(confidence, 0.95), confluences,
                          confidence_components=components)
        if sig:
            out.append(sig)
        return out


class TrendContinuationEngine(BaseEngine):
    """EMA-stack trend continuation: enter at market in the direction of a
    confirmed HTF+MID trend once LTF momentum resumes after a shallow pullback."""
    setup_type = SetupType.TREND_CONTINUATION

    def generate(self, ctx: MarketContext) -> List[Signal]:
        out = []
        close_ltf = ctx.ltf["close"]
        ema21, ema50 = ema(close_ltf, 21), ema(close_ltf, 50)
        trend_up = ema21[-1] > ema50[-1] and ctx.structure_htf.bias == "bullish"
        trend_down = ema21[-1] < ema50[-1] and ctx.structure_htf.bias == "bearish"
        if not (trend_up or trend_down):
            return out
        direction = "long" if trend_up else "short"
        pulled_back = (close_ltf[-3] < ema21[-3]) if direction == "long" else (close_ltf[-3] > ema21[-3])
        resumed = (close_ltf[-1] > ema21[-1]) if direction == "long" else (close_ltf[-1] < ema21[-1])
        if not (pulled_back and resumed):
            return out
        entry = ctx.price
        recent_low = float(ctx.ltf["low"][-10:].min())
        recent_high = float(ctx.ltf["high"][-10:].max())
        stop = recent_low * 0.998 if direction == "long" else recent_high * 1.002
        confluences = ["ema_stack_trend", "shallow_pullback_resumption"]
        adx_bonus = 0.15 if ctx.adx_ltf[-1] > 22 else 0.0
        confidence = 0.5 + adx_bonus
        components = {"base": 0.5, "adx_bonus": adx_bonus}
        sig = _mk_signal(self.setup_type, ctx, direction, entry, stop, min(confidence, 0.9), confluences,
                          confidence_components=components)
        if sig:
            out.append(sig)
        return out


class BreakoutEngine(BaseEngine):
    """Consolidation-range breakout with volume expansion confirmation."""
    setup_type = SetupType.BREAKOUT

    def generate(self, ctx: MarketContext) -> List[Signal]:
        out = []
        high, low, close, vol = ctx.ltf["high"], ctx.ltf["low"], ctx.ltf["close"], ctx.ltf["volume"]
        rng_high = float(high[-25:-1].max())
        rng_low = float(low[-25:-1].min())
        avg_vol = float(vol[-25:-1].mean()) if vol[-25:-1].size else 0.0
        broke_up = close[-1] > rng_high and vol[-1] > 1.4 * avg_vol
        broke_down = close[-1] < rng_low and vol[-1] > 1.4 * avg_vol
        if not (broke_up or broke_down):
            return out
        direction = "long" if broke_up else "short"
        entry = ctx.price
        stop = rng_low if direction == "long" else rng_high
        confluences = ["range_breakout", "volume_expansion"]
        confidence = 0.5
        components = {"base": 0.5}
        if ctx.regime in (Regime.CONSOLIDATION, Regime.EXPANSION):
            confidence += 0.15
            components["regime_supportive"] = 0.15
            confluences.append("regime_supportive")
        sig = _mk_signal(self.setup_type, ctx, direction, entry, stop, min(confidence, 0.9), confluences,
                          confidence_components=components)
        if sig:
            out.append(sig)
        return out


class PullbackEngine(BaseEngine):
    """Fibonacci-style pullback into a discount/premium zone within an
    established trend, entered as a pending limit at the 0.5-0.618 retrace."""
    setup_type = SetupType.PULLBACK

    def generate(self, ctx: MarketContext) -> List[Signal]:
        out = []
        bias = ctx.structure_htf.bias
        if bias == "neutral":
            return out
        direction = "long" if bias == "bullish" else "short"
        lookback_high = float(ctx.mid["high"][-30:].max())
        lookback_low = float(ctx.mid["low"][-30:].min())
        leg = lookback_high - lookback_low
        if leg <= 0:
            return out
        if direction == "long":
            entry = lookback_high - 0.618 * leg
            stop = lookback_low
        else:
            entry = lookback_low + 0.618 * leg
            stop = lookback_high
        confluences = ["fib_0.618_retrace", "htf_trend_alignment"]
        confidence = 0.5
        sig = _mk_signal(self.setup_type, ctx, direction, entry, stop, confidence, confluences,
                          confidence_components={"base": 0.5})
        if sig:
            out.append(sig)
        return out


class LiquiditySweepEngine(BaseEngine):
    """Trades the stop-hunt itself: enters immediately after a swept
    swing pool reclaims, targeting reversion back through the range."""
    setup_type = SetupType.LIQUIDITY_SWEEP

    def generate(self, ctx: MarketContext) -> List[Signal]:
        out = []
        recent_pools = [p for p in ctx.pools if p.swept and p.idx >= len(ctx.mid["close"]) - 8]
        if not recent_pools:
            return out
        pool = recent_pools[-1]
        direction = "long" if pool.kind == "sell_side" else "short"
        # Limit entry offset slightly off market rather than the exact reclaim
        # price: the reclaim price is already left behind by the time the
        # signal fires, so waiting for a full retest rarely fills. Placing the
        # limit a small ATR-fraction to the *favorable* side of market keeps
        # this a passive (maker-fee) order while only needing ordinary
        # candle-to-candle noise to fill it within a bar or two.
        offset = LIQUIDITY_SWEEP_ENTRY_OFFSET_ATR_FRAC * float(ctx.atr_ltf[-1])
        entry = ctx.price - offset if direction == "long" else ctx.price + offset
        stop = pool.level * 0.997 if direction == "long" else pool.level * 1.003
        confluences = ["liquidity_sweep_reclaim", f"{pool.kind}_pool_swept"]
        components = {"base": 0.55}
        confidence = 0.55
        # Forensic review of the chased_a_swept_liquidity_pool loss tag showed the
        # failure mode is concentrated in sweeps taken against the prevailing HTF
        # bias (10 of 12 such losses were shorts fired into an uptrend) — the
        # reclaim looked like a reversal but was actually continuation resuming.
        # Penalize (not hard-gate, since countertrend sweeps at genuine range
        # extremes are still a valid setup) counter-trend entries, and reward
        # ones that agree with HTF bias.
        counter_trend = (direction == "long" and ctx.structure_htf.bias == "bearish") or \
                        (direction == "short" and ctx.structure_htf.bias == "bullish")
        aligned = (direction == "long" and ctx.structure_htf.bias == "bullish") or \
                  (direction == "short" and ctx.structure_htf.bias == "bearish")
        if counter_trend:
            confidence -= 0.08
            components["counter_trend_penalty"] = -0.08
            confluences.append("counter_htf_bias_sweep")
        elif aligned:
            confidence += 0.05
            components["htf_bias_alignment"] = 0.05
            confluences.append("htf_bias_alignment")
        sig = _mk_signal(self.setup_type, ctx, direction, entry, stop, max(min(confidence, 0.85), 0.30), confluences,
                          confidence_components=components)
        if sig:
            out.append(sig)
        return out


class OrderBlockEngine(BaseEngine):
    """Pure order-block retest independent of full SMC bias-stacking (SMCEngine
    requires premium/discount alignment too) — a narrower, higher-precision
    variant entered as a pending limit at the block's midpoint."""
    setup_type = SetupType.ORDER_BLOCK

    def generate(self, ctx: MarketContext) -> List[Signal]:
        out = []
        unmitigated = [z for z in ctx.order_blocks if not z.mitigated]
        if not unmitigated:
            return out
        for zone in unmitigated[-3:]:
            direction = "long" if zone.direction == "bullish" else "short"
            entry = (zone.top + zone.bottom) / 2
            stop = zone.bottom * 0.999 if direction == "long" else zone.top * 1.001
            confluences = ["unmitigated_order_block_retest"]
            confidence = 0.48
            sig = _mk_signal(self.setup_type, ctx, direction, entry, stop, confidence, confluences,
                              confidence_components={"base": 0.48})
            if sig:
                out.append(sig)
        return out


class BreakerBlockEngine(BaseEngine):
    """Failed order block flipped polarity, retested as new S/R."""
    setup_type = SetupType.BREAKER_BLOCK

    def generate(self, ctx: MarketContext) -> List[Signal]:
        out = []
        unmitigated = [z for z in ctx.breaker_blocks if not z.mitigated]
        if not unmitigated:
            return out
        zone = unmitigated[-1]
        direction = "long" if zone.direction == "bullish" else "short"
        entry = (zone.top + zone.bottom) / 2
        stop = zone.bottom * 0.999 if direction == "long" else zone.top * 1.001
        confluences = ["breaker_block_retest", "prior_ob_failure_flip"]
        confidence = 0.5
        sig = _mk_signal(self.setup_type, ctx, direction, entry, stop, confidence, confluences,
                          confidence_components={"base": 0.5})
        if sig:
            out.append(sig)
        return out


class FairValueGapEngine(BaseEngine):
    """Enters on retracement into an unmitigated LTF fair value gap in the
    direction of HTF bias."""
    setup_type = SetupType.FAIR_VALUE_GAP

    def generate(self, ctx: MarketContext) -> List[Signal]:
        out = []
        bias = ctx.structure_htf.bias
        candidates = [z for z in ctx.fvgs if not z.mitigated and
                      ((z.direction == "bullish" and bias == "bullish") or
                       (z.direction == "bearish" and bias == "bearish"))]
        if not candidates:
            return out
        zone = candidates[-1]
        direction = "long" if zone.direction == "bullish" else "short"
        entry = (zone.top + zone.bottom) / 2
        atr_v = float(ctx.atr_ltf[-1])
        stop = zone.bottom - 0.3 * atr_v if direction == "long" else zone.top + 0.3 * atr_v
        confluences = ["unmitigated_fvg", "htf_bias_alignment"]
        confidence = 0.5
        sig = _mk_signal(self.setup_type, ctx, direction, entry, stop, confidence, confluences,
                          confidence_components={"base": 0.5})
        if sig:
            out.append(sig)
        return out


class MomentumEngine(BaseEngine):
    """RSI/ADX-confirmed momentum ignition, market entry."""
    setup_type = SetupType.MOMENTUM

    def generate(self, ctx: MarketContext) -> List[Signal]:
        out = []
        r = ctx.rsi_ltf[-1]
        a = ctx.adx_ltf[-1]
        close = ctx.ltf["close"]
        momentum_up = r > 58 and a > 20 and close[-1] > close[-5]
        momentum_down = r < 42 and a > 20 and close[-1] < close[-5]
        if not (momentum_up or momentum_down):
            return out
        direction = "long" if momentum_up else "short"
        entry = ctx.price
        atr_v = float(ctx.atr_ltf[-1])
        stop = entry - 1.5 * atr_v if direction == "long" else entry + 1.5 * atr_v
        confluences = ["rsi_momentum", "adx_confirmed"]
        adx_bonus = min((a - 20) / 100, 0.2)
        confidence = 0.45 + adx_bonus
        components = {"base": 0.45, "adx_bonus": adx_bonus, "rsi_at_signal": float(r), "adx_at_signal": float(a)}
        sig = _mk_signal(self.setup_type, ctx, direction, entry, stop, min(confidence, 0.85), confluences,
                          confidence_components=components)
        if sig:
            out.append(sig)
        return out


class ReversalEngine(BaseEngine):
    """CHoCH-confirmed reversal at a swept liquidity extreme with RSI
    divergence-style exhaustion."""
    setup_type = SetupType.REVERSAL

    def generate(self, ctx: MarketContext) -> List[Signal]:
        out = []
        s = ctx.structure_mid
        if s.last_choch_idx <= 0:
            return out
        direction = "long" if ctx.ltf["close"][-1] > ctx.ltf["close"][-6] else "short"
        r = ctx.rsi_ltf[-1]
        exhausted = (direction == "long" and r < 35) or (direction == "short" and r > 65)
        if not exhausted:
            return out
        atr_v = float(ctx.atr_ltf[-1])
        entry_offset = MOMENTUM_STYLE_ENTRY_OFFSET_ATR_FRAC * atr_v
        entry = ctx.price - entry_offset if direction == "long" else ctx.price + entry_offset
        stop = entry - 1.4 * atr_v if direction == "long" else entry + 1.4 * atr_v
        confluences = ["mid_tf_choch", "rsi_exhaustion"]
        components = {"base": 0.48}
        confidence = 0.48
        # Deeper RSI exhaustion beyond the 35/65 trigger is a stronger case for
        # reversal, not just a binary yes/no
        rsi_extremity = (35 - r) if direction == "long" else (r - 65)
        extremity_bonus = min(max(rsi_extremity, 0.0) / 100, 0.12)
        if extremity_bonus > 0:
            confidence += extremity_bonus
            components["rsi_extremity_bonus"] = extremity_bonus
            confluences.append("deep_rsi_exhaustion")
        # HTF bias alignment: a mid-TF CHoCH reversal that also matches the
        # higher-timeframe bias is better supported than one fighting it
        htf_dir = "long" if ctx.structure_htf.bias == "bullish" else (
            "short" if ctx.structure_htf.bias == "bearish" else None)
        if htf_dir == direction:
            confidence += 0.1
            components["htf_bias_alignment"] = 0.1
            confluences.append("htf_bias_alignment")
        sig = _mk_signal(self.setup_type, ctx, direction, entry, stop, min(confidence, 0.85), confluences,
                          confidence_components=components)
        if sig:
            out.append(sig)
        return out


class MeanReversionEngine(BaseEngine):
    """Bollinger-band mean reversion inside a ranging regime only."""
    setup_type = SetupType.MEAN_REVERSION

    def generate(self, ctx: MarketContext) -> List[Signal]:
        out = []
        if ctx.regime not in (Regime.RANGING, Regime.CONSOLIDATION):
            return out
        close = ctx.ltf["close"]
        lower, mid, upper = bollinger(close, 20, 2.0)
        atr_v = float(ctx.atr_ltf[-1])
        if close[-1] < lower[-1]:
            direction = "long"
            entry = ctx.price - MOMENTUM_STYLE_ENTRY_OFFSET_ATR_FRAC * atr_v
            stop = float(ctx.ltf["low"][-10:].min()) - 0.2 * atr_v
            band_penetration = (float(lower[-1]) - close[-1]) / atr_v if atr_v > 0 else 0.0
        elif close[-1] > upper[-1]:
            direction = "short"
            entry = ctx.price + MOMENTUM_STYLE_ENTRY_OFFSET_ATR_FRAC * atr_v
            stop = float(ctx.ltf["high"][-10:].max()) + 0.2 * atr_v
            band_penetration = (close[-1] - float(upper[-1])) / atr_v if atr_v > 0 else 0.0
        else:
            return out
        confluences = ["bollinger_band_extreme", "ranging_regime"]
        components = {"base": 0.45}
        confidence = 0.45
        # A deeper close-beyond-band extreme is a more stretched (and historically
        # more mean-reversion-prone) read than a marginal band touch
        penetration_bonus = min(max(band_penetration, 0.0) * 0.1, 0.15)
        if penetration_bonus > 0:
            confidence += penetration_bonus
            components["band_penetration_bonus"] = penetration_bonus
            confluences.append("deep_band_penetration")
        # Low ADX confirms the market is genuinely range-bound rather than just
        # carrying the regime label
        if ctx.adx_ltf[-1] < 20:
            confidence += 0.1
            components["low_adx_range_confirmed"] = 0.1
            confluences.append("low_adx_range_confirmed")
        sig = _mk_signal(self.setup_type, ctx, direction, entry, stop, min(confidence, 0.85), confluences,
                          confidence_components=components)
        if sig:
            out.append(sig)
        return out


class RangeTradingEngine(BaseEngine):
    """Fades range extremes back toward equilibrium within a confirmed
    horizontal range."""
    setup_type = SetupType.RANGE_TRADING

    def generate(self, ctx: MarketContext) -> List[Signal]:
        out = []
        if ctx.regime not in (Regime.RANGING, Regime.CONSOLIDATION):
            return out
        rng_low, eq, rng_high = ctx.pd_zone
        band = rng_high - rng_low
        if band <= 0:
            return out
        near_top = ctx.price > rng_high - 0.15 * band
        near_bottom = ctx.price < rng_low + 0.15 * band
        if not (near_top or near_bottom):
            return out
        direction = "short" if near_top else "long"
        atr_v = float(ctx.atr_ltf[-1])
        entry_offset = MOMENTUM_STYLE_ENTRY_OFFSET_ATR_FRAC * atr_v
        entry = ctx.price - entry_offset if direction == "long" else ctx.price + entry_offset
        stop = rng_high + 0.5 * atr_v if direction == "short" else rng_low - 0.5 * atr_v
        confluences = ["range_extreme_fade", "confirmed_horizontal_range"]
        components = {"base": 0.47}
        confidence = 0.47
        # Reward proximity to the actual extreme, not just being anywhere inside
        # the 15% edge zone
        edge_dist_frac = ((rng_high - ctx.price) / band) if near_top else ((ctx.price - rng_low) / band)
        proximity_bonus = max((0.15 - edge_dist_frac) / 0.15, 0.0) * 0.1
        if proximity_bonus > 0:
            confidence += proximity_bonus
            components["extreme_proximity_bonus"] = proximity_bonus
            confluences.append("tight_range_extreme")
        # Low ADX confirms the market is genuinely range-bound rather than just
        # carrying the regime label
        if ctx.adx_ltf[-1] < 20:
            confidence += 0.1
            components["low_adx_range_confirmed"] = 0.1
            confluences.append("low_adx_range_confirmed")
        sig = _mk_signal(self.setup_type, ctx, direction, entry, stop, min(confidence, 0.85), confluences,
                          confidence_components=components)
        if sig:
            out.append(sig)
        return out


class VolatilityExpansionEngine(BaseEngine):
    """Bollinger squeeze -> expansion breakout, market entry in the direction
    of the expansion candle."""
    setup_type = SetupType.VOLATILITY_EXPANSION

    def generate(self, ctx: MarketContext) -> List[Signal]:
        out = []
        close = ctx.ltf["close"]
        lower, mid, upper = bollinger(close, 20, 2.0)
        bandwidth = (upper - lower) / np.where(mid == 0, 1e-9, mid)
        squeeze = bandwidth[-8:-1].mean()
        expanding = bandwidth[-1] > 1.5 * squeeze
        if not expanding:
            return out
        direction = "long" if close[-1] > close[-2] else "short"
        entry = ctx.price
        atr_v = float(ctx.atr_ltf[-1])
        stop = entry - 1.6 * atr_v if direction == "long" else entry + 1.6 * atr_v
        confluences = ["bollinger_squeeze_release", "volatility_expansion"]
        confidence = 0.48
        sig = _mk_signal(self.setup_type, ctx, direction, entry, stop, confidence, confluences,
                          confidence_components={"base": 0.48})
        if sig:
            out.append(sig)
        return out


ALL_ENGINES: List[BaseEngine] = [
    SMCEngine(), TrendContinuationEngine(), BreakoutEngine(), PullbackEngine(),
    LiquiditySweepEngine(), OrderBlockEngine(), BreakerBlockEngine(), FairValueGapEngine(),
    MomentumEngine(), ReversalEngine(), MeanReversionEngine(), RangeTradingEngine(),
    VolatilityExpansionEngine(),
]

# ==============================================================================
# SECTION K — STATE STORE (Tier 1 aggregates + Tier 2 raw log)
# ==============================================================================

def _default_segment_stat() -> dict:
    return {"n": 0, "wins": 0, "losses": 0, "sum_r": 0.0, "sum_conf": 0.0, "sum_conf_correct": 0.0}


def _default_state() -> dict:
    return {
        "schema_version": 3,
        "engine_name": ENGINE_NAME,
        "engine_version": ENGINE_VERSION,
        # Tier 1 — permanent, incrementally-updated aggregates. Never rebuilt
        # by rescanning Tier 2; this is the sole source auto-tuning reads/writes.
        "tier1": {
            "engine_weights": {e.setup_type.value: 1.0 for e in ALL_ENGINES},
            "confidence_calibration": {e.setup_type.value: 0.0 for e in ALL_ENGINES},
            "confluence_quality": {},  # tag -> learned multiplier, default 1.0 (v1.1.0)
            "asset_quality": {},       # symbol -> learned multiplier, default 1.0 (v1.1.0)
            "setup_direction_quality": {},  # v1.2.0: "{setup_type}|{direction}" -> learned multiplier, default 1.0
            "filter_thresholds": {
                "min_confidence": 0.55,
                "min_score": 0.55,
                # v1.2.0: setup_type -> {"min_confidence": x, "min_score": y} overrides,
                # layered on top of the global thresholds above for setup types whose
                # realized win rate is materially below baseline. Populated/adapted by
                # update_setup_overrides(); seeded for momentum since it's the known
                # worst-performing setup type at 20.6% WR / 57% of volume as of the
                # v1.2.0 upgrade.
                "setup_overrides": {
                    SetupType.MOMENTUM.value: {"min_confidence": 0.68, "min_score": 0.65},
                },
            },
            "segment_stats": {
                "by_asset": {}, "by_regime": {}, "by_timeframe": {}, "by_engine": {},
                "by_confluence": {},  # v1.1.0
                "by_direction": {},           # v1.2.0
                "by_setup_direction": {},     # v1.2.0: "{setup_type}|{direction}"
            },
            "filter_funnel": {},  # stage_name -> {"seen": n, "rejected": n}
            "circuit_breaker": {"tripped": False, "tripped_at": None, "reason": None},
            "rolling_live_trades": [],   # list of {"r": float, "win": bool} capped to CIRCUIT_BREAKER_WINDOW*3
            "active_baseline": dict(BASELINE_NOTE),
            "last_daily_summary_date": None,
        },
        # Tier 2 — bounded, prunable raw trade log used for forensic review.
        # Safe to prune on a schedule without affecting Tier 1 / learned behavior.
        "tier2_trade_log": [],
        "pending_signals": [],
        "active_signals": [],
        # v1.2.1: symbol -> epoch-ms timestamp until which new signals on
        # that symbol are suppressed. Set whenever a trade resolves.
        "symbol_cooldowns": {},
        "last_run_ts": None,
    }


def _setup_direction_key(setup_type: str, direction: str) -> str:
    return f"{setup_type}|{direction}"


class StateStore:
    def __init__(self, path: str = STATE_PATH):
        self.path = path
        self.data = self._load()

    def _load(self) -> dict:
        if not os.path.exists(self.path):
            log.info("No existing state at %s; starting fresh.", self.path)
            return _default_state()
        try:
            with open(self.path, "r") as f:
                data = json.load(f)
            base = _default_state()
            _deep_merge_defaults(data, base)
            _migrate_schema(data)
            return data
        except (json.JSONDecodeError, OSError) as e:
            log.error("Failed to read state (%s); starting fresh to avoid crash-looping.", e)
            return _default_state()

    def save(self):
        """Atomic write: write to temp file then rename, so a crash mid-write
        never corrupts state.json."""
        tmp_path = f"{self.path}.tmp.{os.getpid()}"
        try:
            with open(tmp_path, "w") as f:
                json.dump(self.data, f, indent=2, default=str)
            os.replace(tmp_path, self.path)
        except OSError as e:
            log.error("Failed to persist state: %s", e)
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

    # --- Symbol cooldown (v1.2.1) -----------------------------------------

    def cooldown_until(self, symbol: str) -> int:
        """Epoch-ms timestamp until which `symbol` is excluded from new
        signal generation. 0 if no cooldown is set."""
        return self.data.setdefault("symbol_cooldowns", {}).get(symbol, 0)

    def set_cooldown(self, symbol: str, until_ts: int):
        self.data.setdefault("symbol_cooldowns", {})[symbol] = until_ts

    # --- Tier 1 accessors -----------------------------------------------

    def engine_weight(self, setup_type: SetupType) -> float:
        return self.data["tier1"]["engine_weights"].get(setup_type.value, 1.0)

    def confidence_calibration(self, setup_type: SetupType) -> float:
        return self.data["tier1"]["confidence_calibration"].get(setup_type.value, 0.0)

    def filter_threshold(self, name: str, default: float) -> float:
        return self.data["tier1"]["filter_thresholds"].get(name, default)

    def filter_threshold_for_setup(self, setup_type: str, name: str, default: float) -> float:
        """v1.2.0. Returns the effective threshold for a given setup type:
        the setup-specific override if one exists and is stricter than the
        global threshold, otherwise the global threshold. Overrides are only
        ever additive on top of the global gate — a setup can be gated
        harder than everyone else, never let through easier."""
        global_val = self.filter_threshold(name, default)
        overrides = self.data["tier1"]["filter_thresholds"].get("setup_overrides", {})
        override_val = overrides.get(setup_type, {}).get(name)
        if override_val is None:
            return global_val
        return max(global_val, override_val)

    def confluence_quality(self, tag: str) -> float:
        return self.data["tier1"]["confluence_quality"].get(tag, 1.0)

    def asset_quality(self, symbol: str) -> float:
        return self.data["tier1"]["asset_quality"].get(symbol, 1.0)

    def setup_direction_quality(self, setup_type: str, direction: str) -> float:
        key = _setup_direction_key(setup_type, direction)
        return self.data["tier1"]["setup_direction_quality"].get(key, 1.0)

    def is_circuit_breaker_tripped(self) -> bool:
        return bool(self.data["tier1"]["circuit_breaker"].get("tripped", False))

    def get_effective_baseline(self) -> dict:
        return self.data["tier1"].get("active_baseline", dict(BASELINE_NOTE))

    def log_filter_funnel(self, stage: str, rejected: bool):
        funnel = self.data["tier1"]["filter_funnel"]
        entry = funnel.setdefault(stage, {"seen": 0, "rejected": 0})
        entry["seen"] += 1
        if rejected:
            entry["rejected"] += 1

    # --- Segment stats (Tier 1, incremental) ------------------------------

    def _segment(self, bucket: str, key: str) -> dict:
        b = self.data["tier1"]["segment_stats"][bucket]
        return b.setdefault(key, _default_segment_stat())

    def record_trade_incremental(self, asset: str, regime: str, timeframe: str,
                                  engine: str, r_realized: float, win: bool,
                                  confidence: float, confidence_correct: bool,
                                  confluences: Optional[List[str]] = None,
                                  direction: Optional[str] = None):
        """Update Tier-1 aggregates one trade at a time."""
        segments = [("by_asset", asset), ("by_regime", regime),
                    ("by_timeframe", timeframe), ("by_engine", engine)]
        if direction:
            segments.append(("by_direction", direction))
        for bucket, key in segments:
            seg = self._segment(bucket, key)
            seg["n"] += 1
            seg["wins"] += 1 if win else 0
            seg["losses"] += 0 if win else 1
            seg["sum_r"] += r_realized
            seg["sum_conf"] += confidence
            seg["sum_conf_correct"] += 1 if confidence_correct else 0

        # v1.1.0: same treatment, one bucket per distinct confluence tag that
        # fired on this signal. A signal with 3 tags contributes once to each
        # of those 3 tags' stats — deliberately mirrors how by_asset/by_regime
        # already work (the trade "belongs" to every segment it touches).
        for tag in set(confluences or []):
            seg = self._segment("by_confluence", tag)
            seg["n"] += 1
            seg["wins"] += 1 if win else 0
            seg["losses"] += 0 if win else 1
            seg["sum_r"] += r_realized
            seg["sum_conf"] += confidence
            seg["sum_conf_correct"] += 1 if confidence_correct else 0

        # v1.2.0: setup_type + direction combination, e.g. "momentum|long".
        # Lets the learning system suppress a specific bad combination (a
        # setup type that's fine in one direction but poor in the other)
        # instead of only ever being able to act on setup type or direction
        # in isolation.
        if direction:
            seg = self._segment("by_setup_direction", _setup_direction_key(engine, direction))
            seg["n"] += 1
            seg["wins"] += 1 if win else 0
            seg["losses"] += 0 if win else 1
            seg["sum_r"] += r_realized
            seg["sum_conf"] += confidence
            seg["sum_conf_correct"] += 1 if confidence_correct else 0

        rolling = self.data["tier1"]["rolling_live_trades"]
        rolling.append({"r": r_realized, "win": win})
        max_len = CIRCUIT_BREAKER_WINDOW * 3
        if len(rolling) > max_len:
            del rolling[: len(rolling) - max_len]

    def append_tier2(self, record: dict):
        self.data["tier2_trade_log"].append(record)

    def prune_tier2(self):
        """Bounded raw log. Pruning never touches Tier 1, so
        auto-tuning behavior is unaffected by this — verified by construction
        since record_trade_incremental() is the only writer of Tier 1 and it
        runs at trade-resolution time, independent of Tier 2's contents."""
        cutoff_ts = int((datetime.now(timezone.utc) - timedelta(days=TIER2_RETENTION_DAYS)).timestamp() * 1000)
        log_list = self.data["tier2_trade_log"]
        log_list[:] = [r for r in log_list if r.get("resolved_ts", cutoff_ts) >= cutoff_ts]
        if len(log_list) > TIER2_MAX_RECORDS:
            del log_list[: len(log_list) - TIER2_MAX_RECORDS]


def _deep_merge_defaults(data: dict, defaults: dict):
    """Fills in any keys missing from a loaded (possibly older-schema) state
    file with defaults, so schema evolution never crashes a run."""
    for k, v in defaults.items():
        if k not in data:
            data[k] = v
        elif isinstance(v, dict) and isinstance(data.get(k), dict):
            _deep_merge_defaults(data[k], v)


# Tightened filter-threshold floor introduced in the v1.2.0 upgrade. A
# one-time migration (schema_version < 3) raises any existing deployment's
# thresholds up to this floor — never down — so a live engine that had
# drifted or been adapted down to permissive values (e.g. min_confidence
# 0.3) picks up the correction immediately rather than waiting for
# update_filter_thresholds() to slowly climb back up.
SCHEMA_V3_THRESHOLD_FLOOR = {"min_confidence": 0.55, "min_score": 0.55}


def _migrate_schema(data: dict):
    """Applies one-time, backward-only migrations to a loaded state dict.
    Every migration here only ever raises the bar / adds structure — it
    never resets learned values that could still be a legitimate reflection
    of history (e.g. this never touches engine_weights or segment_stats)."""
    version = data.get("schema_version", 1)
    if version < 3:
        thresholds = data.setdefault("tier1", {}).setdefault("filter_thresholds", {})
        for name, floor in SCHEMA_V3_THRESHOLD_FLOOR.items():
            thresholds[name] = max(thresholds.get(name, floor), floor)
        overrides = thresholds.setdefault("setup_overrides", {})
        overrides.setdefault(SetupType.MOMENTUM.value, {"min_confidence": 0.68, "min_score": 0.65})
        seg = data["tier1"].setdefault("segment_stats", {})
        seg.setdefault("by_direction", {})
        seg.setdefault("by_setup_direction", {})
        data["tier1"].setdefault("setup_direction_quality", {})
        log.info("Migrated state schema 2 -> 3: tightened filter_thresholds floor to %s, "
                  "seeded momentum setup_override.", SCHEMA_V3_THRESHOLD_FLOOR)
        data["schema_version"] = 3

# ==============================================================================
# SECTION L — FORENSIC TAGGING
# ==============================================================================

def forensic_tag(outcome: str, sig: Signal, r_realized: float, mae_r: float) -> str:
    """Concrete, specific reason a trade won or lost — feeds the learning
    system so it reinforces genuine signal, not noise.

    v1.2.0 fix: win classification now compares r_realized (the actual
    realized R at resolution) against rr_tp2, not mfe_r. mfe_r is never
    updated on the tp1_tp2_same_candle resolution path (TP1 and TP2 both
    hit within the same candle) since that path resolves immediately
    without going through the mfe_r-tracking branch — so trades that fully
    extended to TP2 in that same candle were being mislabeled as merely
    "TP1 secured" even though r_realized matches rr_tp2. r_realized is
    always correct and available at the point forensic_tag is called, so
    it's the right signal to classify on regardless of which resolution
    path produced the win."""
    if outcome == "win":
        if r_realized >= sig.rr_tp2 * 0.9:
            return "clean_read_full_extension"
        return "correct_read_tp1_secured"
    if outcome == "loss":
        if mae_r <= -0.9:
            if any("liquidity" in c for c in sig.confluences) is False and mae_r <= -0.95:
                return "stopped_out_before_mtf_confirmation"
            return "structure_invalidated_quickly"
        return "chased_a_swept_liquidity_pool" if any("sweep" in c for c in sig.confluences) else "correct_read_poor_rr"
    return "expired_no_fill"


# ==============================================================================
# SECTION M — CONTINUOUS LEARNING / ADAPTIVE PARAMETER UPDATES
# ==============================================================================

def _damped_step(old: float, target: float, max_step_frac: float, lo: float, hi: float) -> float:
    """Exponential-smoothing-style bounded update: blends toward `target` by at
    most `max_step_frac` of the old value's magnitude (or an absolute floor for
    near-zero values), then hard-clamps to [lo, hi]. This is what makes
    'never overfit to recent trades' structural rather than a slogan."""
    span = max(abs(old), 0.25)
    max_delta = max_step_frac * span
    delta = max(-max_delta, min(max_delta, target - old))
    return max(lo, min(hi, old + delta))


def update_engine_weights(store: StateStore):
    """Raise a specialized engine's weight when its segment-level expectancy is
    trending above baseline; lower it when trending below. Directional, bounded,
    dampened.

    v1.2.0: a segment whose trailing win rate is far below baseline (gap >=
    SEVERE_UNDERPERFORM_WR_GAP) now gets an accelerated step instead of the
    standard ADAPT_MAX_STEP. This is still a damped, bounded step — never a
    jump straight to target — but a segment that's this bad has enough
    signal (not noise) behind it to warrant moving faster than a segment
    sitting near baseline. Without this, a fixed fractional step converges
    geometrically slowly: 34 losing-heavy momentum trades only moved that
    engine's weight from 1.0 to 0.83 under the old fixed-step rule."""
    by_engine = store.data["tier1"]["segment_stats"]["by_engine"]
    weights = store.data["tier1"]["engine_weights"]
    baseline = store.get_effective_baseline()
    for setup_key, seg in by_engine.items():
        if seg["n"] < MIN_SAMPLE_SIZE:
            continue
        win_rate = seg["wins"] / seg["n"]
        avg_r = seg["sum_r"] / seg["n"]
        expectancy_edge = (win_rate - baseline["win_rate"]) + 0.25 * (avg_r - (baseline["avg_rr"] * baseline["win_rate"] - (1 - baseline["win_rate"])))
        target = 1.0 + max(-0.5, min(0.5, expectancy_edge * 2.0))
        old = weights.get(setup_key, 1.0)

        wr_gap = baseline["win_rate"] - win_rate
        if wr_gap >= SEVERE_UNDERPERFORM_WR_GAP:
            step_frac = min(ADAPT_MAX_STEP * ACCELERATED_ADAPT_STEP_MULT, ACCELERATED_ADAPT_STEP_CAP)
        else:
            step_frac = ADAPT_MAX_STEP
        weights[setup_key] = _damped_step(old, target, step_frac, ENGINE_WEIGHT_MIN, ENGINE_WEIGHT_MAX)


def update_confidence_calibration(store: StateStore):
    """If a setup's stated confidence has systematically over/under-predicted
    realized win rate, nudge a calibration offset (never the raw score itself,
    which stays an auditable model output)."""
    by_engine = store.data["tier1"]["segment_stats"]["by_engine"]
    calib = store.data["tier1"]["confidence_calibration"]
    for setup_key, seg in by_engine.items():
        if seg["n"] < MIN_SAMPLE_SIZE:
            continue
        avg_conf = seg["sum_conf"] / seg["n"]
        realized_wr = seg["wins"] / seg["n"]
        error = realized_wr - avg_conf
        old = calib.get(setup_key, 0.0)
        target = old + error
        calib[setup_key] = _damped_step(old, target, ADAPT_MAX_STEP, CONF_CALIBRATION_MIN, CONF_CALIBRATION_MAX)


def update_confluence_quality(store: StateStore):
    """v1.1.0. Same directional/bounded/damped treatment as engine weights,
    but per confluence tag instead of per engine type — this is what lets the
    system learn 'liquidity_sweep confluence tends to win' independent of
    which of the 13 engines happened to attach that tag."""
    by_conf = store.data["tier1"]["segment_stats"]["by_confluence"]
    quality = store.data["tier1"]["confluence_quality"]
    baseline = store.get_effective_baseline()
    for tag, seg in by_conf.items():
        if seg["n"] < MIN_SAMPLE_SIZE:
            continue
        win_rate = seg["wins"] / seg["n"]
        avg_r = seg["sum_r"] / seg["n"]
        expectancy_edge = (win_rate - baseline["win_rate"]) + 0.25 * (avg_r - (baseline["avg_rr"] * baseline["win_rate"] - (1 - baseline["win_rate"])))
        target = 1.0 + max(-0.3, min(0.3, expectancy_edge * 2.0))
        old = quality.get(tag, 1.0)
        quality[tag] = _damped_step(old, target, ADAPT_MAX_STEP, CONFLUENCE_QUALITY_MIN, CONFLUENCE_QUALITY_MAX)


def update_asset_quality(store: StateStore):
    """v1.1.0. Same again, per traded symbol. by_asset stats were already
    being collected; this is the first thing that actually reads them back
    into scoring."""
    by_asset = store.data["tier1"]["segment_stats"]["by_asset"]
    quality = store.data["tier1"]["asset_quality"]
    baseline = store.get_effective_baseline()
    for symbol, seg in by_asset.items():
        if seg["n"] < MIN_SAMPLE_SIZE:
            continue
        win_rate = seg["wins"] / seg["n"]
        avg_r = seg["sum_r"] / seg["n"]
        expectancy_edge = (win_rate - baseline["win_rate"]) + 0.25 * (avg_r - (baseline["avg_rr"] * baseline["win_rate"] - (1 - baseline["win_rate"])))
        target = 1.0 + max(-0.3, min(0.3, expectancy_edge * 2.0))
        old = quality.get(symbol, 1.0)
        quality[symbol] = _damped_step(old, target, ADAPT_MAX_STEP, ASSET_QUALITY_MIN, ASSET_QUALITY_MAX)


def update_setup_direction_quality(store: StateStore):
    """v1.2.0. Same directional/bounded/damped treatment as engine weights
    and confluence quality, but per setup_type+direction combination — this
    is what lets the system learn that e.g. 'momentum + long' specifically
    underperforms even relative to what momentum's own engine_weight or a
    generic long/short split would predict, and suppress that exact
    combination without touching momentum shorts or other engines' longs.
    Uses a wider (and more asymmetric) bound than confluence/asset quality
    since this is explicitly meant to be able to suppress a bad combination
    hard, not just nudge it."""
    by_combo = store.data["tier1"]["segment_stats"]["by_setup_direction"]
    quality = store.data["tier1"]["setup_direction_quality"]
    baseline = store.get_effective_baseline()
    for key, seg in by_combo.items():
        if seg["n"] < MIN_SAMPLE_SIZE:
            continue
        win_rate = seg["wins"] / seg["n"]
        avg_r = seg["sum_r"] / seg["n"]
        expectancy_edge = (win_rate - baseline["win_rate"]) + 0.25 * (avg_r - (baseline["avg_rr"] * baseline["win_rate"] - (1 - baseline["win_rate"])))
        target = 1.0 + max(-0.6, min(0.2, expectancy_edge * 2.0))
        old = quality.get(key, 1.0)
        wr_gap = baseline["win_rate"] - win_rate
        step_frac = (min(ADAPT_MAX_STEP * ACCELERATED_ADAPT_STEP_MULT, ACCELERATED_ADAPT_STEP_CAP)
                     if wr_gap >= SEVERE_UNDERPERFORM_WR_GAP else ADAPT_MAX_STEP)
        quality[key] = _damped_step(old, target, step_frac, SETUP_DIRECTION_QUALITY_MIN, SETUP_DIRECTION_QUALITY_MAX)


def update_setup_overrides(store: StateStore):
    """v1.2.0. Generalizes the momentum-specific filter override seeded at
    migration time: any setup type whose realized win rate sits materially
    below baseline (gap >= SETUP_OVERRIDE_WR_GAP) gets its own, stricter
    min_confidence/min_score gate that tightens further the worse it keeps
    performing, damped and bounded like every other adaptive parameter. A
    setup type that recovers to within the gap of baseline has its override
    relaxed back down (never below the global threshold) rather than staying
    permanently punished for a stretch of bad trades that's since resolved."""
    by_engine = store.data["tier1"]["segment_stats"]["by_engine"]
    thresholds = store.data["tier1"]["filter_thresholds"]
    overrides = thresholds.setdefault("setup_overrides", {})
    baseline = store.get_effective_baseline()
    global_min_conf = thresholds.get("min_confidence", 0.55)
    global_min_score = thresholds.get("min_score", 0.55)

    for setup_key, seg in by_engine.items():
        if seg["n"] < MIN_SAMPLE_SIZE:
            continue
        win_rate = seg["wins"] / seg["n"]
        wr_gap = baseline["win_rate"] - win_rate
        existing = overrides.get(setup_key, {})
        old_conf = existing.get("min_confidence", global_min_conf)
        old_score = existing.get("min_score", global_min_score)

        if wr_gap >= SETUP_OVERRIDE_WR_GAP:
            # Scale the bump with how bad the gap is, capped so a single
            # setup type can never be gated into practical non-existence.
            bump = min(SETUP_OVERRIDE_MAX_BUMP, wr_gap)
            target_conf = min(global_min_conf + bump, FILTER_THRESHOLD_MAX)
            target_score = min(global_min_score + bump, FILTER_THRESHOLD_MAX)
        else:
            # Recovered (or never was bad) -> relax back toward the global
            # threshold, never below it.
            target_conf = global_min_conf
            target_score = global_min_score

        new_conf = _damped_step(old_conf, target_conf, SETUP_OVERRIDE_MAX_STEP, global_min_conf, FILTER_THRESHOLD_MAX)
        new_score = _damped_step(old_score, target_score, SETUP_OVERRIDE_MAX_STEP, global_min_score, FILTER_THRESHOLD_MAX)

        if new_conf <= global_min_conf + 1e-9 and new_score <= global_min_score + 1e-9:
            overrides.pop(setup_key, None)
        else:
            overrides[setup_key] = {"min_confidence": new_conf, "min_score": new_score}


def update_baseline_from_live(store: StateStore):
    """v1.1.0 fix. BASELINE_NOTE was documented as a placeholder to be
    'replaced by live stats once enough trades resolve', but no prior code
    path ever did that — active_baseline sat frozen at the pre-deployment
    prior forever. This makes that real: once there are enough resolved
    trades to trust, win_rate/profit_factor/avg_rr blend toward realized
    live performance, damped the same way as every other adaptive param so
    a short streak can't yank the baseline (and therefore the circuit
    breaker and every expectancy calc) around."""
    by_engine = store.data["tier1"]["segment_stats"]["by_engine"]
    total_n = sum(s["n"] for s in by_engine.values())
    if total_n < BASELINE_MIN_LIVE_TRADES:
        return

    total_wins = sum(s["wins"] for s in by_engine.values())
    total_r = sum(s["sum_r"] for s in by_engine.values())
    total_losses = total_n - total_wins
    live_win_rate = total_wins / total_n
    # Recover average winning-trade R from the aggregate: sum_r = wins*avg_win_r - losses*1.0
    live_avg_rr = ((total_r + total_losses) / total_wins) if total_wins > 0 else BASELINE_NOTE["avg_rr"]

    rolling = store.data["tier1"]["rolling_live_trades"]
    gains = sum(t["r"] for t in rolling if t["r"] > 0)
    losses = -sum(t["r"] for t in rolling if t["r"] < 0)
    live_pf = (gains / losses) if losses > 0 else BASELINE_NOTE["profit_factor"]

    baseline = store.data["tier1"]["active_baseline"]
    baseline["win_rate"] = _damped_step(baseline["win_rate"], live_win_rate, BASELINE_ADAPT_MAX_STEP, 0.10, 0.90)
    baseline["profit_factor"] = _damped_step(baseline["profit_factor"], live_pf, BASELINE_ADAPT_MAX_STEP, 0.30, 5.0)
    baseline["avg_rr"] = _damped_step(baseline["avg_rr"], live_avg_rr, BASELINE_ADAPT_MAX_STEP, 0.50, 5.0)


def update_filter_thresholds(store: StateStore):
    """Filter-funnel-attrition-driven tuning: a filter killing a
    large share of candidates with no realized-quality lift gets relaxed; one
    passing too much low-quality flow gets tightened."""
    funnel = store.data["tier1"]["filter_funnel"]
    thresholds = store.data["tier1"]["filter_thresholds"]
    overall = store.data["tier1"]["segment_stats"]["by_engine"]
    total_n = sum(s["n"] for s in overall.values())
    total_wins = sum(s["wins"] for s in overall.values())
    if total_n < MIN_SAMPLE_SIZE:
        return
    realized_wr = total_wins / total_n
    baseline_wr = store.get_effective_baseline()["win_rate"]

    conf_stage = funnel.get("min_confidence", {"seen": 0, "rejected": 0})
    if conf_stage["seen"] >= 20:
        attrition = conf_stage["rejected"] / conf_stage["seen"]
        old = thresholds.get("min_confidence", 0.55)
        if realized_wr <= baseline_wr:
            target = old + 0.05    # quality at/below baseline -> tighten the gate, regardless
                                    # of attrition; this is the case that most needs a stricter
                                    # filter, not a looser one
        elif attrition > 0.6 and realized_wr >= baseline_wr + 0.05:
            target = old - 0.03    # filter is over-rejecting even though the signals that DO
                                    # pass are already meeting/beating baseline -> safe to relax
        else:
            target = old
        thresholds["min_confidence"] = _damped_step(old, target, ADAPT_MAX_STEP, FILTER_THRESHOLD_MIN, FILTER_THRESHOLD_MAX)


def evaluate_circuit_breaker(store: StateStore, telegram: "TelegramNotifier"):
    """mandatory live-performance circuit breaker."""
    rolling = store.data["tier1"]["rolling_live_trades"][-CIRCUIT_BREAKER_WINDOW:]
    cb = store.data["tier1"]["circuit_breaker"]
    if len(rolling) < CIRCUIT_BREAKER_WINDOW:
        return  # not statistically meaningful yet — same min-sample-size rule

    wins = sum(1 for t in rolling if t["win"])
    win_rate = wins / len(rolling)
    gains = sum(t["r"] for t in rolling if t["r"] > 0)
    losses = -sum(t["r"] for t in rolling if t["r"] < 0)
    profit_factor = (gains / losses) if losses > 0 else float("inf")

    baseline = store.get_effective_baseline()
    wr_dropped = (baseline["win_rate"] - win_rate) >= CIRCUIT_BREAKER_WR_DROP
    pf_dropped = profit_factor < baseline["profit_factor"] * (1 - CIRCUIT_BREAKER_PF_DROP)

    if not cb.get("tripped") and (wr_dropped or pf_dropped):
        cb["tripped"] = True
        cb["tripped_at"] = datetime.now(timezone.utc).isoformat()
        reason = []
        if wr_dropped:
            reason.append(f"win rate {win_rate:.0%} vs baseline {baseline['win_rate']:.0%}")
        if pf_dropped:
            reason.append(f"profit factor {profit_factor:.2f} vs baseline {baseline['profit_factor']:.2f}")
        cb["reason"] = "; ".join(reason)
        telegram.send(
            f"*⚠️ {ENGINE_NAME} {ENGINE_VERSION} — CIRCUIT BREAKER TRIPPED*\n"
            f"Rolling live performance deviated materially below baseline: {cb['reason']}.\n"
            f"Automatic parameter adaptation is now FROZEN at last-known-good values. "
            f"Signal generation continues unaffected. Adaptation resumes automatically once "
            f"a fresh {CIRCUIT_BREAKER_WINDOW}-trade window recovers to baseline."
        )
    elif cb.get("tripped") and not (wr_dropped or pf_dropped):
        cb["tripped"] = False
        cb["tripped_at"] = None
        cb["reason"] = None
        telegram.send(
            f"*✅ {ENGINE_NAME} {ENGINE_VERSION} — Circuit breaker cleared*\n"
            f"Rolling live performance recovered to baseline. Automatic adaptation resumed."
        )


def run_learning_cycle(store: StateStore, telegram: "TelegramNotifier"):
    """Entry point called once per run after any trades resolve. Bound by
    minimum-sample-size rule inside each update function."""
    evaluate_circuit_breaker(store, telegram)
    if store.is_circuit_breaker_tripped():
        log.info("Circuit breaker tripped — skipping all adaptive updates this run.")
        return
    update_baseline_from_live(store)      # v1.1.0 — do this first so today's
                                           # weight/calibration/threshold math
                                           # below reads the freshest baseline
    update_engine_weights(store)
    update_confidence_calibration(store)
    update_confluence_quality(store)      # v1.1.0
    update_asset_quality(store)           # v1.1.0
    update_setup_direction_quality(store) # v1.2.0
    update_filter_thresholds(store)
    update_setup_overrides(store)         # v1.2.0

# ==============================================================================
# SECTION N — DECISION ENGINE
# ==============================================================================

class DecisionEngine:
    def __init__(self, store: StateStore):
        self.store = store

    def score_signal(self, sig: Signal, ctx: MarketContext) -> Optional[RankedSignal]:
        store = self.store

        # Hard invalidation gates.
        atr_ltf = float(ctx.atr_ltf[-1])
        shape = validate_signal_shape(sig, ctx.price, atr_ltf)
        store.log_filter_funnel("shape_validation", rejected=not shape.ok)
        if not shape.ok:
            return None

        liq_mult = liquidity_sanity_check(sig, ctx.pools, atr_ltf)
        store.log_filter_funnel("liquidity_sanity", rejected=liq_mult <= 0.35)
        if liq_mult <= 0.35:
            return None

        # Regime-fit: a strong multiplier, never a hard gate.
        best_regimes = ENGINE_REGIME_FIT.get(sig.setup_type, [])
        regime_fit_mult = 1.0 if ctx.regime in best_regimes else 0.55

        engine_weight = store.engine_weight(sig.setup_type)
        calibration = store.confidence_calibration(sig.setup_type)
        calibrated_conf = max(0.01, min(0.99, sig.confidence + calibration))

        # v1.2.0: setup-type-specific override, layered on top of (never
        # below) the global gate. This is what lets a materially
        # underperforming setup type like momentum get filtered harder
        # without a global threshold bump also cutting into a strong
        # performer like liquidity_sweep.
        min_conf = store.filter_threshold_for_setup(sig.setup_type.value, "min_confidence", 0.55)
        store.log_filter_funnel("min_confidence", rejected=calibrated_conf < min_conf)
        if calibrated_conf < min_conf:
            return None

        confluence_bonus = min(len(sig.confluences) * 0.03, 0.15)  # additive, not an AND-gate
        rr_quality = min((sig.rr_tp1 - TP1_RR_MIN) / (TP1_RR_SOFT_CEILING - TP1_RR_MIN + 1e-9), 1.0)
        rr_quality = max(rr_quality, 0.0)

        ev = calibrated_conf * sig.rr_tp1 - (1 - calibrated_conf) * 1.0  # EV in R, loss = -1R

        raw_score = (
            0.30 * calibrated_conf +
            0.20 * rr_quality +
            0.15 * confluence_bonus / 0.15 +
            0.15 * regime_fit_mult +
            0.10 * liq_mult +
            0.10 * min(max(ev, -1.0), 2.0) / 2.0
        )

        # v1.1.0: confluence_bonus above only ever rewarded *how many* tags a
        # signal had, never *which* ones — the engine had no way to learn that
        # e.g. a liquidity-sweep tag has actually been a better predictor than
        # a generic momentum tag. These two multipliers close that gap using
        # the same per-segment win-rate/avg-R learning as engine_weight,
        # applied to confluence tags and to the traded asset itself. Tags/
        # assets with no data yet default to a neutral 1.0 (no free bonus,
        # no unearned penalty).
        conf_tags = set(sig.confluences)
        confluence_quality_mult = (
            sum(store.confluence_quality(t) for t in conf_tags) / len(conf_tags)
            if conf_tags else 1.0
        )
        asset_quality_mult = store.asset_quality(sig.symbol)

        # v1.2.0: learned quality of this exact setup_type+direction
        # combination (e.g. "momentum|long"), independent of the setup
        # type's own engine_weight — see update_setup_direction_quality().
        setup_direction_quality_mult = store.setup_direction_quality(sig.setup_type.value, sig.direction)

        final_score = (raw_score * engine_weight * confluence_quality_mult *
                        asset_quality_mult * setup_direction_quality_mult)

        min_score = store.filter_threshold_for_setup(sig.setup_type.value, "min_score", 0.55)
        store.log_filter_funnel("min_score", rejected=final_score < min_score)
        if final_score < min_score:
            return None

        if final_score >= 0.85 and calibrated_conf >= 0.75 and rr_quality >= 0.5:
            tier = "A+"
        elif final_score >= 0.68:
            tier = "A"
        else:
            tier = "B"

        return RankedSignal(signal=sig, score=final_score, tier=tier, ev=ev,
                             engine_weight=engine_weight, regime_fit_mult=regime_fit_mult,
                             confluence_quality_mult=confluence_quality_mult,
                             asset_quality_mult=asset_quality_mult,
                             setup_direction_quality_mult=setup_direction_quality_mult)

    def rank_and_select(self, candidates: List[RankedSignal], active_symbols_sectors: Dict[str, int]) -> List[RankedSignal]:
        """Applies the correlation cap and the concurrency cap on top of raw ranking. One candidate per symbol max per
        scan."""
        candidates.sort(key=lambda r: r.score, reverse=True)
        selected: List[RankedSignal] = []
        seen_symbols: set = set()
        sector_counts = dict(active_symbols_sectors)

        for cand in candidates:
            sym = cand.signal.symbol
            if sym in seen_symbols:
                continue
            sector = SECTOR_MAP.get(sym, sym)
            if sector_counts.get(sector, 0) >= MAX_CONCURRENT_PER_SECTOR:
                continue
            if len(selected) + sum(active_symbols_sectors.values()) >= MAX_CONCURRENT_ACTIVE_SIGNALS:
                break
            selected.append(cand)
            seen_symbols.add(sym)
            sector_counts[sector] = sector_counts.get(sector, 0) + 1

        return selected

# ==============================================================================
# SECTION O — TRADE LIFECYCLE
# ==============================================================================
#
# Evaluation order: entry fill is checked before any SL/TP evaluation. If a
# single candle contains both SL and TP1, the one closer to that candle's
# open is assumed reached first, instead of defaulting to "SL always wins."
# ==============================================================================

class TradeLifecycleManager:
    def __init__(self, client: HyperliquidClient, store: StateStore, telegram: "TelegramNotifier"):
        self.client = client
        self.store = store
        self.telegram = telegram

    def register(self, ranked: List[RankedSignal]):
        for r in ranked:
            sig = r.signal
            rec = asdict(sig)
            rec.update({
                "tier": r.tier, "score": r.score,
                "status": "pending" if sig.entry_kind == "pending" else "active",
                "tp1_hit": False, "tp1_r": None, "mae_r": 0.0, "mfe_r": 0.0,
                "last_checked_ts": sig.created_ts, "bars_pending": 0,
            })
            if sig.entry_kind == "market":
                rec["entry_filled"] = True
                rec["fill_ts"] = sig.created_ts
            self.store.data["active_signals"].append(rec)
            self.telegram.send_new_signal(rec)

    def monitor_all(self):
        still_active = []
        for rec in self.store.data["active_signals"]:
            outcome = self._monitor_one(rec)
            if outcome is None:
                still_active.append(rec)
            # else: resolved, dropped from active_signals, recorded to state.
        self.store.data["active_signals"] = still_active

    def _monitor_one(self, rec: dict) -> Optional[str]:
        symbol = rec["symbol"]
        candles = self.client.get_candles(symbol, TF_LTF, CANDLE_LOOKBACK[TF_LTF])
        new_candles = [c for c in candles if c.ts > rec["last_checked_ts"]]
        if not new_candles:
            return None

        direction = rec["direction"]
        entry, sl, tp1, tp2 = rec["entry"], rec["sl"], rec["tp1"], rec["tp2"]

        for c in new_candles:
            rec["last_checked_ts"] = c.ts

            if not rec["entry_filled"]:
                rec["bars_pending"] += 1
                filled_this_candle = c.low <= entry <= c.high
                if not filled_this_candle:
                    if rec["bars_pending"] >= rec.get("pending_expiry_bars", 0) and rec.get("entry_kind") == "pending":
                        self._resolve_expired(rec)
                        return "expired"
                    continue
                rec["entry_filled"] = True
                rec["fill_ts"] = c.ts
                # fall through: this same candle may still register SL/TP.

            hit_sl = (c.low <= sl) if direction == "long" else (c.high >= sl)
            hit_tp1 = (c.high >= tp1) if direction == "long" else (c.low <= tp1)
            hit_tp2 = (c.high >= tp2) if direction == "long" else (c.low <= tp2)

            if hit_sl and hit_tp1 and not rec["tp1_hit"]:
                dist_sl = abs(c.open - sl)
                dist_tp1 = abs(c.open - tp1)
                tp1_first = dist_tp1 <= dist_sl
            else:
                tp1_first = False

            if rec["tp1_hit"]:
                # Original SL never moves — still checked as-is.
                if hit_tp2:
                    self._resolve_win(rec, r_realized=self._rr(direction, entry, sl, tp2), reason="tp2_hit")
                    return "tp2"
                if hit_sl:
                    # TP1 already secured, later returns to original SL -> WIN,
                    # credited at TP1's realized R.
                    self._resolve_win(rec, r_realized=rec["tp1_r"], reason="tp1_then_sl_still_win")
                    return "sl_after_tp1"
                mfe_now = self._rr(direction, entry, sl, c.high if direction == "long" else c.low)
                rec["mfe_r"] = max(rec["mfe_r"], mfe_now)
                continue

            if hit_sl and not hit_tp1:
                self._resolve_loss(rec, r_realized=-1.0)
                return "sl"
            if hit_tp1 and not hit_sl:
                rec["tp1_hit"] = True
                rec["tp1_r"] = self._rr(direction, entry, sl, tp1)
                self.telegram.send_status_update(rec, "TP1")
                if hit_tp2:
                    self._resolve_win(rec, r_realized=self._rr(direction, entry, sl, tp2), reason="tp1_tp2_same_candle")
                    return "tp2"
                continue
            if hit_sl and hit_tp1 and tp1_first:
                rec["tp1_hit"] = True
                rec["tp1_r"] = self._rr(direction, entry, sl, tp1)
                self.telegram.send_status_update(rec, "TP1")
                continue
            if hit_sl and hit_tp1 and not tp1_first:
                self._resolve_loss(rec, r_realized=-1.0)
                return "sl"

            mfe_now = self._rr(direction, entry, sl, c.high if direction == "long" else c.low)
            mae_now = self._rr(direction, entry, sl, c.low if direction == "long" else c.high)
            rec["mfe_r"] = max(rec["mfe_r"], mfe_now)
            rec["mae_r"] = min(rec["mae_r"], mae_now)

        return None

    @staticmethod
    def _rr(direction: str, entry: float, sl: float, price: float) -> float:
        risk = abs(entry - sl) or 1e-9
        move = (price - entry) if direction == "long" else (entry - price)
        return move / risk

    def _resolve_win(self, rec: dict, r_realized: float, reason: str):
        rec["status"] = "closed_win"
        tag = forensic_tag("win", Signal(**{k: rec[k] for k in Signal.__dataclass_fields__ if k in rec}),
                            r_realized, rec["mae_r"])
        self._commit_resolution(rec, "win", r_realized, tag)
        self._apply_cooldown(rec, COOLDOWN_BARS_AFTER_WIN)
        # v1.2.2: TP1 was already reported via send_status_update when it hit.
        # SL later returning to breakeven-locked risk doesn't change the
        # outcome (still a win, still fully counted in stats/cooldown above)
        # and isn't actionable, so skip the redundant resolution ping for it.
        if reason != "tp1_then_sl_still_win":
            self.telegram.send_resolution(rec, "WIN", r_realized, reason)

    def _resolve_loss(self, rec: dict, r_realized: float):
        rec["status"] = "closed_loss"
        tag = forensic_tag("loss", Signal(**{k: rec[k] for k in Signal.__dataclass_fields__ if k in rec}),
                            r_realized, rec["mae_r"])
        self._commit_resolution(rec, "loss", r_realized, tag)
        self._apply_cooldown(rec, COOLDOWN_BARS_AFTER_LOSS)
        self.telegram.send_resolution(rec, "LOSS", r_realized, "sl_hit_no_tp1")

    def _apply_cooldown(self, rec: dict, bars: int):
        """v1.2.1: block new signals on this symbol until `bars` LTF bars
        after resolution, so a stop-out (or take-profit) can't be immediately
        followed by a fresh signal on the same symbol in this same scan."""
        until_ts = rec["resolved_ts"] + bars * TF_MS[TF_LTF]
        self.store.set_cooldown(rec["symbol"], until_ts)

    def _resolve_expired(self, rec: dict):
        """Distinct, excluded result type — never touches win/loss
        statistics, engine weights, confidence calibration, or the daily
        summary's win-rate math."""
        rec["status"] = "expired_no_fill"
        rec["resolved_ts"] = rec["last_checked_ts"]
        self.store.append_tier2({**rec, "outcome": "expired_no_fill", "forensic_tag": "no_fill_expired"})
        # v1.2.2: no telegram ping for silent-fail expiries -- not
        # actionable, since there was never a position. Still resolved and
        # logged to tier2 for forensic review; just not sent to chat.

    def _commit_resolution(self, rec: dict, outcome: str, r_realized: float, forensic: str):
        rec["resolved_ts"] = rec["last_checked_ts"]
        confidence_correct = (outcome == "win" and rec["confidence"] >= 0.5) or (outcome == "loss" and rec["confidence"] < 0.5)
        self.store.record_trade_incremental(
            asset=rec["symbol"], regime=rec["regime_at_signal"], timeframe=rec["timeframe"],
            engine=rec["setup_type"], r_realized=r_realized, win=(outcome == "win"),
            confidence=rec["confidence"], confidence_correct=confidence_correct,
            confluences=rec.get("confluences", []), direction=rec.get("direction"),
        )
        self.store.append_tier2({**rec, "outcome": outcome, "r_realized": r_realized, "forensic_tag": forensic})

# ==============================================================================
# SECTION P — TELEGRAM INTEGRATION
# ==============================================================================

_ACRONYMS = {"htf", "ltf", "mtf", "tf", "rsi", "adx", "fvg", "ob", "bos", "tp", "sl", "ema"}


def _pretty(tag: str) -> str:
    """Turns snake_case tags into readable labels, e.g. htf_bias_bullish ->
    HTF Bias Bullish, choch -> CHoCH."""
    words = tag.replace("_", " ").split(" ")
    out = []
    for w in words:
        lw = w.lower()
        if lw in _ACRONYMS:
            out.append(w.upper())
        elif lw == "choch":
            out.append("CHoCH")
        else:
            out.append(w.capitalize())
    return " ".join(out)


class TelegramNotifier:
    def __init__(self, token: str = TG_BOT_TOKEN, chat_id: str = TG_CHAT_ID):
        self.token = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id)

    def send(self, text: str, reply_to: Optional[int] = None) -> Optional[int]:
        if not self.enabled:
            log.info("[telegram-disabled] %s", text.replace("\n", " | "))
            return None
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        params = {"chat_id": self.chat_id, "text": text, "parse_mode": "Markdown"}
        if reply_to:
            params["reply_to_message_id"] = reply_to
        resp = http_get_form(url, params)
        if resp and resp.get("ok"):
            return resp["result"]["message_id"]
        return None

    @staticmethod
    def _price_line(label: str, value: float) -> str:
        # Bare number only inside the monospace span; label stays outside so a
        # single tap copies exactly that number. Precision scales with price
        # magnitude: fewer decimals for BTC-sized prices, more for sub-$1 alts.
        if not value:
            formatted = "0"
        else:
            av = abs(value)
            dp = 2 if av >= 100 else 4 if av >= 1 else 6
            formatted = f"{value:.{dp}f}".rstrip("0").rstrip(".")
        return f"{label}: `{formatted}`"

    def send_new_signal(self, rec: dict):
        dir_emoji = "🟢" if rec["direction"] == "long" else "🔴"
        pending_note = "\n_Pending — expires in {}h_".format(
            round(rec.get("pending_expiry_bars", 0) * 0.25, 1)
        ) if rec["entry_kind"] == "pending" else ""
        text = (
            f"*{ENGINE_NAME} {ENGINE_VERSION}*\n"
            f"*{rec['symbol']} — {rec['direction'].upper()} {dir_emoji}*\n\n"
            f"Setup: {rec['setup_type'].replace('_', ' ').title()}  |  Tier: {rec['tier']}\n"
            f"Regime: {_pretty(rec['regime_at_signal'])}  |  Confidence: {rec['confidence']:.0%}\n\n"
            f"{self._price_line('Entry', rec['entry'])}\n"
            f"{self._price_line('SL', rec['sl'])}\n"
            f"{self._price_line('TP1', rec['tp1'])}\n"
            f"{self._price_line('TP2', rec['tp2'])}\n\n"
            f"RR: {rec['rr_tp1']:.2f} / {rec['rr_tp2']:.2f}\n\n"
            f"Confluences: {', '.join(_pretty(c) for c in rec['confluences'])}"
            f"{pending_note}"
        )
        mid = self.send(text)
        rec["telegram_message_id"] = mid

    def send_status_update(self, rec: dict, status: str):
        text = (
            f"*{rec['symbol']} — {status} hit*\n\n"
            f"{self._price_line('Entry', rec['entry'])}\n"
            f"{self._price_line('SL', rec['sl'])}\n"
            f"{self._price_line('TP1', rec['tp1'])}\n"
            f"{self._price_line('TP2', rec['tp2'])}\n\n"
            f"_SL unchanged — no auto-breakeven. Move it yourself if you want to lock it in._"
        )
        self.send(text, reply_to=rec.get("telegram_message_id"))

    def send_resolution(self, rec: dict, outcome: str, r_realized: float, reason: str):
        if reason == "tp1_then_sl_still_win":
            headline = f"*{rec['symbol']} — TP1 secured, SL later hit → WIN*"
            detail = "TP1 secured before price returned to SL. Counted as a WIN."
        elif outcome == "WIN":
            headline = f"*{rec['symbol']} — TP2 hit → WIN*"
            detail = "Full extension to TP2."
        else:
            headline = f"*{rec['symbol']} — SL hit, no TP1 → LOSS*"
            detail = "Stopped out before TP1."
        text = f"{headline}\n{detail}\n\nRealized R: {r_realized:+.2f}"
        self.send(text, reply_to=rec.get("telegram_message_id"))

    def send_expired(self, rec: dict):
        # v1.2.2: kept for reference / easy re-enabling, but no longer
        # called by _resolve_expired -- expired (never-filled) signals now
        # resolve silently with no telegram notification.
        text = (
            f"*{rec['symbol']} — EXPIRED (no fill)*\n"
            f"Never filled within {rec.get('pending_expiry_bars', 0)} bars. Cancelled, excluded from stats."
        )
        self.send(text, reply_to=rec.get("telegram_message_id"))

    def send_daily_summary(self, store: StateStore):
        seg = store.data["tier1"]["segment_stats"]
        by_engine = seg["by_engine"]
        by_regime = seg["by_regime"]
        total_n = sum(s["n"] for s in by_engine.values())
        total_wins = sum(s["wins"] for s in by_engine.values())
        total_r = sum(s["sum_r"] for s in by_engine.values())
        wr = (total_wins / total_n) if total_n else 0.0
        avg_rr = (total_r / total_n) if total_n else 0.0
        rolling = store.data["tier1"]["rolling_live_trades"]
        loss_r = -sum(t["r"] for t in rolling if t["r"] < 0)
        gain_r = sum(t["r"] for t in rolling if t["r"] > 0)
        pf_str = f"{gain_r / loss_r:.2f}" if loss_r > 0 else ("—" if not rolling else "∞")

        conf_acc_num = sum(s["sum_conf_correct"] for s in by_engine.values())
        conf_acc = (conf_acc_num / total_n) if total_n else 0.0

        engine_lines = "\n".join(
            f"  {_pretty(k)}: {v['wins']}/{v['n']} ({(v['wins']/v['n']*100 if v['n'] else 0):.0f}%)"
            for k, v in sorted(by_engine.items())
        ) or "  (no resolved trades yet)"
        regime_lines = "\n".join(
            f"  {_pretty(k)}: {v['wins']}/{v['n']} ({(v['wins']/v['n']*100 if v['n'] else 0):.0f}%)"
            for k, v in sorted(by_regime.items())
        ) or "  (no resolved trades yet)"

        baseline = store.get_effective_baseline()
        conf_q = store.data["tier1"]["confluence_quality"]
        asset_q = store.data["tier1"]["asset_quality"]
        top_conf = sorted(conf_q.items(), key=lambda kv: kv[1], reverse=True)[:3]
        bottom_conf = sorted(conf_q.items(), key=lambda kv: kv[1])[:3]
        conf_q_line = (
            "  Best: " + ", ".join(f"{_pretty(k)} ({v:.2f}x)" for k, v in top_conf) + "\n"
            "  Worst: " + ", ".join(f"{_pretty(k)} ({v:.2f}x)" for k, v in bottom_conf)
        ) if conf_q else "  (not enough resolved trades per tag yet)"

        text = (
            f"*{ENGINE_NAME} {ENGINE_VERSION} — Daily Summary*\n\n"
            f"Total signals: {total_n}\n"
            f"Wins/Losses: {total_wins}/{total_n - total_wins}\n"
            f"Win rate: {wr:.1%}\n"
            f"Profit factor: {pf_str}\n"
            f"Average RR: {avg_rr:.2f}\n"
            f"Confidence calibration accuracy: {conf_acc:.1%}\n\n"
            f"Live baseline (win rate / PF / avg RR): "
            f"{baseline['win_rate']:.1%} / {baseline['profit_factor']:.2f} / {baseline['avg_rr']:.2f}\n\n"
            f"By regime:\n{regime_lines}\n\n"
            f"By engine:\n{engine_lines}\n\n"
            f"Learned confluence quality:\n{conf_q_line}\n\n"
            f"Assets tracked: {len(asset_q)}\n\n"
            f"Circuit breaker: {'TRIPPED — ' + str(store.data['tier1']['circuit_breaker'].get('reason')) if store.is_circuit_breaker_tripped() else 'nominal'}"
        )
        self.send(text)

# ==============================================================================
# SECTION Q — MAIN ORCHESTRATION
# ==============================================================================

def _current_active_sector_counts(store: StateStore) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for rec in store.data["active_signals"]:
        sector = SECTOR_MAP.get(rec["symbol"], rec["symbol"])
        counts[sector] = counts.get(sector, 0) + 1
    return counts


def _active_symbols(store: StateStore) -> set:
    return {rec["symbol"] for rec in store.data["active_signals"]}


def run_scan(store: StateStore, client: HyperliquidClient, decision: DecisionEngine,
             lifecycle: TradeLifecycleManager):
    log.info("=== %s %s — scan start ===", ENGINE_NAME, ENGINE_VERSION)

    # 1. Resolve/monitor everything already in flight first, so this run's
    #    learning update reflects trades that just closed.
    lifecycle.monitor_all()

    # 2. Generate + rank fresh candidates across the full watchlist.
    active_syms = _active_symbols(store)
    sector_counts = _current_active_sector_counts(store)
    all_ranked: List[RankedSignal] = []
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    for symbol in WATCHLIST:
        if symbol in active_syms:
            continue  # avoid duplicate concurrent exposure on the same asset
        # v1.2.1: also skip symbols whose trade resolved recently (cooldown
        # set in TradeLifecycleManager._apply_cooldown). This is what
        # actually prevents a stop-out from being immediately followed by a
        # fresh signal on the same symbol -- active_syms alone isn't enough,
        # since monitor_all() above already freed this symbol from
        # active_signals before we got here.
        if store.cooldown_until(symbol) > now_ms:
            continue
        try:
            ctx = build_context(client, symbol)
            if ctx is None:
                continue
            for engine in ALL_ENGINES:
                try:
                    candidates = engine.generate(ctx)
                except Exception:
                    log.error("Engine %s failed on %s:\n%s", engine.setup_type, symbol, traceback.format_exc())
                    continue
                for sig in candidates:
                    ranked = decision.score_signal(sig, ctx)
                    if ranked:
                        all_ranked.append(ranked)
        except Exception:
            log.error("Context build failed for %s:\n%s", symbol, traceback.format_exc())
            continue

    selected = decision.rank_and_select(all_ranked, sector_counts)
    if selected:
        log.info("Selected %d new signal(s) this scan: %s",
                  len(selected), [f"{r.signal.symbol}:{r.tier}" for r in selected])
        lifecycle.register(selected)
    else:
        log.info("No qualifying candidates this scan — producing nothing is correct.")

    # 3. Learning cycle + persistence.
    telegram = lifecycle.telegram
    run_learning_cycle(store, telegram)
    store.prune_tier2()

    now = datetime.now(timezone.utc)
    last_summary = store.data["tier1"].get("last_daily_summary_date")
    today_str = now.strftime("%Y-%m-%d")
    if now.hour == 8 and last_summary != today_str:
        telegram.send_daily_summary(store)
        store.data["tier1"]["last_daily_summary_date"] = today_str

    store.data["last_run_ts"] = int(now.timestamp() * 1000)
    store.save()
    log.info("=== scan complete: %d active signal(s) in flight ===", len(store.data["active_signals"]))


def main():
    store = StateStore()
    client = HyperliquidClient()
    telegram = TelegramNotifier()
    decision = DecisionEngine(store)
    lifecycle = TradeLifecycleManager(client, store, telegram)
    try:
        run_scan(store, client, decision, lifecycle)
    except Exception:
        log.error("Unhandled error during scan:\n%s", traceback.format_exc())
        store.save()
        raise


if __name__ == "__main__":
    main()
