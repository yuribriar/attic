#!/usr/bin/env python3
"""
MERIDIAN Signal Engine v1.0.0
=============================
Adaptive Institutional-Grade Signal Engine for Hyperliquid perpetual futures.
Single-file, scan-per-run architecture (~15 minute cadence via external scheduler).

Pipeline per scan:
  1. Top-down gate (Weekly+Daily -> 4H -> 1H -> 15M) then zone/setup discovery
  2. Regime eligibility & routing (composite Regime Vector)
  3. Fill-verification wrapping (entry_kind lifecycle)
  4. Risk-plan construction (adaptive-percentile SL, liquidity-wall-clipped TP)
  5. Composite scoring (continuous logistic blend, not discrete point stack)
  6. Trade management (no auto-breakeven; TP1-then-SL-still-a-win bookkeeping)
  7. Forensic feedback (closed-taxonomy diagnosis -> deterministic param route)
"""

from __future__ import annotations

import collections
import fcntl
import json
import logging
import math
import os
import statistics
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# ============================================================================
# SECTION 0 — CONFIGURATION
# ============================================================================

ENGINE_NAME = "MERIDIAN"
ENGINE_VERSION = "1.0.0"
RESOLUTION_LOGIC_VERSION = "1.0.0"

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")
TELEGRAM_ENABLED = bool(TG_BOT_TOKEN and TG_CHAT_ID)

STATE_PATH = os.environ.get("MERIDIAN_STATE_PATH", "state.json")
CANDLE_CACHE_PATH = os.environ.get("MERIDIAN_CANDLE_CACHE_PATH", "candle_cache.json")
LOCK_PATH = os.environ.get("MERIDIAN_LOCK_PATH", "meridian_engine.lock")

HL_API_URL = os.environ.get("HL_API_URL", "https://api.hyperliquid.xyz/info")
HL_MAX_WEIGHT_PER_MIN = 1150
HL_REQUEST_TIMEOUT_SEC = 15
HL_MAX_RETRIES = 4
HL_BACKOFF_BASE_SEC = 0.75
HL_DEFAULT_INFO_WEIGHT = 20

SCAN_WORKER_THREADS = int(os.environ.get("MERIDIAN_SCAN_WORKERS", "6"))

WATCHLIST: List[str] = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]
MACRO_ASSET = "BTC"

TF_WEEKLY = "1w"
TF_DAILY = "1d"
TF_4H = "4h"
TF_1H = "1h"
TF_15M = "15m"
TF_5M = "5m"
ALL_TFS = (TF_WEEKLY, TF_DAILY, TF_4H, TF_1H, TF_15M, TF_5M)

TF_MS: Dict[str, int] = {
    TF_5M: 5 * 60_000,
    TF_15M: 15 * 60_000,
    TF_1H: 60 * 60_000,
    TF_4H: 4 * 60 * 60_000,
    TF_DAILY: 24 * 60 * 60_000,
    TF_WEEKLY: 7 * 24 * 60 * 60_000,
}

CANDLE_COUNT: Dict[str, int] = {
    TF_WEEKLY: 156, TF_DAILY: 260, TF_4H: 260, TF_1H: 300, TF_15M: 300, TF_5M: 300,
}

# On every incremental fetch, re-request this many already-cached trailing
# *closed* candles (in addition to anything newer) instead of starting exactly
# at the last cached timestamp. The merge step overwrites cached rows with
# whatever comes back, so this makes the cache self-healing against an
# exchange-side candle that was still finalizing (missing/short-lived stale
# close) at the moment a previous run fetched it. 5M and 15M are the
# entry-vehicle timeframes (Section 7), so they get the widest overlap;
# everything else gets a minimal 1-bar safety net. (Ported from PRISM.)
CANDLE_REFETCH_OVERLAP_BARS: Dict[str, int] = {
    TF_5M: 3, TF_15M: 3, TF_1H: 1, TF_4H: 1, TF_DAILY: 1, TF_WEEKLY: 1,
}

# If a timeframe's cache hasn't been updated in longer than this many
# seconds * 3, treat it as stale beyond the point where a normal incremental
# fetch is trustworthy (e.g. after a long GitHub Actions outage or several
# missed runs) and force a full re-fetch of the whole lookback window instead
# of an incremental one. (Ported from PRISM.)
CANDLE_STALE_AFTER_SEC: Dict[str, int] = {
    TF_5M: 20 * 60, TF_15M: 45 * 60, TF_1H: 3 * 3600, TF_4H: 6 * 3600,
    TF_DAILY: 3 * 86400, TF_WEEKLY: 3 * 7 * 86400,
}

MONITOR_TF = TF_15M

# Stage 4 entry-vehicle search order: try the finer 5M MSS->FVG first, then
# fall back to the original 15M vehicle. SL anchoring, monitoring cadence,
# and PENDING_ENTRY_EXPIRY_BARS all remain 15M-based -- 5M only refines entry.
ENTRY_VEHICLE_TF_ORDER = (TF_5M, TF_15M)

PENDING_ENTRY_EXPIRY_BARS = 12
COUNTERTREND_RETEST_EXPIRY_BARS = 8

ATR_LEN = 14
RSI_LEN = 14
ADX_LEN = 14
EMA_FAST, EMA_SLOW, EMA_TREND = 20, 50, 200
BB_LEN, BB_MULT = 20, 2.0
SWING_LOOKBACK = 3
EQ_CLUSTER_TOLERANCE_ATR = 0.12

RR_MIN_GATE = 1.5
RR_MIN_GATE_COUNTERTREND = 2.0
RR_SOFT_TARGET = 2.0
RR_MAX_GATE = 3.5
MAX_SL_DISTANCE_ATR = 3.0
MIN_ENTRY_SL_DISTANCE_ATR = 0.5
MAX_ENTRY_FROM_MARKET_ATR = 1.2
NOISE_SURVIVAL_FLOOR_ATR = 0.5
MIN_SL_DISTANCE_PCT = 0.006
MAX_SL_DISTANCE_PCT = 0.025
MIN_MOVE_PCT_TP1 = 0.012
MIN_MOVE_PCT_TP2 = 0.020

MAX_CONCURRENT_ACTIVE_SIGNALS = 8
MAX_CORRELATED_CONCURRENT = 2
SAME_SETUP_COOLDOWN_MS = 60 * 60 * 1000

ENABLE_COUNTERTREND_ENGINE = os.environ.get("ENABLE_COUNTERTREND_ENGINE", "false").lower() == "true"

ENGINE_WEIGHT_MIN, ENGINE_WEIGHT_MAX = 0.70, 1.35
ENGINE_WEIGHT_LR = 0.05
CALIBRATION_ADJ_MIN, CALIBRATION_ADJ_MAX = -0.18, 0.18
CALIBRATION_LR = 0.06
FILTER_THRESH_MIN, FILTER_THRESH_MAX = 0.60, 1.40
FILTER_THRESH_LR = 0.05
SL_BUFFER_PCTL_MIN, SL_BUFFER_PCTL_MAX = 55.0, 90.0
SL_BUFFER_PCTL_LR_STEP = 2.0
TP1_RANK_PREF_MIN, TP1_RANK_PREF_MAX = 2, 6

MIN_SAMPLE_SIZE = 20
CIRCUIT_BREAKER_WINDOW = 30
CIRCUIT_BREAKER_WR_DROP = 0.15
CIRCUIT_BREAKER_PF_DROP = 0.35

TIER2_RETENTION_DAYS = 15
TIER2_MAX_TRADES = 1500

BASELINE_WIN_RATE = 0.42
BASELINE_PROFIT_FACTOR = 1.35
BASELINE_AVG_RR = 1.7

MACRO_BLACKOUT_MINUTES_BEFORE = 30
MACRO_BLACKOUT_MINUTES_AFTER = 30

EXIT_MODEL = "full_exit_at_tp1"

# Only these emoji are permitted as Telegram message reactions in this chat
# (per the chat's active-reactions picker) -- setMessageReaction will be
# rejected by Telegram for anything outside this set.
ALLOWED_REACTION_EMOJIS = {
    "\u2764\ufe0f", "\U0001F44D", "\U0001F44E", "\U0001F525", "\U0001F970",
    "\U0001F44F", "\U0001F601", "\U0001F914", "\U0001F92F", "\U0001F631",
    "\U0001F92C", "\U0001F612", "\U0001F389", "\U0001F929", "\U0001F92E",
    "\U0001F4A9", "\U0001F64F", "\U0001F44A", "\U0001F54A\ufe0f", "\U0001F921",
    "\U0001F92D", "\U0001F60F", "\U0001F60D", "\U0001F433", "\u2764\ufe0f\u200d\U0001F525",
    "\U0001F311", "\U0001F32D", "\U0001F4AF", "\U0001F602", "\u26A1",
    "\U0001F34C", "\U0001F3C6", "\U0001F494", "\U0001F928", "\U0001F611",
    "\U0001F353", "\U0001F37E", "\U0001F48B", "\U0001F595", "\U0001F608",
    "\U0001F634", "\U0001F62D", "\U0001F913", "\U0001F47B", "\U0001F468\u200D\U0001F4BB",
    "\U0001F440", "\U0001F383", "\U0001F648", "\U0001F607", "\U0001F628",
    "\U0001F91D", "\u270D\ufe0f", "\U0001F917", "\U0001F385", "\U0001F384",
    "\u26C4", "\U0001F485", "\U0001F92A", "\U0001F5FF", "\U0001F60E",
    "\U0001F498", "\U0001F649", "\U0001F984", "\U0001F618", "\U0001F48A",
    "\U0001F64A", "\U0001F576\ufe0f", "\U0001F937", "\U0001F621",
}

# Event -> reaction emoji, restricted to ALLOWED_REACTION_EMOJIS above.
REACTION_EMOJI = "\U0001F4AF"  # 💯 -- default, used on new-signal dispatch
REACTION_EMOJI_MAP: Dict[str, str] = {
    "dispatch": "\U0001F4AF",   # 💯
    "win": "\U0001F3C6",        # 🏆
    "loss": "\U0001F494",       # 💔
    "expired": "\U0001F937",    # 🤷
}

CORRELATION_CLUSTERS: Dict[str, set] = {
    "majors": {"BTC", "ETH"},
    "l1_alt": {"SOL", "AVAX", "NEAR", "SUI", "APT", "TAO", "DOT", "TRX", "ADA", "BNB"},
    "defi": {"UNI", "AAVE", "LINK", "ONDO", "PENDLE"},
    "meme_beta": {"DOGE", "PENGU"},
    "payments": {"XRP", "XLM", "LTC", "BCH"},
    "hype_zec": {"HYPE", "ZEC"},
}


SCORE_TERM_WEIGHTS_DEFAULT: Dict[str, float] = {
    "regime_fit": 0.22,
    "mtf_alignment": 0.14,
    "confluence_strength": 0.16,
    "segment_performance": 0.20,
    "rr_context": 0.10,
    "liquidity_volatility_context": 0.10,
    "engine_weight": 0.08,
}
TERM_CONTRIBUTION_CAP = 0.30

SIGNAL_STATUSES = ("Pending", "Activated", "TP1", "SL", "Expired", "Closed", "Cancelled")

FORENSIC_CATEGORIES = [
    "regime_mismatch", "structural_invalidation_too_tight", "chased_swept_liquidity",
    "mtf_conflict_ignored", "sfp_mss_sequence_violated", "correct_read_poor_rr",
    "confidence_miscalibration", "filter_over_permissiveness", "genuine_variance",
]

ENGINE_NAMES = [
    "SMC", "Trend Continuation", "Breakout", "Pullback", "Liquidity Sweep",
    "Order Block", "Breaker Block", "Fair Value Gap", "Momentum", "Reversal",
    "Mean Reversion", "Range Trading", "Volatility Expansion", "Counter-Trend Reversal",
]

SESSION_HISTORICAL_WEIGHT = {"asia": 0.55, "london": 0.85, "ny": 0.90, "off_hours": 0.40}
SESSION_OPEN_HOURS_UTC = {"london": 8, "ny": 13}


def correlation_cluster(symbol: str) -> str:
    for cluster, members in CORRELATION_CLUSTERS.items():
        if symbol in members:
            return cluster
    return f"solo:{symbol}"


# ============================================================================
# SECTION 0A — LOGGING
# ============================================================================

LOG_LEVEL = os.environ.get("MERIDIAN_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s UTC | %(levelname)-7s | %(name)s | %(message)s",
)
logging.Formatter.converter = time.gmtime
log = logging.getLogger("MERIDIAN")

if not TELEGRAM_ENABLED:
    log.warning("TG_BOT_TOKEN and/or TG_CHAT_ID missing -- signal-generation-only mode.")


def utcnow_ms() -> int:
    return int(time.time() * 1000)


def human_label(raw: str) -> str:
    if not raw:
        return ""
    words = str(raw).replace("-", "_").split("_")
    out = []
    for w in words:
        for token in w.split(" "):
            if not token:
                continue
            has_internal_case = any(ch.isupper() for ch in token[1:])
            out.append(token if has_internal_case else token.capitalize())
    return " ".join(out)


# ============================================================================
# SECTION 1 — STATE PERSISTENCE (Tier 1 permanent / Tier 2 bounded)
# ============================================================================

def _default_state() -> dict:
    return {
        "schema_version": 1,
        "resolution_logic_version": RESOLUTION_LOGIC_VERSION,
        "tier1": {
            "engine_weights": {},
            "confidence_calibration": {},
            "filter_thresholds": {},
            "sl_buffer_percentile": {},
            "sl_buffer_percentile_dist": {},
            "tp1_rank_preference": {},
            "regime_fit_discount": {},
            "segment_stats": {},
            "calibration_buckets": {},
            "forensic_counts": {},
            "fill_stats": {},
            "filter_funnel": {},
            "session_anchored_stats": {"n": 0, "wins": 0, "sum_r": 0.0},
            "session_non_anchored_stats": {"n": 0, "wins": 0, "sum_r": 0.0},
            "circuit_breaker": {"tripped": False, "tripped_ts": None, "reason": None,
                                "baseline_wr": None, "baseline_pf": None},
            "daily_totals": {},
            "symbol_cooldown": {},
            "totals": {"signals": 0, "wins": 0, "losses": 0, "expired": 0,
                       "sum_r": 0.0, "gross_profit_r": 0.0, "gross_loss_r": 0.0,
                       "sum_hold_minutes": 0.0},
        },
        "tier2_trades": [],
        "active_signals": [],
        "macro_events": [],
        "last_run_ts": None,
    }


def _atomic_write_json(path: str, payload: Any) -> bool:
    tmp_path = f"{path}.tmp-{os.getpid()}-{int(time.time() * 1000)}"
    try:
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, default=str)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
        return True
    except Exception:
        log.exception("Failed to atomically write %s", path)
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass
        return False


def _safe_load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        log.warning("Failed to parse %s -- falling back to default.", path)
        return default


def load_state() -> dict:
    loaded = _safe_load_json(STATE_PATH, None)
    if not isinstance(loaded, dict):
        if loaded is not None:
            log.warning("state.json unreadable -- initializing fresh state.")
        return _default_state()
    base = _default_state()
    merged_t1 = base["tier1"]
    merged_t1.update(loaded.get("tier1", {}))
    base["tier1"] = merged_t1
    for k in loaded:
        if k != "tier1" and k in base:
            base[k] = loaded[k]
    _migrate_active_signals(base)
    return base


# Fields added to the signal-record schema after v1.0.0's initial release.
# Signals persisted by older engine versions won't have these keys; every
# consumer that does a hard sig["key"] lookup (rather than sig.get(...)) will
# KeyError on such legacy records unless we backfill them here at load time.
SIGNAL_RECORD_SCHEMA_DEFAULTS: Dict[str, Any] = {
    "counter_trend": False,
    "sl_anchor_tf": TF_15M,
    "entry_tf": TF_15M,
    "entry_filled": False,
    "pending_bars": 0,
    "status": "pending",
    "filled_ts": None,
    "mae_r": 0.0,
    "mfe_r": 0.0,
    "resolution_logic_version": RESOLUTION_LOGIC_VERSION,
    "tg_message_id": None,
    "session_anchored": False,
    "regime_at_entry": {},
    "_last_checked_t": -1,
}


def _migrate_active_signals(state: dict) -> None:
    active = state.get("active_signals")
    if not isinstance(active, list):
        return
    for sig in active:
        if not isinstance(sig, dict):
            continue
        missing = [k for k in SIGNAL_RECORD_SCHEMA_DEFAULTS if k not in sig]
        if missing:
            log.warning("Backfilling legacy signal %s with default(s) for missing field(s): %s",
                        sig.get("id", "?"), ", ".join(missing))
            for k in missing:
                sig[k] = SIGNAL_RECORD_SCHEMA_DEFAULTS[k]


def save_state(state: dict) -> bool:
    state["last_run_ts"] = utcnow_ms()
    return _atomic_write_json(STATE_PATH, state)


def prune_tier2(state: dict) -> None:
    cutoff_ms = utcnow_ms() - TIER2_RETENTION_DAYS * 86_400_000
    trades = [t for t in state["tier2_trades"] if t.get("resolved_ts", 0) >= cutoff_ms]
    if len(trades) > TIER2_MAX_TRADES:
        trades = trades[-TIER2_MAX_TRADES:]
    state["tier2_trades"] = trades


def load_candle_cache() -> dict:
    data = _safe_load_json(CANDLE_CACHE_PATH, {})
    return data if isinstance(data, dict) else {}


def save_candle_cache(cache: dict) -> bool:
    return _atomic_write_json(CANDLE_CACHE_PATH, cache)


# ============================================================================
# SECTION 2 — HYPERLIQUID CLIENT
# ============================================================================

class _WeightRateLimiter:
    def __init__(self, budget_per_min: float):
        self.budget = budget_per_min
        self.window_s = 60.0
        self._lock = threading.Lock()
        self._events: collections.deque = collections.deque()

    def acquire(self, weight: float) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                cutoff = now - self.window_s
                while self._events and self._events[0][0] < cutoff:
                    self._events.popleft()
                used = sum(w for _, w in self._events)
                if used + weight <= self.budget or (not self._events and weight > self.budget):
                    self._events.append((now, weight))
                    return
                sleep_for = max(0.05, self._events[0][0] + self.window_s - now)
            time.sleep(min(sleep_for, 2.0))


_rate_limiter = _WeightRateLimiter(HL_MAX_WEIGHT_PER_MIN)


def _candle_request_weight(interval: str, start_ms: int, end_ms: int) -> int:
    step = TF_MS.get(interval)
    if not step or end_ms <= start_ms:
        return HL_DEFAULT_INFO_WEIGHT
    n_bars = max(1, math.ceil((end_ms - start_ms) / step))
    return HL_DEFAULT_INFO_WEIGHT * math.ceil(n_bars / 60)


def hl_post(payload: dict, retries: int = HL_MAX_RETRIES, timeout: int = HL_REQUEST_TIMEOUT_SEC) -> Optional[Any]:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        HL_API_URL, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    req_kind = payload.get("type", "?")
    req_args = payload.get("req")
    coin = req_args.get("coin") if isinstance(req_args, dict) else None
    req_label = f"{req_kind}/{coin}" if coin else req_kind
    weight = _candle_request_weight(
        req_args.get("interval", ""), req_args.get("startTime", 0), req_args.get("endTime", 0)
    ) if req_kind == "candleSnapshot" else HL_DEFAULT_INFO_WEIGHT

    import random as _rng
    last_err: Optional[Exception] = None
    for attempt in range(retries):
        _rate_limiter.acquire(weight)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code == 429:
                retry_after = e.headers.get("Retry-After") if e.headers else None
                try:
                    sleep_s = float(retry_after) if retry_after is not None else (
                        HL_BACKOFF_BASE_SEC * (2 ** attempt) + _rng.uniform(0, 0.3))
                except (TypeError, ValueError):
                    sleep_s = HL_BACKOFF_BASE_SEC * (2 ** attempt) + _rng.uniform(0, 0.3)
                log.warning("Rate-limited (429) on %s -- attempt %d/%d, sleeping %.1fs.",
                            req_label, attempt + 1, retries, sleep_s)
                time.sleep(sleep_s)
                continue
            if 500 <= e.code < 600:
                sleep_s = HL_BACKOFF_BASE_SEC * (2 ** attempt) + _rng.uniform(0, 0.3)
                log.warning("Server error (%d) on %s -- attempt %d/%d, sleeping %.1fs.",
                            e.code, req_label, attempt + 1, retries, sleep_s)
                time.sleep(sleep_s)
                continue
            break
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            last_err = e
            sleep_s = HL_BACKOFF_BASE_SEC * (2 ** attempt) + _rng.uniform(0, 0.3)
            log.warning("Network error on %s (%s) -- attempt %d/%d, sleeping %.1fs.",
                        req_label, e, attempt + 1, retries, sleep_s)
            time.sleep(sleep_s)
            continue
    log.error("Hyperliquid request failed after retries: %s (type=%s)", last_err, req_kind)
    return None


def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    step = TF_MS[interval]
    return (reference_ms // step) * step


def filter_closed_candles(candles: list, interval: str, reference_ms: int) -> list:
    cutoff = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if c["t"] < cutoff]


def _request_candles(symbol: str, interval: str, start_ms: int, end_ms: int) -> list:
    payload = {
        "type": "candleSnapshot",
        "req": {"coin": symbol, "interval": interval, "startTime": start_ms, "endTime": end_ms},
    }
    raw = hl_post(payload)
    if not raw:
        return []
    out = []
    for c in raw:
        try:
            out.append({"t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]),
                        "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"])})
        except (KeyError, ValueError, TypeError):
            continue
    out.sort(key=lambda r: r["t"])
    return out


def _full_lookback_fetch(symbol: str, interval: str, n: int, reference_ms: int) -> list:
    lookback_ms = n * TF_MS[interval] * 2 + TF_MS[interval] * 5
    raw = _request_candles(symbol, interval, reference_ms - lookback_ms, reference_ms)
    return filter_closed_candles(raw, interval, reference_ms)[-n:]


def get_candles(symbol: str, interval: str, n: int, reference_ms: Optional[int] = None,
                cache_entry: Optional[list] = None) -> list:
    reference_ms = reference_ms or utcnow_ms()
    if cache_entry:
        step = TF_MS[interval]
        last_cached_t = cache_entry[-1]["t"]

        stale_threshold = CANDLE_STALE_AFTER_SEC.get(interval)
        if stale_threshold is not None:
            age_sec = (reference_ms - last_cached_t) / 1000.0
            if age_sec > stale_threshold * 3:
                log.warning("Cache for %s/%s stale beyond threshold (%.0fs old) -- full re-fetch.",
                            symbol, interval, age_sec)
                return _full_lookback_fetch(symbol, interval, n, reference_ms)

        if current_bar_open_ms(reference_ms, interval) <= last_cached_t + step:
            return filter_closed_candles(cache_entry, interval, reference_ms)[-n:]
        overlap_bars = CANDLE_REFETCH_OVERLAP_BARS.get(interval, 1)
        start_ms = last_cached_t - step * overlap_bars
        new_raw = _request_candles(symbol, interval, start_ms, reference_ms)
        if new_raw:
            merged = {c["t"]: c for c in cache_entry}
            for c in new_raw:
                merged[c["t"]] = c
            candles = [merged[t] for t in sorted(merged.keys())]
        else:
            candles = cache_entry
        return filter_closed_candles(candles, interval, reference_ms)[-n:]
    return _full_lookback_fetch(symbol, interval, n, reference_ms)


def fetch_all_candles(symbol: str, candle_cache: dict, reference_ms: Optional[int] = None) -> Optional[dict]:
    bundle = {}
    sym_cache = candle_cache.get(symbol, {})
    for tf in ALL_TFS:
        cache_entry = sym_cache.get(tf)
        candles = get_candles(symbol, tf, CANDLE_COUNT[tf], reference_ms, cache_entry)
        min_required = 60 if tf != TF_WEEKLY else 30
        if len(candles) < min_required:
            log.error("Insufficient %s candles for %s (%d) -- skipping.", tf, symbol, len(candles))
            return None
        bundle[tf] = candles
        candle_cache.setdefault(symbol, {})[tf] = candles[-CANDLE_COUNT[tf]:]
    return bundle


def get_market_snapshot() -> dict:
    raw = hl_post({"type": "metaAndAssetCtxs"})
    if not raw or len(raw) < 2:
        return {}
    universe = raw[0].get("universe", [])
    ctxs = raw[1]
    out = {}
    wanted = set(WATCHLIST) | {MACRO_ASSET}
    for i, meta in enumerate(universe):
        name = meta.get("name", "")
        if name not in wanted:
            continue
        try:
            ctx = ctxs[i]
            mark = float(ctx.get("markPx", 0) or 0)
            funding = float(ctx.get("funding", 0) or 0)
            oi_coins = float(ctx.get("openInterest", 0) or 0)
            out[name] = {"mark": mark, "funding": funding, "oi_usd": oi_coins * mark}
        except (KeyError, ValueError, TypeError, IndexError):
            continue
    return out


# ============================================================================
# SECTION 3 — INDICATORS
# ============================================================================

def safe(v, fb=0.0):
    try:
        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
            return fb
        return v
    except TypeError:
        return fb


def ema(vals: list, period: int) -> list:
    if not vals:
        return []
    k = 2 / (period + 1)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def rsi(closes: list, period: int = RSI_LEN) -> list:
    if len(closes) < period + 1:
        return [50.0] * len(closes)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    out = [50.0] * len(closes)
    avg_g = sum(gains[1:period + 1]) / period
    avg_l = sum(losses[1:period + 1]) / period
    for i in range(period + 1, len(closes)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs = avg_g / avg_l if avg_l > 1e-12 else 100.0
        out[i] = 100.0 - (100.0 / (1.0 + rs))
    return out


def true_ranges(candles: list) -> list:
    trs = [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return trs


def atr_series(candles: list, period: int = ATR_LEN) -> list:
    trs = true_ranges(candles)
    if len(trs) < period:
        return [statistics.fmean(trs)] * len(trs) if trs else []
    out = [None] * (period - 1)
    first = sum(trs[:period]) / period
    out.append(first)
    for i in range(period, len(trs)):
        out.append((out[-1] * (period - 1) + trs[i]) / period)
    out[0:period - 1] = [first] * (period - 1)
    return out


def adx_series(candles: list, period: int = ADX_LEN) -> list:
    n = len(candles)
    if n < period + 2:
        return [15.0] * n
    plus_dm, minus_dm = [0.0], [0.0]
    for i in range(1, n):
        up = candles[i]["h"] - candles[i - 1]["h"]
        dn = candles[i - 1]["l"] - candles[i]["l"]
        plus_dm.append(up if (up > dn and up > 0) else 0.0)
        minus_dm.append(dn if (dn > up and dn > 0) else 0.0)
    trs = true_ranges(candles)

    def wilder_smooth(vals):
        out = [None] * (period - 1)
        s = sum(vals[:period])
        out.append(s)
        for i in range(period, len(vals)):
            s = s - (s / period) + vals[i]
            out.append(s)
        out[0:period - 1] = [out[period - 1]] * (period - 1)
        return out

    atr_sm = wilder_smooth(trs)
    pdm_sm = wilder_smooth(plus_dm)
    mdm_sm = wilder_smooth(minus_dm)
    dx = []
    for i in range(n):
        a = atr_sm[i] or 1e-9
        pdi = 100 * (pdm_sm[i] / a) if a else 0.0
        mdi = 100 * (mdm_sm[i] / a) if a else 0.0
        denom = pdi + mdi
        dx.append(100 * abs(pdi - mdi) / denom if denom > 1e-9 else 0.0)
    out = [None] * (2 * period - 1)
    first = sum(dx[period - 1:2 * period - 1]) / period
    out.append(first)
    for i in range(2 * period, n):
        out.append((out[-1] * (period - 1) + dx[i]) / period)
    fill = out[2 * period] if len(out) > 2 * period else first
    out[0:2 * period] = [fill] * min(2 * period, n)
    return out[:n]


def bollinger(closes: list, period: int = BB_LEN, mult: float = BB_MULT):
    if len(closes) < period:
        m = statistics.fmean(closes) if closes else 0.0
        return m, m, m
    window = closes[-period:]
    mid = statistics.fmean(window)
    sd = statistics.pstdev(window)
    return mid - mult * sd, mid, mid + mult * sd


def percentile(vals: list, pct: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    k = (len(s) - 1) * (pct / 100.0)
    f, c = math.floor(k), math.ceil(k)
    if f == c:
        return s[int(k)]
    return s[f] + (s[c] - s[f]) * (k - f)


def percentile_rank(vals: list, x: float) -> float:
    if not vals:
        return 50.0
    below = sum(1 for v in vals if v <= x)
    return 100.0 * below / len(vals)


# ============================================================================
# SECTION 4 — STRUCTURAL PRIMITIVES (closed-candle-only, shared by all paths)
# ============================================================================

@dataclass
class Pivot:
    idx: int
    t: int
    price: float
    kind: str


@dataclass
class Zone:
    kind: str
    direction: str
    top: float
    bottom: float
    idx: int
    t: int
    mitigated: bool = False
    from_sweep: bool = False


@dataclass
class LiquidityPool:
    kind: str
    price: float
    idx_list: list
    equal: bool
    swept: bool = False
    swept_idx: Optional[int] = None
    pure_sfp: bool = False


def find_pivots(candles: list, lookback: int = SWING_LOOKBACK) -> list:
    pivots = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        window = candles[i - lookback:i + lookback + 1]
        h, l = candles[i]["h"], candles[i]["l"]
        if h == max(c["h"] for c in window):
            pivots.append(Pivot(i, candles[i]["t"], h, "high"))
        if l == min(c["l"] for c in window):
            pivots.append(Pivot(i, candles[i]["t"], l, "low"))
    return pivots


def liquidity_pools(candles: list, pivots: list, atr: float) -> list:
    highs = sorted([p for p in pivots if p.kind == "high"], key=lambda p: p.price)
    lows = sorted([p for p in pivots if p.kind == "low"], key=lambda p: p.price)
    tol = max(atr * EQ_CLUSTER_TOLERANCE_ATR, 1e-9)
    pools = []

    def cluster(sorted_pivots, kind):
        used = set()
        for i, p in enumerate(sorted_pivots):
            if p.idx in used:
                continue
            group = [p]
            for q in sorted_pivots[i + 1:]:
                if q.idx in used:
                    continue
                if abs(q.price - p.price) <= tol:
                    group.append(q)
            for g in group:
                used.add(g.idx)
            level = statistics.fmean(g.price for g in group)
            pools.append(LiquidityPool(kind=kind, price=level,
                                       idx_list=[g.idx for g in group],
                                       equal=len(group) >= 2))

    cluster(highs, "BSL")
    cluster(lows, "SSL")
    return pools


def mark_sweeps(candles: list, pools: list) -> None:
    n = len(candles)
    for pool in pools:
        origin_idx = max(pool.idx_list) if pool.idx_list else 0
        for i in range(origin_idx + 1, n):
            c = candles[i]
            if pool.kind == "BSL" and c["h"] > pool.price:
                pool.swept = True
                pool.swept_idx = i
                pool.pure_sfp = c["c"] < pool.price
                break
            if pool.kind == "SSL" and c["l"] < pool.price:
                pool.swept = True
                pool.swept_idx = i
                pool.pure_sfp = c["c"] > pool.price
                break


def find_fvgs(candles: list, origin_idx_min: int = 0) -> list:
    zones = []
    for i in range(2, len(candles)):
        if i - 2 < origin_idx_min:
            continue
        a, c = candles[i - 2], candles[i]
        if c["l"] > a["h"]:
            zones.append(Zone(kind="fvg", direction="bullish", top=c["l"], bottom=a["h"], idx=i, t=c["t"]))
        if c["h"] < a["l"]:
            zones.append(Zone(kind="fvg", direction="bearish", top=a["l"], bottom=c["h"], idx=i, t=c["t"]))
    return zones


def structure_shift(candles: list, pivots: list, direction: str, kind: str,
                    start_idx: int = 0) -> Optional[Pivot]:
    highs = [p for p in pivots if p.kind == "high" and p.idx >= start_idx]
    lows = [p for p in pivots if p.kind == "low" and p.idx >= start_idx]
    if direction == "bullish" and highs:
        last_high = highs[-1]
        for i in range(last_high.idx + 1, len(candles)):
            if candles[i]["c"] > last_high.price:
                prior_lows = [p for p in lows if p.idx < last_high.idx]
                was_bearish = len(prior_lows) >= 1 and prior_lows[-1].price < last_high.price
                actual_kind = "CHoCH" if was_bearish else "BOS"
                if actual_kind == kind or kind == "any":
                    return last_high
                return None
    if direction == "bearish" and lows:
        last_low = lows[-1]
        for i in range(last_low.idx + 1, len(candles)):
            if candles[i]["c"] < last_low.price:
                prior_highs = [p for p in highs if p.idx < last_low.idx]
                was_bullish = len(prior_highs) >= 1 and prior_highs[-1].price > last_low.price
                actual_kind = "CHoCH" if was_bullish else "BOS"
                if actual_kind == kind or kind == "any":
                    return last_low
                return None
    return None


def find_order_blocks(candles: list, direction: str, since_idx: int = 0) -> list:
    zones = []
    n = len(candles)
    for i in range(max(since_idx, 1), n - 1):
        c, nxt = candles[i], candles[i + 1]
        body = abs(c["c"] - c["o"])
        impulse = abs(nxt["c"] - nxt["o"])
        if impulse < body * 1.3:
            continue
        if direction == "bullish" and c["c"] < c["o"] and nxt["c"] > nxt["o"] and nxt["c"] > c["h"]:
            zones.append(Zone(kind="ob", direction="bullish", top=c["h"], bottom=c["l"], idx=i, t=c["t"]))
        if direction == "bearish" and c["c"] > c["o"] and nxt["c"] < nxt["o"] and nxt["c"] < c["l"]:
            zones.append(Zone(kind="ob", direction="bearish", top=c["h"], bottom=c["l"], idx=i, t=c["t"]))
    return zones


def find_breaker_blocks(candles: list, pivots: list, direction: str, since_idx: int = 0) -> list:
    shift = structure_shift(candles, pivots, direction, "any", since_idx)
    if shift is None:
        return []
    opposite = "bearish" if direction == "bullish" else "bullish"
    obs = find_order_blocks(candles, opposite, max(0, shift.idx - 20))
    obs = [z for z in obs if z.idx <= shift.idx]
    if not obs:
        return []
    latest = obs[-1]
    return [Zone(kind="breaker", direction=direction, top=latest.top, bottom=latest.bottom,
                 idx=latest.idx, t=latest.t)]


def mark_mitigated(zones: list, candles: list) -> None:
    for z in zones:
        for c in candles[z.idx + 1:]:
            if z.direction == "bullish" and c["l"] <= z.top:
                z.mitigated = True
                break
            if z.direction == "bearish" and c["h"] >= z.bottom:
                z.mitigated = True
                break


def tag_sweep_to_poi_causality(zones: list, pools: list, candles: list, lookahead_bars: int = 6) -> None:
    swept = [p for p in pools if p.swept and p.pure_sfp]
    for z in zones:
        for p in swept:
            if p.swept_idx is not None and 0 <= z.idx - p.swept_idx <= lookahead_bars:
                z.from_sweep = True
                break


# ============================================================================
# SECTION 5 — VIEW MODEL
# ============================================================================

@dataclass
class View:
    tf: str
    candles: list
    pivots: list = field(default_factory=list)
    pools: list = field(default_factory=list)
    atr: float = 0.0
    atr_hist: list = field(default_factory=list)
    rsi: float = 50.0
    adx: float = 15.0
    ema_fast: float = 0.0
    ema_slow: float = 0.0
    ema_trend: float = 0.0
    bb: tuple = (0.0, 0.0, 0.0)
    bull_obs: list = field(default_factory=list)
    bear_obs: list = field(default_factory=list)
    bull_breakers: list = field(default_factory=list)
    bear_breakers: list = field(default_factory=list)
    fvgs: list = field(default_factory=list)
    eq_highs: list = field(default_factory=list)
    eq_lows: list = field(default_factory=list)

    @property
    def last(self) -> dict:
        return self.candles[-1]

    @property
    def close(self) -> float:
        return self.candles[-1]["c"]


def build_view(tf: str, candles: list) -> View:
    closes = [c["c"] for c in candles]
    pivots = find_pivots(candles)
    atr_s = atr_series(candles)
    atr_now = safe(atr_s[-1], statistics.fmean(true_ranges(candles)))
    pools = liquidity_pools(candles, pivots, atr_now)
    mark_sweeps(candles, pools)
    since = max(0, len(candles) - 120)
    bull_obs = find_order_blocks(candles, "bullish", since)
    bear_obs = find_order_blocks(candles, "bearish", since)
    bull_breakers = find_breaker_blocks(candles, pivots, "bullish", since)
    bear_breakers = find_breaker_blocks(candles, pivots, "bearish", since)
    fvgs = find_fvgs(candles, since)
    for zone_list in (bull_obs, bear_obs, bull_breakers, bear_breakers, fvgs):
        mark_mitigated(zone_list, candles)
        tag_sweep_to_poi_causality(zone_list, pools, candles)
    eq_highs = [p for p in pools if p.kind == "BSL" and p.equal]
    eq_lows = [p for p in pools if p.kind == "SSL" and p.equal]
    ema_f = ema(closes, EMA_FAST)[-1] if len(closes) >= EMA_FAST else closes[-1]
    ema_s = ema(closes, EMA_SLOW)[-1] if len(closes) >= EMA_SLOW else closes[-1]
    ema_t = ema(closes, EMA_TREND)[-1] if len(closes) >= EMA_TREND else closes[-1]
    return View(
        tf=tf, candles=candles, pivots=pivots, pools=pools, atr=atr_now, atr_hist=atr_s,
        rsi=safe(rsi(closes)[-1], 50.0), adx=safe(adx_series(candles)[-1], 15.0),
        ema_fast=ema_f, ema_slow=ema_s, ema_trend=ema_t, bb=bollinger(closes),
        bull_obs=bull_obs, bear_obs=bear_obs, bull_breakers=bull_breakers,
        bear_breakers=bear_breakers, fvgs=fvgs, eq_highs=eq_highs, eq_lows=eq_lows,
    )


def build_all_views(bundle: dict) -> dict:
    return {tf: build_view(tf, candles) for tf, candles in bundle.items()}


# ============================================================================
# SECTION 6 — COMPOSITE REGIME VECTOR
# ============================================================================

@dataclass
class RegimeVector:
    macro_bias: str
    volatility_pctl: float
    trend_strength: float
    session: str
    session_weight: float
    session_open_proximity: float
    liquidity_draw: str
    noise_index: float
    breadth: float

    def is_trending(self) -> bool:
        return self.trend_strength >= 22.0

    def is_high_vol(self) -> bool:
        return self.volatility_pctl >= 70.0

    def is_low_vol(self) -> bool:
        return self.volatility_pctl <= 30.0

    def label(self) -> str:
        if self.is_high_vol():
            return "high_vol"
        if self.is_low_vol():
            return "low_vol"
        if self.is_trending() and self.noise_index < 0.5:
            return "trending" if self.macro_bias != "neutral" else "expansion"
        if self.trend_strength < 18:
            return "ranging" if self.noise_index < 0.6 else "consolidation"
        return self.macro_bias if self.macro_bias != "neutral" else "neutral"


def active_session(ts_ms: int) -> str:
    hour = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).hour
    if 0 <= hour < 7:
        return "asia"
    if 7 <= hour < 12:
        return "london"
    if 12 <= hour < 21:
        return "ny"
    return "off_hours"


def session_open_proximity_score(ts_ms: int) -> float:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    minutes_now = dt.hour * 60 + dt.minute
    best = 0.0
    for _, anchor_hour in SESSION_OPEN_HOURS_UTC.items():
        anchor_minutes = anchor_hour * 60
        delta = min(abs(minutes_now - anchor_minutes), 1440 - abs(minutes_now - anchor_minutes))
        score = max(0.0, 1.0 - delta / 90.0)
        best = max(best, score)
    return best


def noise_index(view: View) -> float:
    window = view.candles[-30:]
    if len(window) < 10:
        return 0.5
    net = abs(window[-1]["c"] - window[0]["c"])
    total = sum(abs(window[i]["c"] - window[i - 1]["c"]) for i in range(1, len(window)))
    if total < 1e-9:
        return 0.5
    return max(0.0, min(1.0, 1.0 - net / total))


def compute_breadth(views_by_symbol: dict, macro_bias: str) -> float:
    if macro_bias == "neutral" or not views_by_symbol:
        return 0.0
    agree, total = 0, 0
    for sym, v in views_by_symbol.items():
        total += 1
        bullish = v.close > v.ema_slow
        if (bullish and macro_bias == "bullish") or (not bullish and macro_bias == "bearish"):
            agree += 1
    return (agree / total) * 2 - 1 if total else 0.0


def liquidity_draw_state(view_1h: View) -> str:
    unswept_pools = [p for p in view_1h.pools if not p.swept and p.equal]
    unmitigated_zones = [z for z in (view_1h.fvgs + view_1h.bull_obs + view_1h.bear_obs) if not z.mitigated]
    if len(unswept_pools) > len(unmitigated_zones):
        return "ERL"
    if unmitigated_zones:
        return "IRL"
    return "neutral"


def build_regime_vector(views_by_tf: dict, macro_bias: str, views_by_symbol: dict,
                        ts_ms: int) -> RegimeVector:
    v1h = views_by_tf[TF_1H]
    vol_hist = [x for x in v1h.atr_hist if x is not None][-120:]
    vol_pctl = percentile_rank(vol_hist, v1h.atr) if vol_hist else 50.0
    session = active_session(ts_ms)
    return RegimeVector(
        macro_bias=macro_bias,
        volatility_pctl=vol_pctl,
        trend_strength=v1h.adx,
        session=session,
        session_weight=SESSION_HISTORICAL_WEIGHT[session],
        session_open_proximity=session_open_proximity_score(ts_ms),
        liquidity_draw=liquidity_draw_state(v1h),
        noise_index=noise_index(v1h),
        breadth=compute_breadth(views_by_symbol, macro_bias),
    )


# ============================================================================
# SECTION 7 — MANDATORY TOP-DOWN SEQUENCE
# ============================================================================

@dataclass
class StageResult:
    stage: int
    outcome: str
    reason: str = ""


def stage1_bias(views: dict) -> StageResult:
    w, d = views[TF_WEEKLY], views[TF_DAILY]
    w_bull = w.close > w.ema_slow and w.ema_fast > w.ema_slow
    w_bear = w.close < w.ema_slow and w.ema_fast < w.ema_slow
    d_bull = d.close > d.ema_slow and d.ema_fast > d.ema_slow
    d_bear = d.close < d.ema_slow and d.ema_fast < d.ema_slow
    if w_bull and d_bull:
        return StageResult(1, "bullish")
    if w_bear and d_bear:
        return StageResult(1, "bearish")
    return StageResult(1, "neutral", "Weekly/Daily bias disagreement or no clear trend")


def stage2_context(views: dict, bias: str) -> StageResult:
    h4 = views[TF_4H]
    h4_bull = h4.close > h4.ema_slow and h4.ema_fast >= h4.ema_slow * 0.999
    h4_bear = h4.close < h4.ema_slow and h4.ema_fast <= h4.ema_slow * 1.001
    if bias == "bullish" and h4_bull:
        return StageResult(2, "agree")
    if bias == "bearish" and h4_bear:
        return StageResult(2, "agree")
    return StageResult(2, "disagree", "4H context does not confirm Weekly/Daily bias")


def zone_selection_sequence(views: dict, bias: str, state: dict, symbol: str):
    h1 = views[TF_1H]
    direction = bias
    if direction == "bullish":
        candidates = h1.bull_obs + h1.bull_breakers + [z for z in h1.fvgs if z.direction == "bullish"]
    else:
        candidates = h1.bear_obs + h1.bear_breakers + [z for z in h1.fvgs if z.direction == "bearish"]
    candidates = [z for z in candidates if not z.mitigated]
    if not candidates:
        return "NOT READY", None
    swept_candidates = [z for z in candidates if z.from_sweep]
    pool_candidates = swept_candidates if swept_candidates else candidates
    shift = structure_shift(h1.candles, h1.pivots, direction, "any")
    if shift is None:
        return "NOT READY", None
    breakers = [z for z in pool_candidates if z.kind == "breaker"]
    poi = breakers[-1] if breakers else sorted(pool_candidates, key=lambda z: z.idx)[-1]
    price = h1.close
    inside = poi.bottom <= price <= poi.top
    near = min(abs(price - poi.top), abs(price - poi.bottom)) <= h1.atr * 1.5
    if not (inside or near):
        return "NOT READY", None
    return "VALID", poi


def stage3_zone_selection(views: dict, bias: str, state: dict, symbol: str) -> StageResult:
    outcome, poi = zone_selection_sequence(views, bias, state, symbol)
    result = StageResult(3, outcome, "" if outcome == "VALID" else "zone-selection sequence incomplete")
    result.poi = poi  # type: ignore[attr-defined]
    return result


def fibonacci_ote_refine(direction: str, impulse_low: float, impulse_high: float,
                         poi_top: float, poi_bottom: float):
    span = impulse_high - impulse_low
    if span <= 0:
        return None
    if direction == "bullish":
        ote_low = impulse_high - span * 0.79
        ote_high = impulse_high - span * 0.618
    else:
        ote_low = impulse_low + span * 0.618
        ote_high = impulse_low + span * 0.79
    overlap_low = max(ote_low, poi_bottom)
    overlap_high = min(ote_high, poi_top)
    if overlap_low >= overlap_high:
        return None
    return (overlap_low + overlap_high) / 2.0


def _entry_vehicle_attempt(view: View, bias: str, poi: Zone, tf_label: str):
    """Search a single timeframe's view for a valid MSS->FVG entry vehicle inside poi.
    Returns ((entry_zone, entry_price), "") on success, or (None, reason) on failure."""
    shift = structure_shift(view.candles, view.pivots, bias, "any")
    if shift is None:
        return None, f"no confirmed {tf_label} MSS"
    since = shift.idx
    fresh_fvgs = [z for z in find_fvgs(view.candles, since) if z.direction == bias]
    mark_mitigated(fresh_fvgs, view.candles)
    fresh_fvgs = [z for z in fresh_fvgs if not z.mitigated]
    if not fresh_fvgs:
        return None, f"no MSS-originated {tf_label} FVG entry vehicle"
    entry_zone = fresh_fvgs[-1]
    inside_poi = not (entry_zone.top < poi.bottom or entry_zone.bottom > poi.top)
    if not inside_poi:
        return None, f"{tf_label} FVG did not form inside the validated 1H POI"
    impulse_leg = view.candles[max(0, since - 5):since + 1]
    impulse_low = min(c["l"] for c in impulse_leg) if impulse_leg else entry_zone.bottom
    impulse_high = max(c["h"] for c in impulse_leg) if impulse_leg else entry_zone.top
    refined = fibonacci_ote_refine(bias, impulse_low, impulse_high, entry_zone.top, entry_zone.bottom)
    entry_price = refined if refined is not None else (entry_zone.top + entry_zone.bottom) / 2.0
    return (entry_zone, entry_price), ""


_ENTRY_VEHICLE_TF_LABELS = {TF_5M: "5M", TF_15M: "15M"}


def stage4_entry(views: dict, bias: str, poi: Zone) -> StageResult:
    """Tries the finer 5M MSS->FVG entry vehicle first, then falls back to the
    original 15M vehicle. Whichever timeframe succeeds is recorded as
    result.entry_tf so downstream candidate/signal records can surface it."""
    last_reason = "no entry-vehicle timeframe produced a result"
    for tf in ENTRY_VEHICLE_TF_ORDER:
        view = views.get(tf)
        if view is None:
            continue
        found, reason = _entry_vehicle_attempt(view, bias, poi, _ENTRY_VEHICLE_TF_LABELS.get(tf, tf.upper()))
        if found is not None:
            entry_zone, entry_price = found
            result = StageResult(4, "VALID", "")
            result.entry_zone = entry_zone       # type: ignore[attr-defined]
            result.entry_price = entry_price     # type: ignore[attr-defined]
            result.entry_tf = tf                 # type: ignore[attr-defined]
            return result
        last_reason = reason
    return StageResult(4, "NO TRADE", last_reason)


# ============================================================================
# SECTION 8 — RISK MANAGEMENT: ADAPTIVE STRUCTURAL RISK PLAN
# ============================================================================

def _valid_structural_anchor(direction: str, entry: float, pivots: list) -> Optional[float]:
    opp_kind = "low" if direction == "bullish" else "high"
    candidates = [p for p in pivots if p.kind == opp_kind]
    for p in reversed(candidates):
        if direction == "bullish" and p.price < entry:
            return p.price
        if direction == "bearish" and p.price > entry:
            return p.price
    return None


def _adaptive_sl_buffer(symbol: str, tf: str, view: View, state: dict) -> float:
    key = f"{symbol}|{tf}"
    pctl = state["tier1"]["sl_buffer_percentile"].get(key, 70.0)
    pctl = max(SL_BUFFER_PCTL_MIN, min(SL_BUFFER_PCTL_MAX, pctl))
    wicks = []
    for c in view.candles[-60:]:
        body_top, body_bot = max(c["o"], c["c"]), min(c["o"], c["c"])
        wicks.append(c["h"] - body_top)
        wicks.append(body_bot - c["l"])
    wicks = [w for w in wicks if w > 0]
    if not wicks:
        return view.atr * 0.25
    buf = percentile(wicks, pctl)
    return max(buf, view.atr * 0.05)


def _clear_sl_of_liquidity_pool(direction: str, sl: float, view: View) -> float:
    wanted_kind = "SSL" if direction == "bullish" else "BSL"
    window = view.atr * 1.5
    if direction == "bullish":
        nearby = [p for p in view.pools if p.kind == wanted_kind and not p.swept
                  and sl - window <= p.price <= sl]
        if not nearby:
            return sl
        target = min(nearby, key=lambda p: p.price)
        prices = [view.candles[idx]["l"] for idx in target.idx_list] if target.idx_list else [target.price]
        lo = min(prices) if prices else target.price
        margin = max(target.price - lo, view.atr * 0.08)
        return lo - margin
    else:
        nearby = [p for p in view.pools if p.kind == wanted_kind and not p.swept
                  and sl <= p.price <= sl + window]
        if not nearby:
            return sl
        target = max(nearby, key=lambda p: p.price)
        prices = [view.candles[idx]["h"] for idx in target.idx_list] if target.idx_list else [target.price]
        hi = max(prices) if prices else target.price
        margin = max(hi - target.price, view.atr * 0.08)
        return hi + margin


def _opposing_structural_levels(direction: str, view: View) -> list:
    levels = []
    opp_pivot_kind = "high" if direction == "bullish" else "low"
    for p in view.pivots:
        if p.kind == opp_pivot_kind:
            levels.append({"price": p.price, "confluence": 1, "kind": "pivot"})
    opp_zones = (view.bear_obs + view.bear_breakers) if direction == "bullish" else (view.bull_obs + view.bull_breakers)
    for z in opp_zones:
        if not z.mitigated:
            levels.append({"price": z.top if direction == "bullish" else z.bottom, "confluence": 2, "kind": z.kind})
    opp_pool_kind = "BSL" if direction == "bullish" else "SSL"
    for pool in view.pools:
        if pool.kind == opp_pool_kind and not pool.swept:
            levels.append({"price": pool.price, "confluence": 3 if pool.equal else 1, "kind": "liquidity_pool"})
    return levels


def _merge_confluent_levels(candidates: list, tol: float) -> list:
    if not candidates:
        return []
    candidates = sorted(candidates, key=lambda c: c["price"])
    merged = [dict(candidates[0])]
    for c in candidates[1:]:
        if abs(c["price"] - merged[-1]["price"]) <= tol:
            merged[-1]["confluence"] += c["confluence"]
            merged[-1]["price"] = (merged[-1]["price"] + c["price"]) / 2
        else:
            merged.append(dict(c))
    return merged


def _tp_selection_band(candidates: list, state: dict, asset: str) -> list:
    n = int(state["tier1"]["tp1_rank_preference"].get(asset, 3))
    n = max(TP1_RANK_PREF_MIN, min(TP1_RANK_PREF_MAX, n))
    return candidates[:max(n, 2)]


def tp1_runway_ok(direction: str, entry: float, m15_view: View, state: dict, asset: str) -> bool:
    levels = _opposing_structural_levels(direction, m15_view)
    if direction == "bullish":
        levels = [lv for lv in levels if lv["price"] > entry]
    else:
        levels = [lv for lv in levels if lv["price"] < entry]
    if not levels:
        return False
    merged = _merge_confluent_levels(levels, tol=(m15_view.atr or 1e-9) * 0.05)
    merged.sort(key=lambda c: c["price"], reverse=(direction == "bearish"))
    band = _tp_selection_band(merged, state, asset)
    if not band:
        return False
    best_in_band = max(band, key=lambda c: c["confluence"])
    plausible_reward = abs(best_in_band["price"] - entry)
    typical_risk = state["tier1"]["sl_buffer_percentile_dist"].get(
        f"{asset}|15M", (m15_view.atr or 1e-9) * NOISE_SURVIVAL_FLOOR_ATR)
    return (plausible_reward / max(typical_risk, 1e-9)) >= RR_MIN_GATE * 0.8


def build_risk_plan(direction: str, entry: float, view_15m: View, view_1h: View, view_4h: View,
                    state: dict, symbol: str, rr_min_gate: float = RR_MIN_GATE) -> Optional[dict]:
    # Step 1: SL anchor selection — 15M primary, HTF fallback on noise-survival failure
    anchor_view = view_15m
    anchor_tf = TF_15M
    structural_level = _valid_structural_anchor(direction, entry, view_15m.pivots)

    if structural_level is None:
        for tf_view, tf_name in ((view_1h, TF_1H), (view_4h, TF_4H)):
            candidate = _valid_structural_anchor(direction, entry, tf_view.pivots)
            if candidate is not None:
                anchor_view, anchor_tf, structural_level = tf_view, tf_name, candidate
                break
        if structural_level is None:
            return None
        buffer_final = _adaptive_sl_buffer(symbol, anchor_tf, anchor_view, state)
        sl = structural_level - buffer_final if direction == "bullish" else structural_level + buffer_final
    else:
        buffer_15m = _adaptive_sl_buffer(symbol, TF_15M, view_15m, state)
        sl_15m = structural_level - buffer_15m if direction == "bullish" else structural_level + buffer_15m
        risk_15m = abs(entry - sl_15m)
        if risk_15m < view_15m.atr * NOISE_SURVIVAL_FLOOR_ATR:
            for tf_view, tf_name in ((view_1h, TF_1H), (view_4h, TF_4H)):
                candidate = _valid_structural_anchor(direction, entry, tf_view.pivots)
                if candidate is None:
                    continue
                if abs(entry - candidate) < abs(entry - structural_level) * 4:
                    anchor_view, anchor_tf, structural_level = tf_view, tf_name, candidate
                    break
            buffer_final = _adaptive_sl_buffer(symbol, anchor_tf, anchor_view, state)
            sl = structural_level - buffer_final if direction == "bullish" else structural_level + buffer_final
        else:
            sl = sl_15m

    # Step 3: Liquidity-pool clearing (runs AFTER buffer, BEFORE ceiling check — Section 10A.3)
    sl = _clear_sl_of_liquidity_pool(direction, sl, anchor_view)

    # Step 5: Reject-only gates — symmetric SL/TP distance bounds (Section 10A.2)
    risk = abs(entry - sl)
    if risk <= 1e-12:
        return None
    if risk > view_15m.atr * MAX_SL_DISTANCE_ATR:
        return None
    if risk > entry * MAX_SL_DISTANCE_PCT:
        return None
    min_risk = max(view_15m.atr * MIN_ENTRY_SL_DISTANCE_ATR, entry * MIN_SL_DISTANCE_PCT)
    if risk < min_risk:
        return None

    market_price = view_15m.close
    if abs(entry - market_price) > view_15m.atr * MAX_ENTRY_FROM_MARKET_ATR:
        return None

    # TP selection — confluence-ranked, liquidity-wall-clipped
    levels = _opposing_structural_levels(direction, anchor_view)
    if direction == "bullish":
        levels = [lv for lv in levels if lv["price"] > entry]
    else:
        levels = [lv for lv in levels if lv["price"] < entry]
    if not levels:
        return None
    merged = _merge_confluent_levels(levels, tol=(anchor_view.atr or 1e-9) * 0.05)
    merged.sort(key=lambda c: c["price"], reverse=(direction == "bearish"))
    if len(merged) < 2:
        return None

    band = _tp_selection_band(merged, state, symbol)
    tp1_pick = max(band, key=lambda c: c["confluence"])

    # Liquidity-wall clip for TP1
    wall_kind = "BSL" if direction == "bullish" else "SSL"
    for pool in sorted(anchor_view.pools, key=lambda p: p.price if direction == "bullish" else -p.price):
        if pool.kind != wall_kind or pool.swept:
            continue
        between = (entry < pool.price < tp1_pick["price"]) if direction == "bullish" else \
                  (entry > pool.price > tp1_pick["price"])
        if between:
            tp1_pick = {"price": pool.price, "confluence": tp1_pick["confluence"], "kind": "liquidity_wall_clip"}
            break

    remaining = [c for c in merged if c is not tp1_pick and
                 (c["price"] > tp1_pick["price"] if direction == "bullish"
                  else c["price"] < tp1_pick["price"])]
    if not remaining:
        return None
    tp2_pick = remaining[0]

    tp1, tp2 = tp1_pick["price"], tp2_pick["price"]
    tp1_dist = abs(tp1 - entry)
    tp2_dist = abs(tp2 - entry)

    # TP ordering integrity — guaranteed by construction + explicit assertion
    if tp2_dist <= tp1_dist:
        extension = tp1_dist * 0.6
        tp2 = tp1 + extension if direction == "bullish" else tp1 - extension
        tp2_dist = abs(tp2 - entry)
    assert (tp2 > tp1) if direction == "bullish" else (tp2 < tp1), "TP ordering integrity violated"

    if tp1_dist < entry * MIN_MOVE_PCT_TP1:
        return None
    if tp2_dist < entry * MIN_MOVE_PCT_TP2:
        return None

    rr1 = tp1_dist / risk
    rr2 = tp2_dist / risk

    # RR floor and ceiling — reject-only, never clamped (Section 10A.4)
    if rr1 < rr_min_gate:
        return None
    if rr1 > RR_MAX_GATE:
        return None

    state["tier1"]["sl_buffer_percentile_dist"][f"{symbol}|15M"] = risk

    # Final assertion: displayed RR must match price-implied RR (Section 10A.4)
    assert abs(rr1 - (tp1_dist / risk)) < 1e-6, "displayed RR does not match RR implied by entry/sl/tp1"

    return {
        "direction": direction, "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2,
        "rr1": rr1, "rr2": rr2, "sl_anchor_tf": anchor_tf, "risk": risk,
    }


# ============================================================================
# SECTION 9 — CANDIDATE MODEL & ENTRY-FILL VERIFICATION
# ============================================================================

@dataclass
class Candidate:
    engine: str
    direction: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    confidence: float
    rr1: float
    rr2: float
    confluences: list
    regime_fit: list
    style: str
    entry_kind: str
    symbol: str = ""
    counter_trend: bool = False
    sl_anchor_tf: str = TF_15M
    session_anchored: bool = False
    entry_tf: str = TF_15M


def _retracement_entry(direction: str, m15: View, since_idx: int, fallback: float) -> float:
    leg = m15.candles[max(0, since_idx):]
    if len(leg) < 2:
        return fallback
    impulse_low = min(c["l"] for c in leg)
    impulse_high = max(c["h"] for c in leg)
    span = impulse_high - impulse_low
    if span <= 0:
        return fallback
    if direction == "bullish":
        ote_low = impulse_high - span * 0.79
        ote_high = impulse_high - span * 0.618
    else:
        ote_low = impulse_low + span * 0.618
        ote_high = impulse_low + span * 0.79
    return (ote_low + ote_high) / 2.0


def _base_confidence(confluence_count: int, rr1: float, regime: RegimeVector, best_fit: bool) -> float:
    c = 0.42 + 0.06 * min(confluence_count, 5)
    c += 0.05 if best_fit else -0.05
    c += 0.03 if regime.is_trending() else 0.0
    return max(0.05, min(0.95, c))


# ============================================================================
# SECTION 10 — SPECIALIZED ENGINES (13 base + Counter-Trend)
# Every engine uses entry_kind="pending" with retracement/return-to-level entry.
# ============================================================================

def run_smc_engine(bias: str, views: dict, stage3: StageResult, stage4: StageResult,
                   regime: RegimeVector, state: dict, symbol: str) -> Optional[Candidate]:
    if stage3.outcome != "VALID" or stage4.outcome != "VALID":
        return None
    poi = stage3.poi  # type: ignore[attr-defined]
    entry = stage4.entry_price  # type: ignore[attr-defined]
    entry_tf = getattr(stage4, "entry_tf", TF_15M)
    plan = build_risk_plan(bias, entry, views[TF_15M], views[TF_1H], views[TF_4H], state, symbol)
    if plan is None:
        return None
    confluences = ["1H POI", f"{_ENTRY_VEHICLE_TF_LABELS.get(entry_tf, entry_tf.upper())} MSS->FVG"]
    if poi.kind == "breaker":
        confluences.append("Breaker confirmation")
    if poi.from_sweep:
        confluences.append("Sweep-to-POI causality")
    best_fit = regime.is_trending() and not regime.is_low_vol()
    conf = _base_confidence(len(confluences), plan["rr1"], regime, best_fit)
    return Candidate(engine="SMC", direction=bias, entry=plan["entry"], sl=plan["sl"], tp1=plan["tp1"],
                     tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                     confluences=confluences, regime_fit=["trending", "expansion"], style="intraday",
                     entry_kind="pending", symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"],
                     entry_tf=entry_tf)


def run_trend_continuation_engine(bias: str, views: dict, regime: RegimeVector,
                                  state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    if not regime.is_trending():
        return None
    pullback_pool = h1.bull_obs if bias == "bullish" else h1.bear_obs
    pullback_pool = [z for z in pullback_pool if not z.mitigated]
    if not pullback_pool:
        return None
    zone = pullback_pool[-1]
    price = h1.close
    if not (zone.bottom <= price <= zone.top):
        return None
    trend_confirm = (m15.ema_fast > m15.ema_slow) if bias == "bullish" else (m15.ema_fast < m15.ema_slow)
    if not trend_confirm:
        return None
    entry = (zone.top + zone.bottom) / 2.0
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(2, plan["rr1"], regime, best_fit=True)
    return Candidate(engine="Trend Continuation", direction=bias, entry=plan["entry"], sl=plan["sl"],
                     tp1=plan["tp1"], tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                     confluences=["1H pullback OB", "15M EMA trend confirm"],
                     regime_fit=["trending"], style="swing", entry_kind="pending", symbol=symbol,
                     sl_anchor_tf=plan["sl_anchor_tf"])


def run_breakout_engine(bias: str, views: dict, regime: RegimeVector,
                        state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    lookback = h1.candles[-20:]
    range_high = max(c["h"] for c in lookback[:-1])
    range_low = min(c["l"] for c in lookback[:-1])
    last = h1.candles[-1]
    if bias == "bullish" and last["c"] > range_high and last["c"] > last["o"]:
        broke = True
    elif bias == "bearish" and last["c"] < range_low and last["c"] < last["o"]:
        broke = True
    else:
        broke = False
    if not broke:
        return None
    vol_confirm = last["v"] > statistics.fmean(c["v"] for c in lookback[:-1]) * 1.15
    if not vol_confirm:
        return None
    entry = range_high if bias == "bullish" else range_low
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    best_fit = regime.is_high_vol() or regime.noise_index < 0.4
    conf = _base_confidence(2, plan["rr1"], regime, best_fit)
    return Candidate(engine="Breakout", direction=bias, entry=plan["entry"], sl=plan["sl"], tp1=plan["tp1"],
                     tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                     confluences=["1H range breakout", "volume confirmation", "retest entry"],
                     regime_fit=["expansion", "high_volatility"], style="intraday", entry_kind="pending",
                     symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_pullback_engine(bias: str, views: dict, regime: RegimeVector,
                        state: dict, symbol: str) -> Optional[Candidate]:
    m15, h1 = views[TF_15M], views[TF_1H]
    fib_zone_pool = h1.bull_obs if bias == "bullish" else h1.bear_obs
    fib_zone_pool = [z for z in fib_zone_pool if not z.mitigated]
    shift = structure_shift(m15.candles, m15.pivots, bias, "any")
    if not fib_zone_pool or shift is None:
        return None
    zone = fib_zone_pool[-1]
    entry = (zone.top + zone.bottom) / 2.0
    if not (zone.bottom * 0.995 <= h1.close <= zone.top * 1.005):
        return None
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(2, plan["rr1"], regime, best_fit=regime.is_trending())
    return Candidate(engine="Pullback", direction=bias, entry=plan["entry"], sl=plan["sl"], tp1=plan["tp1"],
                     tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                     confluences=["1H OB pullback zone", "15M structure shift"],
                     regime_fit=["trending", "reversal"], style="intraday", entry_kind="pending",
                     symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_liquidity_sweep_engine(bias: str, views: dict, regime: RegimeVector,
                               state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    wanted_kind = "SSL" if bias == "bullish" else "BSL"
    pools = [p for p in h1.pools if p.kind == wanted_kind and p.swept and p.pure_sfp]
    if not pools:
        return None
    pool = sorted(pools, key=lambda p: p.swept_idx or 0)[-1]
    sweep_t = h1.candles[pool.swept_idx]["t"] if pool.swept_idx is not None else m15.candles[0]["t"]
    since_idx = next((i for i, c in enumerate(m15.candles) if c["t"] >= sweep_t), 0)
    entry = _retracement_entry(bias, m15, since_idx, fallback=m15.close)
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(2 + int(pool.equal), plan["rr1"], regime, best_fit=True)
    return Candidate(engine="Liquidity Sweep", direction=bias, entry=plan["entry"], sl=plan["sl"],
                     tp1=plan["tp1"], tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                     confluences=["EQH/EQL sweep"] + (["equal-cluster liquidity"] if pool.equal else [])
                     + ["OTE retracement entry"],
                     regime_fit=["reversal", "high_volatility"], style="intraday", entry_kind="pending",
                     symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_order_block_engine(bias: str, views: dict, regime: RegimeVector,
                           state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    pool = (h1.bull_obs if bias == "bullish" else h1.bear_obs)
    pool = [z for z in pool if not z.mitigated]
    if not pool:
        return None
    zone = pool[-1]
    price = h1.close
    if not (zone.bottom <= price <= zone.top):
        return None
    entry = (zone.top + zone.bottom) / 2.0
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(1 + int(zone.from_sweep), plan["rr1"], regime, best_fit=regime.is_trending())
    return Candidate(engine="Order Block", direction=bias, entry=plan["entry"], sl=plan["sl"], tp1=plan["tp1"],
                     tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                     confluences=["unmitigated 1H order block"] + (["sweep origin"] if zone.from_sweep else []),
                     regime_fit=["trending", "reversal"], style="intraday", entry_kind="pending",
                     symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_breaker_block_engine(bias: str, views: dict, regime: RegimeVector,
                             state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    pool = h1.bull_breakers if bias == "bullish" else h1.bear_breakers
    if not pool:
        return None
    zone = pool[-1]
    price = h1.close
    if not (zone.bottom <= price <= zone.top):
        return None
    entry = (zone.top + zone.bottom) / 2.0
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(2, plan["rr1"], regime, best_fit=True)
    return Candidate(engine="Breaker Block", direction=bias, entry=plan["entry"], sl=plan["sl"],
                     tp1=plan["tp1"], tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                     confluences=["confirmed breaker block retest"],
                     regime_fit=["reversal", "trending"], style="intraday", entry_kind="pending",
                     symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_fvg_engine(bias: str, views: dict, regime: RegimeVector,
                   state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    pool = [z for z in h1.fvgs if z.direction == bias and not z.mitigated]
    if not pool:
        return None
    zone = pool[-1]
    price = h1.close
    if not (zone.bottom <= price <= zone.top):
        return None
    entry = (zone.top + zone.bottom) / 2.0
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(1, plan["rr1"], regime, best_fit=regime.liquidity_draw == "IRL")
    return Candidate(engine="Fair Value Gap", direction=bias, entry=plan["entry"], sl=plan["sl"],
                     tp1=plan["tp1"], tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                     confluences=["unmitigated 1H FVG rebalance"],
                     regime_fit=["ranging", "consolidation"], style="intraday", entry_kind="pending",
                     symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_momentum_engine(bias: str, views: dict, regime: RegimeVector,
                        state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    rsi_ok = (h1.rsi > 55) if bias == "bullish" else (h1.rsi < 45)
    ema_stack = (m15.ema_fast > m15.ema_slow > m15.ema_trend) if bias == "bullish" else \
                (m15.ema_fast < m15.ema_slow < m15.ema_trend)
    if not (rsi_ok and ema_stack):
        return None
    entry = m15.ema_fast
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(2, plan["rr1"], regime, best_fit=regime.is_trending())
    return Candidate(engine="Momentum", direction=bias, entry=plan["entry"], sl=plan["sl"], tp1=plan["tp1"],
                     tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                     confluences=["1H RSI momentum", "15M EMA stack alignment", "EMA pullback entry"],
                     regime_fit=["trending", "expansion"], style="intraday", entry_kind="pending",
                     symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_reversal_engine(bias: str, views: dict, regime: RegimeVector,
                        state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    choch = structure_shift(m15.candles, m15.pivots, bias, "CHoCH")
    if choch is None:
        return None
    wanted_kind = "SSL" if bias == "bullish" else "BSL"
    pools = [p for p in h1.pools if p.kind == wanted_kind and p.swept and p.pure_sfp]
    if not pools:
        return None
    entry = _retracement_entry(bias, m15, choch.idx, fallback=m15.close)
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(2, plan["rr1"], regime, best_fit=True)
    return Candidate(engine="Reversal", direction=bias, entry=plan["entry"], sl=plan["sl"], tp1=plan["tp1"],
                     tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                     confluences=["1H liquidity sweep", "15M CHoCH", "OTE retracement entry"],
                     regime_fit=["reversal"], style="intraday", entry_kind="pending",
                     symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_mean_reversion_engine(bias: str, views: dict, regime: RegimeVector,
                              state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    if regime.is_trending():
        return None
    lower, mid, upper = h1.bb
    price = h1.close
    if bias == "bullish" and not (price <= lower * 1.01):
        return None
    if bias == "bearish" and not (price >= upper * 0.99):
        return None
    entry = lower if bias == "bullish" else upper
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(1, plan["rr1"], regime, best_fit=not regime.is_trending())
    return Candidate(engine="Mean Reversion", direction=bias, entry=plan["entry"], sl=plan["sl"],
                     tp1=plan["tp1"], tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                     confluences=["1H Bollinger extreme", "band-level entry"],
                     regime_fit=["ranging", "low_volatility"], style="intraday", entry_kind="pending",
                     symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_range_trading_engine(bias: str, views: dict, regime: RegimeVector,
                             state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    if regime.trend_strength > 20:
        return None
    window = h1.candles[-40:]
    range_high = max(c["h"] for c in window)
    range_low = min(c["l"] for c in window)
    span = range_high - range_low
    if span <= 0:
        return None
    price = h1.close
    near_low = (price - range_low) / span < 0.15
    near_high = (range_high - price) / span < 0.15
    if bias == "bullish" and not near_low:
        return None
    if bias == "bearish" and not near_high:
        return None
    entry = range_low if bias == "bullish" else range_high
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(1, plan["rr1"], regime, best_fit=not regime.is_trending())
    return Candidate(engine="Range Trading", direction=bias, entry=plan["entry"], sl=plan["sl"],
                     tp1=plan["tp1"], tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                     confluences=["range extreme rejection", "range-boundary entry"],
                     regime_fit=["ranging", "consolidation"], style="intraday", entry_kind="pending",
                     symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_volatility_expansion_engine(bias: str, views: dict, regime: RegimeVector,
                                    state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    lower, mid, upper = h1.bb
    band_width = (upper - lower) / mid if mid else 0.0
    recent_widths = []
    closes = [c["c"] for c in h1.candles[-40:]]
    for i in range(20, len(closes)):
        lo, md, hi = bollinger(closes[:i + 1])
        recent_widths.append((hi - lo) / md if md else 0.0)
    if not recent_widths:
        return None
    was_squeezed = percentile_rank(recent_widths, band_width) < 30
    breaking_out = (h1.close > upper) if bias == "bullish" else (h1.close < lower)
    if not (was_squeezed and breaking_out):
        return None
    entry = upper if bias == "bullish" else lower
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(2, plan["rr1"], regime, best_fit=regime.is_high_vol())
    return Candidate(engine="Volatility Expansion", direction=bias, entry=plan["entry"], sl=plan["sl"],
                     tp1=plan["tp1"], tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                     confluences=["Bollinger squeeze release", "band-retest entry"],
                     regime_fit=["expansion", "high_volatility"], style="intraday", entry_kind="pending",
                     symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


BASE_ENGINE_RUNNERS = {
    "SMC": run_smc_engine,
    "Trend Continuation": run_trend_continuation_engine,
    "Breakout": run_breakout_engine,
    "Pullback": run_pullback_engine,
    "Liquidity Sweep": run_liquidity_sweep_engine,
    "Order Block": run_order_block_engine,
    "Breaker Block": run_breaker_block_engine,
    "Fair Value Gap": run_fvg_engine,
    "Momentum": run_momentum_engine,
    "Reversal": run_reversal_engine,
    "Mean Reversion": run_mean_reversion_engine,
    "Range Trading": run_range_trading_engine,
    "Volatility Expansion": run_volatility_expansion_engine,
}


# ============================================================================
# SECTION 10A — COUNTER-TREND REVERSAL ENGINE (opt-in, Section 4A)
# ============================================================================

def _htf_poi_pool(direction: str, weekly_view: View, daily_view: View) -> Optional[dict]:
    for view in (daily_view, weekly_view):
        pool = (view.bull_obs + view.bull_breakers + [z for z in view.fvgs if z.direction == "bullish"]) \
            if direction == "bullish" else \
            (view.bear_obs + view.bear_breakers + [z for z in view.fvgs if z.direction == "bearish"])
        pool = [z for z in pool if not z.mitigated]
        price = view.close
        for z in pool:
            if z.bottom <= price <= z.top:
                return {"view": view, "zone": z}
        wanted_kind = "SSL" if direction == "bullish" else "BSL"
        for p in view.pools:
            if p.kind == wanted_kind and p.swept and p.pure_sfp:
                return {"view": view, "zone": None, "swept_pool": p}
    return None


def _exhaustion_signature(direction: str, view: View) -> Optional[float]:
    candles = view.candles[-8:]
    if len(candles) < 6:
        return None
    bodies = [abs(c["c"] - c["o"]) for c in candles]
    shrinking = bodies[-1] < statistics.fmean(bodies[:-1]) * 0.8
    last = candles[-1]
    body_top, body_bot = max(last["o"], last["c"]), min(last["o"], last["c"])
    opp_wick = (body_bot - last["l"]) if direction == "bullish" else (last["h"] - body_top)
    elongated = opp_wick > view.atr * 0.6
    highs = [p for p in view.pivots if p.kind == "high"]
    lows = [p for p in view.pivots if p.kind == "low"]
    no_new_extreme = True
    if direction == "bullish" and len(lows) >= 2:
        no_new_extreme = lows[-1].price >= lows[-2].price
    elif direction == "bearish" and len(highs) >= 2:
        no_new_extreme = highs[-1].price <= highs[-2].price
    if not (shrinking and elongated and no_new_extreme):
        return None
    score = 0.5 + 0.2 * int(shrinking) + 0.2 * int(elongated) + 0.1 * int(no_new_extreme)
    if direction == "bullish" and view.rsi < 35:
        score += 0.1
    elif direction == "bearish" and view.rsi > 65:
        score += 0.1
    return min(1.0, score)


def _retest_and_hold(direction: str, choch_level: float, m15: View, state: dict, symbol: str):
    recent = m15.candles[-COUNTERTREND_RETEST_EXPIRY_BARS:]
    for c in recent:
        touched = c["l"] <= choch_level <= c["h"]
        if not touched:
            continue
        held = (c["c"] > choch_level) if direction == "bullish" else (c["c"] < choch_level)
        body_top, body_bot = max(c["o"], c["c"]), min(c["o"], c["c"])
        rejection_wick = (body_bot - c["l"] > (c["h"] - c["l"]) * 0.4) if direction == "bullish" else \
                         (c["h"] - body_top > (c["h"] - c["l"]) * 0.4)
        if held or rejection_wick:
            return {"entry": choch_level}
    return None


def run_countertrend_engine(base_bias: str, views: dict, regime: RegimeVector,
                            state: dict, symbol: str) -> Optional[Candidate]:
    if not ENABLE_COUNTERTREND_ENGINE:
        return None
    if base_bias not in ("bullish", "bearish"):
        return None
    direction = "bearish" if base_bias == "bullish" else "bullish"

    htf = _htf_poi_pool(direction, views[TF_WEEKLY], views[TF_DAILY])
    if htf is None:
        return None
    exhaustion = _exhaustion_signature(direction, views[TF_4H]) or _exhaustion_signature(direction, views[TF_1H])
    if exhaustion is None:
        return None
    choch = structure_shift(views[TF_1H].candles, views[TF_1H].pivots, direction, "CHoCH") or \
        structure_shift(views[TF_15M].candles, views[TF_15M].pivots, direction, "CHoCH")
    if choch is None:
        return None
    retest = _retest_and_hold(direction, choch.price, views[TF_15M], state, symbol)
    if retest is None:
        return None
    plan = build_risk_plan(direction, retest["entry"], views[TF_15M], views[TF_1H], views[TF_4H],
                           state, symbol, rr_min_gate=RR_MIN_GATE_COUNTERTREND)
    if plan is None:
        return None
    conf = 0.4 + 0.25 * exhaustion
    return Candidate(engine="Counter-Trend Reversal", direction=direction, entry=plan["entry"], sl=plan["sl"],
                     tp1=plan["tp1"], tp2=plan["tp2"], confidence=min(0.9, conf), rr1=plan["rr1"],
                     rr2=plan["rr2"], confluences=["HTF POI", "momentum exhaustion", "CHoCH", "retest-and-hold"],
                     regime_fit=["reversal", "high_volatility"], style="intraday", entry_kind="pending",
                     symbol=symbol, counter_trend=True, sl_anchor_tf=plan["sl_anchor_tf"])


def run_specialized_engines(bias: str, views: dict, stage3: StageResult, stage4: StageResult,
                            regime: RegimeVector, state: dict, symbol: str) -> list:
    candidates = []
    if bias in ("bullish", "bearish"):
        cand = run_smc_engine(bias, views, stage3, stage4, regime, state, symbol)
        if cand:
            candidates.append(cand)
        for name, runner in BASE_ENGINE_RUNNERS.items():
            if name == "SMC":
                continue
            try:
                cand = runner(bias, views, regime, state, symbol)
            except (ValueError, ZeroDivisionError, IndexError, KeyError, statistics.StatisticsError) as e:
                log.warning("%s engine error for %s: %s", name, symbol, e)
                cand = None
            if cand:
                candidates.append(cand)
    ct = run_countertrend_engine(bias, views, regime, state, symbol)
    if ct:
        candidates.append(ct)
    return candidates


# ============================================================================
# SECTION 11 — DECISION ENGINE (continuous composite score)
# ============================================================================

def _mtf_alignment_term(candidate: Candidate, views: dict) -> float:
    aligned = 0
    total = 0
    for tf in (TF_4H, TF_1H):
        v = views[tf]
        total += 1
        bullish = v.ema_fast > v.ema_slow
        if (bullish and candidate.direction == "bullish") or (not bullish and candidate.direction == "bearish"):
            aligned += 1
    return aligned / total if total else 0.5


def _confluence_strength_term(candidate: Candidate) -> float:
    return min(1.0, len(candidate.confluences) / 4.0)


def _segment_performance_term(candidate: Candidate, state: dict, regime: RegimeVector) -> float:
    key = f"{candidate.symbol}|{'trend' if regime.is_trending() else 'range'}|{candidate.style}|{candidate.engine}"
    seg = state["tier1"]["segment_stats"].get(key)
    if not seg or seg.get("n", 0) < MIN_SAMPLE_SIZE:
        return 0.5
    return max(0.0, min(1.0, seg.get("wins", 0) / seg.get("n", 1)))


def _rr_context_term(candidate: Candidate) -> float:
    return min(1.0, (candidate.rr1 - RR_MIN_GATE) / (RR_SOFT_TARGET - RR_MIN_GATE + 1e-9)) if candidate.rr1 else 0.0


def _liquidity_vol_context_term(regime: RegimeVector) -> float:
    vol_score = 1.0 - abs(regime.volatility_pctl - 55.0) / 55.0
    clean = 1.0 - regime.noise_index
    return max(0.0, min(1.0, (vol_score + clean) / 2.0))


def regime_fit_score(candidate: Candidate, regime: RegimeVector) -> float:
    regime_tags = []
    if regime.is_trending():
        regime_tags.append("trending")
    else:
        regime_tags.append("ranging")
    if regime.is_high_vol():
        regime_tags.append("high_volatility")
    if regime.is_low_vol():
        regime_tags.append("low_volatility")
    if regime.noise_index < 0.4:
        regime_tags.append("expansion")
    else:
        regime_tags.append("consolidation")
    match = len(set(candidate.regime_fit) & set(regime_tags))
    return min(1.0, 0.35 + 0.25 * match)


def liquidity_sanity_check(candidate: Candidate, view_1h: View) -> bool:
    if candidate.engine in ("Liquidity Sweep", "Reversal", "Counter-Trend Reversal"):
        return True
    danger_kind = "BSL" if candidate.direction == "bullish" else "SSL"
    for pool in view_1h.pools:
        if pool.kind == danger_kind and not pool.swept and pool.equal:
            if abs(candidate.entry - pool.price) < view_1h.atr * 0.5:
                return False
    return True


def macro_blackout_active(symbol: str, state: dict, now_ms: int) -> bool:
    events = state.get("macro_events", [])
    cluster = correlation_cluster(symbol)
    before_ms = MACRO_BLACKOUT_MINUTES_BEFORE * 60_000
    after_ms = MACRO_BLACKOUT_MINUTES_AFTER * 60_000
    for ev in events:
        ts = ev.get("ts")
        symbols = set(ev.get("symbols", []))
        if ts is None:
            continue
        if symbol in symbols or any(correlation_cluster(s) == cluster for s in symbols):
            if ts - before_ms <= now_ms <= ts + after_ms:
                return True
    return False


def composite_score(candidate: Candidate, views: dict, regime: RegimeVector, state: dict) -> dict:
    weights = dict(SCORE_TERM_WEIGHTS_DEFAULT)
    for term in weights:
        adj = state["tier1"]["filter_thresholds"].get(f"score_term::{term}", 1.0)
        weights[term] *= max(FILTER_THRESH_MIN, min(FILTER_THRESH_MAX, adj))

    engine_weight = state["tier1"]["engine_weights"].get(candidate.engine, 1.0)
    engine_weight = max(ENGINE_WEIGHT_MIN, min(ENGINE_WEIGHT_MAX, engine_weight))

    raw_terms = {
        "regime_fit": regime_fit_score(candidate, regime),
        "mtf_alignment": _mtf_alignment_term(candidate, views),
        "confluence_strength": _confluence_strength_term(candidate),
        "segment_performance": _segment_performance_term(candidate, state, regime),
        "rr_context": _rr_context_term(candidate),
        "liquidity_volatility_context": _liquidity_vol_context_term(regime),
        "engine_weight": (engine_weight - ENGINE_WEIGHT_MIN) / (ENGINE_WEIGHT_MAX - ENGINE_WEIGHT_MIN),
    }
    z = 0.0
    contributions = {}
    for term, raw in raw_terms.items():
        contribution = weights[term] * raw
        contribution = max(-TERM_CONTRIBUTION_CAP, min(TERM_CONTRIBUTION_CAP, contribution))
        contributions[term] = contribution
        z += contribution

    calib_key = f"{candidate.engine}|{_confidence_bucket_from_raw(candidate.confidence)}"
    calib_adj = state["tier1"]["confidence_calibration"].get(calib_key, 0.0)
    calib_adj = max(CALIBRATION_ADJ_MIN, min(CALIBRATION_ADJ_MAX, calib_adj))
    z += calib_adj
    z += regime.session_open_proximity * 0.05  # continuous input, never a hard gate

    prob = 1.0 / (1.0 + math.exp(-(z - 0.15) * 4.0))
    return {"score": prob, "z": z, "contributions": contributions, "engine_weight": engine_weight}


def _confidence_bucket_from_raw(raw: float) -> str:
    if raw >= 0.80:
        return "A+"
    if raw >= 0.68:
        return "A"
    if raw >= 0.55:
        return "B"
    return "C"


def _confidence_bucket(score: float) -> str:
    if score >= 0.80:
        return "A+"
    if score >= 0.68:
        return "A"
    if score >= 0.55:
        return "B"
    return "C"


def _log_funnel(state: dict, name: str, seen: int, rejected: int) -> None:
    entry = state["tier1"]["filter_funnel"].setdefault(name, {"seen": 0, "rejected": 0})
    entry["seen"] += seen
    entry["rejected"] += rejected


def _bump_funnel_rejected(state: dict, name: str) -> None:
    state["tier1"]["filter_funnel"].setdefault(name, {"seen": 0, "rejected": 0})["rejected"] += 1


def rank_and_select(candidates: list, views: dict, regime: RegimeVector, state: dict, symbol: str,
                    now_ms: int) -> list:
    if macro_blackout_active(symbol, state, now_ms):
        _log_funnel(state, "macro_blackout", seen=len(candidates), rejected=len(candidates))
        return []
    scored = []
    for c in candidates:
        _log_funnel(state, "liquidity_sanity", seen=1, rejected=0)
        if not liquidity_sanity_check(c, views[TF_1H]):
            _bump_funnel_rejected(state, "liquidity_sanity")
            continue
        dist_tp1 = abs(c.tp1 - c.entry)
        dist_tp2 = abs(c.tp2 - c.entry)
        if dist_tp2 <= dist_tp1:
            continue
        if c.rr1 < (RR_MIN_GATE_COUNTERTREND if c.counter_trend else RR_MIN_GATE):
            continue
        result = composite_score(c, views, regime, state)
        grade = _confidence_bucket(result["score"])
        scored.append((result["score"], grade, c, result))
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored


def correlation_dedup(ranked: list, active_signals: list, state: dict, now_ms: int) -> list:
    cluster_counts = collections.Counter(correlation_cluster(s["symbol"]) for s in active_signals)
    symbols_with_active = {s["symbol"] for s in active_signals}
    symbols_accepted_this_batch = set()
    cooldowns = state["tier1"].get("symbol_cooldown", {})
    accepted = []
    for score, grade, c, result in ranked:
        if len(accepted) + len(active_signals) >= MAX_CONCURRENT_ACTIVE_SIGNALS:
            break
        if c.symbol in symbols_with_active or c.symbol in symbols_accepted_this_batch:
            continue
        cd = cooldowns.get(c.symbol)
        if cd and cd["direction"] == c.direction and now_ms < cd["until_ts"]:
            continue
        cluster = correlation_cluster(c.symbol)
        if cluster_counts[cluster] >= MAX_CORRELATED_CONCURRENT:
            continue
        cluster_counts[cluster] += 1
        symbols_accepted_this_batch.add(c.symbol)
        accepted.append((score, grade, c, result))
    return accepted


# ============================================================================
# SECTION 12 — SIGNAL LIFECYCLE (dispatch, fill verification, resolution)
# Position-exit model: FULL EXIT AT TP1. SL is NEVER repositioned to breakeven.
# TP2 is NEVER checked after dispatch — informational only.
# ============================================================================

def new_signal_record(candidate: Candidate, score: float, grade: str, symbol: str,
                      now_ms: int, regime: RegimeVector) -> dict:
    return {
        "id": f"{symbol}-{candidate.engine}-{now_ms}",
        "symbol": symbol,
        "engine": candidate.engine,
        "counter_trend": candidate.counter_trend,
        "direction": candidate.direction,
        "style": candidate.style,
        "entry_kind": candidate.entry_kind,
        "entry": candidate.entry,
        "sl": candidate.sl,
        "tp1": candidate.tp1,
        "tp2": candidate.tp2,
        "rr1": candidate.rr1,
        "rr2": candidate.rr2,
        "confidence": score,
        "grade": grade,
        "confluences": candidate.confluences,
        "regime_at_entry": {
            "trend_strength": regime.trend_strength,
            "volatility_pctl": regime.volatility_pctl,
            "macro_bias": regime.macro_bias,
            "label": regime.label(),
        },
        "sl_anchor_tf": candidate.sl_anchor_tf,
        "entry_tf": candidate.entry_tf,
        "entry_filled": False,
        "pending_bars": 0,
        "status": "pending",
        "created_ts": now_ms,
        "filled_ts": None,
        "mae_r": 0.0,
        "mfe_r": 0.0,
        "resolution_logic_version": RESOLUTION_LOGIC_VERSION,
        "tg_message_id": None,
        "session_anchored": candidate.session_anchored,
        "_last_checked_t": current_bar_open_ms(now_ms, MONITOR_TF) - 2 * TF_MS[MONITOR_TF],
    }


def monitor_signal(sig: dict, monitor_candles: list) -> Optional[dict]:
    """Advances one signal through fill verification and resolution.
    TP2 is NEVER checked — only SL and TP1 per Section 11's single-TP rule.
    SL is checked first on same-candle ambiguity (conservative worst-case)."""
    direction = sig["direction"]
    risk = abs(sig["entry"] - sig["sl"])
    for c in monitor_candles:
        if c["t"] <= sig.get("_last_checked_t", -1):
            continue
        sig["_last_checked_t"] = c["t"]

        if not sig["entry_filled"]:
            entry_in_range = c["l"] <= sig["entry"] <= c["h"]
            if not entry_in_range:
                sig["pending_bars"] += 1
                expiry = COUNTERTREND_RETEST_EXPIRY_BARS if sig["counter_trend"] else PENDING_ENTRY_EXPIRY_BARS
                if sig["pending_bars"] >= expiry:
                    sig["status"] = "expired"
                    sig["result"] = "expired"
                    sig["resolved_ts"] = c["t"]
                    return {"type": "expired", "sig": sig}
                continue
            sig["entry_filled"] = True
            sig["filled_ts"] = c["t"]
            sig["status"] = "active"

        if risk > 0:
            if direction == "bullish":
                mfe = (c["h"] - sig["entry"]) / risk
                mae = (sig["entry"] - c["l"]) / risk
            else:
                mfe = (sig["entry"] - c["l"]) / risk
                mae = (c["h"] - sig["entry"]) / risk
            sig["mfe_r"] = max(sig["mfe_r"], mfe)
            sig["mae_r"] = max(sig["mae_r"], mae)

        # SL checked first (conservative worst-case ordering) — Section 11
        sl_hit = (c["l"] <= sig["sl"]) if direction == "bullish" else (c["h"] >= sig["sl"])
        tp1_hit = (c["h"] >= sig["tp1"]) if direction == "bullish" else (c["l"] <= sig["tp1"])

        if sl_hit:
            sig["status"] = "resolved"
            sig["result"] = "loss"
            sig["r_realized"] = -1.0
            sig["resolved_ts"] = c["t"]
            return {"type": "loss", "sig": sig}
        if tp1_hit:
            sig["status"] = "resolved"
            sig["result"] = "win"
            sig["r_realized"] = sig["rr1"]
            assert sig["r_realized"] > 0, "win result with non-positive realized R -- resolution bug"
            sig["resolved_ts"] = c["t"]
            return {"type": "win", "sig": sig}
    return None


# ============================================================================
# SECTION 13 — LOSS/WIN FORENSICS & CLOSED-LOOP ADAPTIVE FEEDBACK
# ============================================================================

def _damp(current: float, target: float, lo: float, hi: float, step: float) -> float:
    direction = 1 if target > current else -1
    moved = current + direction * min(abs(target - current), step)
    return max(lo, min(hi, moved))


def classify_forensics(sig: dict, views: dict, regime: RegimeVector, state: dict) -> str:
    """Every category is reached by a POSITIVE, verifiable condition on recorded
    trade data — never by an else/fallback branch (Section 13, rule 1a)."""
    is_loss = sig["result"] == "loss"
    mfe = sig.get("mfe_r", 0.0)
    mae = sig.get("mae_r", 0.0)
    regime_tags = sig.get("regime_at_entry", {})

    # 1. Regime mismatch — positive condition: engine wanted trend but regime was non-trending
    if is_loss and regime_tags.get("trend_strength") is not None:
        was_trending = regime_tags["trend_strength"] >= 22.0
        engine_wants_trend = "trending" in sig.get("_regime_fit_tags", [])
        if engine_wants_trend and not was_trending:
            return "regime_mismatch"

    # 2. Structural invalidation too tight — positive condition: MAE within buffer's normal noise range
    if is_loss and mae <= 1.05:
        return "structural_invalidation_too_tight"

    # 3. Chased swept liquidity — positive condition: entry flagged as liquidity-adjacent
    if is_loss and sig.get("_liquidity_adjacent"):
        return "chased_swept_liquidity"

    # 4. MTF conflict ignored — positive condition: MTF alignment was below threshold at entry
    if is_loss and sig.get("_mtf_conflict_at_entry"):
        return "mtf_conflict_ignored"

    # 5. SFP/MSS sequence violated — positive condition: SFP purity below threshold
    if is_loss and sig.get("_sfp_impure_or_premature"):
        return "sfp_mss_sequence_violated"

    # 6. Correct read, poor RR — positive condition: MFE >= 80% of TP1 distance
    if is_loss and mfe >= 0.80:
        return "correct_read_poor_rr"

    # 7. Confidence miscalibration — positive condition: assigned confidence materially above realized WR
    bucket = _confidence_bucket(sig["confidence"])
    calib_key = f"{sig['engine']}|{bucket}"
    calib = state["tier1"]["calibration_buckets"].get(calib_key, {"n": 0, "wins": 0})
    if is_loss and calib.get("n", 0) >= MIN_SAMPLE_SIZE:
        realized_wr = calib["wins"] / calib["n"]
        implied_wr = {"A+": 0.75, "A": 0.65, "B": 0.55, "C": 0.45}.get(bucket, 0.5)
        if implied_wr - realized_wr > 0.15:
            return "confidence_miscalibration"

    # 8. Filter over-permissiveness — positive condition: thin-margin pass count >= 2
    if is_loss and sig.get("_thin_margin_count", 0) >= 2:
        return "filter_over_permissiveness"

    # 9. Genuine variance — positive condition: all above checks failed AND MFE near zero
    # (a loss with MFE near zero genuinely had no causal pattern — setup was sound but didn't work)
    if is_loss and mfe < 0.80:
        return "genuine_variance"

    # If somehow none matched (e.g. a win), still classify as genuine_variance
    return "genuine_variance"


def adaptive_route(category: str, sig: dict, state: dict) -> None:
    t1 = state["tier1"]
    symbol, engine = sig["symbol"], sig["engine"]
    cat_stats = t1["forensic_counts"].setdefault(category, {"n": 0, "n_since_last_gate": 0})
    cat_stats["n"] = cat_stats.get("n", 0) + 1
    cat_stats["n_since_last_gate"] = cat_stats.get("n_since_last_gate", 0) + 1
    if cat_stats["n_since_last_gate"] < MIN_SAMPLE_SIZE:
        return
    cat_stats["n_since_last_gate"] = 0

    if category == "regime_mismatch":
        key = f"{engine}|regime"
        cur = t1["regime_fit_discount"].get(key, 1.0)
        t1["regime_fit_discount"][key] = _damp(cur, cur * 0.95, 0.65, 1.0, step=0.05)
    elif category == "structural_invalidation_too_tight":
        for tf in (sig.get("sl_anchor_tf", TF_15M),):
            key = f"{symbol}|{tf}"
            cur = t1["sl_buffer_percentile"].get(key, 70.0)
            t1["sl_buffer_percentile"][key] = _damp(cur, cur + SL_BUFFER_PCTL_LR_STEP,
                                                     SL_BUFFER_PCTL_MIN, SL_BUFFER_PCTL_MAX,
                                                     step=SL_BUFFER_PCTL_LR_STEP)
    elif category == "chased_swept_liquidity":
        key = "liquidity_sanity"
        cur = t1["filter_thresholds"].get(key, 1.0)
        t1["filter_thresholds"][key] = _damp(cur, cur * 1.05, FILTER_THRESH_MIN, FILTER_THRESH_MAX,
                                              step=FILTER_THRESH_LR)
    elif category == "mtf_conflict_ignored":
        key = "score_term::mtf_alignment"
        cur = t1["filter_thresholds"].get(key, 1.0)
        t1["filter_thresholds"][key] = _damp(cur, cur * 1.06, FILTER_THRESH_MIN, FILTER_THRESH_MAX,
                                              step=FILTER_THRESH_LR)
    elif category == "sfp_mss_sequence_violated":
        key = f"{engine}|sfp_purity"
        cur = t1["filter_thresholds"].get(key, 1.0)
        t1["filter_thresholds"][key] = _damp(cur, cur * 1.06, FILTER_THRESH_MIN, FILTER_THRESH_MAX,
                                              step=FILTER_THRESH_LR)
    elif category == "correct_read_poor_rr":
        cur = t1["tp1_rank_preference"].get(symbol, 3)
        t1["tp1_rank_preference"][symbol] = int(max(TP1_RANK_PREF_MIN, min(TP1_RANK_PREF_MAX, cur + 1)))
    elif category == "confidence_miscalibration":
        key = f"{engine}|{_confidence_bucket(sig['confidence'])}"
        cur = t1["confidence_calibration"].get(key, 0.0)
        t1["confidence_calibration"][key] = _damp(cur, cur - 0.03, CALIBRATION_ADJ_MIN, CALIBRATION_ADJ_MAX,
                                                    step=CALIBRATION_LR)
    elif category == "filter_over_permissiveness":
        for name in sig.get("_thin_margin_filters", []):
            cur = t1["filter_thresholds"].get(name, 1.0)
            t1["filter_thresholds"][name] = _damp(cur, cur * 1.05, FILTER_THRESH_MIN, FILTER_THRESH_MAX,
                                                  step=FILTER_THRESH_LR)
    # genuine_variance: no parameter change


def reinforce_win(sig: dict, state: dict) -> None:
    t1 = state["tier1"]
    engine = sig["engine"]
    seg_key = f"{sig['symbol']}|{'trend' if sig.get('regime_at_entry', {}).get('trend_strength', 0) and sig['regime_at_entry']['trend_strength'] >= 22 else 'range'}|{sig['style']}|{engine}"
    seg = t1["segment_stats"].get(seg_key, {"n": 0, "wins": 0, "losses": 0, "sum_r": 0.0})
    if seg.get("n", 0) >= MIN_SAMPLE_SIZE:
        expectancy = seg["sum_r"] / seg["n"] if seg["n"] else 0.0
        cur = t1["engine_weights"].get(engine, 1.0)
        target = cur * (1.02 if expectancy > 0 else 0.99)
        t1["engine_weights"][engine] = _damp(cur, target, ENGINE_WEIGHT_MIN, ENGINE_WEIGHT_MAX, step=ENGINE_WEIGHT_LR)


def update_segment_stats(sig: dict, state: dict) -> None:
    trending = sig.get("regime_at_entry", {}).get("trend_strength")
    regime_bucket = "trend" if (trending is not None and trending >= 22) else "range"
    key = f"{sig['symbol']}|{regime_bucket}|{sig['style']}|{sig['engine']}"
    seg = state["tier1"]["segment_stats"].setdefault(key, {"n": 0, "wins": 0, "losses": 0, "sum_r": 0.0})
    seg["n"] += 1
    seg["sum_r"] += sig["r_realized"]
    if sig["result"] == "win":
        seg["wins"] += 1
    else:
        seg["losses"] += 1

    bucket = _confidence_bucket(sig["confidence"])
    calib_key = f"{sig['engine']}|{bucket}"
    calib = state["tier1"]["calibration_buckets"].setdefault(calib_key, {"n": 0, "wins": 0, "sum_conf": 0.0})
    calib["n"] += 1
    calib["sum_conf"] += sig["confidence"]
    if sig["result"] == "win":
        calib["wins"] += 1

    fill_key = f"{sig['engine']}|{sig['entry_kind']}"
    fs = state["tier1"]["fill_stats"].setdefault(fill_key, {"dispatched": 0, "filled": 0, "expired": 0})
    fs["filled"] += 1

    totals = state["tier1"]["totals"]
    totals["signals"] += 1
    r = sig["r_realized"]
    totals["sum_r"] += r
    if sig["result"] == "win":
        totals["wins"] += 1
        totals["gross_profit_r"] += max(r, 0.0)
    else:
        totals["losses"] += 1
        totals["gross_loss_r"] += max(-r, 0.0)
    hold_minutes = (sig.get("resolved_ts", 0) - sig.get("filled_ts", sig.get("created_ts", 0))) / 60000.0
    totals["sum_hold_minutes"] += hold_minutes

    if sig.get("session_anchored"):
        sa = state["tier1"]["session_anchored_stats"]
        sa["n"] += 1
        sa["sum_r"] += r
        if sig["result"] == "win":
            sa["wins"] += 1
    else:
        sna = state["tier1"]["session_non_anchored_stats"]
        sna["n"] += 1
        sna["sum_r"] += r
        if sig["result"] == "win":
            sna["wins"] += 1


def check_circuit_breaker(state: dict) -> None:
    trades = [t for t in state["tier2_trades"]
              if t.get("result") in ("win", "loss")
              and t.get("resolution_logic_version") == RESOLUTION_LOGIC_VERSION]
    cb = state["tier1"]["circuit_breaker"]
    if len(trades) < CIRCUIT_BREAKER_WINDOW:
        return
    window = trades[-CIRCUIT_BREAKER_WINDOW:]
    wins = sum(1 for t in window if t["result"] == "win")
    wr = wins / len(window)
    gross_win = sum(t["r_realized"] for t in window if t["r_realized"] > 0)
    gross_loss = abs(sum(t["r_realized"] for t in window if t["r_realized"] < 0))
    pf = (gross_win / gross_loss) if gross_loss > 1e-9 else (gross_win if gross_win > 0 else 0.0)

    if cb["baseline_wr"] is None:
        cb["baseline_wr"] = wr
        cb["baseline_pf"] = pf
        return

    wr_dropped = (cb["baseline_wr"] - wr) >= CIRCUIT_BREAKER_WR_DROP
    pf_dropped = pf < cb["baseline_pf"] * (1 - CIRCUIT_BREAKER_PF_DROP) if cb["baseline_pf"] else False

    if (wr_dropped or pf_dropped) and not cb["tripped"]:
        cb["tripped"] = True
        cb["tripped_ts"] = utcnow_ms()
        cb["reason"] = f"win rate={wr:.2f} profit factor={pf:.2f} vs baseline wr={cb['baseline_wr']:.2f} pf={cb['baseline_pf']:.2f}"
        log.warning("Circuit breaker TRIPPED: %s", cb["reason"])
        _send_telegram_safe(_format_circuit_breaker(cb, tripped=True))
    elif cb["tripped"] and not wr_dropped and not pf_dropped:
        cb["tripped"] = False
        cb["reason"] = None
        log.info("Circuit breaker cleared -- live performance recovered to baseline")
        _send_telegram_safe(_format_circuit_breaker(cb, tripped=False))


def process_resolution(sig: dict, views: dict, regime: RegimeVector, state: dict) -> None:
    category = classify_forensics(sig, views, regime, state)
    sig["forensic_category"] = category

    frozen = state["tier1"]["circuit_breaker"]["tripped"]
    if not frozen:
        if sig["result"] == "loss":
            adaptive_route(category, sig, state)
        else:
            reinforce_win(sig, state)

    if sig["result"] == "loss":
        state["tier1"]["symbol_cooldown"][sig["symbol"]] = {
            "direction": sig["direction"],
            "until_ts": sig["resolved_ts"] + SAME_SETUP_COOLDOWN_MS,
        }

    update_segment_stats(sig, state)
    state["tier2_trades"].append({
        "id": sig["id"], "symbol": sig["symbol"], "engine": sig["engine"],
        "counter_trend": sig["counter_trend"], "direction": sig["direction"],
        "entry": sig["entry"], "sl": sig["sl"], "tp1": sig["tp1"], "tp2": sig["tp2"],
        "r_realized": sig["r_realized"], "mae_r": sig["mae_r"], "mfe_r": sig["mfe_r"],
        "forensic_category": category, "confidence": sig["confidence"], "grade": sig["grade"],
        "regime_at_entry": sig["regime_at_entry"], "resolved_ts": sig["resolved_ts"],
        "result": sig["result"], "style": sig["style"], "entry_kind": sig["entry_kind"],
        "resolution_logic_version": sig["resolution_logic_version"],
    })
    check_circuit_breaker(state)


# ============================================================================
# SECTION 14 — TELEGRAM INTEGRATION
# ============================================================================

_MDV2_RESERVED = set("_*[]()~`>#+-=|{}.!\\")


def _escape_md2(value: Any) -> str:
    return "".join(f"\\{ch}" if ch in _MDV2_RESERVED else ch for ch in str(value))


def _escape_md2_code(value: Any) -> str:
    s = str(value)
    return s.replace("\\", "\\\\").replace("`", "\\`")


def _fmt_price(x: Optional[float]) -> str:
    if x is None:
        return "-"
    if abs(x) >= 100:
        return f"{x:,.2f}"
    if abs(x) >= 1:
        return f"{x:,.4f}"
    return f"{x:.8f}".rstrip("0").rstrip(".")


TELEGRAM_MAX_LEN = 4096


def _truncate_tg(text: str) -> str:
    if len(text) <= TELEGRAM_MAX_LEN:
        return text
    marker = "\n\n\\.\\.\\. truncated"
    return text[:TELEGRAM_MAX_LEN - len(marker)] + marker


def _tg_api(method: str, payload: dict) -> Optional[dict]:
    if not TELEGRAM_ENABLED:
        log.info("Telegram dispatch skipped (%s) -- credentials not configured.", method)
        return None
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/{method}"
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError, OSError) as e:
            log.warning("telegram %s attempt %d failed: %s", method, attempt + 1, e)
            time.sleep(0.6 * (attempt + 1))
    return None


def send_telegram(text: str, reply_to: Optional[int] = None) -> Optional[int]:
    text = _truncate_tg(text)
    payload: Dict[str, Any] = {
        "chat_id": TG_CHAT_ID, "text": text, "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }
    if reply_to is not None:
        payload["reply_to_message_id"] = reply_to
    result = _tg_api("sendMessage", payload)
    if result and result.get("ok"):
        return result["result"].get("message_id")
    return None


def _send_telegram_safe(text: str) -> None:
    try:
        send_telegram(text)
    except Exception:
        pass


def send_reaction(message_id: int, emoji: str = REACTION_EMOJI) -> None:
    if not TELEGRAM_ENABLED:
        return
    if emoji not in ALLOWED_REACTION_EMOJIS:
        log.warning("Reaction emoji %r not in ALLOWED_REACTION_EMOJIS -- falling back to default.", emoji)
        emoji = REACTION_EMOJI
    _tg_api("setMessageReaction", {
        "chat_id": TG_CHAT_ID, "message_id": message_id,
        "reaction": [{"type": "emoji", "emoji": emoji}],
    })


def _send_telegram_safe_with_reaction(text: str, emoji: str) -> None:
    try:
        msg_id = send_telegram(text)
        if msg_id:
            send_reaction(msg_id, emoji)
    except Exception:
        pass


def format_signal_message(sig: dict) -> str:
    e = _escape_md2
    ec = _escape_md2_code
    is_long = sig["direction"] == "bullish"
    direction_label = f"{'🟢' if is_long else '🔴'} {'LONG' if is_long else 'SHORT'}"
    engine_label = human_label(sig["engine"])
    style_label = human_label(sig["style"])
    ct_badge = "\n⚠️ *COUNTER\\-TREND* — against the Weekly/Daily bias" if sig["counter_trend"] else ""
    confluences = ", ".join(human_label(x) for x in sig["confluences"])
    confidence_pct = f"{sig['confidence'] * 100:.0f}%"
    rr1_str = f"{sig['rr1']:.2f}"
    rr2_str = f"{sig['rr2']:.2f}"
    lines = [
        f"*{e(ENGINE_NAME)} v{e(ENGINE_VERSION)}*",
        f"{direction_label} {e(sig['symbol'])} — *{e(engine_label)}*{ct_badge}",
        "",
        f"Style: {e(style_label)}   Grade: *{e(sig['grade'])}*   Confidence: *{e(confidence_pct)}*",
        f"Entry type: {e(human_label(sig['entry_kind']))} ({e(sig.get('entry_tf', TF_15M).upper())})",
        "",
        f"Entry:",
        f"`{ec(_fmt_price(sig['entry']))}`",
        f"SL:",
        f"`{ec(_fmt_price(sig['sl']))}`",
        f"TP1:",
        f"`{ec(_fmt_price(sig['tp1']))}`",
        f"TP2 \\(suggested\\):",
        f"`{ec(_fmt_price(sig['tp2']))}`",
        "",
        f"RR to TP1: {e(rr1_str)}   RR to TP2 \\(suggested\\): {e(rr2_str)}",
        f"Confluences: {e(confluences)}",
    ]
    return "\n".join(lines)


def format_status_message(sig: dict, event_type: str) -> str:
    e = _escape_md2
    engine_label = human_label(sig["engine"])
    header = f"*{e(ENGINE_NAME)} v{e(ENGINE_VERSION)}* — {e(sig['symbol'])} {e(engine_label)}"
    r_realized_str = f"{sig.get('r_realized', 0.0):.2f}"
    if event_type == "win":
        body = (f"🏆 *TP1 hit — WIN\\.* Signal resolved\\.\n"
                f"Realized R: {e(r_realized_str)}\n"
                f"SL remains at its original level, unchanged\\.")
    elif event_type == "loss":
        body = f"😭 *SL hit — LOSS\\.* Signal resolved\\.\nRealized R: {e(r_realized_str)}"
    elif event_type == "expired":
        body = "🤷 *Expired — no fill\\.* Price never reached entry within the pending window\\."
    elif event_type == "activated":
        body = "✅ *Activated\\.* Entry price has been reached\\."
    else:
        body = e(human_label(event_type))
    return f"{header}\n\n{body}"


def _format_circuit_breaker(cb: dict, tripped: bool) -> str:
    e = _escape_md2
    header = f"*{e(ENGINE_NAME)} v{e(ENGINE_VERSION)}* — Live\\-Performance Circuit Breaker"
    if tripped:
        return f"{header}\n\n🤯 Adaptation frozen: {e(cb['reason'])}\nSignal generation continues on last\\-known\\-good parameters\\."
    return f"{header}\n\n👏 Adaptation resumed — live performance recovered to baseline\\."


def format_daily_summary(state: dict, day_key: str) -> str:
    e = _escape_md2
    trades = [t for t in state["tier2_trades"]
              if datetime.fromtimestamp(t["resolved_ts"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d") == day_key]
    n = len(trades)
    wins = sum(1 for t in trades if t["result"] == "win")
    losses = n - wins
    wr = (wins / n * 100) if n else 0.0
    gross_win = sum(t["r_realized"] for t in trades if t["r_realized"] > 0)
    gross_loss = abs(sum(t["r_realized"] for t in trades if t["r_realized"] < 0))
    pf = (gross_win / gross_loss) if gross_loss > 1e-9 else (gross_win if gross_win > 0 else 0.0)
    avg_rr = statistics.fmean([t["r_realized"] for t in trades if t["result"] == "win"]) if wins else 0.0
    avg_hold = statistics.fmean([
        (t["resolved_ts"] - t.get("created_ts", t["resolved_ts"])) / 60000.0
        for t in trades
    ]) if trades else 0.0

    by_engine = collections.defaultdict(lambda: {"n": 0, "wins": 0})
    for t in trades:
        by_engine[t["engine"]]["n"] += 1
        by_engine[t["engine"]]["wins"] += 1 if t["result"] == "win" else 0
    engine_lines = [f"  {e(human_label(eng))}: {v['wins']}/{v['n']}" for eng, v in by_engine.items()]

    by_regime = collections.defaultdict(lambda: {"n": 0, "wins": 0})
    for t in trades:
        rk = "Trending" if (t.get("regime_at_entry", {}).get("trend_strength") or 0) >= 22 else "Ranging"
        by_regime[rk]["n"] += 1
        by_regime[rk]["wins"] += 1 if t["result"] == "win" else 0
    regime_lines = [f"  {e(r)}: {v['wins']}/{v['n']}" for r, v in by_regime.items()]

    forensic_lines = [f"  {e(human_label(k))}: {v.get('n', 0)}"
                      for k, v in state["tier1"]["forensic_counts"].items()]

    calib_lines = []
    for key, v in state["tier1"]["calibration_buckets"].items():
        if v.get("n", 0) >= 5:
            engine, bucket = key.split("|", 1)
            realized_wr = v["wins"] / v["n"] * 100
            calib_lines.append(f"  {e(human_label(engine))} [{e(bucket)}]: {e(f'{realized_wr:.0f}%')} realized ({e(v['n'])} trades)")

    fill_stats_lines = []
    for key, v in state["tier1"]["fill_stats"].items():
        if v.get("dispatched", 0) > 0:
            fill_rate = v.get("filled", 0) / v["dispatched"] * 100
            fill_stats_lines.append(f"  {e(human_label(key))}: {e(f'{fill_rate:.0f}%')} filled ({e(v.get('expired', 0))} expired)")

    best = max(trades, key=lambda t: t["r_realized"], default=None)
    worst = min(trades, key=lambda t: t["r_realized"], default=None)
    best_r_str = f"{best['r_realized']:.2f}" if best else ""
    worst_r_str = f"{worst['r_realized']:.2f}" if worst else ""

    cb = state["tier1"]["circuit_breaker"]
    cb_str = f"TRIPPED — {e(str(cb.get('reason', '')))}" if cb["tripped"] else "normal"

    lines = [
        f"*{e(ENGINE_NAME)} v{e(ENGINE_VERSION)} — Daily Summary ({e(day_key)})*",
        "",
        f"Signals resolved: {e(n)}    Wins: {e(wins)}    Losses: {e(losses)}",
        f"Win rate: {e(f'{wr:.1f}%')}    Profit factor: {e(f'{pf:.2f}')}    Avg winning RR: {e(f'{avg_rr:.2f}')}",
        f"Avg hold: {e(f'{avg_hold:.0f}m')}",
        "",
        "*By Engine:*", *(engine_lines or ["  (none)"]),
        "",
        "*By Regime:*", *(regime_lines or ["  (none)"]),
        "",
        "*Forensic Categories:*", *(forensic_lines or ["  (none)"]),
        "",
        "*Confidence Calibration:*", *(calib_lines or ["  (insufficient data)"]),
        "",
        "*Fill Rate:*", *(fill_stats_lines or ["  (none)"]),
        "",
        (f"Best: {e(best['symbol'])} {e(human_label(best['engine']))} "
         f"({e(best_r_str)}R)") if best else "Best: n/a",
        (f"Worst: {e(worst['symbol'])} {e(human_label(worst['engine']))} "
         f"({e(worst_r_str)}R)") if worst else "Worst: n/a",
        "",
        f"Circuit breaker: {e(cb_str)}",
    ]
    return "\n".join(lines)


# ============================================================================
# SECTION 15 — ORCHESTRATION / MAIN LOOP
# ============================================================================

def scan_symbol(symbol: str, candle_cache: dict, state: dict, now_ms: int) -> Optional[dict]:
    bundle = fetch_all_candles(symbol, candle_cache, now_ms)
    if bundle is None:
        return None
    try:
        views = build_all_views(bundle)
    except (ValueError, ZeroDivisionError, IndexError, statistics.StatisticsError) as e:
        log.warning("build_all_views failed for %s: %s", symbol, e)
        return None

    stage1 = stage1_bias(views)
    if stage1.outcome == "neutral" and not ENABLE_COUNTERTREND_ENGINE:
        return {"symbol": symbol, "views": views, "stage1": stage1,
                "stage3": StageResult(3, "INVALID", "neutral bias"),
                "stage4": StageResult(4, "NO TRADE", "neutral bias")}

    if stage1.outcome != "neutral":
        stage2 = stage2_context(views, stage1.outcome)
        if stage2.outcome != "agree":
            return {"symbol": symbol, "views": views, "stage1": stage1,
                    "stage3": StageResult(3, "INVALID", "Stage 2 disagree"),
                    "stage4": StageResult(4, "NO TRADE", "Stage 2 disagree")}
        stage3 = stage3_zone_selection(views, stage1.outcome, state, symbol)
        if stage3.outcome == "VALID":
            stage4 = stage4_entry(views, stage1.outcome, stage3.poi)  # type: ignore[attr-defined]
        else:
            stage4 = StageResult(4, "NO TRADE", "Stage 3 not VALID")
    else:
        stage3 = StageResult(3, "INVALID", "no bias")
        stage4 = StageResult(4, "NO TRADE", "no bias")

    return {"symbol": symbol, "views": views, "stage1": stage1, "stage3": stage3, "stage4": stage4}


def run_scan(state: dict, candle_cache: dict) -> None:
    now_ms = utcnow_ms()
    active = state["active_signals"]
    t_start = time.monotonic()

    log.info("=== %s v%s scan starting: %d active signal(s), %d/%d symbols cached ===",
             ENGINE_NAME, ENGINE_VERSION, len(active), len(candle_cache), len(WATCHLIST))

    # 1. Monitor active signals first
    still_active = []
    for sig in active:
        symbol = sig["symbol"]
        m_candles = get_candles(symbol, MONITOR_TF, 30, now_ms,
                                candle_cache.get(symbol, {}).get(MONITOR_TF))
        event = monitor_signal(sig, m_candles)
        if event is None:
            still_active.append(sig)
            continue
        if event["type"] == "expired":
            fill_key = f"{sig['engine']}|{sig['entry_kind']}"
            fs = state["tier1"]["fill_stats"].setdefault(fill_key, {"dispatched": 0, "filled": 0, "expired": 0})
            fs["expired"] += 1
            state["tier1"]["totals"]["expired"] += 1
            _send_telegram_safe_with_reaction(format_status_message(sig, "expired"), REACTION_EMOJI_MAP["expired"])
            continue
        if event["type"] in ("win", "loss"):
            bundle = fetch_all_candles(symbol, candle_cache, now_ms)
            regime = RegimeVector("neutral", 50, 15, "off_hours", 0.5, 0.0, "neutral", 0.5, 0.0)
            if bundle:
                try:
                    views = build_all_views(bundle)
                    stage1 = stage1_bias(views)
                    macro_bias = stage1.outcome if stage1.outcome != "neutral" else "neutral"
                    regime = build_regime_vector(views, macro_bias, {}, now_ms)
                except (ValueError, ZeroDivisionError, IndexError, statistics.StatisticsError):
                    pass
            process_resolution(sig, {TF_1H: views.get(TF_1H)} if bundle else {}, regime, state)
            _send_telegram_safe_with_reaction(format_status_message(sig, event["type"]),
                                              REACTION_EMOJI_MAP.get(event["type"], REACTION_EMOJI))
            continue
        still_active.append(sig)
    state["active_signals"] = still_active
    log.info("Monitoring done (%.1fs): %d still active.", time.monotonic() - t_start, len(still_active))

    if state["tier1"]["circuit_breaker"]["tripped"]:
        log.info("Circuit breaker active -- adaptation frozen, signal generation continues")

    # 2. Scan watchlist
    views_by_symbol_1h = {}
    scan_results = {}
    with ThreadPoolExecutor(max_workers=SCAN_WORKER_THREADS) as pool:
        futures = {pool.submit(scan_symbol, sym, candle_cache, state, now_ms): sym for sym in WATCHLIST}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                result = fut.result()
            except Exception as e:
                log.error("scan_symbol failed for %s: %s", sym, e)
                result = None
            if result:
                scan_results[sym] = result
                views_by_symbol_1h[sym] = result["views"][TF_1H]
    log.info("Scan done (%.1fs): %d/%d symbols produced data.",
             time.monotonic() - t_start, len(scan_results), len(WATCHLIST))

    macro_bias = "neutral"
    if MACRO_ASSET in scan_results:
        macro_bias = scan_results[MACRO_ASSET]["stage1"].outcome

    all_new_signals = []
    for symbol, result in scan_results.items():
        views, stage1, stage3, stage4 = result["views"], result["stage1"], result["stage3"], result["stage4"]
        try:
            regime = build_regime_vector(views, macro_bias, views_by_symbol_1h, now_ms)
        except (ValueError, ZeroDivisionError, statistics.StatisticsError) as e:
            log.warning("regime vector failed for %s: %s", symbol, e)
            continue
        log.info("%s: regime=%s vol_pctl=%.0f trend=%.0f noise=%.2f",
                 symbol, regime.label(), regime.volatility_pctl, regime.trend_strength, regime.noise_index)

        candidates = run_specialized_engines(stage1.outcome, views, stage3, stage4, regime, state, symbol)
        if not candidates:
            continue
        ranked = rank_and_select(candidates, views, regime, state, symbol, now_ms)
        accepted = correlation_dedup(ranked, state["active_signals"] + all_new_signals, state, now_ms)
        for score, grade, cand, res in accepted:
            sig = new_signal_record(cand, score, grade, symbol, now_ms, regime)
            fill_key = f"{cand.engine}|{cand.entry_kind}"
            fs = state["tier1"]["fill_stats"].setdefault(fill_key, {"dispatched": 0, "filled": 0, "expired": 0})
            fs["dispatched"] += 1
            msg_id = send_telegram(format_signal_message(sig))
            if msg_id:
                send_reaction(msg_id, REACTION_EMOJI_MAP["dispatch"])
            sig["tg_message_id"] = msg_id
            all_new_signals.append(sig)
            log.info("Dispatched %s %s %s grade=%s score=%.2f rr1=%.2f",
                     symbol, cand.engine, cand.direction, grade, score, cand.rr1)

    state["active_signals"].extend(all_new_signals)

    # 3. Daily summary at/after 08:00 UTC
    now_dt = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
    day_key = now_dt.strftime("%Y-%m-%d")
    if now_dt.hour >= 8 and state["tier1"]["daily_totals"].get(day_key) != "sent":
        _send_telegram_safe(format_daily_summary(state, day_key))
        state["tier1"]["daily_totals"][day_key] = "sent"

    prune_tier2(state)
    state["last_run_ts"] = now_ms
    log.info("=== %s v%s scan finished in %.1fs: %d new signal(s), %d active ===",
             ENGINE_NAME, ENGINE_VERSION, time.monotonic() - t_start,
             len(all_new_signals), len(state["active_signals"]))


def main() -> int:
    log.info("=== %s v%s process started (pid=%d) ===", ENGINE_NAME, ENGINE_VERSION, os.getpid())
    run_started = time.monotonic()

    lock_f = open(LOCK_PATH, "a+")
    try:
        fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        log.warning("Another run is already in progress (lock held on %s) -- exiting.", os.path.abspath(LOCK_PATH))
        lock_f.close()
        return 0

    try:
        log.info("State path: %s | Cache path: %s | CWD: %s",
                 os.path.abspath(STATE_PATH), os.path.abspath(CANDLE_CACHE_PATH), os.getcwd())
        state = load_state()
        candle_cache = load_candle_cache()
        log.info("Loaded state.json (%d active) and candle_cache.json (%d symbols).",
                 len(state.get("active_signals", [])), len(candle_cache))
        try:
            run_scan(state, candle_cache)
        except Exception:
            log.exception("Unhandled exception at top level -- run aborted, state will still be saved.")
            return 1
        finally:
            if not save_state(state):
                log.error("Failed to persist state.json")
            if not save_candle_cache(candle_cache):
                log.error("Failed to persist candle_cache.json")
            for p in (STATE_PATH, CANDLE_CACHE_PATH):
                try:
                    log.info("Persisted %s (%d bytes)", os.path.abspath(p), os.path.getsize(p))
                except OSError:
                    log.error("Post-save check failed -- %s does not exist on disk.", os.path.abspath(p))
        duration = time.monotonic() - run_started
        log.info("=== %s v%s run finished in %.1fs ===", ENGINE_NAME, ENGINE_VERSION, duration)
        return 0
    finally:
        fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)
        lock_f.close()


if __name__ == "__main__":
    import sys
    sys.exit(main())
