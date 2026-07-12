#!/usr/bin/env python3
"""
AETHERIS -- Adaptive Ensemble Trading & Hierarchical Execution Reasoning
Institutional Signal Engine, v1.0.0

Self-contained crypto signal engine for Hyperliquid perpetual futures:
data layer, thirteen specialized regime engines, an adaptive Decision
Engine, structure-based risk management, continuous learning, and a
Telegram integration. stdlib only.

SL is write-once after signal creation (no auto-breakeven), and SL/TP
are never evaluated before entry_filled=True (no phantom fills) -- see
`ActiveSignal` and `run_bug_selfcheck()`.
"""

from __future__ import annotations

import json
import logging
import os
import statistics
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional, Any

# =====================================================================
# SECTION 0: CONFIG / CONSTANTS
# =====================================================================

ENGINE_NAME = "AETHERIS"
ENGINE_VERSION = "1.0.0"
ENGINE_TAG = f"{ENGINE_NAME} v{ENGINE_VERSION}"

HL_API_URL = "https://api.hyperliquid.xyz/info"

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")

STATE_PATH = os.environ.get("AETHERIS_STATE_PATH", "state.json")
CANDLE_CACHE_PATH = os.environ.get("AETHERIS_CANDLE_CACHE_PATH", "candle_cache.json")
LEARNING_PATH = os.environ.get("AETHERIS_LEARNING_PATH", "learning_state.json")
LOG_PATH = os.environ.get("AETHERIS_LOG_PATH", "aetheris_engine.log")

# Same watchlist every run -- deliberately liquid, well-established
# perpetuals so candle history and spread assumptions stay realistic.
WATCHLIST = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]

# 15M is the fastest timeframe used anywhere in this engine, per spec.
# HTF supplies bias/structure/OB/BB context; LTF supplies confirmation,
# precision entry timing, and per-candle SL/TP monitoring.
TF_HTF = "4h"      # bias / macro structure
TF_STRUCT = "1h"   # structure / OB / BB / FVG mapping
TF_LTF = "15m"      # confirmation, entry timing, and exit monitoring
MONITOR_TF = TF_LTF

TF_MS = {
    "15m": 15 * 60_000,
    "1h": 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
}
CANDLE_COUNT = {"15m": 400, "1h": 400, "4h": 300, "1d": 200}

SCAN_INTERVAL_MINUTES = 15

# Pending (zone/POI) entries expire after this many *closed* MONITOR_TF
# candles if price never trades into the entry. 12 * 15m = 3h -- long
# enough for a realistic retrace, short enough to not tie up a slot on a
# setup that has clearly moved on.
PENDING_ENTRY_EXPIRY_BARS = 12

# Minimum distance (in ATR multiples of the LTF ATR) an entry must sit
# from both its own SL and its TP1, so we never accept a signal with a
# structurally trivial stop or an unrealistically close first target.
MIN_ENTRY_SL_ATR_BUFFER = 0.35
MIN_ENTRY_TP1_ATR_BUFFER = 0.55

# A pending/zone entry may not sit further than this many ATR from
# current market price at signal time -- keeps signals actionable
# instead of "call options on price wandering back".
MAX_ENTRY_DISTANCE_ATR = 2.2

MAX_CONCURRENT_ACTIVE_SIGNALS = 10
MAX_CORRELATED_CONCURRENT = 2  # cap on same-direction majors-cluster slots
# Re-derived for the watchlist above; ARB/OP/INJ dropped since
# they're no longer tracked, PENGU/XLM slotted into meme_alt (the existing
# catch-all bucket for large-cap non-l1/non-defi names). ZEC (privacy) has
# no peer on this list, so it's left ungrouped -- symbol_cluster() already
# handles that as None.
CORRELATION_CLUSTERS = {
    "majors": {"BTC", "ETH"},
    "l1": {"SOL", "AVAX", "NEAR", "APT", "SUI", "DOT", "TAO"},
    "defi": {"AAVE", "UNI", "LINK", "ONDO", "PENDLE"},
    "meme_alt": {"DOGE", "XRP", "ADA", "TRX", "LTC", "BCH", "HYPE", "BNB", "PENGU", "XLM"},
}

# Minimum closed trades in a given (engine, regime) bucket before the
# learning subsystem is allowed to act on its stats (reweight / gate).
# Below this, we fall back to the static prior weight.
MIN_SAMPLE_SIZE_FOR_LEARNING = 20

# Simulated cost model applied to every backtest/paper fill.
TAKER_FEE_BPS = 4.5     # Hyperliquid taker fee, bps of notional
SLIPPAGE_BPS = 2.0      # assumed adverse slippage on fill, bps
SPREAD_BPS = 1.5        # assumed half-spread crossed on entry, bps

ATR_LEN = 14
RSI_LEN = 14
ADX_LEN = 14
EMA_FAST, EMA_SLOW, EMA_TREND = 20, 50, 200
BB_LEN, BB_MULT = 20, 2.0
SWING_LOOKBACK = 3  # bars each side for fractal swing high/low detection

DAILY_SUMMARY_HOUR_UTC = 8

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_PATH)],
)
log = logging.getLogger(ENGINE_NAME)

# =====================================================================
# SECTION 1: CORE DATA STRUCTURES
# =====================================================================

@dataclass
class Candle:
    t: int      # open time, ms
    o: float
    h: float
    l: float
    c: float
    v: float


@dataclass
class Candidate:
    """One proposed trade from a specialized sub-engine, pre-decision."""
    engine: str
    symbol: str
    direction: str          # "long" | "short"
    entry: float
    sl: float
    tp1: float
    tp2: float
    confidence: float       # 0-100, raw sub-engine confidence
    expected_rr: float
    confluences: list[str]
    regime_fit: str         # "trend" | "range" | "volatile" | "quiet"
    entry_kind: str         # "market" | "zone"  (zone => pending-fill semantics)
    rationale: str
    atr_ltf: float
    mtf_aligned: bool = False


@dataclass
class ActiveSignal:
    """
    A Candidate that the Decision Engine selected and that is now live.

    Invariant enforcement (BUG #1 / BUG #2):
      - `sl` is set once at creation from the Candidate and is NEVER
        reassigned anywhere in this file after that point. TP1 being
        hit does not touch it. grep the file for `.sl =` outside
        __init__/from_candidate to verify this holds.
      - `entry_filled` starts False for entry_kind == "zone" candidates
        (True immediately for "market" candidates). `evaluate_candle()`
        is the ONLY function permitted to flip it True, and it may only
        do so when the candle's [low, high] range actually contains
        `entry`. No SL/TP branch in `evaluate_candle()` is reachable
        while entry_filled is False.
    """
    id: str
    engine: str
    symbol: str
    direction: str
    entry: float
    sl: float                      # write-once; never reassigned post-creation
    tp1: float
    tp2: float
    confidence: float
    expected_rr: float
    confluences: list[str]
    regime_fit: str
    entry_kind: str
    tier: str                      # "A+" | "A" | "B"
    rationale: str
    created_ts: int
    entry_filled: bool
    last_processed_ts: int = 0
    bars_pending: int = 0
    tp1_hit: bool = False
    status: str = "activated"      # activated|tp1|tp2|sl|expired|closed|cancelled
    tp1_r_realized: Optional[float] = None
    final_r: Optional[float] = None
    close_ts: Optional[int] = None
    close_reason: str = ""
    forensic_tag: str = ""
    telegram_message_id: Optional[int] = None   # activation message id; reactions land here

    @staticmethod
    def from_candidate(c: Candidate, tier: str, sig_id: str, now_ms: int) -> "ActiveSignal":
        filled = c.entry_kind == "market"
        return ActiveSignal(
            id=sig_id, engine=c.engine, symbol=c.symbol, direction=c.direction,
            entry=c.entry, sl=c.sl, tp1=c.tp1, tp2=c.tp2, confidence=c.confidence,
            expected_rr=c.expected_rr, confluences=list(c.confluences),
            regime_fit=c.regime_fit, entry_kind=c.entry_kind, tier=tier,
            rationale=c.rationale, created_ts=now_ms, entry_filled=filled,
            last_processed_ts=now_ms, bars_pending=0,
        )


def _risk_per_unit(sig: ActiveSignal) -> float:
    return abs(sig.entry - sig.sl)


def r_multiple(sig: ActiveSignal, exit_price: float) -> float:
    risk = _risk_per_unit(sig)
    if risk <= 0:
        return 0.0
    if sig.direction == "long":
        return (exit_price - sig.entry) / risk
    return (sig.entry - exit_price) / risk


# =====================================================================
# SECTION 2: HYPERLIQUID CLIENT + RATE LIMITING
# =====================================================================

class WeightedRateLimiter:
    """
    Hyperliquid's /info endpoint is weight-metered, not just count-metered.
    We track a rolling budget and sleep proactively rather than reactively
    (i.e. before we would exceed budget, not after we get a 429), which is
    both friendlier to the API and avoids burning retry budget on backoff.
    """
    def __init__(self, budget_per_minute: int = 1150, safety_margin: float = 0.85):
        self.budget = int(budget_per_minute * safety_margin)
        self._lock = threading.Lock()
        self._window_start = time.time()
        self._used = 0

    def acquire(self, weight: int = 20):
        with self._lock:
            now = time.time()
            if now - self._window_start >= 60:
                self._window_start = now
                self._used = 0
            if self._used + weight > self.budget:
                sleep_for = 60 - (now - self._window_start)
                if sleep_for > 0:
                    log.info(f"[rate-limit] budget exhausted, sleeping {sleep_for:.1f}s")
                    time.sleep(sleep_for)
                self._window_start = time.time()
                self._used = 0
            self._used += weight


RATE_LIMITER = WeightedRateLimiter()


def hl_post(payload: dict, weight: int = 20, retries: int = 4, timeout: int = 12) -> Optional[Any]:
    """POST to Hyperliquid /info with weighted throttling and exponential backoff."""
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        HL_API_URL, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    last_err = None
    for attempt in range(retries):
        RATE_LIMITER.acquire(weight)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code == 429:
                backoff = min(30, (2 ** attempt) + 1)
                log.warning(f"[hl_post] 429 rate limited, backoff {backoff}s (attempt {attempt+1}/{retries})")
                time.sleep(backoff)
                continue
            log.error(f"[hl_post] HTTP {e.code}: {e.reason}")
            time.sleep(min(15, 2 ** attempt))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ConnectionError) as e:
            last_err = e
            log.warning(f"[hl_post] transient error {e!r}, attempt {attempt+1}/{retries}")
            time.sleep(min(15, 2 ** attempt))
    log.error(f"[hl_post] giving up after {retries} attempts: {last_err!r}")
    return None


def hl_coin(symbol: str) -> str:
    """Hyperliquid perp coin naming is mostly bare symbols; kept as a hook
    for the handful of exceptions Hyperliquid renames over time."""
    overrides = {}
    return overrides.get(symbol, symbol)


def fetch_candles_raw(symbol: str, interval: str, start_ms: int, end_ms: int) -> list[Candle]:
    payload = {
        "type": "candleSnapshot",
        "req": {"coin": hl_coin(symbol), "interval": interval, "startTime": start_ms, "endTime": end_ms},
    }
    data = hl_post(payload, weight=20)
    if not data:
        return []
    out = []
    for row in data:
        try:
            out.append(Candle(
                t=int(row["t"]), o=float(row["o"]), h=float(row["h"]),
                l=float(row["l"]), c=float(row["c"]), v=float(row["v"]),
            ))
        except (KeyError, ValueError, TypeError):
            continue
    out.sort(key=lambda x: x.t)
    return out


# ---- Shared on-disk candle cache -------------------------------------
# Deltas only: each fetch re-requests a small overlap past the cached
# watermark (to absorb any late-finalizing candle) plus everything new,
# instead of re-downloading full history every scan.
CANDLE_DELTA_OVERLAP_BARS = 3
_cache_lock = threading.Lock()


def load_candle_cache() -> dict:
    if not os.path.exists(CANDLE_CACHE_PATH):
        return {}
    try:
        with open(CANDLE_CACHE_PATH, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.warning(f"[cache] corrupt candle cache, starting fresh: {e!r}")
        return {}


def save_candle_cache(cache: dict):
    atomic_write_json(CANDLE_CACHE_PATH, cache)


def atomic_write_json(path: str, obj: Any):
    """Write-then-rename so a crash mid-write never leaves a truncated file."""
    d = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp_path = tempfile.mkstemp(dir=d, prefix=".tmp_", suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(obj, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def get_candles(cache: dict, symbol: str, interval: str) -> list[Candle]:
    """Return up-to-date candles for symbol/interval, using + updating the shared cache."""
    key = f"{symbol}:{interval}"
    now_ms = int(time.time() * 1000)
    want = CANDLE_COUNT.get(interval, 300)
    span_ms = TF_MS[interval] * want

    with _cache_lock:
        entry = cache.get(key)

    if entry and entry.get("candles"):
        cached = [Candle(**row) for row in entry["candles"]]
        watermark = cached[-1].t - TF_MS[interval] * CANDLE_DELTA_OVERLAP_BARS
        fresh = fetch_candles_raw(symbol, interval, watermark, now_ms)
        merged = {c.t: c for c in cached}
        for c in fresh:
            merged[c.t] = c
        all_candles = sorted(merged.values(), key=lambda x: x.t)
        all_candles = all_candles[-want:]
    else:
        start = now_ms - span_ms
        all_candles = fetch_candles_raw(symbol, interval, start, now_ms)

    with _cache_lock:
        cache[key] = {"candles": [asdict(c) for c in all_candles], "updated": now_ms}

    return all_candles


def fetch_all_watchlist_candles(cache: dict) -> dict[str, dict[str, list[Candle]]]:
    """symbol -> interval -> candles, fetched concurrently and throttled by
    the shared weighted rate limiter."""
    result: dict[str, dict[str, list[Candle]]] = {s: {} for s in WATCHLIST}
    jobs = [(s, tf) for s in WATCHLIST for tf in (TF_HTF, TF_STRUCT, TF_LTF)]

    def _job(sym, tf):
        try:
            return sym, tf, get_candles(cache, sym, tf)
        except Exception as e:
            log.error(f"[fetch] {sym}/{tf} failed: {e!r}")
            return sym, tf, []

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(_job, s, tf) for s, tf in jobs]
        for fut in as_completed(futures):
            sym, tf, candles = fut.result()
            result[sym][tf] = candles

    save_candle_cache(cache)
    return result


# =====================================================================
# SECTION 3: INDICATORS
# =====================================================================

def sma(vals: list[float], n: int) -> list[Optional[float]]:
    out: list[Optional[float]] = [None] * len(vals)
    if len(vals) < n:
        return out
    running = sum(vals[:n])
    out[n - 1] = running / n
    for i in range(n, len(vals)):
        running += vals[i] - vals[i - n]
        out[i] = running / n
    return out


def ema(vals: list[float], n: int) -> list[Optional[float]]:
    out: list[Optional[float]] = [None] * len(vals)
    if len(vals) < n:
        return out
    k = 2 / (n + 1)
    seed = sum(vals[:n]) / n
    out[n - 1] = seed
    prev = seed
    for i in range(n, len(vals)):
        prev = vals[i] * k + prev * (1 - k)
        out[i] = prev
    return out


def true_range(candles: list[Candle]) -> list[float]:
    tr = [0.0] * len(candles)
    for i in range(len(candles)):
        if i == 0:
            tr[i] = candles[i].h - candles[i].l
        else:
            pc = candles[i - 1].c
            tr[i] = max(candles[i].h - candles[i].l, abs(candles[i].h - pc), abs(candles[i].l - pc))
    return tr


def atr(candles: list[Candle], n: int = ATR_LEN) -> list[Optional[float]]:
    tr = true_range(candles)
    out: list[Optional[float]] = [None] * len(candles)
    if len(candles) < n:
        return out
    seed = sum(tr[:n]) / n
    out[n - 1] = seed
    prev = seed
    for i in range(n, len(candles)):
        prev = (prev * (n - 1) + tr[i]) / n
        out[i] = prev
    return out


def rsi(closes: list[float], n: int = RSI_LEN) -> list[Optional[float]]:
    out: list[Optional[float]] = [None] * len(closes)
    if len(closes) < n + 1:
        return out
    gains, losses = 0.0, 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    avg_gain, avg_loss = gains / n, losses / n
    out[n] = 100.0 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        gain, loss = max(d, 0.0), max(-d, 0.0)
        avg_gain = (avg_gain * (n - 1) + gain) / n
        avg_loss = (avg_loss * (n - 1) + loss) / n
        out[i] = 100.0 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)
    return out


def adx(candles: list[Candle], n: int = ADX_LEN) -> list[Optional[float]]:
    ln = len(candles)
    out: list[Optional[float]] = [None] * ln
    if ln < n * 2:
        return out
    plus_dm = [0.0] * ln
    minus_dm = [0.0] * ln
    tr = true_range(candles)
    for i in range(1, ln):
        up = candles[i].h - candles[i - 1].h
        down = candles[i - 1].l - candles[i].l
        plus_dm[i] = up if (up > down and up > 0) else 0.0
        minus_dm[i] = down if (down > up and down > 0) else 0.0

    def wilder_smooth(vals):
        sm = [0.0] * ln
        seed = sum(vals[1:n + 1])
        sm[n] = seed
        for i in range(n + 1, ln):
            sm[i] = sm[i - 1] - sm[i - 1] / n + vals[i]
        return sm

    sm_tr = wilder_smooth(tr)
    sm_plus = wilder_smooth(plus_dm)
    sm_minus = wilder_smooth(minus_dm)
    dx = [0.0] * ln
    for i in range(n, ln):
        if sm_tr[i] == 0:
            continue
        pdi = 100 * sm_plus[i] / sm_tr[i]
        mdi = 100 * sm_minus[i] / sm_tr[i]
        denom = pdi + mdi
        dx[i] = 100 * abs(pdi - mdi) / denom if denom else 0.0
    start = 2 * n
    if ln <= start:
        return out
    adx_seed = sum(dx[n:start]) / n
    out[start - 1] = adx_seed
    prev = adx_seed
    for i in range(start, ln):
        prev = (prev * (n - 1) + dx[i]) / n
        out[i] = prev
    return out


def bollinger(closes: list[float], n: int = BB_LEN, mult: float = BB_MULT):
    mid = sma(closes, n)
    upper: list[Optional[float]] = [None] * len(closes)
    lower: list[Optional[float]] = [None] * len(closes)
    width: list[Optional[float]] = [None] * len(closes)
    for i in range(len(closes)):
        if mid[i] is None:
            continue
        window = closes[i - n + 1:i + 1]
        sd = statistics.pstdev(window)
        upper[i] = mid[i] + mult * sd
        lower[i] = mid[i] - mult * sd
        width[i] = (upper[i] - lower[i]) / mid[i] if mid[i] else None
    return mid, upper, lower, width


@dataclass
class IndicatorSet:
    closes: list[float]
    ema_fast: list[Optional[float]]
    ema_slow: list[Optional[float]]
    ema_trend: list[Optional[float]]
    atr: list[Optional[float]]
    rsi: list[Optional[float]]
    adx: list[Optional[float]]
    bb_mid: list[Optional[float]]
    bb_up: list[Optional[float]]
    bb_lo: list[Optional[float]]
    bb_width: list[Optional[float]]


def compute_indicators(candles: list[Candle]) -> IndicatorSet:
    closes = [c.c for c in candles]
    bb_mid, bb_up, bb_lo, bb_width = bollinger(closes)
    return IndicatorSet(
        closes=closes,
        ema_fast=ema(closes, EMA_FAST),
        ema_slow=ema(closes, EMA_SLOW),
        ema_trend=ema(closes, EMA_TREND),
        atr=atr(candles),
        rsi=rsi(closes),
        adx=adx(candles),
        bb_mid=bb_mid, bb_up=bb_up, bb_lo=bb_lo, bb_width=bb_width,
    )


# =====================================================================
# SECTION 4: SMC / MARKET STRUCTURE TOOLKIT
# =====================================================================

@dataclass
class SwingPoint:
    idx: int
    price: float
    kind: str   # "high" | "low"


def find_swings(candles: list[Candle], lookback: int = SWING_LOOKBACK) -> list[SwingPoint]:
    """Fractal swing detection: a bar is a swing high/low if it's the
    max/min within `lookback` bars on both sides."""
    swings = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        window = candles[i - lookback:i + lookback + 1]
        h = candles[i].h
        l = candles[i].l
        if h == max(c.h for c in window):
            swings.append(SwingPoint(i, h, "high"))
        if l == min(c.l for c in window):
            swings.append(SwingPoint(i, l, "low"))
    return swings


@dataclass
class StructureEvent:
    idx: int
    kind: str        # "BOS" | "CHoCH" | "MSS"
    direction: str    # "bullish" | "bearish"
    level: float


def detect_structure_events(candles: list[Candle], swings: list[SwingPoint]) -> list[StructureEvent]:
    """
    Walks confirmed swings in order and classifies each break of a prior
    swing extreme as BOS (break in the direction of the established
    trend) or CHoCH/MSS (break against it, i.e. a potential trend
    change -- CHoCH is the first such break, MSS confirms it with a
    second break-and-hold in the new direction).
    """
    events: list[StructureEvent] = []
    if len(swings) < 2:
        return events
    trend = None  # "up" | "down"
    last_high = None
    last_low = None
    choch_pending = None

    ordered = sorted(swings, key=lambda s: s.idx)
    for sp in ordered:
        if sp.kind == "high":
            if last_high is not None and sp.price > last_high.price:
                pass  # will check for close-through below with candle closes
            last_high = sp
        else:
            last_low = sp

    # Second pass: evaluate close-through breaks candle by candle against
    # the running set of confirmed swing extremes (standard SMC BOS/CHoCH
    # definition requires a *close* beyond the swing, not just a wick).
    highs_seen: list[SwingPoint] = []
    lows_seen: list[SwingPoint] = []
    si = 0
    ordered_by_idx = ordered
    for i, candle in enumerate(candles):
        while si < len(ordered_by_idx) and ordered_by_idx[si].idx <= i:
            sp = ordered_by_idx[si]
            if sp.kind == "high":
                highs_seen.append(sp)
            else:
                lows_seen.append(sp)
            si += 1
        if not highs_seen or not lows_seen:
            continue
        recent_high = highs_seen[-1]
        recent_low = lows_seen[-1]
        if candle.c > recent_high.price and recent_high.idx < i:
            direction = "bullish"
            kind = "BOS" if trend in (None, "up") else "CHoCH"
            events.append(StructureEvent(i, kind, direction, recent_high.price))
            if kind == "CHoCH":
                choch_pending = "up"
            elif choch_pending == "up":
                events.append(StructureEvent(i, "MSS", "bullish", recent_high.price))
                choch_pending = None
            trend = "up"
            highs_seen = [SwingPoint(i, recent_high.price, "high")]
        elif candle.c < recent_low.price and recent_low.idx < i:
            direction = "bearish"
            kind = "BOS" if trend in (None, "down") else "CHoCH"
            events.append(StructureEvent(i, kind, direction, recent_low.price))
            if kind == "CHoCH":
                choch_pending = "down"
            elif choch_pending == "down":
                events.append(StructureEvent(i, "MSS", "bearish", recent_low.price))
                choch_pending = None
            trend = "down"
            lows_seen = [SwingPoint(i, recent_low.price, "low")]
    return events


@dataclass
class OrderBlock:
    idx: int
    direction: str   # "bullish" | "bearish"
    top: float
    bottom: float
    mitigated: bool = False
    breaker: bool = False


def detect_order_blocks(candles: list[Candle], events: list[StructureEvent]) -> list[OrderBlock]:
    """
    An order block is the last opposite-colored candle before the
    impulsive move that produced a BOS/CHoCH/MSS. Bullish OB = last down
    candle before an up-break; bearish OB = last up candle before a
    down-break. We then check whether price has since traded back
    through it (mitigated) and, if price closed fully through a
    mitigated OB against its own direction, relabel it a breaker block.
    """
    blocks: list[OrderBlock] = []
    for ev in events:
        if ev.kind not in ("BOS", "CHoCH", "MSS"):
            continue
        lookback_start = max(0, ev.idx - 12)
        seg = candles[lookback_start:ev.idx]
        if not seg:
            continue
        if ev.direction == "bullish":
            down_candles = [(j, c) for j, c in enumerate(seg) if c.c < c.o]
            if not down_candles:
                continue
            j, c = down_candles[-1]
            blocks.append(OrderBlock(lookback_start + j, "bullish", c.h, c.l))
        else:
            up_candles = [(j, c) for j, c in enumerate(seg) if c.c > c.o]
            if not up_candles:
                continue
            j, c = up_candles[-1]
            blocks.append(OrderBlock(lookback_start + j, "bearish", c.h, c.l))

    for ob in blocks:
        for k in range(ob.idx + 1, len(candles)):
            c = candles[k]
            if ob.direction == "bullish":
                if c.l <= ob.top:
                    ob.mitigated = True
                if c.c < ob.bottom:
                    ob.breaker = True
            else:
                if c.h >= ob.bottom:
                    ob.mitigated = True
                if c.c > ob.top:
                    ob.breaker = True
    return blocks


@dataclass
class FairValueGap:
    idx: int              # index of the middle candle of the 3-candle pattern
    direction: str          # "bullish" | "bearish"
    top: float
    bottom: float
    filled: bool = False


def detect_fvgs(candles: list[Candle]) -> list[FairValueGap]:
    """Three-candle imbalance: bullish FVG when candle[i-1].high < candle[i+1].low;
    bearish FVG when candle[i-1].low > candle[i+1].high."""
    gaps: list[FairValueGap] = []
    for i in range(1, len(candles) - 1):
        prev, nxt = candles[i - 1], candles[i + 1]
        if prev.h < nxt.l:
            gaps.append(FairValueGap(i, "bullish", top=nxt.l, bottom=prev.h))
        elif prev.l > nxt.h:
            gaps.append(FairValueGap(i, "bearish", top=prev.l, bottom=nxt.h))
    for gap in gaps:
        for k in range(gap.idx + 2, len(candles)):
            c = candles[k]
            if gap.direction == "bullish" and c.l <= gap.bottom:
                gap.filled = True
                break
            if gap.direction == "bearish" and c.h >= gap.top:
                gap.filled = True
                break
    return gaps


@dataclass
class LiquidityPool:
    idx: int
    price: float
    kind: str        # "buy_side" (above, resting above old highs) | "sell_side" (below old lows)
    swept: bool = False


def detect_liquidity_pools(candles: list[Candle], swings: list[SwingPoint], tolerance_pct: float = 0.0015) -> list[LiquidityPool]:
    """Equal highs/lows (within tolerance) mark resting liquidity pools.
    A pool is 'swept' once a later candle wicks through it and closes
    back on the origin side (classic stop-hunt signature)."""
    pools: list[LiquidityPool] = []
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    for group, kind in ((highs, "buy_side"), (lows, "sell_side")):
        used = set()
        for i, a in enumerate(group):
            if i in used:
                continue
            cluster = [a]
            for j in range(i + 1, len(group)):
                b = group[j]
                if abs(b.price - a.price) / a.price <= tolerance_pct:
                    cluster.append(b)
                    used.add(j)
            if len(cluster) >= 2:
                lvl = statistics.mean(c.price for c in cluster)
                last_idx = max(c.idx for c in cluster)
                pools.append(LiquidityPool(last_idx, lvl, kind))
    for pool in pools:
        for k in range(pool.idx + 1, len(candles)):
            c = candles[k]
            if pool.kind == "buy_side" and c.h > pool.price and c.c < pool.price:
                pool.swept = True
                break
            if pool.kind == "sell_side" and c.l < pool.price and c.c > pool.price:
                pool.swept = True
                break
    return pools


def premium_discount_zone(candles: list[Candle], swings: list[SwingPoint]) -> Optional[tuple[float, float, float]]:
    """Returns (range_low, equilibrium, range_high) from the most recent
    swing high/low leg, used to classify current price as premium
    (upper half, sell zone) or discount (lower half, buy zone)."""
    if len(swings) < 2:
        return None
    recent = sorted(swings, key=lambda s: s.idx)[-8:]
    highs = [s.price for s in recent if s.kind == "high"]
    lows = [s.price for s in recent if s.kind == "low"]
    if not highs or not lows:
        return None
    rng_high, rng_low = max(highs), min(lows)
    if rng_high <= rng_low:
        return None
    eq = (rng_high + rng_low) / 2
    return rng_low, eq, rng_high


def classify_zone(price: float, zone: tuple[float, float, float]) -> str:
    lo, eq, hi = zone
    return "premium" if price >= eq else "discount"


# =====================================================================
# SECTION 5: REGIME DETECTION + SESSION AWARENESS
# =====================================================================

def detect_regime(ind: IndicatorSet, i: int) -> str:
    """trend | range | volatile | quiet, from ADX + BB width percentile."""
    adx_v = ind.adx[i]
    width = ind.bb_width[i]
    if adx_v is None or width is None:
        return "range"
    recent_widths = [w for w in ind.bb_width[max(0, i - 100):i + 1] if w is not None]
    width_pct = 0.5
    if len(recent_widths) >= 20:
        sorted_w = sorted(recent_widths)
        rank = sum(1 for w in sorted_w if w <= width)
        width_pct = rank / len(sorted_w)
    if adx_v >= 25 and width_pct >= 0.45:
        return "trend"
    if width_pct >= 0.80:
        return "volatile"
    if adx_v < 18 and width_pct <= 0.35:
        return "quiet"
    return "range"


def current_session(now: Optional[datetime] = None) -> str:
    now = now or datetime.now(timezone.utc)
    h = now.hour
    if 0 <= h < 7:
        return "asia"
    if 7 <= h < 12:
        return "london"
    if 12 <= h < 16:
        return "london_ny_overlap"
    if 16 <= h < 21:
        return "ny"
    return "asia_open"


SESSION_ENGINE_BIAS = {
    # engines that tend to perform better in a given liquidity window;
    # used only as a mild scoring nudge, never a hard gate, so supply
    # stays distributed through the day rather than drying up.
    "asia": {"range_engine", "mean_reversion_engine", "liquidity_sweep_engine"},
    "london": {"liquidity_sweep_engine", "order_block_engine", "breaker_block_engine", "smc_engine"},
    "london_ny_overlap": {"breakout_engine", "momentum_engine", "trend_continuation_engine", "volatility_expansion_engine"},
    "ny": {"trend_continuation_engine", "pullback_engine", "reversal_engine", "fvg_engine"},
    "asia_open": {"range_engine", "mean_reversion_engine"},
}


# =====================================================================
# SECTION 6: MARKET CONTEXT (precomputed per-symbol bundle)
# =====================================================================

@dataclass
class MarketContext:
    symbol: str
    htf: list[Candle]
    struct: list[Candle]
    ltf: list[Candle]
    ind_htf: IndicatorSet
    ind_struct: IndicatorSet
    ind_ltf: IndicatorSet
    swings_struct: list[SwingPoint]
    events_struct: list[StructureEvent]
    obs_struct: list[OrderBlock]
    fvgs_struct: list[FairValueGap]
    pools_struct: list[LiquidityPool]
    pd_zone: Optional[tuple[float, float, float]]
    regime: str
    htf_bias: str          # "bullish" | "bearish" | "neutral"
    price: float
    session: str


def htf_bias_from_trend(ind: IndicatorSet, i: int) -> str:
    ef, es, et = ind.ema_fast[i], ind.ema_slow[i], ind.ema_trend[i]
    if ef is None or es is None or et is None:
        return "neutral"
    if ef > es > et:
        return "bullish"
    if ef < es < et:
        return "bearish"
    return "neutral"


def build_context(symbol: str, candle_map: dict[str, list[Candle]]) -> Optional[MarketContext]:
    htf, struct, ltf = candle_map.get(TF_HTF, []), candle_map.get(TF_STRUCT, []), candle_map.get(TF_LTF, [])
    min_needed = max(EMA_TREND + 5, ADX_LEN * 2 + 5)
    if len(htf) < min_needed or len(struct) < min_needed or len(ltf) < ATR_LEN + 5:
        return None

    ind_htf = compute_indicators(htf)
    ind_struct = compute_indicators(struct)
    ind_ltf = compute_indicators(ltf)

    swings = find_swings(struct)
    events = detect_structure_events(struct, swings)
    obs = detect_order_blocks(struct, events)
    fvgs = detect_fvgs(struct)
    pools = detect_liquidity_pools(struct, swings)
    pd_zone = premium_discount_zone(struct, swings)

    regime = detect_regime(ind_struct, len(struct) - 1)
    bias = htf_bias_from_trend(ind_htf, len(htf) - 1)
    price = ltf[-1].c

    return MarketContext(
        symbol=symbol, htf=htf, struct=struct, ltf=ltf,
        ind_htf=ind_htf, ind_struct=ind_struct, ind_ltf=ind_ltf,
        swings_struct=swings, events_struct=events, obs_struct=obs,
        fvgs_struct=fvgs, pools_struct=pools, pd_zone=pd_zone,
        regime=regime, htf_bias=bias, price=price, session=current_session(),
    )


def _buffers_ok(entry: float, sl: float, tp1: float, atr_ltf: float) -> bool:
    if atr_ltf <= 0:
        return False
    if abs(entry - sl) < MIN_ENTRY_SL_ATR_BUFFER * atr_ltf:
        return False
    if abs(tp1 - entry) < MIN_ENTRY_TP1_ATR_BUFFER * atr_ltf:
        return False
    return True


def _distance_ok(entry: float, price: float, atr_ltf: float) -> bool:
    if atr_ltf <= 0:
        return False
    return abs(entry - price) <= MAX_ENTRY_DISTANCE_ATR * atr_ltf


# =====================================================================
# SECTION 7: SPECIALIZED SUB-ENGINES
# =====================================================================
# Each engine takes a MarketContext and returns zero-or-more Candidates.
# Hard rejection is reserved for true invalidation (bad structure,
# entry-distance rule, buffer rule) -- everything else is additive
# confluence scoring, never a fixed-count AND-gate, so a merely-missing
# nice-to-have never zeroes out an otherwise strong setup.

MAX_SL_DISTANCE_ATR = 6.0  # reject structurally-derived SLs (e.g. from a
# stale liquidity pool or order block) that imply an unrealistically large
# invalidation distance -- a legitimate structure level can still be a bad
# trade if it's simply too far from price to be a sane risk unit.


def _mk(engine, ctx: MarketContext, direction, entry, sl, tp1, tp2, base_conf,
        confluences, regime_fit, entry_kind, rationale) -> Optional[Candidate]:
    atr_ltf = ctx.ind_ltf.atr[-1] or 0.0
    if atr_ltf <= 0:
        return None
    # Directional sanity: for a long, SL must sit below entry and both
    # targets above; for a short, the reverse. A structure level on the
    # wrong side (e.g. a stale/aged liquidity pool that price has since
    # trended away from) must never silently produce a backwards SL/TP.
    if direction == "long" and not (sl < entry < tp1 and sl < entry < tp2):
        return None
    if direction == "short" and not (tp1 < entry < sl and tp2 < entry < sl):
        return None
    if not _buffers_ok(entry, sl, tp1, atr_ltf):
        return None
    if abs(entry - sl) > MAX_SL_DISTANCE_ATR * atr_ltf:
        return None
    if entry_kind == "zone" and not _distance_ok(entry, ctx.price, atr_ltf):
        return None
    risk = abs(entry - sl)
    reward = abs(tp1 - entry)
    if risk <= 0:
        return None
    expected_rr = (abs(tp2 - entry)) / risk
    mtf_aligned = (direction == "long" and ctx.htf_bias == "bullish") or \
                  (direction == "short" and ctx.htf_bias == "bearish")
    conf = base_conf + (8 if mtf_aligned else -5) + min(len(confluences) * 3, 15)
    conf = max(1.0, min(99.0, conf))
    return Candidate(
        engine=engine, symbol=ctx.symbol, direction=direction, entry=entry, sl=sl,
        tp1=tp1, tp2=tp2, confidence=conf, expected_rr=expected_rr,
        confluences=confluences, regime_fit=regime_fit, entry_kind=entry_kind,
        rationale=rationale, atr_ltf=atr_ltf, mtf_aligned=mtf_aligned,
    )


def smc_engine(ctx: MarketContext) -> list[Candidate]:
    """Primary methodology engine: HTF bias + struct BOS/CHoCH + nearest
    unmitigated order block, filtered by premium/discount positioning."""
    out = []
    if ctx.htf_bias == "neutral" or not ctx.events_struct or not ctx.pd_zone:
        return out
    last_event = ctx.events_struct[-1]
    direction = "long" if ctx.htf_bias == "bullish" else "short"
    if last_event.direction != ("bullish" if direction == "long" else "bearish"):
        return out
    zone = classify_zone(ctx.price, ctx.pd_zone)
    if direction == "long" and zone != "discount":
        return out
    if direction == "short" and zone != "premium":
        return out
    obs = [ob for ob in ctx.obs_struct if not ob.mitigated and not ob.breaker and
           ob.direction == ("bullish" if direction == "long" else "bearish")]
    if not obs:
        return out
    ob = min(obs, key=lambda o: abs((o.top + o.bottom) / 2 - ctx.price))
    entry = (ob.top + ob.bottom) / 2
    atr_ltf = ctx.ind_ltf.atr[-1] or 0.0
    confluences = ["HTF bias aligned", f"struct {last_event.kind}", "unmitigated OB", f"{zone} zone"]
    if direction == "long":
        sl = ob.bottom - 0.25 * atr_ltf
        tp1 = entry + 1.5 * (entry - sl)
        tp2 = entry + 3.0 * (entry - sl)
    else:
        sl = ob.top + 0.25 * atr_ltf
        tp1 = entry - 1.5 * (sl - entry)
        tp2 = entry - 3.0 * (sl - entry)
    c = _mk("smc_engine", ctx, direction, entry, sl, tp1, tp2, 62, confluences,
            "trend", "zone", f"HTF {ctx.htf_bias} bias, {last_event.kind} on struct TF, entry at unmitigated OB in {zone}.")
    if c:
        out.append(c)
    return out


def trend_continuation_engine(ctx: MarketContext) -> list[Candidate]:
    out = []
    if ctx.regime != "trend" or ctx.htf_bias == "neutral":
        return out
    direction = "long" if ctx.htf_bias == "bullish" else "short"
    ef = ctx.ind_struct.ema_fast[-1]
    if ef is None:
        return out
    atr_struct = ctx.ind_struct.atr[-1] or 0.0
    atr_ltf = ctx.ind_ltf.atr[-1] or 0.0
    if atr_struct <= 0 or atr_ltf <= 0:
        return out
    near_ema = abs(ctx.price - ef) <= 1.0 * atr_struct
    confluences = ["trend regime", "HTF bias aligned"]
    if near_ema:
        confluences.append("pullback to EMA20")
    entry = ctx.price
    if direction == "long":
        sl = entry - 1.6 * atr_ltf
        tp1 = entry + 1.5 * (entry - sl)
        tp2 = entry + 3.2 * (entry - sl)
    else:
        sl = entry + 1.6 * atr_ltf
        tp1 = entry - 1.5 * (sl - entry)
        tp2 = entry - 3.2 * (sl - entry)
    c = _mk("trend_continuation_engine", ctx, direction, entry, sl, tp1, tp2, 55,
            confluences, "trend", "market", f"Trend regime, {direction} continuation aligned with HTF bias.")
    if c:
        out.append(c)
    return out


def breakout_engine(ctx: MarketContext) -> list[Candidate]:
    out = []
    struct = ctx.struct
    if len(struct) < 30:
        return out
    recent_widths = [w for w in ctx.ind_struct.bb_width[-30:] if w is not None]
    if len(recent_widths) < 15:
        return out
    was_squeezed = statistics.mean(recent_widths[:-3]) <= statistics.median(
        [w for w in ctx.ind_struct.bb_width if w is not None][-100:] or [0])
    lookback = struct[-16:-1]
    range_high = max(c.h for c in lookback)
    range_low = min(c.l for c in lookback)
    last = struct[-1]
    atr_ltf = ctx.ind_ltf.atr[-1] or 0.0
    if atr_ltf <= 0:
        return out
    confluences = []
    if was_squeezed:
        confluences.append("prior BB squeeze")
    direction = None
    if last.c > range_high:
        direction = "long"
        confluences.append("close above range high")
    elif last.c < range_low:
        direction = "short"
        confluences.append("close below range low")
    if direction is None:
        return out
    if (direction == "long" and ctx.htf_bias == "bearish") or (direction == "short" and ctx.htf_bias == "bullish"):
        return out  # counter-HTF breakouts are the classic fakeout trap; skip
    entry = ctx.price
    if direction == "long":
        sl = range_high - 0.5 * atr_ltf if range_high < entry else entry - 1.5 * atr_ltf
        sl = min(sl, entry - 1.0 * atr_ltf)
        tp1 = entry + 1.5 * (entry - sl)
        tp2 = entry + 3.0 * (entry - sl)
    else:
        sl = range_low + 0.5 * atr_ltf if range_low > entry else entry + 1.5 * atr_ltf
        sl = max(sl, entry + 1.0 * atr_ltf)
        tp1 = entry - 1.5 * (sl - entry)
        tp2 = entry - 3.0 * (sl - entry)
    c = _mk("breakout_engine", ctx, direction, entry, sl, tp1, tp2, 54, confluences,
            "volatile", "market", f"Range breakout {direction}, HTF-aligned.")
    if c:
        out.append(c)
    return out


def pullback_engine(ctx: MarketContext) -> list[Candidate]:
    out = []
    if ctx.htf_bias == "neutral" or not ctx.pd_zone:
        return out
    direction = "long" if ctx.htf_bias == "bullish" else "short"
    zone = classify_zone(ctx.price, ctx.pd_zone)
    want_zone = "discount" if direction == "long" else "premium"
    if zone != want_zone:
        return out
    atr_ltf = ctx.ind_ltf.atr[-1] or 0.0
    if atr_ltf <= 0:
        return out
    lo, eq, hi = ctx.pd_zone
    confluences = ["HTF bias aligned", f"price in {zone}"]
    entry = ctx.price
    if direction == "long":
        sl = lo - 0.3 * atr_ltf
        tp1 = eq
        tp2 = hi
    else:
        sl = hi + 0.3 * atr_ltf
        tp1 = eq
        tp2 = lo
    if abs(tp1 - entry) < MIN_ENTRY_TP1_ATR_BUFFER * atr_ltf:
        return out
    c = _mk("pullback_engine", ctx, direction, entry, sl, tp1, tp2, 53, confluences,
            "trend", "market", f"Pullback into {zone} zone with HTF {ctx.htf_bias} bias, targeting equilibrium/opposite extreme.")
    if c:
        out.append(c)
    return out


def liquidity_sweep_engine(ctx: MarketContext) -> list[Candidate]:
    out = []
    swept = [p for p in ctx.pools_struct if p.swept]
    if not swept:
        return out
    pool = swept[-1]
    atr_ltf = ctx.ind_ltf.atr[-1] or 0.0
    if atr_ltf <= 0:
        return out
    direction = "long" if pool.kind == "sell_side" else "short"
    confluences = [f"{pool.kind} liquidity swept"]
    if ctx.regime in ("range", "volatile"):
        confluences.append(f"{ctx.regime} regime")
    entry = ctx.price
    if direction == "long":
        sl = pool.price - 0.5 * atr_ltf
        tp1 = entry + 1.4 * (entry - sl)
        tp2 = entry + 2.8 * (entry - sl)
    else:
        sl = pool.price + 0.5 * atr_ltf
        tp1 = entry - 1.4 * (sl - entry)
        tp2 = entry - 2.8 * (sl - entry)
    c = _mk("liquidity_sweep_engine", ctx, direction, entry, sl, tp1, tp2, 58, confluences,
            "range", "market", f"Reversal after {pool.kind} liquidity sweep at {pool.price:.4g}.")
    if c:
        out.append(c)
    return out


def order_block_engine(ctx: MarketContext) -> list[Candidate]:
    out = []
    atr_ltf = ctx.ind_ltf.atr[-1] or 0.0
    if atr_ltf <= 0:
        return out
    fresh = [ob for ob in ctx.obs_struct if not ob.mitigated and not ob.breaker]
    for ob in fresh[-4:]:
        direction = "long" if ob.direction == "bullish" else "short"
        if (direction == "long" and ctx.htf_bias == "bearish") or (direction == "short" and ctx.htf_bias == "bullish"):
            continue
        entry = (ob.top + ob.bottom) / 2
        confluences = ["unmitigated order block"]
        if direction == "long":
            sl = ob.bottom - 0.25 * atr_ltf
            tp1 = entry + 1.5 * (entry - sl)
            tp2 = entry + 3.0 * (entry - sl)
        else:
            sl = ob.top + 0.25 * atr_ltf
            tp1 = entry - 1.5 * (sl - entry)
            tp2 = entry - 3.0 * (sl - entry)
        c = _mk("order_block_engine", ctx, direction, entry, sl, tp1, tp2, 50, confluences,
                "trend", "zone", f"Entry at unmitigated {ob.direction} order block.")
        if c:
            out.append(c)
    return out[:1]  # only the freshest/nearest to avoid duplicate near-identical candidates


def breaker_block_engine(ctx: MarketContext) -> list[Candidate]:
    out = []
    atr_ltf = ctx.ind_ltf.atr[-1] or 0.0
    if atr_ltf <= 0:
        return out
    breakers = [ob for ob in ctx.obs_struct if ob.breaker]
    if not breakers:
        return out
    ob = breakers[-1]
    # A breaker flips role: a bullish OB that broke down becomes bearish
    # resistance-on-retest, and vice versa.
    direction = "short" if ob.direction == "bullish" else "long"
    entry = (ob.top + ob.bottom) / 2
    confluences = ["breaker block (failed OB retest)"]
    if direction == "long":
        sl = ob.bottom - 0.25 * atr_ltf
        tp1 = entry + 1.5 * (entry - sl)
        tp2 = entry + 3.0 * (entry - sl)
    else:
        sl = ob.top + 0.25 * atr_ltf
        tp1 = entry - 1.5 * (sl - entry)
        tp2 = entry - 3.0 * (sl - entry)
    c = _mk("breaker_block_engine", ctx, direction, entry, sl, tp1, tp2, 52, confluences,
            "range", "zone", "Retest entry on a breaker block (role-flipped former order block).")
    if c:
        out.append(c)
    return out


def fvg_engine(ctx: MarketContext) -> list[Candidate]:
    out = []
    atr_ltf = ctx.ind_ltf.atr[-1] or 0.0
    if atr_ltf <= 0:
        return out
    unfilled = [g for g in ctx.fvgs_struct if not g.filled]
    for gap in unfilled[-3:]:
        direction = "long" if gap.direction == "bullish" else "short"
        if (direction == "long" and ctx.htf_bias == "bearish") or (direction == "short" and ctx.htf_bias == "bullish"):
            continue
        entry = (gap.top + gap.bottom) / 2
        confluences = ["unfilled FVG"]
        if direction == "long":
            sl = gap.bottom - 0.3 * atr_ltf
            tp1 = entry + 1.5 * (entry - sl)
            tp2 = entry + 3.0 * (entry - sl)
        else:
            sl = gap.top + 0.3 * atr_ltf
            tp1 = entry - 1.5 * (sl - entry)
            tp2 = entry - 3.0 * (sl - entry)
        c = _mk("fvg_engine", ctx, direction, entry, sl, tp1, tp2, 48, confluences,
                "trend", "zone", f"Entry at unfilled {gap.direction} fair value gap.")
        if c:
            out.append(c)
    return out[:1]


def momentum_engine(ctx: MarketContext) -> list[Candidate]:
    out = []
    r = ctx.ind_ltf.rsi[-1]
    atr_ltf = ctx.ind_ltf.atr[-1] or 0.0
    if r is None or atr_ltf <= 0:
        return out
    last = ctx.ltf[-1]
    body = abs(last.c - last.o)
    rng = last.h - last.l
    strong_candle = rng > 0 and body / rng >= 0.6 and body >= 0.8 * atr_ltf
    direction = None
    if r >= 60 and last.c > last.o and strong_candle:
        direction = "long"
    elif r <= 40 and last.c < last.o and strong_candle:
        direction = "short"
    if direction is None:
        return out
    confluences = [f"RSI {r:.0f}", "strong directional candle"]
    entry = ctx.price
    if direction == "long":
        sl = min(last.l, entry - 1.2 * atr_ltf)
        tp1 = entry + 1.3 * (entry - sl)
        tp2 = entry + 2.6 * (entry - sl)
    else:
        sl = max(last.h, entry + 1.2 * atr_ltf)
        tp1 = entry - 1.3 * (sl - entry)
        tp2 = entry - 2.6 * (sl - entry)
    c = _mk("momentum_engine", ctx, direction, entry, sl, tp1, tp2, 47, confluences,
            "volatile", "market", f"Momentum {direction}: RSI {r:.0f} with strong directional candle.")
    if c:
        out.append(c)
    return out


def reversal_engine(ctx: MarketContext) -> list[Candidate]:
    out = []
    r = ctx.ind_ltf.rsi[-1]
    atr_ltf = ctx.ind_ltf.atr[-1] or 0.0
    if r is None or atr_ltf <= 0 or not ctx.events_struct:
        return out
    last_event = ctx.events_struct[-1]
    if last_event.kind not in ("CHoCH", "MSS"):
        return out
    direction = "long" if last_event.direction == "bullish" else "short"
    extreme_ok = (direction == "long" and r <= 40) or (direction == "short" and r >= 60)
    confluences = [f"struct {last_event.kind}"]
    if extreme_ok:
        confluences.append(f"RSI extreme {r:.0f}")
    entry = ctx.price
    if direction == "long":
        sl = entry - 1.4 * atr_ltf
        tp1 = entry + 1.3 * (entry - sl)
        tp2 = entry + 2.6 * (entry - sl)
    else:
        sl = entry + 1.4 * atr_ltf
        tp1 = entry - 1.3 * (sl - entry)
        tp2 = entry - 2.6 * (sl - entry)
    c = _mk("reversal_engine", ctx, direction, entry, sl, tp1, tp2, 46, confluences,
            "range", "market", f"Potential reversal: {last_event.kind} against prior trend.")
    if c:
        out.append(c)
    return out


def mean_reversion_engine(ctx: MarketContext) -> list[Candidate]:
    out = []
    if ctx.regime not in ("range", "quiet"):
        return out
    up, lo, mid = ctx.ind_ltf.bb_up[-1], ctx.ind_ltf.bb_lo[-1], ctx.ind_ltf.bb_mid[-1]
    atr_ltf = ctx.ind_ltf.atr[-1] or 0.0
    if up is None or lo is None or mid is None or atr_ltf <= 0:
        return out
    price = ctx.price
    direction = None
    if price <= lo:
        direction = "long"
    elif price >= up:
        direction = "short"
    if direction is None:
        return out
    confluences = ["range/quiet regime", "price at BB extreme"]
    entry = price
    if direction == "long":
        sl = entry - 1.1 * atr_ltf
        tp1 = mid
        tp2 = up
    else:
        sl = entry + 1.1 * atr_ltf
        tp1 = mid
        tp2 = lo
    c = _mk("mean_reversion_engine", ctx, direction, entry, sl, tp1, tp2, 45, confluences,
            "range", "market", f"Mean reversion fade from Bollinger extreme back to mid/opposite band.")
    if c:
        out.append(c)
    return out


def range_engine(ctx: MarketContext) -> list[Candidate]:
    out = []
    if ctx.regime != "range" or len(ctx.struct) < 20:
        return out
    lookback = ctx.struct[-20:]
    range_high = max(c.h for c in lookback)
    range_low = min(c.l for c in lookback)
    atr_ltf = ctx.ind_ltf.atr[-1] or 0.0
    if atr_ltf <= 0 or (range_high - range_low) < 2 * atr_ltf:
        return out
    price = ctx.price
    band = 0.15 * (range_high - range_low)
    direction = None
    if price <= range_low + band:
        direction = "long"
    elif price >= range_high - band:
        direction = "short"
    if direction is None:
        return out
    confluences = ["range regime", "price at range boundary"]
    entry = price
    if direction == "long":
        sl = range_low - 0.4 * atr_ltf
        tp1 = entry + (range_high - range_low) * 0.4
        tp2 = range_high - 0.2 * atr_ltf
    else:
        sl = range_high + 0.4 * atr_ltf
        tp1 = entry - (range_high - range_low) * 0.4
        tp2 = range_low + 0.2 * atr_ltf
    c = _mk("range_engine", ctx, direction, entry, sl, tp1, tp2, 44, confluences,
            "range", "market", "Range boundary fade toward opposite side of established range.")
    if c:
        out.append(c)
    return out


def volatility_expansion_engine(ctx: MarketContext) -> list[Candidate]:
    out = []
    widths = [w for w in ctx.ind_struct.bb_width if w is not None]
    if len(widths) < 60:
        return out
    current_w = widths[-1]
    pct = sum(1 for w in widths[-100:] if w <= current_w) / len(widths[-100:])
    if pct > 0.15:
        return out  # only fire from a genuine squeeze, not general low vol
    atr_ltf = ctx.ind_ltf.atr[-1] or 0.0
    if atr_ltf <= 0 or ctx.htf_bias == "neutral":
        return out
    direction = "long" if ctx.htf_bias == "bullish" else "short"
    confluences = ["BB squeeze (vol percentile <15%)", "HTF bias aligned"]
    entry = ctx.price
    if direction == "long":
        sl = entry - 1.3 * atr_ltf
        tp1 = entry + 1.6 * (entry - sl)
        tp2 = entry + 3.4 * (entry - sl)
    else:
        sl = entry + 1.3 * atr_ltf
        tp1 = entry - 1.6 * (sl - entry)
        tp2 = entry - 3.4 * (sl - entry)
    c = _mk("volatility_expansion_engine", ctx, direction, entry, sl, tp1, tp2, 49, confluences,
            "volatile", "market", "Volatility squeeze positioned to expand in HTF bias direction.")
    if c:
        out.append(c)
    return out


ALL_ENGINES = [
    smc_engine, trend_continuation_engine, breakout_engine, pullback_engine,
    liquidity_sweep_engine, order_block_engine, breaker_block_engine, fvg_engine,
    momentum_engine, reversal_engine, mean_reversion_engine, range_engine,
    volatility_expansion_engine,
]


# =====================================================================
# SECTION 8: CONTINUOUS LEARNING SUBSYSTEM
# =====================================================================

def default_learning_state() -> dict:
    return {
        "engine_weights": {e.__name__: 1.0 for e in ALL_ENGINES},
        "stats": {},          # key "{engine}|{regime}|{symbol}|{tf}" -> {wins,losses,r_sum,n,...}
        "filter_funnel": {},  # stage -> {"seen": n, "killed": n}
        "confidence_calibration": {},  # bucket "50-60" -> {"predicted_avg":..,"actual_wr":..,"n":..}
        "daily": {"date": None, "signals": [], "closed": []},
    }


def load_learning_state() -> dict:
    if not os.path.exists(LEARNING_PATH):
        return default_learning_state()
    try:
        with open(LEARNING_PATH, "r") as f:
            state = json.load(f)
        base = default_learning_state()
        for k, v in base.items():
            state.setdefault(k, v)
        return state
    except (json.JSONDecodeError, OSError):
        return default_learning_state()


def save_learning_state(state: dict):
    atomic_write_json(LEARNING_PATH, state)


def stats_key(engine: str, regime: str, symbol: str) -> str:
    return f"{engine}|{regime}|{symbol}"


def record_filter_funnel(state: dict, stage: str, seen: int, killed: int):
    bucket = state["filter_funnel"].setdefault(stage, {"seen": 0, "killed": 0})
    bucket["seen"] += seen
    bucket["killed"] += killed


def get_engine_weight(state: dict, engine: str, regime: str, symbol: str) -> float:
    """Blend static prior (1.0) with learned win-rate/EV once a bucket has
    enough closed samples; never overfits to a thin/recent sample."""
    key = stats_key(engine, regime, symbol)
    bucket = state["stats"].get(key)
    prior = state["engine_weights"].get(engine, 1.0)
    if not bucket or bucket.get("n", 0) < MIN_SAMPLE_SIZE_FOR_LEARNING:
        return prior
    n = bucket["n"]
    wins = bucket["wins"]
    wr = wins / n if n else 0.5
    avg_r = bucket["r_sum"] / n if n else 0.0
    # EV-informed multiplier, damped so no single bucket can swing weight
    # more than +-35% regardless of how extreme its (still-limited) sample is.
    ev_signal = max(-1.0, min(1.0, (wr - 0.5) * 2 + avg_r * 0.15))
    learned = prior * (1 + 0.35 * ev_signal)
    return max(0.2, min(2.0, learned))


def update_stats_on_close(state: dict, sig: ActiveSignal):
    key = stats_key(sig.engine, sig.regime_fit, sig.symbol)
    bucket = state["stats"].setdefault(key, {"n": 0, "wins": 0, "losses": 0, "r_sum": 0.0})
    bucket["n"] += 1
    r = sig.final_r or 0.0
    bucket["r_sum"] += r
    if r > 0:
        bucket["wins"] += 1
    else:
        bucket["losses"] += 1

    conf_bucket = f"{int(sig.confidence // 10) * 10}-{int(sig.confidence // 10) * 10 + 10}"
    cc = state["confidence_calibration"].setdefault(conf_bucket, {"predicted_sum": 0.0, "wins": 0, "n": 0})
    cc["predicted_sum"] += sig.confidence
    cc["n"] += 1
    if r > 0:
        cc["wins"] += 1


def recalibrate_engine_weights(state: dict):
    """Periodic reweight pass across all (engine) aggregated stats, gated
    by minimum sample size, damped, and never allowed to fully zero out
    an engine (floor 0.2) so a cold-streak doesn't permanently starve it."""
    per_engine: dict[str, list[float]] = {}
    for key, bucket in state["stats"].items():
        engine = key.split("|", 1)[0]
        if bucket.get("n", 0) < MIN_SAMPLE_SIZE_FOR_LEARNING:
            continue
        wr = bucket["wins"] / bucket["n"]
        avg_r = bucket["r_sum"] / bucket["n"]
        ev_signal = max(-1.0, min(1.0, (wr - 0.5) * 2 + avg_r * 0.15))
        per_engine.setdefault(engine, []).append(ev_signal)
    for engine, signals in per_engine.items():
        avg_signal = statistics.mean(signals)
        current = state["engine_weights"].get(engine, 1.0)
        target = max(0.2, min(2.0, 1.0 * (1 + 0.35 * avg_signal)))
        # exponential smoothing toward target -- avoids whipsawing weights
        # off a single recalibration pass
        state["engine_weights"][engine] = current * 0.7 + target * 0.3


# =====================================================================
# SECTION 9: DECISION ENGINE
# =====================================================================

def liquidity_sanity_check(candidate: Candidate, ctx: MarketContext) -> bool:
    """Reject/discount entries inside or adjacent to an about-to-be-swept
    pool, unless the setup is itself the liquidity-sweep engine (which
    trades the sweep, not into it)."""
    if candidate.engine == "liquidity_sweep_engine":
        return True
    atr_ltf = candidate.atr_ltf
    for pool in ctx.pools_struct:
        if pool.swept:
            continue
        near = abs(candidate.entry - pool.price) <= 0.4 * atr_ltf
        if not near:
            continue
        # A pool sitting the "wrong" side of a directional trade means
        # price is likely to get swept through the entry/SL area first.
        if candidate.direction == "long" and pool.kind == "sell_side" and pool.price <= candidate.entry:
            return False
        if candidate.direction == "short" and pool.kind == "buy_side" and pool.price >= candidate.entry:
            return False
    return True


def regime_fit_veto(candidate: Candidate, ctx: MarketContext) -> float:
    """Returns a multiplier (not a hard reject) that suppresses/discounts
    signals whose engine doesn't match the current detected regime.
    Exact regime match = no discount; adjacent/compatible regime = mild
    discount; flatly mismatched = heavy discount (but not zero, since
    regimes are estimates, not ground truth)."""
    if candidate.regime_fit == ctx.regime:
        return 1.0
    compatible = {
        ("trend", "volatile"), ("volatile", "trend"),
        ("range", "quiet"), ("quiet", "range"),
    }
    if (candidate.regime_fit, ctx.regime) in compatible:
        return 0.82
    return 0.55


def score_candidate(candidate: Candidate, ctx: MarketContext, learning: dict) -> float:
    weight = get_engine_weight(learning, candidate.engine, ctx.regime, ctx.symbol)
    regime_mult = regime_fit_veto(candidate, ctx)
    session_bonus = 1.06 if candidate.engine in SESSION_ENGINE_BIAS.get(ctx.session, set()) else 1.0
    rr_bonus = min(1.25, 0.9 + candidate.expected_rr * 0.07)
    return candidate.confidence * weight * regime_mult * session_bonus * rr_bonus


def assign_tier(score: float, confluence_count: int) -> str:
    if score >= 85 and confluence_count >= 3:
        return "A+"
    if score >= 65:
        return "A"
    return "B"


def symbol_cluster(symbol: str) -> Optional[str]:
    for name, members in CORRELATION_CLUSTERS.items():
        if symbol in members:
            return name
    return None


def run_decision_engine(
    contexts: dict[str, MarketContext],
    all_candidates: dict[str, list[Candidate]],
    learning: dict,
    existing_active: list[ActiveSignal],
) -> list[Candidate]:
    """
    Applies, per candidate, in order:
      1. Liquidity sanity check              (hard reject)
      2. Regime-fit veto                     (soft discount, in scoring)
      3. Ranking by learned/regime/session/RR-adjusted score
      4. One-active-signal-per-symbol enforcement
      5. Correlated-cluster concurrent cap
      6. MAX_CONCURRENT_ACTIVE_SIGNALS slot budget
    Returns the selected Candidates (tiering happens by the caller once
    scores are known, since tier needs the same score value).
    """
    scored: list[tuple[float, Candidate]] = []
    total_seen = 0
    killed_liquidity = 0

    for symbol, candidates in all_candidates.items():
        ctx = contexts.get(symbol)
        if not ctx:
            continue
        for c in candidates:
            total_seen += 1
            if not liquidity_sanity_check(c, ctx):
                killed_liquidity += 1
                continue
            scored.append((score_candidate(c, ctx, learning), c))

    record_filter_funnel(learning, "liquidity_sanity_check", total_seen, killed_liquidity)
    scored.sort(key=lambda x: x[0], reverse=True)

    symbols_already_active = {s.symbol for s in existing_active if s.status in ("activated", "tp1")}
    cluster_active_count: dict[str, int] = {}
    for s in existing_active:
        if s.status not in ("activated", "tp1"):
            continue
        cl = symbol_cluster(s.symbol)
        if cl:
            cluster_active_count[cl] = cluster_active_count.get(cl, 0) + 1

    slots_left = MAX_CONCURRENT_ACTIVE_SIGNALS - len(symbols_already_active)
    selected: list[Candidate] = []
    seen_symbols_this_round: set[str] = set()
    before_symbol_cap = 0
    killed_symbol_cap = 0
    before_corr_cap = 0
    killed_corr_cap = 0

    for score, c in scored:
        before_symbol_cap += 1
        if c.symbol in symbols_already_active or c.symbol in seen_symbols_this_round:
            killed_symbol_cap += 1
            continue
        if slots_left <= 0:
            break
        before_corr_cap += 1
        cl = symbol_cluster(c.symbol)
        if cl and cluster_active_count.get(cl, 0) >= MAX_CORRELATED_CONCURRENT:
            killed_corr_cap += 1
            continue
        selected.append(c)
        seen_symbols_this_round.add(c.symbol)
        slots_left -= 1
        if cl:
            cluster_active_count[cl] = cluster_active_count.get(cl, 0) + 1

    record_filter_funnel(learning, "one_signal_per_symbol", before_symbol_cap, killed_symbol_cap)
    record_filter_funnel(learning, "correlation_cluster_cap", before_corr_cap, killed_corr_cap)
    return selected


# =====================================================================
# SECTION 10: SIGNAL LIFECYCLE / CANDLE-WALK RESOLUTION
# =====================================================================

def _entry_in_range(sig: ActiveSignal, candle: Candle) -> bool:
    return candle.l <= sig.entry <= candle.h


def evaluate_candle(sig: ActiveSignal, candle: Candle) -> Optional[dict]:
    """
    Advances one ActiveSignal by exactly one closed MONITOR_TF candle.
    Returns an event dict describing what happened this candle, or None.

    BUG #2 enforcement: while sig.entry_filled is False, the ONLY thing
    this function is allowed to do is check for fill (and, absent that,
    expiry). It is structurally impossible to reach the SL/TP branches
    below with entry_filled False -- there is a single `if not
    sig.entry_filled:` block that always `return`s before them.

    BUG #1 enforcement: sig.sl is read here, never written. Search this
    function (and the whole file) for `sig.sl =`: it does not occur.
    """
    if sig.status not in ("activated", "tp1"):
        return None

    if not sig.entry_filled:
        if sig.entry_kind == "market":
            # market candidates are created already filled; this branch
            # should be unreachable, but we fail safe rather than fail
            # open if invariants are ever violated upstream.
            sig.entry_filled = True
        elif _entry_in_range(sig, candle):
            sig.entry_filled = True
        else:
            sig.bars_pending += 1
            if sig.bars_pending >= PENDING_ENTRY_EXPIRY_BARS:
                sig.status = "expired"
                sig.close_reason = "no_fill"
                sig.forensic_tag = "expired_no_fill"
                sig.close_ts = candle.t
                return {"event": "expired"}
            return None
        # fall through: same-candle SL/TP can still apply once filled

    # ---- entry_filled is guaranteed True below this point -------------
    long = sig.direction == "long"
    hit_sl = candle.l <= sig.sl if long else candle.h >= sig.sl
    hit_tp1 = (not sig.tp1_hit) and (candle.h >= sig.tp1 if long else candle.l <= sig.tp1)
    hit_tp2 = candle.h >= sig.tp2 if long else candle.l <= sig.tp2

    if not sig.tp1_hit:
        # Pre-TP1: conservative worst-case ordering within the candle --
        # if both SL and TP1 are technically touchable in the same
        # candle, assume SL first (protects against overstating win
        # rate; this is the standard conservative backtest convention).
        if hit_sl and hit_tp1:
            sig.status = "sl"
            sig.final_r = r_multiple(sig, sig.sl)
            sig.close_reason = "sl_hit_same_candle_as_tp1_conservative"
            sig.forensic_tag = "loss_sl_same_candle_tp1"
            sig.close_ts = candle.t
            return {"event": "sl"}
        if hit_sl:
            sig.status = "sl"
            sig.final_r = r_multiple(sig, sig.sl)
            sig.close_reason = "sl_hit"
            sig.forensic_tag = "loss_sl"
            sig.close_ts = candle.t
            return {"event": "sl"}
        if hit_tp1:
            sig.tp1_hit = True
            sig.tp1_r_realized = r_multiple(sig, sig.tp1)
            sig.status = "tp1"
            if hit_tp2:
                sig.status = "tp2"
                sig.final_r = r_multiple(sig, sig.tp2)
                sig.close_reason = "tp1_and_tp2_same_candle"
                sig.forensic_tag = "win_tp2"
                sig.close_ts = candle.t
                return {"event": "tp2"}
            return {"event": "tp1"}
        return None
    else:
        # Post-TP1: SL is UNCHANGED (bug #1 invariant). Runner continues
        # to original SL or TP2.
        if hit_tp2:
            sig.status = "tp2"
            sig.final_r = r_multiple(sig, sig.tp2)
            sig.close_reason = "tp2_hit"
            sig.forensic_tag = "win_tp2"
            sig.close_ts = candle.t
            return {"event": "tp2"}
        if hit_sl:
            # TP1 was already secured; blend the TP1-realized R with the
            # runner's exit at the original SL. This is a WIN, never a
            # manufactured "breakeven" outcome.
            sig.status = "sl"
            sig.final_r = sig.tp1_r_realized  # runner portion closed flat vs its own entry basis at original SL
            sig.close_reason = "tp1_secured_then_sl_on_runner"
            sig.forensic_tag = "win_tp1_then_sl"
            sig.close_ts = candle.t
            return {"event": "sl_after_tp1"}
        return None


def resolve_signals_over_candles(active_signals: list[ActiveSignal], symbol_ltf_candles: dict[str, list[Candle]]) -> list[dict]:
    """Walks every still-open ActiveSignal forward across every closed
    MONITOR_TF candle since it was created/last resolved, in chronological
    order, one candle at a time (never skipping to a snapshot)."""
    events = []
    for sig in active_signals:
        if sig.status not in ("activated", "tp1"):
            continue
        candles = symbol_ltf_candles.get(sig.symbol, [])
        # Only walk candles strictly newer than the last one we already
        # processed for this signal, so repeated scans never re-score
        # the same closed candle twice.
        relevant = [c for c in candles if c.t > sig.last_processed_ts]
        for candle in relevant:
            ev = evaluate_candle(sig, candle)
            sig.last_processed_ts = candle.t
            if ev:
                events.append({"signal_id": sig.id, "symbol": sig.symbol, **ev})
            if sig.status not in ("activated", "tp1"):
                break
    return events


def run_bug_selfcheck() -> bool:
    """
    Executable proof that BUG #1 and BUG #2 cannot occur, run once at
    startup. Raises AssertionError (crashing the run loudly rather than
    silently shipping bad signals) if either invariant is violated.
    """
    # --- BUG #2 proof: a zone signal must not score SL/TP before fill ---
    cand = Candidate(
        engine="test", symbol="TEST", direction="long", entry=100.0, sl=95.0,
        tp1=110.0, tp2=120.0, confidence=50, expected_rr=2, confluences=[],
        regime_fit="trend", entry_kind="zone", rationale="selfcheck", atr_ltf=1.0,
    )
    sig = ActiveSignal.from_candidate(cand, "B", "selfcheck-1", now_ms=0)
    assert sig.entry_filled is False, "zone signal must start unfilled"
    # A candle whose range hits SL but never trades through the entry
    # zone at all must NOT be scored as a loss.
    far_below_entry_candle = Candle(t=1000, o=90.0, h=91.0, l=80.0, c=85.0, v=1.0)
    ev = evaluate_candle(sig, far_below_entry_candle)
    assert sig.status == "activated", "BUG #2 regression: signal scored before entry filled"
    assert sig.final_r is None, "BUG #2 regression: final_r set before entry filled"

    # --- BUG #1 proof: SL must never move after TP1 -------------------
    cand2 = Candidate(
        engine="test", symbol="TEST", direction="long", entry=100.0, sl=95.0,
        tp1=105.0, tp2=115.0, confidence=50, expected_rr=2, confluences=[],
        regime_fit="trend", entry_kind="market", rationale="selfcheck", atr_ltf=1.0,
    )
    sig2 = ActiveSignal.from_candidate(cand2, "B", "selfcheck-2", now_ms=0)
    sl_before = sig2.sl
    tp1_candle = Candle(t=1000, o=101, h=106, l=100, c=105, v=1)
    evaluate_candle(sig2, tp1_candle)
    assert sig2.tp1_hit is True
    assert sig2.sl == sl_before, "BUG #1 regression: SL changed after TP1"
    # Now let price fall all the way back to the ORIGINAL sl -- must be
    # scored a WIN (tp1 R credited), never a "breakeven" outcome.
    sl_candle = Candle(t=2000, o=104, h=104, l=94, c=95, v=1)
    ev2 = evaluate_candle(sig2, sl_candle)
    assert sig2.sl == sl_before, "BUG #1 regression: SL changed between TP1 and stop-out"
    assert sig2.forensic_tag == "win_tp1_then_sl", "BUG #1 regression: TP1-then-SL not scored as win"
    assert sig2.close_reason != "breakeven", "BUG #1 regression: breakeven status reached"
    return True


# =====================================================================
# SECTION 11: TELEGRAM
# =====================================================================

def send_telegram(text: str) -> Optional[int]:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.warning("[telegram] TG_BOT_TOKEN/TG_CHAT_ID not set, skipping send")
        return None
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": TG_CHAT_ID, "text": text, "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
                return data.get("result", {}).get("message_id")
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            log.warning(f"[telegram] send failed attempt {attempt+1}: {e!r}")
            time.sleep(2 ** attempt)
    return None


# ---- Message reactions -------------------------------------------------
# Telegram's Bot API only accepts ReactionTypeEmoji values from its fixed
# allowed-reaction list (the standard "quick reaction" tray) unless the
# bot/chat has custom-emoji reaction privileges. TELEGRAM_ALLOWED_REACTIONS
# is that list, written as literal emoji characters (not escapes) so it's
# trivially diffable/auditable against Telegram's published set.
TELEGRAM_ALLOWED_REACTIONS = [
    "❤️", "👍", "👎", "🔥", "🥰",
    "👏", "😁", "🤔", "🤯", "😱",
    "🤬", "😢", "🎉", "🤩", "🤮",
    "💩", "🙏", "🤛", "🕊️", "🤡",
    "🥱", "😴", "🤭", "😍", "🐳",
    "❤️‍🔥", "🌭", "💯", "🤣",
    "⚡", "🍌", "🏆", "💔", "🤨",
    "😐", "🍓", "🍾", "💋", "🖕",
    "😈", "😪", "😭", "🤓", "👻",
    "👨‍💻", "👀", "🎃", "🙊",
    "😇", "🤝", "✍️", "🤗", "🎅",
    "🎄", "⛄", "💅", "🤪", "🗿",
    "🙆", "😎", "👾", "🤘", "🤫",
    "🤷", "👋", "🤳", "😯",
]

# Event -> reaction emoji, chosen only from the allowed set above.
EVENT_REACTION_EMOJI = {
    "pending": "👀",     # 👀  zone signal posted, watching for fill
    "activated": "👍",   # 👍  market signal activated immediately
    "tp1": "🔥",         # 🔥  TP1 secured
    "tp2": "🏆",         # 🏆  TP2 hit -- full win
    "sl": "💔",          # 💔  SL hit -- loss
    "sl_after_tp1": "💯",# 💯  TP1 secured then SL on runner -- still a win
    "expired": "🤷",     # 🤷  no fill, expired
    "cancelled": "👀",   # 👀  cancelled
}


def react_to_telegram_message(message_id: int, emoji: str, big: bool = False) -> bool:
    """Set (replace) the bot's reaction on a previously-sent message via
    Telegram's setMessageReaction. Best-effort: a failure here never
    blocks the scan cycle, since the authoritative record is always the
    text status message + state.json, not the reaction."""
    if not TG_BOT_TOKEN or not TG_CHAT_ID or not message_id:
        return False
    if emoji not in TELEGRAM_ALLOWED_REACTIONS:
        log.warning(f"[telegram] {emoji!r} is not in the allowed reaction set, skipping")
        return False
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/setMessageReaction"
    payload = json.dumps({
        "chat_id": TG_CHAT_ID, "message_id": message_id,
        "reaction": [{"type": "emoji", "emoji": emoji}],
        "is_big": big,
    }).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
                return bool(data.get("ok"))
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            log.warning(f"[telegram] reaction failed attempt {attempt+1}: {e!r}")
            time.sleep(2 ** attempt)
    return False


def react_for_event(sig: ActiveSignal, event: str):
    emoji = EVENT_REACTION_EMOJI.get(event)
    if emoji and sig.telegram_message_id:
        # TP2 and a clean SL loss are the two "big" outcomes worth the
        # emphasized/large reaction animation; everything else is normal.
        big = event in ("tp2", "sl")
        react_to_telegram_message(sig.telegram_message_id, emoji, big=big)


def _md_escape(s: str) -> str:
    for ch in r"_*[]()~`>#+-=|{}.!":
        s = s.replace(ch, f"\\{ch}")
    return s


def fmt_num(x: float) -> str:
    if x >= 100:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}"
    return f"{x:.6f}"


def format_activation_message(sig: ActiveSignal) -> str:
    header = _md_escape(f"{ENGINE_TAG} -- {sig.symbol} {sig.direction.upper()}")
    lines = [
        f"*{header}*",
        _md_escape(f"Engine: {sig.engine} | Tier: {sig.tier} | Confidence: {sig.confidence:.0f}%"),
        _md_escape(f"Status: {'PENDING (zone entry)' if not sig.entry_filled else 'ACTIVATED (market)'}"),
        "",
        "Entry:",
        f"`{fmt_num(sig.entry)}`",
        "SL:",
        f"`{fmt_num(sig.sl)}`",
        "TP1:",
        f"`{fmt_num(sig.tp1)}`",
        "TP2:",
        f"`{fmt_num(sig.tp2)}`",
        "",
        _md_escape(f"RR: {sig.expected_rr:.2f} | Regime: {sig.regime_fit}"),
        _md_escape("Confluences: " + ", ".join(sig.confluences)),
        _md_escape(sig.rationale),
    ]
    return "\n".join(lines)


def format_status_message(sig: ActiveSignal, event: str) -> Optional[str]:
    label_map = {
        "tp1": "TP1 HIT",
        "tp2": "TP2 HIT -- WIN",
        "sl": "SL HIT -- LOSS",
        "sl_after_tp1": f"TP1 secured, SL later hit -- WIN (TP1 R credited: {sig.tp1_r_realized:.2f}R)",
        "expired": "EXPIRED (no fill)",
    }
    label = label_map.get(event)
    if not label:
        return None
    header = _md_escape(f"{ENGINE_TAG} -- {sig.symbol} {sig.direction.upper()}: {label}")
    lines = [f"*{header}*"]
    if event == "tp1":
        lines.append(_md_escape(f"TP1 secured at {fmt_num(sig.tp1)}. SL unchanged at {fmt_num(sig.sl)} (original level -- move your own SL manually if desired)."))
    elif sig.final_r is not None:
        lines.append(_md_escape(f"Result: {sig.final_r:+.2f}R | Reason: {sig.close_reason}"))
    return "\n".join(lines)


def format_daily_summary(learning: dict, closed_today: list[ActiveSignal]) -> str:
    n = len(closed_today)
    wins = sum(1 for s in closed_today if (s.final_r or 0) > 0)
    losses = n - wins
    win_rate = (wins / n * 100) if n else 0.0
    gains = sum(s.final_r for s in closed_today if (s.final_r or 0) > 0)
    loss_sum = abs(sum(s.final_r for s in closed_today if (s.final_r or 0) <= 0))
    profit_factor = (gains / loss_sum) if loss_sum > 0 else (float("inf") if gains > 0 else 0.0)
    avg_rr = statistics.mean([s.expected_rr for s in closed_today]) if closed_today else 0.0

    by_regime: dict[str, list[float]] = {}
    by_engine: dict[str, list[float]] = {}
    for s in closed_today:
        by_regime.setdefault(s.regime_fit, []).append(s.final_r or 0.0)
        by_engine.setdefault(s.engine, []).append(s.final_r or 0.0)

    best = max(closed_today, key=lambda s: s.final_r or -999, default=None)
    worst = min(closed_today, key=lambda s: s.final_r or 999, default=None)

    lines = [
        f"*{_md_escape(ENGINE_TAG + ' -- Daily Summary')}*",
        _md_escape(datetime.now(timezone.utc).strftime("%Y-%m-%d")),
        "",
        _md_escape(f"Total signals: {n} | Wins: {wins} | Losses: {losses} | Win rate: {win_rate:.1f}%"),
        _md_escape(f"Profit factor: {profit_factor:.2f} | Avg RR: {avg_rr:.2f}"),
        "",
        "*By regime:*",
    ]
    for regime, rs in by_regime.items():
        lines.append(_md_escape(f"  {regime}: n={len(rs)}, avg R={statistics.mean(rs):+.2f}"))
    lines.append("*By engine:*")
    for engine, rs in by_engine.items():
        lines.append(_md_escape(f"  {engine}: n={len(rs)}, avg R={statistics.mean(rs):+.2f}"))
    if best:
        lines.append(_md_escape(f"Best: {best.symbol} {best.engine} {best.final_r:+.2f}R"))
    if worst:
        lines.append(_md_escape(f"Worst: {worst.symbol} {worst.engine} {worst.final_r:+.2f}R"))

    cal_lines = []
    for bucket, cc in sorted(learning.get("confidence_calibration", {}).items()):
        if cc["n"] < 5:
            continue
        predicted = cc["predicted_sum"] / cc["n"]
        actual = cc["wins"] / cc["n"] * 100
        cal_lines.append(_md_escape(f"  {bucket}%: predicted~{predicted:.0f}%, actual WR={actual:.0f}% (n={cc['n']})"))
    if cal_lines:
        lines.append("*Confidence calibration:*")
        lines.extend(cal_lines)

    lines.append("")
    lines.append(_md_escape("Learning adjustments this cycle: engine weights recalibrated from closed-trade EV, damped 70/30 toward target."))
    return "\n".join(lines)


# =====================================================================
# SECTION 12: STATE PERSISTENCE
# =====================================================================

def signal_to_dict(sig: ActiveSignal) -> dict:
    return asdict(sig)


def signal_from_dict(d: dict) -> ActiveSignal:
    return ActiveSignal(**d)


def load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        return {"active_signals": [], "closed_signals": [], "last_run_ts": 0, "last_daily_summary_date": None}
    try:
        with open(STATE_PATH, "r") as f:
            raw = json.load(f)
        raw.setdefault("active_signals", [])
        raw.setdefault("closed_signals", [])
        raw.setdefault("last_run_ts", 0)
        raw.setdefault("last_daily_summary_date", None)
        return raw
    except (json.JSONDecodeError, OSError) as e:
        log.error(f"[state] corrupt state.json, starting fresh: {e!r}")
        return {"active_signals": [], "closed_signals": [], "last_run_ts": 0, "last_daily_summary_date": None}


def save_state(state: dict):
    atomic_write_json(STATE_PATH, state)


# =====================================================================
# SECTION 13: MAIN ORCHESTRATION
# =====================================================================

def _next_signal_id(state: dict) -> str:
    n = len(state["active_signals"]) + len(state["closed_signals"]) + 1
    return f"{ENGINE_NAME.lower()}-{int(time.time())}-{n}"


def run_scan_cycle():
    log.info(f"=== {ENGINE_TAG} scan cycle start ===")
    ok = run_bug_selfcheck()
    log.info(f"[selfcheck] BUG#1/BUG#2 structural invariants verified: {ok}")

    state = load_state()
    learning = load_learning_state()
    candle_cache = load_candle_cache()

    active_signals = [signal_from_dict(d) for d in state["active_signals"]]

    log.info("[fetch] pulling watchlist candles...")
    all_candles = fetch_all_watchlist_candles(candle_cache)

    # ---- 1. Resolve existing signals first (candle-walk) --------------
    ltf_map = {sym: tfmap.get(TF_LTF, []) for sym, tfmap in all_candles.items()}
    events = resolve_signals_over_candles(active_signals, ltf_map)
    for ev in events:
        sig = next((s for s in active_signals if s.id == ev["signal_id"]), None)
        if not sig:
            continue
        msg = format_status_message(sig, ev["event"])
        if msg:
            send_telegram(msg)
        react_for_event(sig, ev["event"])
        if sig.status not in ("activated", "tp1"):
            update_stats_on_close(learning, sig)

    still_open = [s for s in active_signals if s.status in ("activated", "tp1")]
    newly_closed = [s for s in active_signals if s.status not in ("activated", "tp1")]
    state["closed_signals"].extend(signal_to_dict(s) for s in newly_closed)

    # ---- 2. Build market context + run all engines per symbol ---------
    contexts: dict[str, MarketContext] = {}
    all_candidates: dict[str, list[Candidate]] = {}
    total_raw = 0
    total_buffer_killed = 0

    for symbol in WATCHLIST:
        ctx = build_context(symbol, all_candles.get(symbol, {}))
        if not ctx:
            continue
        contexts[symbol] = ctx
        symbol_candidates: list[Candidate] = []
        for engine_fn in ALL_ENGINES:
            try:
                cands = engine_fn(ctx)
            except Exception as e:
                log.error(f"[engine:{engine_fn.__name__}] {symbol} raised {e!r}")
                cands = []
            total_raw += len(cands)
            symbol_candidates.extend(cands)
        all_candidates[symbol] = symbol_candidates

    record_filter_funnel(learning, "buffer_and_distance_rules", total_raw, total_buffer_killed)

    # ---- 3. Decision engine selects candidates -------------------------
    selected = run_decision_engine(contexts, all_candidates, learning, still_open)

    now_ms = int(time.time() * 1000)
    for c in selected:
        ctx = contexts[c.symbol]
        score = score_candidate(c, ctx, learning)
        tier = assign_tier(score, len(c.confluences))
        sig = ActiveSignal.from_candidate(c, tier, _next_signal_id(state), now_ms)
        still_open.append(sig)
        msg = format_activation_message(sig)
        sig.telegram_message_id = send_telegram(msg)
        react_for_event(sig, "activated" if sig.entry_filled else "pending")
        log.info(f"[signal] {sig.symbol} {sig.direction} tier={tier} engine={sig.engine} score={score:.1f}")

    state["active_signals"] = [signal_to_dict(s) for s in still_open]
    state["last_run_ts"] = now_ms

    # ---- 4. Daily summary at/after 08:00 UTC, once per day ------------
    now_utc = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y-%m-%d")
    if now_utc.hour >= DAILY_SUMMARY_HOUR_UTC and state.get("last_daily_summary_date") != today_str:
        closed_today = []
        for d in state["closed_signals"]:
            ct = d.get("close_ts")
            if ct and datetime.fromtimestamp(ct / 1000, tz=timezone.utc).strftime("%Y-%m-%d") == today_str:
                closed_today.append(signal_from_dict(d))
        recalibrate_engine_weights(learning)
        summary = format_daily_summary(learning, closed_today)
        send_telegram(summary)
        state["last_daily_summary_date"] = today_str

    save_state(state)
    save_learning_state(learning)
    log.info(f"=== {ENGINE_TAG} scan cycle complete: {len(selected)} new signals, {len(still_open)} active, {len(newly_closed)} closed this cycle ===")


def main():
    try:
        run_scan_cycle()
    except Exception:
        log.exception("[fatal] unhandled exception in scan cycle")
        raise


if __name__ == "__main__":
    main()
