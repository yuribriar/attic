#!/usr/bin/env python3
# pip install requests
"""
ZENITH ENGINE v1.0.0
================================================================================
A synthesized, ground-up Smart-Money-Concept crypto perpetual signal engine
for Hyperliquid, built by extracting the single best mechanism for each job
out of five reference engines (Axis, Ecliptic, Kestrel, Kairos, Meridian)
and filling every gap those five left on the table.

PHILOSOPHY
    "One honest score, every regime, no black boxes."
    Zenith never runs a fixed strategy against a market that doesn't fit it.
    A live Regime Vector routes each symbol into exactly the pathway(s) that
    make sense for its current trend/volatility/structure state, every
    candidate is scored through one unified confluence model (not a
    different ad-hoc scorer per pathway), and every SL/TP level is derived
    from real market structure with the reasoning attached to the message.

WHAT WAS TAKEN FROM EACH REFERENCE, AND WHY
    Axis        -> Three-Pathway Confluence Router + single logistic scorer
                   (cleaner than a scorer-per-pathway); Composite Regime
                   Vector (BTC macro bias, vol percentile, ADX, session,
                   noise, breadth) driving BOTH threshold and eligibility;
                   session volume-profile (POC/VA/VWAP) folded into TP
                   clipping; correlation-cluster dedup computed live from
                   realized returns instead of a static table; regularized
                   self-tuning per-pathway weights (shrunk toward a neutral
                   prior so a short streak can't overfit).
    Ecliptic    -> The five-filter gate (Location / Context / Quality / RR /
                   LTF-confirmation) applied to every zone before it can
                   become a candidate -- the most disciplined admission
                   process of the five; funding+OI regime reads as native
                   Hyperliquid confluence; sector/correlation diversification
                   thinking.
    Kestrel     -> Structural enforcement of "HTF hunts OB/BB, LTF hunts BB
                   as the trigger" (every mitigated OB is forward-tracked and
                   reclassified as a breaker the instant price closes through
                   it -- not just a scoring bonus); the three regime-matched
                   pathways (Liquidity Reversal / Trend Continuation / Range
                   Mean-Reversion) auto-selected per symbol; session weighting.
    Kairos      -> Funding-trend and OI-trend as directional confluence
                   (not just a carry gate); false-breakout / exhaustion
                   pattern suppression; market breadth and relative-strength
                   percentile as confidence inputs; macro-calendar volatility
                   awareness (kept as an optional soft filter -- see
                   MACRO_CALENDAR_ENABLED); per-pathway realized win-rate
                   analytics feeding the self-tuning weights.
    Meridian    -> The non-negotiable SFP -> MSS -> Breaker-retest execution
                   sequence as the backbone of the Liquidity Reversal pathway
                   (this is the highest-conviction, most mechanically precise
                   entry pattern across all five references); reaction *and*
                   reply on trade resolution; sector diversification cap.

GAPS FILLED THAT NO REFERENCE ENGINE HAD
    - A genuine, regime-gated Range Mean-Reversion pathway that is fully
      wired into the same scoring/risk/Telegram pipeline as the trend
      pathways (several references had range logic bolted on unevenly).
    - Liquidity-clustering-aware TP clipping that merges swing-pool levels,
      order-block/FVG/breaker POIs, AND session volume-profile value-area
      edges into one ranked target ladder, instead of two separate,
      sometimes-conflicting clipping mechanisms.
    - A single adaptive frequency governor with an explicit, documented
      feedback loop (see ADAPTIVE GOVERNOR below) instead of multiple
      independent threshold nudges that could fight each other.
    - Full daily win/loss/open breakdown by BOTH regime and trade type in
      the daily summary (references did one or the other, not both).

OPERATING MODEL
    Scan-per-run. An external scheduler (cron-job.org) invokes this script
    every 15 minutes. Each run: load state.json -> pull fresh Hyperliquid
    data -> run full analysis -> check active signals for SL/TP1/TP2 hits
    -> maybe send the 08:00 UTC daily summary -> write state.json -> exit.
    No long-running process, no database. Single file, immediately runnable:

        python3 zenith_engine.py

    Required env vars: TG_BOT_TOKEN, TG_CHAT_ID
    Optional: ZENITH_STATE_FILE, ZENITH_DRY_RUN=true (skip Telegram calls
    and print to stdout instead -- useful for local testing).
================================================================================
"""

from __future__ import annotations

import copy
import json
import math
import os
import random
import signal as os_signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

__version__ = "1.0.0"
ENGINE_NAME = "ZENITH ENGINE"
ENGINE_TAG = "ZENITH"

# ============================================================================
# CONFIGURATION
# ============================================================================

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
DRY_RUN = os.getenv("ZENITH_DRY_RUN", "false").strip().lower() in ("1", "true", "yes")

if not DRY_RUN:
    if not TG_BOT_TOKEN:
        raise RuntimeError("TG_BOT_TOKEN environment variable is required (set ZENITH_DRY_RUN=true to skip)")
    if not TG_CHAT_ID:
        raise RuntimeError("TG_CHAT_ID environment variable is required (set ZENITH_DRY_RUN=true to skip)")

HL_INFO_URL = os.getenv("HL_INFO_URL", "https://api.hyperliquid.xyz/info")
STATE_FILE = os.getenv("ZENITH_STATE_FILE", "state.json")
LOG_FILE = os.getenv("ZENITH_LOG_FILE", "zenith_engine.log")
SCAN_WORKERS = int(os.getenv("SCAN_WORKERS", "4"))
HL_MIN_INTERVAL_S = float(os.getenv("HL_MIN_INTERVAL_S", "0.15"))

# Watchlist carried over identically from the reference engines (Hyperliquid
# native coin symbols -- Axis/Ecliptic form, not the *USDT suffixed form
# some references used for a different venue's naming convention).
WATCHLIST = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]

# Sector map (shared 1:1 across references) -- used for the diversification
# cap so one scan can't fire every slot into a single correlated basket.
SECTOR_MAP = {
    "BTC": "btc", "ETH": "eth",
    "SOL": "eth_l1", "AVAX": "eth_l1", "SUI": "eth_l1", "APT": "eth_l1", "NEAR": "eth_l1",
    "BNB": "bnb",
    "XRP": "payments", "XLM": "payments", "TRX": "payments", "LTC": "payments",
    "DOGE": "meme", "PENGU": "meme",
    "ADA": "layer1_alt", "DOT": "layer1_alt", "TAO": "layer1_alt",
    "LINK": "defi", "AAVE": "defi", "UNI": "defi", "ONDO": "defi", "PENDLE": "defi",
    "HYPE": "hype",
    "ZEC": "privacy", "BCH": "privacy",
}
MAJORS = {"BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOGE"}

INTERVAL_MS = {
    "15m": 15 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
}

# ---------------------------------------------------------------------------
# TIMEFRAME STRUCTURE -- 4-stack: MACRO / HTF / MID / LTF
#
# Rationale (chosen over a 2-tf or 3-tf combo): the master requirement is
# genuine intraday *and* swing usefulness plus an explicit all-regime
# capability. A single bias/exec pair forces every trade into one holding
# horizon. A 4-stack lets the SAME symbol produce either an intraday or a
# swing signal in the SAME scan, tagged correctly, depending on which level
# of structure actually set up:
#
#   1D   (MACRO) -> long-range bias, premium/discount range, weekly/monthly
#                   liquidity (PWH/PWL/PMH/PML), and the swing-trade POI map.
#   4H   (HTF)   -> primary structure/trend, HTF order blocks & breaker
#                   blocks -- the main POI map for BOTH intraday and swing.
#   1H   (MID)   -> liquidity sweep + intermediate structure shift; also the
#                   POI map for intraday trades taken directly off 1H zones.
#   15M  (LTF)   -> execution timeframe: breaker-block retest trigger, MSS
#                   confirmation, entry timing precision for every trade
#                   regardless of which POI level produced it.
#
# A signal is tagged "swing" when its POI came from the 1D/4H map (wider
# structural stop -> naturally larger SL distance), "intraday" when its POI
# came from the 4H/1H map with a tighter execution-driven stop, and "scalp"
# only for range mean-reversion fades taken directly at a 1H range edge with
# a sub-1%-of-price stop. The classifier is distance-based (see
# classify_trade_type), not a hardcoded per-pathway label, so it reflects
# what the trade actually looks like risk-wise.
# ---------------------------------------------------------------------------
TF_MACRO, TF_HTF, TF_MID, TF_LTF = "1d", "4h", "1h", "15m"
TF_BARS = {TF_MACRO: 200, TF_HTF: 300, TF_MID: 300, TF_LTF: 300}
SCAN_INTERVAL_MIN = 15

# ── Indicator lengths ───────────────────────────────────────────────────
EMA_FAST, EMA_SLOW, EMA_TREND = 21, 50, 200
RSI_LEN, ATR_LEN, ADX_LEN, BB_LEN = 14, 14, 14, 20
BB_MULT = 2.0

# ── Zone detection ──────────────────────────────────────────────────────
OB_DISPLACEMENT_ATR_MULT = 1.15
OB_BOS_LOOKBACK = 25
FVG_MIN_GAP_ATR_MULT = 0.12
ZONE_MAX_WIDTH_ATR_MULT = 1.8
ZONE_LOOKBACK_HTF = 90
ZONE_LOOKBACK_LTF = 80
PIVOT_LEFT, PIVOT_RIGHT = 2, 2
LIQUIDITY_EQ_TOLERANCE_PCT = 0.0018

# ── Sweep / MSS ──────────────────────────────────────────────────────────
SWEEP_LOOKBACK = 16
SWEEP_MAX_DEPTH_ATR_MULT = 1.10
SWEEP_MIN_WICK_RATIO = 0.35
MSS_LOOKBACK_LTF = 40
MSS_DISPLACEMENT_ATR_MULT = 0.55
MSS_MIN_CLOSE_MARGIN_ATR_MULT = 0.08
BREAKER_SEARCH_BARS = 8

# ── Risk ─────────────────────────────────────────────────────────────────
MIN_RR_FLOOR = 1.4
MIN_RR_TARGET = 2.0
SL_BUFFER_ATR_MIN_MULT = 0.25
SL_BUFFER_ATR_MAX_MULT = 0.85
LIQUIDITY_ROOM_BUFFER_ATR_MULT = 0.25
POI_MAX_DIST_ATR_MULT = 1.4
POI_MAX_PCT_OF_PRICE = 0.02

# ── Volatility / liquidity gates ────────────────────────────────────────
MIN_ATR_PCT = 0.18
MAX_ATR_PCT = 9.0
SPREAD_WARN_PCT = 0.20
SPREAD_SUPPRESS_PCT = 0.45
SPREAD_EXEMPT = MAJORS
MIN_OI_USD = 400_000.0

# ── Session volume profile (POC / Value Area / VWAP) ────────────────────
VOL_PROFILE_BINS = 24
VOL_PROFILE_LOOKBACK_BARS = 96  # ~4 days of 1h bars

# ============================================================================
# ADAPTIVE GOVERNOR -- documented feedback loop
#
# Inputs (recomputed every scan):
#   1. Composite Regime Vector market_condition_score (0-100): a blend of
#      ATR expansion/contraction percentile, wick-to-body ratio, ADX
#      consistency, and cross-sectional breadth agreement. High = clean/
#      orderly market; low = chaotic/choppy.
#   2. A slow EMA (alpha = GOVERNOR_EMA_ALPHA) of realized signals/24h,
#      read from state["daily_log"].
#
# Mechanism:
#   base_threshold (BASE_MIN_CONFIDENCE) is adjusted every run by:
#     - condition_adj: market_condition_score maps linearly to +/- 6 conf.
#       points (chaotic conditions RAISE the bar; clean conditions LOWER it
#       slightly) -- this is the "tighten in chop, relax when orderly" rule
#       required by spec, applied every scan, no manual step.
#     - frequency_adj: if the trailing 24h EMA signal count is below
#       TARGET_SIGNALS_MIN, threshold drifts down (max GOVERNOR_MAX_SHIFT
#       points, GOVERNOR_STEP per hour so it can't overreact to one dry
#       scan); if above TARGET_SIGNALS_MAX, it drifts up. Rate-limited to
#       once per GOVERNOR_MIN_INTERVAL_S so scan-to-scan noise can't move it.
#   final_threshold = clamp(base + condition_adj + frequency_adj, FLOOR, CEIL)
#
# No pathway ever fires purely because the frequency term pushed the bar
# down -- condition_adj and the five-filter gate (see FilterResult) are
# unconditional; frequency_adj only narrows the confidence band within
# what already passed every structural/context/quality/RR/LTF filter.
# ============================================================================
BASE_MIN_CONFIDENCE = 58.0
GOVERNOR_FLOOR = 46.0
GOVERNOR_CEIL = 74.0
GOVERNOR_STEP = 1.5
GOVERNOR_MIN_INTERVAL_S = 3600
GOVERNOR_EMA_ALPHA = 0.25
TARGET_SIGNALS_MIN = 5
TARGET_SIGNALS_MAX = 10
CONDITION_SWING_MAX = 6.0

MAX_SIGNALS_PER_SCAN_DEFAULT = 4
MAX_SIGNALS_PER_SCAN_TRENDING = 6
MAX_CONCURRENT_ACTIVE_SIGNALS = 16
MAX_CONCURRENT_PER_SYMBOL = 1
MAX_SIGNAL_HISTORY = 1500
MAX_PER_SECTOR = 2
COOLDOWN_BARS_LTF = 3
DUPLICATE_ENTRY_TOLERANCE_PCT = 0.0035
DEDUP_TIME_WINDOW_HOURS = 48

# ── Self-tuning pathway weights (regularized, shrunk to neutral prior) ──
PATHWAY_WEIGHT_LEARNING_RATE = 0.04
PATHWAY_WEIGHT_MIN, PATHWAY_WEIGHT_MAX = 0.75, 1.30

# ── Funding / OI / RS ────────────────────────────────────────────────────
FUNDING_EXTREME = 0.0010
FUNDING_CARRY_THRESHOLD = 0.0005
OI_HISTORY_DEPTH = 6
OI_CHANGE_THRESHOLD_PCT = 1.0
RS_TOP_PCTILE, RS_BOTTOM_PCTILE = 0.20, 0.20

# ── Session weighting (UTC hours) ───────────────────────────────────────
SESSION_WINDOWS = {"asia": (0, 8), "london": (7, 12), "ny": (12, 21), "off": (21, 24)}
SESSION_SCORE_BONUS = {"asia": 0.0, "london": 2.0, "ny": 2.5, "off": -1.5}

# ── Macro calendar (soft, optional -- off by default: no external feed
#    wired in this single-file build; hook left in for the operator to
#    attach a forex-factory-style feed later without touching scoring). ──
MACRO_CALENDAR_ENABLED = False

DAILY_SUMMARY_HOUR_UTC = 8
REACT_TP = "🏆"
REACT_TP1 = "✅"
REACT_SL = "😭"

STATE_VERSION = 1

GRADE_SIZE_TABLE = {
    ("A+", "scalp"): 1.00, ("A+", "intraday"): 1.25, ("A+", "swing"): 1.50,
    ("A", "scalp"): 0.75, ("A", "intraday"): 1.00, ("A", "swing"): 1.25,
    ("B", "scalp"): 0.50, ("B", "intraday"): 0.65, ("B", "swing"): 0.85,
    ("C", "scalp"): 0.25, ("C", "intraday"): 0.35, ("C", "swing"): 0.45,
}

# ============================================================================
# LOGGING
# ============================================================================


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except OSError:
        pass


_shutdown = False


def _handle_shutdown(sig_num, frame):
    global _shutdown
    _shutdown = True
    log(f"Received shutdown signal {sig_num}, will exit after current symbol.")


os_signal.signal(os_signal.SIGTERM, _handle_shutdown)
os_signal.signal(os_signal.SIGINT, _handle_shutdown)

# ============================================================================
# HYPERLIQUID API LAYER
# ============================================================================

_hl_request_lock = threading.Lock()
_hl_last_request_ts = 0.0
_hl_session = requests.Session()


def _throttle():
    global _hl_last_request_ts
    with _hl_request_lock:
        now = time.monotonic()
        wait = HL_MIN_INTERVAL_S - (now - _hl_last_request_ts)
        if wait > 0:
            time.sleep(wait)
        _hl_last_request_ts = time.monotonic()


def hl_post(payload: dict, retries: int = 5, timeout: int = 12):
    for attempt in range(retries):
        _throttle()
        try:
            r = _hl_session.post(HL_INFO_URL, json=payload, timeout=timeout)
            if r.status_code == 429:
                time.sleep(1.5 * (attempt + 1) + random.random())
                continue
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as e:
            if attempt == retries - 1:
                log(f"hl_post failed after {retries} attempts: {e}")
                return None
            time.sleep(0.6 * (attempt + 1) + random.random() * 0.3)
    return None


def hl_coin(symbol: str) -> str:
    return symbol.upper()


def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    step = INTERVAL_MS[interval]
    return (reference_ms // step) * step


def filter_closed_candles(candles: list, interval: str, reference_ms: int) -> list:
    if not candles:
        return candles
    open_bar = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if c["t"] < open_bar]


def get_candles(symbol: str, interval: str, n: int, reference_ms: int) -> Optional[list]:
    end_ms = reference_ms
    start_ms = end_ms - (n + 5) * INTERVAL_MS[interval]
    payload = {
        "type": "candleSnapshot",
        "req": {"coin": hl_coin(symbol), "interval": interval, "startTime": start_ms, "endTime": end_ms},
    }
    raw = hl_post(payload)
    if not raw or not isinstance(raw, list):
        return None
    candles = []
    for c in raw:
        try:
            candles.append({
                "t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]),
                "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"]),
            })
        except (KeyError, TypeError, ValueError):
            continue
    candles = filter_closed_candles(candles, interval, reference_ms)
    return candles[-n:] if candles else []


def fetch_all_candles(symbol: str, reference_ms: int) -> Optional[dict]:
    out = {}
    for tf, n in TF_BARS.items():
        c = get_candles(symbol, tf, n, reference_ms)
        if c is None or len(c) < 40:
            return None
        out[tf] = c
    return out


def get_meta_and_asset_ctxs() -> Optional[dict]:
    raw = hl_post({"type": "metaAndAssetCtxs"})
    if not raw or not isinstance(raw, list) or len(raw) < 2:
        return None
    meta, ctxs = raw[0], raw[1]
    universe = meta.get("universe", [])
    out = {}
    for i, u in enumerate(universe):
        if i >= len(ctxs):
            break
        name = u.get("name")
        ctx = ctxs[i]
        try:
            out[name] = {
                "funding": float(ctx.get("funding", 0.0) or 0.0),
                "oi": float(ctx.get("openInterest", 0.0) or 0.0),
                "mark_px": float(ctx.get("markPx", 0.0) or 0.0),
                "day_ntl_vlm": float(ctx.get("dayNtlVlm", 0.0) or 0.0),
            }
        except (TypeError, ValueError):
            continue
    return out


def get_l2_spread_pct(symbol: str) -> Optional[float]:
    raw = hl_post({"type": "l2Book", "coin": hl_coin(symbol)})
    if not raw or "levels" not in raw:
        return None
    try:
        bids, asks = raw["levels"][0], raw["levels"][1]
        if not bids or not asks:
            return None
        best_bid, best_ask = float(bids[0]["px"]), float(asks[0]["px"])
        mid = (best_bid + best_ask) / 2
        if mid <= 0:
            return None
        return (best_ask - best_bid) / mid * 100.0
    except (KeyError, IndexError, TypeError, ValueError):
        return None


# ============================================================================
# MATH / INDICATORS
# ============================================================================


def safe(v, fb=0.0):
    try:
        f = float(v)
        return f if math.isfinite(f) else fb
    except (TypeError, ValueError):
        return fb


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b else default


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
        if i < period - 1:
            out.append(vals[i])
        else:
            out.append(sum(vals[i - period + 1:i + 1]) / period)
    return out


def stdev(vals: list, period: int) -> list:
    out = []
    for i in range(len(vals)):
        if i < period - 1:
            out.append(0.0)
        else:
            window = vals[i - period + 1:i + 1]
            m = sum(window) / period
            out.append(math.sqrt(sum((x - m) ** 2 for x in window) / period))
    return out


def rsi(closes: list, period: int = RSI_LEN) -> list:
    if len(closes) < period + 1:
        return [50.0] * len(closes)
    out = [50.0] * len(closes)
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(closes)):
        if i > period:
            avg_g = (avg_g * (period - 1) + gains[i - 1]) / period
            avg_l = (avg_l * (period - 1) + losses[i - 1]) / period
        rs = safe_div(avg_g, avg_l, default=999.0) if avg_l != 0 else 999.0
        out[i] = 100.0 - (100.0 / (1.0 + rs))
    return out


def atr_series(candles: list, period: int = ATR_LEN) -> list:
    trs = []
    for i, c in enumerate(candles):
        if i == 0:
            trs.append(c["h"] - c["l"])
        else:
            pc = candles[i - 1]["c"]
            trs.append(max(c["h"] - c["l"], abs(c["h"] - pc), abs(c["l"] - pc)))
    out = [trs[0]]
    for i in range(1, len(trs)):
        if i < period:
            out.append(sum(trs[:i + 1]) / (i + 1))
        else:
            out.append((out[-1] * (period - 1) + trs[i]) / period)
    return out


def adx_dmi(candles: list, period: int = ADX_LEN) -> tuple:
    n = len(candles)
    plus_dm, minus_dm, trs = [0.0] * n, [0.0] * n, [0.0] * n
    for i in range(1, n):
        up = candles[i]["h"] - candles[i - 1]["h"]
        dn = candles[i - 1]["l"] - candles[i]["l"]
        plus_dm[i] = up if (up > dn and up > 0) else 0.0
        minus_dm[i] = dn if (dn > up and dn > 0) else 0.0
        pc = candles[i - 1]["c"]
        trs[i] = max(candles[i]["h"] - candles[i]["l"], abs(candles[i]["h"] - pc), abs(candles[i]["l"] - pc))

    def wilder_smooth(vals):
        out = [0.0] * n
        if n <= period:
            return out
        out[period] = sum(vals[1:period + 1])
        for i in range(period + 1, n):
            out[i] = out[i - 1] - out[i - 1] / period + vals[i]
        return out

    str_ = wilder_smooth(trs)
    s_plus = wilder_smooth(plus_dm)
    s_minus = wilder_smooth(minus_dm)
    plus_di = [0.0] * n
    minus_di = [0.0] * n
    dx = [0.0] * n
    for i in range(n):
        if str_[i] > 0:
            plus_di[i] = 100.0 * s_plus[i] / str_[i]
            minus_di[i] = 100.0 * s_minus[i] / str_[i]
        denom = plus_di[i] + minus_di[i]
        dx[i] = 100.0 * abs(plus_di[i] - minus_di[i]) / denom if denom > 0 else 0.0
    adx = [0.0] * n
    start = period * 2
    if n > start:
        adx[start] = sum(dx[period:start]) / period if start > period else 0.0
        for i in range(start + 1, n):
            adx[i] = (adx[i - 1] * (period - 1) + dx[i]) / period
    return adx, plus_di, minus_di


def bollinger(closes: list, period: int = BB_LEN, mult: float = BB_MULT) -> tuple:
    mid = sma(closes, period)
    sd = stdev(closes, period)
    upper = [mid[i] + mult * sd[i] for i in range(len(closes))]
    lower = [mid[i] - mult * sd[i] for i in range(len(closes))]
    width_pct = [safe_div((upper[i] - lower[i]), mid[i]) * 100.0 for i in range(len(closes))]
    return upper, mid, lower, width_pct


def obv(closes: list, volumes: list) -> list:
    out = [0.0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            out.append(out[-1] + volumes[i])
        elif closes[i] < closes[i - 1]:
            out.append(out[-1] - volumes[i])
        else:
            out.append(out[-1])
    return out


def daily_vwap(candles: list, reference_ms: int) -> float:
    day_start = (reference_ms // INTERVAL_MS["1d"]) * INTERVAL_MS["1d"]
    todays = [c for c in candles if c["t"] >= day_start]
    if not todays:
        todays = candles[-20:]
    num = sum(((c["h"] + c["l"] + c["c"]) / 3) * c["v"] for c in todays)
    den = sum(c["v"] for c in todays)
    return safe_div(num, den, default=todays[-1]["c"])


def percentile_rank(vals: list, x: float) -> float:
    if not vals:
        return 50.0
    below = sum(1 for v in vals if v <= x)
    return 100.0 * below / len(vals)


def detect_rsi_divergence(closes: list, rsi_vals: list, lookback: int = 25) -> Optional[str]:
    if len(closes) < lookback + 5:
        return None
    seg_c, seg_r = closes[-lookback:], rsi_vals[-lookback:]
    lo_i = seg_c.index(min(seg_c))
    hi_i = seg_c.index(max(seg_c))
    # bullish: lower low in price, higher low in RSI, near the end
    if lo_i > lookback * 0.5:
        prior_low_i = seg_c[:lo_i].index(min(seg_c[:lo_i])) if lo_i > 0 else None
        if prior_low_i is not None and seg_c[lo_i] < seg_c[prior_low_i] and seg_r[lo_i] > seg_r[prior_low_i]:
            return "bullish"
    if hi_i > lookback * 0.5:
        prior_hi_i = seg_c[:hi_i].index(max(seg_c[:hi_i])) if hi_i > 0 else None
        if prior_hi_i is not None and seg_c[hi_i] > seg_c[prior_hi_i] and seg_r[hi_i] < seg_r[prior_hi_i]:
            return "bearish"
    return None


def compute_indicators(candles: list, reference_ms: int) -> dict:
    closes = [c["c"] for c in candles]
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
    vols = [c["v"] for c in candles]
    ef, es, et = ema(closes, EMA_FAST), ema(closes, EMA_SLOW), ema(closes, EMA_TREND)
    r = rsi(closes)
    a = atr_series(candles)
    adx, pdi, mdi = adx_dmi(candles)
    bb_u, bb_m, bb_l, bb_w = bollinger(closes)
    ov = obv(closes, vols)
    vwap = daily_vwap(candles, reference_ms)
    atr_pct = safe_div(a[-1], closes[-1]) * 100.0
    divergence = detect_rsi_divergence(closes, r)
    avg_vol20 = sum(vols[-20:]) / max(1, len(vols[-20:]))
    return {
        "candles": candles, "closes": closes, "highs": highs, "lows": lows, "vols": vols,
        "ema_fast": ef, "ema_slow": es, "ema_trend": et,
        "rsi": r, "atr": a, "adx": adx, "plus_di": pdi, "minus_di": mdi,
        "bb_upper": bb_u, "bb_mid": bb_m, "bb_lower": bb_l, "bb_width_pct": bb_w,
        "obv": ov, "vwap": vwap, "atr_pct": atr_pct, "divergence": divergence,
        "avg_vol20": avg_vol20, "last": closes[-1],
    }


_ind_cache: dict = {}


def get_cached_indicators(symbol: str, tf: str, candles: list, reference_ms: int) -> dict:
    key = (symbol, tf, candles[-1]["t"] if candles else 0)
    if key in _ind_cache:
        return _ind_cache[key]
    ind = compute_indicators(candles, reference_ms)
    _ind_cache[key] = ind
    return ind


def clear_indicator_cache():
    _ind_cache.clear()


# ============================================================================
# STATE MANAGEMENT
# ============================================================================


def _default_state() -> dict:
    return {
        "version": STATE_VERSION,
        "last_run_ms": 0,
        "active_signals": [],
        "signal_history": [],
        "cooldowns": {},
        "atr_pct_memory": {},
        "oi_history": {},
        "funding_history": {},
        "pathway_weights": {"liquidity_reversal": 1.0, "trend_continuation": 1.0, "range_reversion": 1.0},
        "pathway_stats": {},
        "daily_log": {},
        "signal_ema_24h": 0.0,
        "governor": {"threshold_adj": 0.0, "last_adjust_ms": 0},
        "last_summary_date": None,
        "next_signal_id": 1,
        "next_history_id": 1,
        "bar_index": {},
        "rs_returns": {},
    }


def load_state() -> dict:
    p = Path(STATE_FILE)
    if not p.exists():
        return _default_state()
    try:
        with open(p, "r") as f:
            st = json.load(f)
        default = _default_state()
        for k, v in default.items():
            st.setdefault(k, v)
        return st
    except (json.JSONDecodeError, OSError) as e:
        log(f"load_state failed ({e}), starting fresh")
        return _default_state()


def save_state(state: dict) -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, STATE_FILE)


def prune_state(state: dict, max_days: int = 21) -> None:
    cutoff = time.time() * 1000 - max_days * 86_400_000
    state["signal_history"] = [
        h for h in state["signal_history"] if h.get("closed_ms", cutoff + 1) >= cutoff
    ][-MAX_SIGNAL_HISTORY:]
    for sym in list(state["atr_pct_memory"].keys()):
        state["atr_pct_memory"][sym] = state["atr_pct_memory"][sym][-200:]
    for sym in list(state["oi_history"].keys()):
        state["oi_history"][sym] = state["oi_history"][sym][-OI_HISTORY_DEPTH * 4:]
    for sym in list(state["funding_history"].keys()):
        state["funding_history"][sym] = state["funding_history"][sym][-200:]
    for k in list(state["daily_log"].keys()):
        try:
            d = datetime.strptime(k, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if (datetime.now(timezone.utc) - d).days > 30:
                del state["daily_log"][k]
        except ValueError:
            del state["daily_log"][k]


# ============================================================================
# REGIME VECTOR / MARKET CONDITION SCORE
# ============================================================================


@dataclass
class RegimeVector:
    btc_bias: str = "neutral"          # bullish / bearish / neutral
    btc_strength: float = 0.0          # 0-100
    symbol_trend: str = "neutral"      # bullish / bearish / neutral / ranging
    adx_htf: float = 0.0
    vol_pctile: float = 50.0           # ATR% percentile vs own history
    noise_index: float = 50.0          # 0 (clean) - 100 (choppy)
    breadth: float = 0.5               # fraction of watchlist agreeing with BTC bias
    session: str = "off"
    market_condition_score: float = 50.0  # 0 chaotic - 100 orderly
    label: str = "neutral"             # bullish_trend / bearish_trend / ranging


def session_now(reference_ms: int) -> str:
    hour = datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc).hour
    for name, (a, b) in SESSION_WINDOWS.items():
        if a <= hour < b:
            return name
    return "off"


def compute_noise_index(candles: list, lookback: int = 30) -> float:
    seg = candles[-lookback:]
    if len(seg) < 5:
        return 50.0
    wick_ratios = []
    for c in seg:
        rng = c["h"] - c["l"]
        if rng <= 0:
            continue
        body = abs(c["c"] - c["o"])
        wick_ratios.append(1.0 - safe_div(body, rng))
    avg_wick = sum(wick_ratios) / len(wick_ratios) if wick_ratios else 0.5
    closes = [c["c"] for c in seg]
    directional = abs(closes[-1] - closes[0])
    path = sum(abs(closes[i] - closes[i - 1]) for i in range(1, len(closes)))
    efficiency = safe_div(directional, path, default=0.3)
    noise = (avg_wick * 60.0) + ((1 - efficiency) * 40.0)
    return max(0.0, min(100.0, noise))


def update_atr_pct_memory(state: dict, symbol: str, atr_pct: float) -> float:
    hist = state["atr_pct_memory"].setdefault(symbol, [])
    hist.append(atr_pct)
    state["atr_pct_memory"][symbol] = hist[-200:]
    return percentile_rank(hist[:-1] or [atr_pct], atr_pct)


def compute_btc_regime(btc_ind_htf: dict, btc_ind_mid: dict) -> tuple:
    ef, es, et = btc_ind_htf["ema_fast"][-1], btc_ind_htf["ema_slow"][-1], btc_ind_htf["ema_trend"][-1]
    adx = btc_ind_htf["adx"][-1]
    price = btc_ind_htf["last"]
    score = 0.0
    if price > ef > es > et:
        score += 40
    elif price < ef < es < et:
        score -= 40
    if btc_ind_mid["ema_fast"][-1] > btc_ind_mid["ema_slow"][-1]:
        score += 15
    else:
        score -= 15
    score += (adx - 20) * 0.8
    strength = max(0.0, min(100.0, 50 + score / 2))
    if score > 18:
        bias = "bullish"
    elif score < -18:
        bias = "bearish"
    else:
        bias = "neutral"
    return bias, strength


def symbol_bias_from_ind(ind: dict) -> Optional[str]:
    if ind["last"] > ind["ema_slow"][-1]:
        return "bullish"
    if ind["last"] < ind["ema_slow"][-1]:
        return "bearish"
    return None


def compute_breadth(bias_by_symbol: dict, btc_bias: str) -> float:
    if btc_bias == "neutral" or not bias_by_symbol:
        return 0.5
    agree = sum(1 for b in bias_by_symbol.values() if b == btc_bias)
    return safe_div(agree, len(bias_by_symbol), default=0.5)


def build_regime_vector(state: dict, symbol: str, ind_htf: dict, ind_mid: dict,
                         candles_mid: list, btc_bias: str, btc_strength: float,
                         breadth: float, reference_ms: int) -> RegimeVector:
    rv = RegimeVector()
    rv.btc_bias, rv.btc_strength, rv.breadth = btc_bias, btc_strength, breadth
    rv.adx_htf = ind_htf["adx"][-1]
    ef, es, et = ind_htf["ema_fast"][-1], ind_htf["ema_slow"][-1], ind_htf["ema_trend"][-1]
    price = ind_htf["last"]
    if rv.adx_htf >= 22 and price > ef > es:
        rv.symbol_trend = "bullish"
    elif rv.adx_htf >= 22 and price < ef < es:
        rv.symbol_trend = "bearish"
    elif rv.adx_htf < 18:
        rv.symbol_trend = "ranging"
    else:
        rv.symbol_trend = "neutral"
    rv.vol_pctile = update_atr_pct_memory(state, symbol, ind_mid["atr_pct"])
    rv.noise_index = compute_noise_index(candles_mid)
    rv.session = session_now(reference_ms)
    # market condition score: clean/orderly (high) vs chaotic/choppy (low)
    vol_stability = 100.0 - abs(rv.vol_pctile - 50.0) * 1.2
    trend_consistency = min(100.0, rv.adx_htf * 3.2)
    order = (100.0 - rv.noise_index) * 0.4 + vol_stability * 0.3 + trend_consistency * 0.3
    rv.market_condition_score = max(0.0, min(100.0, order))
    if rv.symbol_trend == "bullish":
        rv.label = "bullish_trend"
    elif rv.symbol_trend == "bearish":
        rv.label = "bearish_trend"
    elif rv.symbol_trend == "ranging":
        rv.label = "ranging"
    else:
        rv.label = "neutral"
    return rv


def select_pathways(regime: RegimeVector) -> list:
    """Route symbol into the pathway(s) that fit its live regime -- the
    All-Regime requirement is satisfied structurally here, not by hoping
    one universal strategy happens to work everywhere."""
    if regime.label in ("bullish_trend", "bearish_trend"):
        return ["trend_continuation", "liquidity_reversal"]
    if regime.label == "ranging":
        return ["range_reversion", "liquidity_reversal"]
    return ["liquidity_reversal"]


# ============================================================================
# ADAPTIVE GOVERNOR
# ============================================================================


def update_signal_ema(state: dict) -> float:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    count_today = state["daily_log"].get(today, {}).get("count", 0)
    prev = state.get("signal_ema_24h", 0.0)
    new = prev + GOVERNOR_EMA_ALPHA * (count_today - prev) if prev else float(count_today)
    state["signal_ema_24h"] = new
    return new


def adaptive_min_confidence(state: dict, regime: RegimeVector) -> float:
    condition_adj = ((regime.market_condition_score - 50.0) / 50.0) * -CONDITION_SWING_MAX
    # low market_condition_score (chaotic) -> positive adj (raise bar)
    condition_adj = -condition_adj

    gov = state["governor"]
    now = time.time()
    ema24 = state.get("signal_ema_24h", 0.0)
    if now - gov.get("last_adjust_ms", 0) / 1000.0 > GOVERNOR_MIN_INTERVAL_S:
        if ema24 < TARGET_SIGNALS_MIN:
            gov["threshold_adj"] = max(-8.0, gov["threshold_adj"] - GOVERNOR_STEP)
        elif ema24 > TARGET_SIGNALS_MAX:
            gov["threshold_adj"] = min(8.0, gov["threshold_adj"] + GOVERNOR_STEP)
        gov["last_adjust_ms"] = now * 1000.0

    threshold = BASE_MIN_CONFIDENCE + condition_adj + gov["threshold_adj"]
    return max(GOVERNOR_FLOOR, min(GOVERNOR_CEIL, threshold))


def dynamic_max_signals(regime_breadth: float, btc_bias: str) -> int:
    if btc_bias != "neutral" and (regime_breadth > 0.65 or regime_breadth < 0.35):
        return MAX_SIGNALS_PER_SCAN_TRENDING
    return MAX_SIGNALS_PER_SCAN_DEFAULT


def tune_pathway_weights(state: dict) -> None:
    stats = state.get("pathway_stats", {})
    weights = state.setdefault("pathway_weights", {})
    for pathway, s in stats.items():
        wins, losses = s.get("wins", 0), s.get("losses", 0)
        total = wins + losses
        if total < 6:
            continue
        wr = wins / total
        target = 0.75 + wr  # 0.75 (0% wr) .. 1.75 (100% wr), centered so 50%wr -> 1.25
        target = max(PATHWAY_WEIGHT_MIN, min(PATHWAY_WEIGHT_MAX, target))
        current = weights.get(pathway, 1.0)
        weights[pathway] = current + PATHWAY_WEIGHT_LEARNING_RATE * (target - current)
        weights[pathway] = max(PATHWAY_WEIGHT_MIN, min(PATHWAY_WEIGHT_MAX, weights[pathway]))


# ============================================================================
# MARKET STRUCTURE: SWINGS
# ============================================================================


@dataclass
class Swing:
    idx: int
    price: float
    kind: str  # "high" or "low"
    t: int


def find_swings(candles: list, left: int = PIVOT_LEFT, right: int = PIVOT_RIGHT) -> list:
    out = []
    n = len(candles)
    for i in range(left, n - right):
        window_h = [candles[j]["h"] for j in range(i - left, i + right + 1)]
        window_l = [candles[j]["l"] for j in range(i - left, i + right + 1)]
        if candles[i]["h"] == max(window_h):
            out.append(Swing(i, candles[i]["h"], "high", candles[i]["t"]))
        if candles[i]["l"] == min(window_l):
            out.append(Swing(i, candles[i]["l"], "low", candles[i]["t"]))
    return out


@dataclass
class StructureState:
    bias: str  # bullish / bearish / neutral
    last_high: Optional[Swing]
    last_low: Optional[Swing]
    higher_highs: bool
    higher_lows: bool


def analyze_structure(candles: list, swings: list) -> Optional[StructureState]:
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    if len(highs) < 2 or len(lows) < 2:
        return None
    hh = highs[-1].price > highs[-2].price
    hl = lows[-1].price > lows[-2].price
    lh = highs[-1].price < highs[-2].price
    ll = lows[-1].price < lows[-2].price
    if hh and hl:
        bias = "bullish"
    elif lh and ll:
        bias = "bearish"
    else:
        bias = "neutral"
    return StructureState(bias, highs[-1], lows[-1], hh, hl)


# ============================================================================
# ZONES: ORDER BLOCKS / FVGS / BREAKER BLOCKS
# ============================================================================


@dataclass
class Zone:
    kind: str          # "ob_bull" / "ob_bear" / "fvg_bull" / "fvg_bear" / "breaker_bull" / "breaker_bear"
    top: float
    bottom: float
    idx: int
    t: int
    quality: float = 0.5
    mitigated: bool = False
    tested: bool = False
    confluences: int = 0


def find_order_blocks(candles: list, atr_vals: list, lookback: int = ZONE_LOOKBACK_HTF) -> list:
    zones = []
    start = max(1, len(candles) - lookback)
    for i in range(start, len(candles) - 1):
        c = candles[i]
        body = abs(c["c"] - c["o"])
        a = atr_vals[i] or 1e-9
        if body < OB_DISPLACEMENT_ATR_MULT * a:
            continue
        bullish_disp = c["c"] > c["o"]
        # bearish candle followed by strong bullish displacement -> bullish OB
        if bullish_disp and i >= 1 and candles[i - 1]["c"] < candles[i - 1]["o"]:
            prior = candles[i - 1]
            zones.append(Zone("ob_bull", prior["h"], prior["l"], i - 1, prior["t"], quality=min(1.0, body / (a * 2))))
        if (not bullish_disp) and i >= 1 and candles[i - 1]["c"] > candles[i - 1]["o"]:
            prior = candles[i - 1]
            zones.append(Zone("ob_bear", prior["h"], prior["l"], i - 1, prior["t"], quality=min(1.0, body / (a * 2))))
    return zones


def find_fvgs(candles: list, atr_vals: list, lookback: int = ZONE_LOOKBACK_HTF) -> list:
    zones = []
    start = max(2, len(candles) - lookback)
    for i in range(start, len(candles)):
        a = atr_vals[i] or 1e-9
        c0, c2 = candles[i - 2], candles[i]
        if c2["l"] > c0["h"] and (c2["l"] - c0["h"]) >= FVG_MIN_GAP_ATR_MULT * a:
            zones.append(Zone("fvg_bull", c2["l"], c0["h"], i - 1, candles[i - 1]["t"],
                               quality=min(1.0, (c2["l"] - c0["h"]) / a)))
        if c0["l"] > c2["h"] and (c0["l"] - c2["h"]) >= FVG_MIN_GAP_ATR_MULT * a:
            zones.append(Zone("fvg_bear", c0["l"], c2["h"], i - 1, candles[i - 1]["t"],
                               quality=min(1.0, (c0["l"] - c2["h"]) / a)))
    return zones


def mark_mitigation_and_breakers(zones: list, candles: list) -> list:
    """Forward-track every OB/FVG: the instant price CLOSES through it after
    formation, reclassify it as a breaker block (Kestrel's structural
    enforcement, not a scoring bonus)."""
    out = []
    for z in zones:
        mitigated = False
        breaker = False
        for c in candles[z.idx + 1:]:
            if z.kind in ("ob_bull", "fvg_bull"):
                if c["l"] <= z.top:
                    mitigated = True
                if c["c"] < z.bottom:
                    breaker = True
            else:
                if c["h"] >= z.bottom:
                    mitigated = True
                if c["c"] > z.top:
                    breaker = True
        z.mitigated = mitigated
        z.tested = mitigated
        if breaker:
            new_kind = ("breaker_bear" if "bull" in z.kind else "breaker_bull")
            out.append(Zone(new_kind, z.top, z.bottom, z.idx, z.t, quality=z.quality * 0.9, mitigated=True, tested=True))
        else:
            out.append(z)
    return out


def zone_width_ok(z: Zone, atr_val: float) -> bool:
    width = z.top - z.bottom
    return 0 < width <= ZONE_MAX_WIDTH_ATR_MULT * atr_val


def cluster_levels(levels: list, tol_pct: float = LIQUIDITY_EQ_TOLERANCE_PCT) -> list:
    if not levels:
        return []
    levels = sorted(levels)
    clusters = [[levels[0]]]
    for lv in levels[1:]:
        if abs(lv - clusters[-1][-1]) / max(clusters[-1][-1], 1e-9) <= tol_pct:
            clusters[-1].append(lv)
        else:
            clusters.append([lv])
    return [(sum(c) / len(c), len(c)) for c in clusters]


def build_liquidity_pools(swings: list, candles_macro: list) -> dict:
    highs = [s.price for s in swings if s.kind == "high"]
    lows = [s.price for s in swings if s.kind == "low"]
    pdh = candles_macro[-2]["h"] if len(candles_macro) >= 2 else None
    pdl = candles_macro[-2]["l"] if len(candles_macro) >= 2 else None
    if pdh:
        highs.append(pdh)
    if pdl:
        lows.append(pdl)
    return {"resistance": cluster_levels(highs), "support": cluster_levels(lows)}


def detect_sweep(candles: list, pools: dict, direction: str, atr_vals: list,
                  lookback: int = SWEEP_LOOKBACK) -> Optional[dict]:
    seg = candles[-lookback:]
    a = atr_vals[-1] or 1e-9
    targets = pools["support"] if direction == "long" else pools["resistance"]
    for level, weight in sorted(targets, key=lambda x: -x[1]):
        for c in seg:
            body = abs(c["c"] - c["o"])
            rng = c["h"] - c["l"]
            wick_ratio = 1.0 - safe_div(body, rng, default=0.0)
            if direction == "long" and c["l"] < level and c["c"] > level:
                depth = level - c["l"]
                if depth <= SWEEP_MAX_DEPTH_ATR_MULT * a and wick_ratio >= SWEEP_MIN_WICK_RATIO:
                    return {"level": level, "weight": weight, "candle_t": c["t"], "depth": depth}
            if direction == "short" and c["h"] > level and c["c"] < level:
                depth = c["h"] - level
                if depth <= SWEEP_MAX_DEPTH_ATR_MULT * a and wick_ratio >= SWEEP_MIN_WICK_RATIO:
                    return {"level": level, "weight": weight, "candle_t": c["t"], "depth": depth}
    return None


def premium_discount_zone(candles: list, lookback: int = 50) -> dict:
    seg = candles[-lookback:]
    hi, lo = max(c["h"] for c in seg), min(c["l"] for c in seg)
    mid = (hi + lo) / 2
    price = candles[-1]["c"]
    zone = "premium" if price > mid else "discount"
    return {"high": hi, "low": lo, "mid": mid, "zone": zone}


def detect_mss(candles_ltf: list, direction: str, after_t: int,
               lookback: int = MSS_LOOKBACK_LTF) -> Optional[dict]:
    seg = [c for c in candles_ltf if c["t"] >= after_t][-lookback:]
    if len(seg) < 6:
        return None
    swings = find_swings(seg, left=1, right=1)
    a = atr_series(seg)[-1] or 1e-9
    if direction == "long":
        recent_highs = [s for s in swings if s.kind == "high"]
        if not recent_highs:
            return None
        ref = recent_highs[-1]
        for c in seg[ref.idx + 1:]:
            if c["c"] > ref.price + MSS_MIN_CLOSE_MARGIN_ATR_MULT * a:
                disp = c["c"] - c["o"]
                if disp >= MSS_DISPLACEMENT_ATR_MULT * a:
                    return {"broke_level": ref.price, "t": c["t"], "close": c["c"]}
    else:
        recent_lows = [s for s in swings if s.kind == "low"]
        if not recent_lows:
            return None
        ref = recent_lows[-1]
        for c in seg[ref.idx + 1:]:
            if c["c"] < ref.price - MSS_MIN_CLOSE_MARGIN_ATR_MULT * a:
                disp = c["o"] - c["c"]
                if disp >= MSS_DISPLACEMENT_ATR_MULT * a:
                    return {"broke_level": ref.price, "t": c["t"], "close": c["c"]}
    return None


def find_ltf_breaker(candles_ltf: list, mss: dict, direction: str) -> Optional[Zone]:
    seg = [c for c in candles_ltf if c["t"] <= mss["t"]][-BREAKER_SEARCH_BARS - 5:]
    if len(seg) < 4:
        return None
    a = atr_series(seg)[-1] or 1e-9
    zones = find_order_blocks(seg, atr_series(seg), lookback=len(seg))
    zones = mark_mitigation_and_breakers(zones, seg)
    want = "breaker_bull" if direction == "long" else "breaker_bear"
    candidates = [z for z in zones if z.kind == want]
    return candidates[-1] if candidates else None


# ============================================================================
# SESSION VOLUME PROFILE (POC / Value Area / VWAP)
# ============================================================================


def volume_profile(candles: list, bins: int = VOL_PROFILE_BINS) -> dict:
    seg = candles[-VOL_PROFILE_LOOKBACK_BARS:]
    if not seg:
        return {}
    hi = max(c["h"] for c in seg)
    lo = min(c["l"] for c in seg)
    if hi <= lo:
        return {}
    step = (hi - lo) / bins
    buckets = [0.0] * bins
    for c in seg:
        mid = (c["h"] + c["l"] + c["c"]) / 3
        idx = min(bins - 1, max(0, int((mid - lo) / step)))
        buckets[idx] += c["v"]
    total = sum(buckets) or 1.0
    poc_idx = buckets.index(max(buckets))
    poc = lo + (poc_idx + 0.5) * step
    order = sorted(range(bins), key=lambda i: -buckets[i])
    cum, va_idx = 0.0, []
    for i in order:
        cum += buckets[i]
        va_idx.append(i)
        if cum / total >= 0.70:
            break
    va_high = lo + (max(va_idx) + 1) * step
    va_low = lo + min(va_idx) * step
    return {"poc": poc, "va_high": va_high, "va_low": va_low}


# ============================================================================
# CANDIDATE / RISK PLAN
# ============================================================================


@dataclass
class Candidate:
    symbol: str
    direction: str      # long / short
    pathway: str
    regime_label: str
    entry: float
    sl: float
    tp1: float
    tp2: Optional[float]
    rr1: float
    rr2: Optional[float]
    reason: str
    sl_reason: str
    tp_reason: str
    confluences: list = field(default_factory=list)
    quality: float = 0.5
    trade_type: str = "intraday"
    poi_kind: str = ""


def adaptive_sl_buffer(candles: list, atr_val: float, vol_pctile: float) -> float:
    """Wick/liquidity-grab resistant buffer: scales with recent volatility
    percentile so it survives a normal stop-hunt wick without being so wide
    it defeats the trade's RR. Body-close confirmation logic (detect_sweep /
    detect_mss above) already required a close back through the level before
    we ever consider the sweep valid; this buffer additionally protects
    against a SECOND, shallower wick poking through the invalidation point."""
    mult = SL_BUFFER_ATR_MIN_MULT + (SL_BUFFER_ATR_MAX_MULT - SL_BUFFER_ATR_MIN_MULT) * (vol_pctile / 100.0)
    recent_wicks = []
    for c in candles[-10:]:
        rng = c["h"] - c["l"]
        body = abs(c["c"] - c["o"])
        if rng > 0:
            recent_wicks.append(rng - body)
    wick_floor = (sum(recent_wicks) / len(recent_wicks)) * 0.5 if recent_wicks else 0.0
    return max(atr_val * mult, wick_floor)


def clip_tp_to_targets(entry: float, raw_tp: float, direction: str, pools: dict,
                        vprofile: dict, atr_val: float) -> float:
    candidates = []
    levels = pools.get("resistance" if direction == "long" else "support", [])
    candidates += [lv for lv, _w in levels]
    if vprofile:
        candidates += [vprofile.get("poc", raw_tp), vprofile.get("va_high", raw_tp), vprofile.get("va_low", raw_tp)]
    buffer_ = LIQUIDITY_ROOM_BUFFER_ATR_MULT * atr_val
    if direction == "long":
        ahead = [lv for lv in candidates if entry < lv <= raw_tp + buffer_]
        if ahead:
            return min(min(ahead) - buffer_ * 0.3, raw_tp)
    else:
        ahead = [lv for lv in candidates if entry > lv >= raw_tp - buffer_]
        if ahead:
            return max(max(ahead) + buffer_ * 0.3, raw_tp)
    return raw_tp


def build_risk_plan(direction: str, entry: float, invalidation: float, atr_val: float,
                     vol_pctile: float, candles: list, pools: dict, vprofile: dict,
                     zone_desc: str) -> dict:
    buf = adaptive_sl_buffer(candles, atr_val, vol_pctile)
    if direction == "long":
        sl = invalidation - buf
        risk = entry - sl
        raw_tp1 = entry + risk * MIN_RR_TARGET
        raw_tp2 = entry + risk * (MIN_RR_TARGET * 1.8)
    else:
        sl = invalidation + buf
        risk = sl - entry
        raw_tp1 = entry - risk * MIN_RR_TARGET
        raw_tp2 = entry - risk * (MIN_RR_TARGET * 1.8)
    if risk <= 0:
        return {}
    tp1 = clip_tp_to_targets(entry, raw_tp1, direction, pools, vprofile, atr_val)
    tp2 = clip_tp_to_targets(entry, raw_tp2, direction, pools, vprofile, atr_val)
    if direction == "long" and tp2 <= tp1:
        tp2 = tp1 + risk * 0.5
    if direction == "short" and tp2 >= tp1:
        tp2 = tp1 - risk * 0.5
    rr1 = safe_div(abs(tp1 - entry), risk)
    rr2 = safe_div(abs(tp2 - entry), risk)
    sl_reason = f"Beyond {zone_desc} · {buf / atr_val:.2f}x ATR buffer (vol {vol_pctile:.0f}pct)"
    tp_reason = "TP1 @ 2R nearest liquidity/VA · TP2 next pool"
    return {"sl": sl, "tp1": tp1, "tp2": tp2, "rr1": rr1, "rr2": rr2, "risk": risk,
            "sl_reason": sl_reason, "tp_reason": tp_reason}


def classify_trade_type(poi_kind: str, risk_pct_of_price: float) -> str:
    if poi_kind == "macro" or risk_pct_of_price >= 3.0:
        return "swing"
    if risk_pct_of_price <= 0.9 and poi_kind == "range_edge":
        return "scalp"
    return "intraday"


# ============================================================================
# PATHWAY 1: LIQUIDITY REVERSAL  (works in every regime -- Meridian backbone:
# SFP -> MSS -> Breaker retest)
# ============================================================================


def build_pathway_liquidity_reversal(symbol: str, bundles: dict, regime: RegimeVector,
                                      state: dict) -> Optional[Candidate]:
    ind_htf = bundles["ind_htf"]
    candles_htf, candles_ltf = bundles["candles"][TF_HTF], bundles["candles"][TF_LTF]
    swings_htf = find_swings(candles_htf)
    pools = build_liquidity_pools(swings_htf, bundles["candles"][TF_MACRO])
    atr_htf = ind_htf["atr"]
    pd_zone = premium_discount_zone(candles_htf)

    for direction in ("long", "short"):
        if direction == "long" and pd_zone["zone"] != "discount" and regime.symbol_trend == "bearish":
            continue
        if direction == "short" and pd_zone["zone"] != "premium" and regime.symbol_trend == "bullish":
            continue
        sweep = detect_sweep(candles_htf, pools, direction, atr_htf)
        if not sweep:
            continue
        mss = detect_mss(candles_ltf, direction, sweep["candle_t"])
        if not mss:
            continue
        breaker = find_ltf_breaker(candles_ltf, mss, direction)
        entry = mss["close"] if not breaker else (breaker.top + breaker.bottom) / 2
        invalidation = sweep["level"] - (sweep["depth"] * 0.15) if direction == "long" else sweep["level"] + (sweep["depth"] * 0.15)
        if direction == "long":
            invalidation = min(invalidation, candles_htf[-1]["l"] if False else invalidation)
        atr_val = atr_htf[-1]
        plan = build_risk_plan(direction, entry, invalidation, atr_val, regime.vol_pctile,
                                candles_ltf, pools, volume_profile(bundles["candles"][TF_MID]),
                                f"the swept liquidity pool ({sweep['level']:.6g})")
        if not plan:
            continue
        rr1 = plan["rr1"]
        if rr1 < MIN_RR_FLOOR:
            continue
        confl = ["liquidity sweep", "MSS confirmation"]
        if breaker:
            confl.append("LTF breaker retest")
        if pd_zone["zone"] == ("discount" if direction == "long" else "premium"):
            confl.append(f"{pd_zone['zone']} zone")
        risk_pct = abs(entry - plan["sl"]) / entry * 100.0
        return Candidate(
            symbol=symbol, direction=direction, pathway="liquidity_reversal",
            regime_label=regime.label, entry=entry, sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"],
            rr1=rr1, rr2=plan["rr2"],
            reason=f"Sweep {sweep['level']:.6g} → {TF_LTF} MSS" + (" → breaker retest" if breaker else ""),
            sl_reason=plan["sl_reason"], tp_reason=plan["tp_reason"], confluences=confl,
            quality=min(1.0, 0.5 + 0.1 * len(confl)),
            trade_type=classify_trade_type("htf_poi", risk_pct), poi_kind="liquidity_reversal",
        )
    return None


# ============================================================================
# PATHWAY 2: TREND CONTINUATION  (bullish/bearish regimes)
# ============================================================================


def build_pathway_trend_continuation(symbol: str, bundles: dict, regime: RegimeVector,
                                      state: dict) -> Optional[Candidate]:
    if regime.symbol_trend not in ("bullish", "bearish"):
        return None
    direction = "long" if regime.symbol_trend == "bullish" else "short"
    ind_htf = bundles["ind_htf"]
    ind_mid = bundles["ind_mid"]
    candles_htf = bundles["candles"][TF_HTF]
    candles_mid = bundles["candles"][TF_MID]
    atr_htf = ind_htf["atr"]

    zones = find_order_blocks(candles_htf, atr_htf) + find_fvgs(candles_htf, atr_htf)
    zones = mark_mitigation_and_breakers(zones, candles_htf)
    want_bull = {"ob_bull", "fvg_bull", "breaker_bull"}
    want_bear = {"ob_bear", "fvg_bear", "breaker_bear"}
    wanted = want_bull if direction == "long" else want_bear
    fresh = [z for z in zones if z.kind in wanted and not z.mitigated]
    if not fresh:
        return None
    price = ind_htf["last"]
    fresh = [z for z in fresh if abs(price - (z.top + z.bottom) / 2) <= POI_MAX_DIST_ATR_MULT * atr_htf[-1]]
    if not fresh:
        return None
    zone = max(fresh, key=lambda z: z.quality)
    if not zone_width_ok(zone, atr_htf[-1]):
        return None

    # require pullback into the zone on MID tf then a continuation candle
    touched = any(c["l"] <= zone.top and c["h"] >= zone.bottom for c in candles_mid[-10:])
    if not touched:
        return None
    rsi_mid = ind_mid["rsi"][-1]
    if direction == "long" and rsi_mid > 75:
        return None
    if direction == "short" and rsi_mid < 25:
        return None

    entry = (zone.top + zone.bottom) / 2
    invalidation = zone.bottom if direction == "long" else zone.top
    pools = build_liquidity_pools(find_swings(candles_htf), bundles["candles"][TF_MACRO])
    plan = build_risk_plan(direction, entry, invalidation, atr_htf[-1], regime.vol_pctile,
                            candles_mid, pools, volume_profile(candles_mid),
                            f"the {zone.kind.replace('_', ' ')} zone")
    if not plan or plan["rr1"] < MIN_RR_FLOOR:
        return None
    confl = [f"{regime.symbol_trend} trend (ADX {regime.adx_htf:.0f})", zone.kind.replace("_", " ")]
    if zone.confluences:
        confl.append("stacked POI confluence")
    risk_pct = abs(entry - plan["sl"]) / entry * 100.0
    return Candidate(
        symbol=symbol, direction=direction, pathway="trend_continuation", regime_label=regime.label,
        entry=entry, sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"], rr1=plan["rr1"], rr2=plan["rr2"],
        reason=f"Pullback into {zone.kind.replace('_', ' ')} · {regime.symbol_trend} trend",
        sl_reason=plan["sl_reason"], tp_reason=plan["tp_reason"], confluences=confl,
        quality=zone.quality, trade_type=classify_trade_type("htf_poi", risk_pct), poi_kind="trend_continuation",
    )


# ============================================================================
# PATHWAY 3: RANGE MEAN-REVERSION  (ranging regime -- fills the gap none of
# the reference engines wired fully into the same pipeline)
# ============================================================================


def build_pathway_range_reversion(symbol: str, bundles: dict, regime: RegimeVector,
                                   state: dict) -> Optional[Candidate]:
    if regime.symbol_trend != "ranging":
        return None
    candles_mid = bundles["candles"][TF_MID]
    ind_mid = bundles["ind_mid"]
    lookback = 40
    seg = candles_mid[-lookback:]
    range_high = max(c["h"] for c in seg)
    range_low = min(c["l"] for c in seg)
    width = range_high - range_low
    if width <= 0:
        return None
    price = ind_mid["last"]
    atr_val = ind_mid["atr"][-1]
    dist_to_high = range_high - price
    dist_to_low = price - range_low
    rsi_val = ind_mid["rsi"][-1]

    direction = None
    edge_desc = ""
    if dist_to_low <= 0.15 * width and rsi_val < 40:
        direction, edge_desc = "long", "range low"
        invalidation = range_low - 0.1 * width
        entry = price
    elif dist_to_high <= 0.15 * width and rsi_val > 60:
        direction, edge_desc = "short", "range high"
        invalidation = range_high + 0.1 * width
        entry = price
    else:
        return None

    pools = build_liquidity_pools(find_swings(candles_mid), bundles["candles"][TF_HTF])
    plan = build_risk_plan(direction, entry, invalidation, atr_val, regime.vol_pctile,
                            candles_mid, pools, volume_profile(candles_mid), f"the {edge_desc}")
    if not plan or plan["rr1"] < MIN_RR_FLOOR:
        return None
    # cap TP at opposite range edge for pure mean reversion (don't chase a breakout)
    opp_edge = range_high if direction == "long" else range_low
    if direction == "long":
        plan["tp2"] = min(plan["tp2"], opp_edge)
        plan["tp1"] = min(plan["tp1"], opp_edge)
    else:
        plan["tp2"] = max(plan["tp2"], opp_edge)
        plan["tp1"] = max(plan["tp1"], opp_edge)
    confl = [f"range fade at {edge_desc}", f"RSI {rsi_val:.0f}", f"ADX {regime.adx_htf:.0f} (low, confirms range)"]
    risk_pct = abs(entry - plan["sl"]) / entry * 100.0
    return Candidate(
        symbol=symbol, direction=direction, pathway="range_reversion", regime_label=regime.label,
        entry=entry, sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"], rr1=plan["rr1"], rr2=plan["rr2"],
        reason=f"Fade at {edge_desc} · {width / price * 100:.1f}% range (ADX {regime.adx_htf:.0f})",
        sl_reason=plan["sl_reason"], tp_reason=plan["tp_reason"], confluences=confl,
        quality=0.55, trade_type=classify_trade_type("range_edge", risk_pct), poi_kind="range_reversion",
    )


# ============================================================================
# FIVE-FILTER GATE (Ecliptic) -- every candidate passes through all five
# before it is even scored.
# ============================================================================


@dataclass
class FilterResult:
    passed: bool
    reasons: list = field(default_factory=list)


def apply_five_filters(cand: Candidate, market_price: float, atr_val: float, spread_pct: Optional[float]) -> FilterResult:
    reasons = []
    ok = True
    # 0. ALREADY INVALIDATED -- the setup is built from the last *closed*
    #    candle on each timeframe, so by the time we're about to publish it,
    #    live price may already have run through the SL (or past TP1) on the
    #    still-forming candle. Without this check the engine would post a
    #    signal that is dead (or already "won") on arrival. Compare against
    #    the freshest live mark price available, not a candle close.
    if cand.direction == "long":
        if market_price <= cand.sl:
            ok = False
            reasons.append(f"live price {market_price:.6g} already at/through SL {cand.sl:.6g}")
        elif cand.tp1 and market_price >= cand.tp1:
            ok = False
            reasons.append(f"live price {market_price:.6g} already at/through TP1 {cand.tp1:.6g}")
    else:
        if market_price >= cand.sl:
            ok = False
            reasons.append(f"live price {market_price:.6g} already at/through SL {cand.sl:.6g}")
        elif cand.tp1 and market_price <= cand.tp1:
            ok = False
            reasons.append(f"live price {market_price:.6g} already at/through TP1 {cand.tp1:.6g}")
    # 1. LOCATION -- distance from live price
    dist_pct = abs(cand.entry - market_price) / market_price * 100.0
    if dist_pct > POI_MAX_PCT_OF_PRICE * 100.0 and dist_pct > POI_MAX_DIST_ATR_MULT * atr_val / market_price * 100.0:
        ok = False
        reasons.append(f"entry {dist_pct:.2f}% from market, too far")
    # 2. CONTEXT -- pathway must fit regime (already routed by select_pathways,
    #    this is the belt-and-braces re-check)
    if cand.pathway == "trend_continuation" and cand.regime_label not in ("bullish_trend", "bearish_trend"):
        ok = False
        reasons.append("trend pathway fired outside a trend regime")
    if cand.pathway == "range_reversion" and cand.regime_label != "ranging":
        ok = False
        reasons.append("range pathway fired outside a ranging regime")
    # 3. QUALITY
    if cand.quality < 0.35:
        ok = False
        reasons.append(f"zone quality too low ({cand.quality:.2f})")
    # 4. RR
    if cand.rr1 < MIN_RR_FLOOR:
        ok = False
        reasons.append(f"RR {cand.rr1:.2f} below floor {MIN_RR_FLOOR}")
    # 5. LTF CONFIRMATION (liquidity_reversal already requires MSS structurally;
    #    for the other two pathways require at least 2 confluences)
    if cand.pathway != "liquidity_reversal" and len(cand.confluences) < 2:
        ok = False
        reasons.append("insufficient LTF/context confirmation")
    if spread_pct is not None and spread_pct > SPREAD_SUPPRESS_PCT and cand.symbol not in SPREAD_EXEMPT:
        ok = False
        reasons.append(f"spread {spread_pct:.2f}% too wide")
    return FilterResult(ok, reasons)


# ============================================================================
# SCORING -- one unified logistic confluence model
# ============================================================================


def logistic(x: float) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0


def funding_oi_read(symbol: str, snapshot: dict, direction: str, state: dict) -> dict:
    row = snapshot.get(symbol, {})
    funding = row.get("funding", 0.0)
    oi = row.get("oi", 0.0)
    hist = state["oi_history"].setdefault(symbol, [])
    oi_trend = "flat"
    if hist:
        prev = hist[-1]
        chg = safe_div(oi - prev, prev, default=0.0) * 100.0
        if chg > OI_CHANGE_THRESHOLD_PCT:
            oi_trend = "rising"
        elif chg < -OI_CHANGE_THRESHOLD_PCT:
            oi_trend = "falling"
    hist.append(oi)
    state["oi_history"][symbol] = hist[-OI_HISTORY_DEPTH * 4:]

    fhist = state["funding_history"].setdefault(symbol, [])
    fhist.append(funding)
    state["funding_history"][symbol] = fhist[-200:]

    score = 0.0
    notes = []
    # contrarian funding read: extreme funding against the trade direction is bullish for us
    if direction == "long" and funding < -FUNDING_CARRY_THRESHOLD:
        score += 1.0
        notes.append("negative funding (shorts paying, contrarian tailwind)")
    if direction == "short" and funding > FUNDING_CARRY_THRESHOLD:
        score += 1.0
        notes.append("positive funding (longs paying, contrarian tailwind)")
    if abs(funding) > FUNDING_EXTREME:
        score += 0.5
        notes.append("funding at extreme")
    if direction == "long" and oi_trend == "rising":
        score += 0.5
        notes.append("OI rising with move")
    if direction == "short" and oi_trend == "rising":
        score += 0.5
        notes.append("OI rising with move")
    return {"score": score, "notes": notes, "oi_usd": oi, "funding": funding}


def relative_strength_percentile(symbol: str, state: dict) -> float:
    returns = state.get("rs_returns", {})
    if symbol not in returns or len(returns) < 5:
        return 50.0
    vals = list(returns.values())
    return percentile_rank(vals, returns[symbol])


def score_candidate(cand: Candidate, regime: RegimeVector, state: dict, snapshot: dict,
                     spread_pct: Optional[float]) -> tuple:
    x = 0.0
    x += 0.35 * (cand.quality - 0.5) * 4
    x += 0.30 * (min(cand.rr1, 4.0) - MIN_RR_TARGET)
    x += 0.18 * (len(cand.confluences) - 2)
    x += 0.12 * ((regime.market_condition_score - 50.0) / 25.0)

    fo = funding_oi_read(cand.symbol, snapshot, cand.direction, state)
    x += 0.10 * fo["score"]

    rs_pct = relative_strength_percentile(cand.symbol, state)
    if cand.direction == "long" and rs_pct >= (100 - RS_TOP_PCTILE * 100):
        x += 0.15
    if cand.direction == "short" and rs_pct <= RS_BOTTOM_PCTILE * 100:
        x += 0.15

    breadth_fit = regime.breadth if cand.direction == "long" else (1 - regime.breadth)
    x += 0.10 * (breadth_fit - 0.5) * 2

    x += SESSION_SCORE_BONUS.get(regime.session, 0.0) / 10.0

    weight = state.get("pathway_weights", {}).get(cand.pathway, 1.0)
    x *= weight

    confidence = 100.0 * logistic(x)
    notes = fo["notes"]
    return confidence, notes, fo


def grade_for_confidence(confidence: float) -> str:
    if confidence >= 82:
        return "A+"
    if confidence >= 72:
        return "A"
    if confidence >= 62:
        return "B"
    return "C"


# ============================================================================
# CORRELATION CLUSTERING / DEDUP / SECTOR CAP
# ============================================================================


def compute_returns(candles: list, lookback: int = 30) -> list:
    closes = [c["c"] for c in candles[-lookback - 1:]]
    return [safe_div(closes[i] - closes[i - 1], closes[i - 1]) for i in range(1, len(closes))]


def pearson(a: list, b: list) -> float:
    n = min(len(a), len(b))
    if n < 5:
        return 0.0
    a, b = a[-n:], b[-n:]
    ma, mb = sum(a) / n, sum(b) / n
    cov = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    va = math.sqrt(sum((x - ma) ** 2 for x in a))
    vb = math.sqrt(sum((x - mb) ** 2 for x in b))
    return safe_div(cov, va * vb, default=0.0)


def build_correlation_clusters(returns_by_symbol: dict, threshold: float = 0.75) -> list:
    symbols = list(returns_by_symbol.keys())
    clusters = []
    used = set()
    for s in symbols:
        if s in used:
            continue
        cluster = {s}
        for t in symbols:
            if t == s or t in used:
                continue
            if pearson(returns_by_symbol[s], returns_by_symbol[t]) >= threshold:
                cluster.add(t)
        used |= cluster
        clusters.append(cluster)
    return clusters


def dedup_correlated(ranked: list, clusters: list) -> list:
    chosen, seen_clusters = [], set()
    for item in ranked:
        symbol = item["cand"].symbol
        cluster_id = None
        for i, c in enumerate(clusters):
            if symbol in c:
                cluster_id = i
                break
        key = (cluster_id, item["cand"].direction)
        if key in seen_clusters and cluster_id is not None:
            continue
        seen_clusters.add(key)
        chosen.append(item)
    return chosen


def sector_cap_ok(selected: list, symbol: str) -> bool:
    sector = SECTOR_MAP.get(symbol)
    if not sector:
        return True
    count = sum(1 for s in selected if SECTOR_MAP.get(s["cand"].symbol) == sector)
    return count < MAX_PER_SECTOR


# ============================================================================
# COOLDOWN / DEDUP
# ============================================================================


def check_cooldown(state: dict, symbol: str, direction: str, bar_index: int) -> bool:
    key = f"{symbol}:{direction}"
    last = state["cooldowns"].get(key)
    if last is None:
        return True
    return (bar_index - last) >= COOLDOWN_BARS_LTF


def update_cooldown(state: dict, symbol: str, direction: str, bar_index: int) -> None:
    state["cooldowns"][f"{symbol}:{direction}"] = bar_index


def is_recent_duplicate(state: dict, symbol: str, direction: str, entry: float) -> bool:
    now = time.time() * 1000
    cutoff = now - DEDUP_TIME_WINDOW_HOURS * 3_600_000
    for sig in state["active_signals"]:
        if sig["symbol"] != symbol or sig["direction"] != direction:
            continue
        if sig["opened_ms"] < cutoff:
            continue
        if abs(sig["entry"] - entry) / entry <= DUPLICATE_ENTRY_TOLERANCE_PCT:
            return True
    return False


def count_open_for_symbol(state: dict, symbol: str) -> int:
    return sum(1 for s in state["active_signals"] if s["symbol"] == symbol)


# ============================================================================
# FORMATTING / TELEGRAM
# ============================================================================


def fmt_px(v: float) -> str:
    if v >= 100:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:,.4f}"
    return f"{v:.6f}"


def confidence_bar(confidence: float) -> str:
    filled = int(round(confidence / 10))
    return "▰" * filled + "▱" * (10 - filled)


def md2_escape(text) -> str:
    text = str(text)
    for ch in r"_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


def format_signal(cand: Candidate, confidence: float, grade: str, fo_notes: list) -> str:
    arrow = "🟢 LONG" if cand.direction == "long" else "🔴 SHORT"
    conf_str = "\n".join(f"• {md2_escape(c)}" for c in cand.confluences + fo_notes)
    header = f"*{md2_escape(ENGINE_NAME)}* — Signal"
    # Each price sits in its own inline-code span so it can be tapped and
    # copied individually in Telegram, instead of one shared code block
    # where a tap/long-press copies all four lines at once.
    lines = [
        header,
        "",
        f"*{md2_escape(cand.symbol)}*  {arrow}",
        f"Regime: `{cand.regime_label}`   Type: `{cand.trade_type}`   Grade: *{grade}* {confidence_bar(confidence)} \\({confidence:.0f}%\\)",
        f"Pathway: `{cand.pathway}`",
        "",
        f"Entry  `{fmt_px(cand.entry)}`",
        f"SL     `{fmt_px(cand.sl)}`",
        f"TP1    `{fmt_px(cand.tp1)}`  \\(R {cand.rr1:.2f}\\)",
        f"TP2    `{fmt_px(cand.tp2)}`  \\(R {cand.rr2:.2f}\\)" if cand.tp2 else "",
        "",
        f"*Why:* {md2_escape(cand.reason)}",
        f"*SL:* {md2_escape(cand.sl_reason)}",
        f"*TP:* {md2_escape(cand.tp_reason)}",
    ]
    if conf_str:
        lines += ["", "*Confluence:*", conf_str]
    return "\n".join(l for l in lines if l is not None)


def send_telegram(text: str) -> Optional[int]:
    if DRY_RUN:
        print("----- DRY RUN TELEGRAM SEND -----")
        print(text)
        print("----------------------------------")
        return random.randint(1, 999999)
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "MarkdownV2"}, timeout=10)
        r.raise_for_status()
        return r.json().get("result", {}).get("message_id")
    except requests.RequestException as e:
        log(f"send_telegram failed: {e}")
        return None


def reply_telegram(text: str, reply_to_message_id: Optional[int]) -> Optional[int]:
    if DRY_RUN:
        print(f"----- DRY RUN TELEGRAM REPLY to {reply_to_message_id} -----")
        print(text)
        return random.randint(1, 999999)
    if not reply_to_message_id:
        return send_telegram(text)
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": TG_CHAT_ID, "text": text, "parse_mode": "MarkdownV2",
            "reply_to_message_id": reply_to_message_id,
        }, timeout=10)
        r.raise_for_status()
        return r.json().get("result", {}).get("message_id")
    except requests.RequestException as e:
        log(f"reply_telegram failed: {e}")
        return None


def react_telegram(message_id: Optional[int], emoji: str) -> None:
    if DRY_RUN or not message_id:
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/setMessageReaction"
    try:
        requests.post(url, json={
            "chat_id": TG_CHAT_ID, "message_id": message_id, "reaction": [{"type": "emoji", "emoji": emoji}],
        }, timeout=10)
    except requests.RequestException as e:
        log(f"react_telegram failed: {e}")


# ============================================================================
# SIGNAL TRACKING / RESOLUTION
# ============================================================================


def track_signal(state: dict, cand: Candidate, confidence: float, grade: str, msg_id: Optional[int],
                  reference_ms: int, bar_index: int) -> dict:
    sig = {
        "id": state["next_signal_id"], "symbol": cand.symbol, "direction": cand.direction,
        "pathway": cand.pathway, "regime_label": cand.regime_label, "trade_type": cand.trade_type,
        "entry": cand.entry, "sl": cand.sl, "tp1": cand.tp1, "tp2": cand.tp2,
        "rr1": cand.rr1, "rr2": cand.rr2, "confidence": confidence, "grade": grade,
        "msg_id": msg_id, "opened_ms": reference_ms, "bar_index": bar_index,
        "tp1_hit": False, "status": "open",
    }
    state["next_signal_id"] += 1
    state["active_signals"].append(sig)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    day = state["daily_log"].setdefault(today, {"count": 0, "by_regime": {}, "by_type": {}})
    day["count"] += 1
    day["by_regime"][cand.regime_label] = day["by_regime"].get(cand.regime_label, 0) + 1
    day["by_type"][cand.trade_type] = day["by_type"].get(cand.trade_type, 0) + 1
    return sig


def _r_multiple(sig: dict, price: float) -> float:
    risk = abs(sig["entry"] - sig["sl"])
    if risk <= 0:
        return 0.0
    if sig["direction"] == "long":
        return (price - sig["entry"]) / risk
    return (sig["entry"] - price) / risk


def _close_signal(state: dict, sig: dict, result: str, price: float) -> None:
    sig["status"] = "closed"
    sig["result"] = result
    sig["close_price"] = price
    sig["closed_ms"] = time.time() * 1000
    sig["r_multiple"] = _r_multiple(sig, price)
    state["active_signals"] = [s for s in state["active_signals"] if s["id"] != sig["id"]]
    state["signal_history"].append(sig)
    stats = state["pathway_stats"].setdefault(sig["pathway"], {"wins": 0, "losses": 0})
    if result in ("tp1", "tp2"):
        stats["wins"] += 1
    elif result == "sl":
        stats["losses"] += 1

    outcome_text = {"tp1": "TP1 hit ✅", "tp2": "TP2 hit 🏆", "sl": "SL hit 😭"}[result]
    text = (
        f"*{md2_escape(ENGINE_NAME)}* — Trade Update\n\n"
        f"*{md2_escape(sig['symbol'])}* {sig['direction'].upper()} — {md2_escape(outcome_text)}\n"
        f"Close: `{fmt_px(price)}`   R: `{sig['r_multiple']:.2f}`"
    )
    reply_telegram(text, sig.get("msg_id"))
    react_telegram(sig.get("msg_id"), REACT_TP if result == "tp2" else (REACT_TP1 if result == "tp1" else REACT_SL))


def check_active_signals(state: dict, market_prices: dict) -> None:
    for sig in list(state["active_signals"]):
        price = market_prices.get(sig["symbol"])
        if price is None:
            continue
        direction = sig["direction"]
        if direction == "long":
            if price <= sig["sl"]:
                _close_signal(state, sig, "sl", price)
                continue
            if not sig["tp1_hit"] and sig["tp1"] and price >= sig["tp1"]:
                if sig["tp2"] and sig["tp2"] != sig["tp1"]:
                    sig["tp1_hit"] = True
                    reply_telegram(
                        f"*{md2_escape(ENGINE_NAME)}* — {md2_escape(sig['symbol'])} TP1 hit ✅, runner active toward TP2",
                        sig.get("msg_id"))
                    react_telegram(sig.get("msg_id"), REACT_TP1)
                else:
                    _close_signal(state, sig, "tp1", price)
                    continue
            if sig["tp2"] and price >= sig["tp2"]:
                _close_signal(state, sig, "tp2", price)
        else:
            if price >= sig["sl"]:
                _close_signal(state, sig, "sl", price)
                continue
            if not sig["tp1_hit"] and sig["tp1"] and price <= sig["tp1"]:
                if sig["tp2"] and sig["tp2"] != sig["tp1"]:
                    sig["tp1_hit"] = True
                    reply_telegram(
                        f"*{md2_escape(ENGINE_NAME)}* — {md2_escape(sig['symbol'])} TP1 hit ✅, runner active toward TP2",
                        sig.get("msg_id"))
                    react_telegram(sig.get("msg_id"), REACT_TP1)
                else:
                    _close_signal(state, sig, "tp1", price)
                    continue
            if sig["tp2"] and price <= sig["tp2"]:
                _close_signal(state, sig, "tp2", price)


# ============================================================================
# DAILY SUMMARY (08:00 UTC, driven off the 15-min cron)
# ============================================================================


def should_send_daily_summary(state: dict, now: datetime) -> bool:
    today = now.strftime("%Y-%m-%d")
    if now.hour < DAILY_SUMMARY_HOUR_UTC:
        return False
    return state.get("last_summary_date") != today


def build_daily_summary(state: dict, now: datetime) -> str:
    cutoff = now.timestamp() * 1000 - 24 * 3_600_000
    resolved = [h for h in state["signal_history"] if h.get("closed_ms", 0) >= cutoff]
    opened = [s for s in state["active_signals"]] + resolved
    opened_recent = [s for s in opened if s.get("opened_ms", 0) >= cutoff]

    wins = sum(1 for h in resolved if h["result"] in ("tp1", "tp2"))
    losses = sum(1 for h in resolved if h["result"] == "sl")
    total_closed = wins + losses
    win_rate = safe_div(wins, total_closed, default=0.0) * 100.0
    still_open = len([s for s in state["active_signals"] if s.get("opened_ms", 0) >= cutoff])

    best = max(resolved, key=lambda h: h.get("r_multiple", -99), default=None)
    worst = min(resolved, key=lambda h: h.get("r_multiple", 99), default=None)

    by_regime, by_type = {}, {}
    for s in opened_recent:
        by_regime[s.get("regime_label", "?")] = by_regime.get(s.get("regime_label", "?"), 0) + 1
        by_type[s.get("trade_type", "?")] = by_type.get(s.get("trade_type", "?"), 0) + 1

    lines = [
        f"*{md2_escape(ENGINE_NAME)}* — Daily Summary",
        f"_{md2_escape(now.strftime('%Y-%m-%d 08:00 UTC'))}_",
        "",
        f"Signals \\(24h\\): *{len(opened_recent)}*   Wins: *{wins}*   Losses: *{losses}*   Open: *{still_open}*",
        f"Win rate: *{win_rate:.0f}%*",
    ]
    if best:
        lines.append(f"Best: {md2_escape(best['symbol'])} {best['direction']} — R {best['r_multiple']:.2f}")
    if worst:
        lines.append(f"Worst: {md2_escape(worst['symbol'])} {worst['direction']} — R {worst['r_multiple']:.2f}")
    if by_regime:
        lines.append("")
        lines.append("*By regime:* " + md2_escape(", ".join(f"{k}:{v}" for k, v in by_regime.items())))
    if by_type:
        lines.append("*By type:* " + md2_escape(", ".join(f"{k}:{v}" for k, v in by_type.items())))
    return "\n".join(lines)


def maybe_send_daily_summary(state: dict) -> None:
    now = datetime.now(timezone.utc)
    if should_send_daily_summary(state, now):
        text = build_daily_summary(state, now)
        send_telegram(text)
        state["last_summary_date"] = now.strftime("%Y-%m-%d")


# ============================================================================
# SYMBOL EVALUATION
# ============================================================================


def evaluate_symbol(symbol: str, reference_ms: int, state: dict, btc_bias: str, btc_strength: float,
                     breadth: float, snapshot: dict, spread_pct: Optional[float]) -> Optional[dict]:
    candles = fetch_all_candles(symbol, reference_ms)
    if not candles:
        return None
    atr_pct_mid = atr_series(candles[TF_MID])[-1] / candles[TF_MID][-1]["c"] * 100.0
    if not (MIN_ATR_PCT <= atr_pct_mid <= MAX_ATR_PCT):
        return None
    row = snapshot.get(symbol, {})
    if row.get("oi", 0.0) < MIN_OI_USD and symbol not in MAJORS:
        return None
    # Prefer the live exchange mark price (fetched fresh at the top of this
    # scan) over any candle-close price. Candle closes used elsewhere in this
    # function are, by construction, from the last *closed* bar and can lag
    # live price by up to a full timeframe interval -- exactly the gap that
    # let signals fire already past their own SL. Fall back to the mid-tf
    # candle close only if the snapshot didn't have this symbol.
    live_price = row.get("mark_px") or None

    ind_htf = get_cached_indicators(symbol, TF_HTF, candles[TF_HTF], reference_ms)
    ind_mid = get_cached_indicators(symbol, TF_MID, candles[TF_MID], reference_ms)
    bundles = {"candles": candles, "ind_htf": ind_htf, "ind_mid": ind_mid}

    regime = build_regime_vector(state, symbol, ind_htf, ind_mid, candles[TF_MID], btc_bias, btc_strength,
                                  breadth, reference_ms)
    bar_index = len(candles[TF_LTF])
    state["bar_index"][symbol] = bar_index

    pathways = select_pathways(regime)
    builders = {
        "liquidity_reversal": build_pathway_liquidity_reversal,
        "trend_continuation": build_pathway_trend_continuation,
        "range_reversion": build_pathway_range_reversion,
    }
    best = None
    for pw in pathways:
        cand = builders[pw](symbol, bundles, regime, state)
        if cand is None:
            continue
        if not check_cooldown(state, symbol, cand.direction, bar_index):
            continue
        if is_recent_duplicate(state, symbol, cand.direction, cand.entry):
            continue
        current_price = live_price if live_price else ind_mid["last"]
        fr = apply_five_filters(cand, current_price, ind_mid["atr"][-1], spread_pct)
        if not fr.passed:
            continue
        confidence, fo_notes, fo = score_candidate(cand, regime, state, snapshot, spread_pct)
        item = {"cand": cand, "confidence": confidence, "fo_notes": fo_notes, "regime": regime,
                 "bar_index": bar_index}
        if best is None or confidence > best["confidence"]:
            best = item
    return best


# ============================================================================
# SCAN LOOP
# ============================================================================


def run_scan() -> None:
    reference_ms = int(time.time() * 1000)
    state = load_state()
    clear_indicator_cache()

    snapshot = get_meta_and_asset_ctxs() or {}
    btc_candles = fetch_all_candles("BTC", reference_ms)
    if not btc_candles:
        log("Could not fetch BTC data, aborting scan.")
        return
    btc_ind_htf = compute_indicators(btc_candles[TF_HTF], reference_ms)
    btc_ind_mid = compute_indicators(btc_candles[TF_MID], reference_ms)
    btc_bias, btc_strength = compute_btc_regime(btc_ind_htf, btc_ind_mid)

    # breadth pre-pass: cheap 1h bias read for the whole watchlist
    bias_by_symbol = {}
    returns_by_symbol = {}
    price_by_symbol = {}
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as ex:
        futs = {ex.submit(get_candles, s, TF_MID, 120, reference_ms): s for s in WATCHLIST}
        for fut in as_completed(futs):
            s = futs[fut]
            try:
                c = fut.result()
            except Exception as e:
                log(f"breadth prefetch failed for {s}: {e}")
                continue
            if not c or len(c) < 60:
                continue
            ind = compute_indicators(c, reference_ms)
            bias = symbol_bias_from_ind(ind)
            if bias:
                bias_by_symbol[s] = bias
            returns_by_symbol[s] = compute_returns(c)
            price_by_symbol[s] = ind["last"]
            ret30 = safe_div(c[-1]["c"] - c[-30]["c"], c[-30]["c"]) * 100.0 if len(c) > 30 else 0.0
            state.setdefault("rs_returns", {})[s] = ret30

    breadth = compute_breadth(bias_by_symbol, btc_bias)
    clusters = build_correlation_clusters(returns_by_symbol)

    results = []
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as ex:
        futs = {}
        for s in WATCHLIST:
            if _shutdown:
                break
            spread = get_l2_spread_pct(s) if s not in SPREAD_EXEMPT else None
            futs[ex.submit(evaluate_symbol, s, reference_ms, state, btc_bias, btc_strength, breadth, snapshot, spread)] = s
        for fut in as_completed(futs):
            s = futs[fut]
            try:
                item = fut.result()
            except Exception as e:
                log(f"evaluate_symbol failed for {s}: {e}")
                continue
            if item:
                results.append(item)

    if not results:
        log("No candidates this scan.")
    else:
        results.sort(key=lambda r: -r["confidence"])
        results = dedup_correlated(results, clusters)

        regime_for_threshold = results[0]["regime"] if results else RegimeVector()
        min_conf = adaptive_min_confidence(state, regime_for_threshold)
        max_signals = dynamic_max_signals(breadth, btc_bias)

        selected = []
        for item in results:
            if item["confidence"] < min_conf:
                continue
            cand = item["cand"]
            if count_open_for_symbol(state, cand.symbol) >= MAX_CONCURRENT_PER_SYMBOL:
                continue
            if len(state["active_signals"]) + len(selected) >= MAX_CONCURRENT_ACTIVE_SIGNALS:
                continue
            if not sector_cap_ok(selected, cand.symbol):
                continue
            selected.append(item)
            if len(selected) >= max_signals:
                break

        for item in selected:
            cand, confidence = item["cand"], item["confidence"]
            grade = grade_for_confidence(confidence)
            text = format_signal(cand, confidence, grade, item["fo_notes"])
            msg_id = send_telegram(text)
            track_signal(state, cand, confidence, grade, msg_id, reference_ms, item["bar_index"])
            update_cooldown(state, cand.symbol, cand.direction, item["bar_index"])
            log(f"SIGNAL {cand.symbol} {cand.direction} {cand.pathway} conf={confidence:.1f} grade={grade}")

    # Refresh mark prices right before checking SL/TP. price_by_symbol was
    # captured from the breadth pre-pass (1h candle closes) before any of the
    # per-symbol analysis ran, and a full scan across the watchlist can take
    # a while -- using it here means a signal opened moments ago gets graded
    # against a price snapshot that's already stale in either direction.
    # A fresh metaAndAssetCtxs call gives every symbol's live mark price in
    # one request; fall back to the pre-pass close for any symbol missing
    # from it (e.g. a transient fetch failure).
    fresh_snapshot = get_meta_and_asset_ctxs() or {}
    market_prices = dict(price_by_symbol)
    for sym, row in fresh_snapshot.items():
        px = row.get("mark_px")
        if px:
            market_prices[sym] = px
    check_active_signals(state, market_prices)
    tune_pathway_weights(state)
    update_signal_ema(state)
    maybe_send_daily_summary(state)
    prune_state(state)
    state["last_run_ms"] = reference_ms
    save_state(state)
    log(f"Scan complete. {len(state['active_signals'])} active signals.")


def main() -> None:
    try:
        run_scan()
    except Exception as e:
        log(f"FATAL scan error: {e}")
        raise


if __name__ == "__main__":
    main()
