#!/usr/bin/env python3
"""
================================================================================
 LUCERNA  //  Adaptive Confluence Signal Engine  //  v1.0.0
================================================================================

# pip install requests

WHAT IS LUCERNA?
-----------------
Lucerna ("lamp" in Latin) is a triple-timeframe (1D bias / 4H structure /
1H trigger) intraday & swing signal engine for Hyperliquid perpetuals. Its
core idea: three independent signal *families* -- smart-money structure
(sweeps + market-structure-shift), trend continuation, and momentum
breakout -- are each scored on their own merits, then combined through an
ensemble-agreement layer that only rewards confluence it did not already
double-count (correlation-aware), and only punishes conflict when the
conflict is real. Derivatives data (funding + open interest) and a
regime-conditioned adaptive-threshold layer let the engine loosen up in
clean trending tape and tighten automatically in chop or elevated
uncertainty, without any live self-tuning loop (all thresholds are fixed,
regime-conditioned rules established via backtest -- see
`ADAPTIVE THRESHOLD MECHANISM` below for the exact rule table).

WHY IT'S DIFFERENT
--------------------
1. Ensemble-agreement confidence: instead of one scoring formula, three
   independent pathways vote. Strong single-pathway setups can still pass
   on their own merit, but borderline setups only clear the bar when a
   second, structurally-independent pathway agrees -- and setups where
   pathways actively disagree are suppressed rather than averaged.
2. Funding/OI are first-class regime & confluence inputs (not an
   afterthought), used to detect squeeze / crowded-trade conditions that
   pure price action misses.
3. A genuine backtest module with walk-forward validation, a locked
   holdout window, fee/slippage-aware net returns, a real parameter-
   sensitivity sweep (trades are regenerated under each perturbed
   threshold, not re-summarized from an unperturbed list), and a
   simple-baseline comparison run on the holdout too -- so the win rate is
   measured, not asserted.
4. Genuinely triple-timeframe: 1D macro bias (`macro_bias_1d`) is a real
   score input -- a trend/reversal candidate whose direction actively
   opposes the 1D bias is penalized, not just decorated with an unread
   candle set.
5. ONE-SIGNAL-PER-SYMBOL: a symbol with an open, unresolved signal (SL,
   breakeven-after-TP1, or TP2 all count as "resolved") will not produce a
   new signal in either direction until that trade closes. See
   `symbol_has_open_signal` / `ONE_SIGNAL_PER_SYMBOL`.

ADAPTIVE THRESHOLD MECHANISM (how quality/frequency balance is achieved)
--------------------------------------------------------------------------
Every scan computes a `RegimeVector` per symbol from: ADX (trend strength),
Bollinger bandwidth percentile (compression/expansion), ATR percentile
(volatility regime), a noise index (mean-reversion of closes around EMA),
and BTC's own regime (macro backdrop). This maps deterministically
(`adaptive_min_score` / `adaptive_liquidity_floor`) via a fixed lookup
table decided during backtesting -- NOT tuned live:
  - Clean trend regime (ADX >= 25, BTC regime aligned, noise index low):
    min confluence score floor is LOWERED by up to 8 points and the
    minimum 24h-volume liquidity floor is relaxed by 25% -- more setups
    are allowed through because the tape itself is de-risking them.
  - Choppy/uncertain regime (ADX < 15, BB-width percentile < 20th i.e.
    a squeeze with no resolution yet, or noise index high): min score
    floor is RAISED by up to 10 points and liquidity floor is tightened
    by 40% -- fewer, higher-conviction setups only.
  - High volatility regime (ATR percentile > 85): SL/TP distances widen
    (see `adaptive_sl_multiple`) rather than blocking signals outright,
    because wide-but-real moves are exactly what swing trades want to
    catch; but the false-breakout filter's follow-through requirement is
    tightened to guard against volatility-driven wick fakeouts.
  - Every suppressed candidate is logged with the specific filter that
    blocked it (see `log_suppressed`), which is the audit trail called
    for by the spec and lets a human -- not the live engine -- revisit
    the fixed thresholds during the next backtest cycle.
This is a fixed, inspectable rule table. Nothing in the live path adjusts
these numbers based on the engine's own trading results.

INFRASTRUCTURE (unchanged from reference engines)
----------------------------------------------------
  Data source / exchange : Hyperliquid public API (info endpoint)
  Watchlist               : WATCHLIST below (edit as needed)
  Operating model          : scan-per-run (stateless process, stateful file)
  Scheduler                : cron-job.org hitting this script every 15 min
  State                    : state.json (read + write every run)

USAGE
------
  python lucerna_engine_v1_0_0.py                  # normal scan
  python lucerna_engine_v1_0_0.py --dry-run         # scan, log, no send/no state commit
  python lucerna_engine_v1_0_0.py --backtest        # run backtest/evaluation module
  python lucerna_engine_v1_0_0.py --backtest --days 180

REQUIRED ENVIRONMENT VARIABLES
---------------------------------
  TELEGRAM_BOT_TOKEN   Telegram bot token
  TELEGRAM_CHAT_ID     Telegram chat/channel id
Optional:
  LUCERNA_STATE_PATH   override path to state.json (default ./state.json)
  LUCERNA_DRY_RUN      "1" to force dry-run without the CLI flag
================================================================================
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import requests


# ==============================================================================
# CONFIGURATION
# ==============================================================================

HL_API_URL = "https://api.hyperliquid.xyz/info"

TELEGRAM_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TG_CHAT_ID", "")

STATE_PATH = os.environ.get("LUCERNA_STATE_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "state.json"))
LOG_PATH = os.environ.get("LUCERNA_LOG_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "lucerna.log"))

WATCHLIST = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR",
    "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT",
    "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]

# Timeframes: 1D = macro bias, 4H = structure/HTF, 1H = execution trigger
TF_BIAS = "1d"
TF_STRUCT = "4h"
TF_EXEC = "1h"
CANDLE_COUNTS = {TF_BIAS: 220, TF_STRUCT: 300, TF_EXEC: 300}
INTERVAL_MS = {TF_EXEC: 3_600_000, TF_STRUCT: 14_400_000, TF_BIAS: 86_400_000}

# Indicator lengths
RSI_LEN = 14
ATR_LEN = 14
ADX_LEN = 14
BB_LEN = 20
BB_MULT = 2.0
DONCHIAN_LEN = 20
EMA_FAST = 21
EMA_SLOW = 50
EMA_TREND = 200

# Scoring / thresholds (fixed, regime-conditioned -- see module docstring)
BASE_MIN_SCORE = 62.0          # 0-100 confluence score floor in a neutral regime
TREND_REGIME_SCORE_RELIEF = 8.0
CHOP_REGIME_SCORE_PENALTY = 10.0
MIN_RR = 1.5
MAX_CONCURRENT_SIGNALS = 8
MAX_PORTFOLIO_EXPOSURE_PCT = 60.0   # % of notional account capital deployable at once
PER_TRADE_RISK_PCT = 0.75           # % of account risked per trade (for position sizing)
DAILY_LOSS_LIMIT_PCT = -4.0         # stop new signals for the UTC day if breached
COOLDOWN_BARS_EXEC = 6              # min 1H bars between repeat signals same symbol+dir
SIGNAL_FRESHNESS_MAX_DRIFT_ATR = 0.6  # invalidate if price drifted >0.6*ATR before send
LIQUIDITY_MIN_24H_VOL_USD = 5_000_000.0
CORR_LOOKBACK_BARS = 60
CORR_CLUSTER_THRESHOLD = 0.75

# ONE-SIGNAL-PER-SYMBOL POLICY: while a symbol has an open (unresolved) active
# signal, no new signal -- in either direction, from any pathway -- will be
# generated for that symbol. The open signal must fully resolve (stop loss,
# or TP1 -> breakeven-stop, or TP2) before that symbol can fire again. This is
# enforced live in `evaluate_symbol` and, for realism, inside the backtest
# window simulator as well (see `_backtest_window`'s `busy_until` tracking).
ONE_SIGNAL_PER_SYMBOL = True

# Macro (1D) bias gate -- see `macro_bias_1d`. Requires a minimum ADX on the
# daily timeframe before a bias is considered directional rather than neutral.
MACRO_BIAS_MIN_ADX = 15.0
MACRO_OPPOSED_PENALTY = 14.0   # score penalty when 1D bias actively opposes the trade
MACRO_ALIGNED_BONUS = 5.0      # score bonus when 1D bias agrees with the trade

# Backtest universe -- full watchlist by default. Reduce only if runtime is a
# real constraint; the audit flagged a 6/16-symbol subset as an undisclosed
# coverage gap, so the default here is now the entire WATCHLIST.
BACKTEST_SYMBOLS = WATCHLIST

DRY_RUN = os.environ.get("LUCERNA_DRY_RUN", "0") == "1"


# ==============================================================================
# LOGGING
# ==============================================================================

logger = logging.getLogger("lucerna")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _fh = logging.FileHandler(LOG_PATH)
    _fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(_fh)
    _sh = logging.StreamHandler(sys.stdout)
    _sh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(_sh)


def log_suppressed(symbol: str, direction: str, pathway: str, reason: str, score: float = 0.0) -> None:
    """Audit trail: every candidate that was generated but filtered out."""
    logger.info("SUPPRESSED | %s %s | pathway=%s | reason=%s | score=%.1f", symbol, direction, pathway, reason, score)


_SHUTDOWN = False


def _handle_shutdown(sig_num, frame):
    global _SHUTDOWN
    _SHUTDOWN = True
    logger.warning("Shutdown signal received (%s); finishing current step then exiting.", sig_num)


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


# ==============================================================================
# HYPERLIQUID API LAYER
# ==============================================================================

class _RateLimiter:
    def __init__(self, max_per_second: float = 8.0):
        self.min_interval = 1.0 / max_per_second
        self._last = 0.0

    def wait(self):
        now = time.monotonic()
        delta = now - self._last
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)
        self._last = time.monotonic()


_rl = _RateLimiter()


def hl_post(payload: dict, retries: int = 4, timeout: int = 12) -> Optional[dict | list]:
    last_err = None
    for attempt in range(retries):
        _rl.wait()
        try:
            resp = requests.post(HL_API_URL, json=payload, timeout=timeout,
                                  headers={"Content-Type": "application/json"})
            if resp.status_code == 200:
                return resp.json()
            last_err = f"HTTP {resp.status_code}: {resp.text[:200]}"
        except requests.RequestException as e:
            last_err = str(e)
        time.sleep(min(2 ** attempt, 8))
    logger.error("hl_post failed after %d retries: %s | payload=%s", retries, last_err, payload.get("type"))
    return None


def hl_coin(symbol: str) -> str:
    return symbol.upper()


def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    interval_ms = INTERVAL_MS[interval]
    return (reference_ms // interval_ms) * interval_ms


def filter_closed_candles(candles: list[dict], interval: str, reference_ms: int) -> list[dict]:
    open_now = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if c.get("t", 0) < open_now]


def get_candles(symbol: str, interval: str, n: int, reference_ms: Optional[int] = None) -> list[dict]:
    reference_ms = reference_ms or int(time.time() * 1000)
    interval_ms = INTERVAL_MS[interval]
    start = reference_ms - (n + 5) * interval_ms
    payload = {
        "type": "candleSnapshot",
        "req": {"coin": hl_coin(symbol), "interval": interval, "startTime": start, "endTime": reference_ms},
    }
    raw = hl_post(payload)
    if not raw or not isinstance(raw, list):
        return []
    candles = [{"t": c["t"], "o": float(c["o"]), "h": float(c["h"]), "l": float(c["l"]),
                "c": float(c["c"]), "v": float(c["v"])} for c in raw]
    candles = filter_closed_candles(candles, interval, reference_ms)
    return candles[-n:]


def fetch_all_candles(symbol: str, reference_ms: Optional[int] = None) -> Optional[dict[str, list[dict]]]:
    reference_ms = reference_ms or int(time.time() * 1000)
    out = {}
    for tf, n in CANDLE_COUNTS.items():
        candles = get_candles(symbol, tf, n, reference_ms)
        if len(candles) < min(60, n // 2):
            logger.warning("Insufficient %s candles for %s (%d)", tf, symbol, len(candles))
            return None
        out[tf] = candles
    return out


def get_meta_and_ctx() -> Optional[tuple[list[str], list[dict]]]:
    raw = hl_post({"type": "metaAndAssetCtxs"})
    if not raw or not isinstance(raw, list) or len(raw) < 2:
        return None
    universe = [a["name"] for a in raw[0].get("universe", [])]
    return universe, raw[1]


def get_market_snapshot() -> dict[str, dict]:
    """Returns per-symbol: mark price, funding rate, open interest (usd), 24h volume."""
    res = get_meta_and_ctx()
    if not res:
        return {}
    universe, ctxs = res
    out = {}
    for name, ctx in zip(universe, ctxs):
        try:
            mark = float(ctx.get("markPx", 0) or 0)
            oi = float(ctx.get("openInterest", 0) or 0) * mark
            funding = float(ctx.get("funding", 0) or 0)
            vol24 = float(ctx.get("dayNtlVlm", 0) or 0)
            out[name] = {"mark": mark, "oi_usd": oi, "funding": funding, "vol24_usd": vol24}
        except (TypeError, ValueError):
            continue
    return out


def get_l2_book(coin: str) -> Optional[dict]:
    return hl_post({"type": "l2Book", "coin": hl_coin(coin)})


def analyze_orderbook(coin: str) -> dict:
    book = get_l2_book(coin)
    if not book or "levels" not in book:
        return {"spread_pct": None, "imbalance": 0.0, "depth_usd": 0.0}
    try:
        bids, asks = book["levels"][0], book["levels"][1]
        best_bid, best_ask = float(bids[0]["px"]), float(asks[0]["px"])
        spread_pct = (best_ask - best_bid) / best_bid * 100 if best_bid else None
        bid_depth = sum(float(b["px"]) * float(b["sz"]) for b in bids[:10])
        ask_depth = sum(float(a["px"]) * float(a["sz"]) for a in asks[:10])
        total = bid_depth + ask_depth
        imbalance = (bid_depth - ask_depth) / total if total > 0 else 0.0
        return {"spread_pct": spread_pct, "imbalance": imbalance, "depth_usd": total}
    except (KeyError, IndexError, ValueError, ZeroDivisionError):
        return {"spread_pct": None, "imbalance": 0.0, "depth_usd": 0.0}


# ==============================================================================
# INDICATOR LIBRARY
# ==============================================================================

def safe(v, fb: float = 0.0) -> float:
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return fb
        return float(v)
    except (TypeError, ValueError):
        return fb


def ema(vals: list[float], period: int) -> list[float]:
    if not vals:
        return []
    k = 2 / (period + 1)
    n = len(vals)
    # Seed with the SMA of the first `period` values (or all available values,
    # if fewer) rather than the single raw first close. Seeding with a raw
    # price decays only geometrically -- e.g. for EMA_TREND=200 read on the
    # 1D bias timeframe's 220-candle window, ~11% of the seed's weight is
    # still present by the last bar -- which biases a headline macro-bias
    # input toward whatever the price happened to be on an arbitrary
    # start-of-window candle.
    seed_len = min(period, n)
    seed = sum(vals[:seed_len]) / seed_len
    out = [seed] * seed_len
    for v in vals[seed_len:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def sma(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        if i < period - 1:
            out.append(vals[i])
        else:
            out.append(sum(vals[i - period + 1:i + 1]) / period)
    return out


def stdev(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        if i < period - 1:
            out.append(0.0)
        else:
            window = vals[i - period + 1:i + 1]
            m = sum(window) / period
            out.append(math.sqrt(sum((x - m) ** 2 for x in window) / period))
    return out


def rsi(closes: list[float], period: int = RSI_LEN) -> list[float]:
    if len(closes) < period + 1:
        return [50.0] * len(closes)
    out = [50.0] * len(closes)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_gain = sum(gains[1:period + 1]) / period
    avg_loss = sum(losses[1:period + 1]) / period
    for i in range(period + 1, len(closes)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs = avg_gain / avg_loss if avg_loss > 1e-12 else 999.0
        out[i] = 100 - (100 / (1 + rs))
    return out


def atr(highs, lows, closes, period: int = ATR_LEN) -> list[float]:
    trs = [highs[0] - lows[0]]
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        trs.append(tr)
    out = [trs[0]]
    for i in range(1, len(trs)):
        if i < period:
            out.append(sum(trs[:i + 1]) / (i + 1))
        else:
            out.append((out[-1] * (period - 1) + trs[i]) / period)
    return out


def adx_dmi(highs, lows, closes, period: int = ADX_LEN) -> tuple[list[float], list[float], list[float]]:
    n = len(closes)
    if n < period + 2:
        return [0.0] * n, [0.0] * n, [0.0] * n
    plus_dm, minus_dm, trs = [0.0], [0.0], [highs[0] - lows[0]]
    for i in range(1, n):
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))

    def wilder(series):
        out = [0.0] * n
        out[period] = sum(series[1:period + 1])
        for i in range(period + 1, n):
            out[i] = out[i - 1] - (out[i - 1] / period) + series[i]
        return out

    tr_s, pdm_s, mdm_s = wilder(trs), wilder(plus_dm), wilder(minus_dm)
    plus_di, minus_di, dx, adx = [0.0] * n, [0.0] * n, [0.0] * n, [0.0] * n
    for i in range(period, n):
        if tr_s[i] > 1e-12:
            plus_di[i] = 100 * pdm_s[i] / tr_s[i]
            minus_di[i] = 100 * mdm_s[i] / tr_s[i]
        denom = plus_di[i] + minus_di[i]
        dx[i] = 100 * abs(plus_di[i] - minus_di[i]) / denom if denom > 1e-12 else 0.0
    start = period * 2
    if start < n:
        adx[start] = sum(dx[period + 1:start + 1]) / period
        for i in range(start + 1, n):
            adx[i] = (adx[i - 1] * (period - 1) + dx[i]) / period
    return adx, plus_di, minus_di


def bollinger(closes: list[float], period: int = BB_LEN, mult: float = BB_MULT):
    mid = sma(closes, period)
    sd = stdev(closes, period)
    upper = [m + mult * s for m, s in zip(mid, sd)]
    lower = [m - mult * s for m, s in zip(mid, sd)]
    width_pct = [((u - l) / m * 100) if m else 0.0 for u, l, m in zip(upper, lower, mid)]
    return mid, upper, lower, width_pct


def donchian(highs, lows, period: int = DONCHIAN_LEN):
    upper, lower = [], []
    for i in range(len(highs)):
        lo = max(0, i - period + 1)
        upper.append(max(highs[lo:i + 1]))
        lower.append(min(lows[lo:i + 1]))
    return upper, lower


def obv(closes, volumes) -> list[float]:
    out = [0.0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            out.append(out[-1] + volumes[i])
        elif closes[i] < closes[i - 1]:
            out.append(out[-1] - volumes[i])
        else:
            out.append(out[-1])
    return out


def detect_rsi_divergence(closes: list[float], rsi_values: list[float], lookback: int = 25) -> Optional[str]:
    """Simple two-pivot divergence check over the trailing window."""
    if len(closes) < lookback + 2:
        return None
    window_c, window_r = closes[-lookback:], rsi_values[-lookback:]
    lo_idx = min(range(len(window_c)), key=lambda i: window_c[i])
    hi_idx = max(range(len(window_c)), key=lambda i: window_c[i])
    last_c, last_r = window_c[-1], window_r[-1]
    if lo_idx < len(window_c) - 3 and last_c <= window_c[lo_idx] * 1.002 and last_r > window_r[lo_idx]:
        return "bullish"
    if hi_idx < len(window_c) - 3 and last_c >= window_c[hi_idx] * 0.998 and last_r < window_r[hi_idx]:
        return "bearish"
    return None


_INDICATOR_CACHE: dict[str, dict] = {}


def compute_indicators(candles: list[dict]) -> dict:
    closes = [c["c"] for c in candles]
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
    vols = [c["v"] for c in candles]
    adx_v, plus_di, minus_di = adx_dmi(highs, lows, closes)
    bb_mid, bb_up, bb_lo, bb_width = bollinger(closes)
    don_up, don_lo = donchian(highs, lows)
    rsi_v = rsi(closes)
    return {
        "closes": closes, "highs": highs, "lows": lows, "vols": vols,
        "ema_fast": ema(closes, EMA_FAST), "ema_slow": ema(closes, EMA_SLOW),
        "ema_trend": ema(closes, EMA_TREND),
        "rsi": rsi_v, "atr": atr(highs, lows, closes),
        "adx": adx_v, "plus_di": plus_di, "minus_di": minus_di,
        "bb_mid": bb_mid, "bb_up": bb_up, "bb_lo": bb_lo, "bb_width": bb_width,
        "don_up": don_up, "don_lo": don_lo, "obv": obv(closes, vols),
        "rsi_divergence": detect_rsi_divergence(closes, rsi_v),
    }


_INDICATOR_CACHE_MAX_ENTRIES = 256


def get_cached_indicators(symbol: str, tf: str, candles: list[dict]) -> dict:
    key = f"{symbol}:{tf}:{candles[-1]['t'] if candles else 0}"
    if key not in _INDICATOR_CACHE:
        if len(_INDICATOR_CACHE) >= _INDICATOR_CACHE_MAX_ENTRIES:
            # bound memory without evicting every entry on every insert (a scan
            # touches 3 timeframes x len(WATCHLIST) keys, well under the cap)
            _INDICATOR_CACHE.pop(next(iter(_INDICATOR_CACHE)))
        _INDICATOR_CACHE[key] = compute_indicators(candles)
    return _INDICATOR_CACHE[key]


def percentile_of_last(series: list[float], lookback: int = 100) -> float:
    if len(series) < 5:
        return 50.0
    window = series[-lookback:]
    last = window[-1]
    rank = sum(1 for v in window if v <= last)
    return 100.0 * rank / len(window)


# ==============================================================================
# STATE MANAGEMENT
# ==============================================================================

def _default_state() -> dict:
    return {
        "version": "1.0.0",
        "created": datetime.now(timezone.utc).isoformat(),
        "active_signals": [],
        "signal_history": [],
        "cooldowns": {},
        "atr_pct_memory": {},
        "daily": {"date": None, "realized_pct": 0.0, "signal_count": 0, "paused": False},
        "correlation_returns": {},
        "bar_index": 0,
    }


def load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        return _default_state()
    try:
        with open(STATE_PATH, "r") as f:
            state = json.load(f)
        base = _default_state()
        for k, v in base.items():
            state.setdefault(k, v)
        return state
    except (json.JSONDecodeError, OSError) as e:
        logger.error("Failed to load state.json (%s); starting fresh.", e)
        return _default_state()


def save_state(state: dict) -> None:
    if DRY_RUN:
        logger.info("[DRY-RUN] state.json write suppressed.")
        return
    tmp_path = STATE_PATH + ".tmp"
    try:
        with open(tmp_path, "w") as f:
            json.dump(state, f, indent=2, default=str)
        os.replace(tmp_path, STATE_PATH)
    except OSError as e:
        logger.error("Failed to save state.json: %s", e)


def prune_state(state: dict, max_history: int = 1000, max_days: int = 30) -> None:
    cutoff = time.time() - max_days * 86400
    state["signal_history"] = [h for h in state["signal_history"] if h.get("ts", 0) >= cutoff][-max_history:]
    active_cooldowns = {}
    for k, v in state.get("cooldowns", {}).items():
        if v.get("bar_index", 0) >= state.get("bar_index", 0) - 200:
            active_cooldowns[k] = v
    state["cooldowns"] = active_cooldowns


def utc_day_key(reference_ms: Optional[int] = None) -> str:
    dt = datetime.fromtimestamp((reference_ms or int(time.time() * 1000)) / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def roll_daily_bucket(state: dict, reference_ms: int) -> None:
    today = utc_day_key(reference_ms)
    if state["daily"].get("date") != today:
        state["daily"] = {"date": today, "realized_pct": 0.0, "signal_count": 0, "paused": False}


def daily_loss_limit_breached(state: dict) -> bool:
    return state["daily"].get("realized_pct", 0.0) <= DAILY_LOSS_LIMIT_PCT or state["daily"].get("paused", False)


# ==============================================================================
# REGIME DETECTION
# ==============================================================================

@dataclass
class RegimeVector:
    trend_strength: float       # 0-1, from ADX
    volatility_pctile: float    # 0-100, ATR percentile vs own history
    bb_width_pctile: float      # 0-100, compression (low) vs expansion (high)
    noise_index: float          # 0-1, higher = choppier / more mean-reverting
    btc_bias: str                # "bullish" | "bearish" | "neutral"
    btc_strength: float          # 0-1
    label: str = "neutral"

    def is_clean_trend(self) -> bool:
        return self.trend_strength >= 0.5 and self.noise_index < 0.45

    def is_choppy(self) -> bool:
        return self.trend_strength < 0.35 or self.bb_width_pctile < 20 or self.noise_index > 0.65

    def is_high_vol(self) -> bool:
        return self.volatility_pctile > 85


def compute_noise_index(candles: list[dict], lookback: int = 30) -> float:
    closes = [c["c"] for c in candles[-lookback:]]
    if len(closes) < 5:
        return 0.5
    e = ema(closes, min(10, len(closes) - 1))
    crossings = sum(1 for i in range(1, len(closes)) if (closes[i] - e[i]) * (closes[i - 1] - e[i - 1]) < 0)
    return min(1.0, crossings / max(1, lookback - 1) * 2.2)


def update_atr_pct_memory(state: dict, symbol: str, atr_pct: float) -> float:
    mem = state["atr_pct_memory"].setdefault(symbol, [])
    mem.append(atr_pct)
    state["atr_pct_memory"][symbol] = mem[-150:]
    return percentile_of_last(state["atr_pct_memory"][symbol], 150)


def compute_btc_regime(btc_indicators: dict) -> tuple[str, float]:
    adx_v = btc_indicators["adx"][-1]
    plus_di, minus_di = btc_indicators["plus_di"][-1], btc_indicators["minus_di"][-1]
    ema_fast, ema_slow = btc_indicators["ema_fast"][-1], btc_indicators["ema_slow"][-1]
    strength = min(1.0, adx_v / 40.0)
    if ema_fast > ema_slow and plus_di > minus_di:
        return "bullish", strength
    if ema_fast < ema_slow and minus_di > plus_di:
        return "bearish", strength
    return "neutral", strength * 0.5


def build_regime_vector(state: dict, symbol: str, ind_exec: dict, candles_exec: list[dict],
                         btc_bias: str, btc_strength: float) -> RegimeVector:
    adx_v = ind_exec["adx"][-1]
    trend_strength = min(1.0, adx_v / 35.0)
    atr_pct = ind_exec["atr"][-1] / ind_exec["closes"][-1] * 100 if ind_exec["closes"][-1] else 0.0
    vol_pctile = update_atr_pct_memory(state, symbol, atr_pct)
    bb_width_pctile = percentile_of_last(ind_exec["bb_width"], 100)
    noise = compute_noise_index(candles_exec)
    rv = RegimeVector(trend_strength, vol_pctile, bb_width_pctile, noise, btc_bias, btc_strength)
    if rv.is_clean_trend():
        rv.label = "clean_trend"
    elif rv.is_choppy():
        rv.label = "choppy"
    elif rv.is_high_vol():
        rv.label = "high_volatility"
    else:
        rv.label = "neutral"
    return rv


def adaptive_min_score(regime: RegimeVector) -> float:
    """Fixed regime-conditioned rule table -- see module docstring."""
    score = BASE_MIN_SCORE
    if regime.label == "clean_trend":
        score -= TREND_REGIME_SCORE_RELIEF
    elif regime.label == "choppy":
        score += CHOP_REGIME_SCORE_PENALTY
    return max(45.0, min(80.0, score))


def adaptive_liquidity_floor(regime: RegimeVector) -> float:
    if regime.label == "clean_trend":
        return LIQUIDITY_MIN_24H_VOL_USD * 0.75
    if regime.label == "choppy":
        return LIQUIDITY_MIN_24H_VOL_USD * 1.4
    return LIQUIDITY_MIN_24H_VOL_USD


def adaptive_sl_multiple(regime: RegimeVector) -> float:
    base = 1.4
    if regime.is_high_vol():
        base += 0.5
    if regime.label == "choppy":
        base += 0.2
    return base


def adaptive_followthrough_bars(regime: RegimeVector) -> int:
    return 2 if regime.is_high_vol() else 1


def macro_bias_1d(ind_bias: dict) -> str:
    """Real macro-bias read from the 1D timeframe: price vs 50-EMA, direction
    of the 50/200 EMA stack, and a minimum daily ADX so a flat/choppy daily
    tape reports 'neutral' rather than a spurious direction. This is the
    fix for the previously-fetched-but-unused 1D candle set."""
    price = ind_bias["closes"][-1]
    ema_s, ema_t = ind_bias["ema_slow"][-1], ind_bias["ema_trend"][-1]
    adx_v = ind_bias["adx"][-1]
    if adx_v < MACRO_BIAS_MIN_ADX:
        return "neutral"
    if price > ema_s and ema_s >= ema_t:
        return "bullish"
    if price < ema_s and ema_s <= ema_t:
        return "bearish"
    return "neutral"


def macro_alignment(direction: str, macro_bias: str) -> tuple[float, Optional[str]]:
    """Score delta + note for how a candidate's direction relates to the 1D
    macro bias. Opposed = real penalty (macro bias is a genuine risk signal,
    not just another vote); aligned = modest bonus; neutral = no-op."""
    if macro_bias == "neutral":
        return 0.0, None
    aligned = (direction == "long" and macro_bias == "bullish") or (direction == "short" and macro_bias == "bearish")
    if aligned:
        return MACRO_ALIGNED_BONUS, "1D macro bias aligned"
    return -MACRO_OPPOSED_PENALTY, "1D macro bias opposed -- confidence reduced"


# ==============================================================================
# MARKET STRUCTURE (swings, order blocks, FVGs, liquidity, sweeps, MSS)
# ==============================================================================

@dataclass
class Swing:
    index: int
    price: float
    kind: str  # "high" | "low"


def find_swings(candles: list[dict], left: int = 2, right: int = 2) -> list[Swing]:
    out = []
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
    for i in range(left, len(candles) - right):
        if highs[i] == max(highs[i - left:i + right + 1]):
            out.append(Swing(i, highs[i], "high"))
        if lows[i] == min(lows[i - left:i + right + 1]):
            out.append(Swing(i, lows[i], "low"))
    return out


@dataclass
class StructureState:
    bias: str  # "bullish" | "bearish" | "neutral"
    last_high: Optional[float]
    last_low: Optional[float]
    bos_direction: Optional[str]


def analyze_structure(candles: list[dict], swings: list[Swing]) -> StructureState:
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    if len(highs) < 2 or len(lows) < 2:
        return StructureState("neutral", None, None, None)
    hh = highs[-1].price > highs[-2].price
    hl = lows[-1].price > lows[-2].price
    lh = highs[-1].price < highs[-2].price
    ll = lows[-1].price < lows[-2].price
    bos = None
    last_close = candles[-1]["c"]
    if last_close > highs[-1].price:
        bos = "bullish"
    elif last_close < lows[-1].price:
        bos = "bearish"
    if hh and hl:
        bias = "bullish"
    elif lh and ll:
        bias = "bearish"
    else:
        bias = "neutral"
    return StructureState(bias, highs[-1].price, lows[-1].price, bos)


@dataclass
class Zone:
    low: float
    high: float
    kind: str  # "bullish_ob" | "bearish_ob" | "bullish_fvg" | "bearish_fvg"
    index: int
    tested: bool = False

    def mid(self) -> float:
        return (self.low + self.high) / 2

    def contains(self, price: float, buf: float = 0.0) -> bool:
        return (self.low - buf) <= price <= (self.high + buf)


def find_order_blocks(candles: list[dict], atr_vals: list[float], lookback: int = 60) -> list[Zone]:
    zones = []
    start = max(1, len(candles) - lookback)
    for i in range(start, len(candles) - 1):
        c, nxt = candles[i], candles[i + 1]
        body = abs(c["c"] - c["o"])
        move = abs(nxt["c"] - nxt["o"])
        if move > atr_vals[i] * 1.3 and body > 0:
            if nxt["c"] > nxt["o"] and c["c"] < c["o"]:
                zones.append(Zone(c["l"], c["h"], "bullish_ob", i))
            elif nxt["c"] < nxt["o"] and c["c"] > c["o"]:
                zones.append(Zone(c["l"], c["h"], "bearish_ob", i))
    return zones[-12:]


def find_fvgs(candles: list[dict], lookback: int = 60) -> list[Zone]:
    zones = []
    start = max(2, len(candles) - lookback)
    for i in range(start, len(candles)):
        a, c = candles[i - 2], candles[i]
        if c["l"] > a["h"]:
            zones.append(Zone(a["h"], c["l"], "bullish_fvg", i))
        elif c["h"] < a["l"]:
            zones.append(Zone(c["h"], a["l"], "bearish_fvg", i))
    return zones[-12:]


def mark_untested(zones: list[Zone], candles: list[dict]) -> list[Zone]:
    for z in zones:
        for c in candles[z.index + 1:]:
            if z.contains(c["c"]) or (c["l"] <= z.high and c["h"] >= z.low):
                z.tested = True
                break
    return zones


def cluster_levels(levels: list[float], tol_pct: float = 0.0015) -> list[tuple[float, int]]:
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


def build_liquidity_pools(swings: list[Swing]) -> dict:
    highs = [s.price for s in swings if s.kind == "high"]
    lows = [s.price for s in swings if s.kind == "low"]
    return {"resistance": cluster_levels(highs), "support": cluster_levels(lows)}


def detect_sweep(candles: list[dict], pools: dict, direction: str, lookback: int = 10) -> Optional[dict]:
    """direction 'long' -> sweep of support (stop hunt below) then reclaim.

    Scans every level and keeps the most recent qualifying sweep (ties broken
    by nearest price to the last close), rather than returning the first
    match found while iterating levels in ascending price order -- otherwise
    this can pick the lowest support / highest resistance level touched
    anywhere in the lookback, even when a more recent, closer sweep also
    qualifies, producing an unnecessarily distant sweep level and an
    oversized stop."""
    recent = candles[-lookback:]
    if len(recent) < 2:
        return None
    levels = pools["support"] if direction == "long" else pools["resistance"]
    if not levels:
        return None
    last_close = recent[-1]["c"]
    best: Optional[dict] = None
    best_dist = None
    for level, weight in levels:
        reclaimed = (last_close > level) if direction == "long" else (last_close < level)
        if not reclaimed:
            continue
        wick_idxs = [i for i, c in enumerate(recent[:-1])
                     if ((c["l"] < level) if direction == "long" else (c["h"] > level))]
        if not wick_idxs:
            continue
        bars_ago = len(recent) - max(wick_idxs)  # most recent wick of this level
        dist = abs(level - last_close)
        if best is None or bars_ago < best["bars_ago"] or (bars_ago == best["bars_ago"] and dist < best_dist):
            best = {"level": level, "weight": weight, "bars_ago": bars_ago}
            best_dist = dist
    return best


def premium_discount_zone(candles: list[dict], lookback: int = 50) -> dict:
    window = candles[-lookback:]
    hi = max(c["h"] for c in window)
    lo = min(c["l"] for c in window)
    last = candles[-1]["c"]
    pct = (last - lo) / (hi - lo) if hi > lo else 0.5
    zone = "premium" if pct > 0.618 else ("discount" if pct < 0.382 else "equilibrium")
    return {"zone": zone, "pct": pct, "range_high": hi, "range_low": lo}


def detect_mss(candles_exec: list[dict], direction: str, lookback: int = 30) -> Optional[dict]:
    """Market-structure-shift confirmation on the execution timeframe."""
    swings = find_swings(candles_exec[-lookback:])
    if len(swings) < 2:
        return None
    last_close = candles_exec[-1]["c"]
    if direction == "long":
        recent_highs = [s.price for s in swings if s.kind == "high"]
        if recent_highs and last_close > recent_highs[-1]:
            return {"confirmed": True, "level": recent_highs[-1]}
    else:
        recent_lows = [s.price for s in swings if s.kind == "low"]
        if recent_lows and last_close < recent_lows[-1]:
            return {"confirmed": True, "level": recent_lows[-1]}
    return None


# ==============================================================================
# DERIVATIVES / ORDERFLOW / VOLUME CONFIRMATION
# ==============================================================================

def funding_oi_read(snapshot: dict, state: dict, symbol: str, direction: str) -> dict:
    """Squeeze / crowded-trade detection from funding + OI. Frequency-additive:
    can add confidence for setups price action alone would rate as merely average."""
    sym_data = snapshot.get(symbol, {})
    funding = sym_data.get("funding")
    oi_usd = sym_data.get("oi_usd")
    hist = state.setdefault("correlation_returns", {}).setdefault(f"funding:{symbol}", [])
    hist.append(funding if funding is not None else 0.0)
    state["correlation_returns"][f"funding:{symbol}"] = hist[-100:]
    fund_pctile = percentile_of_last(hist, 100) if len(hist) >= 10 else 50.0

    squeeze_bonus = 0.0
    note = None
    if funding is not None:
        # Longs crowded (high positive funding) + long signal -> against-crowd risk unless
        # combined with a genuine short squeeze setup (direction == short benefits instead).
        if direction == "short" and fund_pctile > 85:
            squeeze_bonus += 6.0
            note = "elevated funding favors short squeeze fade"
        elif direction == "long" and fund_pctile < 15:
            squeeze_bonus += 6.0
            note = "deeply negative funding favors long squeeze"
        elif direction == "long" and fund_pctile > 90:
            squeeze_bonus -= 5.0
            note = "crowded long funding -- headwind"
        elif direction == "short" and fund_pctile < 10:
            squeeze_bonus -= 5.0
            note = "crowded short funding -- headwind"
    return {"funding": funding, "oi_usd": oi_usd, "fund_pctile": fund_pctile,
            "squeeze_bonus": squeeze_bonus, "note": note}


def orderflow_proxy(candles: list[dict], direction: str, lookback: int = 24) -> dict:
    """Proxy for buy/sell pressure using candle body position within range (no L3 data)."""
    window = candles[-lookback:]
    pressure = 0.0
    for c in window:
        rng = c["h"] - c["l"]
        if rng <= 0:
            continue
        close_loc = (c["c"] - c["l"]) / rng
        pressure += (close_loc - 0.5) * 2 * c["v"]
    total_vol = sum(c["v"] for c in window) or 1.0
    net = pressure / total_vol
    aligned = (net > 0.05 and direction == "long") or (net < -0.05 and direction == "short")
    return {"net_pressure": net, "aligned": aligned}


def volume_confirmation(candles: list[dict], ind: dict) -> dict:
    recent_vol = candles[-1]["v"]
    avg_vol = sum(c["v"] for c in candles[-21:-1]) / 20 if len(candles) > 21 else recent_vol
    ratio = recent_vol / avg_vol if avg_vol > 0 else 1.0
    return {"ratio": ratio, "confirmed": ratio >= 1.15}


# ==============================================================================
# CANDIDATE / PATHWAY MODEL
# ==============================================================================

@dataclass
class Candidate:
    symbol: str
    direction: str  # "long" | "short"
    pathway: str     # "liquidity_reversal" | "trend_continuation" | "momentum_breakout"
    entry: float
    sl: float
    tp1: float
    tp2: float
    raw_score: float
    confluences: list[str] = field(default_factory=list)
    duration_hint: str = "intraday"

    def rr(self) -> float:
        risk = abs(self.entry - self.sl)
        reward = abs(self.tp2 - self.entry)
        return reward / risk if risk > 1e-12 else 0.0


def pathway_liquidity_reversal(symbol: str, candles_exec: list[dict], ind_exec: dict,
                                struct: StructureState, pools: dict, regime: RegimeVector) -> list[Candidate]:
    out = []
    price = candles_exec[-1]["c"]
    atr_val = ind_exec["atr"][-1]
    # Exclude the current/last candle from the "already tested" scan -- it's
    # the same candle whose close is used as `price` below, so including it
    # here would flip z.tested True on the exact first-touch reaction this
    # pathway is meant to reward, before the `not z.tested` check ever runs.
    ob_zones = mark_untested(find_order_blocks(candles_exec, ind_exec["atr"]), candles_exec[:-1])
    fvg_zones = mark_untested(find_fvgs(candles_exec), candles_exec[:-1])
    for direction in ("long", "short"):
        sweep = detect_sweep(candles_exec, pools, direction)
        if not sweep:
            continue
        mss = detect_mss(candles_exec, direction)
        if not mss or not mss["confirmed"]:
            continue
        # HTF structural bias check (previously accepted but never read): a
        # reversal is allowed with neutral HTF structure, but is blocked when
        # 4H structure is actively opposed to the reversal direction -- a
        # reversal fighting both the sweep-origin trend AND HTF bias is the
        # lowest-quality setup this pathway can produce.
        if direction == "long" and struct.bias == "bearish":
            continue
        if direction == "short" and struct.bias == "bullish":
            continue
        pdz = premium_discount_zone(candles_exec)
        confluences = [f"liquidity sweep @ {sweep['level']:.4g} (weight {sweep['weight']})",
                       f"MSS confirmed vs {mss['level']:.4g}"]
        struct_bonus = 0.0
        if direction == "long" and struct.bias == "bullish":
            confluences.append("HTF (4H) structure bullish -- aligned")
            struct_bonus = 6.0
        elif direction == "short" and struct.bias == "bearish":
            confluences.append("HTF (4H) structure bearish -- aligned")
            struct_bonus = 6.0
        if direction == "long" and pdz["zone"] == "discount":
            confluences.append("entry in discount zone")
        elif direction == "short" and pdz["zone"] == "premium":
            confluences.append("entry in premium zone")

        zone_bonus = 0.0
        want_kind = "bullish" if direction == "long" else "bearish"
        for z in ob_zones + fvg_zones:
            if not z.tested and z.kind.startswith(want_kind) and z.contains(price, buf=atr_val * 0.15):
                label = "order block" if "ob" in z.kind else "fair value gap"
                confluences.append(f"reacting from untested {label}")
                zone_bonus = 5.0
                break

        div_bonus = 0.0
        rsi_div = ind_exec.get("rsi_divergence")
        if (direction == "long" and rsi_div == "bullish") or (direction == "short" and rsi_div == "bearish"):
            confluences.append(f"RSI {rsi_div} divergence")
            div_bonus = 5.0

        sl_mult = adaptive_sl_multiple(regime)
        if direction == "long":
            sl = min(sweep["level"], price) - atr_val * sl_mult * 0.4
            tp1 = price + atr_val * sl_mult * 1.5
            tp2 = price + atr_val * sl_mult * 2.6
        else:
            sl = max(sweep["level"], price) + atr_val * sl_mult * 0.4
            tp1 = price - atr_val * sl_mult * 1.5
            tp2 = price - atr_val * sl_mult * 2.6
        raw_score = 55 + min(15, sweep["weight"] * 4) + struct_bonus + zone_bonus + div_bonus
        out.append(Candidate(symbol, direction, "liquidity_reversal", price, sl, tp1, tp2, raw_score, confluences, "intraday"))
    return out


def pathway_trend_continuation(symbol: str, candles_exec: list[dict], ind_exec: dict,
                                struct_htf: StructureState, regime: RegimeVector) -> list[Candidate]:
    out = []
    price = candles_exec[-1]["c"]
    atr_val = ind_exec["atr"][-1]
    ema_f, ema_s, ema_t = ind_exec["ema_fast"][-1], ind_exec["ema_slow"][-1], ind_exec["ema_trend"][-1]
    rsi_v = ind_exec["rsi"][-1]
    plus_di, minus_di = ind_exec["plus_di"][-1], ind_exec["minus_di"][-1]

    def pullback_reset(direction: str) -> bool:
        return (35 <= rsi_v <= 55) if direction == "long" else (45 <= rsi_v <= 65)

    if struct_htf.bias == "bullish" and ema_f > ema_s > ema_t and price > ema_s and plus_di > minus_di:
        confluences = ["HTF structure bullish", "EMA stack aligned bullish", "ADX +DI leading"]
        if pullback_reset("long"):
            confluences.append("RSI pullback reset")
        sl_mult = adaptive_sl_multiple(regime)
        sl = min(ind_exec["ema_slow"][-1], price - atr_val * sl_mult)
        tp1 = price + atr_val * sl_mult * 1.6
        tp2 = price + atr_val * sl_mult * 2.8
        raw_score = 58 + (6 if "RSI pullback reset" in confluences else 0) + min(10, regime.trend_strength * 12)
        out.append(Candidate(symbol, "long", "trend_continuation", price, sl, tp1, tp2, raw_score, confluences, "swing"))

    if struct_htf.bias == "bearish" and ema_f < ema_s < ema_t and price < ema_s and minus_di > plus_di:
        confluences = ["HTF structure bearish", "EMA stack aligned bearish", "ADX -DI leading"]
        if pullback_reset("short"):
            confluences.append("RSI pullback reset")
        sl_mult = adaptive_sl_multiple(regime)
        sl = max(ind_exec["ema_slow"][-1], price + atr_val * sl_mult)
        tp1 = price - atr_val * sl_mult * 1.6
        tp2 = price - atr_val * sl_mult * 2.8
        raw_score = 58 + (6 if "RSI pullback reset" in confluences else 0) + min(10, regime.trend_strength * 12)
        out.append(Candidate(symbol, "short", "trend_continuation", price, sl, tp1, tp2, raw_score, confluences, "swing"))

    return out


def pathway_momentum_breakout(symbol: str, candles_exec: list[dict], ind_exec: dict,
                               regime: RegimeVector, followthrough_bars: int) -> list[Candidate]:
    out = []
    price = candles_exec[-1]["c"]
    atr_val = ind_exec["atr"][-1]
    don_up, don_lo = ind_exec["don_up"], ind_exec["don_lo"]
    if len(candles_exec) < DONCHIAN_LEN + followthrough_bars + 1:
        return out
    prior_upper = don_up[-(followthrough_bars + 1)]
    prior_lower = don_lo[-(followthrough_bars + 1)]
    recent = candles_exec[-followthrough_bars:]
    vol_ok = volume_confirmation(candles_exec, ind_exec)["confirmed"]

    broke_up = all(c["c"] > prior_upper for c in recent)
    broke_down = all(c["c"] < prior_lower for c in recent)

    obv_v = ind_exec["obv"]
    obv_rising = len(obv_v) > 10 and obv_v[-1] > obv_v[-10]
    obv_falling = len(obv_v) > 10 and obv_v[-1] < obv_v[-10]

    if broke_up and vol_ok:
        confluences = [f"{followthrough_bars}-bar close-through Donchian upper", "volume confirmed"]
        if ind_exec["bb_width"][-1] > ind_exec["bb_width"][-6] and percentile_of_last(ind_exec["bb_width"], 60) < 40:
            confluences.append("breakout from volatility compression")
        obv_bonus = 0.0
        if obv_rising:
            confluences.append("OBV confirms accumulation")
            obv_bonus = 4.0
        sl = prior_upper - atr_val * 0.6
        tp1 = price + atr_val * adaptive_sl_multiple(regime) * 1.4
        tp2 = price + atr_val * adaptive_sl_multiple(regime) * 2.4
        raw_score = 56 + (6 if "compression" in confluences[-1] else 0) + obv_bonus
        out.append(Candidate(symbol, "long", "momentum_breakout", price, sl, tp1, tp2, raw_score, confluences, "intraday"))

    if broke_down and vol_ok:
        confluences = [f"{followthrough_bars}-bar close-through Donchian lower", "volume confirmed"]
        if ind_exec["bb_width"][-1] > ind_exec["bb_width"][-6] and percentile_of_last(ind_exec["bb_width"], 60) < 40:
            confluences.append("breakout from volatility compression")
        obv_bonus = 0.0
        if obv_falling:
            confluences.append("OBV confirms distribution")
            obv_bonus = 4.0
        sl = prior_lower + atr_val * 0.6
        tp1 = price - atr_val * adaptive_sl_multiple(regime) * 1.4
        tp2 = price - atr_val * adaptive_sl_multiple(regime) * 2.4
        raw_score = 56 + (6 if "compression" in confluences[-1] else 0) + obv_bonus
        out.append(Candidate(symbol, "short", "momentum_breakout", price, sl, tp1, tp2, raw_score, confluences, "intraday"))

    return out


# ==============================================================================
# ENSEMBLE SCORING
# ==============================================================================

def logistic(x: float) -> float:
    return 1 / (1 + math.exp(-x))


def score_candidate(cand: Candidate, all_pathway_directions: dict[str, list[str]], regime: RegimeVector,
                     funding_read: dict, orderflow: dict, ob_analysis: dict,
                     macro_bias: str = "neutral") -> tuple[float, list[str]]:
    """Ensemble-agreement scoring: reward independent pathway agreement, penalize
    genuine conflict, never simple averaging."""
    score = cand.raw_score
    notes = list(cand.confluences)

    macro_delta, macro_note = macro_alignment(cand.direction, macro_bias)
    if macro_note:
        score += macro_delta
        notes.append(macro_note)

    # Only pathways that fired a single, unambiguous direction can count as
    # "agreeing" or "conflicting" -- a pathway that fired BOTH long and short
    # in the same call (e.g. a choppy tape triggering both a support and a
    # resistance sweep-reclaim) is internally contradictory and should be a
    # red flag, not free confluence points for whichever direction someone
    # else proposes.
    unambiguous = {pw: dirs for pw, dirs in all_pathway_directions.items()
                    if dirs and len(set(dirs)) == 1}
    agree = sum(1 for pw, dirs in unambiguous.items()
                if pw != cand.pathway and cand.direction in dirs)
    conflict = sum(1 for pw, dirs in unambiguous.items()
                    if pw != cand.pathway and cand.direction not in dirs)
    if agree >= 1:
        score += 10 + 4 * (agree - 1)
        notes.append(f"{agree} independent pathway(s) agree")
    if conflict >= 1 and agree == 0:
        score -= 12
        notes.append("conflicting pathway signal -- confidence reduced")

    score += funding_read.get("squeeze_bonus", 0.0)
    if funding_read.get("note"):
        notes.append(funding_read["note"])

    if orderflow.get("aligned"):
        score += 5
        notes.append("orderflow proxy aligned")

    if ob_analysis.get("spread_pct") is not None and ob_analysis["spread_pct"] < 0.06:
        score += 3
        notes.append("tight spread")
    if ob_analysis.get("imbalance", 0) and abs(ob_analysis["imbalance"]) > 0.15:
        favors_long = ob_analysis["imbalance"] > 0
        if (favors_long and cand.direction == "long") or (not favors_long and cand.direction == "short"):
            score += 3
            notes.append("book imbalance aligned")

    if regime.is_clean_trend() and cand.pathway == "trend_continuation":
        score += 4
    if regime.is_choppy() and cand.pathway == "momentum_breakout":
        score -= 6
        notes.append("breakout pathway discounted in choppy regime")

    score = max(0.0, min(100.0, score))
    return score, notes


def grade_for_confidence(confidence: float) -> str:
    if confidence >= 85:
        return "A+"
    if confidence >= 75:
        return "A"
    if confidence >= 68:
        return "B+"
    if confidence >= 62:
        return "B"
    return "C"


# ==============================================================================
# CORRELATION CONTROL
# ==============================================================================

def compute_returns(candles: list[dict], lookback: int) -> list[float]:
    closes = [c["c"] for c in candles[-lookback:]]
    return [(closes[i] / closes[i - 1] - 1) for i in range(1, len(closes))] if len(closes) > 1 else []


def pearson(a: list[float], b: list[float]) -> float:
    n = min(len(a), len(b))
    if n < 5:
        return 0.0
    a, b = a[-n:], b[-n:]
    ma, mb = sum(a) / n, sum(b) / n
    cov = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    va = math.sqrt(sum((x - ma) ** 2 for x in a))
    vb = math.sqrt(sum((x - mb) ** 2 for x in b))
    return cov / (va * vb) if va > 1e-12 and vb > 1e-12 else 0.0


def build_correlation_clusters(returns_by_symbol: dict[str, list[float]]) -> list[set[str]]:
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
            r = pearson(returns_by_symbol[symbols[i]], returns_by_symbol[symbols[j]])
            if r >= CORR_CLUSTER_THRESHOLD:
                union(symbols[i], symbols[j])
    clusters: dict[str, set[str]] = {}
    for s in symbols:
        clusters.setdefault(find(s), set()).add(s)
    return list(clusters.values())


def dedup_correlated(ranked: list[Candidate], clusters: list[set[str]]) -> list[Candidate]:
    """Treat correlated signals as one effective bet -- keep only the highest scorer per
    cluster per direction. This does not reduce true opportunity, it avoids double counting."""

    def cluster_of(sym: str) -> frozenset:
        for c in clusters:
            if sym in c:
                return frozenset(c)
        return frozenset([sym])

    seen: dict[tuple, Candidate] = {}
    out = []
    for cand in ranked:
        key = (cluster_of(cand.symbol), cand.direction)
        if key not in seen:
            seen[key] = cand
            out.append(cand)
        else:
            log_suppressed(cand.symbol, cand.direction, cand.pathway,
                            f"correlated with already-selected {seen[key].symbol}", 0)
    return out


# ==============================================================================
# HARD FILTERS, COOLDOWN, FRESHNESS
# ==============================================================================

def passes_hard_filters(symbol: str, snapshot: dict, ob_analysis: dict, atr_pct: float,
                         cand: Candidate, regime: RegimeVector,
                         check_liquidity: bool = True, check_spread: bool = True) -> tuple[bool, str]:
    """`check_liquidity`/`check_spread` default True for the live path. The
    backtest calls this with both False and documents why in its report:
    no historical L2 order-book or true rolling 24h-volume series exists to
    reconstruct these two checks faithfully. Every other check (R:R, ATR
    sanity) is shared identically between live and backtest so both paths
    run through this one function rather than two drifting copies of it."""
    if check_liquidity:
        vol24 = snapshot.get(symbol, {}).get("vol24_usd", 0.0)
        floor = adaptive_liquidity_floor(regime)
        if vol24 < floor:
            return False, f"liquidity below floor ({vol24:,.0f} < {floor:,.0f})"
    if check_spread and ob_analysis.get("spread_pct") is not None and ob_analysis["spread_pct"] > 0.25:
        return False, f"spread too wide ({ob_analysis['spread_pct']:.2f}%)"
    if cand.rr() < MIN_RR:
        return False, f"R:R below minimum ({cand.rr():.2f} < {MIN_RR})"
    if atr_pct <= 0 or atr_pct > 15:
        return False, f"ATR% out of sane bounds ({atr_pct:.2f})"
    return True, "ok"


def check_cooldown(state: dict, symbol: str, direction: str, bar_index: int) -> bool:
    key = f"{symbol}:{direction}"
    entry = state["cooldowns"].get(key)
    if not entry:
        return True
    return bar_index - entry.get("bar_index", -999) >= COOLDOWN_BARS_EXEC


def update_cooldown(state: dict, symbol: str, direction: str, bar_index: int) -> None:
    state["cooldowns"][f"{symbol}:{direction}"] = {"bar_index": bar_index, "ts": time.time()}


def symbol_has_open_signal(state: dict, symbol: str) -> bool:
    """One-signal-per-symbol policy: True if `symbol` already has an active,
    unresolved signal (regardless of direction). A symbol only becomes
    eligible again once that signal resolves via SL or TP2 in
    `check_active_signals`/`_resolve`."""
    if not ONE_SIGNAL_PER_SYMBOL:
        return False
    return any(s.get("symbol") == symbol for s in state.get("active_signals", []))


def signal_still_fresh(cand: Candidate, latest_price: float, atr_val: float) -> bool:
    """Compare the candidate's generation-time entry against a price captured
    LATER (after ranking/dedup/portfolio-gating, right before send -- see
    `run_scan`'s pre-send freshness pass). Comparing against the same
    generation-time price it was built from is a no-op by construction, which
    was the original bug: drift is always exactly 0 in that case."""
    drift = abs(latest_price - cand.entry)
    return drift <= SIGNAL_FRESHNESS_MAX_DRIFT_ATR * atr_val


# ==============================================================================
# RISK MANAGEMENT
# ==============================================================================

def position_size_pct(cand: Candidate, account_equity_pct: float = 100.0) -> float:
    """Returns suggested position notional as % of equity, based on fixed
    per-trade risk % and stop distance, scaled by the fraction of account
    equity currently available (`account_equity_pct`; 100.0 = fully
    available, matching prior behavior)."""
    risk_frac = abs(cand.entry - cand.sl) / cand.entry
    if risk_frac <= 1e-9:
        return 0.0
    size_pct = (PER_TRADE_RISK_PCT / 100.0) / risk_frac * 100.0
    size_pct *= max(0.0, min(1.0, account_equity_pct / 100.0))
    return min(size_pct, MAX_PORTFOLIO_EXPOSURE_PCT / max(1, MAX_CONCURRENT_SIGNALS))


def portfolio_capacity_ok(state: dict) -> tuple[bool, str]:
    active = state.get("active_signals", [])
    if len(active) >= MAX_CONCURRENT_SIGNALS:
        return False, f"max concurrent signals reached ({MAX_CONCURRENT_SIGNALS})"
    exposure = sum(a.get("size_pct", 0.0) for a in active)
    if exposure >= MAX_PORTFOLIO_EXPOSURE_PCT:
        return False, f"max portfolio exposure reached ({exposure:.1f}%)"
    if daily_loss_limit_breached(state):
        return False, "daily loss limit breached -- paused for remainder of UTC day"
    return True, "ok"


# ==============================================================================
# TELEGRAM
# ==============================================================================

def send_telegram(text: str) -> Optional[int]:
    if DRY_RUN:
        logger.info("[DRY-RUN] Telegram send suppressed:\n%s", text)
        return None
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram credentials missing; skipping send.")
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
        if resp.status_code == 200:
            return resp.json().get("result", {}).get("message_id")
        logger.error("Telegram send failed: %s", resp.text[:200])
    except requests.RequestException as e:
        logger.error("Telegram send error: %s", e)
    return None


def reply_telegram(text: str, reply_to_message_id: Optional[int]) -> Optional[int]:
    """Sends a message threaded as a reply to the original signal post (if
    we have its message_id), so TP1/SL/close-out updates show up attached
    to the trade they belong to instead of as standalone messages."""
    if DRY_RUN:
        logger.info("[DRY-RUN] Telegram reply suppressed:\n%s", text)
        return None
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram credentials missing; skipping reply.")
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            return resp.json().get("result", {}).get("message_id")
        logger.error("Telegram reply failed: %s", resp.text[:200])
    except requests.RequestException as e:
        logger.error("Telegram reply error: %s", e)
    return None


def react_to_message(message_id: int, emoji: str) -> None:
    if DRY_RUN or not message_id:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setMessageReaction"
    try:
        resp = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "message_id": message_id,
                                         "reaction": [{"type": "emoji", "emoji": emoji}]}, timeout=8)
        if resp.status_code != 200:
            logger.warning("Telegram reaction failed (msg_id=%s, emoji=%s): %s",
                            message_id, emoji, resp.text[:200])
    except requests.RequestException as e:
        logger.warning("Telegram reaction error (msg_id=%s, emoji=%s): %s", message_id, emoji, e)


def fmt_px(v: float) -> str:
    """Human-readable display format (with thousands separators)."""
    if v >= 100:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:,.4f}"
    return f"{v:.6f}"


def fmt_px_raw(v: float) -> str:
    """Copy-paste-safe format: NO thousands separators, since a comma pasted
    into an exchange order-entry field breaks the value. This is what goes
    inside <code> tags -- tapping/long-pressing it in Telegram selects and
    copies exactly this string."""
    if v >= 100:
        return f"{v:.2f}"
    if v >= 1:
        return f"{v:.4f}"
    return f"{v:.6f}"


def confidence_bar(confidence: float) -> str:
    filled = int(round(confidence / 10))
    return "\u2588" * filled + "\u2591" * (10 - filled)


def format_signal(cand: Candidate, confidence: float, notes: list[str], size_pct: float) -> str:
    arrow = "\U0001F7E2 LONG" if cand.direction == "long" else "\U0001F534 SHORT"
    grade = grade_for_confidence(confidence)
    # Each price value is wrapped alone in its own <code> block so a single
    # tap (Telegram) or click (Telegram Desktop) copies just that number --
    # no label text, no thousands separators, nothing else to strip out.
    lines = [
        "\U0001F52E <b>LUCERNA</b>",
        f"<b>{arrow} — {cand.symbol}</b>  [{grade}]",
        f"Pathway: {cand.pathway.replace('_', ' ').title()}  ({cand.duration_hint})",
        "",
        f"Entry:  <code>{fmt_px_raw(cand.entry)}</code>",
        f"SL:     <code>{fmt_px_raw(cand.sl)}</code>",
        f"TP1:    <code>{fmt_px_raw(cand.tp1)}</code>",
        f"TP2:    <code>{fmt_px_raw(cand.tp2)}</code>",
        f"R:R (to TP2): {cand.rr():.2f}",
        f"Suggested size: {size_pct:.2f}% of equity",
        "",
        f"Confidence: {confidence:.0f}/100  {confidence_bar(confidence)}",
        "",
        "Confluences:",
    ]
    for n in notes[:8]:
        lines.append(f"  • {n}")
    lines.append("")
    lines.append(f"<i>Lucerna v1.0.0 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</i>")
    return "\n".join(lines)


# ==============================================================================
# SIGNAL TRACKING (win-rate accounting)
# ==============================================================================

def track_signal(state: dict, cand: Candidate, confidence: float, msg_id: Optional[int], size_pct: float) -> None:
    now_ms = int(time.time() * 1000)
    state["active_signals"].append({
        "id": str(uuid.uuid4()), "symbol": cand.symbol, "direction": cand.direction,
        "pathway": cand.pathway, "entry": cand.entry, "sl": cand.sl, "tp1": cand.tp1, "tp2": cand.tp2,
        "confidence": confidence, "size_pct": size_pct, "msg_id": msg_id,
        "opened_ts": time.time(), "tp1_hit": False, "last_checked_ts_ms": now_ms,
    })
    state["daily"]["signal_count"] += 1


def _apply_bar_to_signal(sig: dict, direction: str, lo: float, hi: float) -> Optional[str]:
    """Applies a single price bar's [lo, hi] range to an active signal's
    TP/SL state, in place. Returns 'loss', 'breakeven', or 'win_tp2' if the
    signal should be closed as a result of this bar, else None (still
    active -- though tp1_hit/sl may have just been updated for a breakeven
    move, flagged via `_tp1_hit_this_check` for the caller to react to).
    Within a single bar, SL is checked before TP2 (the conservative
    assumption if one bar's range spans both), mirroring the backtest's
    `_simulate_forward`."""
    hit_sl = (lo <= sig["sl"]) if direction == "long" else (hi >= sig["sl"])
    hit_tp2 = (hi >= sig["tp2"]) if direction == "long" else (lo <= sig["tp2"])
    hit_tp1 = (hi >= sig["tp1"]) if direction == "long" else (lo <= sig["tp1"])

    if hit_sl:
        return "breakeven" if sig.get("tp1_hit") else "loss"
    if hit_tp2:
        return "win_tp2"
    if hit_tp1 and not sig.get("tp1_hit"):
        sig["tp1_hit"] = True
        sig["sl"] = sig["entry"]  # move to breakeven
        sig["_tp1_hit_this_check"] = True
    return None


def check_active_signals(state: dict, latest_prices: dict[str, float]) -> None:
    """Resolve active signals against price action since the last check --
    not just a single current mark-price snapshot.

    Resolution previously compared only the latest point-in-time mark price
    against SL/TP, so a stop that was touched and price recovered before the
    next 15-minute scan was invisible: it was never recorded as a loss, and
    `state["daily"]["realized_pct"]` (which feeds the daily-loss circuit
    breaker) silently under-counted real drawdown. This reconstructs the
    high/low range from closed 1H candles since the signal's last check
    (falling back to the current mark price only for the still-forming
    candle) and walks that range chronologically, so an intra-cycle touch is
    caught even if price has since moved away again.

    A resolved signal (SL, breakeven-stop after TP1, or TP2) is what frees a
    symbol up under the one-signal-per-symbol policy -- see
    `symbol_has_open_signal`."""
    reference_ms = int(time.time() * 1000)
    still_active = []
    for sig in state.get("active_signals", []):
        symbol = sig["symbol"]
        direction = sig["direction"]
        since_ts_ms = sig.get("last_checked_ts_ms") or int(sig.get("opened_ts", time.time()) * 1000)

        range_bars: list[tuple[float, float]] = []
        candle_fetch_ok = False
        try:
            recent = get_candles(symbol, TF_EXEC, 30, reference_ms)
            range_bars = [(c["l"], c["h"]) for c in recent if c["t"] + INTERVAL_MS[TF_EXEC] > since_ts_ms]
            candle_fetch_ok = True
        except Exception as e:  # noqa: BLE001 - a candle-fetch hiccup must never block resolution
            logger.warning("check_active_signals: range fetch failed for %s: %s", symbol, e)

        price = latest_prices.get(symbol)
        if price is not None:
            range_bars.append((price, price))  # current still-forming candle / latest mark

        if not range_bars:
            still_active.append(sig)
            continue

        resolved_result = None
        for lo, hi in range_bars:
            sig.pop("_tp1_hit_this_check", None)
            resolved_result = _apply_bar_to_signal(sig, direction, lo, hi)
            if sig.pop("_tp1_hit_this_check", False) and not DRY_RUN and sig.get("msg_id"):
                react_to_message(sig["msg_id"], "\U0001F525")  # TP1 hit, SL moved to breakeven
                tp1_text = (f"\U0001F525 <b>TP1 hit</b> — {sig['symbol']} {sig['direction'].upper()}\n"
                            f"Price: <code>{fmt_px_raw(sig['tp1'])}</code>\n"
                            f"SL moved to breakeven (<code>{fmt_px_raw(sig['entry'])}</code>).")
                reply_telegram(tp1_text, sig["msg_id"])
            if resolved_result is not None:
                break

        if candle_fetch_ok:
            # Only advance the watermark on a successful candle fetch, so a
            # transient fetch failure doesn't silently skip the gap it left --
            # the next successful fetch will still cover it.
            sig["last_checked_ts_ms"] = reference_ms

        if resolved_result is None:
            still_active.append(sig)
            continue

        # P&L is size_pct (equity %) x actual stop distance (fraction of entry) --
        # this mirrors the win-side formula and is what keeps the two consistent
        # when position_size_pct() has clipped size_pct for exposure reasons.
        # After TP1, sig["sl"] has been moved to entry, so this naturally
        # evaluates to ~0 on a breakeven stop-out instead of a full loss.
        stop_distance_frac = abs(sig["entry"] - sig["sl"]) / sig["entry"] if sig["entry"] else 0.0
        if resolved_result in ("loss", "breakeven"):
            pnl_pct = -sig["size_pct"] * stop_distance_frac
        else:  # "win_tp2"
            pnl_pct = sig["size_pct"] * (abs(sig["tp2"] - sig["entry"]) / sig["entry"])
        _resolve(state, sig, resolved_result, pnl_pct)
    state["active_signals"] = still_active


def _resolve(state: dict, sig: dict, result: str, pnl_pct: float) -> None:
    sig["result"] = result
    sig["closed_ts"] = time.time()
    sig["pnl_pct"] = pnl_pct
    state["signal_history"].append({**sig, "ts": time.time()})
    state["daily"]["realized_pct"] = state["daily"].get("realized_pct", 0.0) + pnl_pct
    if state["daily"]["realized_pct"] <= DAILY_LOSS_LIMIT_PCT:
        state["daily"]["paused"] = True
    if not DRY_RUN and sig.get("msg_id"):
        emoji = "\U0001F3C6" if result == "win_tp2" else ("\U0001F44D" if result == "breakeven" else "\U0001F62D")
        react_to_message(sig["msg_id"], emoji)
        if result != "breakeven":
            # Breakeven stop-outs (SL hit after TP1 already moved it to entry)
            # were already announced via the TP1 reply -- no need for a
            # second reply, the reaction alone is enough.
            exit_price = sig["tp2"] if result == "win_tp2" else sig["sl"]
            headline = "\U0001F3C6 <b>TP2 hit — WIN</b>" if result == "win_tp2" else "\U0001F62D <b>SL hit — LOSS</b>"
            close_text = (f"{headline} — {sig['symbol']} {sig['direction'].upper()}\n"
                          f"Exit: <code>{fmt_px_raw(exit_price)}</code>  |  P&L: {pnl_pct:+.2f}%")
            reply_telegram(close_text, sig["msg_id"])


# ==============================================================================
# MAIN SCAN FLOW
# ==============================================================================

def generate_candidates(symbol: str, candles_exec: list[dict], ind_exec: dict, candles_struct: list[dict],
                         regime: RegimeVector, ind_bias: Optional[dict] = None
                         ) -> tuple[list[Candidate], dict[str, list[str]], str]:
    """Pure candidate generation -- no filtering, no state mutation. Shared by
    the live scan (`evaluate_symbol`) and the backtest (`_backtest_window`) so
    both paths run the identical three-pathway logic."""
    struct_htf = analyze_structure(candles_struct, find_swings(candles_struct))
    swings_exec = find_swings(candles_exec)
    pools = build_liquidity_pools(swings_exec)
    followthrough_bars = adaptive_followthrough_bars(regime)

    candidates: list[Candidate] = []
    candidates += pathway_liquidity_reversal(symbol, candles_exec, ind_exec, struct_htf, pools, regime)
    candidates += pathway_trend_continuation(symbol, candles_exec, ind_exec, struct_htf, regime)
    candidates += pathway_momentum_breakout(symbol, candles_exec, ind_exec, regime, followthrough_bars)

    pathway_directions: dict[str, list[str]] = {}
    for c in candidates:
        pathway_directions.setdefault(c.pathway, []).append(c.direction)

    macro_bias = macro_bias_1d(ind_bias) if ind_bias is not None else "neutral"
    return candidates, pathway_directions, macro_bias


def score_and_filter_candidates(symbol: str, state: dict, candidates: list[Candidate],
                                 pathway_directions: dict[str, list[str]], regime: RegimeVector,
                                 macro_bias: str, snapshot: dict, ob_analysis: dict, atr_pct: float,
                                 funding_by_dir: dict[str, dict], orderflow_fn, bar_index: int,
                                 check_liquidity: bool = True, check_spread: bool = True,
                                 apply_cooldown: bool = True, apply_one_signal_lock: bool = True
                                 ) -> list[tuple[Candidate, float, list[str]]]:
    """Shared score + filter pipeline -- identical for live and backtest apart
    from the two explicit, documented bypass flags (`check_liquidity`,
    `check_spread`) for data that has no honest historical reconstruction.
    NOTE: the freshness/decay check is intentionally NOT here -- it must run
    at send-time against a freshly captured price, not at generation time
    (see `signal_still_fresh`'s docstring). `run_scan` applies it after
    ranking/dedup, immediately before send."""
    if apply_one_signal_lock and symbol_has_open_signal(state, symbol):
        for cand in candidates:
            log_suppressed(symbol, cand.direction, cand.pathway,
                            "symbol already has an open signal (one-signal-per-symbol policy)", 0)
        return []

    results = []
    min_score = adaptive_min_score(regime)
    for cand in candidates:
        orderflow = orderflow_fn(cand)
        funding_read = funding_by_dir.get(cand.direction, {"squeeze_bonus": 0.0})
        score, notes = score_candidate(cand, pathway_directions, regime, funding_read, orderflow, ob_analysis, macro_bias)

        ok, reason = passes_hard_filters(symbol, snapshot, ob_analysis, atr_pct, cand, regime,
                                          check_liquidity, check_spread)
        if not ok:
            log_suppressed(symbol, cand.direction, cand.pathway, reason, score)
            continue
        if score < min_score:
            log_suppressed(symbol, cand.direction, cand.pathway, f"score {score:.1f} < floor {min_score:.1f}", score)
            continue
        if apply_cooldown and not check_cooldown(state, symbol, cand.direction, bar_index):
            log_suppressed(symbol, cand.direction, cand.pathway, "cooldown active", score)
            continue
        results.append((cand, score, notes))
    return results


def evaluate_symbol(symbol: str, state: dict, bundle: dict[str, list[dict]], snapshot: dict,
                     btc_bias: str, btc_strength: float, bar_index: int
                     ) -> list[tuple[Candidate, float, list[str], float]]:
    """Returns (candidate, score, notes, atr_val) tuples. `atr_val` travels
    with each result so `run_scan` can run the send-time freshness check
    without recomputing indicators."""
    candles_exec, candles_struct, candles_bias = bundle[TF_EXEC], bundle[TF_STRUCT], bundle[TF_BIAS]
    ind_exec = get_cached_indicators(symbol, TF_EXEC, candles_exec)
    ind_bias = get_cached_indicators(symbol, TF_BIAS, candles_bias)

    regime = build_regime_vector(state, symbol, ind_exec, candles_exec, btc_bias, btc_strength)
    candidates, pathway_directions, macro_bias = generate_candidates(
        symbol, candles_exec, ind_exec, candles_struct, regime, ind_bias)
    if not candidates:
        return []

    ob_analysis = analyze_orderbook(symbol)
    atr_val = ind_exec["atr"][-1]
    atr_pct = atr_val / ind_exec["closes"][-1] * 100 if ind_exec["closes"][-1] else 0.0
    funding_by_dir = {d: funding_oi_read(snapshot, state, symbol, d) for d in ("long", "short")}

    def orderflow_fn(cand: Candidate) -> dict:
        return orderflow_proxy(candles_exec, cand.direction)

    results = score_and_filter_candidates(
        symbol, state, candidates, pathway_directions, regime, macro_bias, snapshot, ob_analysis,
        atr_pct, funding_by_dir, orderflow_fn, bar_index,
        check_liquidity=True, check_spread=True, apply_cooldown=True, apply_one_signal_lock=True)
    return [(cand, score, notes, atr_val) for cand, score, notes in results]


def run_scan(dry_run: bool = False) -> None:
    global DRY_RUN
    DRY_RUN = dry_run or DRY_RUN
    reference_ms = int(time.time() * 1000)
    state = load_state()
    roll_daily_bucket(state, reference_ms)
    state["bar_index"] = state.get("bar_index", 0) + 1
    bar_index = state["bar_index"]

    logger.info("=== Lucerna scan start | dry_run=%s | bar_index=%d ===", DRY_RUN, bar_index)

    snapshot = get_market_snapshot()
    latest_prices = {s: d["mark"] for s, d in snapshot.items() if d.get("mark")}
    check_active_signals(state, latest_prices)

    cap_ok, cap_reason = portfolio_capacity_ok(state)
    if not cap_ok:
        logger.info("Portfolio-level gate closed: %s", cap_reason)
        save_state(state)
        return

    # NOTE: this is a deliberate, documented exception to the "skip one asset on
    # failure" error-handling policy (see the per-symbol try/except in the loop
    # below). BTC's regime feeds `build_regime_vector` for every other symbol
    # this scan, so a missing BTC read isn't "one asset's data is unavailable"
    # -- it's "no symbol this scan has valid regime context" -- and aborting
    # the whole scan is safer than silently scanning with stale/default regime.
    btc_bundle = fetch_all_candles("BTC", reference_ms)
    if not btc_bundle:
        logger.error("Could not fetch BTC candles; aborting scan (BTC regime is required context for all symbols).")
        return
    btc_ind = get_cached_indicators("BTC", TF_EXEC, btc_bundle[TF_EXEC])
    btc_bias, btc_strength = compute_btc_regime(btc_ind)

    all_candidates: list[Candidate] = []
    scored_meta: dict[str, tuple[float, list[str], float]] = {}
    returns_by_symbol: dict[str, list[float]] = {}

    for symbol in WATCHLIST:
        if _SHUTDOWN:
            break
        try:
            # BTC's bundle was already fetched above for the regime read --
            # reuse it here instead of hitting the API for the same data again.
            bundle = btc_bundle if symbol == "BTC" else fetch_all_candles(symbol, reference_ms)
            if not bundle:
                logger.warning("Skipping %s: candle data unavailable this scan.", symbol)
                continue
            returns_by_symbol[symbol] = compute_returns(bundle[TF_EXEC], CORR_LOOKBACK_BARS)
            results = evaluate_symbol(symbol, state, bundle, snapshot, btc_bias, btc_strength, bar_index)
            for cand, score, notes, atr_val in results:
                all_candidates.append(cand)
                scored_meta[id(cand)] = (score, notes, atr_val)
        except Exception as e:  # noqa: BLE001 - never abort the whole run over one asset
            logger.error("Error evaluating %s: %s", symbol, e, exc_info=True)
            continue

    if not all_candidates:
        logger.info("No qualifying candidates this scan.")
        save_state(state)
        return

    ranked = sorted(all_candidates, key=lambda c: scored_meta[id(c)][0], reverse=True)
    clusters = build_correlation_clusters(returns_by_symbol)
    deduped = dedup_correlated(ranked, clusters)

    active = state.get("active_signals", [])
    slots_left = max(0, MAX_CONCURRENT_SIGNALS - len(active))
    exposure_left = max(0.0, MAX_PORTFOLIO_EXPOSURE_PCT - sum(a.get("size_pct", 0.0) for a in active))

    # Freshness/decay check runs here -- at send-time, after ranking/dedup --
    # against a freshly re-fetched price, not the generation-time price the
    # candidate was built from (that comparison is definitionally a no-op).
    fresh_snapshot = get_market_snapshot()

    symbols_sent_this_scan: set[str] = set()
    sent = 0
    for cand in deduped:
        if slots_left <= 0 or exposure_left <= 0 or daily_loss_limit_breached(state):
            log_suppressed(cand.symbol, cand.direction, cand.pathway, "portfolio capacity exhausted mid-scan",
                            scored_meta[id(cand)][0])
            continue
        # One-signal-per-symbol also applies within a single scan: two
        # different pathways can each independently produce a candidate for
        # the same symbol before either becomes "active" in state, so this
        # guards the case `symbol_has_open_signal` can't see yet.
        if cand.symbol in symbols_sent_this_scan:
            log_suppressed(cand.symbol, cand.direction, cand.pathway,
                            "another signal for this symbol already selected this scan", scored_meta[id(cand)][0])
            continue
        score, notes, atr_val = scored_meta[id(cand)]
        fresh_price = fresh_snapshot.get(cand.symbol, {}).get("mark")
        if fresh_price is not None and not signal_still_fresh(cand, fresh_price, atr_val):
            log_suppressed(cand.symbol, cand.direction, cand.pathway, "signal decayed (price drift since generation)", score)
            continue
        size_pct = position_size_pct(cand)
        if size_pct > exposure_left:
            size_pct = exposure_left
        if size_pct <= 0:
            continue
        msg = format_signal(cand, score, notes, size_pct)
        msg_id = send_telegram(msg)
        symbols_sent_this_scan.add(cand.symbol)
        if not DRY_RUN:
            track_signal(state, cand, score, msg_id, size_pct)
            update_cooldown(state, cand.symbol, cand.direction, bar_index)
        else:
            logger.info("[DRY-RUN] Would-be signal:\n%s", msg)
        slots_left -= 1
        exposure_left -= size_pct
        sent += 1

    logger.info("Scan complete: %d candidates evaluated, %d signals sent/logged.", len(all_candidates), sent)
    prune_state(state)
    save_state(state)


# ==============================================================================
# BACKTESTING / EVALUATION MODULE
# ==============================================================================

FEE_TAKER = 0.00045   # Hyperliquid taker fee (approx, both sides applied)
FEE_MAKER = 0.00015
SLIPPAGE_EST = 0.0006  # conservative round-trip slippage estimate for liquid perps
SIM_FORWARD_MAX_BARS = 96  # _simulate_forward's resolution horizon (bars)


@dataclass
class BacktestTrade:
    symbol: str
    direction: str
    pathway: str
    entry_ts: int
    entry: float
    sl: float
    tp2: float
    exit_price: float
    result: str
    r_multiple: float
    regime_label: str
    window_id: str


def _simulate_forward(candles: list[dict], start_idx: int, cand: Candidate, max_bars: int = SIM_FORWARD_MAX_BARS
                       ) -> tuple[str, float, int]:
    """No look-ahead: only candles strictly after start_idx are used to resolve the
    trade. Also returns the resolving bar index, which the caller uses to
    enforce one-signal-per-symbol (`busy_until`) during backtest replay."""
    for i in range(start_idx + 1, min(len(candles), start_idx + 1 + max_bars)):
        c = candles[i]
        if cand.direction == "long":
            if c["l"] <= cand.sl:
                return "loss", cand.sl, i
            if c["h"] >= cand.tp2:
                return "win", cand.tp2, i
        else:
            if c["h"] >= cand.sl:
                return "loss", cand.sl, i
            if c["l"] <= cand.tp2:
                return "win", cand.tp2, i
    end_idx = min(len(candles) - 1, start_idx + max_bars)
    return "timeout", candles[end_idx]["c"], end_idx


def _net_return(direction: str, entry: float, exit_price: float, fee: float = FEE_TAKER,
                 slippage: float = SLIPPAGE_EST) -> float:
    gross = (exit_price - entry) / entry if direction == "long" else (entry - exit_price) / entry
    return gross - fee * 2 - slippage


def slice_by_time(candles: list[dict], cutoff_ts: int, interval_ms: int = 0) -> list[dict]:
    """Strictly-historical slice: only candles that have actually CLOSED
    before `cutoff_ts`. `c["t"]` is a candle's OPEN time, not its close, so
    `interval_ms` (the timeframe's own bar length) must be added before
    comparing -- otherwise a candle that opened before cutoff_ts but whose
    close is still in the future relative to the simulated point in time
    would leak its high/low/close into the backtest."""
    return [c for c in candles if c["t"] + interval_ms <= cutoff_ts]


def _backtest_window(symbol: str, candles: list[dict], candles_struct_all: list[dict],
                      candles_bias_all: list[dict], window_id: str,
                      min_rr: float = None, min_score_override: Optional[float] = None) -> list[BacktestTrade]:
    """Runs the SAME generate_candidates -> score_and_filter_candidates pipeline
    the live engine uses (see `evaluate_symbol`), instead of a separate,
    looser reimplementation. Differences from live, both intentional and
    explicitly flagged in the report:
      - `check_liquidity`/`check_spread` are bypassed (no honest historical
        L2 book or rolling-volume series exists to reconstruct them).
      - funding/OI squeeze bonus is zeroed (no historical funding series
        fetched in this version -- documented limitation, not silently
        assumed neutral).
    True historical 4H/1D candles are fetched and time-sliced per bar (see
    `slice_by_time`) rather than reconstructing HTF structure from the 1H
    window, so `struct_htf`/macro bias reflect the real timeframes the live
    engine uses. `min_rr`/`min_score_override` let the sensitivity sweep
    actually regenerate trades under perturbed thresholds instead of
    re-summarizing an unperturbed trade list.
    """
    global MIN_RR, BASE_MIN_SCORE
    trades: list[BacktestTrade] = []
    warmup = max(EMA_TREND, BB_LEN, ADX_LEN * 2) + 5
    if len(candles) < warmup + 30:
        return trades

    orig_rr, orig_score = MIN_RR, BASE_MIN_SCORE
    if min_rr is not None:
        MIN_RR = min_rr
    if min_score_override is not None:
        BASE_MIN_SCORE = min_score_override

    local_state = {"cooldowns": {}, "atr_pct_memory": {}, "correlation_returns": {}, "active_signals": []}
    busy_until = -1
    try:
        for i in range(warmup, len(candles) - 5, 3):  # stride 3 bars to bound compute
            if i < busy_until:
                continue
            # Strictly historical 1H window, clipped to the same trailing length
            # live always uses (CANDLE_COUNTS[TF_EXEC]) -- otherwise this grows
            # unboundedly within a chunk and indicators end up computed over far
            # more history than the live engine ever actually sees.
            window = candles[max(0, i + 1 - CANDLE_COUNTS[TF_EXEC]):i + 1]
            ind = compute_indicators(window)
            cutoff_ts = window[-1]["t"] + INTERVAL_MS[TF_EXEC]  # this bar's close = next bar's open
            # Time-slice on each candle's true CLOSE (not open) so no future
            # high/low/close leaks into struct/bias history, then cap to the
            # same trailing length live uses (CANDLE_COUNTS), for the same
            # reason as the 1H window above.
            struct_hist = slice_by_time(candles_struct_all, cutoff_ts, INTERVAL_MS[TF_STRUCT])[-CANDLE_COUNTS[TF_STRUCT]:]
            bias_hist = slice_by_time(candles_bias_all, cutoff_ts, INTERVAL_MS[TF_BIAS])[-CANDLE_COUNTS[TF_BIAS]:]
            if len(struct_hist) < 60 or len(bias_hist) < 60:
                continue  # insufficient real HTF history at this point -- skip rather than fake it
            regime = build_regime_vector(local_state, symbol, ind, window, "neutral", 0.5)
            ind_bias = compute_indicators(bias_hist)
            candidates, pathway_directions, macro_bias = generate_candidates(
                symbol, window, ind, struct_hist, regime, ind_bias)
            if not candidates:
                continue
            atr_pct = ind["atr"][-1] / ind["closes"][-1] * 100 if ind["closes"][-1] else 0.0
            funding_by_dir = {"long": {"squeeze_bonus": 0.0}, "short": {"squeeze_bonus": 0.0}}
            results = score_and_filter_candidates(
                symbol, local_state, candidates, pathway_directions, regime, macro_bias, {}, {},
                atr_pct, funding_by_dir, lambda c: {"aligned": False}, i,
                check_liquidity=False, check_spread=False, apply_cooldown=True, apply_one_signal_lock=False)
            if not results:
                continue
            cand, score, _notes = max(results, key=lambda r: r[1])  # one signal per symbol per bar, best score wins
            update_cooldown(local_state, symbol, cand.direction, i)
            result, exit_price, exit_idx = _simulate_forward(candles, i, cand)
            busy_until = exit_idx + 1  # symbol stays "busy" until this trade resolves -- mirrors live policy
            r = (exit_price - cand.entry) / (cand.entry - cand.sl) if cand.direction == "long" and (cand.entry - cand.sl) else 0.0
            if cand.direction == "short" and (cand.sl - cand.entry):
                r = (cand.entry - exit_price) / (cand.sl - cand.entry)
            trades.append(BacktestTrade(symbol, cand.direction, cand.pathway, window[-1]["t"], cand.entry,
                                         cand.sl, cand.tp2, exit_price, result, r, regime.label, window_id))
    finally:
        MIN_RR, BASE_MIN_SCORE = orig_rr, orig_score
    return trades


def _baseline_ma_crossover(candles: list[dict], window_id: str, symbol: str) -> list[BacktestTrade]:
    trades = []
    closes = [c["c"] for c in candles]
    fast, slow = ema(closes, 20), ema(closes, 50)
    for i in range(55, len(candles) - 5):
        crossed_up = fast[i - 1] <= slow[i - 1] and fast[i] > slow[i]
        crossed_dn = fast[i - 1] >= slow[i - 1] and fast[i] < slow[i]
        if not (crossed_up or crossed_dn):
            continue
        direction = "long" if crossed_up else "short"
        entry = candles[i]["c"]
        atr_val = atr([c["h"] for c in candles[:i + 1]], [c["l"] for c in candles[:i + 1]],
                       closes[:i + 1])[-1]
        sl = entry - atr_val * 1.5 if direction == "long" else entry + atr_val * 1.5
        tp2 = entry + atr_val * 3.0 if direction == "long" else entry - atr_val * 3.0
        cand = Candidate(symbol, direction, "baseline_ma_cross", entry, sl, entry, tp2, 0)
        result, exit_price, _exit_idx = _simulate_forward(candles, i, cand)
        r = (exit_price - entry) / (entry - sl) if direction == "long" and (entry - sl) else 0.0
        if direction == "short" and (sl - entry):
            r = (entry - exit_price) / (sl - entry)
        trades.append(BacktestTrade(symbol, direction, "baseline_ma_cross", candles[i]["t"], entry, sl, tp2,
                                     exit_price, result, r, "n/a", window_id))
    return trades


def _summarize(trades: list[BacktestTrade], min_sample: int = 20) -> dict:
    if len(trades) < min_sample:
        return {"n": len(trades), "flagged_low_sample": True}
    wins = [t for t in trades if t.result == "win"]
    gross_wr = len(wins) / len(trades) * 100
    net_returns = [_net_return(t.direction, t.entry, t.exit_price) for t in trades]
    net_wr = sum(1 for r in net_returns if r > 0) / len(trades) * 100
    avg_r = sum(t.r_multiple for t in trades) / len(trades)
    r_mean = avg_r
    r_stdev = math.sqrt(sum((t.r_multiple - r_mean) ** 2 for t in trades) / len(trades))
    return {
        "n": len(trades), "gross_win_rate": round(gross_wr, 2), "net_win_rate": round(net_wr, 2),
        "avg_r_multiple": round(avg_r, 3), "r_multiple_stdev": round(r_stdev, 3),
        "avg_net_return_pct": round(sum(net_returns) / len(net_returns) * 100, 3),
        "flagged_low_sample": False,
    }


def _sensitivity_flag(baseline: dict, perturbed: dict, metric: str = "net_win_rate",
                       collapse_threshold_pct: float = 40.0) -> bool:
    """True if a perturbation causes `metric` to collapse (drop by more than
    `collapse_threshold_pct` relative to baseline) -- the actual overfitting
    signal the spec asked this check to be able to detect."""
    if baseline.get("flagged_low_sample") or perturbed.get("flagged_low_sample"):
        return False  # can't judge collapse without a real baseline sample
    b, p = baseline.get(metric, 0.0), perturbed.get(metric, 0.0)
    if b <= 0:
        return False
    return (b - p) / b * 100 >= collapse_threshold_pct


def run_backtest(days: int = 180, holdout_days: int = 30, min_sample: int = 20) -> dict:
    """Walk-forward validation with a locked final holdout window, fee/slippage-aware
    net returns, a genuine sensitivity sweep, low-sample flagging, and a
    baseline comparison run on the holdout window too.

    KNOWN, DOCUMENTED LIMITATIONS (also printed in the report):
      - Funding/OI squeeze bonus is zeroed throughout -- no historical funding
        time series is fetched in this version, so that scoring input is
        untested here even though it's live in production. Treat any signal
        whose live edge depends heavily on funding/OI as unvalidated by this
        report until a historical funding fetch is added.
      - Liquidity-floor and spread hard filters are bypassed (no historical
        L2 book or true rolling 24h-volume series exists to reconstruct
        them), via `_backtest_window`'s check_liquidity=False/check_spread=False.
    """
    logger.info("=== Backtest start: %d days (holdout=%d) ===", days, holdout_days)
    reference_ms = int(time.time() * 1000)
    n_bars_1h = int(days * 24)
    n_bars_4h = int(days * 24 / 4) + 60
    n_bars_1d = int(days) + 60
    report: dict = {"windows": {}, "sensitivity": {}, "baseline": {}, "regime_breakdown": {},
                     "caveats": ["funding/OI scoring input untested (hardcoded neutral)",
                                 "liquidity-floor and spread hard filters bypassed (no historical L2/volume series)"]}

    n_windows = 3

    for sym in BACKTEST_SYMBOLS:
        candles_all = get_candles(sym, TF_EXEC, n_bars_1h, reference_ms)
        candles_struct_all = get_candles(sym, TF_STRUCT, n_bars_4h, reference_ms)
        candles_bias_all = get_candles(sym, TF_BIAS, n_bars_1d, reference_ms)
        if len(candles_all) < 200 or len(candles_struct_all) < 100 or len(candles_bias_all) < 100:
            logger.warning("Backtest: insufficient history for %s, skipping.", sym)
            continue
        holdout_bars = holdout_days * 24
        train_pool = candles_all[:-holdout_bars] if len(candles_all) > holdout_bars else candles_all
        holdout = candles_all[-holdout_bars:] if len(candles_all) > holdout_bars else []

        def run_all_windows(rr_override=None, score_override=None) -> tuple[list, dict, list]:
            """Runs train windows + holdout under the given thresholds; returns
            (all_trades_with_holdout, per_window_summaries, holdout_trades)."""
            all_trades: list[BacktestTrade] = []
            windows_out = {}
            step = max(1, len(train_pool) // n_windows)
            for w in range(n_windows):
                # Extend past the window's own end by at least the resolution
                # horizon (SIM_FORWARD_MAX_BARS) so a trade generated near the
                # tail of the window still has enough follow-on data within
                # this chunk to resolve genuinely, instead of spuriously
                # timing out purely because data ran out.
                chunk = train_pool[w * step: (w + 1) * step + SIM_FORWARD_MAX_BARS]
                if len(chunk) < 200:
                    continue
                wid = f"{sym}:train_w{w}"
                trades = _backtest_window(sym, chunk, candles_struct_all, candles_bias_all, wid,
                                           min_rr=rr_override, min_score_override=score_override)
                all_trades += trades
                windows_out[wid] = _summarize(trades, min_sample)
            holdout_trades = []
            if holdout:
                hid = f"{sym}:holdout"
                holdout_trades = _backtest_window(sym, holdout, candles_struct_all, candles_bias_all, hid,
                                                   min_rr=rr_override, min_score_override=score_override)
                windows_out[hid] = _summarize(holdout_trades, min_sample)
                all_trades += holdout_trades
            return all_trades, windows_out, holdout_trades

        all_trades_with_holdout, window_summaries, holdout_trades = run_all_windows()
        report["windows"].update(window_summaries)

        for label in {"clean_trend", "choppy", "neutral", "high_volatility"}:
            subset = [t for t in all_trades_with_holdout if t.regime_label == label]
            report["regime_breakdown"][f"{sym}:{label}"] = _summarize(subset, min_sample)

        # Baseline comparison run on BOTH train_pool and the locked holdout --
        # the holdout comparison is the one that actually matters out-of-sample.
        baseline_train = _baseline_ma_crossover(train_pool, f"{sym}:baseline_train", sym)
        baseline_holdout = _baseline_ma_crossover(holdout, f"{sym}:baseline_holdout", sym) if holdout else []
        report["baseline"][sym] = {
            "train": _summarize(baseline_train, min_sample),
            "holdout": _summarize(baseline_holdout, min_sample) if holdout else {"n": 0, "flagged_low_sample": True},
        }

        # Genuine parameter sensitivity: regenerate trades from scratch under each
        # perturbed threshold (not a re-summary of the unperturbed trade list).
        # Both MIN_RR and BASE_MIN_SCORE are perturbed, as the spec requires.
        baseline_summary = _summarize(all_trades_with_holdout, min_sample)
        sens_results: dict = {"baseline": baseline_summary}
        for pct in (-0.10, 0.10):
            rr_perturbed_trades, _, _ = run_all_windows(rr_override=MIN_RR * (1 + pct))
            summary = _summarize(rr_perturbed_trades, min_sample)
            summary["collapsed"] = _sensitivity_flag(baseline_summary, summary)
            sens_results[f"min_rr_{pct:+.0%}"] = summary

            score_perturbed_trades, _, _ = run_all_windows(score_override=BASE_MIN_SCORE * (1 + pct))
            summary2 = _summarize(score_perturbed_trades, min_sample)
            summary2["collapsed"] = _sensitivity_flag(baseline_summary, summary2)
            sens_results[f"base_min_score_{pct:+.0%}"] = summary2
        report["sensitivity"][sym] = sens_results

    logger.info("Backtest complete. See report for per-window, per-regime, baseline, and sensitivity breakdowns.")
    return report


def print_backtest_report(report: dict) -> None:
    print("\n" + "=" * 70)
    print("LUCERNA BACKTEST / EVALUATION REPORT")
    print("=" * 70)
    print("\n-- Walk-forward windows (includes locked holdout) --")
    for wid, summary in report["windows"].items():
        if summary.get("flagged_low_sample"):
            print(f"  {wid}: n={summary['n']} -> LOW SAMPLE, not statistically meaningful")
        else:
            print(f"  {wid}: n={summary['n']} gross_wr={summary['gross_win_rate']}% "
                  f"net_wr={summary['net_win_rate']}% avg_R={summary['avg_r_multiple']} "
                  f"avg_net_ret={summary['avg_net_return_pct']}%")
    print("\n-- Regime breakdown --")
    for key, summary in report["regime_breakdown"].items():
        if summary.get("flagged_low_sample"):
            print(f"  {key}: n={summary['n']} -> LOW SAMPLE")
        else:
            print(f"  {key}: n={summary['n']} net_wr={summary['net_win_rate']}% avg_R={summary['avg_r_multiple']}")
    print("\n-- Baseline (EMA20/50 crossover) comparison: train vs locked holdout --")
    for sym, both in report["baseline"].items():
        for split, summary in both.items():
            if summary.get("flagged_low_sample"):
                print(f"  {sym} [{split}]: n={summary['n']} -> LOW SAMPLE")
            else:
                print(f"  {sym} [{split}]: n={summary['n']} net_wr={summary['net_win_rate']}% avg_R={summary['avg_r_multiple']}")
    print("\n-- Parameter sensitivity (MIN_RR and BASE_MIN_SCORE, +/-10%, trades REGENERATED per perturbation) --")
    for sym, sens in report["sensitivity"].items():
        print(f"  {sym}:")
        for k, v in sens.items():
            if v.get("flagged_low_sample"):
                print(f"    {k}: LOW SAMPLE")
            else:
                collapse = "  <-- COLLAPSED (possible overfit)" if v.get("collapsed") else ""
                print(f"    {k}: net_wr={v.get('net_win_rate')}% avg_R={v.get('avg_r_multiple')}{collapse}")
    print("\n-- Caveats --")
    for c in report.get("caveats", []):
        print(f"  - {c}")
    print("\nNote: any 'LOW SAMPLE' slice above should not be treated as a confirmed edge; it")
    print("firms up only as more historical or live data accumulates.")
    print("=" * 70 + "\n")


# ==============================================================================
# ENTRYPOINT
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="Lucerna Adaptive Confluence Signal Engine v1.0.0")
    parser.add_argument("--dry-run", action="store_true", help="Run full scan, log would-be signals, no send/commit")
    parser.add_argument("--backtest", action="store_true", help="Run the backtesting/evaluation module")
    parser.add_argument("--days", type=int, default=180, help="Backtest lookback window in days")
    parser.add_argument("--holdout-days", type=int, default=30, help="Locked holdout window size in days")
    args = parser.parse_args()

    if args.backtest:
        report = run_backtest(days=args.days, holdout_days=args.holdout_days)
        print_backtest_report(report)
        return

    try:
        run_scan(dry_run=args.dry_run)
    except Exception as e:  # noqa: BLE001 - top-level guard so cron never sees a hard crash without a log
        logger.critical("Fatal error during scan: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
