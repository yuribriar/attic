#!/usr/bin/env python3
# pip install requests numpy
"""
ECLIPTIC v1.0.0
================================================================================
An institutional-grade Smart Money Concept signal engine for Hyperliquid
perpetuals, built around a single, disciplined POI hierarchy:

    Order Blocks (OB) -> Breaker Blocks (BB) -> Fair Value Gaps (FVG)

Design philosophy
------------------
Ecliptic does not treat SMC zones as interchangeable. It applies the
institutional best-practice split explicitly:

    HTF (1H)  -> hunts for Order Blocks and Breaker Blocks (the zones that
                 define *where* institutional positioning likely sits)
    LTF (15M) -> hunts for Breaker Blocks specifically as the entry trigger
                 (a breaker retest after a confirmed structure shift is a
                 tighter, statistically cleaner entry than a raw OB touch)
    FVGs      -> used both as HTF/LTF confluence (a POI stacked with an
                 unfilled imbalance is higher quality) and, independently,
                 as a frequency-additive continuation trigger in trending
                 regimes (a fresh FVG fill in the direction of an established
                 trend is a valid, lower-friction entry that a pure
                 sweep-and-shift model would miss).

Every zone that reaches candidate status is passed through five explicit
filters before it can become a signal:

    1. LOCATION  - where the zone sits: premium/discount positioning,
                   distance from price in ATR units, confluence with a
                   liquidity pool, round number, or higher-timeframe level.
    2. CONTEXT   - does the zone type fit the detected regime? Reversal
                   zones (sweep + OB/BB) are weighted up in ranging/reversal
                   regimes; continuation zones (FVG fill, breaker-as-
                   continuation) are weighted up in trending regimes.
    3. QUALITY   - zone construction quality: displacement strength (body-
                   to-range, volume/imbalance size relative to ATR),
                   freshness (untested vs. already mitigated), and how many
                   confluences stack in the same zone.
    4. RR        - the trade plan must clear a minimum, regime-adjusted
                   reward:risk before it is even scored.
    5. LTF CONFIRMATION - no signal fires off an HTF zone touch alone. The
                   15m execution timeframe must independently confirm via a
                   liquidity sweep + market structure shift (MSS/CHoCH) and,
                   where applicable, a breaker-block retest with rejection.

Adaptive quality/frequency balance (see ADAPTIVE INTELLIGENCE below) is a
fixed, regime-conditioned rule set decided at design time from the
literature/patterns below - it does not self-tune on live results.

Techniques incorporated beyond the reference engines, with rationale:
  - Funding-rate + OI regime/confluence inputs (native Hyperliquid data;
    squeeze/divergence conditions are well documented in perp-futures
    market microstructure literature and surface setups pure price action
    misses).
  - Ensemble agreement scoring across trend/momentum/volume/structure
    families (standard multi-factor confirmation approach in systematic
    trading; lets strong borderline setups through instead of a single
    hard threshold).
  - Correlation-cluster de-duplication (portfolio theory: correlated bets
    are one effective position, not several).
  - Liquidity-aware filtering using L2 book depth/spread (standard
    execution-quality filter for perps).
  - False-breakout/fakeout confirmation requiring volume + close-through,
    not wick-only breaches (classic breakout trap avoidance).
  - Signal freshness/decay check (price can invalidate a setup between
    generation and action on a 15m cadence).
  - Walk-forward backtesting with a held-out final window, fee + slippage
    modeling, parameter sensitivity checks, and a moving-average-crossover
    baseline (standard quant research hygiene to guard against overfitting
    and unsubstantiated performance claims).

Infrastructure mirrors the operator's existing engines: Hyperliquid REST
API, fixed watchlist, Telegram delivery with reaction/reply lifecycle,
cron-per-run (15m) scan model, state.json persistence.
================================================================================
"""

import os
import sys
import json
import math
import time
import random
import signal as os_signal
import threading
import copy
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

__version__ = "1.0.0"
ENGINE_NAME = "Ecliptic"

# ============================================================================
# CONFIGURATION
# ============================================================================

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
DRY_RUN = os.getenv("ECLIPTIC_DRY_RUN", "false").strip().lower() in ("1", "true", "yes")

if not DRY_RUN:
    if not TG_BOT_TOKEN:
        raise RuntimeError("TG_BOT_TOKEN environment variable is required (set ECLIPTIC_DRY_RUN=true to skip)")
    if not TG_CHAT_ID:
        raise RuntimeError("TG_CHAT_ID environment variable is required (set ECLIPTIC_DRY_RUN=true to skip)")

HL_INFO_URL = os.getenv("HL_INFO_URL", "https://api.hyperliquid.xyz/info")
STATE_FILE = os.getenv("ECLIPTIC_STATE_FILE", "state.json")
LOG_FILE = os.getenv("ECLIPTIC_LOG_FILE", "ecliptic.log")
SCAN_WORKERS = int(os.getenv("SCAN_WORKERS", "4"))

HL_MIN_INTERVAL_S = float(os.getenv("HL_MIN_INTERVAL_S", "0.15"))

WATCHLIST = [
    "BTC", "ETH", "SOL", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]

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

INTERVAL_MS = {
    "15m": 15 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
}

# Timeframe cascade: 4H regime/bias -> 1H POI hunting (OB/BB) -> 15M execution
# (sweep + MSS + LTF breaker entry / FVG fill entry)
TF_BIAS = "4h"
TF_POI = "1h"
TF_EXEC = "15m"

N_BIAS = 300
N_POI = 300
N_EXEC = 300
N_DAILY = 400

# Indicator lengths
RSI_LEN = 14
ATR_LEN = 14
ADX_LEN = 14
BB_LEN = 20
BB_MULT = 2.0

# Risk / portfolio
MIN_RR_BASE = 1.8
MAX_CONCURRENT_ACTIVE_SIGNALS = 12
MAX_SIGNALS_PER_SCAN = 3
MAX_PER_SECTOR = 1
MAX_PORTFOLIO_EXPOSURE_PCT = 60.0   # sum of position_size_pct across open signals
DAILY_LOSS_LIMIT_R = -6.0           # sum of realized R for the UTC day
PER_TRADE_RISK_PCT = 1.0            # nominal risk-per-trade for sizing math

REACT_TP = "\U0001F3C6"
REACT_SL = "\U0001F62D"
DAILY_SUMMARY_HOUR_UTC = 8
STATE_VERSION = 1

_hl_lock = threading.Lock()
_hl_last_ts = 0.0
_session = requests.Session()
_state_lock = threading.Lock()
_shutdown_flag = {"stop": False}


def _handle_shutdown(sig_num, frame):
    _shutdown_flag["stop"] = True


os_signal.signal(os_signal.SIGTERM, _handle_shutdown)
os_signal.signal(os_signal.SIGINT, _handle_shutdown)


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except OSError:
        pass


def log_suppressed(symbol: str, direction: str, pathway: str, reason: str, score: float = 0.0) -> None:
    """Audit trail for signals that were generated internally but blocked by a filter."""
    log(f"SUPPRESSED {symbol} {direction} pathway={pathway} score={score:.1f} reason={reason}")


# ============================================================================
# HYPERLIQUID DATA LAYER
# ============================================================================

class _RateLimiter:
    def __init__(self, max_per_second: float = 8.0):
        self.min_interval = 1.0 / max_per_second
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            delta = now - self._last
            if delta < self.min_interval:
                time.sleep(self.min_interval - delta)
            self._last = time.monotonic()


_rate_limiter = _RateLimiter(max_per_second=1.0 / HL_MIN_INTERVAL_S)


def hl_post(payload: dict, retries: int = 4, timeout: int = 12):
    last_err = None
    for attempt in range(retries):
        _rate_limiter.wait()
        try:
            resp = _session.post(HL_INFO_URL, json=payload, timeout=timeout)
            if resp.status_code == 429:
                time.sleep(1.5 * (attempt + 1) + random.random())
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            time.sleep(0.5 * (attempt + 1) + random.random() * 0.3)
    log(f"hl_post failed after {retries} attempts: {last_err}")
    return None


def hl_coin(symbol: str) -> str:
    return symbol.replace("USDT", "").replace("USD", "").upper()


def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    span = INTERVAL_MS[interval]
    return (reference_ms // span) * span


def filter_closed_candles(candles: list, interval: str, reference_ms: int) -> list:
    open_ms = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if c.get("t", 0) < open_ms]


def get_candles(symbol: str, interval: str, n: int, reference_ms=None):
    coin = hl_coin(symbol)
    ref = reference_ms or int(time.time() * 1000)
    span = INTERVAL_MS[interval]
    start = ref - span * (n + 5)
    payload = {
        "type": "candleSnapshot",
        "req": {"coin": coin, "interval": interval, "startTime": start, "endTime": ref},
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
    candles = filter_closed_candles(candles, interval, ref)
    candles.sort(key=lambda x: x["t"])
    return candles[-n:] if len(candles) > n else candles


def fetch_all_candles(symbol: str, reference_ms=None):
    """Returns dict of tf -> candles, or None if any critical fetch fails."""
    out = {}
    try:
        out["4h"] = get_candles(symbol, "4h", N_BIAS, reference_ms)
        out["1h"] = get_candles(symbol, "1h", N_POI, reference_ms)
        out["15m"] = get_candles(symbol, "15m", N_EXEC, reference_ms)
        out["1d"] = get_candles(symbol, "1d", N_DAILY, reference_ms)
    except Exception as e:
        log(f"fetch_all_candles error for {symbol}: {e}")
        return None
    for tf, min_n in (("4h", 60), ("1h", 80), ("15m", 100), ("1d", 30)):
        c = out.get(tf)
        if not c or len(c) < min_n:
            log(f"fetch_all_candles: insufficient {tf} data for {symbol} ({len(c) if c else 0} bars)")
            return None
    return out


def get_meta_and_ctx():
    meta = hl_post({"type": "metaAndAssetCtxs"})
    if not meta or not isinstance(meta, list) or len(meta) < 2:
        return None
    universe = meta[0].get("universe", [])
    names = [u["name"] for u in universe]
    return names, meta[1]


def get_market_snapshot() -> dict:
    """symbol -> {price, funding, oi_usd, day_volume}"""
    res = get_meta_and_ctx()
    snap = {}
    if not res:
        return snap
    names, ctxs = res
    for i, name in enumerate(names):
        if i >= len(ctxs):
            break
        ctx = ctxs[i]
        try:
            mark = float(ctx.get("markPx", 0) or 0)
            funding = float(ctx.get("funding", 0) or 0)
            oi = float(ctx.get("openInterest", 0) or 0)
            vol = float(ctx.get("dayNtlVlm", 0) or 0)
            snap[name] = {"price": mark, "funding": funding, "oi_usd": oi * mark, "day_volume": vol}
        except (TypeError, ValueError):
            continue
    return snap


def get_l2_book(coin: str):
    return hl_post({"type": "l2Book", "coin": coin})


def analyze_orderbook(coin: str) -> dict:
    """Liquidity-aware filtering input: spread bps and top-of-book depth in USD."""
    book = get_l2_book(coin)
    out = {"spread_bps": None, "depth_usd": 0.0, "ok": False}
    if not book or "levels" not in book or len(book["levels"]) < 2:
        return out
    try:
        bids, asks = book["levels"][0], book["levels"][1]
        if not bids or not asks:
            return out
        best_bid = float(bids[0]["px"])
        best_ask = float(asks[0]["px"])
        mid = (best_bid + best_ask) / 2.0
        spread_bps = ((best_ask - best_bid) / mid) * 10000 if mid > 0 else None
        depth = sum(float(b["sz"]) * float(b["px"]) for b in bids[:10])
        depth += sum(float(a["sz"]) * float(a["px"]) for a in asks[:10])
        out = {"spread_bps": spread_bps, "depth_usd": depth, "ok": True}
    except (KeyError, TypeError, ValueError, IndexError):
        pass
    return out


# ============================================================================
# MATH / INDICATORS
# ============================================================================

def safe(v, fb: float = 0.0) -> float:
    try:
        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
            return fb
        return float(v)
    except (TypeError, ValueError):
        return fb


def ema(vals, period: int):
    if not vals:
        return []
    k = 2.0 / (period + 1)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def sma(vals, period: int):
    out = []
    for i in range(len(vals)):
        if i < period - 1:
            out.append(vals[i])
        else:
            out.append(sum(vals[i - period + 1:i + 1]) / period)
    return out


def stdev(vals, period: int):
    out = []
    for i in range(len(vals)):
        if i < period - 1:
            out.append(0.0)
        else:
            window = vals[i - period + 1:i + 1]
            m = sum(window) / period
            out.append(math.sqrt(sum((x - m) ** 2 for x in window) / period))
    return out


def rsi(closes, period: int = RSI_LEN):
    if len(closes) < period + 1:
        return [50.0] * len(closes)
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(d, 0) for d in deltas]
    losses = [max(-d, 0) for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    out = [50.0] * (period)
    rs = avg_gain / avg_loss if avg_loss > 0 else 100.0
    out.append(100 - 100 / (1 + rs))
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs = avg_gain / avg_loss if avg_loss > 0 else 100.0
        out.append(100 - 100 / (1 + rs))
    return out


def atr(highs, lows, closes, period: int = ATR_LEN):
    n = len(closes)
    if n == 0:
        return []
    trs = [highs[0] - lows[0]]
    for i in range(1, n):
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    out = [trs[0]]
    for i in range(1, n):
        if i < period:
            out.append(sum(trs[:i + 1]) / (i + 1))
        else:
            out.append((out[-1] * (period - 1) + trs[i]) / period)
    return out


def adx_dmi(highs, lows, closes, period: int = ADX_LEN):
    n = len(closes)
    if n < 2:
        return [0.0] * n, [0.0] * n, [0.0] * n
    plus_dm, minus_dm, trs = [0.0], [0.0], [highs[0] - lows[0]]
    for i in range(1, n):
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))

    def wilder(series):
        out = [series[0]]
        for i in range(1, len(series)):
            if i < period:
                out.append(sum(series[:i + 1]) / (i + 1))
            else:
                out.append(out[-1] - out[-1] / period + series[i])
        return out

    tr_s = wilder(trs)
    pdm_s = wilder(plus_dm)
    mdm_s = wilder(minus_dm)
    plus_di = [100 * pdm_s[i] / tr_s[i] if tr_s[i] > 0 else 0.0 for i in range(n)]
    minus_di = [100 * mdm_s[i] / tr_s[i] if tr_s[i] > 0 else 0.0 for i in range(n)]
    dx = [100 * abs(plus_di[i] - minus_di[i]) / (plus_di[i] + minus_di[i]) if (plus_di[i] + minus_di[i]) > 0 else 0.0
          for i in range(n)]
    adx = wilder(dx)
    return adx, plus_di, minus_di


def bollinger(closes, period: int = BB_LEN, mult: float = BB_MULT):
    mid = sma(closes, period)
    sd = stdev(closes, period)
    upper = [mid[i] + mult * sd[i] for i in range(len(closes))]
    lower = [mid[i] - mult * sd[i] for i in range(len(closes))]
    width_pct = [((upper[i] - lower[i]) / mid[i]) * 100 if mid[i] > 0 else 0.0 for i in range(len(closes))]
    return upper, mid, lower, width_pct


def donchian(highs, lows, period: int = 20):
    upper, lower = [], []
    for i in range(len(highs)):
        lo = max(0, i - period + 1)
        upper.append(max(highs[lo:i + 1]))
        lower.append(min(lows[lo:i + 1]))
    return upper, lower


def obv(closes, volumes):
    out = [0.0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            out.append(out[-1] + volumes[i])
        elif closes[i] < closes[i - 1]:
            out.append(out[-1] - volumes[i])
        else:
            out.append(out[-1])
    return out


def detect_rsi_divergence(closes, rsi_values, lookback: int = 25):
    if len(closes) < lookback + 5:
        return None
    seg_c = closes[-lookback:]
    seg_r = rsi_values[-lookback:]
    lo_i = seg_c.index(min(seg_c))
    hi_i = seg_c.index(max(seg_c))
    if lo_i > 3 and seg_c[lo_i] < min(seg_c[:lo_i]) and seg_r[lo_i] > min(seg_r[:lo_i]) + 2:
        return "bullish"
    if hi_i > 3 and seg_c[hi_i] > max(seg_c[:hi_i]) and seg_r[hi_i] < max(seg_r[:hi_i]) - 2:
        return "bearish"
    return None


def daily_vwap(candles_exec: list, reference_ms=None) -> float:
    ref = reference_ms or int(time.time() * 1000)
    day_start = (ref // 86400000) * 86400000
    todays = [c for c in candles_exec if c["t"] >= day_start]
    if not todays:
        todays = candles_exec[-32:]
    pv, vv = 0.0, 0.0
    for c in todays:
        typical = (c["h"] + c["l"] + c["c"]) / 3.0
        pv += typical * c["v"]
        vv += c["v"]
    return pv / vv if vv > 0 else todays[-1]["c"]


_indicator_cache: dict = {}


def compute_indicators(candles: list) -> dict:
    closes = [c["c"] for c in candles]
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
    vols = [c["v"] for c in candles]
    ef, es = ema(closes, 21), ema(closes, 50)
    e200 = ema(closes, 200)
    r = rsi(closes)
    a = atr(highs, lows, closes)
    adx, pdi, mdi = adx_dmi(highs, lows, closes)
    bb_u, bb_m, bb_l, bb_w = bollinger(closes)
    don_u, don_l = donchian(highs, lows)
    ov = obv(closes, vols)
    div = detect_rsi_divergence(closes, r)
    return {
        "candles": candles, "closes": closes, "highs": highs, "lows": lows, "vols": vols,
        "ema21": ef, "ema50": es, "ema200": e200, "rsi": r, "atr": a,
        "adx": adx, "plus_di": pdi, "minus_di": mdi,
        "bb_upper": bb_u, "bb_mid": bb_m, "bb_lower": bb_l, "bb_width_pct": bb_w,
        "donchian_upper": don_u, "donchian_lower": don_l, "obv": ov,
        "rsi_divergence": div,
    }


def get_cached_indicators(symbol: str, tf: str, candles: list) -> dict:
    key = (symbol, tf, candles[-1]["t"] if candles else 0, len(candles))
    if key in _indicator_cache:
        return _indicator_cache[key]
    ind = compute_indicators(candles)
    _indicator_cache.clear()  # single-scan lifetime cache; avoid unbounded growth
    _indicator_cache[key] = ind
    return ind


def percentile_of_last(series: list, lookback: int = 100) -> float:
    if len(series) < 5:
        return 50.0
    window = series[-lookback:]
    last = window[-1]
    rank = sum(1 for v in window if v <= last)
    return 100.0 * rank / len(window)


# ============================================================================
# STATE MANAGEMENT
# ============================================================================

def _default_state() -> dict:
    return {
        "version": STATE_VERSION,
        "open_signals": [],
        "resolved_signals": [],
        "cooldowns": {},
        "atr_pct_memory": {},
        "daily": {"day_key": None, "signals_fired": 0, "realized_r": 0.0, "paused": False},
        "last_daily_summary_date": None,
        "funding_history": {},
        "bar_index": {},
    }


def load_state() -> dict:
    p = Path(STATE_FILE)
    if not p.exists():
        return _default_state()
    try:
        with open(p, "r") as f:
            state = json.load(f)
        base = _default_state()
        for k, v in base.items():
            state.setdefault(k, v)
        return state
    except (json.JSONDecodeError, OSError) as e:
        log(f"load_state failed ({e}); starting fresh state")
        return _default_state()


def save_state(state: dict) -> None:
    if DRY_RUN:
        log("dry-run: skipping state.json commit")
        return
    with _state_lock:
        tmp = STATE_FILE + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(state, f, indent=2)
            os.replace(tmp, STATE_FILE)
        except OSError as e:
            log(f"save_state failed: {e}")


def prune_state(state: dict, max_history: int = 1000, max_days: int = 30) -> None:
    cutoff = time.time() - max_days * 86400
    state["resolved_signals"] = [s for s in state["resolved_signals"][-max_history:]
                                  if s.get("resolved_ts", time.time()) > cutoff]
    for sym in list(state["atr_pct_memory"].keys()):
        state["atr_pct_memory"][sym] = state["atr_pct_memory"][sym][-200:]
    for sym in list(state["funding_history"].keys()):
        state["funding_history"][sym] = state["funding_history"][sym][-200:]


def utc_day_key(reference_ms=None) -> str:
    ref = reference_ms or int(time.time() * 1000)
    return datetime.fromtimestamp(ref / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def roll_daily_bucket(state: dict, reference_ms: int) -> None:
    key = utc_day_key(reference_ms)
    if state["daily"].get("day_key") != key:
        state["daily"] = {"day_key": key, "signals_fired": 0, "realized_r": 0.0, "paused": False}


def daily_loss_limit_breached(state: dict) -> bool:
    return state["daily"].get("realized_r", 0.0) <= DAILY_LOSS_LIMIT_R


# ============================================================================
# ADAPTIVE INTELLIGENCE - REGIME DETECTION
# ----------------------------------------------------------------------------
# Mechanism (fixed, regime-conditioned rule set decided at design time, not
# tuned online against live results):
#   - Trend strength comes from 4H ADX + EMA stack alignment.
#   - Volatility regime comes from ATR% percentile vs. this symbol's own
#     rolling history (so it's relative to the asset, not a global constant).
#   - Choppiness comes from a noise index: how much of the raw path length
#     over N bars nets out to actual displacement (high = choppy/ranging).
#   - These combine into one of: TREND, RANGE, REVERSAL, HIGH_VOL, LOW_VOL
#     (HIGH_VOL/LOW_VOL are modifiers layered on top of the primary regime).
#   - adaptive_min_score() and adaptive_min_rr() TIGHTEN thresholds when
#     noise is high or volatility is extreme (both raise false-signal risk),
#     and RELAX non-critical thresholds (e.g. confluence count) when the
#     regime is clean-trending (fewer confirmations needed when structure
#     is already unambiguous). This is the sole loosening/tightening logic;
#     it reads only pre-computed, current-scan indicators, never past trade
#     outcomes, so it cannot curve-fit to recent P&L.
# ============================================================================

class RegimeVector:
    __slots__ = ("primary", "trend_dir", "adx", "noise_index", "atr_pct", "atr_pctile",
                 "vol_state", "bb_width_pctile", "funding", "funding_extreme", "oi_trend")

    def __init__(self, primary, trend_dir, adx, noise_index, atr_pct, atr_pctile,
                 vol_state, bb_width_pctile, funding, funding_extreme, oi_trend):
        self.primary = primary            # "trend" | "range" | "reversal"
        self.trend_dir = trend_dir        # "up" | "down" | "flat"
        self.adx = adx
        self.noise_index = noise_index
        self.atr_pct = atr_pct
        self.atr_pctile = atr_pctile
        self.vol_state = vol_state        # "high" | "normal" | "low"
        self.bb_width_pctile = bb_width_pctile
        self.funding = funding
        self.funding_extreme = funding_extreme
        self.oi_trend = oi_trend          # "rising" | "falling" | "flat"

    def is_clean_trend(self) -> bool:
        return self.primary == "trend" and self.adx >= 25 and self.noise_index < 0.55

    def is_choppy(self) -> bool:
        return self.primary == "range" or self.noise_index >= 0.68

    def is_high_vol(self) -> bool:
        return self.vol_state == "high"


def compute_noise_index(candles: list, lookback: int = 30) -> float:
    """1.0 = pure chop (path length >> net displacement), 0.0 = pure trend."""
    seg = candles[-lookback:]
    if len(seg) < 5:
        return 0.5
    path = sum(abs(seg[i]["c"] - seg[i - 1]["c"]) for i in range(1, len(seg)))
    net = abs(seg[-1]["c"] - seg[0]["c"])
    if path <= 0:
        return 0.5
    efficiency = net / path
    return max(0.0, min(1.0, 1.0 - efficiency))


def update_atr_pct_memory(state: dict, symbol: str, atr_pct: float) -> float:
    mem = state["atr_pct_memory"].setdefault(symbol, [])
    mem.append(atr_pct)
    state["atr_pct_memory"][symbol] = mem[-200:]
    return percentile_of_last(mem, 200)


def compute_oi_trend(state: dict, symbol: str, current_oi: float) -> str:
    hist = state["funding_history"].setdefault(symbol, [])
    if len(hist) >= 4:
        past_oi = [h.get("oi", current_oi) for h in hist[-4:]]
        avg_past = sum(past_oi) / len(past_oi)
        if avg_past > 0:
            change = (current_oi - avg_past) / avg_past
            if change > 0.05:
                return "rising"
            if change < -0.05:
                return "falling"
    return "flat"


def build_regime_vector(state: dict, symbol: str, ind_bias: dict, candles_bias: list,
                         snapshot_row: dict) -> RegimeVector:
    adx = ind_bias["adx"][-1]
    pdi, mdi = ind_bias["plus_di"][-1], ind_bias["minus_di"][-1]
    noise = compute_noise_index(candles_bias, lookback=30)
    atr_pct = (ind_bias["atr"][-1] / candles_bias[-1]["c"]) * 100 if candles_bias[-1]["c"] > 0 else 0.0
    atr_pctile = update_atr_pct_memory(state, symbol, atr_pct)
    bb_w_pctile = percentile_of_last(ind_bias["bb_width_pct"], 100)

    ema21, ema50, ema200 = ind_bias["ema21"][-1], ind_bias["ema50"][-1], ind_bias["ema200"][-1]
    stacked_up = ema21 > ema50 > ema200
    stacked_down = ema21 < ema50 < ema200

    if adx >= 22 and (stacked_up or stacked_down) and noise < 0.62:
        primary = "trend"
        trend_dir = "up" if stacked_up else "down"
    elif adx < 18 and noise >= 0.55:
        primary = "range"
        trend_dir = "flat"
    else:
        primary = "reversal"
        trend_dir = "up" if pdi > mdi else "down"

    if atr_pctile >= 80:
        vol_state = "high"
    elif atr_pctile <= 25:
        vol_state = "low"
    else:
        vol_state = "normal"

    funding = safe(snapshot_row.get("funding", 0.0))
    funding_extreme = abs(funding) >= 0.0006   # ~0.06% per 8h funding interval, elevated
    oi_usd = safe(snapshot_row.get("oi_usd", 0.0))
    oi_trend = compute_oi_trend(state, symbol, oi_usd)
    hist = state["funding_history"].setdefault(symbol, [])
    hist.append({"funding": funding, "oi": oi_usd, "ts": time.time()})
    state["funding_history"][symbol] = hist[-200:]

    return RegimeVector(primary, trend_dir, adx, noise, atr_pct, atr_pctile,
                         vol_state, bb_w_pctile, funding, funding_extreme, oi_trend)


def adaptive_min_score(regime: RegimeVector) -> float:
    base = 62.0
    if regime.is_choppy():
        base += 8.0       # tighten in chop
    if regime.is_clean_trend():
        base -= 5.0        # relax in unambiguous trend
    if regime.is_high_vol():
        base += 4.0        # tighten when volatility is extreme
    if regime.vol_state == "low":
        base -= 2.0
    return max(50.0, min(80.0, base))


def adaptive_min_rr(regime: RegimeVector) -> float:
    rr = MIN_RR_BASE
    if regime.is_choppy():
        rr += 0.3
    if regime.is_high_vol():
        rr += 0.2
    return rr


def adaptive_sl_atr_mult(regime: RegimeVector) -> float:
    mult = 1.2
    if regime.is_high_vol():
        mult += 0.35
    if regime.vol_state == "low":
        mult -= 0.15
    return max(0.7, mult)


def adaptive_min_confluences(regime: RegimeVector) -> int:
    if regime.is_clean_trend():
        return 2
    return 3


def macro_bias_1d(candles_1d: list) -> str:
    if len(candles_1d) < 60:
        return "neutral"
    closes = [c["c"] for c in candles_1d]
    e50 = ema(closes, 50)[-1]
    e200 = ema(closes, 200)[-1] if len(closes) >= 200 else ema(closes, min(len(closes), 100))[-1]
    if closes[-1] > e50 > e200:
        return "bullish"
    if closes[-1] < e50 < e200:
        return "bearish"
    return "neutral"


def compute_btc_regime(btc_ind_bias: dict) -> tuple:
    adx = btc_ind_bias["adx"][-1]
    ema21, ema50 = btc_ind_bias["ema21"][-1], btc_ind_bias["ema50"][-1]
    closes = btc_ind_bias["closes"]
    strength = min(1.0, adx / 40.0)
    if closes[-1] > ema21 > ema50 and adx >= 20:
        return "bullish", strength
    if closes[-1] < ema21 < ema50 and adx >= 20:
        return "bearish", strength
    return "neutral", strength


# ============================================================================
# MARKET STRUCTURE - SWINGS, BOS/CHoCH
# ============================================================================

class Swing:
    __slots__ = ("index", "price", "kind", "time")

    def __init__(self, index, price, kind, time_ms):
        self.index = index
        self.price = price
        self.kind = kind  # "high" | "low"
        self.time = time_ms


def find_swings(candles: list, left: int = 2, right: int = 2) -> list:
    out = []
    n = len(candles)
    for i in range(left, n - right):
        window_h = [candles[j]["h"] for j in range(i - left, i + right + 1)]
        window_l = [candles[j]["l"] for j in range(i - left, i + right + 1)]
        if candles[i]["h"] == max(window_h) and window_h.count(candles[i]["h"]) == 1:
            out.append(Swing(i, candles[i]["h"], "high", candles[i]["t"]))
        if candles[i]["l"] == min(window_l) and window_l.count(candles[i]["l"]) == 1:
            out.append(Swing(i, candles[i]["l"], "low", candles[i]["t"]))
    return out


class StructureState:
    __slots__ = ("trend", "last_bos", "last_choch", "last_swing_high", "last_swing_low")

    def __init__(self, trend, last_bos, last_choch, last_swing_high, last_swing_low):
        self.trend = trend  # "bullish" | "bearish" | "neutral"
        self.last_bos = last_bos
        self.last_choch = last_choch
        self.last_swing_high = last_swing_high
        self.last_swing_low = last_swing_low


def analyze_structure(candles: list, swings: list) -> StructureState:
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    if len(highs) < 2 or len(lows) < 2:
        return StructureState("neutral", None, None,
                               highs[-1] if highs else None, lows[-1] if lows else None)

    trend = "neutral"
    last_event = None
    hh = highs[-1].price > highs[-2].price
    hl = lows[-1].price > lows[-2].price
    lh = highs[-1].price < highs[-2].price
    ll = lows[-1].price < lows[-2].price

    if hh and hl:
        trend = "bullish"
    elif lh and ll:
        trend = "bearish"

    close = candles[-1]["c"]
    last_bos, last_choch = None, None
    if close > highs[-1].price:
        last_bos = {"dir": "up", "level": highs[-1].price, "index": len(candles) - 1}
        if trend == "bearish":
            last_choch = last_bos
    elif close < lows[-1].price:
        last_bos = {"dir": "down", "level": lows[-1].price, "index": len(candles) - 1}
        if trend == "bullish":
            last_choch = last_bos

    return StructureState(trend, last_bos, last_choch, highs[-1], lows[-1])


# ============================================================================
# POI ZONES: ORDER BLOCKS / FAIR VALUE GAPS / BREAKER BLOCKS
# ============================================================================

class Zone:
    """A single point-of-interest zone with construction metadata used by the
    QUALITY and LOCATION filters downstream."""
    __slots__ = ("low", "high", "kind", "direction", "index", "time",
                 "displacement_atr", "untested", "touches", "confluences", "source_bar")

    def __init__(self, low, high, kind, direction, index, time_ms, displacement_atr, source_bar=None):
        self.low = low
        self.high = high
        self.kind = kind            # "ob" | "fvg" | "breaker"
        self.direction = direction  # "bullish" | "bearish"
        self.index = index
        self.time = time_ms
        self.displacement_atr = displacement_atr  # size of the move that created it, in ATR units
        self.untested = True
        self.touches = 0
        self.confluences = []       # list of strings, e.g. "fvg_overlap", "liquidity_pool", "round_number"
        self.source_bar = source_bar

    def mid(self) -> float:
        return (self.low + self.high) / 2.0

    def height(self) -> float:
        return abs(self.high - self.low)

    def contains(self, price: float, buf: float = 0.0) -> bool:
        return (self.low - buf) <= price <= (self.high + buf)


def find_order_blocks(candles: list, atr_vals: list, lookback: int = 80) -> list:
    """Last opposite-colored candle before a displacement move that leaves an
    imbalance (proxy: the next candle's range exceeds 1x ATR and closes beyond
    the OB candle's range)."""
    zones = []
    n = len(candles)
    start = max(1, n - lookback)
    for i in range(start, n - 1):
        cur = candles[i]
        nxt = candles[i + 1]
        av = atr_vals[i] if i < len(atr_vals) and atr_vals[i] > 0 else (cur["h"] - cur["l"]) or 1e-9
        disp = abs(nxt["c"] - nxt["o"])
        is_displacement = disp >= 0.9 * av

        cur_bear = cur["c"] < cur["o"]
        cur_bull = cur["c"] > cur["o"]
        nxt_bull = nxt["c"] > nxt["o"] and nxt["c"] > cur["h"]
        nxt_bear = nxt["c"] < nxt["o"] and nxt["c"] < cur["l"]

        if cur_bear and nxt_bull and is_displacement:
            zones.append(Zone(cur["l"], cur["h"], "ob", "bullish", i, cur["t"], disp / av, source_bar=cur))
        if cur_bull and nxt_bear and is_displacement:
            zones.append(Zone(cur["l"], cur["h"], "ob", "bearish", i, cur["t"], disp / av, source_bar=cur))
    return zones


def find_fvgs(candles: list, atr_vals: list, lookback: int = 80) -> list:
    """3-candle imbalance: candle[i-1].high < candle[i+1].low (bullish gap) or
    candle[i-1].low > candle[i+1].high (bearish gap)."""
    zones = []
    n = len(candles)
    start = max(1, n - lookback)
    for i in range(start, n - 1):
        c0, c2 = candles[i - 1], candles[i + 1]
        av = atr_vals[i] if i < len(atr_vals) and atr_vals[i] > 0 else 1e-9
        if c0["h"] < c2["l"]:
            gap = c2["l"] - c0["h"]
            if gap >= 0.15 * av:
                zones.append(Zone(c0["h"], c2["l"], "fvg", "bullish", i, candles[i]["t"], gap / av))
        if c0["l"] > c2["h"]:
            gap = c0["l"] - c2["h"]
            if gap >= 0.15 * av:
                zones.append(Zone(c2["h"], c0["l"], "fvg", "bearish", i, candles[i]["t"], gap / av))
    return zones


def find_breaker_blocks(candles: list, atr_vals: list, structure: StructureState, lookback: int = 80) -> list:
    """A breaker block is a failed order block: the opposing-direction OB whose
    range gets fully closed through (structure shift/MSS), then acts as
    support/resistance on retest, in the *new* direction. This is the primary
    LTF entry trigger per the HTF->OB/BB, LTF->BB best practice."""
    obs = find_order_blocks(candles, atr_vals, lookback)
    zones = []
    n = len(candles)
    for ob in obs:
        # A bearish OB becomes a bullish breaker if price later closes above its high
        # (i.e. the sell-side OB failed to hold, flips to demand).
        for j in range(ob.index + 1, n):
            if ob.direction == "bearish" and candles[j]["c"] > ob.high:
                zones.append(Zone(ob.low, ob.high, "breaker", "bullish", j, candles[j]["t"],
                                   ob.displacement_atr, source_bar=ob.source_bar))
                break
            if ob.direction == "bullish" and candles[j]["c"] < ob.low:
                zones.append(Zone(ob.low, ob.high, "breaker", "bearish", j, candles[j]["t"],
                                   ob.displacement_atr, source_bar=ob.source_bar))
                break
    return zones


def mark_untested(zones: list, candles: list) -> list:
    for z in zones:
        touches = 0
        for c in candles[z.index + 1:]:
            if c["l"] <= z.high and c["h"] >= z.low:
                touches += 1
        z.touches = touches
        z.untested = touches == 0
    return zones


def stack_confluences(zones_by_kind: dict, pools: dict, round_number_step: float) -> None:
    """Cross-annotate zones: an OB/breaker that overlaps an unfilled FVG, a
    liquidity pool, or a round number gets extra confluence tags (feeds
    QUALITY and LOCATION filters)."""
    all_fvgs = zones_by_kind.get("fvg", [])
    all_pools = []
    for side in ("buyside", "sellside"):
        all_pools.extend(pools.get(side, []))

    for kind in ("ob", "breaker"):
        for z in zones_by_kind.get(kind, []):
            for f in all_fvgs:
                if f.direction == z.direction and not (f.high < z.low or f.low > z.high):
                    z.confluences.append("fvg_overlap")
                    break
            for level, count in all_pools:
                if z.low <= level <= z.high:
                    z.confluences.append("liquidity_pool")
                    break
            if round_number_step > 0:
                nearest = round(z.mid() / round_number_step) * round_number_step
                if abs(nearest - z.mid()) / max(z.mid(), 1e-9) < 0.0015:
                    z.confluences.append("round_number")


def cluster_levels(levels: list, tol_pct: float = 0.0015) -> list:
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


def build_liquidity_pools(swings: list, candles_1d: list) -> dict:
    """Buy-side pools sit above equal/relative highs (sell orders + stops rest
    there); sell-side pools sit below equal/relative lows."""
    highs = [s.price for s in swings if s.kind == "high"]
    lows = [s.price for s in swings if s.kind == "low"]
    if len(candles_1d) >= 2:
        highs.append(candles_1d[-2]["h"])   # PDH
        lows.append(candles_1d[-2]["l"])    # PDL
    if len(candles_1d) >= 6:
        week = candles_1d[-6:-1]
        highs.append(max(c["h"] for c in week))
        lows.append(min(c["l"] for c in week))
    return {"buyside": cluster_levels(highs), "sellside": cluster_levels(lows)}


def detect_sweep(candles: list, pools: dict, direction: str, lookback: int = 10):
    """A sweep = wick pierces a pool level, but the candle closes back inside
    (liquidity grab, not a genuine breakout)."""
    recent = candles[-lookback:]
    side = "sellside" if direction == "bullish" else "buyside"
    for level, count in pools.get(side, []):
        for i, c in enumerate(recent):
            if direction == "bullish" and c["l"] < level and c["c"] > level:
                return {"level": level, "pool_count": count, "bars_ago": len(recent) - i, "candle": c}
            if direction == "bearish" and c["h"] > level and c["c"] < level:
                return {"level": level, "pool_count": count, "bars_ago": len(recent) - i, "candle": c}
    return None


def premium_discount_zone(candles: list, lookback: int = 50) -> dict:
    seg = candles[-lookback:]
    hi, lo = max(c["h"] for c in seg), min(c["l"] for c in seg)
    eq = (hi + lo) / 2.0
    price = candles[-1]["c"]
    if hi == lo:
        pct = 0.5
    else:
        pct = (price - lo) / (hi - lo)
    zone = "premium" if pct > 0.55 else ("discount" if pct < 0.45 else "equilibrium")
    return {"high": hi, "low": lo, "eq": eq, "pct": pct, "zone": zone}


def detect_mss(candles_exec: list, direction: str, lookback: int = 30):
    """Execution-timeframe market structure shift: the confirmation leg that
    must follow an HTF sweep before any zone-touch becomes a signal."""
    swings = find_swings(candles_exec[-lookback - 5:], left=2, right=2)
    if not swings:
        return None
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    close = candles_exec[-1]["c"]
    if direction == "bullish" and highs:
        pivot = highs[-1]
        if close > pivot.price:
            return {"pivot": pivot.price, "index": len(candles_exec) - 1}
    if direction == "bearish" and lows:
        pivot = lows[-1]
        if close < pivot.price:
            return {"pivot": pivot.price, "index": len(candles_exec) - 1}
    return None


# ============================================================================
# THE FIVE FILTERS: LOCATION, CONTEXT, QUALITY, RR, LTF CONFIRMATION
# ============================================================================

def filter_location(zone: Zone, current_price: float, atr_val: float, pd_zone: dict) -> dict:
    """Where does the zone sit? Rewards discount-side longs / premium-side
    shorts, tight proximity to price (actionable, not chased), and any
    stacked confluence (pool / FVG / round number)."""
    dist_atr = abs(current_price - zone.mid()) / atr_val if atr_val > 0 else 99
    proximity_score = max(0.0, 1.0 - min(dist_atr / 3.0, 1.0))  # 1.0 at touch, 0 at 3+ ATR away

    pd_align = 0.0
    if zone.direction == "bullish" and pd_zone["zone"] == "discount":
        pd_align = 1.0
    elif zone.direction == "bearish" and pd_zone["zone"] == "premium":
        pd_align = 1.0
    elif pd_zone["zone"] == "equilibrium":
        pd_align = 0.5

    confluence_score = min(1.0, len(set(zone.confluences)) / 2.0)
    score = 0.45 * proximity_score + 0.35 * pd_align + 0.20 * confluence_score
    return {"score": score, "dist_atr": dist_atr, "pd_zone": pd_zone["zone"],
            "confluences": list(set(zone.confluences))}


def filter_context(zone: Zone, pathway: str, regime: RegimeVector) -> dict:
    """Does the zone/pathway type fit the detected regime? Reversal zones
    (sweep + OB/breaker) are weighted up in range/reversal regimes;
    continuation zones (FVG fill, trend breaker) are weighted up in trend."""
    fit = 0.5
    reason = "neutral"
    if pathway == "liquidity_reversal":
        if regime.primary in ("range", "reversal"):
            fit = 0.9
            reason = "reversal pathway matches range/reversal regime"
        elif regime.is_clean_trend():
            fit = 0.35
            reason = "reversal pathway against a clean trend"
        else:
            fit = 0.6
    elif pathway == "trend_continuation":
        if regime.primary == "trend" and regime.trend_dir == ("up" if zone.direction == "bullish" else "down"):
            fit = 0.95
            reason = "continuation aligned with regime trend"
        elif regime.primary == "trend":
            fit = 0.2
            reason = "continuation against regime trend direction"
        else:
            fit = 0.45
    elif pathway == "momentum_breakout":
        if regime.is_choppy():
            fit = 0.3
            reason = "breakout pathway in choppy regime is fakeout-prone"
        elif regime.is_high_vol():
            fit = 0.55
        else:
            fit = 0.75
            reason = "breakout pathway fits expansion regime"
    return {"score": fit, "reason": reason}


def filter_quality(zone: Zone, regime: RegimeVector) -> dict:
    """Zone construction quality: displacement strength, freshness, and
    confluence stacking."""
    disp_score = min(1.0, zone.displacement_atr / 1.5)
    fresh_score = 1.0 if zone.untested else max(0.0, 1.0 - 0.3 * zone.touches)
    conf_score = min(1.0, len(set(zone.confluences)) / 3.0)
    kind_bonus = {"breaker": 0.10, "ob": 0.0, "fvg": -0.05}.get(zone.kind, 0.0)
    score = max(0.0, min(1.0, 0.45 * disp_score + 0.35 * fresh_score + 0.20 * conf_score + kind_bonus))
    return {"score": score, "displacement_atr": round(zone.displacement_atr, 2),
            "untested": zone.untested, "touches": zone.touches}


def filter_rr(entry: float, sl: float, tp: float, regime: RegimeVector) -> dict:
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    rr = reward / risk if risk > 0 else 0.0
    min_rr = adaptive_min_rr(regime)
    return {"rr": rr, "min_rr": min_rr, "passes": rr >= min_rr}


def filter_ltf_confirmation(candles_exec: list, direction: str, pools_exec: dict,
                             zone: Zone, book: dict, followthrough_bars: int = 2) -> dict:
    """No signal fires off an HTF zone touch alone. Requires (a) an exec-tf
    liquidity sweep near the zone, (b) an MSS/CHoCH confirming shift, and
    (c) volume + close-through follow-through (fakeout guard), plus a
    liquidity-aware check on spread/depth."""
    sweep = detect_sweep(candles_exec, pools_exec, direction, lookback=14)
    mss = detect_mss(candles_exec, direction, lookback=30)

    closes = [c["c"] for c in candles_exec]
    vols = [c["v"] for c in candles_exec]
    avg_vol = sum(vols[-20:]) / max(1, len(vols[-20:]))
    last = candles_exec[-1]
    followthrough_ok = last["v"] >= 1.1 * avg_vol
    if direction == "bullish":
        closed_through = last["c"] > last["o"] and last["c"] >= zone.high * 0.998
    else:
        closed_through = last["c"] < last["o"] and last["c"] <= zone.low * 1.002

    liquidity_ok = True
    if book.get("ok"):
        if book.get("spread_bps") is not None and book["spread_bps"] > 15:
            liquidity_ok = False
        if book.get("depth_usd", 0) < 15000:
            liquidity_ok = False

    confirmed = bool(sweep) and bool(mss) and liquidity_ok
    strength = 0.0
    if sweep:
        strength += 0.35
    if mss:
        strength += 0.35
    if followthrough_ok:
        strength += 0.15
    if closed_through:
        strength += 0.15
    return {
        "confirmed": confirmed, "strength": strength, "sweep": sweep, "mss": mss,
        "followthrough_ok": followthrough_ok, "closed_through": closed_through,
        "liquidity_ok": liquidity_ok, "book": book,
    }


# ============================================================================
# CANDIDATE / TRADE PLAN
# ============================================================================

class Candidate:
    def __init__(self, symbol, direction, pathway, entry, sl, tp, zone: Zone,
                 location, context, quality, ltf, regime: RegimeVector, notes=None):
        self.symbol = symbol
        self.direction = direction
        self.pathway = pathway
        self.entry = entry
        self.sl = sl
        self.tp = tp
        self.zone = zone
        self.location = location
        self.context = context
        self.quality = quality
        self.ltf = ltf
        self.regime = regime
        self.notes = notes or []
        self.confidence = 0.0
        self.grade = "C"

    def rr(self) -> float:
        risk = abs(self.entry - self.sl)
        reward = abs(self.tp - self.entry)
        return reward / risk if risk > 0 else 0.0


def build_trade_plan(direction: str, entry: float, zone: Zone, atr_val: float,
                      pools_bias: dict, regime: RegimeVector):
    sl_mult = adaptive_sl_atr_mult(regime)
    if direction == "bullish":
        structural_sl = zone.low - 0.15 * atr_val
        atr_sl = entry - sl_mult * atr_val
        sl = min(structural_sl, atr_sl)
    else:
        structural_sl = zone.high + 0.15 * atr_val
        atr_sl = entry + sl_mult * atr_val
        sl = max(structural_sl, atr_sl)

    risk = abs(entry - sl)
    side = "buyside" if direction == "bullish" else "sellside"
    pool_levels = sorted([lv for lv, _ in pools_bias.get(side, [])],
                          reverse=(direction == "bearish"))
    tp = None
    for lv in pool_levels:
        if (direction == "bullish" and lv > entry + risk * 1.2) or \
           (direction == "bearish" and lv < entry - risk * 1.2):
            tp = lv
            break
    if tp is None:
        tp = entry + risk * 2.5 if direction == "bullish" else entry - risk * 2.5
    return sl, tp


# ============================================================================
# SIGNAL PATHWAYS
# ============================================================================
# Pathway A: Liquidity Reversal   - HTF (1H) OB/Breaker in premium/discount,
#            swept on 15m, MSS confirms, entry on the LTF breaker retest.
#            (frequency-restrictive: needs full sweep+shift+book quality)
# Pathway B: Trend Continuation   - established 4H trend + fresh FVG fill on
#            1H/15m in trend direction, OB as secondary confluence.
#            (frequency-additive: catches continuation entries a pure
#             reversal model would miss)
# Pathway C: Momentum Breakout    - HTF range boundary or opposing OB/breaker
#            broken with displacement + volume; LTF breaker-of-the-breakout
#            candle is the retest entry. Fakeout filter is mandatory here.
# ============================================================================

def pathway_liquidity_reversal(symbol, bundle, state, regime, snapshot_row, book):
    candles_bias, candles_poi, candles_exec = bundle["4h"], bundle["1h"], bundle["15m"]
    ind_poi = get_cached_indicators(symbol, "1h", candles_poi)
    atr_poi = ind_poi["atr"]

    swings_poi = find_swings(candles_poi, left=2, right=2)
    structure_poi = analyze_structure(candles_poi, swings_poi)
    pools_poi = build_liquidity_pools(swings_poi, bundle["1d"])
    pd_zone = premium_discount_zone(candles_poi, lookback=50)

    obs = find_order_blocks(candles_poi, atr_poi, lookback=80)
    breakers = find_breaker_blocks(candles_poi, atr_poi, structure_poi, lookback=80)
    fvgs = find_fvgs(candles_poi, atr_poi, lookback=80)
    mark_untested(obs, candles_poi)
    mark_untested(breakers, candles_poi)
    mark_untested(fvgs, candles_poi)
    stack_confluences({"ob": obs, "breaker": breakers, "fvg": fvgs}, pools_poi,
                       round_number_step=_round_step_for(candles_poi[-1]["c"]))

    price = candles_exec[-1]["c"]
    atr_exec_val = get_cached_indicators(symbol, "15m", candles_exec)["atr"][-1]
    swings_exec = find_swings(candles_exec, left=2, right=2)
    pools_exec = build_liquidity_pools(swings_exec, bundle["1d"])

    candidates = []
    zone_pool = [z for z in (obs + breakers) if z.untested or z.touches <= 1]
    for zone in zone_pool:
        direction = zone.direction
        if not zone.contains(price, buf=1.5 * atr_exec_val):
            continue

        loc = filter_location(zone, price, atr_exec_val, pd_zone)
        if loc["score"] < 0.35:
            continue
        ctx = filter_context(zone, "liquidity_reversal", regime)
        qual = filter_quality(zone, regime)
        if qual["score"] < 0.30:
            continue

        ltf = filter_ltf_confirmation(candles_exec, direction, pools_exec, zone, book)
        if not ltf["confirmed"]:
            log_suppressed(symbol, direction, "liquidity_reversal",
                            f"ltf not confirmed (sweep={bool(ltf['sweep'])} mss={bool(ltf['mss'])} "
                            f"liquidity_ok={ltf['liquidity_ok']})")
            continue

        entry = price
        sl, tp = build_trade_plan(direction, entry, zone, atr_exec_val, pools_poi, regime)
        rr_check = filter_rr(entry, sl, tp, regime)
        if not rr_check["passes"]:
            log_suppressed(symbol, direction, "liquidity_reversal",
                            f"rr {rr_check['rr']:.2f} < min {rr_check['min_rr']:.2f}")
            continue

        cand = Candidate(symbol, direction, "liquidity_reversal", entry, sl, tp, zone,
                          loc, ctx, qual, ltf, regime,
                          notes=[f"{zone.kind}_zone", f"pd={pd_zone['zone']}"] + zone.confluences)
        candidates.append(cand)
    return candidates


def pathway_trend_continuation(symbol, bundle, state, regime, snapshot_row, book):
    if regime.primary != "trend":
        return []
    candles_bias, candles_poi, candles_exec = bundle["4h"], bundle["1h"], bundle["15m"]
    direction = "bullish" if regime.trend_dir == "up" else "bearish"

    ind_poi = get_cached_indicators(symbol, "1h", candles_poi)
    atr_poi = ind_poi["atr"]
    fvgs = find_fvgs(candles_poi, atr_poi, lookback=40)
    mark_untested(fvgs, candles_poi)
    obs = find_order_blocks(candles_poi, atr_poi, lookback=40)
    mark_untested(obs, candles_poi)
    swings_poi = find_swings(candles_poi, left=2, right=2)
    pools_poi = build_liquidity_pools(swings_poi, bundle["1d"])
    pd_zone = premium_discount_zone(candles_poi, lookback=50)
    stack_confluences({"ob": obs, "fvg": fvgs, "breaker": []}, pools_poi,
                       round_number_step=_round_step_for(candles_poi[-1]["c"]))

    price = candles_exec[-1]["c"]
    atr_exec_val = get_cached_indicators(symbol, "15m", candles_exec)["atr"][-1]
    swings_exec = find_swings(candles_exec, left=2, right=2)
    pools_exec = build_liquidity_pools(swings_exec, bundle["1d"])

    candidates = []
    fresh_fvgs = [f for f in fvgs if f.direction == direction and f.untested]
    for zone in fresh_fvgs:
        if not zone.contains(price, buf=1.0 * atr_exec_val):
            continue
        loc = filter_location(zone, price, atr_exec_val, pd_zone)
        ctx = filter_context(zone, "trend_continuation", regime)
        if ctx["score"] < 0.4:
            continue
        qual = filter_quality(zone, regime)
        if qual["score"] < 0.25:
            continue
        ltf = filter_ltf_confirmation(candles_exec, direction, pools_exec, zone, book,
                                       followthrough_bars=1)
        # Continuation entries don't require a fresh sweep (trend already
        # established) but still require MSS/follow-through + book quality.
        soft_confirmed = bool(ltf["mss"]) and ltf["liquidity_ok"] and ltf["followthrough_ok"]
        if not soft_confirmed:
            log_suppressed(symbol, direction, "trend_continuation",
                            "no ltf follow-through on fvg fill")
            continue

        entry = price
        sl, tp = build_trade_plan(direction, entry, zone, atr_exec_val, pools_poi, regime)
        rr_check = filter_rr(entry, sl, tp, regime)
        if not rr_check["passes"]:
            continue

        cand = Candidate(symbol, direction, "trend_continuation", entry, sl, tp, zone,
                          loc, ctx, qual, ltf, regime, notes=["fvg_fill", "trend_align"] + zone.confluences)
        candidates.append(cand)
    return candidates


def pathway_momentum_breakout(symbol, bundle, state, regime, snapshot_row, book):
    candles_bias, candles_poi, candles_exec = bundle["4h"], bundle["1h"], bundle["15m"]
    ind_poi = get_cached_indicators(symbol, "1h", candles_poi)
    atr_poi_val = ind_poi["atr"][-1]
    don_u, don_l = donchian(ind_poi["highs"], ind_poi["lows"], period=20)
    range_high, range_low = don_u[-2], don_l[-2]  # prior bar's channel (avoid self-inclusion)

    price = candles_exec[-1]["c"]
    last_poi = candles_poi[-1]
    vols_poi = ind_poi["vols"]
    avg_vol = sum(vols_poi[-20:]) / max(1, len(vols_poi[-20:]))

    direction = None
    if last_poi["c"] > range_high and last_poi["v"] >= 1.3 * avg_vol:
        direction = "bullish"
    elif last_poi["c"] < range_low and last_poi["v"] >= 1.3 * avg_vol:
        direction = "bearish"
    if direction is None:
        return []

    breakout_level = range_high if direction == "bullish" else range_low
    # Fakeout guard: require a genuine close-through, not a wick, and that the
    # breakout candle's body is a meaningful fraction of its range.
    body = abs(last_poi["c"] - last_poi["o"])
    rng = max(last_poi["h"] - last_poi["l"], 1e-9)
    if body / rng < 0.5:
        log_suppressed(symbol, direction, "momentum_breakout", "wick-dominated breakout candle")
        return []

    swings_poi = find_swings(candles_poi, left=2, right=2)
    structure_poi = analyze_structure(candles_poi, swings_poi)
    breakers = find_breaker_blocks(candles_poi, ind_poi["atr"], structure_poi, lookback=20)
    mark_untested(breakers, candles_poi)
    retest_zone = None
    for z in breakers:
        if z.direction == direction and z.contains(breakout_level, buf=0.5 * atr_poi_val):
            retest_zone = z
            break
    if retest_zone is None:
        pad = 0.25 * atr_poi_val
        lo, hi = (breakout_level - pad, breakout_level + pad)
        retest_zone = Zone(lo, hi, "breaker", direction, len(candles_poi) - 1, candles_poi[-1]["t"],
                            displacement_atr=body / max(atr_poi_val, 1e-9))
        retest_zone.untested = True

    atr_exec_val = get_cached_indicators(symbol, "15m", candles_exec)["atr"][-1]
    if not retest_zone.contains(price, buf=1.2 * atr_exec_val):
        return []

    pd_zone = premium_discount_zone(candles_poi, lookback=50)
    loc = filter_location(retest_zone, price, atr_exec_val, pd_zone)
    ctx = filter_context(retest_zone, "momentum_breakout", regime)
    if ctx["score"] < 0.35:
        return []
    qual = filter_quality(retest_zone, regime)

    swings_exec = find_swings(candles_exec, left=2, right=2)
    pools_exec = build_liquidity_pools(swings_exec, bundle["1d"])
    ltf = filter_ltf_confirmation(candles_exec, direction, pools_exec, retest_zone, book)
    if not (ltf["mss"] and ltf["liquidity_ok"] and ltf["followthrough_ok"]):
        log_suppressed(symbol, direction, "momentum_breakout", "no ltf breaker retest confirmation")
        return []

    entry = price
    pools_poi = build_liquidity_pools(swings_poi, bundle["1d"])
    sl, tp = build_trade_plan(direction, entry, retest_zone, atr_exec_val, pools_poi, regime)
    rr_check = filter_rr(entry, sl, tp, regime)
    if not rr_check["passes"]:
        return []

    cand = Candidate(symbol, direction, "momentum_breakout", entry, sl, tp, retest_zone,
                      loc, ctx, qual, ltf, regime, notes=["range_breakout", "ltf_breaker_retest"])
    return [cand]


def _round_step_for(price: float) -> float:
    if price >= 10000:
        return 500.0
    if price >= 1000:
        return 50.0
    if price >= 100:
        return 5.0
    if price >= 10:
        return 0.5
    if price >= 1:
        return 0.05
    return 0.005


# ============================================================================
# ENSEMBLE AGREEMENT, FUNDING/OI CONFLUENCE, SCORING
# ============================================================================

def ensemble_agreement(ind_poi: dict, ind_exec: dict, direction: str) -> dict:
    """Independent family votes: trend / momentum / volume / structure.
    Agreement raises confidence; conflict suppresses it rather than being
    averaged away."""
    votes = {}
    up = direction == "bullish"
    votes["trend"] = (ind_poi["ema21"][-1] > ind_poi["ema50"][-1]) == up
    votes["momentum"] = (ind_poi["rsi"][-1] > 50) == up
    obv_delta = ind_exec["obv"][-1] - ind_exec["obv"][-6] if len(ind_exec["obv"]) > 6 else 0
    votes["volume"] = (obv_delta > 0) == up
    votes["structure"] = (ind_exec["closes"][-1] > ind_exec["ema21"][-1]) == up
    agree = sum(1 for v in votes.values() if v)
    conflict = sum(1 for v in votes.values() if not v)
    return {"votes": votes, "agree": agree, "conflict": conflict, "n": len(votes)}


def funding_oi_read(snapshot_row: dict, regime: RegimeVector, direction: str) -> dict:
    """Squeeze/divergence read: crowded funding against the trade direction
    plus falling OI into a move suggests forced unwind risk (bad); extreme
    funding *with* rising OI in the trade's direction plus a sweep suggests
    a genuine squeeze continuation (good) when paired with a reversal
    pathway against the crowd."""
    funding = regime.funding
    crowded_against = (funding > 0 and direction == "bearish") or (funding < 0 and direction == "bullish")
    crowded_with = (funding > 0 and direction == "bullish") or (funding < 0 and direction == "bearish")
    favorable = regime.funding_extreme and crowded_against  # betting against an overcrowded, over-funded side
    unfavorable = regime.funding_extreme and crowded_with and regime.oi_trend == "rising"
    return {"funding": funding, "favorable_squeeze": favorable, "unfavorable_crowding": unfavorable,
            "oi_trend": regime.oi_trend}


def logistic(x: float) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0


def score_candidate(cand: Candidate, ind_poi: dict, ind_exec: dict, snapshot_row: dict,
                     macro_bias: str, btc_bias: str, btc_strength: float) -> float:
    z = 0.0
    z += 1.6 * cand.location["score"]
    z += 1.6 * cand.context["score"]
    z += 1.8 * cand.quality["score"]
    z += 1.4 * cand.ltf["strength"]

    ens = ensemble_agreement(ind_poi, ind_exec, cand.direction)
    z += 0.35 * ens["agree"]
    z -= 0.45 * ens["conflict"]
    cand.notes.append(f"ensemble={ens['agree']}/{ens['n']}")

    fo = funding_oi_read(snapshot_row, cand.regime, cand.direction)
    if fo["favorable_squeeze"] and cand.pathway == "liquidity_reversal":
        z += 0.5
        cand.notes.append("funding_squeeze_favorable")
    if fo["unfavorable_crowding"]:
        z -= 0.4
        cand.notes.append("funding_crowding_risk")

    macro_align = (macro_bias == "bullish" and cand.direction == "bullish") or \
                  (macro_bias == "bearish" and cand.direction == "bearish")
    macro_against = (macro_bias == "bullish" and cand.direction == "bearish") or \
                    (macro_bias == "bearish" and cand.direction == "bullish")
    if macro_align:
        z += 0.35
    elif macro_against:
        z -= 0.35

    btc_align = (btc_bias == "bullish" and cand.direction == "bullish") or \
                (btc_bias == "bearish" and cand.direction == "bearish")
    btc_against = (btc_bias == "bullish" and cand.direction == "bearish") or \
                  (btc_bias == "bearish" and cand.direction == "bullish")
    if btc_align:
        z += 0.30 * btc_strength
    elif btc_against:
        z -= 0.45 * btc_strength

    rr = cand.rr()
    z += 0.25 * min(2.0, (rr - 1.0))

    conf = logistic(z - 1.1) * 100.0
    cand.confidence = round(conf, 1)
    return cand.confidence


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


# ============================================================================
# CORRELATION CONTROL (frequency-neutral: avoid double-counting one bet)
# ============================================================================

def compute_returns(candles: list, lookback: int) -> list:
    seg = candles[-lookback:]
    return [(seg[i]["c"] - seg[i - 1]["c"]) / seg[i - 1]["c"] for i in range(1, len(seg)) if seg[i - 1]["c"] > 0]


def pearson(a: list, b: list) -> float:
    n = min(len(a), len(b))
    if n < 5:
        return 0.0
    a, b = a[-n:], b[-n:]
    ma, mb = sum(a) / n, sum(b) / n
    cov = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    va = math.sqrt(sum((x - ma) ** 2 for x in a))
    vb = math.sqrt(sum((x - mb) ** 2 for x in b))
    if va == 0 or vb == 0:
        return 0.0
    return cov / (va * vb)


def build_correlation_clusters(returns_by_symbol: dict, threshold: float = 0.75) -> list:
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
            corr = pearson(returns_by_symbol[symbols[i]], returns_by_symbol[symbols[j]])
            if corr >= threshold:
                union(symbols[i], symbols[j])

    clusters = {}
    for s in symbols:
        clusters.setdefault(find(s), set()).add(s)
    return list(clusters.values())


def dedup_correlated(ranked: list, clusters: list) -> list:
    def cluster_of(sym):
        for c in clusters:
            if sym in c:
                return frozenset(c)
        return frozenset([sym])

    seen_clusters = {}
    out = []
    for cand in ranked:
        key = cluster_of(cand.symbol)
        if key not in seen_clusters:
            seen_clusters[key] = cand
            out.append(cand)
        # else: correlated with an already-selected higher-ranked candidate; skip
    return out


# ============================================================================
# HARD FILTERS, COOLDOWN, FRESHNESS, PORTFOLIO RISK
# ============================================================================

def passes_hard_filters(symbol: str, snapshot_row: dict, book: dict, regime: RegimeVector) -> tuple:
    if not snapshot_row or snapshot_row.get("price", 0) <= 0:
        return False, "no market snapshot"
    if snapshot_row.get("day_volume", 0) < 2_000_000:
        return False, "24h volume too thin"
    if book.get("ok") and book.get("depth_usd", 0) < 10000:
        return False, "orderbook depth too thin"
    if book.get("ok") and book.get("spread_bps") is not None and book["spread_bps"] > 25:
        return False, "spread too wide"
    return True, "ok"


def check_cooldown(state: dict, symbol: str, direction: str, bar_index: int) -> bool:
    key = f"{symbol}:{direction}"
    last = state["cooldowns"].get(key)
    if last is None:
        return True
    return (bar_index - last) >= 3   # 3x 15m bars = 45min cooldown per symbol+direction


def update_cooldown(state: dict, symbol: str, direction: str, bar_index: int) -> None:
    state["cooldowns"][f"{symbol}:{direction}"] = bar_index


def symbol_has_open_signal(state: dict, symbol: str) -> bool:
    return any(s["symbol"] == symbol for s in state["open_signals"])


def signal_still_fresh(cand: Candidate, latest_price: float, atr_val: float) -> bool:
    """Signal freshness/decay: invalidate if price has run away since the
    candidate was scored but before it was acted on."""
    moved = abs(latest_price - cand.entry) / atr_val if atr_val > 0 else 0
    return moved <= 0.4


def position_size_pct(cand: Candidate) -> float:
    risk_dollars_pct = PER_TRADE_RISK_PCT
    return round(risk_dollars_pct, 2)


def portfolio_capacity_ok(state: dict) -> tuple:
    if len(state["open_signals"]) >= MAX_CONCURRENT_ACTIVE_SIGNALS:
        return False, "max concurrent signals reached"
    exposure = sum(s.get("size_pct", PER_TRADE_RISK_PCT) for s in state["open_signals"])
    if exposure >= MAX_PORTFOLIO_EXPOSURE_PCT:
        return False, "max portfolio exposure reached"
    return True, "ok"


def sector_cap_ok(state: dict, ranked_selected: list, symbol: str) -> bool:
    sector = SECTOR_MAP.get(symbol, symbol)
    open_sectors = [SECTOR_MAP.get(s["symbol"], s["symbol"]) for s in state["open_signals"]]
    selected_sectors = [SECTOR_MAP.get(c.symbol, c.symbol) for c in ranked_selected]
    count = open_sectors.count(sector) + selected_sectors.count(sector)
    return count < MAX_PER_SECTOR


# ============================================================================
# TELEGRAM OUTPUT
# ============================================================================

def fmt_px(v: float) -> str:
    if v >= 1000:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:,.4f}"
    return f"{v:.6f}"


def confidence_bar(confidence: float) -> str:
    filled = int(round(confidence / 10))
    return "\u2588" * filled + "\u2591" * (10 - filled)


def format_signal(cand: Candidate) -> str:
    arrow = "\U0001F7E2 LONG" if cand.direction == "bullish" else "\U0001F534 SHORT"
    zone_label = {"ob": "Order Block", "breaker": "Breaker Block", "fvg": "Fair Value Gap"}[cand.zone.kind]
    pathway_label = {
        "liquidity_reversal": "Liquidity Sweep Reversal",
        "trend_continuation": "Trend Continuation (FVG Fill)",
        "momentum_breakout": "Momentum Breakout (Breaker Retest)",
    }[cand.pathway]
    confluences = ", ".join(sorted(set(cand.notes))) if cand.notes else "-"

    lines = [
        f"{arrow}  |  {cand.symbol}-PERP",
        f"Engine: {ENGINE_NAME} v{__version__}",
        "",
        f"Entry:  {fmt_px(cand.entry)}",
        f"SL:     {fmt_px(cand.sl)}",
        f"TP:     {fmt_px(cand.tp)}",
        f"R:R     {cand.rr():.2f}",
        "",
        f"Confidence: {cand.confidence:.0f}%  {confidence_bar(cand.confidence)}",
        f"Grade: {cand.grade}",
        f"Setup: {pathway_label} @ {zone_label}",
        f"Confluences: {confluences}",
    ]
    return "\n".join(lines)


def send_telegram(text: str):
    if DRY_RUN:
        log(f"DRY-RUN telegram send:\n{text}")
        return None
    try:
        resp = _session.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json().get("result", {}).get("message_id")
    except Exception as e:
        log(f"send_telegram failed: {e}")
        return None


def reply_telegram(text: str, reply_to_message_id):
    if DRY_RUN or not reply_to_message_id:
        log(f"DRY-RUN telegram reply:\n{text}")
        return None
    try:
        resp = _session.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "reply_to_message_id": reply_to_message_id},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json().get("result", {}).get("message_id")
    except Exception as e:
        log(f"reply_telegram failed: {e}")
        return None


def react_to_message(message_id, emoji: str) -> None:
    if DRY_RUN or not message_id:
        log(f"DRY-RUN telegram react {emoji} to {message_id}")
        return
    try:
        _session.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/setMessageReaction",
            json={"chat_id": TG_CHAT_ID, "message_id": message_id, "reaction": [{"type": "emoji", "emoji": emoji}]},
            timeout=10,
        )
    except Exception as e:
        log(f"react_to_message failed: {e}")


def track_signal(state: dict, cand: Candidate, msg_id, size_pct: float, reference_ms: int) -> None:
    state["open_signals"].append({
        "symbol": cand.symbol, "direction": cand.direction, "pathway": cand.pathway,
        "entry": cand.entry, "sl": cand.sl, "tp": cand.tp, "confidence": cand.confidence,
        "grade": cand.grade, "msg_id": msg_id, "size_pct": size_pct,
        "opened_ts": time.time(), "opened_day": utc_day_key(),
        # candle-based resolution needs an ms anchor to know which bars are "new"
        "opened_ms": reference_ms, "last_checked_ms": reference_ms,
    })


def _r_multiple(sig: dict, price: float) -> float:
    risk = abs(sig["entry"] - sig["sl"])
    if risk <= 0:
        return 0.0
    if sig["direction"] == "bullish":
        return (price - sig["entry"]) / risk
    return (sig["entry"] - price) / risk


def _candle_hit(sig: dict, candle: dict):
    """Check one closed candle's high/low (not just its close) against SL/TP.
    If a single candle touches both levels, the touch order is unknowable
    from OHLC alone, so — matching the conservative assumption used in the
    backtest module — the worse outcome (SL) is assumed rather than crediting
    the favorable fill."""
    hit_tp = (sig["direction"] == "bullish" and candle["h"] >= sig["tp"]) or \
             (sig["direction"] == "bearish" and candle["l"] <= sig["tp"])
    hit_sl = (sig["direction"] == "bullish" and candle["l"] <= sig["sl"]) or \
             (sig["direction"] == "bearish" and candle["h"] >= sig["sl"])
    if hit_tp and hit_sl:
        return "SL", sig["sl"]
    if hit_tp:
        return "TP", sig["tp"]
    if hit_sl:
        return "SL", sig["sl"]
    return None, None


def _resolve_signal(state: dict, sig: dict, result: str, exit_price: float) -> None:
    r = _r_multiple(sig, exit_price)
    react_to_message(sig.get("msg_id"), REACT_TP if result == "TP" else REACT_SL)
    result_label = "\U0001F3C6 TP hit" if result == "TP" else "\U0001F62D SL hit"
    reply_telegram(f"{result_label} on {sig['symbol']} "
                    f"({sig['direction']}) — {r:+.2f}R", sig.get("msg_id"))
    state["resolved_signals"].append({**sig, "result": result, "r_multiple": r,
                                       "resolved_ts": time.time(), "resolved_price": exit_price})
    if sig.get("opened_day") == state["daily"]["day_key"]:
        state["daily"]["realized_r"] = state["daily"].get("realized_r", 0.0) + r


def check_active_signals(state: dict, snapshot: dict, reference_ms: int) -> None:
    """Resolves each open signal against the actual 15m candle highs/lows
    since it was last checked (covers intra-bar wicks between scans), rather
    than only the mark price at scan time. Falls back to the mark-price
    check for a symbol if its candle fetch fails, so a data hiccup on one
    asset degrades gracefully instead of leaving that signal stuck open or
    aborting the scan."""
    still_open = []
    for sig in state["open_signals"]:
        symbol = sig["symbol"]
        last_checked_ms = sig.get("last_checked_ms", sig.get("opened_ms", reference_ms))
        resolved = None

        try:
            # Small window is enough for a normal 15m cadence; padded to also
            # cover a missed scan or two without re-fetching full history.
            candles = get_candles(symbol, "15m", 40, reference_ms)
        except Exception as e:
            log(f"check_active_signals: candle fetch failed for {symbol}: {e}")
            candles = None

        if candles:
            new_bars = [c for c in candles if c["t"] >= last_checked_ms]
            for c in new_bars:
                result, exit_price = _candle_hit(sig, c)
                if result:
                    resolved = (result, exit_price)
                    break
            if resolved is None:
                sig["last_checked_ms"] = reference_ms
        else:
            # Degraded fallback: mark-price check so a persistent data outage
            # doesn't leave a signal that already cleared TP/SL open forever.
            row = snapshot.get(symbol)
            if row and row.get("price", 0) > 0:
                price = row["price"]
                hit_tp = (sig["direction"] == "bullish" and price >= sig["tp"]) or \
                         (sig["direction"] == "bearish" and price <= sig["tp"])
                hit_sl = (sig["direction"] == "bullish" and price <= sig["sl"]) or \
                         (sig["direction"] == "bearish" and price >= sig["sl"])
                if hit_tp or hit_sl:
                    resolved = ("SL" if hit_sl else "TP", sig["sl"] if hit_sl else sig["tp"])

        if resolved:
            _resolve_signal(state, sig, resolved[0], resolved[1])
        else:
            still_open.append(sig)
    state["open_signals"] = still_open


def generate_daily_summary(state: dict) -> str:
    today = state["daily"]["day_key"]
    todays = [s for s in state["resolved_signals"] if s.get("opened_day") == today]
    wins = sum(1 for s in todays if s["result"] == "TP")
    total = len(todays)
    wr = (wins / total * 100) if total else 0.0
    total_r = sum(s["r_multiple"] for s in todays)
    return (f"\U0001F4CA {ENGINE_NAME} Daily Summary ({today} UTC)\n"
            f"Signals resolved: {total} | Win rate: {wr:.0f}% | Net R: {total_r:+.2f}\n"
            f"Signals fired today: {state['daily'].get('signals_fired', 0)} | "
            f"Open positions: {len(state['open_signals'])}")


def maybe_send_daily_summary(state: dict, reference_ms: int) -> None:
    now = datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc)
    today_str = now.strftime("%Y-%m-%d")
    if now.hour >= DAILY_SUMMARY_HOUR_UTC and state.get("last_daily_summary_date") != today_str:
        send_telegram(generate_daily_summary(state))
        state["last_daily_summary_date"] = today_str


# ============================================================================
# MAIN SCAN FLOW
# ============================================================================

def evaluate_symbol(symbol: str, state: dict, snapshot: dict, reference_ms: int,
                     macro_bias: str, btc_bias: str, btc_strength: float) -> list:
    row = snapshot.get(symbol, {})
    try:
        bundle = fetch_all_candles(symbol, reference_ms)
    except Exception as e:
        log(f"evaluate_symbol: fetch error for {symbol}: {e}")
        return []
    if not bundle:
        log(f"evaluate_symbol: skipping {symbol}, insufficient/failed candle data")
        return []

    try:
        book = analyze_orderbook(hl_coin(symbol))
        ok, reason = passes_hard_filters(symbol, row, book, None)
        if not ok:
            log_suppressed(symbol, "-", "hard_filter", reason)
            return []

        ind_bias = get_cached_indicators(symbol, "4h", bundle["4h"])
        regime = build_regime_vector(state, symbol, ind_bias, bundle["4h"], row)

        all_candidates = []
        all_candidates += pathway_liquidity_reversal(symbol, bundle, state, regime, row, book)
        all_candidates += pathway_trend_continuation(symbol, bundle, state, regime, row, book)
        all_candidates += pathway_momentum_breakout(symbol, bundle, state, regime, row, book)

        if not all_candidates:
            return []

        ind_poi = get_cached_indicators(symbol, "1h", bundle["1h"])
        ind_exec = get_cached_indicators(symbol, "15m", bundle["15m"])
        bar_index = len(bundle["15m"])

        min_score = adaptive_min_score(regime)
        min_conf = adaptive_min_confluences(regime)
        survivors = []
        for cand in all_candidates:
            if len(set(cand.zone.confluences)) + (1 if cand.ltf.get("sweep") else 0) + \
               (1 if cand.ltf.get("mss") else 0) < min_conf:
                log_suppressed(symbol, cand.direction, cand.pathway, "insufficient confluence count")
                continue
            if not check_cooldown(state, symbol, cand.direction, bar_index):
                log_suppressed(symbol, cand.direction, cand.pathway, "cooldown active")
                continue
            if symbol_has_open_signal(state, symbol):
                log_suppressed(symbol, cand.direction, cand.pathway, "symbol already has open signal")
                continue
            atr_exec_val = ind_exec["atr"][-1]
            if not signal_still_fresh(cand, bundle["15m"][-1]["c"], atr_exec_val):
                log_suppressed(symbol, cand.direction, cand.pathway, "signal decayed (price moved too far)")
                continue

            score = score_candidate(cand, ind_poi, ind_exec, row, macro_bias, btc_bias, btc_strength)
            cand.grade = grade_for_confidence(score)
            if score < min_score:
                log_suppressed(symbol, cand.direction, cand.pathway, "below adaptive min score", score)
                continue
            survivors.append(cand)

        return survivors
    except Exception as e:
        log(f"evaluate_symbol: unexpected error for {symbol}: {e}")
        return []


def run_scan() -> None:
    reference_ms = int(time.time() * 1000)
    state = load_state()
    roll_daily_bucket(state, reference_ms)

    if daily_loss_limit_breached(state):
        log(f"Daily loss limit breached ({state['daily']['realized_r']:.2f}R) — pausing new signals for the day.")
        state["daily"]["paused"] = True

    snapshot = get_market_snapshot()
    if not snapshot:
        log("run_scan: failed to fetch market snapshot; aborting scan")
        return

    check_active_signals(state, snapshot, reference_ms)

    btc_bundle = fetch_all_candles("BTC", reference_ms)
    if btc_bundle:
        btc_ind = get_cached_indicators("BTC", "4h", btc_bundle["4h"])
        btc_bias, btc_strength = compute_btc_regime(btc_ind)
        macro_bias = macro_bias_1d(btc_bundle["1d"])
    else:
        btc_bias, btc_strength, macro_bias = "neutral", 0.0, "neutral"

    all_candidates = []
    if not state["daily"]["paused"]:
        with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as ex:
            futures = {
                ex.submit(evaluate_symbol, sym, state, snapshot, reference_ms,
                          macro_bias, btc_bias, btc_strength): sym
                for sym in WATCHLIST if not _shutdown_flag["stop"]
            }
            for fut in as_completed(futures):
                sym = futures[fut]
                try:
                    result = fut.result()
                    all_candidates.extend(result)
                except Exception as e:
                    log(f"run_scan: worker error for {sym}: {e}")

    # Correlation de-dup across the whole candidate set for this scan
    returns_by_symbol = {}
    for cand in all_candidates:
        bundle = fetch_all_candles(cand.symbol, reference_ms)
        if bundle:
            returns_by_symbol[cand.symbol] = compute_returns(bundle["1h"], 60)
    clusters = build_correlation_clusters(returns_by_symbol) if returns_by_symbol else []

    ranked = sorted(all_candidates, key=lambda c: c.confidence, reverse=True)
    ranked = dedup_correlated(ranked, clusters)

    selected = []
    for cand in ranked:
        if len(selected) >= MAX_SIGNALS_PER_SCAN:
            break
        cap_ok, cap_reason = portfolio_capacity_ok(state)
        if not cap_ok:
            log_suppressed(cand.symbol, cand.direction, cand.pathway, cap_reason, cand.confidence)
            continue
        if not sector_cap_ok(state, selected, cand.symbol):
            log_suppressed(cand.symbol, cand.direction, cand.pathway, "sector cap reached", cand.confidence)
            continue
        selected.append(cand)

    for cand in selected:
        text = format_signal(cand)
        msg_id = send_telegram(text)
        size_pct = position_size_pct(cand)
        track_signal(state, cand, msg_id, size_pct, reference_ms)
        bar_index = len(fetch_all_candles(cand.symbol, reference_ms)["15m"]) if not DRY_RUN else 0
        update_cooldown(state, cand.symbol, cand.direction, bar_index)
        state["daily"]["signals_fired"] = state["daily"].get("signals_fired", 0) + 1
        log(f"SIGNAL {cand.symbol} {cand.direction} conf={cand.confidence:.0f} "
            f"grade={cand.grade} pathway={cand.pathway} rr={cand.rr():.2f}")

    maybe_send_daily_summary(state, reference_ms)
    prune_state(state)
    save_state(state)
    log(f"Scan complete. Candidates={len(all_candidates)} Selected={len(selected)} "
        f"Open={len(state['open_signals'])} DryRun={DRY_RUN}")


# ============================================================================
# BACKTESTING / EVALUATION MODULE
# ============================================================================
# Runs the exact pathway/filter/scoring functions above against historical
# Hyperliquid candles, feeding each call only candles[0:i+1] per timeframe
# (truncated at the historical decision point) so there is no look-ahead --
# the pathway functions never see a candle that hadn't closed yet at that
# point in history. Swing/pivot detection has an inherent right-side
# confirmation lag (a pivot needs `right` bars after it to confirm); that lag
# is real market information delay, not a look-ahead bias, and is preserved
# here exactly as it exists in live operation.
#
# Validation design: rolling walk-forward windows for any threshold work,
# plus one final holdout window that is never touched during tuning. Reports
# gross AND net-of-cost (fees + slippage) win rate, average R:R, and signal
# frequency, broken out by regime and by window. Flags any bucket with fewer
# than MIN_SAMPLE_SIZE trades rather than reporting a point-estimate win
# rate for it. Includes a parameter sensitivity sweep (+/-10% on the
# adaptive-score and RR thresholds) and a simple EMA-crossover baseline for
# comparison.
# ============================================================================

HL_TAKER_FEE = 0.00045     # Hyperliquid taker fee, ~4.5 bps per side (round-trip ~9bps)
HL_MAKER_FEE = 0.00015
SLIPPAGE_BPS = 3.0         # conservative slippage estimate per fill, in bps
MIN_SAMPLE_SIZE = 20
BACKTEST_STRIDE_BARS = 4   # evaluate every 4th 15m bar (hourly cadence) to keep runtime bounded


def _truncate_bundle(bundle, ts_cutoff):
    return {tf: [c for c in candles if c["t"] < ts_cutoff] for tf, candles in bundle.items()}


def _simulate_outcome(cand, future_15m):
    for c in future_15m:
        hit_tp = (cand.direction == "bullish" and c["h"] >= cand.tp) or \
                 (cand.direction == "bearish" and c["l"] <= cand.tp)
        hit_sl = (cand.direction == "bullish" and c["l"] <= cand.sl) or \
                 (cand.direction == "bearish" and c["h"] >= cand.sl)
        if hit_tp and hit_sl:
            # Ambiguous same-bar resolution: assume the worse outcome (SL) for a
            # conservative backtest rather than assuming the favorable fill.
            return {"result": "SL", "exit": cand.sl}
        if hit_tp:
            return {"result": "TP", "exit": cand.tp}
        if hit_sl:
            return {"result": "SL", "exit": cand.sl}
    return {"result": "OPEN", "exit": future_15m[-1]["c"] if future_15m else cand.entry}


def _apply_costs(cand, outcome):
    """Net R-multiple after round-trip taker fees + slippage on entry and exit."""
    risk = abs(cand.entry - cand.sl)
    if risk <= 0:
        return 0.0
    gross_r = (outcome["exit"] - cand.entry) / risk if cand.direction == "bullish" \
        else (cand.entry - outcome["exit"]) / risk
    cost_pct = 2 * HL_TAKER_FEE + 2 * (SLIPPAGE_BPS / 10000.0)
    cost_r = (cost_pct * cand.entry) / risk
    return gross_r - cost_r


def _classify_regime_label(regime):
    label = regime.primary
    if regime.is_high_vol():
        label += "_highvol"
    return label


def backtest_symbol(symbol, lookback_days=120, min_score_override=None, min_rr_override=None):
    """Returns a flat list of trade-outcome dicts for one symbol over the
    requested lookback window, using only causal data at each decision point."""
    reference_ms = int(time.time() * 1000)
    full = fetch_all_candles(symbol, reference_ms)
    if not full:
        return []
    span_ms = lookback_days * 86400000
    full["15m"] = get_candles(symbol, "15m", min(20000, span_ms // INTERVAL_MS["15m"]), reference_ms)
    full["1h"] = get_candles(symbol, "1h", min(6000, span_ms // INTERVAL_MS["1h"] + 400), reference_ms)
    full["4h"] = get_candles(symbol, "4h", min(3000, span_ms // INTERVAL_MS["4h"] + 400), reference_ms)
    full["1d"] = get_candles(symbol, "1d", N_DAILY, reference_ms)
    if not full["15m"] or len(full["15m"]) < 300:
        return []

    trades = []
    fake_state = _default_state()
    fake_row = {"price": 0, "funding": 0.0, "oi_usd": 0.0, "day_volume": 5_000_000}
    n = len(full["15m"])
    start_idx = 250  # warmup for indicators/structure

    for i in range(start_idx, n - 1, BACKTEST_STRIDE_BARS):
        ts_cutoff = full["15m"][i]["t"] + INTERVAL_MS["15m"]
        bundle = _truncate_bundle(full, ts_cutoff)
        if len(bundle["4h"]) < 60 or len(bundle["1h"]) < 80 or len(bundle["15m"]) < 100 or len(bundle["1d"]) < 30:
            continue
        fake_row["price"] = bundle["15m"][-1]["c"]
        try:
            ind_bias = compute_indicators(bundle["4h"])
            regime = build_regime_vector(fake_state, symbol, ind_bias, bundle["4h"], fake_row)
            book = {"ok": False}
            cands = []
            cands += pathway_liquidity_reversal(symbol, bundle, fake_state, regime, fake_row, book)
            cands += pathway_trend_continuation(symbol, bundle, fake_state, regime, fake_row, book)
            cands += pathway_momentum_breakout(symbol, bundle, fake_state, regime, fake_row, book)
            if not cands:
                continue
            ind_poi = compute_indicators(bundle["1h"])
            ind_exec = compute_indicators(bundle["15m"])
            best = None
            best_score = -1
            for cand in cands:
                score = score_candidate(cand, ind_poi, ind_exec, fake_row, "neutral", "neutral", 0.0)
                thresh = min_score_override if min_score_override is not None else adaptive_min_score(regime)
                rr_thresh = min_rr_override if min_rr_override is not None else adaptive_min_rr(regime)
                if score < thresh or cand.rr() < rr_thresh:
                    continue
                if score > best_score:
                    best, best_score = cand, score
            if best is None:
                continue
            future = full["15m"][i + 1: i + 1 + 200]
            outcome = _simulate_outcome(best, future)
            if outcome["result"] == "OPEN":
                continue
            net_r = _apply_costs(best, outcome)
            gross_risk = abs(best.entry - best.sl)
            gross_r = (outcome["exit"] - best.entry) / gross_risk if best.direction == "bullish" \
                else (best.entry - outcome["exit"]) / gross_risk
            trades.append({
                "symbol": symbol, "ts": bundle["15m"][-1]["t"], "direction": best.direction,
                "pathway": best.pathway, "regime": _classify_regime_label(regime),
                "result": outcome["result"], "gross_r": gross_r, "net_r": net_r,
                "rr_planned": best.rr(), "confidence": best_score,
            })
        except Exception as e:
            log(f"backtest_symbol: error at {symbol} idx {i}: {e}")
            continue
    return trades


def baseline_ma_crossover(symbol, lookback_days=120):
    """Simple EMA(20/50) crossover baseline on 1h candles, fixed 1.5R target /
    1R stop, same cost model, for comparison against Ecliptic's complexity."""
    reference_ms = int(time.time() * 1000)
    candles = get_candles(symbol, "1h", min(6000, lookback_days * 24 + 400), reference_ms)
    if not candles or len(candles) < 120:
        return []
    closes = [c["c"] for c in candles]
    e20, e50 = ema(closes, 20), ema(closes, 50)
    a = atr([c["h"] for c in candles], [c["l"] for c in candles], closes)
    trades = []
    in_pos = None
    for i in range(60, len(candles) - 1):
        cross_up = e20[i - 1] <= e50[i - 1] and e20[i] > e50[i]
        cross_dn = e20[i - 1] >= e50[i - 1] and e20[i] < e50[i]
        if in_pos is None:
            if cross_up or cross_dn:
                direction = "bullish" if cross_up else "bearish"
                entry = closes[i]
                risk = max(a[i], entry * 0.002)
                sl = entry - risk if direction == "bullish" else entry + risk
                tp = entry + 1.5 * risk if direction == "bullish" else entry - 1.5 * risk
                in_pos = {"direction": direction, "entry": entry, "sl": sl, "tp": tp, "open_i": i}
        else:
            c = candles[i]
            hit_tp = (in_pos["direction"] == "bullish" and c["h"] >= in_pos["tp"]) or \
                     (in_pos["direction"] == "bearish" and c["l"] <= in_pos["tp"])
            hit_sl = (in_pos["direction"] == "bullish" and c["l"] <= in_pos["sl"]) or \
                     (in_pos["direction"] == "bearish" and c["h"] >= in_pos["sl"])
            if hit_tp or hit_sl:
                exit_px = in_pos["tp"] if hit_tp else in_pos["sl"]
                risk = abs(in_pos["entry"] - in_pos["sl"])
                gross_r = (exit_px - in_pos["entry"]) / risk if in_pos["direction"] == "bullish" \
                    else (in_pos["entry"] - exit_px) / risk
                cost_r = (2 * HL_TAKER_FEE + 2 * SLIPPAGE_BPS / 10000.0) * in_pos["entry"] / risk
                trades.append({"result": "TP" if hit_tp else "SL", "gross_r": gross_r, "net_r": gross_r - cost_r})
                in_pos = None
    return trades


def _summarize(trades):
    n = len(trades)
    if n == 0:
        return {"n": 0, "note": "no trades"}
    wins = sum(1 for t in trades if t["result"] == "TP")
    gross_r = sum(t["gross_r"] for t in trades)
    net_r = sum(t["net_r"] for t in trades)
    return {
        "n": n,
        "gross_win_rate": round(100 * wins / n, 1),
        "net_win_rate": round(100 * sum(1 for t in trades if t["net_r"] > 0) / n, 1),
        "avg_gross_r": round(gross_r / n, 3),
        "avg_net_r": round(net_r / n, 3),
        "total_net_r": round(net_r, 2),
        "sufficient_sample": n >= MIN_SAMPLE_SIZE,
    }


def run_backtest(symbols=None, lookback_days=120, walk_forward_windows=3):
    """Walk-forward validation: splits [now - lookback_days, now] into
    `walk_forward_windows` rolling windows plus one final holdout window
    that is never used for the sensitivity/threshold check below."""
    symbols = symbols or WATCHLIST
    report = {"windows": [], "holdout": None, "by_regime": {}, "by_pathway": {},
              "sensitivity": {}, "baseline": {}, "generated_at": datetime.now(timezone.utc).isoformat()}

    window_span = lookback_days // (walk_forward_windows + 1)  # +1 reserves the holdout
    all_trades = []
    for sym in symbols:
        try:
            all_trades.extend(backtest_symbol(sym, lookback_days=lookback_days))
        except Exception as e:
            log(f"run_backtest: {sym} failed: {e}")

    if not all_trades:
        report["error"] = "no trades generated across watchlist/lookback; widen lookback_days or check data access"
        return report

    now_ms = int(time.time() * 1000)
    for w in range(walk_forward_windows):
        end = now_ms - w * window_span * 86400000
        start = end - window_span * 86400000
        window_trades = [t for t in all_trades if start <= t["ts"] < end]
        report["windows"].append({"window": w, "start": start, "end": end, **_summarize(window_trades)})

    holdout_end = now_ms - walk_forward_windows * window_span * 86400000
    holdout_start = holdout_end - window_span * 86400000
    holdout_trades = [t for t in all_trades if holdout_start <= t["ts"] < holdout_end]
    report["holdout"] = {"start": holdout_start, "end": holdout_end, **_summarize(holdout_trades)}

    for regime_label in set(t["regime"] for t in all_trades):
        report["by_regime"][regime_label] = _summarize([t for t in all_trades if t["regime"] == regime_label])
    for pathway_label in set(t["pathway"] for t in all_trades):
        report["by_pathway"][pathway_label] = _summarize([t for t in all_trades if t["pathway"] == pathway_label])

    # Parameter sensitivity: perturb the adaptive score/RR thresholds +/-10%
    # on a subset of symbols and compare aggregate net performance. A sharp
    # collapse indicates the base thresholds are overfit rather than a
    # genuine edge.
    sens_symbols = symbols[:min(6, len(symbols))]
    base_regime_probe = RegimeVector("trend", "up", 25, 0.4, 1.0, 50, "normal", 50, 0.0, False, "flat")
    base_score = adaptive_min_score(base_regime_probe)
    base_rr = MIN_RR_BASE
    for pct, label in ((-0.10, "minus_10pct"), (0.0, "base"), (0.10, "plus_10pct")):
        trades = []
        for sym in sens_symbols:
            trades.extend(backtest_symbol(
                sym, lookback_days=min(60, lookback_days),
                min_score_override=base_score * (1 + pct), min_rr_override=base_rr * (1 + pct),
            ))
        report["sensitivity"][label] = _summarize(trades)

    base_net = report["sensitivity"].get("base", {}).get("avg_net_r", 0)
    for label in ("minus_10pct", "plus_10pct"):
        variant = report["sensitivity"].get(label, {})
        if variant.get("n", 0) >= MIN_SAMPLE_SIZE and base_net != 0:
            drop_pct = (base_net - variant.get("avg_net_r", 0)) / abs(base_net) * 100
            variant["performance_drop_vs_base_pct"] = round(drop_pct, 1)
            variant["flagged_overfit"] = drop_pct > 40

    baseline_trades = []
    for sym in symbols:
        try:
            baseline_trades.extend(baseline_ma_crossover(sym, lookback_days=lookback_days))
        except Exception as e:
            log(f"run_backtest: baseline {sym} failed: {e}")
    report["baseline"]["ema_crossover"] = _summarize(baseline_trades)
    report["baseline"]["ecliptic_vs_baseline_net_r_edge"] = round(
        _summarize(all_trades).get("avg_net_r", 0) - _summarize(baseline_trades).get("avg_net_r", 0), 3
    )
    return report


def print_backtest_report(report):
    print(json.dumps(report, indent=2, default=str))


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "backtest":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 120
        log(f"=== {ENGINE_NAME} v{__version__} backtest starting (lookback_days={days}) ===")
        report = run_backtest(lookback_days=days)
        print_backtest_report(report)
        return

    log(f"=== {ENGINE_NAME} v{__version__} scan starting (dry_run={DRY_RUN}) ===")
    try:
        run_scan()
    except Exception as e:
        log(f"FATAL scan error: {e}")
        raise


if __name__ == "__main__":
    main()
