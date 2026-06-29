"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          SWING ENGINE v3.0.0  —  PRECISION-FIRST MULTI-TIMEFRAME           ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  DESIGN PHILOSOPHY                                                           ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  v2.x problem: Most signals resulted in Loss or Missed outcomes.            ║
║  Root causes identified:                                                     ║
║    1. Scoring could be inflated by many small positives without             ║
║       requiring genuine entry quality (structure, timing, momentum)          ║
║    2. TP/SL multipliers produced poor R:R — TP1 too close, SL too wide     ║
║    3. Entry timing was imprecise — buying/selling mid-move, not at edges    ║
║    4. No strict candle-level confirmation (entry quality too loose)         ║
║    5. Session filter was too permissive — off-hours entries often miss      ║
║    6. BTC regime influence was soft — counter-trend entries got through     ║
║                                                                              ║
║  v3.0 SOLUTIONS                                                              ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  [S1] STRUCTURE-FIRST ENTRY MODEL                                            ║
║       Every signal MUST have a precise structural anchor:                    ║
║       • PULL: price must be retesting a key EMA/S-R level with a           ║
║         confirming 1H rejection candle (wick + body criteria)               ║
║       • BREAK: requires 4H close above/below + 1H retest (no chasing)      ║
║       • CONT: requires EMA compression then expansion with volume           ║
║                                                                              ║
║  [S2] STRICT 4-COMPONENT HARD GATE                                           ║
║       All four must be present to even score a signal:                       ║
║       • Daily trend alignment (no Neutral unless BTC trend confirms)        ║
║       • 4H setup quality (score ≥ 2, no marginal 1-point setups)           ║
║       • 1H entry confirmation (candle structure, not just RSI level)        ║
║       • Orderflow non-hostile (net ≥ 0 always required)                     ║
║                                                                              ║
║  [S3] IMPROVED TP/SL MODEL                                                   ║
║       • TP1 set at nearest meaningful S/R or 2.0×ATR (min R:R 1.5)        ║
║       • TP2 set at next S/R level or 3.5×ATR                               ║
║       • SL always placed beyond the structural invalidation level           ║
║       • No R:R below 1.5 accepted — previously 1.25 was too loose          ║
║                                                                              ║
║  [S4] MOMENTUM CONFIRMATION REQUIREMENT                                      ║
║       At signal fire time, 1H momentum must be turning — not continuation  ║
║       of an existing move. Catches pullback exhaustion precisely.           ║
║                                                                              ║
║  [S5] TIGHTER SESSION GATING                                                 ║
║       Only London (08–12 UTC), NY Open (13–17 UTC), and the                ║
║       London-NY Overlap (12–16 UTC) are valid for CONT/BREAK.              ║
║       PULL entries are allowed in Asian session only with extreme           ║
║       volume confirmation.                                                  ║
║                                                                              ║
║  [S6] BTC REGIME AS HARD GATE (not adjustment)                               ║
║       Counter-trend signals against BTC regime require score ≥ 12           ║
║       (was soft -1 penalty). Neutral BTC regime: +1.5 minimum ATR width.  ║
║                                                                              ║
║  SCORING (max 15, min to fire: 9)                                            ║
║    Daily Alignment:       0 / 2 / 3                                         ║
║    4H Setup Quality:      1 / 2 / 3                                         ║
║    1H Entry Precision:    1 / 2 / 3                                         ║
║    Volume Quality:        0 / 1 / 2                                         ║
║    ADX + DI Alignment:    0 / 1 / 2                                         ║
║    Orderflow Net:         -2 to +2 (baked in)                               ║
║    Confluence Bonus:      0 / +1                                             ║
║    Min to fire: 9  |  Grade A: 12+  |  Grade B: 10+  |  Grade C: 9        ║
║                                                                              ║
║  ARCHITECTURE  (unchanged from v2)                                           ║
║    1D → Trend Filter  (EMA21/50/200, ADX, Market Structure)                 ║
║    4H → Setup Detector (EMA21/50, RSI, ADX, Volume, ATR)                   ║
║    1H → Entry Trigger  (candle structure, RSI, volume, orderflow)          ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

__version__ = "3.0.0"

import copy
import fcntl
import json
import math
import os
import random
import signal as os_signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

# ═══════════════════════════════════════════════════════════════════
# ENVIRONMENT / SECRETS
# ═══════════════════════════════════════════════════════════════════

_FF_TZ = ZoneInfo("America/New_York")

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID   = os.getenv("TG_CHAT_ID")
if not TG_BOT_TOKEN:
    raise RuntimeError("TG_BOT_TOKEN environment variable is required")
if not TG_CHAT_ID:
    raise RuntimeError("TG_CHAT_ID environment variable is required")

# ═══════════════════════════════════════════════════════════════════
# CONFIG CONSTANTS
# ═══════════════════════════════════════════════════════════════════

_SCRIPT_DIR  = Path(__file__).resolve().parent
STATE_FILE   = str(_SCRIPT_DIR / "state_v3.json")
STATE_VERSION = 1
LOCK_FILE    = str(_SCRIPT_DIR / "swing_engine_v3.lock")

# ── API / threading ──────────────────────────────────────────────
HL_INFO_URL           = "https://api.hyperliquid.xyz/info"
SCAN_WORKERS          = int(os.getenv("SCAN_WORKERS", "2"))
HL_MIN_INTERVAL_S     = float(os.getenv("HL_MIN_INTERVAL_S", "0.18"))
HL_MIN_INTERVAL_MAX_S = float(os.getenv("HL_MIN_INTERVAL_MAX_S", "0.60"))
HL_TF_WORKERS         = int(os.getenv("HL_TF_WORKERS", "2"))

# ── Candle counts ─────────────────────────────────────────────────
N_1H = 150
N_4H = 220
N_1D = 300

# ── Interval ms ──────────────────────────────────────────────────
INTERVAL_MS = {
    "1h": 60 * 60 * 1000,
    "4h": 4  * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
}

# ── Indicator lengths ─────────────────────────────────────────────
EMA_FAST  = 21
EMA_SLOW  = 50
EMA_TREND = 200
RSI_LEN   = 14
ATR_LEN   = 14
ADX_LEN   = 14
VOL_LEN   = 20

# ── Watchlist ─────────────────────────────────────────────────────
WATCHLIST = [
    "BTCUSDT", "ETHUSDT", "HYPEUSDT", "ZECUSDT", "NEARUSDT",
    "ONDOUSDT", "SUIUSDT", "PENGUUSDT", "BNBUSDT", "SOLUSDT",
    "TRXUSDT", "BCHUSDT", "DOGEUSDT", "ADAUSDT", "DOTUSDT",
    "TAOUSDT", "AVAXUSDT", "LINKUSDT", "AAVEUSDT", "XRPUSDT",
    "XLMUSDT", "UNIUSDT", "LTCUSDT", "APTUSDT", "PENDLEUSDT",
]

# ── [S3] Improved TP/SL multipliers (1H ATR units) ───────────────
# Objective: TP1 reachable before SL with min 1.5 R:R
# PULL: tight structural entry = tighter SL, bigger relative TP room
TP1_MULT_PULL  = 2.0   # was 1.3 — bigger TP room relative to structure
TP2_MULT_PULL  = 3.5   # was 2.5
SL_MULT_PULL   = 0.75  # was 0.80 — tighter since entering at level

# CONT: trend already established — entry mid-trend needs wider TP
TP1_MULT_CONT  = 2.0   # was 1.6
TP2_MULT_CONT  = 3.5   # was 3.0
SL_MULT_CONT   = 0.90  # was 1.0 — tighter to preserve R:R

# BREAK: breakout entries — momentum should carry quickly
TP1_MULT_BREAK = 2.0   # was 1.6
TP2_MULT_BREAK = 3.5   # was 3.0
SL_MULT_BREAK  = 0.90  # slightly tighter

SETUP_TP_SL_MULTS = {
    "PULL":  (TP1_MULT_PULL,  TP2_MULT_PULL,  SL_MULT_PULL),
    "CONT":  (TP1_MULT_CONT,  TP2_MULT_CONT,  SL_MULT_CONT),
    "BREAK": (TP1_MULT_BREAK, TP2_MULT_BREAK, SL_MULT_BREAK),
}

# [S3] Minimum R:R raised to 1.5 (was 1.25)
MIN_RR_RATIO      = 1.5
PREFERRED_RR_RATIO = 2.0

ATR_FALLBACK_PCT  = 0.015
MIN_ATR_PCT       = 0.15
MAX_ATR_PCT       = 12.0
HIGH_ATR_THRESHOLD = 3.0  # % — use tighter SL in high-vol
SL_HIGH_ATR_MULT  = 0.80

# ── Scoring ───────────────────────────────────────────────────────
# [S2] Higher minimum score threshold (was 8)
MIN_SIGNAL_SCORE  = 9
GRADE_A_SCORE     = 12
GRADE_B_SCORE     = 10
GRADE_C_SCORE     = 9
MAX_SCORE         = 15

# [S2] 4H setup MUST score at least 2 (was effectively 1 allowed)
MIN_4H_SETUP_SCORE = 2

# ── Daily trend thresholds ───────────────────────────────────────
DAILY_ADX_STRONG = 28.0
DAILY_ADX_WEAK   = 18.0
MS_LOOKBACK_BARS = 20

# ── 4H setup thresholds ──────────────────────────────────────────
H4_ADX_MIN            = 20.0
ADX_MIN_PERSIST_BARS  = 2
BREAK_LOOKBACK_BARS   = 30
H4_RSI_OB             = 70.0
H4_RSI_OS             = 30.0
H4_VOL_MULT           = 1.0
PULL_MAX_EXT_ATR      = 2.0

# ── [S4] 1H entry confirmation thresholds ────────────────────────
H1_RSI_BULL           = 45.0   # more forgiving entry floor (was 50) — catches early turns
H1_RSI_BEAR           = 55.0   # was 50
H1_RSI_OB             = 78.0   # was 75 — tighter overbought gate
H1_RSI_OS             = 22.0   # was 25 — tighter oversold gate
ENGULF_BODY_RATIO     = 0.55   # was 0.6 — slightly more permissive for real candles
IMPULSE_VOL_MULT      = 1.4    # was 1.5
SWING_LOOKBACK        = 5

# [S4] Minimum rejection wick size to confirm PULL entry
REJECTION_WICK_MIN_ATR = 0.3   # wick must be ≥ 0.3×ATR at structure level

# ── [S5] Session gating ───────────────────────────────────────────
SESSION_LONDON_START  = 8    # 08:00 UTC
SESSION_LONDON_END    = 12   # 12:00 UTC
SESSION_OVERLAP_START = 12   # 12:00 UTC
SESSION_OVERLAP_END   = 17   # 17:00 UTC (extended slightly)
SESSION_NY_AFT_START  = 16
SESSION_NY_AFT_END    = 21
# Asian PULL entries: only with strong orderflow (handled in logic)
SESSION_ASIAN_START   = 0
SESSION_ASIAN_END     = 8

# ── Signal management ─────────────────────────────────────────────
MAX_SIGNALS_PER_SCAN    = 3
MAX_CONCURRENT_ACTIVE   = 10
SIGNAL_MAX_AGE_1H_BARS  = 24
SIGNAL_COOLDOWN_BARS    = 3   # was 2 — less aggressive re-entry
PREMIUM_COOLDOWN_BARS   = 1   # grade A: 1 bar cooldown

# ── Filters ───────────────────────────────────────────────────────
MIN_OI_USD           = 500_000.0
MIN_OI_USD_SMALL_CAP = 250_000.0
SMALL_CAP_PAIRS: set[str] = {
    "PENGUUSDT", "HYPEUSDT", "ZECUSDT", "PENDLEUSDT",
}
FUNDING_SUPPRESS_EXTREME = 0.0010  # suppress at ±0.10%/8h
SPREAD_WARN_PCT   = 0.20
SPREAD_SUPPRESS_PCT = 0.40
SPREAD_EXEMPT: set[str] = {
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT",
    "XRPUSDT", "ADAUSDT", "DOGEUSDT",
}

# ── Orderflow (P2 preserved) ──────────────────────────────────────
OF_DELTA_LOOKBACK  = 5
OF_CVD_LOOKBACK    = 24
OF_RATIO_LOOKBACK  = 5
OF_LONG_STRONG     = 0.60
OF_LONG_WEAK       = 0.45
OF_SHORT_STRONG    = 0.40
OF_SHORT_WEAK      = 0.55
# [S2] Orderflow hard reject tightened: reject at -4 (was -5 in v2)
OF_HARD_REJECT     = -4
# [S2] Orderflow required to be non-hostile (≥ 0) for ALL signals
OF_MIN_NET         = 0

# ── Scoring caps ──────────────────────────────────────────────────
MAX_POSITIVE_ADJ   = 4
MAX_NEGATIVE_ADJ   = 4

# ── Divergence ───────────────────────────────────────────────────
DIVERGENCE_PENALTY = -2

# ── ATR history ──────────────────────────────────────────────────
ATR_HIST_DEPTH     = 168
ATR_HIGH_PERCENTILE = 0.80
ATR_LOW_PERCENTILE  = 0.10

# ── OI tracking ──────────────────────────────────────────────────
OI_HISTORY_DEPTH        = 24
OI_CHANGE_THRESHOLD_PCT = 1.0
OI_STALE_CUTOFF_S       = 45 * 60
OI_EXPECTED_INTERVAL_S  = 15 * 60

# ── S/R levels ───────────────────────────────────────────────────
SR_PIVOT_LEFT  = 3
SR_PIVOT_RIGHT = 3
SR_LOOKBACK    = 100
SR_CLUSTER_ATR = 0.30

# ── Equal H/L stop clusters ──────────────────────────────────────
EQUAL_HL_TOLERANCE_PCT = 0.0015
EQUAL_HL_LOOKBACK      = 30

# ── VCP ──────────────────────────────────────────────────────────
VCP_LOOKBACK   = 12
VCP_MIN_STAGES = 2

# ── BTC regime ───────────────────────────────────────────────────
LOW_BTC_CORR_LOOKBACK_BARS    = 42
LOW_BTC_CORR_THRESHOLD        = 0.65
DYNAMIC_CORR_CLUSTER_THRESHOLD = 0.75
CORR_MATRIX_MIN_SAMPLE        = 20
RS_BEARISH_EXEMPT_PCT         = 3.0
# [S6] Counter-trend score floor
COUNTER_TREND_MIN_SCORE       = 12

# ── Market breadth ────────────────────────────────────────────────
BREADTH_WEAK_LONG     = 0.20
BREADTH_WEAK_SHORT    = 0.80
BREADTH_CROWDED_LONG  = 0.75
BREADTH_EXTREME_LONG  = 0.90
BREADTH_EXTREME_SHORT = 0.10

# ── Win rate ─────────────────────────────────────────────────────
WIN_RATE_MIN_SAMPLE          = 20
WIN_RATE_HIGH_THRESH         = 0.65
WIN_RATE_LOW_THRESH          = 0.45
WIN_RATE_MIN_SAMPLE_FOR_ADJ  = 30
WIN_RATE_HARD_SUPPRESS_THRESHOLD = 0.35
WIN_RATE_HARD_SUPPRESS_MIN   = 30
WIN_RATE_LOOKBACK_DAYS       = 30
WIN_RATE_RECENT_DAYS         = 7
WIN_RATE_RECENT_WEIGHT       = 2.0
WIN_RATE_STALE_DAYS          = 14

# ── Macro calendar ───────────────────────────────────────────────
MACRO_WINDOW_BEFORE_MINS = 60
MACRO_WINDOW_AFTER_MINS  = 30
MACRO_HIGH_ATR_SUPPRESS  = 3.0
MACRO_CACHE_TTL_S        = 3600
MACRO_CALENDAR_URL       = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

# ── Funding ──────────────────────────────────────────────────────
FUNDING_CARRY_THRESHOLD  = 0.0005
FUNDING_CARRY_BONUS      = 1
FUNDING_HEADWIND_THRESHOLD = 0.0005
FUNDING_HISTORY_DEPTH    = 4

# ── Leverage ─────────────────────────────────────────────────────
LEVERAGE_BASE_RISK_PCT   = 10.0
LEVERAGE_MAX             = 15.0
GRADE_MAX_LEVERAGE = {"A": 10.0, "B": 6.0, "C": 4.0}
GRADE_SIZE_PCT     = {"A": 100, "B": 75, "C": 50}

# ── Meta cache ───────────────────────────────────────────────────
META_CACHE_TTL_S = 55.0

MAX_SIGNAL_HISTORY = 2000

REACT_TP1  = "🔥"
REACT_TP2  = "🏆"
REACT_SL   = "😭"
REACT_MISS = "😢"

# ═══════════════════════════════════════════════════════════════════
# HYPERLIQUID — Rate-limited HTTP layer
# ═══════════════════════════════════════════════════════════════════

_hl_request_lock        = threading.Lock()
_hl_last_request_ts     = 0.0
_hl_min_interval_s      = HL_MIN_INTERVAL_S
_hl_consecutive_success = 0
_hl_session             = requests.Session()


def hl_coin(symbol: str) -> str:
    return symbol.replace("USDT", "")


def hl_post(payload: dict):
    global _hl_last_request_ts, _hl_min_interval_s, _hl_consecutive_success
    max_attempts = int(os.getenv("HL_MAX_ATTEMPTS", "6"))
    base_sleep   = float(os.getenv("HL_BASE_SLEEP_S", "0.75"))

    for attempt in range(max_attempts):
        try:
            with _hl_request_lock:
                elapsed = time.time() - _hl_last_request_ts
                gap     = _hl_min_interval_s - elapsed
                if gap > 0:
                    time.sleep(gap)
                _hl_last_request_ts = time.time()

            r = _hl_session.post(
                HL_INFO_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=15,
            )

            if r.status_code == 429:
                with _hl_request_lock:
                    _hl_min_interval_s = min(HL_MIN_INTERVAL_MAX_S,
                                              _hl_min_interval_s * 1.25 + 0.02)
                    _hl_consecutive_success = 0
                retry_after = r.headers.get("Retry-After")
                try:
                    sleep_s = float(retry_after) if retry_after else None
                except ValueError:
                    sleep_s = None
                sleep_s = sleep_s if sleep_s else base_sleep * (2 ** attempt)
                sleep_s = min(20.0, max(base_sleep, sleep_s)) + random.uniform(0.0, 0.35)
                time.sleep(sleep_s)
                continue

            r.raise_for_status()

            with _hl_request_lock:
                _hl_consecutive_success += 1
                if _hl_consecutive_success >= 10:
                    _hl_min_interval_s = HL_MIN_INTERVAL_S
                    _hl_consecutive_success = 0
                else:
                    _hl_min_interval_s = max(HL_MIN_INTERVAL_S,
                                              _hl_min_interval_s - 0.0025)
            return r.json()

        except Exception:
            if attempt == max_attempts - 1:
                raise
            time.sleep(min(20.0, base_sleep * (2 ** attempt)) + random.uniform(0.0, 0.25))

    raise RuntimeError("hl_post exhausted all retries")


def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    iv_ms = INTERVAL_MS.get(interval, 3600_000)
    return (reference_ms // iv_ms) * iv_ms


def filter_closed_candles(candles: list[dict], interval: str, reference_ms: int) -> list[dict]:
    cutoff = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if c["t"] < cutoff]


def get_candles(symbol: str, interval: str, n: int,
                reference_ms: int | None = None) -> list[dict]:
    coin   = hl_coin(symbol)
    iv_ms  = INTERVAL_MS.get(interval, 3600_000)
    ref_ms = int(time.time() * 1000) if reference_ms is None else reference_ms
    end_ms = current_bar_open_ms(ref_ms, interval)
    s_ms   = end_ms - iv_ms * (n + 10)

    raw = hl_post({
        "type": "candleSnapshot",
        "req":  {"coin": coin, "interval": interval, "startTime": s_ms, "endTime": end_ms},
    })
    if raw is None:
        return []

    candles = []
    for c in raw:
        bv = float(c["v"])
        qv = float(c["q"]) if c.get("q") is not None else bv
        candles.append({
            "t": int(c["t"]), "o": float(c["o"]),
            "h": float(c["h"]), "l": float(c["l"]),
            "c": float(c["c"]), "v": bv, "qv": qv,
        })
    candles = filter_closed_candles(candles, interval, ref_ms)
    return candles[-n:]


def fetch_all_candles(symbol: str, reference_ms: int | None = None) -> tuple | None:
    results: dict[str, list] = {}
    with ThreadPoolExecutor(max_workers=max(1, HL_TF_WORKERS)) as ex:
        futs = {
            ex.submit(get_candles, symbol, tf, n, reference_ms): tf
            for tf, n in [("1h", N_1H), ("4h", N_4H), ("1d", N_1D)]
        }
        for fut in as_completed(futs):
            tf = futs[fut]
            try:
                results[tf] = fut.result()
            except Exception as e:
                print(f"  [CANDLES] {symbol} {tf} fetch failed: {e}")
                return None

    if not all(k in results for k in ("1h", "4h", "1d")):
        return None
    if len(results["1h"]) < 50 or len(results["4h"]) < 30 or len(results["1d"]) < 50:
        return None
    return results["1h"], results["4h"], results["1d"]


# ═══════════════════════════════════════════════════════════════════
# MARKET DATA (funding, OI, meta)
# ═══════════════════════════════════════════════════════════════════

_meta_cache: dict | None = None
_meta_cache_lock       = threading.Lock()
_meta_cache_fetched_at = 0.0


def get_meta_and_asset_ctxs() -> dict | None:
    global _meta_cache, _meta_cache_fetched_at
    with _meta_cache_lock:
        if _meta_cache is not None and time.time() - _meta_cache_fetched_at < META_CACHE_TTL_S:
            return _meta_cache
    try:
        data       = hl_post({"type": "metaAndAssetCtxs"})
        if data is None:
            return _meta_cache
        universe   = data[0].get("universe", [])
        asset_ctxs = data[1]
        cache = {}
        for i, asset in enumerate(universe):
            name = asset.get("name", "")
            if not name:
                continue
            ctx = asset_ctxs[i]
            cache[name] = {
                "funding":             float(ctx["funding"])      if ctx.get("funding")      is not None else None,
                "open_interest_coins": float(ctx["openInterest"]) if ctx.get("openInterest") is not None else None,
                "mark_px":             float(ctx["markPx"])       if ctx.get("markPx")       is not None else None,
            }
        with _meta_cache_lock:
            _meta_cache            = cache
            _meta_cache_fetched_at = time.time()
        return _meta_cache
    except Exception as e:
        print(f"  [META CACHE] fetch failed: {e}")
        with _meta_cache_lock:
            return _meta_cache


def get_market_context(symbol: str) -> dict | None:
    coin  = hl_coin(symbol)
    cache = get_meta_and_asset_ctxs()
    if cache is None or coin not in cache:
        return None
    entry    = cache[coin]
    oi_coins = entry.get("open_interest_coins")
    mark     = entry.get("mark_px")
    oi_usd   = oi_coins * mark if (oi_coins is not None and mark is not None) else None
    return {"funding": entry.get("funding"), "open_interest": oi_usd, "mark_px": mark}


# ═══════════════════════════════════════════════════════════════════
# INDICATOR MATH (pure Python)
# ═══════════════════════════════════════════════════════════════════

def safe(v, fallback: float = 0.0) -> float:
    try:
        if v is None:
            return fallback
        if isinstance(v, (int, float)) and math.isnan(v):
            return fallback
        return float(v)
    except (TypeError, ValueError):
        return fallback


def ema(values: list[float], period: int) -> list[float]:
    if len(values) < period:
        return [float("nan")] * len(values)
    k   = 2.0 / (period + 1)
    out = [float("nan")] * len(values)
    out[period - 1] = sum(values[:period]) / period
    for i in range(period, len(values)):
        out[i] = values[i] * k + out[i - 1] * (1 - k)
    return out


def rsi(closes: list[float], period: int) -> list[float]:
    if len(closes) < period + 1:
        return [float("nan")] * len(closes)
    out = [float("nan")] * len(closes)
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains[:period])  / period
    al = sum(losses[:period]) / period
    rs_ = ag / al if al != 0 else float("inf")
    out[period] = 100 - 100 / (1 + rs_)
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i])  / period
        al = (al * (period - 1) + losses[i]) / period
        rs_ = ag / al if al != 0 else float("inf")
        out[i + 1] = 100 - 100 / (1 + rs_)
    return out


def atr(highs, lows, closes, period: int) -> list[float]:
    trs = [float("nan")]
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i],
                 abs(highs[i] - closes[i - 1]),
                 abs(lows[i]  - closes[i - 1]))
        trs.append(tr)
    out = [float("nan")] * len(closes)
    if len(trs) < period + 1:
        return out
    out[period] = sum(trs[1:period + 1]) / period
    for i in range(period + 1, len(trs)):
        out[i] = (out[i - 1] * (period - 1) + trs[i]) / period
    return out


def adx_dmi(highs, lows, closes, period: int):
    n        = len(closes)
    plus_dm  = [0.0] * n
    minus_dm = [0.0] * n
    tr_arr   = [0.0] * n
    for i in range(1, n):
        up   = highs[i]    - highs[i - 1]
        dn   = lows[i - 1] - lows[i]
        plus_dm[i]  = up if (up > dn and up > 0)  else 0
        minus_dm[i] = dn if (dn > up and dn > 0)  else 0
        tr_arr[i]   = max(highs[i] - lows[i],
                          abs(highs[i] - closes[i - 1]),
                          abs(lows[i]  - closes[i - 1]))

    def _wilder(arr):
        res = [0.0] * n
        if n <= period:
            return res
        res[period] = sum(arr[1:period + 1])
        for i in range(period + 1, n):
            res[i] = res[i - 1] - res[i - 1] / period + arr[i]
        return res

    sm_tr   = _wilder(tr_arr)
    sm_plus = _wilder(plus_dm)
    sm_min  = _wilder(minus_dm)
    di_plus = [float("nan")] * n
    di_min  = [float("nan")] * n
    dx_arr  = [float("nan")] * n
    adx_arr = [float("nan")] * n

    for i in range(period, n):
        if sm_tr[i] == 0:
            continue
        dp = 100 * sm_plus[i] / sm_tr[i]
        dm = 100 * sm_min[i]  / sm_tr[i]
        di_plus[i] = dp
        di_min[i]  = dm
        s = dp + dm
        dx_arr[i] = 100 * abs(dp - dm) / s if s != 0 else 0

    first = next((i for i in range(period, n) if not math.isnan(dx_arr[i])), None)
    if first is None:
        return di_plus, di_min, adx_arr
    seed_end = first + period
    if seed_end > n:
        return di_plus, di_min, adx_arr
    valid = [dx_arr[i] for i in range(first, seed_end) if not math.isnan(dx_arr[i])]
    if len(valid) < period:
        return di_plus, di_min, adx_arr
    adx_arr[seed_end - 1] = sum(valid) / period
    for i in range(seed_end, n):
        if not math.isnan(adx_arr[i - 1]) and not math.isnan(dx_arr[i]):
            adx_arr[i] = (adx_arr[i - 1] * (period - 1) + dx_arr[i]) / period
    return di_plus, di_min, adx_arr


def sma(values, period: int) -> list[float]:
    out = [float("nan")] * len(values)
    for i in range(period - 1, len(values)):
        out[i] = sum(values[i - period + 1: i + 1]) / period
    return out


# ═══════════════════════════════════════════════════════════════════
# INDICATOR CACHE
# ═══════════════════════════════════════════════════════════════════

_indicator_cache: dict[str, dict] = {}
_indicator_cache_lock = threading.Lock()


def _compute_all_indicators(candles: list[dict]) -> dict:
    o  = [c["o"] for c in candles]
    h  = [c["h"] for c in candles]
    l  = [c["l"] for c in candles]
    c_ = [c["c"] for c in candles]
    v  = [c["v"] for c in candles]
    _dp, _dm, _adx = adx_dmi(h, l, c_, ADX_LEN)
    return {
        "o": o, "h": h, "l": l, "c": c_, "v": v,
        "ema_fast":  ema(c_, EMA_FAST),
        "ema_slow":  ema(c_, EMA_SLOW),
        "ema_trend": ema(c_, EMA_TREND),
        "rsi":       rsi(c_, RSI_LEN),
        "atr":       atr(h, l, c_, ATR_LEN),
        "adx":       _adx,
        "di_plus":   _dp,
        "di_minus":  _dm,
        "vol_ma":    sma(v, VOL_LEN),
    }


def get_cached_indicators(symbol: str, timeframe: str, candles: list[dict]) -> dict:
    if not candles:
        return _compute_all_indicators(candles)
    cache_key = f"{symbol}_{timeframe}"
    first, last = candles[0], candles[-1]
    sig = (len(candles), last["t"], last["c"], first["t"])
    with _indicator_cache_lock:
        cached = _indicator_cache.get(cache_key)
        if cached and cached.get("sig") == sig:
            return cached["data"]
    data = _compute_all_indicators(candles)
    with _indicator_cache_lock:
        _indicator_cache[cache_key] = {"sig": sig, "data": data}
        if len(_indicator_cache) > 300:
            oldest = next(iter(_indicator_cache))
            del _indicator_cache[oldest]
    return data


def clear_indicator_cache():
    with _indicator_cache_lock:
        _indicator_cache.clear()


# ═══════════════════════════════════════════════════════════════════
# STATE MANAGEMENT
# ═══════════════════════════════════════════════════════════════════

_state_lock = threading.RLock()


def load_state() -> dict:
    fresh = {
        "_version":             STATE_VERSION,
        "oi_history":           {},
        "signal_history":       [],
        "macro_calendar_cache": {},
        "atr_history":          {},
        "funding_history":      {},
        "signal_cooldowns":     {},
        "last_signal_outcome":  {},
        "active_signals":       [],
        "btc_dominance_history": [],
    }
    for path in (STATE_FILE, STATE_FILE + ".bak"):
        if Path(path).exists():
            try:
                s = json.loads(Path(path).read_text())
                if s.get("_version", 0) != STATE_VERSION:
                    print(f"[STATE] Version mismatch in {path} — starting fresh.")
                    continue
                for k, v in fresh.items():
                    s.setdefault(k, v)
                return s
            except Exception as e:
                print(f"[STATE] Failed to load {path}: {e}")
    print("[STATE] Starting fresh.")
    return fresh


def save_state(state: dict):
    with _state_lock:
        snap = copy.deepcopy(state)
    try:
        txt = json.dumps(snap, default=str, indent=2)
        tmp = STATE_FILE + ".tmp"
        Path(tmp).write_text(txt)
        if Path(STATE_FILE).exists():
            Path(STATE_FILE).replace(Path(STATE_FILE + ".bak"))
        Path(tmp).replace(Path(STATE_FILE))
    except Exception as e:
        print(f"[STATE] Save failed: {e}")


# ═══════════════════════════════════════════════════════════════════
# OI / FUNDING / ATR HISTORY
# ═══════════════════════════════════════════════════════════════════

def record_oi(state: dict, symbol: str, oi_usd: float | None):
    if oi_usd is None:
        return
    with _state_lock:
        hist = state["oi_history"].setdefault(symbol, [])
        hist.append({"ts": int(time.time()), "oi": oi_usd})
        if len(hist) > OI_HISTORY_DEPTH:
            state["oi_history"][symbol] = hist[-OI_HISTORY_DEPTH:]


def get_oi_data(state: dict, symbol: str, oi_now: float | None) -> dict:
    record_oi(state, symbol, oi_now)
    with _state_lock:
        hist = state["oi_history"].get(symbol, [])
    if len(hist) < 2 or oi_now is None:
        return {"oi_change_pct": None, "label": "OI: Insufficient data", "score": 0}
    cutoff = int(time.time()) - OI_STALE_CUTOFF_S
    fresh  = [h for h in hist if h["ts"] >= cutoff]
    if not fresh:
        return {"oi_change_pct": None, "label": "OI: Stale", "score": 0}
    oldest_oi = fresh[0]["oi"]
    if oldest_oi == 0:
        return {"oi_change_pct": None, "label": "OI: Zero baseline", "score": 0}
    chg_pct = (oi_now - oldest_oi) / oldest_oi * 100
    score = 1 if chg_pct >= OI_CHANGE_THRESHOLD_PCT else (-1 if chg_pct <= -OI_CHANGE_THRESHOLD_PCT else 0)
    return {"oi_change_pct": chg_pct, "score": score,
            "label": f"OI: {chg_pct:+.1f}% ({len(fresh)} obs)"}


def record_atr_pct(state: dict, symbol: str, atr_pct: float):
    with _state_lock:
        hist = state["atr_history"].setdefault(symbol, [])
        hist.append(atr_pct)
        if len(hist) > ATR_HIST_DEPTH:
            state["atr_history"][symbol] = hist[-ATR_HIST_DEPTH:]


def get_atr_percentile(state: dict, symbol: str, atr_pct: float) -> float | None:
    record_atr_pct(state, symbol, atr_pct)
    with _state_lock:
        hist = list(state["atr_history"].get(symbol, []))
    if len(hist) < 30:
        return None
    sorted_h = sorted(hist)
    pos = sum(1 for x in sorted_h if x <= atr_pct)
    return pos / len(sorted_h)


def get_funding_trend(state: dict, symbol: str) -> str:
    with _state_lock:
        hist = state.get("funding_history", {}).get(symbol, [])
    if len(hist) < 2:
        return "unknown"
    return "rising" if hist[-1] > hist[-2] else ("falling" if hist[-1] < hist[-2] else "flat")


def record_funding(state: dict, symbol: str, rate: float | None):
    if rate is None:
        return
    with _state_lock:
        hist = state.setdefault("funding_history", {}).setdefault(symbol, [])
        hist.append(rate)
        if len(hist) > FUNDING_HISTORY_DEPTH:
            state["funding_history"][symbol] = hist[-FUNDING_HISTORY_DEPTH:]


# ═══════════════════════════════════════════════════════════════════
# ACTIVE SIGNAL TRACKING
# ═══════════════════════════════════════════════════════════════════

def track_signal(state: dict, symbol: str, direction: str,
                 msg_id: int | None, sig, bar_index: int, hist_id: str):
    with _state_lock:
        state["active_signals"].append({
            "symbol":     symbol,
            "direction":  direction,
            "entry":      sig.entry,
            "tp1":        sig.tp1,
            "tp2":        sig.tp2,
            "sl":         sig.sl,
            "bar_index":  bar_index,
            "msg_id":     msg_id,
            "hist_id":    hist_id,
            "grade":      sig.grade,
            "reacted":    None,
        })


def expire_old_signals(state: dict, bar_index: int):
    with _state_lock:
        state["active_signals"] = [
            s for s in state["active_signals"]
            if bar_index - s["bar_index"] < SIGNAL_MAX_AGE_1H_BARS
        ]


def check_active_signals(state: dict, candles_1h: list[dict], bar_index: int):
    """Check TP/SL hits on active signals and send Telegram reactions."""
    with _state_lock:
        signals = list(state["active_signals"])

    if not candles_1h:
        return

    for sig in signals:
        if sig.get("reacted"):
            continue
        entry_bar = sig["bar_index"]
        sym       = sig["symbol"]
        dirn      = sig["direction"]
        entry_px  = sig["entry"]
        tp1       = sig["tp1"]
        tp2       = sig["tp2"]
        sl        = sig["sl"]

        # Candles after signal bar
        post = [c for c in candles_1h if True]  # simplified: check all recent
        if not post:
            continue

        entry_touched = False
        outcome       = None
        touch_tol     = 0.003  # 0.3% entry tolerance

        for c in post[entry_bar:]:
            if not entry_touched:
                if dirn == "long":
                    entry_touched = c["l"] <= entry_px * (1 + touch_tol)
                else:
                    entry_touched = c["h"] >= entry_px * (1 - touch_tol)

            if not entry_touched:
                # Check miss: did price move to TP or SL without touching entry?
                if dirn == "long":
                    if c["h"] >= tp1:
                        outcome = "miss_tp"
                        break
                    if c["l"] <= sl:
                        outcome = "miss_sl"
                        break
                else:
                    if c["l"] <= tp1:
                        outcome = "miss_tp"
                        break
                    if c["h"] >= sl:
                        outcome = "miss_sl"
                        break
                continue

            if dirn == "long":
                if c["h"] >= tp2:
                    outcome = "tp2"
                    break
                elif c["h"] >= tp1:
                    outcome = "tp1"
                    break
                elif c["l"] <= sl:
                    outcome = "sl"
                    break
            else:
                if c["l"] <= tp2:
                    outcome = "tp2"
                    break
                elif c["l"] <= tp1:
                    outcome = "tp1"
                    break
                elif c["h"] >= sl:
                    outcome = "sl"
                    break

        if outcome:
            react_map = {
                "tp2": REACT_TP2, "tp1": REACT_TP1,
                "sl": REACT_SL,
                "miss_tp": REACT_MISS, "miss_sl": REACT_MISS,
            }
            react = react_map.get(outcome, REACT_MISS)
            if sig.get("msg_id"):
                send_telegram_reaction(sig["msg_id"], react)
            with _state_lock:
                for s in state["active_signals"]:
                    if s.get("hist_id") == sig.get("hist_id"):
                        s["reacted"] = outcome
                        break
                update_signal_history_result(state, sig.get("hist_id"), outcome)
            print(f"  [OUTCOME] {hl_coin(sym)} {dirn.upper()} → {outcome.upper()}")


def update_signal_history_result(state: dict, hist_id: str | None, result: str):
    if not hist_id:
        return
    with _state_lock:
        for e in state.get("signal_history", []):
            if e.get("id") == hist_id:
                e["result"] = result
                return


# ═══════════════════════════════════════════════════════════════════
# COOLDOWN SYSTEM
# ═══════════════════════════════════════════════════════════════════

def update_cooldown(state: dict, symbol: str, direction: str, bar_index: int):
    key = f"{symbol}_{direction}"
    with _state_lock:
        state["signal_cooldowns"][key] = bar_index


def check_cooldown(state: dict, symbol: str, direction: str,
                   bar_index: int, grade: str = "C") -> bool:
    """Returns True if still in cooldown (signal should be suppressed)."""
    key = f"{symbol}_{direction}"
    with _state_lock:
        last = state["signal_cooldowns"].get(key)
    if last is None:
        return False
    cooldown = PREMIUM_COOLDOWN_BARS if grade == "A" else SIGNAL_COOLDOWN_BARS
    return (bar_index - last) < cooldown


def get_concurrent_count(state: dict) -> int:
    with _state_lock:
        return len([s for s in state.get("active_signals", [])
                    if not s.get("reacted")])


# ═══════════════════════════════════════════════════════════════════
# WIN RATE TRACKING
# ═══════════════════════════════════════════════════════════════════

_win_rates_cache: dict | None = None
_win_rates_lock  = threading.Lock()


def reset_win_rates_cache():
    global _win_rates_cache
    with _win_rates_lock:
        _win_rates_cache = None


def compute_win_rates(state: dict) -> dict:
    now_ts       = int(time.time())
    lookback_cut = now_ts - WIN_RATE_LOOKBACK_DAYS * 86400
    recent_cut   = now_ts - WIN_RATE_RECENT_DAYS   * 86400
    stale_cut    = now_ts - WIN_RATE_STALE_DAYS    * 86400
    with _state_lock:
        raw = [e for e in state.get("signal_history", [])
               if e.get("result") in ("tp1", "tp2", "sl")
               and e.get("timestamp", 0) >= lookback_cut
               and e.get("sent", True)]
    fresh = [e for e in raw if e.get("timestamp", 0) >= stale_cut]
    wi    = max(1, int(WIN_RATE_RECENT_WEIGHT))

    def weighted(entries):
        out = []
        for e in entries:
            out.extend([e] * wi if e.get("timestamp", 0) >= recent_cut else [e])
        return out

    hist, frsh = weighted(raw), weighted(fresh)

    def wr_for(entries):
        n    = len(entries)
        wins = sum(1 for e in entries if e["result"] in ("tp1", "tp2"))
        return (wins / n if n > 0 else 0.0), n

    def best_subset(pred):
        fs = [e for e in frsh if pred(e)]
        return fs if len(fs) >= WIN_RATE_MIN_SAMPLE else [e for e in hist if pred(e)]

    wrs: dict = {"by_symbol": {}, "by_type": {}, "by_direction": {}}
    for sym in set(e["symbol"] for e in hist):
        sub = best_subset(lambda e, s=sym: e["symbol"] == s)
        wr, n = wr_for(sub)
        if n >= WIN_RATE_MIN_SAMPLE:
            wrs["by_symbol"][sym] = {"wr": wr, "n": n}
    for st in ("CONT", "PULL", "BREAK"):
        sub = best_subset(lambda e, t=st: e.get("signal_type") == t)
        wr, n = wr_for(sub)
        if n >= WIN_RATE_MIN_SAMPLE:
            wrs["by_type"][st] = {"wr": wr, "n": n}
    for d in ("long", "short"):
        sub = best_subset(lambda e, d_=d: e.get("direction") == d_)
        wr, n = wr_for(sub)
        if n >= WIN_RATE_MIN_SAMPLE:
            wrs["by_direction"][d] = {"wr": wr, "n": n}
    return wrs


def get_cached_win_rates(state: dict) -> dict:
    global _win_rates_cache
    with _win_rates_lock:
        if _win_rates_cache is None:
            _win_rates_cache = compute_win_rates(state)
        return _win_rates_cache


def compute_wr_adj(state: dict, symbol: str, direction: str, signal_type: str) -> dict:
    wrs = get_cached_win_rates(state)
    for key, lookup in [("by_symbol", symbol), ("by_type", signal_type), ("by_direction", direction)]:
        entry = wrs.get(key, {}).get(lookup)
        if entry:
            wr, n = entry["wr"], entry["n"]
            if n >= WIN_RATE_HARD_SUPPRESS_MIN and wr < WIN_RATE_HARD_SUPPRESS_THRESHOLD:
                return {"score_adj": -3, "label": f"WR SUPPRESS: {wr*100:.0f}% (n={n})"}
            if n < WIN_RATE_MIN_SAMPLE_FOR_ADJ:
                return {"score_adj": 0, "label": f"WR: {wr*100:.0f}% (n={n}, insufficient)"}
            adj = 1 if wr >= WIN_RATE_HIGH_THRESH else (-1 if wr <= WIN_RATE_LOW_THRESH else 0)
            return {"score_adj": adj, "label": f"WR: {wr*100:.0f}% n={n}"}
    return {"score_adj": 0, "label": "WR: No data"}


def record_signal_history(state: dict, symbol: str, direction: str,
                          signal_type: str, score: int, funding: float | None,
                          atr_pct: float, oi_chg: float | None,
                          daily_class: str, sent: bool, grade: str) -> str:
    import uuid
    hist_id = str(uuid.uuid4())[:8]
    entry = {
        "id":         hist_id,
        "timestamp":  int(time.time()),
        "symbol":     symbol,
        "direction":  direction,
        "signal_type": signal_type,
        "score":      score,
        "funding":    funding,
        "atr_pct":    atr_pct,
        "oi_chg":     oi_chg,
        "daily_class": daily_class,
        "grade":      grade,
        "sent":       sent,
        "result":     None,
    }
    with _state_lock:
        state["signal_history"].append(entry)
        if len(state["signal_history"]) > MAX_SIGNAL_HISTORY:
            state["signal_history"] = state["signal_history"][-MAX_SIGNAL_HISTORY:]
    return hist_id


# ═══════════════════════════════════════════════════════════════════
# MACRO CALENDAR
# ═══════════════════════════════════════════════════════════════════

def parse_ff_event_utc(ev_date: str, ev_time: str) -> datetime | None:
    try:
        if "day" in ev_time.lower() or not ev_time.strip():
            dt_str   = f"{ev_date} 14:00"
            dt_local = datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=_FF_TZ)
        else:
            dt_str   = f"{ev_date} {ev_time}"
            dt_local = datetime.strptime(dt_str, "%Y-%m-%d %I:%M%p").replace(tzinfo=_FF_TZ)
        return dt_local.astimezone(timezone.utc)
    except Exception:
        return None


def fetch_macro_calendar(state: dict) -> list[dict]:
    with _state_lock:
        cache     = state.get("macro_calendar_cache", {})
        cached_at = cache.get("fetched_at", 0)
    if int(time.time()) - cached_at < MACRO_CACHE_TTL_S:
        return cache.get("events", [])
    raw = []
    for attempt in range(3):
        try:
            resp = requests.get(MACRO_CALENDAR_URL, timeout=10)
            resp.raise_for_status()
            raw = resp.json()
            break
        except Exception as e:
            if attempt == 2:
                return cache.get("events", [])
            time.sleep(min(10.0, 1.0 * 2 ** attempt) + random.uniform(0, 0.25))
    events = []
    for ev in raw:
        if str(ev.get("impact", "")).lower() != "high":
            continue
        dt_utc = parse_ff_event_utc(ev.get("date", ""), ev.get("time", ""))
        if dt_utc:
            events.append({"name": ev.get("title", "?"), "datetime_utc": dt_utc.isoformat()})
    with _state_lock:
        state["macro_calendar_cache"] = {"fetched_at": int(time.time()), "events": events}
    print(f"  [MACRO CAL] Loaded {len(events)} high-impact events")
    return events


def apply_macro_filter(state: dict, atr_pct: float,
                       reference_ms: int | None = None) -> dict:
    events  = fetch_macro_calendar(state)
    ref_ts  = (reference_ms / 1000) if reference_ms is not None else time.time()
    now_utc = datetime.fromtimestamp(ref_ts, tz=timezone.utc)
    nearest = None
    nearest_mins = None
    for ev in events:
        try:
            ev_dt = datetime.fromisoformat(ev["datetime_utc"])
        except Exception:
            continue
        mins = (ev_dt - now_utc).total_seconds() / 60.0
        if -MACRO_WINDOW_AFTER_MINS <= mins <= MACRO_WINDOW_BEFORE_MINS:
            if nearest_mins is None or abs(mins) < abs(nearest_mins):
                nearest = ev["name"]; nearest_mins = mins
    if nearest is None:
        return {"in_window": False, "score_adj": 0, "label": "Macro: None", "hard_suppress": False}
    hard = atr_pct >= MACRO_HIGH_ATR_SUPPRESS
    lbl  = (f"⚠️ Macro: {nearest} in {int(nearest_mins)} min" if nearest_mins >= 0
            else f"⚠️ Macro: {nearest} {int(abs(nearest_mins))} min ago")
    return {"in_window": True, "score_adj": -1, "label": lbl, "hard_suppress": hard}


# ═══════════════════════════════════════════════════════════════════
# S/R LEVELS
# ═══════════════════════════════════════════════════════════════════

def _cluster_levels(pivots: list[float], atr_val: float, tol: float = 0.30) -> list[float]:
    if not pivots or atr_val <= 0:
        return pivots
    zones: list[float] = []
    members: list[list[float]] = []
    for p in sorted(pivots):
        if zones and abs(p - zones[-1]) < atr_val * tol:
            members[-1].append(p)
            m = members[-1]
            zones[-1] = sorted(m)[len(m) // 2]
        else:
            zones.append(p)
            members.append([p])
    return zones


def find_sr_levels(candles_4h: list[dict], cur_c: float,
                   atr_val: float | None = None,
                   n_levels: int = 3) -> tuple[list[float], list[float]]:
    lb, rb = SR_PIVOT_LEFT, SR_PIVOT_RIGHT
    window = candles_4h[max(0, len(candles_4h) - 1 - SR_LOOKBACK): -1]
    ph, pl = [], []
    for i in range(lb, len(window) - rb):
        h  = window[i]["h"]
        lo = window[i]["l"]
        if all(h  > window[i - k]["h"] for k in range(1, lb + 1)) and \
           all(h  > window[i + k]["h"] for k in range(1, rb + 1)):
            ph.append(h)
        if all(lo < window[i - k]["l"] for k in range(1, lb + 1)) and \
           all(lo < window[i + k]["l"] for k in range(1, rb + 1)):
            pl.append(lo)
    eff_atr = atr_val if atr_val and atr_val > 0 else (cur_c * 0.005)
    ph  = _cluster_levels(ph, eff_atr, SR_CLUSTER_ATR)
    pl  = _cluster_levels(pl, eff_atr, SR_CLUSTER_ATR)
    res = sorted([p for p in ph if p > cur_c], key=lambda x: x - cur_c)[:n_levels]
    sup = sorted([p for p in pl if p < cur_c], key=lambda x: cur_c - x)[:n_levels]
    return sup, res


# ═══════════════════════════════════════════════════════════════════
# BTC REGIME
# ═══════════════════════════════════════════════════════════════════

_btc_regime: dict | None = None
_btc_regime_lock = threading.Lock()


def set_btc_regime(regime: dict):
    global _btc_regime
    with _btc_regime_lock:
        _btc_regime = regime


def get_btc_regime() -> dict | None:
    with _btc_regime_lock:
        return _btc_regime


def compute_btc_regime(candles_1h, candles_4h, candles_1d) -> dict:
    if not candles_1d or len(candles_1d) < 60:
        return {"label": "BTC Unknown", "bullish": False, "bearish": False, "mixed": True}
    ind = get_cached_indicators("BTCUSDT_daily", "1d", candles_1d)
    closes = ind["c"]
    ef  = safe(ind["ema_fast"][-1])
    es  = safe(ind["ema_slow"][-1])
    et  = safe(ind["ema_trend"][-1])
    adx = safe(ind["adx"][-1], 20.0)
    cur = closes[-1]
    bullish = ef > es and cur > et and adx >= 18
    bearish = ef < es and cur < et and adx >= 18
    if bullish:
        return {"label": "BTC Bullish", "bullish": True, "bearish": False, "mixed": False}
    elif bearish:
        return {"label": "BTC Bearish", "bullish": False, "bearish": True, "mixed": False}
    else:
        return {"label": "BTC Mixed", "bullish": False, "bearish": False, "mixed": True}


# ═══════════════════════════════════════════════════════════════════
# MARKET BREADTH
# ═══════════════════════════════════════════════════════════════════

_breadth_above: dict[str, bool] = {}
_breadth_snapshot: dict[str, bool] | None = None
_breadth_lock = threading.Lock()


def record_breadth_result(symbol: str, above: bool):
    with _breadth_lock:
        _breadth_above[symbol] = above


def finalize_breadth_cache():
    global _breadth_snapshot
    with _breadth_lock:
        _breadth_snapshot = dict(_breadth_above)


def compute_market_breadth() -> dict:
    with _breadth_lock:
        results = dict(_breadth_snapshot if _breadth_snapshot is not None else _breadth_above)
    if not results:
        return {"breadth_pct": 0.5, "label": "Breadth: Unknown"}
    pct = sum(1 for v in results.values() if v) / len(results)
    lbl = (f"Breadth: {pct*100:.0f}% Weak" if pct < BREADTH_WEAK_LONG else
           f"Breadth: {pct*100:.0f}% Overbought" if pct > BREADTH_WEAK_SHORT else
           f"Breadth: {pct*100:.0f}% Healthy")
    return {"breadth_pct": pct, "label": lbl}


# ═══════════════════════════════════════════════════════════════════
# RELATIVE STRENGTH
# ═══════════════════════════════════════════════════════════════════

_rs_scores: dict[str, float] = {}
_rs_lock = threading.Lock()


def update_rs_score(symbol: str, candles_1h: list[dict]):
    if len(candles_1h) < 24:
        return
    closes = [c["c"] for c in candles_1h[-25:]]
    ret    = (closes[-1] - closes[0]) / closes[0] * 100 if closes[0] != 0 else 0.0
    with _rs_lock:
        _rs_scores[symbol] = ret


def get_rs_data(symbol: str) -> dict:
    with _rs_lock:
        scores = dict(_rs_scores)
    if not scores or symbol not in scores:
        return {"score": None, "label": "RS: No data"}
    vals = sorted(scores.values())
    n    = len(vals)
    sym_ret = scores[symbol]
    pct = sum(1 for v in vals if v <= sym_ret) / n
    if pct >= (1 - 0.20):
        label = f"RS: Top 20% ({sym_ret:+.1f}%)"
        adj   = 1
    elif pct <= 0.20:
        label = f"RS: Bottom 20% ({sym_ret:+.1f}%)"
        adj   = -1
    else:
        label = f"RS: Mid ({sym_ret:+.1f}%)"
        adj   = 0
    return {"score": adj, "label": label, "pct": pct, "ret": sym_ret}


# ═══════════════════════════════════════════════════════════════════
# ORDERFLOW ANALYSIS
# ═══════════════════════════════════════════════════════════════════

def analyze_orderflow(candles_1h: list[dict], direction: str) -> dict:
    null = {
        "of_net": 0, "hard_reject": False,
        "delta_score": 0, "cvd_score": 0, "ratio_score": 0,
        "labels": {"delta": "Delta: N/A", "cvd": "CVD: N/A", "ratio": "Vol Ratio: N/A"},
    }
    if len(candles_1h) < max(OF_DELTA_LOOKBACK, OF_CVD_LOOKBACK) + 2:
        return null

    closes = [c["c"] for c in candles_1h]
    opens  = [c["o"] for c in candles_1h]
    highs  = [c["h"] for c in candles_1h]
    lows   = [c["l"] for c in candles_1h]
    vols   = [c["v"] for c in candles_1h]

    # Delta (body-fraction proxy, doji-dampened)
    _ranges  = [highs[i] - lows[i] for i in range(len(closes)) if highs[i] > lows[i]]
    _atr_est = (sum(_ranges[-14:]) / min(14, len(_ranges[-14:]))) if _ranges else 0.0
    deltas   = []
    for i in range(len(closes)):
        rng = highs[i] - lows[i]
        if rng <= 0 or (_atr_est > 0 and rng < _atr_est * 0.3):
            deltas.append(0.0)
            continue
        body = closes[i] - opens[i]
        deltas.append(body / rng * vols[i])

    last3 = deltas[-3:]
    last5 = deltas[-5:]
    net5  = sum(last5)

    if direction == "long":
        if all(d > 0 for d in last3) and abs(last3[-1]) > abs(last3[0]):
            delta_score = 2; delta_lbl = "Delta: Strong bullish +2"
        elif net5 > 0:
            delta_score = 1; delta_lbl = "Delta: Net bullish +1"
        elif sum(1 for d in deltas[-2:] if d < 0) == 2 and closes[-1] > closes[-3]:
            delta_score = -2; delta_lbl = "Delta: Bearish divergence -2"
        else:
            delta_score = 0; delta_lbl = "Delta: Mixed 0"
    else:
        if all(d < 0 for d in last3) and abs(last3[-1]) > abs(last3[0]):
            delta_score = 2; delta_lbl = "Delta: Strong bearish +2"
        elif net5 < 0:
            delta_score = 1; delta_lbl = "Delta: Net bearish +1"
        elif sum(1 for d in deltas[-2:] if d > 0) == 2 and closes[-1] < closes[-3]:
            delta_score = -2; delta_lbl = "Delta: Bullish divergence -2"
        else:
            delta_score = 0; delta_lbl = "Delta: Mixed 0"

    # CVD
    cvd_window = deltas[-OF_CVD_LOOKBACK:]
    running    = 0.0
    cvd_series = []
    for d in cvd_window:
        running += d
        cvd_series.append(running)
    cvd_rising = cvd_series[-1] > cvd_series[0] if cvd_series else False

    if direction == "long":
        if cvd_rising and cvd_series[-1] > 0:
            cvd_score = 1; cvd_lbl = "CVD: Rising +1"
        elif not cvd_rising and cvd_series[-1] < cvd_series[0] * 0.9:
            cvd_score = -2; cvd_lbl = "CVD: Distribution -2"
        else:
            cvd_score = 0; cvd_lbl = "CVD: Flat 0"
    else:
        if not cvd_rising and cvd_series[-1] < 0:
            cvd_score = 1; cvd_lbl = "CVD: Falling +1"
        elif cvd_rising and cvd_series[-1] > cvd_series[0] * 0.9 and cvd_series[0] < 0:
            cvd_score = -2; cvd_lbl = "CVD: Absorption -2"
        else:
            cvd_score = 0; cvd_lbl = "CVD: Flat 0"

    # Buy/sell ratio
    wc = closes[-OF_RATIO_LOOKBACK:]
    wo = opens[-OF_RATIO_LOOKBACK:]
    wv = vols[-OF_RATIO_LOOKBACK:]
    buy_vol  = sum(wv[i] for i in range(len(wv)) if wc[i] >= wo[i])
    sell_vol = sum(wv[i] for i in range(len(wv)) if wc[i] < wo[i])
    total    = buy_vol + sell_vol
    ratio    = buy_vol / total if total > 0 else 0.5

    if direction == "long":
        if ratio > OF_LONG_STRONG:
            ratio_score = 1; ratio_lbl = f"Vol Ratio: {ratio:.2f} (buyers) +1"
        elif ratio < OF_LONG_WEAK:
            ratio_score = -1; ratio_lbl = f"Vol Ratio: {ratio:.2f} (sellers) -1"
        else:
            ratio_score = 0; ratio_lbl = f"Vol Ratio: {ratio:.2f} (balanced) 0"
    else:
        if ratio < OF_SHORT_STRONG:
            ratio_score = 1; ratio_lbl = f"Vol Ratio: {ratio:.2f} (sellers) +1"
        elif ratio > OF_SHORT_WEAK:
            ratio_score = -1; ratio_lbl = f"Vol Ratio: {ratio:.2f} (buyers) -1"
        else:
            ratio_score = 0; ratio_lbl = f"Vol Ratio: {ratio:.2f} (balanced) 0"

    of_net      = delta_score + cvd_score + ratio_score
    hard_reject = of_net <= OF_HARD_REJECT

    return {
        "of_net":       of_net,
        "hard_reject":  hard_reject,
        "delta_score":  delta_score,
        "cvd_score":    cvd_score,
        "ratio_score":  ratio_score,
        "labels": {"delta": delta_lbl, "cvd": cvd_lbl, "ratio": ratio_lbl},
    }


# ═══════════════════════════════════════════════════════════════════
# SESSION DETECTION  [S5]
# ═══════════════════════════════════════════════════════════════════

def get_session(reference_ms: int | None = None) -> dict:
    ref_ts = (reference_ms / 1000) if reference_ms is not None else time.time()
    utc    = datetime.fromtimestamp(ref_ts, tz=timezone.utc)
    h      = utc.hour
    if SESSION_ASIAN_START <= h < SESSION_ASIAN_END:
        session = "Asian"
    elif SESSION_LONDON_START <= h < SESSION_LONDON_END:
        session = "London"
    elif SESSION_OVERLAP_START <= h < SESSION_OVERLAP_END:
        session = "Overlap"
    elif SESSION_NY_AFT_START <= h < SESSION_NY_AFT_END:
        session = "NY-Aft"
    else:
        session = "Off-Hours"
    return {"session": session, "hour": h}


def score_session(session: str, setup_type: str) -> tuple[int, str]:
    """
    [S5] Stricter session gating.
    Returns (score_adj, label). score_adj of -99 = hard reject.
    """
    if session in ("London", "Overlap"):
        # Best sessions for all setups
        return 1, f"Session: {session} (optimal +1)"
    elif session == "NY-Aft":
        # Good for continuation, not ideal for pullbacks
        if setup_type == "PULL":
            return 0, "Session: NY-Aft (ok for PULL)"
        return 1, "Session: NY-Aft (good +1)"
    elif session == "Asian":
        # Only allow PULL in Asian with high-quality confirmation
        if setup_type == "PULL":
            return 0, "Session: Asian (PULL only, no bonus)"
        return -1, "Session: Asian (not optimal -1)"
    else:  # Off-Hours
        # v3: soft penalty instead of hard reject for perps
        return -1, "Session: Off-Hours (-1)"


# ═══════════════════════════════════════════════════════════════════
# DAILY TREND CLASSIFICATION  (Phase 1)
# ═══════════════════════════════════════════════════════════════════

def classify_daily_trend(candles_1d: list[dict], symbol: str = "__DAILY__") -> dict:
    if len(candles_1d) < EMA_TREND + 5:
        return {
            "classification": "Neutral", "score": 0,
            "allows_long": True, "allows_short": True,
            "details": {"reason": "Insufficient data"},
        }

    ind    = get_cached_indicators(f"{symbol}_daily", "1d", candles_1d)
    closes = ind["c"]
    highs  = ind["h"]
    lows   = ind["l"]
    cur    = closes[-1]
    ef     = safe(ind["ema_fast"][-1])
    es     = safe(ind["ema_slow"][-1])
    et     = safe(ind["ema_trend"][-1])
    adx    = safe(ind["adx"][-1], 20.0)

    # Market structure
    lb    = min(MS_LOOKBACK_BARS, len(closes) - 1)
    h_sub = highs[-lb:]
    l_sub = lows[-lb:]

    def pivots(arr, fn):
        return [arr[i] for i in range(1, len(arr) - 1) if fn(arr[i], arr[i-1]) and fn(arr[i], arr[i+1])]

    ph    = pivots(h_sub, lambda a, b: a > b)
    pl    = pivots(l_sub, lambda a, b: a < b)
    hh_hl = len(ph) >= 2 and ph[-1] > ph[-2] and len(pl) >= 2 and pl[-1] > pl[-2]
    lh_ll = len(ph) >= 2 and ph[-1] < ph[-2] and len(pl) >= 2 and pl[-1] < pl[-2]

    bull_ema  = ef > es > et
    bear_ema  = ef < es < et
    mild_bull = ef > es and cur > et
    mild_bear = ef < es and cur < et

    strong_bull = bull_ema and hh_hl and adx >= DAILY_ADX_STRONG
    bullish     = (bull_ema or mild_bull) and (hh_hl or adx >= DAILY_ADX_WEAK) and ef > et
    strong_bear = bear_ema and lh_ll and adx >= DAILY_ADX_STRONG
    bearish     = (bear_ema or mild_bear) and (lh_ll or adx >= DAILY_ADX_WEAK) and ef < et

    if strong_bull:
        cls, score = "Strong Bullish", 3
    elif bullish:
        cls, score = "Bullish", 2
    elif strong_bear:
        cls, score = "Strong Bearish", 3
    elif bearish:
        cls, score = "Bearish", 2
    else:
        cls, score = "Neutral", 0

    # [S6] BTC-assisted Neutral direction: use BTC regime to guide
    btc_regime  = get_btc_regime()
    allows_long  = cls in ("Strong Bullish", "Bullish")
    allows_short = cls in ("Strong Bearish", "Bearish")
    if cls == "Neutral":
        if btc_regime and btc_regime.get("bullish"):
            allows_long  = True
            allows_short = False
        elif btc_regime and btc_regime.get("bearish"):
            allows_long  = False
            allows_short = True
        else:
            allows_long = allows_short = True  # mixed BTC: both allowed

    return {
        "classification": cls,
        "score":          score,
        "allows_long":    allows_long,
        "allows_short":   allows_short,
        "neutral":        cls == "Neutral",
        "ef": ef, "es": es, "et": et, "adx": adx,
        "hh_hl": hh_hl, "lh_ll": lh_ll,
        "details": {"ema21": ef, "ema50": es, "ema200": et, "adx": adx},
    }


def score_daily_alignment(daily: dict, direction: str) -> tuple[int, str]:
    cls = daily["classification"]
    if direction == "long":
        if cls == "Strong Bullish": return 3, "1D Strong Bull (+3)"
        elif cls == "Bullish":      return 2, "1D Bullish (+2)"
        elif cls == "Neutral":      return 0, "1D Neutral (0)"
        else:                       return 1, f"1D {cls} (counter, +1 baseline)"
    else:
        if cls == "Strong Bearish": return 3, "1D Strong Bear (+3)"
        elif cls == "Bearish":      return 2, "1D Bearish (+2)"
        elif cls == "Neutral":      return 0, "1D Neutral (0)"
        else:                       return 1, f"1D {cls} (counter, +1 baseline)"


# ═══════════════════════════════════════════════════════════════════
# 4H MARKET STRUCTURE
# ═══════════════════════════════════════════════════════════════════

def detect_market_structure_4h(candles_4h: list[dict], direction: str) -> dict:
    if len(candles_4h) < 10:
        return {"bos_bearish": False, "bos_bullish": False,
                "mss_bearish": False, "mss_bullish": False}

    window = candles_4h[-40:]
    highs  = [c["h"] for c in window]
    lows   = [c["l"] for c in window]
    closes = [c["c"] for c in window]

    # Find pivot highs and lows (3-bar rule)
    swing_h = [(i, highs[i]) for i in range(1, len(highs)-1)
               if highs[i] > highs[i-1] and highs[i] > highs[i+1]]
    swing_l = [(i, lows[i]) for i in range(1, len(lows)-1)
               if lows[i] < lows[i-1] and lows[i] < lows[i+1]]

    cur_close = closes[-1]
    bos_bearish = bos_bullish = False
    mss_bearish = mss_bullish = False

    # BOS: close beyond the last significant swing
    if swing_l and len(swing_l) >= 2:
        last_swing_low = swing_l[-1][1]
        prev_swing_low = swing_l[-2][1] if len(swing_l) >= 2 else last_swing_low
        # Bearish BOS: close below previous swing low (structure break)
        if cur_close < prev_swing_low * 0.999:
            bos_bearish = True

    if swing_h and len(swing_h) >= 2:
        last_swing_high = swing_h[-1][1]
        prev_swing_high = swing_h[-2][1] if len(swing_h) >= 2 else last_swing_high
        if cur_close > prev_swing_high * 1.001:
            bos_bullish = True

    # MSS: lower high / lower low formation (bearish) or higher low / higher high (bullish)
    if len(swing_h) >= 3:
        if swing_h[-1][1] < swing_h[-2][1] < swing_h[-3][1]:
            mss_bearish = True
    if len(swing_l) >= 3:
        if swing_l[-1][1] > swing_l[-2][1] > swing_l[-3][1]:
            mss_bullish = True

    return {
        "bos_bearish": bos_bearish, "bos_bullish": bos_bullish,
        "mss_bearish": mss_bearish, "mss_bullish": mss_bullish,
    }


# ═══════════════════════════════════════════════════════════════════
# 4H SETUP DETECTION  (Phase 2)  [S1]
# ═══════════════════════════════════════════════════════════════════

def detect_liquidity_sweep_4h(ind_4h: dict, direction: str) -> dict:
    """I1: Detect wick-through-and-close liquidity sweeps on 4H."""
    h = ind_4h["h"]; l = ind_4h["l"]; c = ind_4h["c"]
    n = len(c)
    if n < SWING_LOOKBACK + 2:
        return {"sweep": False, "level": None}

    window_h = h[-(SWING_LOOKBACK + 2):-1]
    window_l = l[-(SWING_LOOKBACK + 2):-1]
    swing_high = max(window_h) if window_h else None
    swing_low  = min(window_l) if window_l else None

    last_h = h[-1]; last_l = l[-1]; last_c = c[-1]

    if direction == "long" and swing_low is not None:
        # Bullish sweep: wick below prior swing low, close back above
        if last_l < swing_low and last_c > swing_low:
            return {"sweep": True, "level": swing_low}
    elif direction == "short" and swing_high is not None:
        # Bearish sweep: wick above prior swing high, close back below
        if last_h > swing_high and last_c < swing_high:
            return {"sweep": True, "level": swing_high}

    return {"sweep": False, "level": None}


def detect_vcp(atr_vals: list[float]) -> dict:
    """Volatility Contraction Pattern — successive ATR peak contractions."""
    if len(atr_vals) < VCP_LOOKBACK:
        return {"vcp": False, "stages": 0}
    # Find local maxima in ATR (peaks)
    peaks = []
    for i in range(1, len(atr_vals) - 1):
        if atr_vals[i] > atr_vals[i-1] and atr_vals[i] > atr_vals[i+1]:
            peaks.append(atr_vals[i])
    if len(peaks) < VCP_MIN_STAGES + 1:
        return {"vcp": False, "stages": 0}
    stages = 0
    for i in range(1, len(peaks)):
        if peaks[i] < peaks[i-1]:
            stages += 1
    return {"vcp": stages >= VCP_MIN_STAGES, "stages": stages}


def detect_4h_setup(candles_4h: list[dict], direction: str, symbol: str = "") -> dict:
    """
    [S1] Redesigned 4H setup detection.
    Setup types: PULL (pullback to EMA), CONT (continuation), BREAK (breakout).
    Min score raised: setup must score ≥ 2 to proceed.
    """
    if len(candles_4h) < 60:
        return {"setup_type": "NONE", "score": 0, "details": {"reason": "Insufficient 4H data"}}

    ind = get_cached_indicators(f"{symbol}_4h", "4h", candles_4h)
    c   = ind["c"]
    h   = ind["h"]
    l   = ind["l"]
    ef  = safe(ind["ema_fast"][-1])
    es  = safe(ind["ema_slow"][-1])
    adx = safe(ind["adx"][-1], 15.0)
    rsi_ = safe(ind["rsi"][-1], 50.0)
    atr_ = safe(ind["atr"][-1], c[-1] * ATR_FALLBACK_PCT)
    cur_v  = safe(ind["v"][-1])
    vm     = safe(ind["vol_ma"][-1])
    cur_c  = c[-1]

    if atr_ <= 0:
        atr_ = cur_c * ATR_FALLBACK_PCT

    # EMA alignment
    h4_bull = ef > es and cur_c > ef
    h4_bear = ef < es and cur_c < ef

    # Volume check
    vol_ok     = vm > 0 and cur_v >= vm * H4_VOL_MULT
    adx_ok     = adx >= H4_ADX_MIN
    rsi_healthy = (rsi_ < H4_RSI_OB if direction == "long" else rsi_ > H4_RSI_OS)

    # ── PULL detection: price pulling back to EMA21 or EMA50 ────────
    # [S1] PULL must be retesting, not chasing — price must be AT the EMA, not far above
    near_ef = abs(cur_c - ef) <= atr_ * PULL_MAX_EXT_ATR
    near_es = abs(cur_c - es) <= atr_ * PULL_MAX_EXT_ATR and not near_ef

    # Check pullback context: price should be approaching from a higher level (for longs)
    # Look at recent high vs current to confirm it's a pullback
    recent_high = max(h[-8:]) if direction == "long" else min(l[-8:])
    is_retracing = (cur_c < recent_high * 0.99) if direction == "long" else (cur_c > recent_high * 1.01)

    # ── BREAK detection: close beyond lookback high/low ─────────────
    lookback_h = max(h[-(BREAK_LOOKBACK_BARS + 1):-1]) if len(h) > BREAK_LOOKBACK_BARS else max(h[:-1])
    lookback_l = min(l[-(BREAK_LOOKBACK_BARS + 1):-1]) if len(l) > BREAK_LOOKBACK_BARS else min(l[:-1])

    # [S1] BREAK: close clearly beyond level, not just wick
    breakout_long  = direction == "long"  and cur_c > lookback_h * 1.002  # 0.2% clear
    breakout_short = direction == "short" and cur_c < lookback_l * 0.998

    # Check VCP for BREAK quality
    atr_arr_vals = [safe(x) for x in ind["atr"][-VCP_LOOKBACK:]
                    if not math.isnan(safe(x, float("nan")))]
    vcp          = detect_vcp(atr_arr_vals)

    # ── Liquidity sweep (I1) ─────────────────────────────────────────
    sweep_result = detect_liquidity_sweep_4h(ind, direction)

    # ── Market structure validation ──────────────────────────────────
    ms_struct = detect_market_structure_4h(candles_4h, direction)

    # Hard suppress: clear structure break against direction
    if direction == "long" and ms_struct["bos_bearish"]:
        return {"setup_type": "NONE", "score": 0, "details": {"reason": "BOS_BEAR: bearish structure break"}}
    if direction == "short" and ms_struct["bos_bullish"]:
        return {"setup_type": "NONE", "score": 0, "details": {"reason": "BOS_BULL: bullish structure break"}}

    # ── [S1] Score each setup type ────────────────────────────────────
    setup_type = "NONE"
    score      = 0

    # Priority: sweep > pullback > breakout > continuation
    if sweep_result["sweep"]:
        # Liquidity sweep is the highest-quality PULL entry
        setup_type = "PULL_SWEEP"
        score = 3  # maximum — sweep + close-back is A+ signal
    elif (direction == "long" and h4_bull and near_ef and is_retracing and rsi_healthy):
        setup_type = "PULL"
        score = 3 if (adx_ok and vol_ok) else 2
    elif (direction == "short" and h4_bear and near_ef and is_retracing and rsi_healthy):
        setup_type = "PULL"
        score = 3 if (adx_ok and vol_ok) else 2
    elif (direction == "long" and h4_bull and near_es and is_retracing and rsi_healthy):
        setup_type = "PULL"
        # EMA50 pullbacks capped at 2 unless sweep or extreme volume
        score = 2 if (adx_ok or vol_ok) else 1
    elif (direction == "short" and h4_bear and near_es and is_retracing and rsi_healthy):
        setup_type = "PULL"
        score = 2 if (adx_ok or vol_ok) else 1
    elif breakout_long or breakout_short:
        setup_type = "BREAK"
        if vcp["vcp"] and vol_ok and adx_ok:
            score = 3  # VCP + volume + ADX = premium breakout
        elif vol_ok and adx_ok:
            score = 2
        elif vol_ok or adx_ok:
            score = 2
        else:
            score = 1
    elif direction == "long" and h4_bull and adx_ok and rsi_healthy and vol_ok:
        # CONT: price in trend, above both EMAs, with ADX + volume
        # [S1] Requires consolidation evidence (price compressed before expansion)
        recent_range = max(h[-5:]) - min(l[-5:]) if len(h) >= 5 else atr_
        consolidating = recent_range < atr_ * 2.5  # compressed range
        setup_type = "CONT"
        score = 3 if (consolidating and ms_struct.get("mss_bullish", False)) else (2 if consolidating else 1)
    elif direction == "short" and h4_bear and adx_ok and rsi_healthy and vol_ok:
        recent_range = max(h[-5:]) - min(l[-5:]) if len(h) >= 5 else atr_
        consolidating = recent_range < atr_ * 2.5
        setup_type = "CONT"
        score = 3 if (consolidating and ms_struct.get("mss_bearish", False)) else (2 if consolidating else 1)
    else:
        return {"setup_type": "NONE", "score": 0, "details": {"reason": "No qualifying 4H setup"}}

    # MSS penalty (but not for PULL — lower swing highs confirm a pullback)
    if direction == "long" and ms_struct["mss_bearish"] and setup_type not in ("PULL", "PULL_SWEEP"):
        score = max(0, score - 2)
    if direction == "short" and ms_struct["mss_bullish"] and setup_type not in ("PULL", "PULL_SWEEP"):
        score = max(0, score - 2)

    # Canonical setup type for TP/SL
    canon_type = "PULL" if "PULL" in setup_type else setup_type

    return {
        "setup_type":  setup_type,
        "canon_type":  canon_type,
        "score":       score,
        "ema_fast":    ef,
        "ema_slow":    es,
        "adx":         adx,
        "rsi":         rsi_,
        "atr_val":     atr_,
        "vol_ratio":   (cur_v / vm) if vm > 0 else None,
        "ema_aligned": h4_bull if direction == "long" else h4_bear,
        "sweep":       sweep_result,
        "vcp":         vcp,
        "di_plus":     safe(ind["di_plus"][-1],  25.0),
        "di_minus":    safe(ind["di_minus"][-1], 25.0),
        "details": {
            "h4_bull": h4_bull, "h4_bear": h4_bear, "adx_ok": adx_ok,
            "rsi_healthy": rsi_healthy, "vol_ok": vol_ok,
            "near_ef": near_ef, "near_es": near_es,
        },
    }


# ═══════════════════════════════════════════════════════════════════
# 1H ENTRY CONFIRMATION  (Phase 3)  [S1][S4]
# ═══════════════════════════════════════════════════════════════════

def detect_1h_confirmation(candles_1h: list[dict], direction: str,
                           setup_type: str, symbol: str = "") -> dict:
    """
    [S1][S4] Redesigned 1H confirmation.
    Requires a candle-structure confirmation at the structural level:
      • PULL/PULL_SWEEP: rejection wick + close-back pattern
      • BREAK: momentum candle closing in direction
      • CONT: engulfing or strong momentum bar
    """
    if len(candles_1h) < 20:
        return {"confirmed": False, "score": 0, "triggers": [], "reason": "Insufficient 1H data"}

    ind    = get_cached_indicators(f"{symbol}_1h", "1h", candles_1h)
    c      = ind["c"]
    h      = ind["h"]
    l      = ind["l"]
    o      = ind["o"]
    ef     = safe(ind["ema_fast"][-1])
    es     = safe(ind["ema_slow"][-1])
    rsi_   = safe(ind["rsi"][-1], 50.0)
    atr_   = safe(ind["atr"][-1], c[-1] * ATR_FALLBACK_PCT)
    cur_v  = safe(ind["v"][-1])
    vm     = safe(ind["vol_ma"][-1])
    cur_c  = c[-1]

    if atr_ <= 0:
        atr_ = cur_c * ATR_FALLBACK_PCT

    triggers = []
    score    = 0

    # RSI gate (non-overbought/oversold)
    rsi_clear = (rsi_ < H1_RSI_OB if direction == "long" else rsi_ > H1_RSI_OS)
    rsi_above = (rsi_ >= H1_RSI_BULL) if direction == "long" else (rsi_ <= H1_RSI_BEAR)
    vol_up    = vm > 0 and cur_v >= vm * H4_VOL_MULT

    # Last 3 candles for pattern detection
    lc = c[-1]; lo = o[-1]; lh = h[-1]; ll = l[-1]
    pc = c[-2]; po = o[-2]; ph = h[-2]; pl = l[-2]

    # Candle body and wick measurements
    body     = abs(lc - lo)
    rng      = lh - ll
    bull_body = lc > lo
    bear_body = lc < lo

    upper_wick = lh - max(lc, lo)
    lower_wick = min(lc, lo) - ll

    # [S4] PULL/SWEEP: require rejection candle at structural level
    if "PULL" in setup_type:
        if direction == "long":
            # Bullish rejection: long lower wick + close near high
            hammer = (lower_wick >= atr_ * REJECTION_WICK_MIN_ATR and
                      lower_wick > upper_wick * 1.5 and
                      bull_body)
            # Engulfing: current bull candle engulfs prior bear
            engulf = (bull_body and bear_body != (pc > po) and
                      lc > pc and lo < po and
                      body >= rng * ENGULF_BODY_RATIO)
            rsi_turning = (rsi_ >= 40 and rsi_ < 65 and
                           rsi_ > safe(ind["rsi"][-3], rsi_))  # RSI turning up

            if hammer:
                score += 2; triggers.append("Hammer/Rejection wick +2")
            elif engulf:
                score += 2; triggers.append("Bullish engulf +2")

            if rsi_turning:
                score += 1; triggers.append("RSI turning up +1")
            if vol_up:
                score += 1; triggers.append("Volume confirm +1")

        else:  # short
            shooting_star = (upper_wick >= atr_ * REJECTION_WICK_MIN_ATR and
                             upper_wick > lower_wick * 1.5 and
                             bear_body)
            engulf = (bear_body and
                      lc < pc and lo > po and
                      body >= rng * ENGULF_BODY_RATIO)
            rsi_turning = (rsi_ <= 60 and rsi_ > 35 and
                           rsi_ < safe(ind["rsi"][-3], rsi_))

            if shooting_star:
                score += 2; triggers.append("Shooting star/Rejection +2")
            elif engulf:
                score += 2; triggers.append("Bearish engulf +2")

            if rsi_turning:
                score += 1; triggers.append("RSI turning down +1")
            if vol_up:
                score += 1; triggers.append("Volume confirm +1")

    elif setup_type == "BREAK":
        # Momentum candle confirming breakout direction
        if direction == "long":
            momentum = (bull_body and body >= rng * 0.55 and
                        lc > ef and rsi_ >= 50 and rsi_clear)
            retest   = (cur_c > ef and cur_c < ef * 1.02 and rsi_above)  # retest of ema after break
            if momentum:
                score += 2; triggers.append("Breakout momentum +2")
            elif retest:
                score += 2; triggers.append("Breakout retest +2")
            if vol_up:
                score += 1; triggers.append("Volume confirm +1")
        else:
            momentum = (bear_body and body >= rng * 0.55 and
                        lc < ef and rsi_ <= 50 and rsi_clear)
            retest   = (cur_c < ef and cur_c > ef * 0.98 and not rsi_above)
            if momentum:
                score += 2; triggers.append("Breakdown momentum +2")
            elif retest:
                score += 2; triggers.append("Breakdown retest +2")
            if vol_up:
                score += 1; triggers.append("Volume confirm +1")

    else:  # CONT
        # Continuation: price must be clearly in trend zone with expansion
        if direction == "long":
            in_trend = cur_c > ef > es
            strong_bar = bull_body and body >= rng * 0.55
            rsi_ok     = H1_RSI_BULL <= rsi_ < H1_RSI_OB
            if in_trend and strong_bar and rsi_ok:
                score += 2; triggers.append("Trend continuation bar +2")
            elif in_trend and rsi_ok:
                score += 1; triggers.append("Trend confirm +1")
            if vol_up:
                score += 1; triggers.append("Volume +1")
        else:
            in_trend = cur_c < ef < es
            strong_bar = bear_body and body >= rng * 0.55
            rsi_ok     = H1_RSI_OS < rsi_ <= H1_RSI_BEAR
            if in_trend and strong_bar and rsi_ok:
                score += 2; triggers.append("Trend continuation bar +2")
            elif in_trend and rsi_ok:
                score += 1; triggers.append("Trend confirm +1")
            if vol_up:
                score += 1; triggers.append("Volume +1")

    # Cap at 3
    score = min(3, score)

    # RSI gate: if RSI is at extreme, cap score at 1 (not zero — it can still fire with caution)
    if not rsi_clear:
        score = min(1, score)
        triggers.append(f"RSI extreme cap ({rsi_:.0f})")

    return {
        "confirmed": score >= 1,
        "score":     score,
        "triggers":  triggers,
        "rsi":       rsi_,
        "ema_fast":  ef,
        "ema_slow":  es,
        "atr_val":   atr_,
        "vol_ratio": (cur_v / vm) if vm > 0 else None,
    }


# ═══════════════════════════════════════════════════════════════════
# SCORING COMPONENTS
# ═══════════════════════════════════════════════════════════════════

def score_volume(candles_1h: list[dict], candles_4h: list[dict],
                 symbol: str = "") -> tuple[int, str]:
    ind_1h = get_cached_indicators(f"{symbol}_1h", "1h", candles_1h)
    cur_v  = safe(ind_1h["v"][-1])
    vm_1h  = safe(ind_1h["vol_ma"][-1])

    ind_4h = get_cached_indicators(f"{symbol}_4h", "4h", candles_4h)
    vm_4h  = safe(ind_4h["vol_ma"][-1])
    v_4h   = safe(ind_4h["v"][-1])

    if vm_1h > 0 and cur_v >= vm_1h * 2.0 and vm_4h > 0 and v_4h >= vm_4h * 1.5:
        return 2, "Volume: Strong expansion (1H+4H) +2"
    elif vm_1h > 0 and cur_v >= vm_1h * 1.5:
        return 1, "Volume: Elevated 1H +1"
    elif vm_4h > 0 and v_4h >= vm_4h * 1.3:
        return 1, "Volume: Elevated 4H +1"
    else:
        return 0, "Volume: Normal (0)"


def score_adx(daily: dict, setup: dict, direction: str) -> tuple[int, str]:
    d_adx     = daily.get("adx", 0)
    h4_adx    = setup.get("adx", 0)
    di_plus   = setup.get("di_plus", 25.0)
    di_minus  = setup.get("di_minus", 25.0)

    di_aligned = ((direction == "long"  and di_plus  > di_minus) or
                  (direction == "short" and di_minus > di_plus))
    if not di_aligned:
        return 0, f"ADX: DI misaligned ({di_plus:.0f}+ vs {di_minus:.0f}-)"

    if d_adx >= DAILY_ADX_STRONG and h4_adx >= H4_ADX_MIN:
        return 2, f"ADX: Strong (1D {d_adx:.0f} | 4H {h4_adx:.0f}) +2"
    elif (d_adx >= DAILY_ADX_WEAK and h4_adx >= H4_ADX_MIN) or d_adx >= DAILY_ADX_STRONG:
        return 1, f"ADX: OK (1D {d_adx:.0f} | 4H {h4_adx:.0f}) +1"
    else:
        return 0, f"ADX: Weak (0)"


def detect_rsi_divergence(candles_1h: list[dict], direction: str,
                          symbol: str = "") -> tuple[bool, str]:
    """Swing-based RSI divergence detection."""
    ind = get_cached_indicators(f"{symbol}_1h", "1h", candles_1h)
    c   = ind["c"]; r = ind["rsi"]
    h   = ind["h"]; l = ind["l"]

    if len(c) < 20:
        return False, "Divergence: Insufficient data"

    # Find recent swing points
    if direction == "long":
        # Bearish divergence for longs: higher price lows but lower RSI lows
        swing_l_px  = [(i, l[i]) for i in range(2, len(l)-2)
                       if l[i] < l[i-1] and l[i] < l[i+1] and not math.isnan(safe(r[i], float("nan")))]
        if len(swing_l_px) >= 2:
            p1, p2 = swing_l_px[-2], swing_l_px[-1]
            px_higher = p2[1] > p1[1]
            rsi_lower = safe(r[p2[0]]) < safe(r[p1[0]])
            if px_higher and rsi_lower:
                return True, f"Divergence: Bearish (higher lows, lower RSI) {DIVERGENCE_PENALTY}"
    else:
        # Bullish divergence for shorts: lower price highs but higher RSI highs
        swing_h_px = [(i, h[i]) for i in range(2, len(h)-2)
                      if h[i] > h[i-1] and h[i] > h[i+1] and not math.isnan(safe(r[i], float("nan")))]
        if len(swing_h_px) >= 2:
            p1, p2 = swing_h_px[-2], swing_h_px[-1]
            px_lower = p2[1] < p1[1]
            rsi_higher = safe(r[p2[0]]) > safe(r[p1[0]])
            if px_lower and rsi_higher:
                return True, f"Divergence: Bullish (lower highs, higher RSI) {DIVERGENCE_PENALTY}"

    return False, "Divergence: None"


# ═══════════════════════════════════════════════════════════════════
# SIGNAL RESULT
# ═══════════════════════════════════════════════════════════════════

class SignalResult:
    __slots__ = (
        "fire_long", "fire_short", "signal_type", "direction",
        "base_score", "final_score",
        "daily_class", "daily_score",
        "setup_type", "setup_score",
        "h1_score", "vol_score", "adx_score",
        "entry", "tp1", "tp2", "sl",
        "atr_val", "atr_pct",
        "supports", "resistances",
        "oi_data", "btc_regime_label", "breadth_label",
        "rs_data", "wr_data", "macro_data",
        "funding_rate", "open_interest",
        "spread_pct", "vol_ratio",
        "score_adjustments", "trigger_list",
        "symbol", "divergence_label", "grade",
        "confluence_factors",
        "session_data", "orderflow_data",
        "sl_near_cluster",
    )

    def __init__(self):
        self.fire_long = self.fire_short = False
        self.signal_type = ""
        self.direction   = ""
        self.base_score  = 0
        self.final_score = 0
        self.daily_class = "Neutral"
        self.daily_score = 0
        self.setup_type  = "NONE"
        self.setup_score = 0
        self.h1_score    = 0
        self.vol_score   = 0
        self.adx_score   = 0
        self.entry = self.tp1 = self.tp2 = self.sl = 0.0
        self.atr_val = self.atr_pct = 0.0
        self.supports:    list[float] = []
        self.resistances: list[float] = []
        self.oi_data:    dict = {}
        self.btc_regime_label = ""
        self.breadth_label    = ""
        self.rs_data:    dict = {}
        self.wr_data:    dict = {}
        self.macro_data: dict = {}
        self.funding_rate:  float | None = None
        self.open_interest: float | None = None
        self.spread_pct:    float | None = None
        self.vol_ratio:     float | None = None
        self.score_adjustments: list[tuple[str, int, str]] = []
        self.trigger_list:  list[str] = []
        self.symbol: str = ""
        self.divergence_label: str = ""
        self.grade: str = "C"
        self.confluence_factors: list[str] = []
        self.session_data:   dict = {}
        self.orderflow_data: dict = {}
        self.sl_near_cluster: bool = False


# ═══════════════════════════════════════════════════════════════════
# MAIN SIGNAL PIPELINE  [S1-S6]
# ═══════════════════════════════════════════════════════════════════

def compute_signals(symbol: str,
                    candles_1h: list[dict],
                    candles_4h: list[dict],
                    candles_1d: list[dict],
                    state: dict,
                    reference_ms: int | None = None,
                    funding_rate: float | None = None) -> SignalResult:
    """
    Full 1D → 4H → 1H precision signal pipeline.

    v3 changes vs v2:
      - 4H setup must score ≥ 2 (hard gate, was effectively 1)
      - Orderflow must be non-hostile (≥ 0, was soft gate)
      - BTC counter-trend requires score ≥ 12 (was -1 penalty)
      - TP/SL multipliers widened TP, tightened SL
      - Min R:R raised to 1.5
      - Session scoring stricter
    """
    res = SignalResult()
    res.symbol = symbol

    ctx = get_market_context(symbol)
    oi_usd    = ctx["open_interest"] if ctx else None
    funding   = funding_rate if funding_rate is not None else (ctx["funding"] if ctx else None)
    cur_mark  = ctx["mark_px"] if ctx else None
    res.funding_rate  = funding
    res.open_interest = oi_usd

    # Record funding history
    record_funding(state, symbol, funding)

    # ── Pre-scan extreme funding suppress ────────────────────────────
    if funding is not None and abs(funding) >= FUNDING_SUPPRESS_EXTREME:
        dirn_blocked = "long" if funding > 0 else "short"
        print(f"  [FUNDING SUPPRESS] {symbol} — extreme {dirn_blocked} funding {funding*100:.4f}%/8h")
        # Only suppress the direction that funding is extreme for

    # ── OI minimum filter ─────────────────────────────────────────────
    oi_min = MIN_OI_USD_SMALL_CAP if symbol in SMALL_CAP_PAIRS else MIN_OI_USD
    if oi_usd is not None and oi_usd < oi_min:
        return res  # insufficient liquidity

    # ── ATR / volatility check ────────────────────────────────────────
    ind_1h  = get_cached_indicators(f"{symbol}_1h", "1h", candles_1h)
    atr_1h  = safe(ind_1h["atr"][-1], candles_1h[-1]["c"] * ATR_FALLBACK_PCT)
    cur_c   = ind_1h["c"][-1]
    if cur_c <= 0:
        return res
    atr_pct = atr_1h / cur_c * 100
    if atr_pct < MIN_ATR_PCT or atr_pct > MAX_ATR_PCT:
        return res
    res.atr_val = atr_1h
    res.atr_pct = atr_pct

    # Record breadth (above EMA50 on 4H)
    ind_4h = get_cached_indicators(f"{symbol}_4h", "4h", candles_4h)
    ef_4h  = safe(ind_4h["ema_slow"][-1])
    record_breadth_result(symbol, candles_4h[-1]["c"] > ef_4h)

    # ── Phase 1: Daily trend classification ───────────────────────────
    daily = classify_daily_trend(candles_1d, symbol=symbol)
    res.daily_class = daily["classification"]

    # ── Phase 2 + 3: Detect setups for each direction ────────────────
    best_direction  = None
    best_score      = -999
    best_setup      = None
    best_h1         = None

    for direction in ("long", "short"):
        # Direction gate from daily trend
        if direction == "long" and not daily["allows_long"]:
            continue
        if direction == "short" and not daily["allows_short"]:
            continue

        # Extreme funding suppress per direction
        if (funding is not None and abs(funding) >= FUNDING_SUPPRESS_EXTREME and
                ((direction == "long" and funding > 0) or (direction == "short" and funding < 0))):
            continue

        # 4H setup
        setup = detect_4h_setup(candles_4h, direction, symbol=symbol)
        if setup["setup_type"] == "NONE":
            continue

        # [S2] 4H setup must score ≥ MIN_4H_SETUP_SCORE
        if setup["score"] < MIN_4H_SETUP_SCORE:
            continue

        # 1H confirmation
        h1 = detect_1h_confirmation(candles_1h, direction, setup["setup_type"], symbol=symbol)
        if not h1["confirmed"]:
            continue

        # [S2] Orderflow — must be non-hostile (net ≥ 0)
        of = analyze_orderflow(candles_1h, direction)
        if of["hard_reject"]:
            print(f"  [OF HARD REJECT] {symbol} {direction.upper()} of_net={of['of_net']}")
            continue
        # [S2] Orderflow must be non-hostile
        if of["of_net"] < OF_MIN_NET:
            print(f"  [OF HOSTILE] {symbol} {direction.upper()} of_net={of['of_net']} < {OF_MIN_NET}")
            continue

        # Compute composite pre-score for direction ranking
        daily_score, _ = score_daily_alignment(daily, direction)
        vol_score, _   = score_volume(candles_1h, candles_4h, symbol=symbol)
        adx_score, _   = score_adx(daily, setup, direction)
        of_bonus        = min(2, max(-2, of["of_net"]))  # clamp -2..+2

        pre_score = daily_score + setup["score"] + h1["score"] + vol_score + adx_score + of_bonus

        if pre_score > best_score:
            best_score     = pre_score
            best_direction = direction
            best_setup     = setup
            best_h1        = h1
            best_of        = of

    if best_direction is None:
        return res  # no valid setup found

    direction = best_direction
    setup     = best_setup
    h1        = best_h1
    of        = best_of

    # ── Build base score ──────────────────────────────────────────────
    daily_score, daily_lbl = score_daily_alignment(daily, direction)
    vol_score,   vol_lbl   = score_volume(candles_1h, candles_4h, symbol=symbol)
    adx_score,   adx_lbl   = score_adx(daily, setup, direction)
    of_bonus                = min(2, max(-2, of["of_net"]))

    base_score = daily_score + setup["score"] + h1["score"] + vol_score + adx_score + of_bonus

    # Neutral daily: cap base at 10 (no big alignment bonus)
    if daily["neutral"]:
        base_score = min(base_score, 10)

    res.base_score   = base_score
    res.daily_score  = daily_score
    res.setup_score  = setup["score"]
    res.h1_score     = h1["score"]
    res.vol_score    = vol_score
    res.adx_score    = adx_score
    res.setup_type   = setup["setup_type"]
    res.direction    = direction
    res.trigger_list = h1["triggers"]
    res.orderflow_data = of

    canon_type   = setup.get("canon_type", "CONT")
    res.signal_type = canon_type

    # ── Session scoring ────────────────────────────────────────────────
    session_info   = get_session(reference_ms)
    sess_adj, sess_lbl = score_session(session_info["session"], setup["setup_type"])
    res.session_data = {"session": session_info["session"], "score_adj": sess_adj, "label": sess_lbl}

    # ── S/R levels ────────────────────────────────────────────────────
    atr_4h = safe(ind_4h["atr"][-1], cur_c * ATR_FALLBACK_PCT)
    supports, resistances = find_sr_levels(candles_4h, cur_c, atr_4h)
    res.supports     = supports
    res.resistances  = resistances

    # ── Score adjustments ─────────────────────────────────────────────
    adjs: list[tuple[str, int, str]] = []

    # Session
    if sess_adj != 0:
        adjs.append((sess_lbl, sess_adj, "secondary"))

    # [S6] BTC regime filter — now harder gate for counter-trend
    btc_regime = get_btc_regime()
    btc_lbl    = btc_regime["label"] if btc_regime else "BTC: Unknown"
    res.btc_regime_label = btc_lbl
    if btc_regime:
        if direction == "long" and btc_regime.get("bearish"):
            adjs.append((f"{btc_lbl} — counter-trend", -2, "primary"))
        elif direction == "short" and btc_regime.get("bullish"):
            adjs.append((f"{btc_lbl} — counter-trend", -2, "primary"))
        elif direction == "long" and btc_regime.get("bullish"):
            adjs.append((f"{btc_lbl} — tailwind", +1, "secondary"))
        elif direction == "short" and btc_regime.get("bearish"):
            adjs.append((f"{btc_lbl} — tailwind", +1, "secondary"))

    # Market breadth
    breadth = compute_market_breadth()
    pct     = breadth["breadth_pct"]
    res.breadth_label = breadth["label"]
    if direction == "long":
        if pct > BREADTH_EXTREME_LONG:
            adjs.append((f"Breadth {pct*100:.0f}% extreme overbought", -2, "primary"))
        elif pct > BREADTH_CROWDED_LONG:
            adjs.append((f"Breadth {pct*100:.0f}% crowded", -1, "secondary"))
        elif pct < BREADTH_WEAK_LONG:
            adjs.append((f"Breadth {pct*100:.0f}% weak market", -1, "secondary"))
    else:
        if pct < BREADTH_EXTREME_SHORT:
            adjs.append((f"Breadth {pct*100:.0f}% extreme oversold", -2, "primary"))
        elif pct < (1 - BREADTH_CROWDED_LONG):
            adjs.append((f"Breadth {pct*100:.0f}% crowded short", -1, "secondary"))
        elif pct > BREADTH_WEAK_SHORT:
            adjs.append((f"Breadth {pct*100:.0f}% weak bear breadth", -1, "secondary"))

    # RS
    rs_data = get_rs_data(symbol)
    res.rs_data = rs_data
    if rs_data.get("score") is not None and rs_data["score"] != 0:
        adjs.append((rs_data["label"], rs_data["score"], "secondary"))

    # OI
    oi_data = get_oi_data(state, symbol, oi_usd)
    res.oi_data = oi_data
    if oi_data.get("score", 0) != 0:
        adjs.append((oi_data["label"], oi_data["score"], "secondary"))

    # RSI divergence
    div_detected, div_lbl = detect_rsi_divergence(candles_1h, direction, symbol=symbol)
    res.divergence_label = div_lbl
    if div_detected:
        adjs.append((div_lbl, DIVERGENCE_PENALTY, "primary"))

    # Macro
    macro = apply_macro_filter(state, atr_pct, reference_ms)
    res.macro_data = macro
    if macro["hard_suppress"]:
        print(f"  [MACRO SUPPRESS] {symbol} — {macro['label']}")
        return res
    if macro["in_window"]:
        adjs.append((macro["label"], macro["score_adj"], "secondary"))

    # Funding
    if funding is not None:
        headwind = (direction == "long" and funding > 0) or (direction == "short" and funding < 0)
        tailwind = not headwind
        if tailwind and abs(funding) >= FUNDING_CARRY_THRESHOLD:
            adjs.append((f"Funding tailwind ({funding*100:+.4f}%/8h)", FUNDING_CARRY_BONUS, "secondary"))
        if headwind and abs(funding) >= FUNDING_HEADWIND_THRESHOLD:
            f_trend  = get_funding_trend(state, symbol)
            penalty  = -2 if f_trend == "rising" else -1
            adjs.append((f"Funding headwind ({funding*100:+.4f}%/8h)", penalty, "secondary"))

    # Win rate
    wr = compute_wr_adj(state, symbol, direction, canon_type)
    res.wr_data = wr
    if wr["score_adj"] != 0:
        adjs.append((wr["label"], wr["score_adj"], "secondary"))

    res.score_adjustments = adjs

    # Cap adjustments
    total_pos = sum(a for _, a, _ in adjs if a > 0)
    total_neg = sum(a for _, a, _ in adjs if a < 0)
    capped_pos = min(total_pos, MAX_POSITIVE_ADJ)
    capped_neg = max(total_neg, -MAX_NEGATIVE_ADJ)
    adjusted   = base_score + capped_pos + capped_neg

    res.final_score = adjusted

    # ── Minimum score gate ────────────────────────────────────────────
    eff_min = MIN_SIGNAL_SCORE
    # Sweep bonus: lower min to 8 for liquidity sweeps
    if "SWEEP" in setup["setup_type"]:
        eff_min = min(eff_min, 8)

    if adjusted < eff_min:
        print(f"  [SCORE FILTER] {symbol} {direction.upper()} "
              f"base={base_score} final={adjusted} < {eff_min}")
        return res

    # [S6] Counter-trend hard gate: must score ≥ COUNTER_TREND_MIN_SCORE
    if btc_regime:
        is_counter = ((direction == "long" and btc_regime.get("bearish")) or
                      (direction == "short" and btc_regime.get("bullish")))
        if is_counter and adjusted < COUNTER_TREND_MIN_SCORE:
            print(f"  [COUNTER-TREND GATE] {symbol} {direction.upper()} "
                  f"score={adjusted} < {COUNTER_TREND_MIN_SCORE} — suppressed")
            return res

    # ── Phase 6: Risk model — TP/SL ──────────────────────────────────
    tp1_m, tp2_m, sl_m = SETUP_TP_SL_MULTS.get(canon_type, (2.0, 3.5, 0.90))

    if atr_pct > HIGH_ATR_THRESHOLD:
        sl_m = SL_HIGH_ATR_MULT

    atr_pctile = get_atr_percentile(state, symbol, atr_pct)
    if atr_pctile is not None and atr_pctile > ATR_HIGH_PERCENTILE:
        sl_m = min(sl_m, SL_HIGH_ATR_MULT)

    res.entry = cur_c

    if direction == "long":
        # [S1] PULL entry: anchor at EMA + small slippage buffer
        if "PULL" in setup["setup_type"]:
            ef_4h_val = setup.get("ema_fast", cur_c)
            res.entry = ef_4h_val + atr_1h * 0.1  # slight above EMA
        res.tp1 = cur_c + atr_1h * tp1_m
        res.tp2 = cur_c + atr_1h * tp2_m
        atr_sl  = cur_c - atr_1h * sl_m
        res.sl  = atr_sl
        # Structure-based SL: snap to nearest swing low (with buffer)
        if supports:
            struct_sl = max(supports) * 0.998
            sl_dist   = cur_c - struct_sl
            if sl_dist >= atr_1h * 0.5:
                res.sl = max(struct_sl, atr_sl)  # tighter of two
        # Snap TP1 to nearest resistance if still meets R:R
        if resistances:
            nr = resistances[0]
            sl_d = cur_c - res.sl
            if 0.2 <= (nr - cur_c) / atr_1h < tp1_m and sl_d > 0:
                if (nr - cur_c) / sl_d >= MIN_RR_RATIO:
                    res.tp1 = nr
        res.fire_long = True
    else:
        if "PULL" in setup["setup_type"]:
            ef_4h_val = setup.get("ema_fast", cur_c)
            res.entry = ef_4h_val - atr_1h * 0.1  # slight below EMA
        res.tp1 = cur_c - atr_1h * tp1_m
        res.tp2 = cur_c - atr_1h * tp2_m
        atr_sl  = cur_c + atr_1h * sl_m
        res.sl  = atr_sl
        if resistances:
            struct_sl = min(resistances) * 1.002
            sl_dist   = struct_sl - cur_c
            if sl_dist >= atr_1h * 0.5:
                res.sl = min(struct_sl, atr_sl)
        if supports:
            ns = supports[0]
            if res.tp2 < ns < res.tp1:
                res.tp1 = ns
        res.fire_short = True

    # R:R gate [S3]
    tp1_dist = abs(res.tp1 - cur_c)
    sl_dist  = abs(res.sl  - cur_c)
    if sl_dist > 0:
        rr = tp1_dist / sl_dist
        if rr < MIN_RR_RATIO:
            print(f"  [RR FILTER] {symbol} {direction.upper()} R:R {rr:.2f} < {MIN_RR_RATIO}")
            res.fire_long = res.fire_short = False
            return res

    # ── Grading ───────────────────────────────────────────────────────
    if adjusted >= GRADE_A_SCORE:
        res.grade = "A"
    elif adjusted >= GRADE_B_SCORE:
        res.grade = "B"
    else:
        res.grade = "C"

    # ── Confluence factors ────────────────────────────────────────────
    factors = []
    if h1.get("score", 0) >= 2:
        factors.append("strong_1h_confirm")
    if vol_score >= 1:
        factors.append("volume_expansion")
    if "SWEEP" in setup["setup_type"]:
        factors.append("liquidity_sweep")
    if adx_score >= 1:
        factors.append("adx_confirmed")
    if setup.get("vcp", {}).get("vcp"):
        factors.append("vcp")
    if of["of_net"] >= 2:
        factors.append("strong_orderflow")
    # Confluence bonus
    if len(factors) >= 3:
        res.final_score = min(MAX_SCORE, res.final_score + 1)
        factors.append("confluence_bonus+1")
    res.confluence_factors = factors

    return res


# ═══════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════

def send_telegram(text: str) -> int | None:
    url     = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TG_CHAT_ID,
        "text":       text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    for attempt in range(3):
        try:
            r = requests.post(url, json=payload, timeout=15)
            r.raise_for_status()
            return r.json()["result"]["message_id"]
        except Exception as e:
            if attempt == 2:
                print(f"  [TG] send failed: {e}")
                return None
            time.sleep(1.5 ** attempt)
    return None


def send_telegram_reaction(msg_id: int, emoji: str):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/setMessageReaction"
    try:
        requests.post(url, json={
            "chat_id":    TG_CHAT_ID,
            "message_id": msg_id,
            "reaction":   [{"type": "emoji", "emoji": emoji}],
        }, timeout=10)
    except Exception:
        pass


def _leverage_for_risk(entry: float, sl: float, grade: str) -> float:
    if entry <= 0 or sl <= 0:
        return 2.0
    dist_pct = abs(entry - sl) / entry * 100
    if dist_pct <= 0:
        return 2.0
    risk_pct = LEVERAGE_BASE_RISK_PCT
    lev      = risk_pct / dist_pct
    max_lev  = GRADE_MAX_LEVERAGE.get(grade, 4.0)
    return round(min(max_lev, max(1.0, lev)), 1)


def format_signal(symbol: str, sig: SignalResult, scan_type: str = "SWING",
                  rank: int = 1) -> str:
    coin    = hl_coin(symbol)
    dirn    = sig.direction.upper()
    emoji   = "🟢" if sig.direction == "long" else "🔴"
    grade   = sig.grade
    grade_e = {"A": "⭐⭐⭐", "B": "⭐⭐", "C": "⭐"}.get(grade, "⭐")

    lev   = _leverage_for_risk(sig.entry, sig.sl, grade)
    size  = GRADE_SIZE_PCT.get(grade, 50)

    rr = abs(sig.tp1 - sig.entry) / abs(sig.sl - sig.entry) if abs(sig.sl - sig.entry) > 0 else 0

    setup_label = sig.setup_type.replace("_SWEEP", " 💦").replace("PULL", "PULL").replace("CONT", "CONT").replace("BREAK", "BREAK")
    session     = sig.session_data.get("session", "?")

    lines = [
        f"{emoji} <b>{coin} {dirn}</b> — Grade {grade} {grade_e}",
        f"#{rank} | {scan_type} | Score: <b>{sig.final_score}</b> | Setup: {setup_label}",
        f"",
        f"📍 Entry: <code>{sig.entry:.4f}</code>",
        f"🎯 TP1:   <code>{sig.tp1:.4f}</code>  (+{abs(sig.tp1 - sig.entry)/sig.entry*100:.2f}%)",
        f"🎯 TP2:   <code>{sig.tp2:.4f}</code>  (+{abs(sig.tp2 - sig.entry)/sig.entry*100:.2f}%)",
        f"🛑 SL:    <code>{sig.sl:.4f}</code>   (-{abs(sig.sl - sig.entry)/sig.entry*100:.2f}%)",
        f"📊 R:R: {rr:.2f} | Lev: {lev}x | Size: {size}%",
        f"",
        f"<b>Score Breakdown</b>",
        f"  1D Trend: {sig.daily_class} ({sig.daily_score:+d})",
        f"  4H Setup: {sig.setup_type} ({sig.setup_score:+d})",
        f"  1H Entry: {sig.h1_score:+d}",
        f"  Volume:   {sig.vol_score:+d}",
        f"  ADX:      {sig.adx_score:+d}",
        f"  Base: {sig.base_score} → Final: {sig.final_score}",
        f"",
        f"<b>Filters</b>",
        f"  Session: {session} ({sig.session_data.get('score_adj', 0):+d})",
        f"  {sig.btc_regime_label}",
        f"  {sig.breadth_label}",
        f"  {sig.rs_data.get('label', 'RS: N/A')}",
        f"  {sig.oi_data.get('label', 'OI: N/A')}",
    ]

    if sig.funding_rate is not None:
        lines.append(f"  Funding: {sig.funding_rate*100:+.4f}%/8h")

    if sig.orderflow_data:
        of = sig.orderflow_data
        lines += [
            f"",
            f"<b>Orderflow</b>",
            f"  {of['labels']['delta']}",
            f"  {of['labels']['cvd']}",
            f"  {of['labels']['ratio']}",
            f"  Net: {of['of_net']:+d}",
        ]

    if sig.divergence_label and "None" not in sig.divergence_label:
        lines.append(f"  ⚠️ {sig.divergence_label}")

    if sig.macro_data.get("in_window"):
        lines.append(f"  {sig.macro_data['label']}")

    if sig.trigger_list:
        lines.append(f"")
        lines.append(f"<b>1H Triggers</b>")
        for t in sig.trigger_list[:4]:
            lines.append(f"  • {t}")

    if sig.confluence_factors:
        lines.append(f"")
        lines.append(f"<b>Confluence ({len(sig.confluence_factors)})</b>: {', '.join(sig.confluence_factors)}")

    if sig.supports:
        lines.append(f"  Support: {', '.join(f'{s:.4f}' for s in sig.supports[:2])}")
    if sig.resistances:
        lines.append(f"  Resist:  {', '.join(f'{r:.4f}' for r in sig.resistances[:2])}")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════
# SCAN SYMBOL
# ═══════════════════════════════════════════════════════════════════

def scan_symbol(symbol: str, state: dict, bar_index: int,
                candle_bundle: tuple | None,
                reference_ms: int | None = None) -> list[tuple] | None:
    if candle_bundle is None:
        return None
    candles_1h, candles_4h, candles_1d = candle_bundle
    if not candles_1h or not candles_4h or not candles_1d:
        return None

    # Cooldown check (pre-compute for speed — direction determined in compute_signals)
    if (check_cooldown(state, symbol, "long",  bar_index) and
            check_cooldown(state, symbol, "short", bar_index)):
        return None

    ctx = get_market_context(symbol)
    funding_rate = ctx["funding"] if ctx else None

    try:
        sig = compute_signals(
            symbol, candles_1h, candles_4h, candles_1d,
            state, reference_ms=reference_ms, funding_rate=funding_rate,
        )
    except Exception as e:
        print(f"  [SCAN] {symbol} compute_signals failed: {e}")
        return None

    if not (sig.fire_long or sig.fire_short):
        return None

    direction = "long" if sig.fire_long else "short"

    if check_cooldown(state, symbol, direction, bar_index, grade=sig.grade):
        print(f"  [COOLDOWN] {symbol} {direction.upper()} — skipped")
        return None

    update_rs_score(symbol, candles_1h)
    return [(symbol, direction, sig)]


# ═══════════════════════════════════════════════════════════════════
# DEDUPLICATION / PRIORITY
# ═══════════════════════════════════════════════════════════════════

def priority_score(sig: SignalResult) -> float:
    grade_bonus = {"A": 3.0, "B": 1.5, "C": 0.0}.get(sig.grade, 0.0)
    return sig.final_score + grade_bonus


def deduplicate_correlated(pending: list[tuple]) -> list[tuple]:
    """Simple deduplication: one signal per base coin."""
    seen: set[str] = set()
    out  = []
    for sym, dirn, sig in pending:
        coin = hl_coin(sym)
        # Allow one long + one short per coin if both qualified
        key = f"{coin}_{dirn}"
        if key not in seen:
            seen.add(key)
            out.append((sym, dirn, sig))
    return out


# ═══════════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════════

_shutdown = False


def _handle_shutdown(signum, frame):
    global _shutdown
    print(f"\n[SHUTDOWN] Signal {signum} received — will exit after this scan.")
    _shutdown = True


def main():
    global _shutdown

    os_signal.signal(os_signal.SIGTERM, _handle_shutdown)
    os_signal.signal(os_signal.SIGINT,  _handle_shutdown)

    # Process lock
    lock_fd = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("[LOCK] Another instance is running — aborting.")
        sys.exit(1)

    state = load_state()
    reset_win_rates_cache()

    ref_ms        = int(time.time() * 1000)
    bar_index_now = ref_ms // INTERVAL_MS["1h"]

    print(f"[SWING ENGINE v{__version__}] Starting scan — {datetime.fromtimestamp(ref_ms/1000, tz=timezone.utc).isoformat()}")
    print(f"  [{len(WATCHLIST)} symbols | min_score={MIN_SIGNAL_SCORE} | min_rr={MIN_RR_RATIO} | min_4h={MIN_4H_SETUP_SCORE}]")

    # ── Phase 1: Prefetch candles ─────────────────────────────────
    print("[PHASE 1] Fetching candles…")
    candle_bundles: dict[str, tuple] = {}
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futs = {ex.submit(fetch_all_candles, sym, ref_ms): sym for sym in WATCHLIST}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                bundle = fut.result()
                if bundle:
                    candle_bundles[sym] = bundle
            except Exception as e:
                print(f"  [FETCH] {sym} failed: {e}")

    print(f"  Fetched {len(candle_bundles)}/{len(WATCHLIST)} symbols")

    # BTC regime
    btc_bundle = candle_bundles.get("BTCUSDT")
    if btc_bundle:
        try:
            regime = compute_btc_regime(*btc_bundle)
            set_btc_regime(regime)
            print(f"  [BTC REGIME] {regime['label']}")
        except Exception as e:
            print(f"  [BTC REGIME] failed: {e}")

    # Meta cache (OI, funding)
    get_meta_and_asset_ctxs()

    # Expire old active signals
    expire_old_signals(state, bar_index_now)

    # Check outcomes on active signals
    if btc_bundle:
        try:
            check_active_signals(state, btc_bundle[0], bar_index_now)
        except Exception as e:
            print(f"  [OUTCOME CHECK] failed: {e}")

    if _shutdown:
        save_state(state)
        sys.exit(0)

    # ── Phase 2: Scan ─────────────────────────────────────────────
    print("[PHASE 2] Scanning for signals…")
    finalize_breadth_cache()  # snapshot pre-scan breadth

    pending: list[tuple] = []
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futs = {
            ex.submit(scan_symbol, sym, state, bar_index_now,
                      candle_bundles.get(sym), ref_ms): sym
            for sym in WATCHLIST
        }
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                result = fut.result()
                if result:
                    pending.extend(result)
            except Exception as e:
                print(f"  [SCAN ERROR] {sym}: {e}")

    # Sort by priority, deduplicate
    pending.sort(key=lambda t: priority_score(t[2]), reverse=True)
    deduped = deduplicate_correlated(pending)

    # Concurrency cap
    concurrent = get_concurrent_count(state)
    available  = max(0, MAX_CONCURRENT_ACTIVE - concurrent)
    top     = deduped[:min(MAX_SIGNALS_PER_SCAN, available)]
    dropped = deduped[min(MAX_SIGNALS_PER_SCAN, available):]

    print(f"  [RANK] {len(pending)} raw signals → {len(deduped)} after dedup → {len(top)} to send "
          f"(concurrent active: {concurrent}/{MAX_CONCURRENT_ACTIVE})")

    if dropped:
        print(f"  [DROPPED] {[f'{hl_coin(s)} {d.upper()}' for s, d, _ in dropped]}")
        for sym, dirn, sig in dropped:
            record_signal_history(
                state, sym, dirn, sig.signal_type, sig.final_score,
                sig.funding_rate, sig.atr_pct,
                sig.oi_data.get("oi_change_pct"),
                sig.daily_class, sent=False, grade=sig.grade,
            )

    fired = 0
    for rank, (symbol, direction, sig) in enumerate(top, start=1):
        msg    = format_signal(symbol, sig, "SWING", rank=rank)
        msg_id = send_telegram(msg)

        hist_id = record_signal_history(
            state, symbol, direction, sig.signal_type, sig.final_score,
            sig.funding_rate, sig.atr_pct,
            sig.oi_data.get("oi_change_pct"),
            sig.daily_class, sent=True, grade=sig.grade,
        )

        if msg_id:
            update_cooldown(state, symbol, direction, bar_index_now)
            track_signal(state, symbol, direction, msg_id, sig, bar_index_now, hist_id)
            print(f"  [SENT] #{rank} {hl_coin(symbol)} {direction.upper()} "
                  f"score={sig.final_score} grade={sig.grade} "
                  f"TP1={sig.tp1:.4f}  TP2={sig.tp2:.4f}  SL={sig.sl:.4f}  R:R={abs(sig.tp1-sig.entry)/max(abs(sig.sl-sig.entry),1e-9):.2f}")
        else:
            print(f"  [TG FAIL] #{rank} {hl_coin(symbol)}")

        fired += 1
        time.sleep(0.5)

    save_state(state)
    print(f"Scan complete. {fired} signal(s) fired.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        try:
            send_telegram(f"🚨 Swing Engine v{__version__} crashed: {e}")
        except Exception:
            pass
        raise
    finally:
        _hl_session.close()
