"""
SWING ENGINE v3.0.0  —  1D → 4H → 1H  MULTI-TIMEFRAME
High-Quality Signal Engine (Rebuilt from scratch)

DESIGN PHILOSOPHY vs v2.x:
  The core problem with v2.x was signal quantity over quality — too many setups
  fired with weak confirmation stacks, resulting in losses and missed entries.

  v3.0 fixes this through:
    1. Stricter confluence gate (3 of 5 factors required, up from 2)
    2. Orderflow is a HARD gate in both directions — no signal without alignment
    3. Trend quality score replaces binary allow/deny on daily classification
    4. PULL entries require price to be within 0.4×ATR of the EMA (was 2.0×ATR)
    5. 4H structure must show confirming MSS (swing lows/highs stepping in direction)
    6. BREAK signals require volatility compression (VCP or ATR < 60th pct) beforehand
    7. Session hard-gating is stricter — only London/Overlap/NY for high-risk setups
    8. Divergence is a hard reject (was -2 penalty, could still pass)
    9. ADX DI alignment is mandatory for any scored setup (not just bonus)
   10. Adaptive minimum score raised to 9 baseline (from 8)
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

_FF_TZ = ZoneInfo("America/New_York")

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID   = os.getenv("TG_CHAT_ID")
if not TG_BOT_TOKEN:
    raise RuntimeError("TG_BOT_TOKEN environment variable is required")
if not TG_CHAT_ID:
    raise RuntimeError("TG_CHAT_ID environment variable is required")

_SCRIPT_DIR  = Path(__file__).resolve().parent
STATE_FILE   = str(_SCRIPT_DIR / "state.json")
STATE_VERSION = 1
LOCK_FILE    = str(_SCRIPT_DIR / "swing_engine.lock")

HL_INFO_URL           = "https://api.hyperliquid.xyz/info"
SCAN_WORKERS          = int(os.getenv("SCAN_WORKERS", "2"))
HL_MIN_INTERVAL_S     = float(os.getenv("HL_MIN_INTERVAL_S", "0.18"))
HL_MIN_INTERVAL_MAX_S = float(os.getenv("HL_MIN_INTERVAL_MAX_S", "0.60"))
HL_TF_WORKERS         = int(os.getenv("HL_TF_WORKERS", "2"))

N_1H = 150
N_4H = 220
N_1D = 300

INTERVAL_MS = {
    "1h":  60 * 60 * 1000,
    "4h":  4  * 60 * 60 * 1000,
    "1d":  24 * 60 * 60 * 1000,
}

EMA_FAST  = 21
EMA_SLOW  = 50
EMA_TREND = 200
RSI_LEN   = 14
ATR_LEN   = 14
ADX_LEN   = 14
VOL_LEN   = 20

WATCHLIST = [
    "BTCUSDT", "ETHUSDT", "HYPEUSDT", "ZECUSDT", "NEARUSDT",
    "ONDOUSDT", "SUIUSDT", "PENGUUSDT", "BNBUSDT", "SOLUSDT",
    "TRXUSDT", "BCHUSDT", "DOGEUSDT", "ADAUSDT", "DOTUSDT",
    "TAOUSDT", "AVAXUSDT", "LINKUSDT", "AAVEUSDT", "XRPUSDT",
    "XLMUSDT", "UNIUSDT", "LTCUSDT", "APTUSDT", "PENDLEUSDT",
]

MIN_SIGNAL_SCORE   = 9
PREMIUM_SCORE      = 12
MAX_SCORE          = 15

DAILY_ADX_STRONG   = 28.0
DAILY_ADX_WEAK     = 18.0
MS_LOOKBACK_BARS   = 20

H4_ADX_MIN         = 22.0
H4_RSI_OB          = 68.0
H4_RSI_OS          = 32.0
H4_VOL_MULT        = 1.0

H1_RSI_BULL        = 50.0
H1_RSI_BEAR        = 50.0
H1_RSI_OB          = 72.0
H1_RSI_OS          = 28.0
ENGULF_BODY_RATIO  = 0.6
IMPULSE_VOL_MULT   = 1.5
SWING_LOOKBACK     = 5

PULL_MAX_EMA_DIST_ATR = 0.4

ATR_FALLBACK_PCT   = 0.015
MIN_ATR_PCT        = 0.15
MAX_ATR_PCT        = 12.0
MIN_RR_RATIO       = 1.3
PREFERRED_RR_RATIO = 2.0

TP1_MULT_CONT  = 1.7
TP2_MULT_CONT  = 3.2
SL_MULT_CONT   = 1.0

TP1_MULT_PULL  = 1.4
TP2_MULT_PULL  = 2.8
SL_MULT_PULL   = 0.75

TP1_MULT_BREAK = 1.7
TP2_MULT_BREAK = 3.5
SL_MULT_BREAK  = 1.0

SETUP_TP_SL_MULTS = {
    "CONT":  (TP1_MULT_CONT,  TP2_MULT_CONT,  SL_MULT_CONT),
    "PULL":  (TP1_MULT_PULL,  TP2_MULT_PULL,  SL_MULT_PULL),
    "BREAK": (TP1_MULT_BREAK, TP2_MULT_BREAK, SL_MULT_BREAK),
}

SL_HIGH_ATR_MULT       = 0.80
HIGH_ATR_THRESHOLD     = 3.0

REGIME_BULL_TP2_MULT   = 1.15
REGIME_BEAR_TP1_MULT   = 0.85
REGIME_HIGHVOL_SL_MULT = 0.90

MAX_SIGNALS_PER_SCAN       = 3
MAX_CONCURRENT_ACTIVE      = 10
SIGNAL_MAX_AGE_1H_BARS     = 24
SIGNAL_COOLDOWN_1H_BARS    = 3
SIGNAL_COOLDOWN_POST_WIN   = 1
PULL_REENTRY_COOLDOWN_S    = 3600

MIN_OI_USD           = 500_000.0
MIN_OI_USD_SMALL_CAP = 250_000.0
SMALL_CAP_PAIRS: set[str] = {
    "PENGUUSDT", "HYPEUSDT", "ZECUSDT", "PENDLEUSDT",
}
FUNDING_SUPPRESS_EXTREME = 0.0010
SPREAD_WARN_PCT      = 0.20
SPREAD_SUPPRESS_PCT  = 0.40
SPREAD_EXEMPT: set[str] = {
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT",
    "XRPUSDT", "ADAUSDT", "DOGEUSDT",
}

MAX_NEGATIVE_ADJUSTMENTS = 4
MAX_POSITIVE_ADJUSTMENTS = 4

DYNAMIC_CORR_CLUSTER_THRESHOLD = 0.75
CORR_MATRIX_MIN_SAMPLE         = 20
LOW_BTC_CORR_LOOKBACK_BARS     = 42
LOW_BTC_CORR_THRESHOLD         = 0.65

RS_TOP_PERCENTILE       = 0.20
RS_BOTTOM_PERCENTILE    = 0.20
RS_BEARISH_EXEMPT_PCT   = 3.0

BREADTH_WEAK_LONG    = 0.20
BREADTH_WEAK_SHORT   = 0.80
BREADTH_CROWDED_LONG = 0.75
BREADTH_EXTREME_LONG = 0.90
BREADTH_EXTREME_SHORT = 0.10

OI_HISTORY_DEPTH        = 24
OI_CHANGE_THRESHOLD_PCT = 1.0
OI_STALE_CUTOFF_S       = 45 * 60
OI_EXPECTED_INTERVAL_S  = 15 * 60
OI_SCORE_CAP            = 2

ATR_HIST_DEPTH      = 168
ATR_HIGH_PERCENTILE = 0.80

MACRO_WINDOW_BEFORE_MINS = 60
MACRO_WINDOW_AFTER_MINS  = 30
MACRO_HIGH_ATR_SUPPRESS  = 3.0
MACRO_CACHE_TTL_S        = 3600
MACRO_CALENDAR_URL       = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

FUNDING_CARRY_POS_THRESHOLD = 0.0005
FUNDING_CARRY_NEG_THRESHOLD = -0.0005
FUNDING_CARRY_BONUS         = 1
FUNDING_HEADWIND_THRESHOLD  = 0.0005
FUNDING_HISTORY_DEPTH       = 4

SWEEP_LOOKBACK    = 12
VCP_LOOKBACK      = 14
VCP_MIN_STAGES    = 2
ATR_REGIME_LOW_PCT = 0.20
ATR_REGIME_MED_PCT = 0.50
ATR_REGIME_HIGH_PCT = 0.80

CONFLUENCE_MIN_FACTORS  = 3
CONFLUENCE_BONUS_FACTORS = 4

GRADE_A_PLUS_SCORE = 14
GRADE_A_SCORE      = 12
GRADE_B_SCORE      = 10
GRADE_C_SCORE      = 9

GRADE_MAX_LEVERAGE = {"A+": 10.0, "A": 8.0, "B": 5.0, "C": 3.0}
GRADE_SIZE_PCT     = {"A+": 100,  "A": 100,  "B": 75,  "C": 50}

ORDERFLOW_CVD_LOOKBACK       = 24
ORDERFLOW_VOL_RATIO_LOOKBACK = 5
ORDERFLOW_LONG_RATIO_STRONG  = 0.60
ORDERFLOW_LONG_RATIO_WEAK    = 0.45
ORDERFLOW_SHORT_RATIO_STRONG = 0.40
ORDERFLOW_SHORT_RATIO_WEAK   = 0.55

EQUAL_HL_TOLERANCE_PCT = 0.0015
EQUAL_HL_LOOKBACK      = 30
ROUND_NUMBER_TOLERANCE = 0.005

WIN_RATE_MIN_SAMPLE         = 20
WIN_RATE_HIGH_THRESH        = 0.60
WIN_RATE_LOW_THRESH         = 0.40
WIN_RATE_MIN_SAMPLE_FOR_ADJ = 25
WIN_RATE_HARD_SUPPRESS_THRESHOLD  = 0.35
WIN_RATE_HARD_SUPPRESS_MIN_SAMPLE = 25
WIN_RATE_LOOKBACK_DAYS = 30
WIN_RATE_RECENT_DAYS   = 7
WIN_RATE_RECENT_WEIGHT = 3.0
WIN_RATE_STALE_DAYS    = 14

MAX_SIGNAL_HISTORY = 2000
META_CACHE_TTL_S   = 55.0

SR_PIVOT_LEFT  = 3
SR_PIVOT_RIGHT = 3
SR_LOOKBACK    = 100
SR_CLUSTER_ATR = 0.30

BREAK_LOOKBACK_LOCAL = 10
BREAK_LOOKBACK_MAJOR = 30

REACT_TP1  = "🔥"
REACT_TP2  = "🏆"
REACT_SL   = "😭"
REACT_MISS = "😢"

_hl_request_lock        = threading.Lock()
_hl_last_request_ts     = 0.0
_hl_min_interval_s      = HL_MIN_INTERVAL_S
_hl_consecutive_success = 0
_hl_session             = requests.Session()
_shutdown               = False


def _handle_shutdown(sig, frame):
    global _shutdown
    _shutdown = True
    print(f"\n[SHUTDOWN] Signal {sig} received — finishing current scan…")


for _sig in (os_signal.SIGINT, os_signal.SIGTERM):
    try:
        os_signal.signal(_sig, _handle_shutdown)
    except Exception:
        pass


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
                gap = _hl_min_interval_s - elapsed
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
                start_time_ms: int | None = None,
                reference_ms: int | None = None) -> list[dict]:
    coin   = hl_coin(symbol)
    iv_ms  = INTERVAL_MS.get(interval, 3600_000)
    ref_ms = int(time.time() * 1000) if reference_ms is None else reference_ms
    end_ms = current_bar_open_ms(ref_ms, interval)
    s_ms   = start_time_ms if start_time_ms is not None else end_ms - iv_ms * (n + 10)

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
            ex.submit(get_candles, symbol, tf, n, None, reference_ms): tf
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


_meta_cache: dict | None = None
_meta_cache_lock          = threading.Lock()
_meta_cache_fetched_at    = 0.0


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
        ag  = (ag * (period - 1) + gains[i])  / period
        al  = (al * (period - 1) + losses[i]) / period
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
        plus_dm[i]  = up if (up > dn and up > 0)   else 0
        minus_dm[i] = dn if (dn > up and dn > 0)   else 0
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
    sig = (len(candles), last["t"], last["c"], first["t"], first["c"])
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


_state_lock = threading.RLock()


def load_state() -> dict:
    fresh = {
        "_version":             STATE_VERSION,
        "oi_history":           {},
        "signal_history":       [],
        "macro_calendar_cache": {},
        "post_loss_cooldown":   {},
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
        data = copy.deepcopy(state)
    path = Path(STATE_FILE)
    bak  = Path(STATE_FILE + ".bak")
    try:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, default=str, indent=2))
        if path.exists():
            path.replace(bak)
        tmp.replace(path)
    except Exception as e:
        print(f"[STATE] Save failed: {e}")


def update_atr_history(state: dict, symbol: str, atr_pct: float):
    with _state_lock:
        hist = state.setdefault("atr_history", {}).setdefault(symbol, [])
        hist.append(atr_pct)
        if len(hist) > ATR_HIST_DEPTH:
            state["atr_history"][symbol] = hist[-ATR_HIST_DEPTH:]


def get_atr_percentile(state: dict, symbol: str, atr_pct: float) -> float | None:
    with _state_lock:
        hist = state.get("atr_history", {}).get(symbol, [])
    if len(hist) < 30:
        return None
    below = sum(1 for v in hist if v <= atr_pct)
    return below / len(hist)


def update_oi_history(state: dict, symbol: str, oi: float):
    with _state_lock:
        hist = state.setdefault("oi_history", {}).setdefault(symbol, [])
        hist.append({"oi": oi, "ts": int(time.time())})
        if len(hist) > OI_HISTORY_DEPTH:
            state["oi_history"][symbol] = hist[-OI_HISTORY_DEPTH:]


def compute_oi_score(state: dict, symbol: str, direction: str) -> dict:
    with _state_lock:
        hist = list(state.get("oi_history", {}).get(symbol, []))
    if len(hist) < 2:
        return {"score_adj": 0, "oi_change_pct": None, "label": "OI: N/A"}

    now_ts = int(time.time())
    valid  = [e for e in hist if now_ts - e["ts"] < OI_STALE_CUTOFF_S]
    if len(valid) < 2:
        return {"score_adj": 0, "oi_change_pct": None, "label": "OI: Stale data"}

    oldest_oi = valid[0]["oi"]
    newest_oi = valid[-1]["oi"]
    if oldest_oi <= 0:
        return {"score_adj": 0, "oi_change_pct": None, "label": "OI: Invalid baseline"}

    change_pct = (newest_oi - oldest_oi) / oldest_oi * 100
    rising     = change_pct >= OI_CHANGE_THRESHOLD_PCT
    falling    = change_pct <= -OI_CHANGE_THRESHOLD_PCT

    if direction == "long":
        if rising:
            adj = 1; lbl = f"OI rising {change_pct:+.1f}% +1"
        elif falling:
            adj = -1; lbl = f"OI falling {change_pct:+.1f}% -1"
        else:
            adj = 0; lbl = f"OI flat {change_pct:+.1f}% (0)"
    else:
        if falling:
            adj = 1; lbl = f"OI falling {change_pct:+.1f}% +1"
        elif rising:
            adj = -1; lbl = f"OI rising {change_pct:+.1f}% -1"
        else:
            adj = 0; lbl = f"OI flat {change_pct:+.1f}% (0)"

    return {"score_adj": min(adj, OI_SCORE_CAP), "oi_change_pct": change_pct, "label": f"OI: {lbl}"}


def get_funding_trend(state: dict, symbol: str) -> str:
    with _state_lock:
        hist = list(state.get("funding_history", {}).get(symbol, []))
    if len(hist) < 2:
        return "unknown"
    return "rising" if hist[-1] > hist[-2] else "falling"


def update_funding_history(state: dict, symbol: str, rate: float):
    with _state_lock:
        hist = state.setdefault("funding_history", {}).setdefault(symbol, [])
        hist.append(rate)
        if len(hist) > FUNDING_HISTORY_DEPTH:
            state["funding_history"][symbol] = hist[-FUNDING_HISTORY_DEPTH:]


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
    if not candles_1d or len(candles_1d) < EMA_SLOW + 5:
        return {"label": "BTC Regime: Unknown", "bullish": False, "bearish": False, "neutral": True}
    ind   = _compute_all_indicators(candles_1d)
    ef    = safe(ind["ema_fast"][-1])
    es    = safe(ind["ema_slow"][-1])
    et    = safe(ind["ema_trend"][-1])
    cur   = ind["c"][-1]
    rsi_v = safe(ind["rsi"][-1], 50.0)
    bull  = ef > es > et and cur > et and rsi_v > 50
    bear  = ef < es < et and cur < et and rsi_v < 50
    if bull:
        return {"label": "BTC: Bullish", "bullish": True, "bearish": False, "neutral": False}
    if bear:
        return {"label": "BTC: Bearish", "bullish": False, "bearish": True, "neutral": False}
    return {"label": "BTC: Mixed", "bullish": False, "bearish": False, "neutral": True}


_rs_scores:   dict[str, float]        = {}
_rs_snapshot: dict[str, float] | None = None
_rs_lock = threading.Lock()


def reset_rs_cache():
    global _rs_snapshot
    with _rs_lock:
        _rs_scores.clear()
        _rs_snapshot = None


def record_rs_return(symbol: str, ret_pct: float):
    with _rs_lock:
        _rs_scores[symbol] = ret_pct


def finalize_rs_cache():
    global _rs_snapshot
    with _rs_lock:
        _rs_snapshot = dict(_rs_scores)


def compute_relative_strength(symbol: str) -> dict:
    with _rs_lock:
        scores = dict(_rs_snapshot if _rs_snapshot is not None else _rs_scores)
    btc_ret  = scores.get("BTCUSDT")
    coin_ret = scores.get(symbol)
    if btc_ret is None or coin_ret is None:
        return {"rs_pct": None, "percentile": None, "score_adj": 0, "label": "RS: N/A"}
    rs = coin_ret - btc_ret
    others  = {k: v - btc_ret for k, v in scores.items() if k != "BTCUSDT"}
    all_rs  = sorted(others.values())
    n = len(all_rs)
    if n == 0:
        return {"rs_pct": rs, "percentile": 0.5, "score_adj": 0, "label": f"RS: {rs:+.1f}%"}
    try:
        rank = next(i for i, v in enumerate(all_rs) if v >= rs)
        pct  = rank / max(n - 1, 1)
    except StopIteration:
        pct = 1.0
    adj = 1 if pct >= 1.0 - RS_TOP_PERCENTILE else (-1 if pct <= RS_BOTTOM_PERCENTILE else 0)
    return {"rs_pct": rs, "percentile": pct, "score_adj": adj, "label": f"RS: {rs:+.1f}%"}


_breadth_above: dict[str, bool] = {}
_breadth_snapshot: dict[str, bool] | None = None
_breadth_lock = threading.Lock()


def reset_breadth_cache():
    global _breadth_snapshot
    with _breadth_lock:
        _breadth_above.clear()
        _breadth_snapshot = None


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
    if pct < BREADTH_WEAK_LONG:
        lbl = f"Breadth: {pct*100:.0f}% (Weak)"
    elif pct > BREADTH_WEAK_SHORT:
        lbl = f"Breadth: {pct*100:.0f}% (Overbought)"
    else:
        lbl = f"Breadth: {pct*100:.0f}% (Healthy)"
    return {"breadth_pct": pct, "label": lbl}


def apply_breadth_adjustment(direction: str, rs_pct: float | None = None) -> tuple[int, str]:
    breadth = compute_market_breadth()
    pct     = breadth["breadth_pct"]
    label   = breadth["label"]
    adj     = 0
    if direction == "long":
        if pct > BREADTH_EXTREME_LONG:
            adj = -2; label += " (-2 extreme)"
        elif pct > BREADTH_CROWDED_LONG:
            rs_weak = rs_pct is not None and rs_pct <= 0
            adj = -2 if rs_weak else -1
            label += f" (-{abs(adj)}, crowded)"
        elif pct < BREADTH_WEAK_LONG:
            adj = -1; label += " (-1, weak)"
    elif direction == "short":
        if pct < BREADTH_EXTREME_SHORT:
            adj = -2; label += " (-2 extreme)"
        elif pct > BREADTH_WEAK_SHORT:
            rs_str = rs_pct is not None and rs_pct >= 0
            adj = -2 if rs_str else -1
            label += f" (-{abs(adj)}, crowded)"
    return adj, label


def check_btc_regime_filter(direction: str, symbol: str) -> tuple[int, str]:
    if hl_coin(symbol) == "BTC":
        return 0, "BTC Regime: N/A"
    regime = get_btc_regime()
    if regime is None:
        return 0, "BTC Regime: Unknown"
    label = regime["label"]
    if direction == "long" and regime["bearish"]:
        return -1, f"{label} — counter-trend (-1)"
    if direction == "short" and regime["bullish"]:
        return -1, f"{label} — counter-trend (-1)"
    if direction == "long" and regime["bullish"]:
        return +1, f"{label} — tailwind (+1)"
    if direction == "short" and regime["bearish"]:
        return +1, f"{label} — tailwind (+1)"
    return 0, f"{label} — Mixed (0)"


def record_signal_history(state: dict, symbol: str, direction: str,
                           signal_type: str, score: int,
                           funding_rate: float | None, atr_pct: float,
                           oi_change_pct: float | None,
                           daily_class: str = "Neutral",
                           sent: bool = True,
                           grade: str = "C") -> str:
    with _state_lock:
        hist     = state.setdefault("signal_history", [])
        entry_id = f"{symbol}_{int(time.time())}"
        hist.append({
            "id": entry_id, "symbol": symbol, "direction": direction,
            "signal_type": signal_type, "score": score,
            "daily_class": daily_class,
            "funding_rate": funding_rate, "atr_pct": atr_pct,
            "oi_change_pct": oi_change_pct,
            "result": None, "sent": sent,
            "timestamp": int(time.time()),
            "grade": grade,
        })
        if len(hist) > MAX_SIGNAL_HISTORY:
            state["signal_history"] = hist[-MAX_SIGNAL_HISTORY:]
    return entry_id


def update_signal_result(state: dict, signal_id: str, result: str):
    with _state_lock:
        for e in state.get("signal_history", []):
            if e.get("id") == signal_id:
                e["result"] = result
                return


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
    wi = max(1, int(WIN_RATE_RECENT_WEIGHT))

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

    wrs: dict = {"by_symbol": {}, "by_type": {}, "by_direction": {}, "by_daily_class": {}}
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


def compute_wr_analytics(state: dict, symbol: str, direction: str,
                          signal_type: str, daily_class: str) -> dict:
    wrs = get_cached_win_rates(state)
    for key, lookup in [
        ("by_symbol",    symbol),
        ("by_type",      signal_type),
        ("by_direction", direction),
    ]:
        entry = wrs.get(key, {}).get(lookup)
        if entry:
            wr, n = entry["wr"], entry["n"]
            if n >= WIN_RATE_HARD_SUPPRESS_MIN_SAMPLE and wr < WIN_RATE_HARD_SUPPRESS_THRESHOLD:
                return {"win_rate": wr, "sample_size": n, "score_adj": -3,
                        "label": f"WR SUPPRESS: {wr*100:.0f}% (n={n})"}
            if n < WIN_RATE_MIN_SAMPLE_FOR_ADJ:
                return {"win_rate": wr, "sample_size": n, "score_adj": 0,
                        "label": f"WR: {wr*100:.0f}% (n={n}, insufficient)"}
            adj = 1 if wr >= WIN_RATE_HIGH_THRESH else (-1 if wr <= WIN_RATE_LOW_THRESH else 0)
            return {"win_rate": wr, "sample_size": n, "score_adj": adj,
                    "label": f"WR: {wr*100:.0f}% n={n}"}
    return {"win_rate": None, "sample_size": 0, "score_adj": 0, "label": "WR: N/A"}


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
                print(f"  [MACRO] fetch failed: {e}")
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
    print(f"  [MACRO] Loaded {len(events)} high-impact events")
    return events


def apply_macro_filter(state: dict, atr_pct: float,
                        reference_ms: int | None = None) -> dict:
    events  = fetch_macro_calendar(state)
    ref_ts  = (reference_ms / 1000) if reference_ms is not None else time.time()
    now_utc = datetime.fromtimestamp(ref_ts, tz=timezone.utc)
    nearest = None; nearest_mins = None
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
                   n_levels: int = 2) -> tuple[list[float], list[float]]:
    lb = SR_PIVOT_LEFT; rb = SR_PIVOT_RIGHT
    window = candles_4h[max(0, len(candles_4h) - 1 - SR_LOOKBACK): -1]
    ph, pl = [], []
    for i in range(lb, len(window) - rb):
        h  = window[i]["h"]; lo = window[i]["l"]
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


def volume_rising(ind: dict, bars: int = 3) -> bool:
    v = ind["v"]
    if len(v) < bars * 2:
        return False
    recent = v[-bars:]
    prior  = v[-bars * 2:-bars]
    if not prior:
        return False
    return (sum(recent) / len(recent)) >= (sum(prior) / len(prior))


def detect_swing_divergence(ind_4h: dict, direction: str, lookback: int = 14) -> dict:
    closes  = ind_4h["c"]
    rsi_arr = ind_4h["rsi"]
    if len(closes) < lookback + 2:
        return {"divergent": False, "label": "Divergence: N/A"}

    c_window = closes[-lookback:]
    r_window = rsi_arr[-lookback:]
    if any(math.isnan(r) for r in r_window):
        return {"divergent": False, "label": "Divergence: RSI not seeded"}

    if direction == "long":
        swing_highs = [i for i in range(1, len(c_window) - 1)
                       if c_window[i] > c_window[i - 1] and c_window[i] > c_window[i + 1]]
        if len(swing_highs) < 2:
            return {"divergent": False, "label": "Divergence: none"}
        sh1, sh2 = swing_highs[-2], swing_highs[-1]
        divergent = c_window[sh2] > c_window[sh1] and r_window[sh2] < r_window[sh1]
        label = "Bearish divergence (HH price, lower RSI)" if divergent else "Divergence: none"
    else:
        swing_lows = [i for i in range(1, len(c_window) - 1)
                      if c_window[i] < c_window[i - 1] and c_window[i] < c_window[i + 1]]
        if len(swing_lows) < 2:
            return {"divergent": False, "label": "Divergence: none"}
        sl1, sl2 = swing_lows[-2], swing_lows[-1]
        divergent = c_window[sl2] < c_window[sl1] and r_window[sl2] > r_window[sl1]
        label = "Bullish divergence vs short (LL price, higher RSI)" if divergent else "Divergence: none"

    return {"divergent": divergent, "label": label}


def detect_vcp(atr_history: list[float], lookback: int = VCP_LOOKBACK) -> dict:
    if len(atr_history) < lookback:
        return {"vcp": False, "stages": 0}
    window = atr_history[-lookback:]
    peaks  = [
        window[i]
        for i in range(1, len(window) - 1)
        if window[i] > window[i - 1] and window[i] > window[i + 1]
    ]
    if len(peaks) < 2:
        return {"vcp": False, "stages": 0}
    stages = sum(1 for i in range(1, len(peaks)) if peaks[i] < peaks[i - 1])
    return {"vcp": stages >= VCP_MIN_STAGES, "stages": stages}


def detect_liquidity_sweep(candles_4h: list[dict], direction: str, atr_4h: float) -> dict:
    null = {"type": "NONE", "score": 0, "sweep_level": None}
    if len(candles_4h) < SWEEP_LOOKBACK + 2:
        return null
    window = candles_4h[-(SWEEP_LOOKBACK + 1):-1]
    cur    = candles_4h[-1]
    if direction == "long":
        swing_level = min(c["l"] for c in window)
        if cur["l"] < swing_level - atr_4h * 0.3 and cur["c"] > swing_level:
            return {"type": "SWEEP", "score": 3, "sweep_level": swing_level}
    else:
        swing_level = max(c["h"] for c in window)
        if cur["h"] > swing_level + atr_4h * 0.3 and cur["c"] < swing_level:
            return {"type": "SWEEP", "score": 3, "sweep_level": swing_level}
    return null


def detect_market_structure_4h(candles_4h: list[dict], direction: str) -> dict:
    if len(candles_4h) < 10:
        return {"confirming_mss": False, "adverse_bos": False,
                "adverse_mss": False, "label": "MSS/BOS: N/A"}

    highs  = [c["h"] for c in candles_4h]
    lows   = [c["l"] for c in candles_4h]
    closes = [c["c"] for c in candles_4h]
    cur_c  = closes[-1]
    n      = len(candles_4h)

    swing_highs = [highs[i] for i in range(1, n - 1)
                   if highs[i] > highs[i - 1] and highs[i] > highs[i + 1]]
    swing_lows  = [lows[i]  for i in range(1, n - 1)
                   if lows[i]  < lows[i - 1]  and lows[i]  < lows[i + 1]]

    _atr_arr = atr(highs, lows, closes, ATR_LEN)
    _atr_bos = safe(_atr_arr[-1], closes[-1] * 0.01)
    BOS_BUFFER = 0.3

    confirming_mss = adverse_bos = adverse_mss = False
    bos_level = None

    if direction == "long":
        if len(swing_lows) >= 2:
            confirming_mss = swing_lows[-1] > swing_lows[-2]
            adverse_mss    = swing_lows[-1] < swing_lows[-2]
        if swing_lows:
            if cur_c < swing_lows[-1] - _atr_bos * BOS_BUFFER:
                adverse_bos = True
                bos_level   = swing_lows[-1]
    else:
        if len(swing_highs) >= 2:
            confirming_mss = swing_highs[-1] < swing_highs[-2]
            adverse_mss    = swing_highs[-1] > swing_highs[-2]
        if swing_highs:
            if cur_c > swing_highs[-1] + _atr_bos * BOS_BUFFER:
                adverse_bos = True
                bos_level   = swing_highs[-1]

    return {
        "confirming_mss": confirming_mss,
        "adverse_bos":    adverse_bos,
        "adverse_mss":    adverse_mss,
        "bos_level":      bos_level,
        "label": (
            "MSS_CONFIRM" if confirming_mss else
            "BOS_ADVERSE" if adverse_bos else
            "MSS_ADVERSE" if adverse_mss else
            "Neutral"
        ),
    }


def detect_order_blocks(candles_4h: list[dict], direction: str) -> list[dict]:
    if len(candles_4h) < 5:
        return []
    blocks = []
    n = len(candles_4h)
    for i in range(2, n - 1):
        c     = candles_4h[i]
        nxt   = candles_4h[i + 1]
        body  = abs(c["c"] - c["o"])
        rng   = c["h"] - c["l"]
        if rng <= 0 or body / rng < 0.5:
            continue
        if direction == "long":
            if c["c"] < c["o"] and nxt["c"] > nxt["o"] and nxt["c"] > c["h"]:
                blocks.append({"high": c["h"], "low": c["l"], "mid": (c["h"] + c["l"]) / 2})
        else:
            if c["c"] > c["o"] and nxt["c"] < nxt["o"] and nxt["c"] < c["l"]:
                blocks.append({"high": c["h"], "low": c["l"], "mid": (c["h"] + c["l"]) / 2})
    return blocks[-3:]


def price_in_ob_zone(cur_c: float, blocks: list[dict], atr_val: float,
                      direction: str) -> bool:
    for ob in blocks:
        if direction == "long":
            if ob["low"] - atr_val * 0.2 <= cur_c <= ob["high"] + atr_val * 0.2:
                return True
        else:
            if ob["low"] - atr_val * 0.2 <= cur_c <= ob["high"] + atr_val * 0.2:
                return True
    return False


def compute_orderflow(candles_1h: list[dict], direction: str) -> dict:
    null = {
        "delta_score": 0, "cvd_score": 0, "ratio_score": 0, "of_net": 0,
        "hard_reject": False, "cvd_rising_bars": 0,
        "labels": {"delta": "Delta: N/A", "cvd": "CVD: N/A", "ratio": "Vol Ratio: N/A"},
    }
    if len(candles_1h) < ORDERFLOW_CVD_LOOKBACK + 2:
        return null

    closes = [c["c"] for c in candles_1h]
    opens  = [c["o"] for c in candles_1h]
    highs  = [c["h"] for c in candles_1h]
    lows   = [c["l"] for c in candles_1h]
    vols   = [c["v"] for c in candles_1h]

    _ranges  = [highs[i] - lows[i] for i in range(len(closes)) if highs[i] > lows[i]]
    _atr_est = (sum(_ranges[-14:]) / min(14, len(_ranges[-14:]))) if _ranges else 0.0
    deltas   = []
    for i in range(len(closes)):
        rng = highs[i] - lows[i]
        if rng <= 0 or (_atr_est > 0 and rng < _atr_est * 0.3):
            deltas.append(0.0)
            continue
        deltas.append((closes[i] - opens[i]) / rng * vols[i])

    last3 = deltas[-3:]; last5 = deltas[-5:]
    net5  = sum(last5)

    if direction == "long":
        if all(d > 0 for d in last3) and abs(last3[-1]) > abs(last3[0]):
            delta_score = 2; delta_lbl = "Delta: Strong bull +2"
        elif net5 > 0:
            delta_score = 1; delta_lbl = "Delta: Net bull +1"
        elif sum(1 for d in deltas[-2:] if d < 0) == 2 and closes[-1] > closes[-3]:
            delta_score = -2; delta_lbl = "Delta: Bear divergence -2"
        else:
            delta_score = 0; delta_lbl = "Delta: Mixed 0"
    else:
        if all(d < 0 for d in last3) and abs(last3[-1]) > abs(last3[0]):
            delta_score = 2; delta_lbl = "Delta: Strong bear +2"
        elif net5 < 0:
            delta_score = 1; delta_lbl = "Delta: Net bear +1"
        elif sum(1 for d in deltas[-2:] if d > 0) == 2 and closes[-1] < closes[-3]:
            delta_score = -2; delta_lbl = "Delta: Bull divergence -2"
        else:
            delta_score = 0; delta_lbl = "Delta: Mixed 0"

    cvd_window = deltas[-ORDERFLOW_CVD_LOOKBACK:]
    cvd_series = []
    running    = 0.0
    for d in cvd_window:
        running += d
        cvd_series.append(running)

    cvd_rising_bars = 0
    for i in range(len(cvd_series) - 1, 0, -1):
        if cvd_series[i] > cvd_series[i - 1]:
            cvd_rising_bars += 1
        else:
            break

    cvd_lows = [cvd_series[i] for i in range(1, len(cvd_series) - 1)
                if cvd_series[i] < cvd_series[i - 1] and cvd_series[i] < cvd_series[i + 1]]
    cvd_rising = (len(cvd_lows) >= 2 and cvd_lows[-1] > cvd_lows[-2]) if len(cvd_lows) >= 2 else (cvd_series[-1] > cvd_series[0])

    if direction == "long":
        if cvd_rising and cvd_series[-1] > 0:
            cvd_score = 1; cvd_lbl = "CVD: Rising+pos +1"
        elif not cvd_rising and cvd_series[-1] < cvd_series[0] * 0.9:
            cvd_score = -2; cvd_lbl = "CVD: Distribution -2"
        else:
            cvd_score = 0; cvd_lbl = "CVD: Mixed 0"
    else:
        if not cvd_rising and cvd_series[-1] < 0:
            cvd_score = 1; cvd_lbl = "CVD: Falling+neg +1"
        elif cvd_rising and cvd_series[-1] > cvd_series[0]:
            cvd_score = -2; cvd_lbl = "CVD: Absorption -2"
        else:
            cvd_score = 0; cvd_lbl = "CVD: Mixed 0"

    window_c = closes[-ORDERFLOW_VOL_RATIO_LOOKBACK:]
    window_o = opens[-ORDERFLOW_VOL_RATIO_LOOKBACK:]
    window_v = vols[-ORDERFLOW_VOL_RATIO_LOOKBACK:]
    buy_vol  = sum(window_v[i] for i in range(len(window_v)) if window_c[i] >= window_o[i])
    sell_vol = sum(window_v[i] for i in range(len(window_v)) if window_c[i] < window_o[i])
    total    = buy_vol + sell_vol
    ratio    = buy_vol / total if total > 0 else 0.5

    if direction == "long":
        if ratio > ORDERFLOW_LONG_RATIO_STRONG:
            ratio_score = 1; ratio_lbl = f"Vol Ratio: {ratio:.2f} buyers +1"
        elif ratio < ORDERFLOW_LONG_RATIO_WEAK:
            ratio_score = -1; ratio_lbl = f"Vol Ratio: {ratio:.2f} sellers -1"
        else:
            ratio_score = 0; ratio_lbl = f"Vol Ratio: {ratio:.2f} balanced 0"
    else:
        if ratio < ORDERFLOW_SHORT_RATIO_STRONG:
            ratio_score = 1; ratio_lbl = f"Vol Ratio: {ratio:.2f} sellers +1"
        elif ratio > ORDERFLOW_SHORT_RATIO_WEAK:
            ratio_score = -1; ratio_lbl = f"Vol Ratio: {ratio:.2f} buyers -1"
        else:
            ratio_score = 0; ratio_lbl = f"Vol Ratio: {ratio:.2f} balanced 0"

    of_net = delta_score + cvd_score + ratio_score
    hard_reject = of_net <= -4

    return {
        "delta_score": delta_score, "cvd_score": cvd_score,
        "ratio_score": ratio_score, "of_net": of_net,
        "hard_reject": hard_reject, "cvd_rising_bars": cvd_rising_bars,
        "labels": {"delta": delta_lbl, "cvd": cvd_lbl, "ratio": ratio_lbl},
    }


def classify_daily_trend(candles_1d: list[dict], symbol: str = "__DAILY__") -> dict:
    if len(candles_1d) < EMA_TREND + 5:
        return {
            "classification": "Neutral", "score": 0,
            "allows_long": True, "allows_short": True, "neutral_cap": True,
            "details": {"reason": "Insufficient daily data"},
        }

    ind    = get_cached_indicators(f"{symbol}_daily", "1d", candles_1d)
    closes = ind["c"]; highs = ind["h"]; lows = ind["l"]
    cur    = closes[-1]
    ef     = safe(ind["ema_fast"][-1])
    es     = safe(ind["ema_slow"][-1])
    et     = safe(ind["ema_trend"][-1])
    adx    = safe(ind["adx"][-1], 20.0)

    lb    = min(MS_LOOKBACK_BARS, len(closes) - 1)
    h_sub = highs[-lb:]; l_sub = lows[-lb:]

    def pivots(arr, fn):
        return [arr[i] for i in range(1, len(arr) - 1)
                if fn(arr[i], arr[i - 1]) and fn(arr[i], arr[i + 1])]

    ph   = pivots(h_sub, lambda a, b: a > b)
    pl   = pivots(l_sub, lambda a, b: a < b)
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
        cls = "Strong Bullish"; score = 3
    elif bullish:
        cls = "Bullish"; score = 2
    elif strong_bear:
        cls = "Strong Bearish"; score = -3
    elif bearish:
        cls = "Bearish"; score = -2
    else:
        cls = "Neutral"; score = 0

    neutral_cap  = (cls == "Neutral")
    allows_long  = cls in ("Strong Bullish", "Bullish") or neutral_cap
    allows_short = cls in ("Strong Bearish", "Bearish") or neutral_cap

    return {
        "classification": cls, "score": score,
        "allows_long": allows_long, "allows_short": allows_short,
        "neutral_cap": neutral_cap,
        "adx": adx, "ef": ef, "es": es, "et": et,
        "hh_hl": hh_hl, "lh_ll": lh_ll,
        "details": {"ema21": ef, "ema50": es, "ema200": et, "adx": adx},
    }


def score_daily_alignment(daily: dict, direction: str) -> tuple[int, str]:
    cls = daily["classification"]
    if direction == "long":
        if cls == "Strong Bullish": return 3, "1D Strong Bull (+3)"
        elif cls == "Bullish":      return 2, "1D Bullish (+2)"
        elif cls == "Neutral":      return 0, "1D Neutral (0)"
        else:                       return 1, f"1D {cls} (+1, baseline)"
    else:
        if cls == "Strong Bearish": return 3, "1D Strong Bear (+3)"
        elif cls == "Bearish":      return 2, "1D Bearish (+2)"
        elif cls == "Neutral":      return 0, "1D Neutral (0)"
        else:                       return 1, f"1D {cls} (+1, baseline)"


def detect_4h_setup(candles_4h: list[dict], daily: dict,
                    direction: str, symbol: str = "__4H__",
                    atr_history: list[float] | None = None) -> dict:
    if len(candles_4h) < EMA_SLOW + 5:
        return {"setup_type": "NONE", "score": 0, "details": {}}

    ind   = get_cached_indicators(f"{symbol}_4h", "4h", candles_4h)
    c     = ind["c"]; h = ind["h"]; l_ = ind["l"]; v = ind["v"]
    ef    = safe(ind["ema_fast"][-1])
    es    = safe(ind["ema_slow"][-1])
    adx   = safe(ind["adx"][-1], 20.0)
    rsi_  = safe(ind["rsi"][-1], 50.0)
    atr_  = safe(ind["atr"][-1], c[-1] * 0.01)
    vm    = safe(ind["vol_ma"][-1])
    di_p  = safe(ind["di_plus"][-1], 25.0)
    di_m  = safe(ind["di_minus"][-1], 25.0)
    cur_c = c[-1]; cur_v = v[-1]

    h4_bull   = ef > es
    h4_bear   = ef < es
    ema_aligned = (direction == "long" and h4_bull) or (direction == "short" and h4_bear)

    rsi_healthy = (direction == "long"  and 28 < rsi_ < H4_RSI_OB) or \
                  (direction == "short" and H4_RSI_OS < rsi_ < 72)

    vol_ok = vm > 0 and cur_v >= vm * H4_VOL_MULT

    di_aligned = (direction == "long"  and di_p > di_m) or \
                 (direction == "short" and di_m > di_p)

    adx_ok = adx >= H4_ADX_MIN and di_aligned

    struct = detect_market_structure_4h(candles_4h, direction)
    if struct["adverse_bos"]:
        return {"setup_type": "NONE", "score": 0, "details": {"reason": "Adverse BOS"}}

    sweep  = detect_liquidity_sweep(candles_4h, direction, atr_)
    ob_list = detect_order_blocks(candles_4h, direction)

    vcp_hist = atr_history or []
    vcp_data = detect_vcp(vcp_hist) if len(vcp_hist) >= VCP_LOOKBACK else {"vcp": False, "stages": 0}

    atr_pctile: float | None = None
    if len(vcp_hist) >= 30:
        below = sum(1 for x in vcp_hist if x <= (atr_ / cur_c * 100))
        atr_pctile = below / len(vcp_hist)

    if sweep["type"] == "SWEEP":
        setup_type = "PULL_SWEEP"
        score = 3
        details = {
            "adx": adx, "rsi": rsi_, "ema_fast": ef, "ema_slow": es,
            "atr_val": atr_, "di_plus_4h": di_p, "di_minus_4h": di_m,
            "vol_ratio": cur_v / vm if vm > 0 else None,
            "sweep": sweep, "struct": struct, "vcp": vcp_data,
        }
        return {"setup_type": setup_type, "score": score, "details": details}

    setup_type = "NONE"; score = 0; details = {}

    if direction == "long":
        dist_ef = cur_c - ef
        dist_es = cur_c - es
        near_ef = abs(dist_ef) <= atr_ * PULL_MAX_EMA_DIST_ATR and dist_ef >= -atr_ * 0.2
        near_es = abs(dist_es) <= atr_ * PULL_MAX_EMA_DIST_ATR and dist_es >= -atr_ * 0.2

        if near_ef or near_es:
            if near_ef and rsi_healthy and adx_ok and struct["confirming_mss"]:
                setup_type = "PULL"; score = 3
            elif near_ef and rsi_healthy and adx_ok:
                setup_type = "PULL"; score = 2
            elif near_es and rsi_healthy:
                setup_type = "PULL"; score = 2
            elif (near_ef or near_es) and rsi_healthy:
                setup_type = "PULL"; score = 1
        elif ema_aligned and rsi_healthy and adx_ok:
            if vol_ok and struct["confirming_mss"]:
                setup_type = "CONT"; score = 3
            elif vol_ok:
                setup_type = "CONT"; score = 2
            elif adx >= H4_ADX_MIN and rsi_healthy:
                setup_type = "CONT"; score = 1
        else:
            n = len(c)
            local_high = max(c[max(0, n - 1 - BREAK_LOOKBACK_LOCAL): n - 1]) if n > BREAK_LOOKBACK_LOCAL else None
            major_high = max(c[max(0, n - 1 - BREAK_LOOKBACK_MAJOR): n - 1]) if n > BREAK_LOOKBACK_MAJOR else None
            broke_local = local_high is not None and cur_c > local_high * 1.002
            broke_major = major_high is not None and cur_c > major_high * 1.002
            compression_ok = vcp_data["vcp"] or (atr_pctile is not None and atr_pctile < 0.60)
            if broke_major and compression_ok and adx_ok:
                setup_type = "BREAK"; score = 3
            elif broke_local and compression_ok and adx_ok:
                setup_type = "BREAK"; score = 2
            elif broke_local and adx_ok:
                setup_type = "BREAK"; score = 1

    else:
        dist_ef = ef - cur_c
        dist_es = es - cur_c
        near_ef = abs(dist_ef) <= atr_ * PULL_MAX_EMA_DIST_ATR and dist_ef >= -atr_ * 0.2
        near_es = abs(dist_es) <= atr_ * PULL_MAX_EMA_DIST_ATR and dist_es >= -atr_ * 0.2

        if near_ef or near_es:
            if near_ef and rsi_healthy and adx_ok and struct["confirming_mss"]:
                setup_type = "PULL"; score = 3
            elif near_ef and rsi_healthy and adx_ok:
                setup_type = "PULL"; score = 2
            elif near_es and rsi_healthy:
                setup_type = "PULL"; score = 2
            elif (near_ef or near_es) and rsi_healthy:
                setup_type = "PULL"; score = 1
        elif ema_aligned and rsi_healthy and adx_ok:
            if vol_ok and struct["confirming_mss"]:
                setup_type = "CONT"; score = 3
            elif vol_ok:
                setup_type = "CONT"; score = 2
            elif adx >= H4_ADX_MIN and rsi_healthy:
                setup_type = "CONT"; score = 1
        else:
            n = len(c)
            local_low = min(c[max(0, n - 1 - BREAK_LOOKBACK_LOCAL): n - 1]) if n > BREAK_LOOKBACK_LOCAL else None
            major_low = min(c[max(0, n - 1 - BREAK_LOOKBACK_MAJOR): n - 1]) if n > BREAK_LOOKBACK_MAJOR else None
            broke_local = local_low is not None and cur_c < local_low * 0.998
            broke_major = major_low is not None and cur_c < major_low * 0.998
            compression_ok = vcp_data["vcp"] or (atr_pctile is not None and atr_pctile < 0.60)
            if broke_major and compression_ok and adx_ok:
                setup_type = "BREAK"; score = 3
            elif broke_local and compression_ok and adx_ok:
                setup_type = "BREAK"; score = 2
            elif broke_local and adx_ok:
                setup_type = "BREAK"; score = 1

    in_ob = price_in_ob_zone(cur_c, ob_list, atr_, direction)

    details = {
        "adx": adx, "rsi": rsi_, "ema_fast": ef, "ema_slow": es,
        "atr_val": atr_, "di_plus_4h": di_p, "di_minus_4h": di_m,
        "vol_ratio": cur_v / vm if vm > 0 else None,
        "ema_aligned": ema_aligned, "rsi_healthy": rsi_healthy,
        "adx_ok": adx_ok, "vol_ok": vol_ok, "in_ob": in_ob,
        "struct": struct, "sweep": sweep, "vcp": vcp_data,
    }
    return {"setup_type": setup_type, "score": score, "details": details}


def detect_1h_confirmation(candles_1h: list[dict], direction: str,
                            setup_type: str, symbol: str = "__1H__") -> dict:
    if len(candles_1h) < EMA_SLOW + 5:
        return {"score": 0, "triggers": [], "ema_fast": 0.0, "ema_slow": 0.0,
                "vol_ratio": None, "atr_val": 0.0, "rsi": 50.0}

    ind   = get_cached_indicators(f"{symbol}_1h", "1h", candles_1h)
    c     = ind["c"]; h = ind["h"]; l_ = ind["l"]; v = ind["v"]
    ef    = safe(ind["ema_fast"][-1])
    es    = safe(ind["ema_slow"][-1])
    rsi_  = safe(ind["rsi"][-1], 50.0)
    atr_  = safe(ind["atr"][-1], c[-1] * 0.01)
    vm    = safe(ind["vol_ma"][-1])
    cur_c = c[-1]; cur_v = v[-1]; cur_h = h[-1]; cur_l = l_[-1]
    prev_c = c[-2]; prev_o = ind["o"][-1] if ind["o"] else cur_c

    h1_ema_aligned = (direction == "long" and ef > es) or \
                     (direction == "short" and ef < es)

    rsi_ok = (direction == "long"  and H1_RSI_BULL <= rsi_ < H1_RSI_OB) or \
             (direction == "short" and H1_RSI_OS  < rsi_ <= H1_RSI_BEAR)

    triggers = []
    score    = 0

    body      = abs(cur_c - prev_o)
    candle_rng = cur_h - cur_l
    strong_body = candle_rng > 0 and body / candle_rng >= ENGULF_BODY_RATIO

    rsi_arr = ind["rsi"]
    rsi_prev = safe(rsi_arr[-2], 50.0) if len(rsi_arr) >= 2 else 50.0

    if direction == "long":
        bull_engulf = (cur_c > cur_c and cur_c > prev_c) and strong_body
        rsi_cross   = rsi_prev < H1_RSI_BULL <= rsi_
        ema_cross   = not (len(c) < 2) and (c[-2] < ef) and (cur_c >= ef)
        vol_impulse = vm > 0 and cur_v >= vm * IMPULSE_VOL_MULT

        engulf = strong_body and cur_c > prev_c and (cur_c - cur_l) < atr_ * 0.4
        if rsi_ok and h1_ema_aligned:
            if vol_impulse:
                score = 3; triggers.append("1H RSI+EMA+Vol")
            elif engulf:
                score = 3; triggers.append("1H Engulf+RSI+EMA")
            else:
                score = 2; triggers.append("1H RSI+EMA confirmed")
        elif rsi_ok:
            if rsi_cross:
                score = 2; triggers.append("1H RSI crossed 50")
            elif vol_impulse:
                score = 2; triggers.append("1H Vol+RSI")
            else:
                score = 1; triggers.append("1H RSI ok")
        elif h1_ema_aligned and vol_impulse:
            score = 1; triggers.append("1H EMA+Vol")

    else:
        rsi_cross   = rsi_prev > H1_RSI_BEAR >= rsi_
        vol_impulse = vm > 0 and cur_v >= vm * IMPULSE_VOL_MULT
        engulf      = strong_body and cur_c < prev_c and (cur_h - cur_c) < atr_ * 0.4

        if rsi_ok and h1_ema_aligned:
            if vol_impulse:
                score = 3; triggers.append("1H RSI+EMA+Vol")
            elif engulf:
                score = 3; triggers.append("1H Engulf+RSI+EMA")
            else:
                score = 2; triggers.append("1H RSI+EMA confirmed")
        elif rsi_ok:
            if rsi_cross:
                score = 2; triggers.append("1H RSI crossed 50")
            elif vol_impulse:
                score = 2; triggers.append("1H Vol+RSI")
            else:
                score = 1; triggers.append("1H RSI ok")
        elif h1_ema_aligned and vol_impulse:
            score = 1; triggers.append("1H EMA+Vol")

    return {
        "score":         score,
        "triggers":      triggers,
        "rsi":           rsi_,
        "ema_fast":      ef,
        "ema_slow":      es,
        "h1_ema_aligned": h1_ema_aligned,
        "vol_ratio":     cur_v / vm if vm > 0 else None,
        "atr_val":       atr_,
    }


def score_volume(setup: dict, h1_conf: dict, ind_4h: dict | None = None) -> tuple[int, str]:
    vr_4h       = setup.get("details", {}).get("vol_ratio")
    vr_1h       = h1_conf.get("vol_ratio")
    both_strong = (vr_4h is not None and vr_4h >= 1.5 and
                   vr_1h is not None and vr_1h >= IMPULSE_VOL_MULT)
    one_strong  = (vr_4h is not None and vr_4h >= H4_VOL_MULT) or \
                  (vr_1h is not None and vr_1h >= 1.0)
    rising      = bool(ind_4h) and volume_rising(ind_4h, bars=3)
    if both_strong:
        return 2, f"Volume strong (4H {vr_4h:.1f}x|1H {vr_1h:.1f}x) +2"
    elif one_strong and rising:
        vr = vr_4h or vr_1h
        return 2, f"Volume ok+rising ({vr:.1f}x) +2"
    elif one_strong:
        vr = vr_4h or vr_1h
        return 1, f"Volume ok ({vr:.1f}x) +1"
    else:
        return 0, "Volume weak (0)"


def score_adx(daily: dict, setup: dict, direction: str = "long") -> tuple[int, str]:
    d_adx       = daily.get("adx", 20.0)
    h4_adx      = setup.get("details", {}).get("adx", 20.0)
    di_plus_4h  = safe(setup.get("details", {}).get("di_plus_4h", 25.0))
    di_minus_4h = safe(setup.get("details", {}).get("di_minus_4h", 25.0))
    di_aligned  = (direction == "long"  and di_plus_4h  > di_minus_4h) or \
                  (direction == "short" and di_minus_4h > di_plus_4h)
    if not di_aligned:
        return 0, f"ADX unconfirmed by DI ({di_plus_4h:.0f}+ vs {di_minus_4h:.0f}-)"
    if d_adx >= DAILY_ADX_STRONG and h4_adx >= H4_ADX_MIN:
        return 2, f"ADX strong (1D {d_adx:.0f}|4H {h4_adx:.0f}) +2"
    elif (d_adx >= DAILY_ADX_WEAK and h4_adx >= H4_ADX_MIN) or d_adx >= DAILY_ADX_STRONG:
        return 1, f"ADX ok (1D {d_adx:.0f}|4H {h4_adx:.0f}) +1"
    else:
        return 0, f"ADX weak (1D {d_adx:.0f}|4H {h4_adx:.0f}) (0)"


def check_confluence(setup: dict, h1_result: dict,
                      vol_score: int, orderflow: dict,
                      struct: dict) -> dict:
    factors: list[str] = []

    triggers = h1_result.get("triggers", [])
    if any("crossed" in t.lower() for t in triggers):
        factors.append("rsi_recovery")

    if vol_score >= 1:
        factors.append("volume_expansion")

    if setup.get("details", {}).get("in_ob"):
        factors.append("ob_zone_tap")

    if setup.get("setup_type") == "PULL":
        ema_ref  = setup.get("details", {}).get("ema_fast", 0.0)
        atr_ref  = setup.get("details", {}).get("atr_val", 1.0)
        h1_close = safe(h1_result.get("ema_fast", ema_ref))
        ema_dist = abs(h1_close - ema_ref) / atr_ref if atr_ref > 0 else 999.0
        if ema_dist <= 0.4:
            factors.append("tight_ema_proximity")

    if safe(setup.get("details", {}).get("adx", 0)) >= 25:
        factors.append("adx_confirmed")

    if orderflow.get("of_net", 0) > 0:
        factors.append("orderflow_aligned")

    if struct.get("confirming_mss"):
        factors.append("confirming_mss")

    if setup.get("details", {}).get("sweep", {}).get("type") == "SWEEP":
        factors.append("liquidity_sweep")

    return {"factors": factors, "count": len(factors)}


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
        "score_adjustments", "trigger_list", "symbol",
        "divergence_label", "grade", "confluence_factors",
        "orderflow_data", "session_label", "sweep_level",
    )

    def __init__(self):
        self.fire_long   = False
        self.fire_short  = False
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
        self.oi_data:          dict = {}
        self.btc_regime_label: str  = ""
        self.breadth_label:    str  = ""
        self.rs_data:          dict = {}
        self.wr_data:          dict = {}
        self.macro_data:       dict = {}
        self.funding_rate:  float | None = None
        self.open_interest: float | None = None
        self.spread_pct:    float | None = None
        self.vol_ratio:     float | None = None
        self.score_adjustments: list[tuple[str, int, str]] = []
        self.trigger_list: list[str] = []
        self.symbol: str = ""
        self.divergence_label: str = ""
        self.grade: str = "C"
        self.confluence_factors: list[str] = []
        self.orderflow_data: dict = {}
        self.session_label: str = ""
        self.sweep_level: float | None = None


def compute_signals(symbol: str,
                    candles_1h: list[dict],
                    candles_4h: list[dict],
                    candles_1d: list[dict],
                    state: dict,
                    record_inputs: bool = True,
                    reference_ms: int | None = None,
                    funding_rate: float | None = None) -> SignalResult:
    res = SignalResult()
    res.symbol = symbol

    daily = classify_daily_trend(candles_1d, symbol=symbol)
    daily_class = daily["classification"]
    res.daily_class = daily_class

    if record_inputs:
        ind_4h_full = get_cached_indicators(f"{symbol}_4h", "4h", candles_4h)
        ind_1h_full = get_cached_indicators(f"{symbol}_1h", "1h", candles_1h)

        cur_c_4h = ind_4h_full["c"][-1] if ind_4h_full["c"] else 0.0
        atr_4h   = safe(ind_4h_full["atr"][-1], cur_c_4h * 0.01)
        atr_4h_pct = atr_4h / cur_c_4h * 100 if cur_c_4h > 0 else 0.0
        update_atr_history(state, symbol, atr_4h_pct)

        ef_1h = safe(ind_1h_full["ema_fast"][-1])
        es_1h = safe(ind_1h_full["ema_slow"][-1])
        above_ema50 = ind_1h_full["c"][-1] > es_1h if ind_1h_full["c"] else False
        record_breadth_result(symbol, above_ema50)

        closes_4h = ind_4h_full["c"]
        if len(closes_4h) >= 2:
            ret_pct = (closes_4h[-1] - closes_4h[-20]) / closes_4h[-20] * 100 \
                      if len(closes_4h) >= 20 else 0.0
            record_rs_return(symbol, ret_pct)

    for direction in ["long", "short"]:
        if direction == "long" and not daily["allows_long"]:
            continue
        if direction == "short" and not daily["allows_short"]:
            continue

        atr_history_sym = state.get("atr_history", {}).get(symbol, [])
        setup = detect_4h_setup(candles_4h, daily, direction,
                                 symbol=symbol, atr_history=atr_history_sym)

        if setup["setup_type"] == "NONE":
            continue

        h1_conf = detect_1h_confirmation(candles_1h, direction, setup["setup_type"], symbol=symbol)

        if h1_conf["score"] == 0:
            continue

        ind_4h = get_cached_indicators(f"{symbol}_4h", "4h", candles_4h)
        cur_c  = ind_4h["c"][-1]
        atr_v  = safe(ind_4h["atr"][-1], cur_c * 0.01)
        atr_pct = atr_v / cur_c * 100 if cur_c > 0 else 0.0

        if atr_pct < MIN_ATR_PCT or atr_pct > MAX_ATR_PCT:
            print(f"  [ATR FILTER] {symbol} ATR {atr_pct:.2f}% out of range")
            continue

        div_data = detect_swing_divergence(ind_4h, direction)
        if div_data["divergent"]:
            print(f"  [DIVERGENCE REJECT] {symbol} {direction.upper()} — {div_data['label']}")
            continue

        of_data = compute_orderflow(candles_1h, direction)
        if of_data["hard_reject"]:
            print(f"  [OF HARD REJECT] {symbol} {direction.upper()} — OF net={of_data['of_net']}")
            continue

        if of_data["of_net"] < -2:
            print(f"  [OF SOFT REJECT] {symbol} {direction.upper()} — OF net={of_data['of_net']}")
            continue

        struct  = setup["details"].get("struct", {})
        vol_score, vol_lbl = score_volume(setup, h1_conf, ind_4h)
        adx_score, adx_lbl = score_adx(daily, setup, direction)

        conf = check_confluence(setup, h1_conf, vol_score, of_data, struct)
        if conf["count"] < CONFLUENCE_MIN_FACTORS:
            print(f"  [CONFLUENCE GATE] {symbol} {direction.upper()} — "
                  f"only {conf['count']}/{CONFLUENCE_MIN_FACTORS} factors: {conf['factors']}")
            continue

        daily_score, daily_lbl = score_daily_alignment(daily, direction)
        setup_score = setup["score"]
        h1_score    = h1_conf["score"]
        of_bonus    = max(-3, min(3, of_data["of_net"]))

        base_score = daily_score + setup_score + h1_score + vol_score + adx_score + of_bonus

        if daily["neutral_cap"] and base_score > 10:
            base_score = 10

        if conf["count"] >= CONFLUENCE_BONUS_FACTORS:
            base_score += 1

        if struct.get("confirming_mss"):
            base_score += 1

        res.base_score  = base_score
        res.daily_score = daily_score
        res.setup_score = setup_score
        res.h1_score    = h1_score
        res.vol_score   = vol_score
        res.adx_score   = adx_score
        res.signal_type = (
            "PULL" if "PULL" in setup["setup_type"] else
            "BREAK" if setup["setup_type"] == "BREAK" else
            "CONT"
        )
        res.setup_type         = setup["setup_type"]
        res.direction          = direction
        res.atr_val            = atr_v
        res.atr_pct            = atr_pct
        res.divergence_label   = div_data["label"]
        res.confluence_factors = conf["factors"]
        res.orderflow_data     = of_data
        res.trigger_list       = h1_conf["triggers"]
        res.sweep_level        = setup["details"].get("sweep", {}).get("sweep_level")

        if direction == "long":
            res.fire_long  = True
            res.fire_short = False
        else:
            res.fire_long  = False
            res.fire_short = True

        supports, resistances = find_sr_levels(candles_4h, cur_c, atr_v)
        res.supports    = supports
        res.resistances = resistances

        ref_ms   = reference_ms or int(time.time() * 1000)
        utc_hour = datetime.fromtimestamp(ref_ms / 1000, tz=timezone.utc).hour
        session_map = {
            range(0, 8):   "Asian",
            range(8, 12):  "London",
            range(12, 16): "Overlap",
            range(16, 21): "NY_Afternoon",
            range(21, 24): "Off_Hours",
        }
        session = next((v for rng, v in session_map.items() if utc_hour in rng), "Unknown")
        res.session_label = session

        adjs  = []
        adj   = base_score

        btc_adj, btc_lbl = check_btc_regime_filter(direction, symbol)
        if btc_adj != 0:
            adjs.append((btc_lbl, btc_adj, "btc"))

        rs_data = compute_relative_strength(symbol)
        res.rs_data = rs_data
        if rs_data["score_adj"] != 0:
            adjs.append((rs_data["label"], rs_data["score_adj"], "rs"))

        breadth_adj, breadth_lbl = apply_breadth_adjustment(direction, rs_data.get("rs_pct"))
        res.breadth_label = breadth_lbl
        if breadth_adj != 0:
            adjs.append((breadth_lbl, breadth_adj, "breadth"))

        oi_data = compute_oi_score(state, symbol, direction)
        res.oi_data = oi_data
        if oi_data["score_adj"] != 0:
            adjs.append((oi_data["label"], oi_data["score_adj"], "oi"))

        wr_data = compute_wr_analytics(state, symbol, direction,
                                        res.signal_type, daily_class)
        res.wr_data = wr_data
        if wr_data["score_adj"] != 0:
            adjs.append((wr_data["label"], wr_data["score_adj"], "wr"))

        macro_data = apply_macro_filter(state, atr_pct, reference_ms)
        res.macro_data = macro_data
        if macro_data["hard_suppress"]:
            print(f"  [MACRO SUPPRESS] {symbol} {direction.upper()} — {macro_data['label']}")
            res.fire_long = res.fire_short = False
            return res
        if macro_data["score_adj"] != 0:
            adjs.append((macro_data["label"], macro_data["score_adj"], "macro"))

        if funding_rate is not None:
            res.funding_rate = funding_rate
            update_funding_history(state, symbol, funding_rate)
            headwind = (direction == "long"  and funding_rate > 0) or \
                       (direction == "short" and funding_rate < 0)
            tailwind = not headwind
            if tailwind and abs(funding_rate) >= FUNDING_CARRY_POS_THRESHOLD:
                adjs.append((f"Funding tailwind ({funding_rate*100:+.4f}%/8h)",
                             FUNDING_CARRY_BONUS, "funding"))
            if headwind and abs(funding_rate) >= FUNDING_HEADWIND_THRESHOLD:
                f_trend = get_funding_trend(state, symbol)
                penalty = -2 if f_trend == "rising" else -1
                adjs.append((f"Funding headwind ({funding_rate*100:+.4f}%/8h)",
                             penalty, "funding"))

        total_pos = sum(a for _, a, _ in adjs if a > 0)
        total_neg = sum(a for _, a, _ in adjs if a < 0)
        adj = base_score + min(total_pos, MAX_POSITIVE_ADJUSTMENTS) + \
              max(total_neg, -MAX_NEGATIVE_ADJUSTMENTS)
        res.final_score      = adj
        res.score_adjustments = adjs
        res.btc_regime_label  = btc_lbl

        eff_min = MIN_SIGNAL_SCORE
        btc_regime = get_btc_regime()
        breadth_pct = compute_market_breadth()["breadth_pct"]
        if btc_regime and btc_regime.get("bearish") and breadth_pct > 0.75:
            eff_min += 1

        if adj < eff_min:
            print(f"  [SCORE FILTER] {symbol} {direction.upper()} "
                  f"base={base_score} final={adj} < {eff_min}")
            res.fire_long = res.fire_short = False
            return res

        atr_pctile = get_atr_percentile(state, symbol, atr_pct)
        tp1_m, tp2_m, sl_m = SETUP_TP_SL_MULTS.get(
            res.signal_type, (TP1_MULT_CONT, TP2_MULT_CONT, SL_MULT_CONT)
        )

        if atr_pct > HIGH_ATR_THRESHOLD:
            sl_m = SL_HIGH_ATR_MULT
        if btc_regime:
            if btc_regime.get("bearish"):
                tp1_m *= REGIME_BEAR_TP1_MULT
            if btc_regime.get("bullish"):
                tp2_m *= REGIME_BULL_TP2_MULT
        if atr_pctile is not None and atr_pctile > 0.80:
            sl_m *= REGIME_HIGHVOL_SL_MULT

        mark_px = _get_mid_price(symbol)
        live_px = mark_px if mark_px is not None else cur_c
        limit_offset = atr_v * 0.2
        res.entry = live_px - limit_offset if res.fire_long else live_px + limit_offset
        if res.fire_long:
            res.tp1 = res.entry + atr_v * tp1_m
            res.tp2 = res.entry + atr_v * tp2_m
            atr_based_sl = res.entry - atr_v * sl_m
            res.sl = atr_based_sl
            if supports:
                struct_sl    = max(supports) * 0.998
                sl_dist      = res.entry - struct_sl
                if sl_dist >= atr_v * 0.5:
                    res.sl = max(struct_sl, atr_based_sl)
            if resistances:
                nr      = resistances[0]
                sr_dist = (nr - res.entry) / atr_v
                sl_d    = res.entry - res.sl
                if 0.2 <= sr_dist < tp1_m and sl_d > 0:
                    snapped_rr = (nr - res.entry) / sl_d
                    if snapped_rr >= MIN_RR_RATIO:
                        res.tp1 = nr
        else:
            res.tp1 = res.entry - atr_v * tp1_m
            res.tp2 = res.entry - atr_v * tp2_m
            atr_based_sl = res.entry + atr_v * sl_m
            res.sl = atr_based_sl
            if resistances:
                struct_sl = min(resistances) * 1.002
                sl_dist   = struct_sl - res.entry
                if sl_dist >= atr_v * 0.5:
                    res.sl = min(struct_sl, atr_based_sl)
            if supports:
                ns = supports[0]
                if res.tp2 < ns < res.tp1:
                    res.tp1 = ns

        tp1_dist = abs(res.tp1 - res.entry)
        sl_dist  = abs(res.sl  - res.entry)
        if sl_dist > 0:
            rr = tp1_dist / sl_dist
            if rr < MIN_RR_RATIO:
                print(f"  [RR FILTER] {symbol} {direction.upper()} R:R {rr:.2f} < {MIN_RR_RATIO}")
                res.fire_long = res.fire_short = False
                return res

        if sl_dist > atr_v * 2.5:
            print(f"  [SL TOO WIDE] {symbol} {direction.upper()} SL > 2.5×ATR")
            res.fire_long = res.fire_short = False
            return res

        if adj >= GRADE_A_PLUS_SCORE:
            res.grade = "A+"
        elif adj >= GRADE_A_SCORE:
            res.grade = "A"
        elif adj >= GRADE_B_SCORE:
            res.grade = "B"
        else:
            res.grade = "C"

        return res

    return res


def check_cooldown(state: dict, symbol: str, direction: str,
                   bar_index: int, candidate_score: int = 0) -> bool:
    with _state_lock:
        active       = list(state.get("active_signals", []))
        last_bar     = state.get("signal_cooldowns", {}).get(f"{symbol}_{direction}")
        last_sl_ts   = state.get("post_loss_cooldown", {}).get(f"{symbol}_{direction}")
        prev_outcome = state.get("last_signal_outcome", {}).get(f"{symbol}_{direction}", "")

    active_count = sum(1 for s in active if not s.get("resolved", False))
    if active_count >= MAX_CONCURRENT_ACTIVE:
        print(f"  [MAX CONCURRENT] {hl_coin(symbol)} blocked — {active_count}/{MAX_CONCURRENT_ACTIVE}")
        return False

    for sig in active:
        if (sig.get("symbol") == symbol and sig.get("direction") == direction
                and not sig.get("resolved")):
            # Block re-entry unconditionally until TP or SL is hit.
            # Age-based expiry is handled by check_active_signals, not here.
            print(f"  [ACTIVE BLOCK] {hl_coin(symbol)} {direction.upper()} — "
                  f"signal still open (TP/SL not hit), suppressing duplicate")
            return False

    if last_bar is not None:
        bars = bar_index - last_bar
        if prev_outcome in ("tp1", "tp2"):
            req = SIGNAL_COOLDOWN_POST_WIN
        elif candidate_score >= GRADE_A_SCORE:
            req = 1
        elif candidate_score >= GRADE_B_SCORE:
            req = 2
        else:
            req = SIGNAL_COOLDOWN_1H_BARS
        if bars < req:
            print(f"  [COOLDOWN] {hl_coin(symbol)} {direction.upper()} — {req - bars} bar(s) remaining")
            return False

    if last_sl_ts is not None:
        elapsed = int(time.time()) - last_sl_ts
        sig_type = state.get("last_signal_type", {}).get(f"{symbol}_{direction}", "")
        if sig_type == "PULL" and elapsed < PULL_REENTRY_COOLDOWN_S:
            print(f"  [POST-LOSS COOLDOWN] {hl_coin(symbol)} {direction.upper()} PULL — "
                  f"{PULL_REENTRY_COOLDOWN_S - elapsed}s remaining")
            return False

    return True


def update_cooldown(state: dict, symbol: str, direction: str, bar_index: int):
    with _state_lock:
        state.setdefault("signal_cooldowns", {})[f"{symbol}_{direction}"] = bar_index


def track_signal(state: dict, symbol: str, direction: str,
                 msg_id: int, sig: SignalResult, bar_index: int, hist_id: str | None = None):
    with _state_lock:
        state.setdefault("active_signals", []).append({
            "symbol":          symbol,
            "direction":       direction,
            "msg_id":          msg_id,
            "bar_index":       bar_index,
            "signal_bar_time": bar_index * INTERVAL_MS["1h"],
            "tp1":             sig.tp1,
            "tp2":             sig.tp2,
            "sl":              sig.sl,
            "tp1_hit":         False,
            "entry_touched":   False,
            "resolved":        False,
            "hist_id":         hist_id,
            "signal_type":     sig.signal_type,
            "entry":           sig.entry,
            "atr_val":         sig.atr_val,
        })


def _get_mid_price(symbol: str) -> float | None:
    try:
        cache = get_meta_and_asset_ctxs()
        if cache:
            mark = cache.get(hl_coin(symbol), {}).get("mark_px")
            if mark is not None:
                return float(mark)
    except Exception:
        pass
    return None


def check_active_signals(state: dict, bar_index_now: int,
                          scan_reference_ms: int | None = None):
    with _state_lock:
        signals = list(state.get("active_signals", []))
    if not signals:
        return

    ref_ms = scan_reference_ms or int(time.time() * 1000)
    still_active = []

    for sig in signals:
        age = bar_index_now - sig.get("bar_index", bar_index_now)
        if age > SIGNAL_MAX_AGE_1H_BARS:
            print(f"  [TRACK] {sig['symbol']} expired after {age} 1H bars")
            hist_id = sig.get("hist_id")
            if hist_id:
                update_signal_result(state, hist_id, "expired")
            continue
        if sig.get("resolved"):
            continue

        symbol    = sig["symbol"]
        direction = sig["direction"]
        msg_id    = sig["msg_id"]
        tp1, tp2, sl_ = sig["tp1"], sig["tp2"], sig["sl"]
        tp1_hit       = sig.get("tp1_hit", False)
        entry_touched = sig.get("entry_touched", False)
        entry_price   = sig.get("entry", 0.0)
        atr_val_sig   = sig.get("atr_val", entry_price * 0.01 if entry_price else 0.01)
        entry_tol     = atr_val_sig * 0.55
        last_ts       = sig.get("last_processed_candle_ts",
                                sig.get("signal_bar_time", 0))

        try:
            candles = get_candles(symbol, "1h", N_1H,
                                  start_time_ms=last_ts,
                                  reference_ms=ref_ms)
        except Exception as e:
            print(f"  [TRACK] candle fetch failed for {symbol}: {e}")
            still_active.append(sig)
            continue

        if not candles:
            still_active.append(sig)
            continue

        new = [c for c in candles if c["t"] > last_ts]
        if not new:
            if not entry_touched and entry_price:
                mid = _get_mid_price(symbol)
                if mid is not None and abs(mid - entry_price) <= entry_tol:
                    sig["entry_touched"] = True
            if not sig.get("resolved"):
                still_active.append(sig)
            continue

        last_ts = new[-1]["t"]

        def resolve(result_: str):
            sig["resolved"] = True
            with _state_lock:
                if result_ == "sl":
                    state.setdefault("post_loss_cooldown", {})[f"{symbol}_{direction}"] = int(time.time())
                    state.setdefault("last_signal_type", {})[f"{symbol}_{direction}"] = sig.get("signal_type", "")
                state.setdefault("last_signal_outcome", {})[f"{symbol}_{direction}"] = result_
            hist_id = sig.get("hist_id")
            if hist_id:
                update_signal_result(state, hist_id, result_)

        for bar in new:
            ch = bar["h"]; cl = bar["l"]
            if entry_price and not entry_touched:
                if abs((ch + cl) / 2 - entry_price) <= entry_tol:
                    sig["entry_touched"] = True
                    entry_touched = True

            if direction == "long":
                if not tp1_hit:
                    if ch >= tp1:
                        if entry_touched:
                            react_to_message(msg_id, REACT_TP1)
                            sig["tp1_hit"] = True; tp1_hit = True
                        else:
                            # TP1 ran before entry — price moved without us
                            react_to_message(msg_id, REACT_MISS)
                            resolve("missed"); break
                    elif cl <= sl_:
                        if entry_touched:
                            react_to_message(msg_id, REACT_SL)
                            resolve("sl")
                        else:
                            # SL hit before entry — signal invalidated, not a loss
                            resolve("expired")
                        break
                else:
                    if ch >= tp2:
                        react_to_message(msg_id, REACT_TP2)
                        resolve("tp2"); break
                    if cl <= sl_:
                        resolve("tp1"); break
            else:
                if not tp1_hit:
                    if cl <= tp1:
                        if entry_touched:
                            react_to_message(msg_id, REACT_TP1)
                            sig["tp1_hit"] = True; tp1_hit = True
                        else:
                            # TP1 ran before entry — price moved without us
                            react_to_message(msg_id, REACT_MISS)
                            resolve("missed"); break
                    elif ch >= sl_:
                        if entry_touched:
                            react_to_message(msg_id, REACT_SL)
                            resolve("sl")
                        else:
                            # SL hit before entry — signal invalidated, not a loss
                            resolve("expired")
                        break
                else:
                    if cl <= tp2:
                        react_to_message(msg_id, REACT_TP2)
                        resolve("tp2"); break
                    if ch >= sl_:
                        resolve("tp1"); break

        if not sig.get("resolved"):
            sig["last_processed_candle_ts"] = last_ts
            still_active.append(sig)

    with _state_lock:
        state["active_signals"] = still_active


def send_telegram(text: str) -> int | None:
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    for attempt in range(3):
        try:
            r = requests.post(url, json={
                "chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"
            }, timeout=10)
            r.raise_for_status()
            return r.json()["result"]["message_id"]
        except Exception as e:
            if attempt == 2:
                print(f"[TG ERROR] {e.__class__.__name__}: {str(e)[:200]}")
            time.sleep(2)
    return None


def react_to_message(message_id: int, emoji: str):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/setMessageReaction"
    try:
        r = requests.post(url, json={
            "chat_id": TG_CHAT_ID, "message_id": message_id,
            "reaction": [{"type": "emoji", "emoji": emoji}], "is_big": False,
        }, timeout=10)
        r.raise_for_status()
        print(f"  [REACT] {emoji} → msg_id {message_id}")
    except Exception as e:
        print(f"  [REACT ERROR] {e.__class__.__name__}: {str(e)[:200]}")


def stars(score: int) -> str:
    capped = max(0, min(score, MAX_SCORE))
    filled = min(capped, 8)
    return "★" * filled + "☆" * max(0, 8 - filled)


RANK_MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}


def _leverage_for_risk(atr_pct: float, account_risk_pct: float,
                        signal_type: str | None = None,
                        grade: str = "C") -> float:
    _, _, sl_m = SETUP_TP_SL_MULTS.get(
        signal_type, (TP1_MULT_CONT, TP2_MULT_CONT, SL_MULT_CONT)
    )
    sl_pct = atr_pct * sl_m
    if sl_pct <= 0:
        return 1.0
    grade_max = GRADE_MAX_LEVERAGE.get(grade, 3.0)
    return min(max(1.0, account_risk_pct / sl_pct), grade_max)


def format_signal(symbol: str, sig: SignalResult,
                  engine_tag: str = "V3", rank: int = 0) -> str:
    grade_map = {
        "A+": ("💎", "💎 GRADE A+ (MAX CONVICTION)"),
        "A":  ("🟢" if sig.fire_long else "🔴", "⭐ GRADE A"),
        "B":  ("🔵", "GRADE B"),
    }
    base_emoji, grade_tag = grade_map.get(sig.grade, ("⚪", "GRADE C (monitor)"))

    direction   = "▲ LONG" if sig.fire_long else "▼ SHORT"
    ts          = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    premium_tag = " ⚡ PREMIUM" if sig.final_score >= PREMIUM_SCORE else ""
    max_lev     = GRADE_MAX_LEVERAGE.get(sig.grade, 3.0)
    size_pct    = GRADE_SIZE_PCT.get(sig.grade, 50)
    medal       = RANK_MEDALS.get(rank, "")
    rank_tag    = f"{medal} <b>Priority #{rank}</b>\n" if rank else ""

    def fmt(v):
        if v >= 1000: return f"{v:,.2f}"
        if v >= 1:    return f"{v:.4f}"
        return f"{v:.6f}"

    tp1_dist = abs(sig.tp1 - sig.entry)
    sl_dist  = abs(sig.sl  - sig.entry)
    tp2_dist = abs(sig.tp2 - sig.entry)
    rr       = tp1_dist / sl_dist if sl_dist > 0 else 0.0
    rr2      = tp2_dist / sl_dist if sl_dist > 0 else 0.0

    sr_lines = []
    if sig.resistances:
        vals = "  |  ".join(f"<code>{fmt(r)}</code>" for r in sig.resistances)
        sr_lines.append(f"🔴 Resistance: {vals}")
    if sig.supports:
        vals = "  |  ".join(f"<code>{fmt(s)}</code>" for s in sig.supports)
        sr_lines.append(f"🟢 Support:    {vals}")
    if sig.sweep_level:
        sr_lines.append(f"💧 Sweep level: <code>{fmt(sig.sweep_level)}</code>")
    sr_block = ("\n" + "\n".join(sr_lines) + "\n") if sr_lines else ""

    of = sig.orderflow_data
    of_block = (
        f"\n<b>Orderflow</b> (net {of.get('of_net', 0):+d})\n"
        f"  {of.get('labels', {}).get('delta', '')}\n"
        f"  {of.get('labels', {}).get('cvd', '')}\n"
        f"  {of.get('labels', {}).get('ratio', '')}\n"
    ) if of else ""

    return (
        f"{rank_tag}"
        f"{base_emoji} <b>{direction} [{sig.signal_type}]{premium_tag}  {grade_tag}</b>  {stars(sig.final_score)}\n"
        f"<b>Pair:</b>  {symbol}   |   <b>Daily:</b> {sig.daily_class}   |   <b>Session:</b> {sig.session_label}\n"
        f"\n"
        f"<b>Entry:</b> <code>{fmt(sig.entry)}</code>\n"
        f"<b>TP1:</b>   <code>{fmt(sig.tp1)}</code>  (R:R {rr:.1f})\n"
        f"<b>TP2:</b>   <code>{fmt(sig.tp2)}</code>  (R:R {rr2:.1f})\n"
        f"<b>SL:</b>    <code>{fmt(sig.sl)}</code>   (ATR {sig.atr_pct:.2f}%)\n"
        f"\n"
        f"<b>Leverage:</b> Max {max_lev:.0f}x   <b>Size:</b> {size_pct}%\n"
        f"{sr_block}"
        f"\n<i>Swing Engine {__version__} [1D/4H/1H] • Hyperliquid Perps • {ts}</i>"
    )


def priority_score(sig: SignalResult) -> tuple:
    rs   = sig.rs_data.get("rs_pct") or 0.0
    wr   = sig.wr_data.get("win_rate") or 0.5
    oi_ok = sig.oi_data.get("score_adj", 0) > 0
    return (sig.final_score, round(wr, 2), int(oi_ok), rs, sig.symbol)


def deduplicate_correlated(signals: list[tuple]) -> list[tuple]:
    signals.sort(key=lambda t: priority_score(t[2]), reverse=True)
    seen_syms: set[str] = set()
    deduped: list[tuple] = []
    for sym, dirn, sig in signals:
        key = f"{sym}_{dirn}"
        if key in seen_syms:
            continue
        seen_syms.add(key)
        deduped.append((sym, dirn, sig))
    return deduped


_dynamic_low_btc_corr: set[str] = set()
_dynamic_corr_lock = threading.Lock()
_dynamic_corr_clusters: list[frozenset[str]] = []
_clusters_lock = threading.Lock()


def set_dynamic_corr_clusters(clusters: list[set[str]]):
    global _dynamic_corr_clusters
    with _clusters_lock:
        _dynamic_corr_clusters = [frozenset(c) for c in clusters]


def get_dynamic_corr_clusters() -> list[frozenset[str]]:
    with _clusters_lock:
        return list(_dynamic_corr_clusters)


def compute_pairwise_correlation_matrix(symbols: list[str],
                                         bundles: dict[str, tuple],
                                         candle_idx: int = 1,
                                         lookback: int = LOW_BTC_CORR_LOOKBACK_BARS,
                                         ) -> dict[tuple[str, str], float]:
    returns: dict[str, list[float]] = {}
    for sym in symbols:
        b = bundles.get(sym)
        if not b or len(b) <= candle_idx:
            continue
        candles = b[candle_idx]
        if len(candles) < lookback + 2:
            continue
        closes = [c["c"] for c in candles[-(lookback + 1):]]
        rets   = [(closes[i] - closes[i - 1]) / closes[i - 1]
                  for i in range(1, len(closes)) if closes[i - 1] != 0]
        if len(rets) >= CORR_MATRIX_MIN_SAMPLE:
            returns[sym] = rets

    matrix: dict[tuple[str, str], float] = {}
    syms = sorted(returns.keys())
    for i, a in enumerate(syms):
        for b_sym in syms[i + 1:]:
            ra, rb = returns[a], returns[b_sym]
            n = min(len(ra), len(rb))
            if n < CORR_MATRIX_MIN_SAMPLE:
                continue
            ra_, rb_ = ra[:n], rb[:n]
            ma_ = sum(ra_) / n; mb_ = sum(rb_) / n
            cov = sum((x - ma_) * (y - mb_) for x, y in zip(ra_, rb_)) / n
            va_ = sum((x - ma_) ** 2 for x in ra_) / n
            vb_ = sum((y - mb_) ** 2 for y in rb_) / n
            if va_ <= 0 or vb_ <= 0:
                continue
            matrix[(a, b_sym)] = cov / (math.sqrt(va_) * math.sqrt(vb_))
    return matrix


def cluster_by_correlation(symbols: list[str],
                            matrix: dict[tuple[str, str], float],
                            threshold: float = DYNAMIC_CORR_CLUSTER_THRESHOLD,
                            ) -> list[set[str]]:
    parent = {s: s for s in symbols}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        rx, ry = find(x), find(y)
        if rx != ry:
            parent[rx] = ry

    for (a, b_sym), corr in matrix.items():
        if a in parent and b_sym in parent and corr >= threshold:
            union(a, b_sym)

    clusters: dict[str, set[str]] = {}
    for s in symbols:
        clusters.setdefault(find(s), set()).add(s)
    return list(clusters.values())


def scan_symbol(symbol: str, state: dict, bar_index_now: int,
                candle_bundle: tuple | None,
                ref_ms: int | None = None) -> list[tuple] | None:
    if candle_bundle is None:
        return None

    candles_1h, candles_4h, candles_1d = candle_bundle

    ind_4h = get_cached_indicators(f"{symbol}_4h", "4h", candles_4h)
    if not ind_4h["c"]:
        return None
    cur_c = ind_4h["c"][-1]
    atr_v = safe(ind_4h["atr"][-1], cur_c * 0.01)
    atr_pct = atr_v / cur_c * 100 if cur_c > 0 else 0.0

    mkt = get_market_context(symbol)
    funding_rate = None
    if mkt:
        oi = mkt.get("open_interest")
        funding_rate = mkt.get("funding")
        if oi is not None:
            update_oi_history(state, symbol, oi)
            min_oi = MIN_OI_USD_SMALL_CAP if symbol in SMALL_CAP_PAIRS else MIN_OI_USD
            if oi < min_oi:
                print(f"  [OI FILTER] {hl_coin(symbol)} OI ${oi:,.0f} < ${min_oi:,.0f}")
                return None

        if funding_rate is not None:
            with _state_lock:
                for dirn in ("long", "short"):
                    headwind = (dirn == "long"  and funding_rate > 0) or \
                               (dirn == "short" and funding_rate < 0)
                    if headwind and abs(funding_rate) > FUNDING_SUPPRESS_EXTREME:
                        print(f"  [FUNDING SUPPRESS] {hl_coin(symbol)} {dirn.upper()} — "
                              f"extreme funding {funding_rate*100:+.4f}%/8h")

    result = compute_signals(
        symbol, candles_1h, candles_4h, candles_1d, state,
        record_inputs=True, reference_ms=ref_ms,
        funding_rate=funding_rate,
    )

    signals = []
    for direction in (["long"] if result.fire_long else []) + \
                     (["short"] if result.fire_short else []):
        if not check_cooldown(state, symbol, direction, bar_index_now, result.final_score):
            continue
        signals.append((symbol, direction, result))

    return signals if signals else None


def main():
    global _shutdown

    lock_fd = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("[LOCK] Another instance is running — exiting.")
        sys.exit(1)

    print(f"[INIT] Swing Engine v{__version__} starting…")
    state = load_state()
    reset_rs_cache()
    reset_breadth_cache()
    reset_win_rates_cache()
    clear_indicator_cache()

    ref_ms        = int(time.time() * 1000)
    bar_index_now = ref_ms // INTERVAL_MS["1h"]

    print("[PHASE 0] Checking active signals…")
    check_active_signals(state, bar_index_now, ref_ms)
    save_state(state)

    print(f"[PHASE 1] Fetching candles for {len(WATCHLIST)} symbols…")
    candle_bundles: dict[str, tuple] = {}
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futs = {ex.submit(fetch_all_candles, sym, ref_ms): sym for sym in WATCHLIST}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                bundle = fut.result()
                if bundle:
                    candle_bundles[sym] = bundle
                else:
                    print(f"  [SKIP] {sym} — insufficient candle data")
            except Exception as e:
                print(f"  [ERROR] {sym} candle fetch: {e}")

    finalize_breadth_cache()
    finalize_rs_cache()

    print("[INIT] Computing BTC regime…")
    btc_bundle = candle_bundles.get("BTCUSDT")
    if btc_bundle:
        try:
            c1h, c4h, c1d = btc_bundle
            regime = compute_btc_regime(c1h, c4h, c1d)
            set_btc_regime(regime)
            print(f"  {regime['label']}")
        except Exception as e:
            print(f"  [BTC REGIME] failed: {e}")
    else:
        print("  [BTC REGIME] BTCUSDT unavailable")

    try:
        _matrix   = compute_pairwise_correlation_matrix(WATCHLIST, candle_bundles)
        _clusters = cluster_by_correlation(WATCHLIST, _matrix)
        set_dynamic_corr_clusters(_clusters)
        multi = [sorted(c) for c in _clusters if len(c) > 1]
        if multi:
            print(f"  [CORR] Clusters: {multi}")
    except Exception as e:
        print(f"  [CORR] failed: {e}")
        set_dynamic_corr_clusters([{s} for s in WATCHLIST])

    if _shutdown:
        save_state(state)
        sys.exit(0)

    print("[PHASE 2] Scanning for signals (1D → 4H → 1H)…")
    get_meta_and_asset_ctxs()

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
                print(f"    ERROR processing {sym}: {e}")

    pending.sort(key=lambda t: priority_score(t[2]), reverse=True)
    deduped  = deduplicate_correlated(pending)
    top      = deduped[:MAX_SIGNALS_PER_SCAN]
    dropped  = deduped[MAX_SIGNALS_PER_SCAN:]

    btc_regime   = get_btc_regime()
    breadth_pct  = compute_market_breadth()["breadth_pct"]
    print(f"  [SCAN SUMMARY] {len(deduped)} signal(s) found | "
          f"BTC: {btc_regime['label'] if btc_regime else 'Unknown'} | "
          f"Breadth: {breadth_pct*100:.0f}%")

    if dropped:
        print(f"  [RANK] Dropped {len(dropped)} lower-priority: "
              f"{[f'{hl_coin(s)} {d.upper()}' for s, d, _ in dropped]}")

    fired = 0
    fired_keys: set[str] = set()  # guard against same-scan duplicates
    for rank, (symbol, direction, sig) in enumerate(top, start=1):
        key = f"{symbol}_{direction}"
        if key in fired_keys:
            print(f"  [SAME-SCAN DUP] {hl_coin(symbol)} {direction.upper()} — skipped duplicate in this scan")
            continue
        fired_keys.add(key)
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
                  f"grade={sig.grade} score={sig.final_score}  "
                  f"TP1={sig.tp1:.4f}  TP2={sig.tp2:.4f}  SL={sig.sl:.4f}")
        else:
            print(f"  [TG FAIL] #{rank} {hl_coin(symbol)} — Telegram send failed")
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
