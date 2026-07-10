# ══════════════════════════════════════════════════════════════════════════
#  VANTAGE — Adaptive Institutional Signal Engine
#  v1.0.0
#
#  Philosophy: markets are auctioned by institutions leaving footprints
#  (Order Blocks, Breaker Blocks, Fair Value Gaps, liquidity sweeps).
#  VANTAGE does not run one strategy — it runs an ensemble of specialized
#  engines, each an expert on one market condition, and lets a central
#  Decision Engine pick the best-scored, highest-EV candidate per symbol
#  per scan. Weights, thresholds, and confidence calibration all adapt
#  from realized trade outcomes stored in state.json, bounded so learning
#  refines the system instead of overfitting to a short sample.
#
#  Engine roster: SMC Liquidity Reversal, Order/Breaker Block Continuation,
#  Trend Pullback, Momentum Breakout, Volatility Expansion, Range Mean-
#  Reversion. Every candidate is scored on Location, Context, Confluence,
#  Volatility/Volume fitness, Multi-timeframe alignment, and Expected
#  Value before the Decision Engine ranks and (optionally) fires it.
#
#  Runtime: single scan-per-invocation, safe for GitHub Actions cron
#  (every 15 min), Hyperliquid REST only, state.json is the only memory.
# ══════════════════════════════════════════════════════════════════════════

from __future__ import annotations

import os
import sys
import json
import math
import time
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Any

import requests

ENGINE_NAME = "VANTAGE"
__version__ = "1.0.0"

# ══════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
if not TG_BOT_TOKEN:
    raise RuntimeError("TG_BOT_TOKEN environment variable is required")
if not TG_CHAT_ID:
    raise RuntimeError("TG_CHAT_ID environment variable is required")

STATE_FILE = os.getenv("STATE_FILE", "state.json")
SCAN_WORKERS = int(os.getenv("SCAN_WORKERS", "4"))
HL_BASE_URL = "https://api.hyperliquid.xyz/info"
HL_MIN_INTERVAL_S = float(os.getenv("HL_MIN_INTERVAL_S", "0.12"))
REQUEST_TIMEOUT_S = 12

WATCHLIST = [
    "BTCUSDT", "ETHUSDT", "HYPEUSDT", "ZECUSDT", "NEARUSDT",
    "ONDOUSDT", "SUIUSDT", "PENGUUSDT", "BNBUSDT", "SOLUSDT",
    "TRXUSDT", "BCHUSDT", "DOGEUSDT", "ADAUSDT", "DOTUSDT",
    "TAOUSDT", "AVAXUSDT", "LINKUSDT", "AAVEUSDT", "XRPUSDT",
    "XLMUSDT", "UNIUSDT", "LTCUSDT", "APTUSDT", "PENDLEUSDT",
]
MAJORS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"}

# ── TIMEFRAME STACK (15m minimum, per mandate) ─────────────────────────────
# MACRO 1D  -> bias, premium/discount range, PDH/PDL/PWH/PWL
# HTF   4H  -> primary structure, Order Block / Breaker Block zone map
# MID   1H  -> liquidity sweep + intermediate BOS/CHoCH
# LTF   15m -> execution trigger: MSS confirmation, entry timing
TF_MACRO, TF_HTF, TF_MID, TF_LTF = "1d", "4h", "1h", "15m"
TF_BARS = {TF_MACRO: 150, TF_HTF: 300, TF_MID: 320, TF_LTF: 320}
SCAN_INTERVAL_MIN = 15

EMA_FAST, EMA_SLOW, EMA_TREND = 21, 50, 200
RSI_LEN, ATR_LEN, ADX_LEN, BB_LEN = 14, 14, 14, 20
BB_MULT = 2.0

OB_DISPLACEMENT_ATR_MULT = 1.15
OB_BOS_LOOKBACK = 25
FVG_MIN_GAP_ATR_MULT = 0.12
ZONE_MAX_WIDTH_ATR_MULT = 1.8
ZONE_LOOKBACK_HTF = 90
ZONE_LOOKBACK_LTF = 80
VOL_PROFILE_BINS = 24

BASE_CONFIDENCE_THRESHOLD = 62.0
MIN_RR = 1.6
MAX_SIGNALS_PER_SYMBOL_PER_DAY = 3
COOLDOWN_BARS_LTF = 4
DUPLICATE_PRICE_TOL_PCT = 0.006

DAILY_SUMMARY_HOUR_UTC = 8

# ══════════════════════════════════════════════════════════════════════════
#  SHUTDOWN / SIGNAL HANDLING
# ══════════════════════════════════════════════════════════════════════════

_SHUTDOWN = False


def _handle_shutdown(sig_num, frame):
    global _SHUTDOWN
    _SHUTDOWN = True


try:
    import signal as _os_signal
    _os_signal.signal(_os_signal.SIGTERM, _handle_shutdown)
    _os_signal.signal(_os_signal.SIGINT, _handle_shutdown)
except Exception:
    pass

# ══════════════════════════════════════════════════════════════════════════
#  HYPERLIQUID API CLIENT — rate limited, cached, retried
# ══════════════════════════════════════════════════════════════════════════


def hl_coin(symbol: str) -> str:
    return symbol.replace("USDT", "").replace("USD", "")


class _RateLimiter:
    """Throttles outbound requests to stay safely within Hyperliquid weight limits."""

    def __init__(self, min_interval_s: float):
        self.min_interval_s = min_interval_s
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            delta = now - self._last
            if delta < self.min_interval_s:
                time.sleep(self.min_interval_s - delta)
            self._last = time.monotonic()


_LIMITER = _RateLimiter(HL_MIN_INTERVAL_S)


def hl_post(payload: dict, retries: int = 4, timeout: int = REQUEST_TIMEOUT_S) -> Any:
    backoff = 0.5
    last_err = None
    for attempt in range(retries):
        _LIMITER.wait()
        try:
            resp = requests.post(HL_BASE_URL, json=payload, timeout=timeout)
            if resp.status_code == 429:
                time.sleep(backoff)
                backoff = min(backoff * 2, 8.0)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:  # network hiccup, malformed body, timeout
            last_err = e
            time.sleep(backoff)
            backoff = min(backoff * 2, 8.0)
    print(f"[hl_post] failed after {retries} retries: {last_err}", file=sys.stderr)
    return None


def interval_ms(interval: str) -> int:
    unit = interval[-1]
    n = int(interval[:-1])
    mult = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}[unit]
    return n * mult


def get_candles(symbol: str, interval: str, n: int) -> list[dict]:
    coin = hl_coin(symbol)
    now_ms = int(time.time() * 1000)
    step = interval_ms(interval)
    start_ms = now_ms - step * (n + 3)
    payload = {
        "type": "candleSnapshot",
        "req": {"coin": coin, "interval": interval, "startTime": start_ms, "endTime": now_ms},
    }
    data = hl_post(payload)
    if not isinstance(data, list):
        return []
    candles = []
    for c in data:
        try:
            candles.append({
                "t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]),
                "l": float(c["l"]), "c": float(c["c"]), "v": float(c.get("v", 0.0)),
            })
        except (KeyError, TypeError, ValueError):
            continue
    candles.sort(key=lambda x: x["t"])
    # drop the still-forming last bar so all analysis operates on closed candles only
    if candles and candles[-1]["t"] + step > now_ms:
        candles = candles[:-1]
    return candles[-n:]


def fetch_bundle(symbol: str) -> dict[str, list[dict]] | None:
    bundle = {}
    for tf in (TF_MACRO, TF_HTF, TF_MID, TF_LTF):
        candles = get_candles(symbol, tf, TF_BARS[tf])
        if len(candles) < 40:
            return None
        bundle[tf] = candles
    return bundle


def get_meta_and_ctx() -> tuple[list[str], list[dict]] | None:
    data = hl_post({"type": "metaAndAssetCtxs"})
    if not isinstance(data, list) or len(data) < 2:
        return None
    universe = [a["name"] for a in data[0].get("universe", [])]
    return universe, data[1]


def get_market_snapshot() -> dict[str, dict]:
    """Shared single call: funding, open interest, mark price for every coin."""
    result = get_meta_and_ctx()
    if not result:
        return {}
    universe, ctxs = result
    snap = {}
    for name, ctx in zip(universe, ctxs):
        try:
            snap[name] = {
                "mark": float(ctx.get("markPx", 0.0)),
                "funding": float(ctx.get("funding", 0.0)),
                "oi": float(ctx.get("openInterest", 0.0)),
                "day_vol": float(ctx.get("dayNtlVlm", 0.0)),
            }
        except (TypeError, ValueError):
            continue
    return snap


# ══════════════════════════════════════════════════════════════════════════
#  INDICATORS
# ══════════════════════════════════════════════════════════════════════════


def _safe(v, fb=0.0):
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return fb
    return v


def ema(vals: list[float], period: int) -> list[float]:
    if not vals:
        return []
    k = 2.0 / (period + 1)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def sma(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        lo = max(0, i - period + 1)
        window = vals[lo:i + 1]
        out.append(sum(window) / len(window))
    return out


def stdev(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        lo = max(0, i - period + 1)
        window = vals[lo:i + 1]
        m = sum(window) / len(window)
        var = sum((x - m) ** 2 for x in window) / len(window)
        out.append(math.sqrt(var))
    return out


def rsi(closes: list[float], period: int = RSI_LEN) -> list[float]:
    if len(closes) < 2:
        return [50.0] * len(closes)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g = sum(gains[1:period + 1]) / period if len(gains) > period else sum(gains) / max(len(gains), 1)
    avg_l = sum(losses[1:period + 1]) / period if len(losses) > period else sum(losses) / max(len(losses), 1)
    out = [50.0] * min(period, len(closes))
    for i in range(period, len(closes)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs = avg_g / avg_l if avg_l > 1e-12 else 100.0
        out.append(100 - (100 / (1 + rs)))
    while len(out) < len(closes):
        out.append(out[-1] if out else 50.0)
    return out[:len(closes)]


def atr(highs, lows, closes, period: int = ATR_LEN) -> list[float]:
    trs = []
    for i in range(len(closes)):
        if i == 0:
            trs.append(highs[i] - lows[i])
        else:
            trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    out = [trs[0]]
    for i in range(1, len(trs)):
        out.append((out[-1] * (period - 1) + trs[i]) / period)
    return out


def adx_dmi(highs, lows, closes, period: int = ADX_LEN) -> tuple[list[float], list[float], list[float]]:
    n = len(closes)
    plus_dm = [0.0] * n
    minus_dm = [0.0] * n
    trs = [0.0] * n
    for i in range(1, n):
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm[i] = up if (up > down and up > 0) else 0.0
        minus_dm[i] = down if (down > up and down > 0) else 0.0
        trs[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
    atr_s = [trs[0]]
    pdm_s = [plus_dm[0]]
    mdm_s = [minus_dm[0]]
    for i in range(1, n):
        atr_s.append((atr_s[-1] * (period - 1) + trs[i]) / period)
        pdm_s.append((pdm_s[-1] * (period - 1) + plus_dm[i]) / period)
        mdm_s.append((mdm_s[-1] * (period - 1) + minus_dm[i]) / period)
    plus_di = [100 * pdm_s[i] / atr_s[i] if atr_s[i] > 1e-12 else 0.0 for i in range(n)]
    minus_di = [100 * mdm_s[i] / atr_s[i] if atr_s[i] > 1e-12 else 0.0 for i in range(n)]
    dx = [100 * abs(plus_di[i] - minus_di[i]) / (plus_di[i] + minus_di[i]) if (plus_di[i] + minus_di[i]) > 1e-12 else 0.0 for i in range(n)]
    adx = [dx[0]]
    for i in range(1, n):
        adx.append((adx[-1] * (period - 1) + dx[i]) / period)
    return adx, plus_di, minus_di


def bollinger_width_pct(closes: list[float], period: int = BB_LEN, mult: float = BB_MULT) -> list[float]:
    mid = sma(closes, period)
    sd = stdev(closes, period)
    return [(2 * mult * sd[i]) / mid[i] * 100 if mid[i] > 1e-12 else 0.0 for i in range(len(closes))]


def compute_indicators(candles: list[dict]) -> dict:
    closes = [c["c"] for c in candles]
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
    ind = {
        "ema_fast": ema(closes, EMA_FAST),
        "ema_slow": ema(closes, EMA_SLOW),
        "ema_trend": ema(closes, EMA_TREND),
        "rsi": rsi(closes, RSI_LEN),
        "atr": atr(highs, lows, closes, ATR_LEN),
        "bb_width": bollinger_width_pct(closes, BB_LEN, BB_MULT),
    }
    adx, pdi, mdi = adx_dmi(highs, lows, closes, ADX_LEN)
    ind["adx"], ind["plus_di"], ind["minus_di"] = adx, pdi, mdi
    return ind


# ══════════════════════════════════════════════════════════════════════════
#  STATE PERSISTENCE
# ══════════════════════════════════════════════════════════════════════════


def _default_state() -> dict:
    return {
        "open_signals": [],
        "closed_signals": [],
        "engine_stats": {},
        "cooldowns": {},
        "recent_prices": {},
        "atr_pct_memory": {},
        "threshold": BASE_CONFIDENCE_THRESHOLD,
        "last_daily_summary_date": None,
        "signal_seq": 0,
        "last_scan_ts": None,
    }


def load_state() -> dict:
    path = Path(STATE_FILE)
    if not path.exists():
        return _default_state()
    try:
        data = json.loads(path.read_text())
        base = _default_state()
        base.update(data)
        return base
    except Exception as e:
        print(f"[state] failed to load, starting fresh: {e}", file=sys.stderr)
        return _default_state()


def save_state(state: dict):
    tmp = Path(STATE_FILE + ".tmp")
    tmp.write_text(json.dumps(state, indent=2, default=str))
    tmp.replace(STATE_FILE)


def prune_state(state: dict, max_closed: int = 1000, max_days: int = 45):
    cutoff = time.time() - max_days * 86400
    state["closed_signals"] = [
        s for s in state["closed_signals"] if s.get("closed_ts", cutoff) >= cutoff
    ][-max_closed:]


# ══════════════════════════════════════════════════════════════════════════
#  MARKET REGIME
# ══════════════════════════════════════════════════════════════════════════


@dataclass
class RegimeVector:
    symbol: str
    trend_dir: str          # "up" | "down" | "flat"
    trend_strength: float   # 0-100 (ADX based)
    volatility_pctile: float  # 0-100, relative to symbol's own recent history
    is_choppy: bool
    session_weight: float
    breadth: float           # -1..1 market-wide bias agreement


def session_weight_now() -> float:
    hour = datetime.now(timezone.utc).hour
    # London/NY overlap and NY session get full weight; late Asia session dampened
    if 12 <= hour <= 20:
        return 1.0
    if 6 <= hour < 12 or 20 < hour <= 23:
        return 0.9
    return 0.75


def update_atr_pct_memory(state: dict, symbol: str, atr_pct: float) -> float:
    mem = state["atr_pct_memory"].setdefault(symbol, [])
    mem.append(atr_pct)
    if len(mem) > 200:
        mem.pop(0)
    sorted_mem = sorted(mem)
    rank = sum(1 for x in sorted_mem if x <= atr_pct)
    return 100.0 * rank / len(sorted_mem)


def compute_noise_index(candles: list[dict], lookback: int = 30) -> float:
    """Ratio of net displacement to total path length; low = choppy, high = trending."""
    window = candles[-lookback:]
    if len(window) < 5:
        return 50.0
    net = abs(window[-1]["c"] - window[0]["c"])
    path = sum(abs(window[i]["c"] - window[i - 1]["c"]) for i in range(1, len(window)))
    if path < 1e-9:
        return 50.0
    return 100.0 * net / path


def build_regime_vector(state: dict, symbol: str, bundle: dict, breadth: float) -> RegimeVector:
    htf_ind = compute_indicators(bundle[TF_HTF])
    closes = [c["c"] for c in bundle[TF_HTF]]
    ema_fast, ema_slow, ema_trend = htf_ind["ema_fast"][-1], htf_ind["ema_slow"][-1], htf_ind["ema_trend"][-1]
    adx_val = htf_ind["adx"][-1]

    if ema_fast > ema_slow > ema_trend and closes[-1] > ema_fast:
        trend_dir = "up"
    elif ema_fast < ema_slow < ema_trend and closes[-1] < ema_fast:
        trend_dir = "down"
    else:
        trend_dir = "flat"

    atr_pct = 100 * htf_ind["atr"][-1] / closes[-1] if closes[-1] > 1e-9 else 0.0
    vol_pctile = update_atr_pct_memory(state, symbol, atr_pct)
    noise = compute_noise_index(bundle[TF_HTF])
    is_choppy = adx_val < 18 and noise < 35

    return RegimeVector(
        symbol=symbol, trend_dir=trend_dir, trend_strength=_safe(adx_val),
        volatility_pctile=vol_pctile, is_choppy=is_choppy,
        session_weight=session_weight_now(), breadth=breadth,
    )


def compute_breadth(bias_by_symbol: dict[str, str]) -> float:
    if not bias_by_symbol:
        return 0.0
    ups = sum(1 for b in bias_by_symbol.values() if b == "up")
    downs = sum(1 for b in bias_by_symbol.values() if b == "down")
    total = len(bias_by_symbol)
    return (ups - downs) / total


# ══════════════════════════════════════════════════════════════════════════
#  MARKET STRUCTURE: SWINGS, BOS/CHoCH
# ══════════════════════════════════════════════════════════════════════════


@dataclass
class Swing:
    index: int
    price: float
    kind: str  # "high" | "low"


def find_swings(candles: list[dict], left: int = 2, right: int = 2) -> list[Swing]:
    swings = []
    n = len(candles)
    for i in range(left, n - right):
        window_h = [candles[j]["h"] for j in range(i - left, i + right + 1)]
        window_l = [candles[j]["l"] for j in range(i - left, i + right + 1)]
        if candles[i]["h"] == max(window_h) and window_h.count(candles[i]["h"]) == 1:
            swings.append(Swing(i, candles[i]["h"], "high"))
        if candles[i]["l"] == min(window_l) and window_l.count(candles[i]["l"]) == 1:
            swings.append(Swing(i, candles[i]["l"], "low"))
    return swings


@dataclass
class StructureState:
    bias: str            # "bullish" | "bearish" | "neutral"
    last_bos_index: int
    last_choch_index: int
    last_swing_high: Optional[float]
    last_swing_low: Optional[float]


def analyze_structure(candles: list[dict], swings: list[Swing]) -> StructureState:
    if len(swings) < 4:
        return StructureState("neutral", -1, -1, None, None)
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    bias = "neutral"
    last_bos, last_choch = -1, -1
    if len(highs) >= 2 and len(lows) >= 2:
        hh = highs[-1].price > highs[-2].price
        hl = lows[-1].price > lows[-2].price
        lh = highs[-1].price < highs[-2].price
        ll = lows[-1].price < lows[-2].price
        if hh and hl:
            bias = "bullish"
            last_bos = highs[-1].index
        elif lh and ll:
            bias = "bearish"
            last_bos = lows[-1].index
        elif hh and ll:
            bias = "bullish"
            last_choch = lows[-1].index
        elif lh and hl:
            bias = "bearish"
            last_choch = highs[-1].index
    return StructureState(
        bias=bias, last_bos_index=last_bos, last_choch_index=last_choch,
        last_swing_high=highs[-1].price if highs else None,
        last_swing_low=lows[-1].price if lows else None,
    )


# ══════════════════════════════════════════════════════════════════════════
#  SMC ZONES: ORDER BLOCKS -> BREAKER BLOCKS, FAIR VALUE GAPS
# ══════════════════════════════════════════════════════════════════════════


@dataclass
class Zone:
    kind: str          # "OB" | "BB" | "FVG"
    direction: str      # "bullish" | "bearish"
    top: float
    bottom: float
    index: int
    mitigated: bool = False
    broken: bool = False   # closed straight through -> becomes a Breaker Block


def find_order_blocks(candles: list[dict], atr_vals: list[float], lookback: int = 60) -> list[Zone]:
    """A displacement candle immediately preceding a BOS defines the OB;
    every OB tracked forward is reclassified as a Breaker Block the moment
    price closes back through it in the opposite direction."""
    zones: list[Zone] = []
    n = len(candles)
    start = max(1, n - lookback)
    for i in range(start, n - 1):
        body = abs(candles[i]["c"] - candles[i]["o"])
        if atr_vals[i] <= 1e-9 or body / atr_vals[i] < OB_DISPLACEMENT_ATR_MULT:
            continue
        bullish_disp = candles[i]["c"] > candles[i]["o"]
        lookback_hi = max(candles[j]["h"] for j in range(max(0, i - OB_BOS_LOOKBACK), i))
        lookback_lo = min(candles[j]["l"] for j in range(max(0, i - OB_BOS_LOOKBACK), i))
        if bullish_disp and candles[i]["c"] > lookback_hi:
            base_idx = i - 1
            while base_idx > 0 and candles[base_idx]["c"] > candles[base_idx]["o"]:
                base_idx -= 1
            top, bottom = candles[base_idx]["h"], candles[base_idx]["l"]
            if atr_vals[i] > 0 and (top - bottom) / atr_vals[i] <= ZONE_MAX_WIDTH_ATR_MULT:
                zones.append(Zone("OB", "bullish", top, bottom, base_idx))
        elif not bullish_disp and candles[i]["c"] < lookback_lo:
            base_idx = i - 1
            while base_idx > 0 and candles[base_idx]["c"] < candles[base_idx]["o"]:
                base_idx -= 1
            top, bottom = candles[base_idx]["h"], candles[base_idx]["l"]
            if atr_vals[i] > 0 and (top - bottom) / atr_vals[i] <= ZONE_MAX_WIDTH_ATR_MULT:
                zones.append(Zone("OB", "bearish", top, bottom, base_idx))
    return zones


def reclassify_breakers(zones: list[Zone], candles: list[dict]) -> list[Zone]:
    """Walk forward from each OB's origin; the first close fully through the
    zone in the opposite direction flips it into a Breaker Block."""
    n = len(candles)
    for z in zones:
        for i in range(z.index + 1, n):
            c = candles[i]
            if z.direction == "bullish" and c["c"] < z.bottom:
                z.broken = True
                z.direction = "bearish"
                z.kind = "BB"
                break
            if z.direction == "bearish" and c["c"] > z.top:
                z.broken = True
                z.direction = "bullish"
                z.kind = "BB"
                break
        # mitigation: price returned into the (possibly reclassified) zone at least once
        for i in range(max(z.index + 1, 0), n):
            c = candles[i]
            if c["l"] <= z.top and c["h"] >= z.bottom:
                z.mitigated = True
                break
    return zones


def find_fvgs(candles: list[dict], atr_vals: list[float], lookback: int = 60) -> list[Zone]:
    zones = []
    n = len(candles)
    start = max(2, n - lookback)
    for i in range(start, n):
        a, b, c = candles[i - 2], candles[i - 1], candles[i]
        if atr_vals[i] <= 1e-9:
            continue
        if c["l"] > a["h"] and (c["l"] - a["h"]) / atr_vals[i] >= FVG_MIN_GAP_ATR_MULT:
            zones.append(Zone("FVG", "bullish", c["l"], a["h"], i))
        if c["h"] < a["l"] and (a["l"] - c["h"]) / atr_vals[i] >= FVG_MIN_GAP_ATR_MULT:
            zones.append(Zone("FVG", "bearish", a["l"], c["h"], i))
    for z in zones:
        for i in range(z.index + 1, n):
            if candles[i]["l"] <= z.top and candles[i]["h"] >= z.bottom:
                z.mitigated = True
                break
    return zones


def cluster_levels(levels: list[float], tol_pct: float = 0.0015) -> list[tuple[float, int]]:
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


def build_liquidity_pools(swings: list[Swing]) -> dict:
    highs = [s.price for s in swings if s.kind == "high"]
    lows = [s.price for s in swings if s.kind == "low"]
    return {"buy_side": cluster_levels(highs), "sell_side": cluster_levels(lows)}


def detect_sweep(candles: list[dict], pools: dict, direction: str, lookback: int = 10) -> Optional[dict]:
    """direction 'bullish' = looking for a sell-side liquidity sweep (wick below a pool, close back above)."""
    window = candles[-lookback:]
    targets = pools["sell_side"] if direction == "bullish" else pools["buy_side"]
    for level, weight in targets:
        for c in window:
            if direction == "bullish" and c["l"] < level <= c["c"]:
                return {"level": level, "weight": weight, "wick_low": c["l"]}
            if direction == "bearish" and c["h"] > level >= c["c"]:
                return {"level": level, "weight": weight, "wick_high": c["h"]}
    return None


def premium_discount_zone(candles: list[dict], lookback: int = 50) -> dict:
    window = candles[-lookback:]
    hi = max(c["h"] for c in window)
    lo = min(c["l"] for c in window)
    mid = (hi + lo) / 2
    price = candles[-1]["c"]
    if hi <= lo:
        pct = 50.0
    else:
        pct = 100 * (price - lo) / (hi - lo)
    zone = "premium" if pct > 55 else ("discount" if pct < 45 else "equilibrium")
    return {"high": hi, "low": lo, "mid": mid, "pct": pct, "zone": zone}


def detect_mss(candles_exec: list[dict], direction: str, lookback: int = 30) -> Optional[dict]:
    """Market Structure Shift on the LTF: a close beyond the most recent
    opposing swing point, used as the execution trigger."""
    swings = find_swings(candles_exec[-lookback:], left=1, right=1)
    if not swings:
        return None
    offset = len(candles_exec) - lookback
    price = candles_exec[-1]["c"]
    if direction == "bullish":
        recent_highs = [s.price + offset for s in swings if s.kind == "high"]
        prior_highs = [s.price for s in swings if s.kind == "high"]
        if prior_highs and price > max(prior_highs):
            return {"level": max(prior_highs), "bar": len(candles_exec) - 1}
    else:
        prior_lows = [s.price for s in swings if s.kind == "low"]
        if prior_lows and price < min(prior_lows):
            return {"level": min(prior_lows), "bar": len(candles_exec) - 1}
    return None


# ══════════════════════════════════════════════════════════════════════════
#  CANDIDATE MODEL
# ══════════════════════════════════════════════════════════════════════════


@dataclass
class Candidate:
    symbol: str
    engine: str
    direction: str  # "long" | "short"
    entry: float
    sl: float
    tp1: float
    tp2: float
    rr1: float
    rr2: float
    reasons: list[str] = field(default_factory=list)
    raw_scores: dict = field(default_factory=dict)
    confidence: float = 0.0
    ev: float = 0.0


def adaptive_sl_buffer(atr_val: float, vol_pctile: float) -> float:
    # widen the buffer in high-volatility regimes to survive wick sweeps
    mult = 0.25 + 0.35 * (vol_pctile / 100.0)
    return atr_val * mult


def clamp_rr(cand: Candidate) -> Candidate:
    risk = abs(cand.entry - cand.sl)
    if risk <= 1e-9:
        cand.rr1 = cand.rr2 = 0.0
        return cand
    cand.rr1 = abs(cand.tp1 - cand.entry) / risk
    cand.rr2 = abs(cand.tp2 - cand.entry) / risk
    return cand


# ══════════════════════════════════════════════════════════════════════════
#  SPECIALIZED ENGINES (candidate builders)
# ══════════════════════════════════════════════════════════════════════════


def build_liquidity_reversal(symbol: str, bundle: dict, regime: RegimeVector) -> Optional[Candidate]:
    """SMC engine: HTF OB/BB zone + liquidity sweep + LTF MSS confirmation."""
    htf_candles, ltf_candles = bundle[TF_HTF], bundle[TF_LTF]
    htf_ind = compute_indicators(htf_candles)
    htf_swings = find_swings(htf_candles)
    struct = analyze_structure(htf_candles, htf_swings)
    zones = find_order_blocks(htf_candles, htf_ind["atr"], ZONE_LOOKBACK_HTF)
    zones = reclassify_breakers(zones, htf_candles)
    pools = build_liquidity_pools(htf_swings)
    pd = premium_discount_zone(htf_candles)
    price = ltf_candles[-1]["c"]

    for direction, want_zone_dir, want_pd in (("long", "bullish", "discount"), ("short", "bearish", "premium")):
        if struct.bias not in ("bullish", "bearish", "neutral"):
            continue
        sweep = detect_sweep(bundle[TF_MID], pools, direction)
        if not sweep:
            continue
        active_zones = [z for z in zones if z.direction == want_zone_dir and not z.mitigated
                        and z.bottom <= price <= z.top * 1.02 or z.bottom * 0.98 <= price <= z.top]
        if not active_zones:
            continue
        mss = detect_mss(ltf_candles, want_zone_dir, lookback=30)
        if not mss:
            continue
        zone = min(active_zones, key=lambda z: abs((z.top + z.bottom) / 2 - price))
        atr_val = htf_ind["atr"][-1]
        buf = adaptive_sl_buffer(atr_val, regime.volatility_pctile)
        entry = price
        if direction == "long":
            sl = zone.bottom - buf
            risk = entry - sl
            tp1 = entry + risk * 1.8
            tp2 = entry + risk * 3.0
        else:
            sl = zone.top + buf
            risk = sl - entry
            tp1 = entry - risk * 1.8
            tp2 = entry - risk * 3.0
        reasons = [
            f"{zone.kind} {zone.direction} zone reclaim", "liquidity sweep confirmed",
            "LTF MSS trigger", f"HTF structure: {struct.bias}", f"price in {pd['zone']}",
        ]
        cand = Candidate(symbol, "Liquidity Reversal", direction, entry, sl, tp1, tp2, 0, 0, reasons)
        cand = clamp_rr(cand)
        cand.raw_scores = {
            "location": 90.0 if pd["zone"] == want_pd else 60.0,
            "context": 85.0 if struct.bias == want_zone_dir[:len(struct.bias)] or struct.bias != "neutral" else 55.0,
            "confluence": 80.0 + min(10, sweep["weight"] * 3),
            "mtf": 75.0,
        }
        return cand
    return None


def build_ob_breaker_continuation(symbol: str, bundle: dict, regime: RegimeVector) -> Optional[Candidate]:
    """Trend-following: pullback into an HTF OB (or reclaimed Breaker) in the
    direction of the prevailing trend, LTF Breaker Block as the entry trigger."""
    if regime.trend_dir == "flat" or regime.is_choppy:
        return None
    htf_candles, ltf_candles = bundle[TF_HTF], bundle[TF_LTF]
    htf_ind = compute_indicators(htf_candles)
    ltf_ind = compute_indicators(ltf_candles)
    zones = find_order_blocks(htf_candles, htf_ind["atr"], ZONE_LOOKBACK_HTF)
    zones = reclassify_breakers(zones, htf_candles)
    want_dir = "bullish" if regime.trend_dir == "up" else "bearish"
    direction = "long" if regime.trend_dir == "up" else "short"
    price = ltf_candles[-1]["c"]
    ltf_zones = find_order_blocks(ltf_candles, ltf_ind["atr"], ZONE_LOOKBACK_LTF)
    ltf_zones = reclassify_breakers(ltf_zones, ltf_candles)
    trigger_zones = [z for z in ltf_zones if z.kind == "BB" and z.direction == want_dir
                     and z.bottom <= price <= z.top]
    htf_zones_nearby = [z for z in zones if z.direction == want_dir and z.bottom * 0.985 <= price <= z.top * 1.015]
    if not trigger_zones and not htf_zones_nearby:
        return None
    atr_val = htf_ind["atr"][-1]
    buf = adaptive_sl_buffer(atr_val, regime.volatility_pctile)
    zone_pool = trigger_zones or htf_zones_nearby
    zone = min(zone_pool, key=lambda z: abs((z.top + z.bottom) / 2 - price))
    entry = price
    if direction == "long":
        sl = zone.bottom - buf
        risk = entry - sl
        tp1, tp2 = entry + risk * 1.7, entry + risk * 2.8
    else:
        sl = zone.top + buf
        risk = sl - entry
        tp1, tp2 = entry - risk * 1.7, entry - risk * 2.8
    reasons = [f"trend {regime.trend_dir} (ADX {regime.trend_strength:.0f})", f"{zone.kind} pullback entry"]
    cand = Candidate(symbol, "OB/Breaker Continuation", direction, entry, sl, tp1, tp2, 0, 0, reasons)
    cand = clamp_rr(cand)
    cand.raw_scores = {
        "location": 78.0, "context": min(95.0, 55 + regime.trend_strength),
        "confluence": 75.0 if trigger_zones else 65.0, "mtf": 80.0,
    }
    return cand


def build_momentum_breakout(symbol: str, bundle: dict, regime: RegimeVector) -> Optional[Candidate]:
    """Volatility-expansion engine: range compression followed by an ADX/BB
    expansion breakout with volume confirmation."""
    mid_candles = bundle[TF_MID]
    ind = compute_indicators(mid_candles)
    bb_width = ind["bb_width"]
    if len(bb_width) < 20:
        return None
    compression = min(bb_width[-20:-3]) if len(bb_width) > 23 else bb_width[-1]
    expanding = bb_width[-1] > compression * 1.4 and ind["adx"][-1] > 20
    if not expanding:
        return None
    closes = [c["c"] for c in mid_candles]
    vols = [c["v"] for c in mid_candles]
    avg_vol = sum(vols[-20:-1]) / max(len(vols[-20:-1]), 1)
    vol_confirm = vols[-1] > avg_vol * 1.3
    if not vol_confirm:
        return None
    direction = "long" if closes[-1] > closes[-2] and ind["plus_di"][-1] > ind["minus_di"][-1] else \
        ("short" if closes[-1] < closes[-2] and ind["minus_di"][-1] > ind["plus_di"][-1] else None)
    if not direction:
        return None
    atr_val = ind["atr"][-1]
    entry = closes[-1]
    buf = adaptive_sl_buffer(atr_val, regime.volatility_pctile)
    recent_range_low = min(c["l"] for c in mid_candles[-8:])
    recent_range_high = max(c["h"] for c in mid_candles[-8:])
    if direction == "long":
        sl = recent_range_low - buf
        risk = entry - sl
        tp1, tp2 = entry + risk * 1.6, entry + risk * 2.6
    else:
        sl = recent_range_high + buf
        risk = sl - entry
        tp1, tp2 = entry - risk * 1.6, entry - risk * 2.6
    reasons = ["volatility expansion after compression", "volume confirmation", f"ADX {ind['adx'][-1]:.0f}"]
    cand = Candidate(symbol, "Momentum Breakout", direction, entry, sl, tp1, tp2, 0, 0, reasons)
    cand = clamp_rr(cand)
    cand.raw_scores = {"location": 65.0, "context": 70.0, "confluence": 70.0, "mtf": 60.0}
    return cand


def build_range_mean_reversion(symbol: str, bundle: dict, regime: RegimeVector) -> Optional[Candidate]:
    """Range engine: only active when the regime is genuinely non-trending;
    fades extremes back toward equilibrium with RSI + Bollinger confluence."""
    if not regime.is_choppy:
        return None
    mid_candles = bundle[TF_MID]
    ind = compute_indicators(mid_candles)
    closes = [c["c"] for c in mid_candles]
    rsi_v = ind["rsi"][-1]
    pd = premium_discount_zone(mid_candles, lookback=40)
    direction = None
    if pd["zone"] == "premium" and rsi_v > 68:
        direction = "short"
    elif pd["zone"] == "discount" and rsi_v < 32:
        direction = "long"
    if not direction:
        return None
    entry = closes[-1]
    atr_val = ind["atr"][-1]
    buf = adaptive_sl_buffer(atr_val, regime.volatility_pctile)
    if direction == "long":
        sl = pd["low"] - buf
        risk = entry - sl
        tp1, tp2 = pd["mid"], pd["high"]
    else:
        sl = pd["high"] + buf
        risk = sl - entry
        tp1, tp2 = pd["mid"], pd["low"]
    reasons = [f"range {pd['zone']} extreme", f"RSI {rsi_v:.0f}", "mean reversion to equilibrium"]
    cand = Candidate(symbol, "Range Mean-Reversion", direction, entry, sl, tp1, tp2, 0, 0, reasons)
    cand = clamp_rr(cand)
    cand.raw_scores = {"location": 72.0, "context": 60.0, "confluence": 68.0, "mtf": 55.0}
    return cand


ENGINE_BUILDERS = {
    "Liquidity Reversal": build_liquidity_reversal,
    "OB/Breaker Continuation": build_ob_breaker_continuation,
    "Momentum Breakout": build_momentum_breakout,
    "Range Mean-Reversion": build_range_mean_reversion,
}

DEFAULT_ENGINE_WEIGHT = 1.0


# ══════════════════════════════════════════════════════════════════════════
#  DECISION ENGINE — adaptive weighting, confidence, expected value
# ══════════════════════════════════════════════════════════════════════════


def logistic(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def get_engine_weight(state: dict, engine_name: str) -> float:
    stats = state["engine_stats"].get(engine_name)
    if not stats or stats.get("n", 0) < 8:
        return DEFAULT_ENGINE_WEIGHT
    win_rate = stats["wins"] / max(stats["n"], 1)
    # bounded adaptive nudge: +/-25% around baseline, centered on 50% win rate
    return max(0.75, min(1.25, 1.0 + (win_rate - 0.5) * 0.5))


def score_candidate(cand: Candidate, regime: RegimeVector, state: dict) -> Candidate:
    s = cand.raw_scores
    base = (
        s.get("location", 60) * 0.28
        + s.get("context", 60) * 0.27
        + s.get("confluence", 60) * 0.25
        + s.get("mtf", 60) * 0.20
    )
    rr_bonus = min(15.0, (cand.rr2 - MIN_RR) * 6.0) if cand.rr2 > MIN_RR else -20.0
    session_adj = (regime.session_weight - 0.85) * 10
    breadth_adj = 6.0 * regime.breadth if cand.direction == "long" else -6.0 * regime.breadth
    vol_fit = 8.0 if 25 <= regime.volatility_pctile <= 85 else -6.0
    engine_weight = get_engine_weight(state, cand.engine)
    raw = (base + rr_bonus + session_adj + breadth_adj + vol_fit) * engine_weight
    confidence = max(0.0, min(99.0, raw))
    cand.confidence = confidence
    # expected value proxy assuming calibrated confidence as win probability
    p = confidence / 100.0
    cand.ev = p * cand.rr2 - (1 - p) * 1.0
    return cand


def tune_engine_stats(state: dict):
    """Recompute rolling per-engine win/loss counts from closed_signals."""
    stats: dict[str, dict] = {}
    for s in state["closed_signals"][-500:]:
        eng = s.get("engine", "unknown")
        d = stats.setdefault(eng, {"n": 0, "wins": 0})
        d["n"] += 1
        if s.get("result") in ("tp1", "tp2"):
            d["wins"] += 1
    state["engine_stats"] = stats


def governor_adjust_threshold(state: dict):
    """Nudge the global confidence threshold from realized recent win rate,
    bounded so a short losing/winning streak cannot cause runaway drift."""
    recent = state["closed_signals"][-40:]
    if len(recent) < 15:
        state["threshold"] = BASE_CONFIDENCE_THRESHOLD
        return
    wins = sum(1 for s in recent if s.get("result") in ("tp1", "tp2"))
    win_rate = wins / len(recent)
    delta = (0.5 - win_rate) * 20.0
    new_threshold = BASE_CONFIDENCE_THRESHOLD + delta
    state["threshold"] = max(BASE_CONFIDENCE_THRESHOLD - 8, min(BASE_CONFIDENCE_THRESHOLD + 8, new_threshold))


def passes_hard_filters(cand: Candidate, threshold: float) -> tuple[bool, str]:
    if cand.rr2 < MIN_RR:
        return False, f"RR {cand.rr2:.2f} below minimum {MIN_RR}"
    if cand.confidence < threshold:
        return False, f"confidence {cand.confidence:.1f} below threshold {threshold:.1f}"
    if cand.sl == cand.entry:
        return False, "degenerate stop-loss"
    return True, ""


def check_cooldown(state: dict, symbol: str, direction: str) -> bool:
    key = f"{symbol}:{direction}"
    last_bar = state["cooldowns"].get(key)
    if last_bar is None:
        return True
    return (time.time() - last_bar) >= COOLDOWN_BARS_LTF * 15 * 60


def update_cooldown(state: dict, symbol: str, direction: str):
    state["cooldowns"][f"{symbol}:{direction}"] = time.time()


def is_recent_duplicate(state: dict, symbol: str, direction: str, entry: float) -> bool:
    for s in state["open_signals"]:
        if s["symbol"] == symbol and s["direction"] == direction:
            if abs(s["entry"] - entry) / max(entry, 1e-9) <= DUPLICATE_PRICE_TOL_PCT:
                return True
    return False


def signals_today_for_symbol(state: dict, symbol: str) -> int:
    cutoff = time.time() - 86400
    return sum(
        1 for s in state["open_signals"] + state["closed_signals"]
        if s.get("symbol") == symbol and s.get("opened_ts", 0) >= cutoff
    )


# ══════════════════════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════════════════════

TG_API = f"https://api.telegram.org/bot{TG_BOT_TOKEN}"


def tg_escape(value) -> str:
    text = str(value)
    for ch in r"_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


def fmt_px(v: float) -> str:
    if v >= 100:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:.4f}"
    return f"{v:.6f}"


def confidence_bar(confidence: float) -> str:
    filled = int(round(confidence / 10))
    return "█" * filled + "░" * (10 - filled)


def tg_send(text: str, reply_to: Optional[int] = None) -> Optional[int]:
    payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "MarkdownV2"}
    if reply_to:
        payload["reply_to_message_id"] = reply_to
    try:
        resp = requests.post(f"{TG_API}/sendMessage", json=payload, timeout=10)
        data = resp.json()
        if data.get("ok"):
            return data["result"]["message_id"]
        print(f"[telegram] send failed: {data}", file=sys.stderr)
    except Exception as e:
        print(f"[telegram] exception: {e}", file=sys.stderr)
    return None


def tg_react(message_id: int, emoji: str):
    try:
        requests.post(f"{TG_API}/setMessageReaction", json={
            "chat_id": TG_CHAT_ID, "message_id": message_id,
            "reaction": [{"type": "emoji", "emoji": emoji}],
        }, timeout=10)
    except Exception:
        pass


def format_signal_message(cand: Candidate, seq: int) -> str:
    arrow = "🟢 LONG" if cand.direction == "long" else "🔴 SHORT"
    lines = [
        f"*{tg_escape(ENGINE_NAME)}* v{tg_escape(__version__)} — signal \\#{seq}",
        f"{tg_escape(cand.symbol)}  {tg_escape(arrow)}",
        f"_{tg_escape(cand.engine)}_",
        "",
        f"Entry: `{tg_escape(fmt_px(cand.entry))}`",
        f"SL: `{tg_escape(fmt_px(cand.sl))}`",
        f"TP1: `{tg_escape(fmt_px(cand.tp1))}`  \\(RR {tg_escape(f'{cand.rr1:.2f}')}\\)",
        f"TP2: `{tg_escape(fmt_px(cand.tp2))}`  \\(RR {tg_escape(f'{cand.rr2:.2f}')}\\)",
        "",
        f"Confidence: {tg_escape(f'{cand.confidence:.0f}')}/100 {tg_escape(confidence_bar(cand.confidence))}",
        f"EV: {tg_escape(f'{cand.ev:+.2f}R')}",
        "",
        "Reasons:",
    ]
    for r in cand.reasons:
        lines.append(f"• {tg_escape(r)}")
    return "\n".join(lines)


def send_signal_alert(cand: Candidate, state: dict) -> int:
    state["signal_seq"] += 1
    seq = state["signal_seq"]
    msg = format_signal_message(cand, seq)
    mid = tg_send(msg)
    return seq, mid


def send_update(open_sig: dict, event: str):
    labels = {
        "activated": ("Activated", "👀"), "tp1": ("TP1 hit", "✅"), "tp2": ("TP2 hit — closed", "🎯"),
        "sl": ("Stop loss hit", "🛑"), "breakeven": ("Moved to break-even", "🔒"),
        "closed": ("Closed", "🏁"), "cancelled": ("Cancelled", "🚫"),
    }
    label, emoji = labels.get(event, (event, "ℹ️"))
    text = f"*{tg_escape(open_sig['symbol'])}* \\#{tg_escape(open_sig['seq'])} — {tg_escape(label)}"
    tg_send(text, reply_to=open_sig.get("message_id"))
    if open_sig.get("message_id"):
        tg_react(open_sig["message_id"], emoji)


def send_daily_summary(state: dict):
    closed_24h = [s for s in state["closed_signals"] if s.get("closed_ts", 0) >= time.time() - 86400]
    if not closed_24h:
        body = "No completed trades in the last 24h\\."
    else:
        wins = sum(1 for s in closed_24h if s.get("result") in ("tp1", "tp2"))
        win_rate = 100 * wins / len(closed_24h)
        avg_r = sum(s.get("r_multiple", 0.0) for s in closed_24h) / len(closed_24h)
        by_engine: dict[str, list] = {}
        for s in closed_24h:
            by_engine.setdefault(s.get("engine", "unknown"), []).append(s)
        best_engine = max(by_engine.items(), key=lambda kv: sum(1 for x in kv[1] if x.get("result") in ("tp1", "tp2")) / len(kv[1]))
        lines = [
            f"Trades: {len(closed_24h)}  Win rate: {win_rate:.0f}%  Avg R: {avg_r:+.2f}",
            f"Best performing engine: {tg_escape(best_engine[0])}",
            f"Current confidence threshold: {state['threshold']:.1f}",
        ]
        body = "\n".join(tg_escape(l) if False else l for l in lines)
    text = f"*{tg_escape(ENGINE_NAME)} — Daily Summary*\n\n{body}"
    tg_send(text)


# ══════════════════════════════════════════════════════════════════════════
#  TRADE LIFECYCLE MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════


def manage_open_signals(state: dict, latest_prices: dict[str, float]):
    still_open = []
    for sig in state["open_signals"]:
        price = latest_prices.get(sig["symbol"])
        if price is None:
            still_open.append(sig)
            continue
        direction = sig["direction"]
        hit_tp1 = (direction == "long" and price >= sig["tp1"]) or (direction == "short" and price <= sig["tp1"])
        hit_tp2 = (direction == "long" and price >= sig["tp2"]) or (direction == "short" and price <= sig["tp2"])
        hit_sl = (direction == "long" and price <= sig["sl"]) or (direction == "short" and price >= sig["sl"])

        if not sig.get("tp1_hit") and hit_tp1:
            sig["tp1_hit"] = True
            sig["sl"] = sig["entry"]  # move to break-even after TP1
            send_update(sig, "tp1")
            send_update(sig, "breakeven")

        if sig.get("tp1_hit") and hit_tp2:
            sig["result"] = "tp2"
            sig["closed_ts"] = time.time()
            sig["r_multiple"] = sig["rr2"]
            send_update(sig, "tp2")
            state["closed_signals"].append(sig)
            continue

        if hit_sl:
            sig["result"] = "sl" if not sig.get("tp1_hit") else "breakeven_stop"
            sig["closed_ts"] = time.time()
            sig["r_multiple"] = 0.0 if sig.get("tp1_hit") else -1.0
            send_update(sig, "sl" if not sig.get("tp1_hit") else "closed")
            state["closed_signals"].append(sig)
            continue

        still_open.append(sig)
    state["open_signals"] = still_open


# ══════════════════════════════════════════════════════════════════════════
#  SYMBOL SCAN PIPELINE
# ══════════════════════════════════════════════════════════════════════════


def scan_symbol(symbol: str) -> tuple[str, Optional[dict], Optional[list[Candidate]]]:
    try:
        bundle = fetch_bundle(symbol)
        if not bundle:
            return symbol, None, None
        return symbol, bundle, None
    except Exception as e:
        print(f"[scan_symbol] {symbol} error: {e}", file=sys.stderr)
        return symbol, None, None


def build_candidates_for_symbol(symbol: str, bundle: dict, regime: RegimeVector) -> list[Candidate]:
    candidates = []
    for name, builder in ENGINE_BUILDERS.items():
        try:
            cand = builder(symbol, bundle, regime)
            if cand:
                candidates.append(cand)
        except Exception as e:
            print(f"[{name}] {symbol} error: {e}", file=sys.stderr)
    return candidates


def run_scan():
    state = load_state()
    tune_engine_stats(state)
    governor_adjust_threshold(state)

    snapshot = get_market_snapshot()
    latest_prices = {sym: snapshot.get(hl_coin(sym), {}).get("mark", 0.0) for sym in WATCHLIST}
    latest_prices = {k: v for k, v in latest_prices.items() if v}

    manage_open_signals(state, latest_prices)

    bundles: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as pool:
        futures = {pool.submit(scan_symbol, sym): sym for sym in WATCHLIST}
        for fut in as_completed(futures):
            symbol, bundle, _ = fut.result()
            if bundle:
                bundles[symbol] = bundle
            if _SHUTDOWN:
                break

    # first pass: bias per symbol for market breadth
    bias_by_symbol = {}
    for sym, bundle in bundles.items():
        closes = [c["c"] for c in bundle[TF_HTF]]
        ema_f, ema_s = ema(closes, EMA_FAST)[-1], ema(closes, EMA_SLOW)[-1]
        bias_by_symbol[sym] = "up" if ema_f > ema_s else ("down" if ema_f < ema_s else "flat")
    breadth = compute_breadth(bias_by_symbol)

    ranked: list[Candidate] = []
    for sym, bundle in bundles.items():
        regime = build_regime_vector(state, sym, bundle, breadth)
        candidates = build_candidates_for_symbol(sym, bundle, regime)
        for cand in candidates:
            cand = score_candidate(cand, regime, state)
            ranked.append(cand)

    ranked.sort(key=lambda c: c.ev, reverse=True)

    fired = 0
    for cand in ranked:
        if signals_today_for_symbol(state, cand.symbol) >= MAX_SIGNALS_PER_SYMBOL_PER_DAY:
            continue
        if not check_cooldown(state, cand.symbol, cand.direction):
            continue
        if is_recent_duplicate(state, cand.symbol, cand.direction, cand.entry):
            continue
        ok, reason = passes_hard_filters(cand, state["threshold"])
        if not ok:
            continue

        seq, message_id = send_signal_alert(cand, state)
        state["open_signals"].append({
            "seq": seq, "symbol": cand.symbol, "engine": cand.engine, "direction": cand.direction,
            "entry": cand.entry, "sl": cand.sl, "tp1": cand.tp1, "tp2": cand.tp2,
            "rr1": cand.rr1, "rr2": cand.rr2, "confidence": cand.confidence,
            "opened_ts": time.time(), "message_id": message_id, "tp1_hit": False,
        })
        update_cooldown(state, cand.symbol, cand.direction)
        fired += 1

    today = datetime.now(timezone.utc).date().isoformat()
    if datetime.now(timezone.utc).hour >= DAILY_SUMMARY_HOUR_UTC and state.get("last_daily_summary_date") != today:
        send_daily_summary(state)
        state["last_daily_summary_date"] = today

    prune_state(state)
    state["last_scan_ts"] = time.time()
    save_state(state)
    print(f"[run_scan] symbols={len(bundles)} candidates={len(ranked)} fired={fired} threshold={state['threshold']:.1f}")


if __name__ == "__main__":
    run_scan()
