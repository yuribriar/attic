#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ------------------------------------------------------------
#  PHOENIX v1.0.0 – Ultra‑Adaptive Intraday & Swing Signal Engine
# ------------------------------------------------------------
# A next‑generation crypto signal engine that fuses multi‑timeframe
# price, volume, funding‑rate and open‑interest analysis with a
# regime‑aware confidence scoring system.  It self‑balances signal
# quality vs. frequency by tightening or relaxing filters according to
# real‑time market volatility and liquidity, while never sacrificing
# institutional‑grade risk‑/‑reward.
#
# Required dependencies (install via pip):
#   pip install pandas numpy requests python-telegram-bot
#
# ----------------------------------------------------------------
#  Configuration (environment variables)
# ----------------------------------------------------------------
#   HL_INFO_URL          – Hyperliquid “info” endpoint (default see code)
#   TG_BOT_TOKEN        – Telegram bot token
#   TG_CHAT_ID          – Telegram chat / channel ID
#   SCAN_WORKERS        – Number of parallel workers for candle fetches
#   DRY_RUN             – Set to "1" to run without sending Telegram
#   PHOENIX_START_EQUITY – Starting portfolio equity in USD (default 100_000)
# ----------------------------------------------------------------

import os
import json
import time
import logging
import signal
import threading
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd
import numpy as np

# ----------------------------------------------------------------
#  Global constants (mostly sourced from the reference engines)
# ----------------------------------------------------------------
# API endpoints
HL_INFO_URL = os.getenv("HL_INFO_URL", "https://api.hyperliquid.xyz/info")
HL_REST_URL = os.getenv("HL_REST_URL", "https://api.hyperliquid.xyz")
WS_URL = os.getenv("HL_WS_URL", "wss://api.hyperliquid.xyz/ws")

# Engine metadata
VERSION = "1.0.0"
ENGINE_NAME = "PHOENIX"

# Watchlist (identical to all reference engines)
WATCHLIST = [
    "BTC", "ETH", "SOL", "AVAX", "ARB", "OP", "MATIC", "LINK",
    "DOGE", "SUI", "APT", "NEAR", "LTC", "BNB", "XRP", "INJ",
]

# Time‑frame definitions (ms)
INTERVAL_MS = {
    "15m": 15 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
}
TF_EXEC = "15m"          # scan frequency (matches the cron schedule)
TF_CONFIRM = "1h"
TF_BIAS = "4h"

# Technical indicator defaults
EMA_FAST = 21
EMA_SLOW = 55
EMA_TREND = 200
RSI_LEN = 14
ATR_LEN = 14
ADX_LEN = 14
BB_LEN = 20
BB_MULT = 2.0

# Risk / portfolio limits
MAX_CONCURRENT = 10
DAILY_LOSS_LIMIT_PCT = 0.03          # stop trading if >3 % loss in UTC day
MAX_RISK_PER_TRADE = 0.01           # 1 % of portfolio equity per trade
MIN_OI_USD = 500_000.0              # liquidity filter
MIN_VOLUME_USD = 200_000.0          # volume filter (used only for sanity)

# Fees / slippage (used in back‑testing)
FEE_TAKER = 0.00045
FEE_MAKER = 0.00015
SLIPPAGE_EST = 0.0006

# ----------------------------------------------------------------
#  Logging configuration
# ----------------------------------------------------------------
log_file = os.getenv("PHOENIX_LOG_PATH", "phoenix_engine.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(ENGINE_NAME)

# ----------------------------------------------------------------
#  State management (identical pattern to the reference engines)
# ----------------------------------------------------------------
STATE_PATH = os.getenv(
    "PHOENIX_STATE_PATH",
    os.path.join(os.path.dirname(__file__), "state.json")
)
STATE_VERSION = 1
_state_lock = threading.Lock()


def load_state() -> Dict[str, Any]:
    """Load or initialise persistent engine state."""
    fresh = {
        "_version": STATE_VERSION,
        "daily": {},                 # per‑UTC‑day loss tracking
        "active_signals": [],        # currently open positions
        "signal_history": [],        # closed signals for analysis
        "signal_cooldowns": {},      # per‑symbol cooldown timestamps
    }
    for path in (STATE_PATH, f"{STATE_PATH}.bak"):
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    s = json.load(f)
                if s.get("_version") != STATE_VERSION:
                    logger.info("State version mismatch in %s – resetting.", path)
                    continue
                fresh.update(s)
                if path != STATE_PATH:
                    logger.info("Loaded state from backup %s", path)
                return fresh
            except Exception as e:
                logger.warning("Failed to read state %s: %s", path, e)
    logger.info("Starting with fresh state.")
    return fresh


def save_state(state: Dict[str, Any]) -> None:
    """Atomic write of state to disk."""
    with _state_lock:
        tmp = f"{STATE_PATH}.tmp"
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, STATE_PATH)


# ----------------------------------------------------------------
#  Hyperliquid API helpers
# ----------------------------------------------------------------
_session = requests.Session()
_session.headers.update({"Content-Type": "application/json"})


def hl_post(payload: Dict[str, Any], timeout: float = 10.0) -> Optional[Dict]:
    """Robust POST wrapper with exponential back‑off on 429."""
    url = f"{HL_REST_URL}/public"
    for attempt in range(5):
        try:
            resp = _session.post(url, json=payload, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            if resp.status_code == 429:
                wait = float(resp.headers.get("Retry-After", 2 ** attempt))
                logger.warning("Rate limited (429); waiting %.1fs", wait)
                time.sleep(wait)
            else:
                logger.error("HTTP error %s: %s", resp.status_code, e)
                break
        except Exception as e:
            logger.warning("Request failure (attempt %d): %s", attempt + 1, e)
            time.sleep(0.5 * (attempt + 1))
    return None


def fetch_candles(symbol: str, tf: str, reference_ms: int) -> Optional[pd.DataFrame]:
    """Return a DataFrame of OHLCV candles for `symbol` at timeframe `tf`."""
    payload = {
        "type": "candle",
        "ticker": symbol,
        "interval": tf,
        "from": reference_ms - INTERVAL_MS[tf] * 500,  # ~500 bars back
        "to": reference_ms,
    }
    data = hl_post(payload)
    if not data or "candles" not in data:
        logger.info("No candle data for %s %s", symbol, tf)
        return None
    df = pd.DataFrame(data["candles"])
    df.rename(columns={"t": "timestamp", "o": "open", "c": "close",
                       "h": "high", "l": "low", "v": "volume"}, inplace=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df


def fetch_funding_and_oi(symbol: str) -> Tuple[Optional[float], Optional[float]]:
    """Return (funding_rate, open_interest_usd) for a symbol."""
    payload = {"type": "metaAndAssetCtxs"}
    data = hl_post(payload)
    if not data:
        return None, None
    universe, asset_ctxs = data[0].get("universe", []), data[1]
    for asset, ctx in zip(universe, asset_ctxs):
        if asset.get("name") == symbol:
            fr = float(ctx.get("funding", 0.0))
            oi = float(ctx.get("openInterest", 0.0))
            price = float(ctx.get("markPx", 0.0))
            return fr, oi * price
    return None, None


# ----------------------------------------------------------------
#  Technical indicator suite (pandas‑based)
# ----------------------------------------------------------------
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Attach EMA, ATR, RSI, ADX, Bollinger values to the candle DataFrame."""
    df = df.copy()
    df["ema_fast"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()
    df["ema_trend"] = df["close"].ewm(span=EMA_TREND, adjust=False).mean()

    # ATR
    delta = df["close"].diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.ewm(span=ATR_LEN, adjust=False).mean()
    roll_down = down.ewm(span=ATR_LEN, adjust=False).mean()
    df["atr"] = roll_up + roll_down

    # RSI
    gain = up.rolling(RSI_LEN).sum()
    loss = down.rolling(RSI_LEN).sum()
    rs = gain / loss.replace(to_replace=0, method="ffill")
    df["rsi"] = 100 - (100 / (1 + rs))

    # ADX (simplified)
    high_low = df["high"] - df["low"]
    high_close = np.abs(df["high"] - df["close"].shift())
    low_close = np.abs(df["low"] - df["close"].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    dm_pos = (df["high"] - df["high"].shift()).where(
        (df["high"] - df["high"].shift()) > (df["low"].shift() - df["low"]), 0)
    )
    dm_neg = (df["low"].shift() - df["low"]).where(
        (df["low"].shift() - df["low"]) > (df["high"] - df["high"].shift()), 0
    )
    di_pos = 100 * (dm_pos.ewm(alpha=1/ADX_LEN).mean() / df["atr"])
    di_neg = 100 * (dm_neg.ewm(alpha=1/ADX_LEN).mean() / df["atr"])
    dx = 100 * (np.abs(di_pos - di_neg) / (di_pos + di_neg)).replace(to_replace=0, method="ffill")
    df["adx"] = dx.ewm(alpha=1/ADX_LEN).mean()

    # Bollinger Bands
    ma = df["close"].rolling(BB_LEN).mean()
    std = df["close"].rolling(BB_LEN).std()
    df["bb_upper"] = ma + BB_MULT * std
    df["bb_lower"] = ma - BB_MULT * std
    return df


# ----------------------------------------------------------------
#  Market‑regime detector
# ----------------------------------------------------------------
def detect_regime(df_1h: pd.DataFrame) -> str:
    """Return regime label: bull, bear, high_vol, low_vol, ranging."""
    vol = df_1h["atr"].iloc[-1] / df_1h["close"].iloc[-1]
    price_change = (df_1h["close"].iloc[-1] - df_1h["close"].iloc[0]) / df_1h["close"].iloc[0]

    if vol > 0.03:
        regime = "high_vol"
    elif vol < 0.008:
        regime = "low_vol"
    else:
        regime = "ranging"

    if price_change > 0.02:
        regime = "bull"
    elif price_change < -0.02:
        regime = "bear"
    return regime


# ----------------------------------------------------------------
#  Scoring & confidence model
# ----------------------------------------------------------------
def compute_score(
    df_15m: pd.DataFrame,
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    funding: Optional[float],
    oi_usd: Optional[float],
    regime: str,
) -> Tuple[float, List[str]]:
    """Return a 0‑100 confidence score and a list of confluence tags."""
    score = 0.0
    tags: List[str] = []

    # Trend family – EMA crossover + ADX strength
    ema_cross = df_15m["ema_fast"].iloc[-1] > df_15m["ema_slow"].iloc[-1]
    adx = df_15m["adx"].iloc[-1]
    if ema_cross and adx > 25:
        score += 20
        tags.append("trend_up")
    elif not ema_cross and adx > 25:
        score += 10
        tags.append("trend_down")

    # Momentum family – RSI + Bollinger position
    rsi = df_15m["rsi"].iloc[-1]
    price_above_bb = df_15m["close"].iloc[-1] > df_15m["bb_upper"].iloc[-1]
    price_below_bb = df_15m["close"].iloc[-1] < df_15m["bb_lower"].iloc[-1]
    if rsi < 30 and price_below_bb:
        score += 15
        tags.append("oversold")
    elif rsi > 70 and price_above_bb:
        score += 15
        tags.append("overbought")

    # Volatility & regime influence
    if regime == "high_vol":
        score -= 10
        tags.append("high_vol")
    elif regime == "low_vol":
        score += 5
        tags.append("low_vol")

    # Liquidity filter
    if oi_usd is not None and oi_usd < MIN_OI_USD:
        score -= 15
        tags.append("low_oi")
    else:
        score += 5

    # Funding‑rate (derivatives) contribution
    if funding is not None:
        if funding > 0.0005:
            score += 8
            tags.append("funding_long")
        elif funding < -0.0005:
            score += 8
            tags.append("funding_short")
        else:
            tags.append("funding_neutral")
    else:
        tags.append("funding_missing")

    # Regime‑specific boost
    if regime in {"bull", "bear"}:
        score += 5

    final_score = max(0.0, min(100.0, score))
    return final_score, tags


# ----------------------------------------------------------------
#  Signal construction (entry/SL/TP logic)
# ----------------------------------------------------------------
def build_signal(
    symbol: str,
    direction: str,
    df: pd.DataFrame,
    confidence: float,
    tags: List[str],
) -> Dict[str, Any]:
    """Create a fully‑specified trade signal."""
    price = df["close"].iloc[-1]
    atr = df["atr"].iloc[-1]

    # Entry is the latest close
    entry = price

    # SL – ATR‑based, opposite side
    sl = entry - (1.5 * atr) if direction == "long" else entry + (1.5 * atr)

    # TP targets – 2× and 3× RR based on SL distance
    rr_target = 2.0
    tp1 = entry + (rr_target * (entry - sl)) if direction == "long" else entry - (rr_target * (sl - entry))
    tp2 = entry + (3.0 * (entry - sl)) if direction == "long" else entry - (3.0 * (sl - entry))

    return {
        "symbol": symbol,
        "direction": direction,
        "entry": round(entry, 4),
        "sl": round(sl, 4),
        "tp1": round(tp1, 4),
        "tp2": round(tp2, 4),
        "confidence": round(confidence, 1),
        "tags": tags,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ----------------------------------------------------------------
#  Telegram output helper
# ----------------------------------------------------------------
def send_telegram(text: str) -> Optional[int]:
    token = os.getenv("TG_BOT_TOKEN")
    chat_id = os.getenv("TG_CHAT_ID")
    if not token or not chat_id:
        logger.info("Telegram not configured – printing signal:\n%s", text)
        return None

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }).encode()
    try:
        resp = _session.post(url, data=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("result", {}).get("message_id")
    except Exception as e:
        logger.error("Telegram send failed: %s", e)
        return None


def format_signal_msg(sig: Dict[str, Any]) -> str:
    arrow = "🟢 LONG" if sig["direction"] == "long" else "🔴 SHORT"
    lines = [
        f"<b>{arrow} — {sig['symbol']}</b>",
        f"<i>{ENGINE_NAME} v{VERSION} | Confidence {sig['confidence']}%</i>",
        "",
        f"<b>Entry:</b> <code>{sig['entry']}</code>",
        f"<b>Stop‑Loss:</b> <code>{sig['sl']}</code>",
        f"<b>TP1:</b> <code>{sig['tp1']}</code>  (RR 2.0)",
        f"<b>TP2:</b> <code>{sig['tp2']}</code>  (RR 3.0)",
        "",
        f"<b>Confluences:</b> {' , '.join(sig['tags'])}",
    ]
    return "\n".join(lines)


# ----------------------------------------------------------------
#  Portfolio‑level risk manager
# ----------------------------------------------------------------
class PortfolioRisk:
    """Tracks capital, exposure, daily P&L and enforces portfolio limits."""

    def __init__(self, state: Dict[str, Any]):
        self.state = state
        self.start_equity = float(os.getenv("PHOENIX_START_EQUITY", "100000"))
        self.equity = self.start_equity
        self.max_concurrent = MAX_CONCURRENT
        self.max_risk = MAX_RISK_PER_TRADE * self.equity
        self.daily_limit = DAILY_LOSS_LIMIT_PCT * self.equity
        self._reconcile_equity()

    def _reconcile_equity(self) -> None:
        """Re‑calculate equity from closed trades."""
        pnl = 0.0
        for t in self.state.get("signal_history", []):
            rr = (t["tp2"] - t["entry"]) / (t["entry"] - t["sl"])
            if t["result"] == "win":
                pnl += rr * (t["entry"] - t["sl"])
            else:
                pnl -= (t["entry"] - t["sl"])
        self.equity = self.start_equity + pnl
        self.max_risk = MAX_RISK_PER_TRADE * self.equity

    def can_open(self) -> bool:
        """Return True if the portfolio still allows a new position."""
        if len(self.state.get("active_signals", [])) >= self.max_concurrent:
            logger.info("Portfolio concurrent limit reached (%d).", self.max_concurrent)
            return False
        utc_today = datetime.now(timezone.utc).date().isoformat()
        day = self.state["daily"].setdefault(utc_today, {"pnl_pct": 0.0, "signals": 0})
        if day["pnl_pct"] <= -DAILY_LOSS_LIMIT_PCT:
            logger.warning("Daily loss limit breached (%.2f%%) – no new signals.", day["pnl_pct"] * 100)
            return False
        return True

    def allocate_position_size(self, entry_price: float, sl_price: float) -> float:
        """Return USD size such that risk = max_risk."""
        risk_pct = abs(entry_price - sl_price) / entry_price
        if risk_pct == 0:
            return 0.0
        size_usd = self.max_risk / risk_pct
        size_usd = min(size_usd, 0.05 * self.equity)  # cap at 5 % of equity per trade
        return round(size_usd, 2)

    def record_trade(self, trade: Dict[str, Any]) -> None:
        """Move a signal from active → history and update daily stats."""
        # Remove from actives
        self.state["active_signals"] = [
            s for s in self.state.get("active_signals", [])
            if not (s["symbol"] == trade["symbol"] and s["direction"] == trade["direction"])
        ]
        self.state.setdefault("signal_history", []).append(trade)

        # Update daily P&L
        utc_today = datetime.now(timezone.utc).date().isoformat()
        day = self.state["daily"].setdefault(utc_today, {"pnl_pct": 0.0, "signals": 0})
        if trade["result"] == "win":
            pnl = (trade["tp2"] - trade["entry"]) * trade["size_usd"] / trade["entry"]
        else:
            pnl = (trade["entry"] - trade["sl"]) * trade["size_usd"] / trade["entry"]
        day["pnl_pct"] += pnl / self.equity
        day["signals"] += 1


# ----------------------------------------------------------------
#  Correlation‑control (duplicate‑signal suppression)
# ----------------------------------------------------------------
def prune_correlated(signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    If two signals share >80 % Pearson correlation over the last 4 h,
    keep the one with higher confidence.
    """
    if not signals:
        return []

    # Gather recent returns for each symbol (last 4 h ≈ 16 15‑m bars)
    recent_returns: Dict[str, np.ndarray] = {}
    for sig in signals:
        sym = sig["symbol"]
        df = fetch_candles(sym, "15m", int(time.time() * 1000))
        if df is None or len(df) < 17:
            continue
        closes = df["close"].values[-17:]
        recent_returns[sym] = np.diff(np.log(closes))

    kept: List[Dict[str, Any]] = []
    for cand in signals:
        discard = False
        for kept_sig in kept:
            if cand["symbol"] == kept_sig["symbol"]:
                continue
            if cand["symbol"] not in recent_returns or kept_sig["symbol"] not in recent_returns:
                continue
            corr = np.corrcoef(recent_returns[cand["symbol"]], recent_returns[kept_sig["symbol"]])[0, 1]
            if corr > 0.8:
                # Keep the higher‑confidence one
                if cand["confidence"] > kept_sig["confidence"]:
                    kept.remove(kept_sig)
                else:
                    discard = True
                break
        if not discard:
            kept.append(cand)
    return kept


# ----------------------------------------------------------------
#  Back‑testing / walk‑forward validation (compact but functional)
# ----------------------------------------------------------------
@dataclass
class Candidate:
    """Minimal representation used by the quick back‑test."""
    direction: str   # "long" or "short"
    sl: float
    tp2: float


def _net_return(direction: str, entry: float, exit_price: float) -> float:
    """Net % return after fees & slippage."""
    gross = (exit_price - entry) / entry if direction == "long" else (entry - exit_price) / entry
    return gross - (FEE_TAKER + SLIPPAGE_EST)


def _simulate_forward(candles: List[dict], start_idx: int, cand: Candidate,
                     max_bars: int = 96) -> Tuple[str, float]:
    """Step through candles after `start_idx` until SL or TP2 is hit."""
    for i in range(start_idx + 1, min(len(candles), start_idx + 1 + max_bars)):
        c = candles[i]
        if cand.direction == "long":
            if c["l"] <= cand.sl:
                return "loss", cand.sl
            if c["h"] >= cand.tp2:
                return "win", cand.tp2
        else:  # short
            if c["h"] >= cand.sl:
                return "loss", cand.sl
            if c["l"] <= cand.tp2:
                return "win", cand.tp2
    # If neither SL nor TP hit within the window, treat as loss at final bar
    final_price = candles[min(start_idx + max_bars, len(candles) - 1)]["c"]
    return "loss", final_price


def load_historical(symbol: str, tf: str, start_iso: str, end_iso: str) -> pd.DataFrame:
    """Fetch historical candles via the `candleSnapshot` endpoint."""
    start_ms = int(datetime.fromisoformat(start_iso).replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(datetime.fromisoformat(end_iso).replace(tzinfo=timezone.utc).timestamp() * 1000)
    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": symbol.replace("USDT", ""),
            "interval": tf,
            "startTime": start_ms,
            "endTime": end_ms,
        },
    }
    raw = hl_post(payload)
    if not raw:
        raise RuntimeError(f"Failed to load historical data for {symbol} {tf}")

    rows = []
    for c in raw:
        rows.append({
            "timestamp": pd.to_datetime(c["t"], unit="ms", utc=True),
            "open": float(c["o"]),
            "high": float(c["h"]),
            "low": float(c["l"]),
            "close": float(c["c"]),
            "volume": float(c["v"]),
        })
    df = pd.DataFrame(rows).set_index("timestamp")
    return df


def _quick_backtest(
    symbols: List[str],
    tf_main: str,
    tf_conf: str,
    tf_bias: str,
    start_dt: datetime,
    end_dt: datetime,
    min_conf_high_vol: int = 55,
    min_conf_low_vol: int = 45,
    sl_mul: float = 1.5,
    tp_mul: float = 2.5,
) -> Dict[str, Any]:
    """Lightweight back‑test used inside walk‑forward windows."""
    trades: List[Dict[str, Any]] = []

    for symbol in symbols:
        try:
            df_main = load_historical(symbol, tf_main, start_dt.isoformat(), end_dt.isoformat())
            df_conf = load_historical(symbol, tf_conf, start_dt.isoformat(), end_dt.isoformat())
            df_bias = load_historical(symbol, tf_bias, start_dt.isoformat(), end_dt.isoformat())
        except Exception as e:
            logger.warning("Historical fetch failed for %s: %s", symbol, e)
            continue

        df_main = add_indicators(df_main)
        df_conf = add_indicators(df_conf)
        df_bias = add_indicators(df_bias)

        # Walk through each 15‑m candle as a decision point
        for idx in range(30, len(df_main)):
            regime = detect_regime(df_conf.iloc[max(0, idx - 4): idx + 1])  # rough regime estimate
            funding, oi_usd = None, None  # not available historically via snapshot
            conf, tags = compute_score(
                df_main.iloc[idx - 1: idx + 1],
                df_conf.iloc[idx // 4 - 1: idx // 4 + 1],
                df_bias.iloc[idx // 16 - 1: idx // 16 + 1],
                funding,
                oi_usd,
                regime,
            )
            min_conf = min_conf_high_vol if regime in {"high_vol", "low_vol"} else min_conf_low_vol
            if conf < min_conf:
                continue

            direction = "long" if df_main["ema_fast"].iloc[idx] > df_main["ema_slow"].iloc[idx] else "short"
            price = df_main["close"].iloc[idx]
            atr = df_main["atr"].iloc[idx]
            sl = price - sl_mul * atr if direction == "long" else price + sl_mul * atr
            tp = price + tp_mul * (price - sl) if direction == "long" else price - tp_mul * (sl - price)

            cand = Candidate(direction=direction, sl=sl, tp2=tp)
            result, exit_price = _simulate_forward(
                candles=df_main.reset_index().to_dict("records"),
                start_idx=idx,
                cand=cand,
                max_bars=96,
            )
            trades.append({
                "symbol": symbol,
                "direction": direction,
                "entry": price,
                "sl": sl,
                "tp2": tp,
                "exit_price": exit_price,
                "result": result,
                "regime": regime,
            })

    # Aggregate results
    wins = sum(1 for t in trades if t["result"] == "win")
    losses = sum(1 for t in trades if t["result"] == "loss")
    total = len(trades)
    rr_sum = 0.0
    by_regime: Dict[str, Dict[str, int]] = {}
    for t in trades:
        rr = (t["tp2"] - t["entry"]) / (t["entry"] - t["sl"])
        rr = rr if t["result"] == "win" else -1.0
        rr_sum += rr
        reg = t["regime"]
        by_regime.setdefault(reg, {"wins": 0, "losses": 0, "total": 0})
        if t["result"] == "win":
            by_regime[reg]["wins"] += 1
        else:
            by_regime[reg]["losses"] += 1
        by_regime[reg]["total"] += 1

    # Simple sensitivity check (±10 % on core params)
    sens_ok = _sensitivity_check(
        symbols, tf_main, tf_conf, tf_bias,
        start_dt, end_dt,
        min_conf_high_vol, min_conf_low_vol, sl_mul, tp_mul,
    )
    if not sens_ok:
        logger.warning("Sensitivity check flagged potential over‑fit.")

    return {
        "wins": wins,
        "losses": losses,
        "total": total,
        "win_rate": wins / total if total else 0.0,
        "avg_rr": rr_sum / total if total else 0.0,
        "by_regime": by_regime,
    }


def _sensitivity_check(
    symbols,
    tf_main,
    tf_conf,
    tf_bias,
    start_dt,
    end_dt,
    min_conf_high_vol,
    min_conf_low_vol,
    sl_mul,
    tp_mul,
    perturb: float = 0.10,
) -> bool:
    """Run small +-10 % perturbations on each key param and ensure win‑rate stays within 5 %."""
    base = _quick_backtest(
        symbols, tf_main, tf_conf, tf_bias,
        start_dt, end_dt,
        min_conf_high_vol, min_conf_low_vol, sl_mul, tp_mul,
    )
    base_wr = base["win_rate"]
    for name, base_val in [
        ("min_conf_high_vol", min_conf_high_vol),
        ("min_conf_low_vol", min_conf_low_vol),
        ("sl_mul", sl_mul),
        ("tp_mul", tp_mul),
    ]:
        for factor in (1 - perturb, 1 + perturb):
            kwargs = {
                "min_conf_high_vol": min_conf_high_vol,
                "min_conf_low_vol": min_conf_low_vol,
                "sl_mul": sl_mul,
                "tp_mul": tp_mul,
            }
            kwargs[name] = int(base_val * factor) if "conf" in name else base_val * factor
            alt = _quick_backtest(
                symbols, tf_main, tf_conf, tf_bias,
                start_dt, end_dt, **kwargs
            )
            if abs(alt["win_rate"] - base_wr) > 0.05:
                logger.warning(
                    "Sensitivity: %s +/-10%% changed win‑rate from %.2f to %.2f",
                    name, base_wr, alt["win_rate"]
                )
                return False
    return True


def walk_forward_validate(
    symbols: List[str],
    start: str,
    end: str,
    train_days: int = 30,
    test_days: int = 7,
    holdout_start: str = "2024-01-01",
    holdout_end: str = "2024-02-01",
) -> Dict[str, Any]:
    """
    Perform rolling walk‑forward validation across the supplied date range.
    Returns aggregated metrics, regime breakdown, and a final hold‑out report.
    """
    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    end_dt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)

    # Simple grid search (tiny for illustration)
    grid = {
        "min_conf_high_vol": [45, 55],
        "min_conf_low_vol": [35, 45],
        "sl_mul": [1.2, 1.5, 1.8],
        "tp_mul": [2.0, 2.5, 3.0],
    }

    def optimise(train_start: datetime, train_end: datetime) -> Dict[str, Any]:
        best_metric = -np.inf
        best_params = {}
        for mc in grid["min_conf_high_vol"]:
            for ml in grid["min_conf_low_vol"]:
                for sm in grid["sl_mul"]:
                    for tm in grid["tp_mul"]:
                        perf = _quick_backtest(
                            symbols, TF_EXEC, TF_CONFIRM, TF_BIAS,
                            train_start, train_end,
                            min_conf_high_vol=mc,
                            min_conf_low_vol=ml,
                            sl_mul=sm,
                            tp_mul=tm,
                        )
                        metric = perf["win_rate"] * perf["avg_rr"]
                        if metric > best_metric:
                            best_metric = metric
                            best_params = {
                                "min_conf_high_vol": mc,
                                "min_conf_low_vol": ml,
                                "sl_mul": sm,
                                "tp_mul": tm,
                            }
        return best_params

    # Walk‑forward loop
    cur = start_dt
    agg = {
        "wins": 0,
        "losses": 0,
        "total": 0,
        "rr_sum": 0.0,
        "by_regime": {},
    }

    while cur + timedelta(days=train_days + test_days) <= end_dt:
        train_start = cur
        train_end = cur + timedelta(days=train_days)
        test_start = train_end
        test_end = train_end + timedelta(days=test_days)

        best = optimise(train_start, train_end)
        test_perf = _quick_backtest(
            symbols, TF_EXEC, TF_CONFIRM, TF_BIAS,
            test_start, test_end,
            **best,
        )

        agg["wins"] += test_perf["wins"]
        agg["losses"] += test_perf["losses"]
        agg["total"] += test_perf["total"]
        agg["rr_sum"] += test_perf["avg_rr"] * test_perf["total"]
        for r, sub in test_perf["by_regime"].items():
            agg["by_regime"].setdefault(r, {"wins": 0, "losses": 0, "total": 0})
            agg["by_regime"][r]["wins"] += sub["wins"]
            agg["by_regime"][r]["losses"] += sub["losses"]
            agg["by_regime"][r]["total"] += sub["total"]

        logger.info(
            "Window %s → %s | win%% %.2f | avg RR %.2f",
            test_start.date(),
            test_end.date(),
            100 * test_perf["win_rate"],
            test_perf["avg_rr"],
        )
        cur = test_end

    # Final hold‑out (no optimisation)
    hold_start_dt = datetime.fromisoformat(holdout_start).replace(tzinfo=timezone.utc)
    hold_end_dt = datetime.fromisoformat(holdout_end).replace(tzinfo=timezone.utc)
    hold_perf = _quick_backtest(
        symbols, TF_EXEC, TF_CONFIRM, TF_BIAS,
        hold_start_dt, hold_end_dt,
        min_conf_high_vol=55,
        min_conf_low_vol=45,
        sl_mul=1.5,
        tp_mul=2.5,
    )
    agg["wins"] += hold_perf["wins"]
    agg["losses"] += hold_perf["losses"]
    agg["total"] += hold_perf["total"]
    agg["rr_sum"] += hold_perf["avg_rr"] * hold_perf["total"]
    for r, sub in hold_perf["by_regime"].items():
        agg["by_regime"].setdefault(r, {"wins": 0, "losses": 0, "total": 0})
        agg["by_regime"][r]["wins"] += sub["wins"]
        agg["by_regime"][r]["losses"] += sub["losses"]
        agg["by_regime"][r]["total"] += sub["total"]

    summary = {
        "total_trades": agg["total"],
        "win_rate": agg["wins"] / agg["total"] if agg["total"] else 0.0,
        "avg_rr": agg["rr_sum"] / agg["total"] if agg["total"] else 0.0,
        "by_regime": agg["by_regime"],
    }
    logger.info("=== Walk‑forward summary === Trades:%d Win%%: %.2f Avg RR: %.2f",
                summary["total_trades"], summary["win_rate"] * 100, summary["avg_rr"])
    return summary


# ----------------------------------------------------------------
#  Core scan (risk‑aware, correlation‑controlled)
# ----------------------------------------------------------------
def run_scan() -> None:
    """Main entry point executed every 15 minutes."""
    logger.info("=== PHOENIX scan start (dry_run=%s) ===", os.getenv("DRY_RUN", "0"))
    state = load_state()
    portfolio = PortfolioRisk(state)

    if not portfolio.can_open():
        logger.info("Portfolio blocks new signals; exiting scan.")
        return

    reference_ms = int(time.time() * 1000)
    bundles: Dict[str, Dict[str, pd.DataFrame]] = {}

    # Parallel candle fetching
    with ThreadPoolExecutor(max_workers=int(os.getenv("SCAN_WORKERS", "4"))) as exe:
        futures = {}
        for sym in WATCHLIST:
            for tf in ("15m", "1h", "4h"):
                futures[exe.submit(fetch_candles, sym, tf, reference_ms)] = (sym, tf)

        for fut in as_completed(futures):
            sym, tf = futures[fut]
            try:
                df = fut.result()
                if df is not None:
                    bundles.setdefault(sym, {})[tf] = add_indicators(df)
                else:
                    logger.info("Missing %s %s data – skipping.", sym, tf)
            except Exception as e:
                logger.error("Error fetching %s %s: %s", sym, tf, e)

    raw_signals: List[Dict[str, Any]] = []
    for sym, tfs in bundles.items():
        if not all(k in tfs for k in ("15m", "1h", "4h")):
            continue

        funding, oi_usd = fetch_funding_and_oi(sym)
        regime = detect_regime(tfs["1h"])

        confidence, tags = compute_score(
            tfs["15m"], tfs["1h"], tfs["4h"],
            funding, oi_usd, regime,
        )
        min_conf = 55 if regime in {"high_vol", "low_vol"} else 45
        if confidence < min_conf:
            logger.info("Filtered %s (conf %.1f < %d) – %s", sym, confidence, min_conf, ", ".join(tags))
            continue

        direction = "long" if tfs["15m"]["ema_fast"].iloc[-1] > tfs["15m"]["ema_slow"].iloc[-1] else "short"
        sig = build_signal(sym, direction, tfs["15m"], confidence, tags)

        # Position sizing
        size_usd = portfolio.allocate_position_size(sig["entry"], sig["sl"])
        if size_usd <= 0:
            logger.info("Signal %s sized to $0 – skipping.", sym)
            continue
        sig["size_usd"] = size_usd
        raw_signals.append(sig)

    # Correlation de‑duplication
    final_signals = prune_correlated(raw_signals)

    dry = os.getenv("DRY_RUN") == "1"
    for s in final_signals:
        if not portfolio.can_open():
            logger.info("Portfolio limit reached – halting further signals.")
            break

        if dry:
            logger.info("[DRY‑RUN] Would send: %s", s)
            continue

        # Register active signal & cooldown
        state["active_signals"].append(s)
        state["signal_cooldowns"][s["symbol"]] = time.time() + 30 * 60

        txt = format_signal_msg(s)
        send_telegram(txt)

    if not dry:
        save_state(state)
        logger.info("State persisted – %d active signals now tracked.", len(state["active_signals"]))
    else:
        logger.info("[DRY‑RUN] Scan completed – state not saved.")


# ----------------------------------------------------------------
#  Graceful shutdown handling
# ----------------------------------------------------------------
_SHUTDOWN = False


def _handle_shutdown(sig_num, _frame):
    global _SHUTDOWN
    _SHUTDOWN = True
    logger.warning("Shutdown signal %s received – exiting after current scan.", sig_num)


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


# ----------------------------------------------------------------
#  Entry point
# ----------------------------------------------------------------
if __name__ == "__main__":
    try:
        run_scan()
    except Exception as exc:
        logger.exception("Unhandled exception in scan: %s", exc)
