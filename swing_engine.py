"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          SWING ENGINE v2.0.0  —  1D → 4H → 1H  MULTI-TIMEFRAME            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  ARCHITECTURE                                                                ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  Timeframe Stack:                                                            ║
║    • 1D  → Trend Filter  (EMA21/50/200, ADX, ATR, Market Structure)         ║
║    • 4H  → Setup Detector (EMA21/50, RSI, ADX, Volume, ATR)                ║
║    • 1H  → Entry Trigger  (RSI, EMA21/50, Volume, ATR, Candle Structure)   ║
║                                                                              ║
║  SCORING (max 13)                                                            ║
║    Daily Alignment:  +1 to +3                                               ║
║    4H Setup Quality: +1 to +3                                               ║
║    1H Confirmation:  +1 to +3                                               ║
║    Volume:           +1 to +2                                               ║
║    ADX:              +1 to +2                                               ║
║    Min to fire: 8   |  Premium: 11+                                         ║
║                                                                              ║
║  NEW IN v1.1.0  (ported from MTF v0.1 after audit)                          ║
║    ✔ Per-setup-type TP/SL ATR multipliers (CONT/PULL/BREAK each tuned       ║
║      independently — pullback entries get a tighter SL, since they enter   ║
║      closer to a structural level than continuation/breakout entries)      ║
║    ✔ EMA21 vs EMA50 pullback quality split in detect_4h_setup() — an       ║
║      EMA21 touch is the higher-probability setup and can reach the score   ║
║      ceiling; an EMA50 touch is capped lower                                ║
║                                                                              ║
║  NEW IN v1.1.1  (risk-margin patch)                                         ║
║    ✔ R:R exact-tie fix: TP1 multipliers bumped to give buffer above         ║
║      MIN_RR_RATIO (PULL: 1.625, CONT: 1.60, BREAK: 1.60)                  ║
║    ✔ Branch-order comment on EMA21/EMA50 near_ef/near_es mutual            ║
║      exclusivity — order-dependency made explicit                           ║
║    ✔ _leverage_for_risk() documented as approximate/display-only            ║
║                                                                              ║
║  NEW IN v1.2.0  (signal-quality patch)                                      ║
║    ✔ PULL entry extension gate — rejects chase entries where 1H price      ║
║      has moved > PULL_MAX_EXTENSION_ATR away from the pullback EMA         ║
║    ✔ RSI/price divergence detection (DIVERGENCE_PENALTY=-1, soft gate)     ║
║    ✔ Asymmetric rsi_healthy threshold comment (intentional, documented)    ║
║    ✔ ADX persistence requirement for CONT score-3 ceiling                  ║
║      (ADX_MIN_PERSISTENCE_BARS=2 consecutive bars required)                ║
║    ✔ volume_rising() tiebreaker — promotes "ok" → "strong" vol score       ║
║      when 3-bar rising volume trend accompanies single-bar elevation        ║
║                                                                              ║
║  NEW IN v1.2.0 — Signal Frequency Upgrades F1–F10                          ║
║    ✔ F1  Neutral daily trend unlocks signal generation — allows_long and   ║
║          allows_short both True in Neutral; neutral_cap flag enforces a    ║
║          base_score ceiling of 10; score_daily_alignment() returns 0 for   ║
║          Neutral (no false alignment bonus)                                 ║
║    ✔ F2  4H EMA200 fallback PULL zone — when price holds above/below       ║
║          EMA200 but outside EMA21/EMA50 range, a PULL_EMA200 setup at      ║
║          score=2 is detected (only fires when full chain returns NONE)     ║
║    ✔ F3  PULL extension gate calibrated for 4H ATR — threshold widened     ║
║          1.0 → 2.0 (1H ATR units) to account for 4H ATR ≈ 2-3× 1H ATR;   ║
║          valid pullbacks that measured > 1.0×atr_1h are no longer rejected ║
║    ✔ F4  Premium-score 1-bar cooldown — setups with score ≥ PREMIUM_SCORE  ║
║          (11) re-fire after 1 bar instead of SIGNAL_COOLDOWN_1H_BARS       ║
║    ✔ F5  Regime-adaptive MAX_CONCURRENT_ACTIVE — extends from 12 → 15      ║
║          when BTC is bullish and breadth ≥ BREADTH_CROWDED_LONG (75%)      ║
║    ✔ F6  Expired signals allow immediate re-entry — check_cooldown() skips ║
║          the same-direction active-signal block when age ≥                  ║
║          SIGNAL_MAX_AGE_1H_BARS (sideways expiry ≠ loss)                   ║
║    ✔ F7  Fibonacci retracement PULL path — compute_fib_levels() added;     ║
║          price within atr*0.4 of 38.2%/50%/61.8% fib generates PULL_FIB   ║
║          at score=1 (fires only when all EMA + EMA200 checks return NONE)  ║
║    ✔ F8  BTC-regime directional bias in Neutral daily — bearish BTC regime  ║
║          restricts Neutral daily to shorts; bullish BTC to longs; mixed    ║
║          BTC keeps both open (layered on top of F1)                        ║
║    ✔ F9  Tiered OI minimum — SMALL_CAP_PAIRS get MIN_OI_USD_SMALL_CAP      ║
║          ($250k) instead of the default $500k floor                        ║
║    ✔ F10 WIN_RATE_MIN_SAMPLE_FOR_ADJ lowered 80 → 30; hard suppress added  ║
║          for setups with WR < 35% at ≥30 samples (score_adj = -3)         ║
║    ✔ Q1  DI+ / DI- direction validation in score_adx() — ADX points        ║
║          only awarded when dominant DI aligns with trade direction          ║
║    ✔ Q2  Symbol-scoped indicator cache keys — eliminates __4H__ / __1H__   ║
║          race condition under SCAN_WORKERS > 1 (critical correctness fix)  ║
║    ✔ Q3  Market Structure Shift (MSS) + Break of Structure (BOS) on 4H     ║
║          — MSS penalises score −2, BOS hard-suppresses setup               ║
║    ✔ Q4  BREAK_LOOKBACK_BARS widened 10 → 30 (~5 days on 4H); ATR         ║
║          compression pre-condition adds +1 quality bonus on squeeze+break  ║
║    ✔ Q5  Structure-based SL placement — SL snapped to nearest 4H swing     ║
║          low/high (0.2% buffer); ATR-based floor still applies             ║
║    ✔ Q6  ATR_HIST_DEPTH increased 48 → 168 (1 week of 1H bars); graceful  ║
║          midpoint fallback below 30 samples; high-ATR (>80th pct) −1      ║
║    ✔ Q7  Swing-based RSI divergence detection replaces last-bar-only;      ║
║          DIVERGENCE_PENALTY increased −1 → −2                              ║
║    ✔ Q8  OB/BB zone consumption tracking — consumed zones persisted in     ║
║          state["consumed_ob_zones"], expired after 24 bars                 ║
║    ✔ Q9  CONT consolidation pre-condition — requires price range < 2×ATR   ║
║          and ≥3/5 compressed bars; otherwise score −1                      ║
║    ✔ Q10 BTC Dominance regime filter — rising BTC.D penalises alt longs    ║
║          −1; falling BTC.D (alt season) adds +1; graceful None fallback   ║
║                                                                              ║
║  NEW IN v2.0.0  — Institutional Features I1–I10                             ║
║    ✔ I1  Liquidity Sweeps Detection — bullish/bearish wick-through-and-     ║
║          close confirms highest-probability reversal entries (PULL, score 3)║
║    ✔ I2  Market Structure Shift extension — confirming MSS adds +1 quality  ║
║          bonus; state-persisted to avoid re-detecting same MSS each scan    ║
║    ✔ I3  BOS level tracking — confirmed swing level stored in result dict;  ║
║          surfaced as BOS @ price in Telegram; used for SL refinement        ║
║    ✔ I4  Volatility Contraction Pattern (VCP) — multi-stage ATR peak       ║
║          contraction check; BREAK_VCP bonus; no-contraction penalty         ║
║    ✔ I5  Relative Volume (RVOL) with time-of-day normalisation — volume    ║
║          profile keyed by hour_of_day; score_volume() updated; nightly      ║
║          refresh task in main()                                              ║
║    ✔ I6  Trend Acceleration Detection — EMA slope + ADX velocity gate;     ║
║          CONT_ACCEL/CONT_DECEL tags; +1/-1 adjustments on CONT setups       ║
║    ✔ I7  Regime-Adaptive Scoring Weights — REGIME_SCORE_WEIGHTS dict;       ║
║          volume_weight and adx_weight applied per daily classification       ║
║    ✔ I8  Dynamic ATR Regime Classification — low/medium/high/extreme;       ║
║          widens PULL zone in low-ATR, tightens + penalises in extreme        ║
║    ✔ I9  Multi-Factor Confluence Model — hard gate ≥ 2 factors required;   ║
║          +1 bonus for ≥ 3 factors; factors listed in Telegram message       ║
║    ✔ I10 Multi-Tier Signal Grading — Grade A(12+)/B(10+)/C(8+); tiered     ║
║          leverage (100%/80%/60%), cooldowns (1/2/3 bars), Telegram emoji    ║
║          differentiation; grade stored in signal_history                     ║
║                                                                              ║
║  NEW IN v1.3.0  (Order Block / Breaker Block entry path)                    ║
║    ✔ detect_order_blocks() — 4H OB zone detection (bullish/bearish)        ║
║    ✔ detect_breaker_blocks() — violated OBs with polarity-flip retest;     ║
║      polarity convention resolved and documented in function docstring     ║
║    ✔ detect_ob_bb_tap() — 1H wick-into-zone rejection detection            ║
║    ✔ OB/BB integrated as alternative 1H confirmation path in              ║
║      compute_signals() (OR logic, not AND — either path confirms)          ║
║    ✔ ob_zone_label surfaced in SignalResult and format_signal (📦 marker)  ║
║    ✔ Scan-scoped consumed-zone guard (cross-scan persistence flagged       ║
║      as known limitation in code comment)                                  ║
║                                                                              ║
║  PRESERVED FROM v15.5.5                                                      ║
║    ✔ Hyperliquid API integration (rate-limited, session pooled)             ║
║    ✔ Telegram integration (HTML, reactions)                                 ║
║    ✔ State persistence (JSON, backup, versioned)                            ║
║    ✔ Win-rate tracking                                                      ║
║    ✔ Active signal monitoring (TP1/TP2/SL)                                  ║
║    ✔ Threading + ThreadPoolExecutor                                          ║
║    ✔ Indicator caching                                                      ║
║    ✔ Rate-limit / backoff handling                                          ║
║    ✔ Cooldown system                                                        ║
║    ✔ Dynamic pairwise correlation clustering                                ║
║    ✔ BTC Regime Filter                                                      ║
║    ✔ Market Breadth Filter                                                  ║
║    ✔ Spread / Liquidity Filter                                              ║
║    ✔ Funding Filter                                                         ║
║    ✔ Open Interest Filter                                                   ║
║    ✔ Macro calendar filter                                                  ║
║    ✔ RS ranking                                                             ║
║    ✔ ATR-based risk management                                              ║
║                                                                              ║
║  REMOVED                                                                     ║
║    ✘ 15M-centric detection logic                                            ║
║    ✘ 15M candle fetching as primary signal layer                            ║
║    ✘ BB / OBV base-score components                                         ║
║    ✘ pull_recover / pull_zone on 15m EMA                                   ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

__version__ = "2.0.0"

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
# ── CONFIG CONSTANTS ───────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════

STATE_FILE            = "state_swing.json"
STATE_VERSION         = 1

# ── API / threading ──────────────────────────────────────────────
HL_INFO_URL           = "https://api.hyperliquid.xyz/info"
SCAN_WORKERS          = int(os.getenv("SCAN_WORKERS", "2"))
HL_MIN_INTERVAL_S     = float(os.getenv("HL_MIN_INTERVAL_S", "0.18"))
HL_MIN_INTERVAL_MAX_S = float(os.getenv("HL_MIN_INTERVAL_MAX_S", "0.60"))
HL_TF_WORKERS         = int(os.getenv("HL_TF_WORKERS", "2"))  # parallel TF fetches

# ── Candle counts ─────────────────────────────────────────────────
N_1H  = 150   # 1H bars (execution trigger)
N_4H  = 120   # 4H bars (setup detection)
N_1D  = 300   # 1D bars (trend filter — needs 200 for EMA200)

# ── Interval ms ──────────────────────────────────────────────────
INTERVAL_MS = {
    "1h":  60 * 60 * 1000,
    "4h":  4  * 60 * 60 * 1000,
    "1d":  24 * 60 * 60 * 1000,
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

# ── Scoring thresholds ───────────────────────────────────────────
MIN_SIGNAL_SCORE    = 8    # minimum to generate a signal
PREMIUM_SCORE       = 11   # premium signal threshold
MAX_SCORE           = 13   # theoretical max

# ── Daily trend classification thresholds ────────────────────────
# ADX thresholds for daily trend
DAILY_ADX_STRONG    = 30.0   # strong trend
DAILY_ADX_WEAK      = 18.0   # minimum for valid trend
# EMA structure requirements
DAILY_EMA_STRONG_BULL = True  # EMA21 > EMA50 > EMA200 for "Strong Bullish"
# Market structure lookback (bars on daily)
MS_LOOKBACK_BARS    = 20     # look back 20 daily bars for HH/HL or LH/LL

# ── 4H Setup thresholds ───────────────────────────────────────────
H4_ADX_MIN          = 20.0   # 4H ADX floor for valid setup
ADX_MIN_PERSISTENCE_BARS = 2   # consecutive 4H bars ADX must hold above
                               # H4_ADX_MIN before a CONT setup can score 3
# Q4: Widened from 10 → 30 bars (~5 days on 4H) to filter noise BREAK signals.
# A 10-bar lookback (~1.7 days) is too short to identify meaningful structural
# breakouts and generates excessive signals on intraday noise.
BREAK_LOOKBACK_BARS = 30
H4_RSI_OB           = 70.0   # 4H RSI overbought
H4_RSI_OS           = 30.0   # 4H RSI oversold
H4_VOL_MULT         = 1.0    # volume vs average multiplier for setup
PULL_MAX_EXTENSION_ATR = 2.0   # F3: widened 1.0 → 2.0 — the pullback zone is
                               # defined in 4H-ATR units (~2-3× 1H ATR), so a
                               # valid pull of 0.4×4H_ATR ≈ 1.2×1H_ATR; the old
                               # threshold of 1.0 silently rejected valid setups

# ── 1H Entry thresholds ───────────────────────────────────────────
H1_RSI_BULL         = 50.0   # RSI > 50 for long confirmation
H1_RSI_BEAR         = 50.0   # RSI < 50 for short confirmation
H1_RSI_OB           = 75.0   # overbought — don't chase longs
H1_RSI_OS           = 25.0   # oversold — don't chase shorts
ENGULF_BODY_RATIO   = 0.6    # body/range ratio for engulfing candle
IMPULSE_VOL_MULT    = 1.5    # volume multiplier for strong impulse
SWING_LOOKBACK      = 5      # bars to look back for swing high/low

# ── Risk management ───────────────────────────────────────────────
ATR_FALLBACK_PCT    = 0.30   # fallback ATR % if calculation fails
MIN_ATR_PCT         = 0.15   # dead market filter (lower than v15 — daily moves smaller)
MAX_ATR_PCT         = 12.0
MIN_RR_RATIO        = 1.3    # minimum TP1 R:R
PREFERRED_RR_RATIO  = 2.0    # preferred R:R

# ATR-based TP/SL (1H ATR units) — per setup-type (ported from MTF v0.1)
# CONT  = trend continuation, PULL = pullback entry (tighter SL), BREAK = breakout
TP1_MULT_CONT       = 1.6    # was 1.5 — R:R 1.5 buffer above MIN_RR_RATIO
TP2_MULT_CONT       = 3.0
SL_MULT_CONT        = 1.0

TP1_MULT_PULL       = 1.3    # was 1.2 — keeps "tighter than CONT" intent, adds buffer
TP2_MULT_PULL       = 2.5
SL_MULT_PULL        = 0.80   # and a tighter stop, since invalidation is closer

TP1_MULT_BREAK      = 1.6    # was 1.5 — same fix as CONT
TP2_MULT_BREAK      = 3.0
SL_MULT_BREAK       = 1.0

SETUP_TP_SL_MULTS = {
    "CONT":  (TP1_MULT_CONT,  TP2_MULT_CONT,  SL_MULT_CONT),
    "PULL":  (TP1_MULT_PULL,  TP2_MULT_PULL,  SL_MULT_PULL),
    "BREAK": (TP1_MULT_BREAK, TP2_MULT_BREAK, SL_MULT_BREAK),
}

SL_HIGH_ATR_MULT    = 0.85   # tighter SL in high-vol
HIGH_ATR_THRESHOLD  = 3.0    # % — above this use tighter SL

# Regime-aware TP/SL tweaks
REGIME_BULL_TP2_MULT    = 1.15
REGIME_BEAR_TP1_MULT    = 0.85
REGIME_HIGHVOL_SL_MULT  = 0.90

# ── Signal management ─────────────────────────────────────────────
MAX_SIGNALS_PER_SCAN        = 3
MAX_SIGNALS_BULL_TREND      = 5
BREADTH_BULL_THRESHOLD      = 0.70
MAX_CONCURRENT_ACTIVE       = 12
SIGNAL_MAX_AGE_1H_BARS      = 24   # signals expire after 24 1H bars
SIGNAL_COOLDOWN_1H_BARS     = 2    # 2×1H = 2h cooldown between same-direction signals
SIGNAL_COOLDOWN_POST_WIN    = 1    # 1 bar post-win
SIGNAL_HIGHSCORE_THRESHOLD  = 10
PULL_REENTRY_COOLDOWN_S     = 3600 # 1h cooldown after SL

# ── Filters ───────────────────────────────────────────────────────
MIN_OI_USD              = 500_000.0
# F9: Tiered OI minimum — smaller-cap Hyperliquid pairs with genuine
# liquidity are blocked by the 500k floor; halved threshold for these pairs
MIN_OI_USD_SMALL_CAP    = 250_000.0
SMALL_CAP_PAIRS: set[str] = {
    "PENGUUSDT", "HYPEUSDT", "ZECUSDT", "PENDLEUSDT",
}
FUNDING_SUPPRESS_EXTREME = 0.0010
SPREAD_WARN_PCT         = 0.20
SPREAD_SUPPRESS_PCT     = 0.40
SPREAD_EXEMPT: set[str] = {
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT",
    "XRPUSDT", "ADAUSDT", "DOGEUSDT",
}

# ── Scoring adjustments (positive caps) ──────────────────────────
MAX_NEGATIVE_ADJUSTMENTS = 3
MAX_POSITIVE_ADJUSTMENTS = 5
DIVERGENCE_PENALTY = -2   # Q7: increased from -1 → -2. Swing-based divergence
                          # detection (Q7) now fires on real divergences only,
                          # so a stronger penalty is warranted — a confirmed
                          # divergence is a meaningful quality signal.

# ── Correlation clustering ────────────────────────────────────────
DYNAMIC_CORR_CLUSTER_THRESHOLD = 0.75
CORR_MATRIX_MIN_SAMPLE         = 20
LOW_BTC_CORR_LOOKBACK_BARS     = 42   # 7 days of 4H candles
LOW_BTC_CORR_THRESHOLD         = 0.65

# ── RS / breadth ─────────────────────────────────────────────────
RS_TOP_PERCENTILE        = 0.20
RS_BOTTOM_PERCENTILE     = 0.20
RS_BREAK_HARD_GATE_PCT   = -6.0
RS_BREAK_HARD_PENALTY    = -3
RS_BEARISH_EXEMPT_PCT    = 3.0

BREADTH_WEAK_LONG        = 0.20
BREADTH_WEAK_SHORT       = 0.80
BREADTH_CROWDED_LONG     = 0.75
BREADTH_EXTREME_LONG     = 0.90
BREADTH_EXTREME_SHORT    = 0.10

# ── OI ─────────────────────────────────────────────────────────
OI_HISTORY_DEPTH         = 6
OI_CHANGE_THRESHOLD_PCT  = 1.0
OI_STALE_CUTOFF_S        = 45 * 60
OI_EXPECTED_INTERVAL_S   = 15 * 60
MAX_OI_SCALE             = 3.0
OI_ACCEL_MIN_THRESHOLD   = 1.0
OI_SCORE_CAP             = 2

# ── Order Block / Breaker Block ──────────────────────────────────
OB_BB_TAP_SCORE = 2   # equivalent weight to a moderate 1H confirmation —
                       # not the max of 3, since OB/BB taps alone don't
                       # carry the multi-trigger redundancy of the
                       # existing engulfing+RSI+volume combination

# ── Leverage ─────────────────────────────────────────────────────
LEVERAGE_BASE_RISK_PCT     = 10.0
LEVERAGE_RANGE_LOW_PCT     = 5.0
LEVERAGE_RANGE_HIGH_PCT    = 15.0
LEVERAGE_MAX               = 15.0

# ── Win rate ─────────────────────────────────────────────────────
WIN_RATE_MIN_SAMPLE         = 20
WIN_RATE_HIGH_THRESH        = 0.65
WIN_RATE_LOW_THRESH         = 0.45
# F10: Lowered 80 → 30 so the adjustment activates in 2-4 weeks of operation
# rather than months.  At 80 samples the feature was practically dormant.
WIN_RATE_MIN_SAMPLE_FOR_ADJ = 30
WIN_RATE_HARD_SUPPRESS_THRESHOLD = 0.35  # F10: < 35% WR with 30+ samples → suppress
WIN_RATE_HARD_SUPPRESS_MIN_SAMPLE = 30
WIN_RATE_LOOKBACK_DAYS      = 30
WIN_RATE_RECENT_DAYS        = 7
WIN_RATE_RECENT_WEIGHT      = 2.0
WIN_RATE_STALE_DAYS         = 14

# ── Historical signal storage ─────────────────────────────────────
MAX_SIGNAL_HISTORY = 2000

# ── ATR history ─────────────────────────────────────────────────
# Q6: Widened from 48 → 168 (1 week of 1H bars) for statistically valid
# percentile readings. The previous 48-bar (~2 day) depth was too shallow
# for reliable regime detection. The regime-aware SL tightening and the
# high-ATR caution penalty require ≥30 samples to be meaningful.
ATR_HIST_DEPTH        = 168
ATR_LOW_PERCENTILE    = 0.10
ATR_HIGH_PERCENTILE   = 0.80   # Q6: lowered from 0.90 → 0.80 for earlier penalty trigger

# ── Macro calendar ───────────────────────────────────────────────
MACRO_WINDOW_BEFORE_MINS  = 60
MACRO_WINDOW_AFTER_MINS   = 30
MACRO_HIGH_ATR_SUPPRESS   = 3.0
MACRO_CACHE_TTL_S         = 3600
# R3: MACRO_EVENT_KEYWORDS is deprecated and no longer used as an active filter.
# The impact == "high" gate alone is sufficient and less failure-prone.
# Left here as reference only.
MACRO_EVENT_KEYWORDS = [
    "fomc", "federal funds", "interest rate decision",
    "cpi", "consumer price index", "ppi", "producer price index",
    "nfp", "nonfarm payroll", "non-farm payroll",
    "gdp", "gross domestic product", "ecb", "bank of england", "boe",
]
MACRO_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

# ── Funding ─────────────────────────────────────────────────────
FUNDING_CARRY_POS_THRESHOLD = 0.0005
FUNDING_CARRY_NEG_THRESHOLD = -0.0005
FUNDING_CARRY_BONUS         = 1
FUNDING_HEADWIND_THRESHOLD  = 0.0005
FUNDING_HISTORY_DEPTH       = 4

# ── Institutional Feature Flags (I1–I10) ─────────────────────────
# Set to False to disable any feature for rollback or A/B testing.
ENABLE_LIQUIDITY_SWEEPS          = True   # I1
ENABLE_MSS_CONFIRMING_BONUS      = True   # I2 extension
ENABLE_BOS_LEVEL_TRACKING        = True   # I3
ENABLE_VCP                       = True   # I4
ENABLE_RVOL                      = True   # I5
ENABLE_TREND_ACCELERATION        = True   # I6
ENABLE_REGIME_SCORE_WEIGHTS      = True   # I7
ENABLE_DYNAMIC_ATR_REGIME        = True   # I8
ENABLE_CONFLUENCE_MODEL          = True   # I9
ENABLE_MULTI_TIER_GRADING        = True   # I10

# ── I1: Liquidity Sweeps ─────────────────────────────────────────
SWEEP_LOOKBACK = 12   # 4H bars to look back for swing high/low

# ── I4: Volatility Contraction Pattern ───────────────────────────
VCP_LOOKBACK = 12     # ATR bars for VCP detection
VCP_MIN_STAGES = 2    # minimum successive ATR peak contractions required

# ── I5: Relative Volume (RVOL) ───────────────────────────────────
RVOL_STRONG_THRESHOLD  = 2.5   # RVOL ≥ 2.5 → strong (score 2)
RVOL_MODERATE_THRESHOLD = 1.3  # RVOL ≥ 1.3 → moderate (score 1)
VOLUME_PROFILE_TTL_S   = 86400 # rebuild volume profile once per day

# ── I6: Trend Acceleration ───────────────────────────────────────
EMA_SLOPE_ACCEL_THRESHOLD = 0.002   # 0.2% per bar for acceleration
ADX_VELOCITY_RISING       = 2.0     # ADX gain over 3 bars = rising
ADX_VELOCITY_FALLING      = -2.0    # ADX drop over 3 bars = falling
ADX_LATE_TREND_THRESHOLD  = 40.0    # only penalise decel above this ADX

# ── I7: Regime-Adaptive Scoring Weights ──────────────────────────
REGIME_SCORE_WEIGHTS = {
    "Strong Bullish": {"volume_weight": 1.0, "rs_weight": 1.5, "adx_weight": 1.0},
    "Bullish":        {"volume_weight": 1.2, "rs_weight": 1.0, "adx_weight": 1.0},
    "Bearish":        {"volume_weight": 1.5, "rs_weight": 1.0, "adx_weight": 1.2},
    "Strong Bearish": {"volume_weight": 1.5, "rs_weight": 1.0, "adx_weight": 1.2},
    "Neutral":        {"volume_weight": 1.3, "rs_weight": 0.8, "adx_weight": 1.0},
}

# ── I8: Dynamic ATR Regime ────────────────────────────────────────
ATR_REGIME_LOW_PCT     = 0.20   # below 20th percentile → "low"
ATR_REGIME_MED_PCT     = 0.50   # below 50th percentile → "medium"
ATR_REGIME_HIGH_PCT    = 0.80   # below 80th percentile → "high" (matches ATR_HIGH_PERCENTILE)
# above 80th percentile → "extreme"

# ── I9: Multi-Factor Confluence ───────────────────────────────────
CONFLUENCE_MIN_FACTORS  = 2   # hard gate: reject if fewer factors present
CONFLUENCE_BONUS_FACTORS = 3  # bonus +1 if this many or more factors

# ── I10: Multi-Tier Signal Grading ────────────────────────────────
GRADE_A_SCORE = 12   # Premium — full leverage, 1-bar cooldown
GRADE_B_SCORE = 10   # Quality  — 80% leverage, 2-bar cooldown
GRADE_C_SCORE = 8    # Marginal — 60% leverage, 3-bar cooldown
GRADE_A_LEVERAGE_FACTOR = 1.00
GRADE_B_LEVERAGE_FACTOR = 0.80
GRADE_C_LEVERAGE_FACTOR = 0.60


REACT_TP1 = "🔥"
REACT_TP2 = "🏆"
REACT_SL  = "😭"

# ── Meta cache ──────────────────────────────────────────────────
META_CACHE_TTL_S = 55.0

# ═══════════════════════════════════════════════════════════════════
# HYPERLIQUID  —  Rate-limited HTTP layer
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

    raise RuntimeError("hl_post exhausted all retries (persistent 429)")


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
            "t": int(c["t"]),  "o": float(c["o"]),
            "h": float(c["h"]),"l": float(c["l"]),
            "c": float(c["c"]),"v": bv, "qv": qv,
        })
    candles = filter_closed_candles(candles, interval, ref_ms)
    return candles[-n:]


def fetch_all_candles(symbol: str, reference_ms: int | None = None) -> tuple | None:
    """Fetch 1H, 4H, 1D candles in parallel.  No 15M fetching."""
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
                print(f"  [CANDLES] {symbol} {tf} fetch failed: {e} — skipping")
                return None

    if not all(k in results for k in ("1h", "4h", "1d")):
        return None
    if len(results["1h"]) < 50 or len(results["4h"]) < 30 or len(results["1d"]) < 50:
        return None
    return results["1h"], results["4h"], results["1d"]


# ═══════════════════════════════════════════════════════════════════
# FUNDING / OI / META
# ═══════════════════════════════════════════════════════════════════

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
            _meta_cache             = cache
            _meta_cache_fetched_at  = time.time()
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
# INDICATOR MATH  (pure Python, no deps)
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
    k      = 2.0 / (period + 1)
    out    = [float("nan")] * len(values)
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


# ═══════════════════════════════════════════════════════════════════
# INDICATOR CACHE
# ═══════════════════════════════════════════════════════════════════

_indicator_cache: dict[str, dict] = {}
_indicator_cache_lock = threading.Lock()


def _compute_all_indicators(candles: list[dict]) -> dict:
    o = [c["o"] for c in candles]
    h = [c["h"] for c in candles]
    l = [c["l"] for c in candles]  # noqa: E741
    c_ = [c["c"] for c in candles]
    v  = [c["v"] for c in candles]
    _dp, _dm, _adx = adx_dmi(h, l, c_, ADX_LEN)
    return {
        "o": o, "h": h, "l": l, "c": c_, "v": v,
        "ema_fast": ema(c_, EMA_FAST),
        "ema_slow": ema(c_, EMA_SLOW),
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
        "post_loss_cooldown":   {},
        "atr_history":          {},
        "funding_history":      {},
        "signal_cooldowns":     {},
        "last_signal_outcome":  {},
        "active_signals":       [],
        # Q8: OB/BB zone consumption tracking (keyed by symbol → zone_id → bar)
        # Prevents repeated signals on the same order-block / breaker-block zone.
        "consumed_ob_zones":    {},
        # Q10: BTC dominance history (last 5 readings) for alt rotation filter
        "btc_dominance_history": [],
        # I5: Per-symbol volume profile keyed by hour-of-day string (0..23)
        "volume_profile":        {},
        "volume_profile_built_at": 0,
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
                s.pop("resolved_signals", None)  # removed in v1.3.0
                if path != STATE_FILE:
                    print(f"[STATE] Loaded from backup {path}")
                return s
            except Exception as e:
                print(f"[STATE] Failed to load {path}: {e}")
    print("[STATE] Starting fresh — no valid state found.")
    return fresh


def save_state(state: dict):
    with _state_lock:
        state_copy = copy.deepcopy(state)
    tmp = STATE_FILE + ".tmp"
    Path(tmp).write_text(json.dumps(state_copy, indent=2))
    os.replace(tmp, STATE_FILE)
    try:
        import shutil
        shutil.copy2(STATE_FILE, STATE_FILE + ".bak")
    except Exception:
        pass


def prune_state(state: dict):
    now = int(time.time())
    now_1h_bar = int(time.time() * 1000) // INTERVAL_MS["1h"]

    with _state_lock:
        # OI history — prune entries older than 24h
        for sym in list(state.get("oi_history", {}).keys()):
            state["oi_history"][sym] = [e for e in state["oi_history"][sym]
                                        if now - e["ts"] < 86400]
            if not state["oi_history"][sym]:
                del state["oi_history"][sym]

        # Post-loss cooldown
        cutoff = now - PULL_REENTRY_COOLDOWN_S * 3
        state["post_loss_cooldown"] = {
            k: v for k, v in state.get("post_loss_cooldown", {}).items() if v > cutoff
        }

        # ATR history
        for sym in list(state.get("atr_history", {}).keys()):
            state["atr_history"][sym] = [e for e in state["atr_history"][sym]
                                         if now - e["ts"] < 86400]
            if not state["atr_history"][sym]:
                del state["atr_history"][sym]

        # Funding history
        for sym in list(state.get("funding_history", {}).keys()):
            state["funding_history"][sym] = [
                e for e in state["funding_history"][sym] if now - e["ts"] < 7200
            ]
            if not state["funding_history"][sym]:
                del state["funding_history"][sym]

        # Signal cooldowns — based on 1H bars
        state["signal_cooldowns"] = {
            k: v for k, v in state.get("signal_cooldowns", {}).items()
            if now_1h_bar - v < 96
        }

        # Q8: Consumed OB/BB zones — expire entries older than 24 bars
        consumed = state.setdefault("consumed_ob_zones", {})
        for sym in list(consumed.keys()):
            consumed[sym] = {
                zid: bar for zid, bar in consumed[sym].items()
                if now_1h_bar - bar < 24
            }
            if not consumed[sym]:
                del consumed[sym]

        # Active signals
        state["active_signals"] = [
            s for s in state.get("active_signals", [])
            if now_1h_bar - s.get("bar_index", now_1h_bar) < SIGNAL_MAX_AGE_1H_BARS
            and not s.get("resolved", False)
        ]


# ═══════════════════════════════════════════════════════════════════
# OI STATE
# ═══════════════════════════════════════════════════════════════════

def update_oi_history(state: dict, symbol: str, oi_usd: float | None):
    if oi_usd is None:
        return
    with _state_lock:
        h = state.setdefault("oi_history", {}).setdefault(symbol, [])
        h.append({"ts": int(time.time()), "oi": oi_usd})
        state["oi_history"][symbol] = h[-OI_HISTORY_DEPTH:]


def compute_oi_trend(state: dict, symbol: str, price_dir: str, trade_dir: str) -> dict:
    with _state_lock:
        hist = list(state.get("oi_history", {}).get(symbol, []))
    null = {"oi_trend": "unknown", "oi_change_pct": None,
            "oi_acceleration": None, "score_adj": 0,
            "label": "OI: Unknown", "tag": "OI?"}
    if len(hist) < 2:
        return null
    r, p = hist[-1]["oi"], hist[-2]["oi"]
    if p == 0:
        return null
    elapsed = max(1.0, hist[-1]["ts"] - hist[-2]["ts"])
    if elapsed > OI_STALE_CUTOFF_S:
        return {**null, "label": "OI: Unknown (stale)"}
    scale  = min(MAX_OI_SCALE, OI_EXPECTED_INTERVAL_S / elapsed)
    chg    = (r - p) / p * 100.0 * scale

    accel = None
    if len(hist) >= 3:
        p2 = hist[-3]["oi"]
        if p2 != 0:
            el2    = max(1.0, hist[-2]["ts"] - hist[-3]["ts"])
            sc2    = min(MAX_OI_SCALE, OI_EXPECTED_INTERVAL_S / el2)
            prev   = (p - p2) / p2 * 100.0 * sc2
            accel  = chg - prev

    rising  = chg >  OI_CHANGE_THRESHOLD_PCT
    falling = chg < -OI_CHANGE_THRESHOLD_PCT
    trend   = "rising" if rising else ("falling" if falling else "flat")

    bull_conf = price_dir == "up"   and rising
    bear_conf = price_dir == "down" and rising

    if trade_dir == "long":
        if bull_conf:   adj, tag = +1, "OI↑"
        elif bear_conf: adj, tag = -1, "OI Div"
        else:           adj, tag =  0, "OI→"
    else:
        if bear_conf:   adj, tag = +1, "OI↑"
        elif bull_conf: adj, tag = -1, "OI Div"
        elif rising and price_dir == "up": adj, tag = -1, "OI↓"
        else:           adj, tag =  0, "OI→"

    return {
        "oi_trend": trend, "oi_change_pct": chg,
        "oi_acceleration": accel, "score_adj": adj,
        "label": f"OI Trend: {trend.capitalize()}  Δ{chg:+.1f}% (norm)", "tag": tag,
    }


# ═══════════════════════════════════════════════════════════════════
# FUNDING STATE
# ═══════════════════════════════════════════════════════════════════

def update_funding_history(state: dict, symbol: str, rate: float | None):
    if rate is None:
        return
    with _state_lock:
        h = state.setdefault("funding_history", {}).setdefault(symbol, [])
        h.append({"ts": int(time.time()), "rate": rate})
        state["funding_history"][symbol] = h[-FUNDING_HISTORY_DEPTH:]


def get_funding_trend(state: dict, symbol: str) -> str:
    with _state_lock:
        hist = list(state.get("funding_history", {}).get(symbol, []))
    if len(hist) < 2:
        return "stable"
    delta = hist[-1]["rate"] - hist[-2]["rate"]
    if delta > 0.0001:
        return "rising"
    if delta < -0.0001:
        return "falling"
    return "stable"


def format_funding(rate: float | None, direction: str) -> str:
    if rate is None:
        return "Funding: n/a"
    pct = rate * 100
    hw  = (rate > 0 and direction == "long") or (rate < 0 and direction == "short")
    tw  = (rate < 0 and direction == "long") or (rate > 0 and direction == "short")
    if hw and abs(rate) >= 0.001:
        tag = "⚠️ EXTREME"
    elif hw and abs(rate) >= 0.0005:
        tag = "⚡ elevated against trade"
    elif tw and abs(rate) >= 0.0005:
        tag = "✅ tailwind"
    else:
        tag = "✅ neutral"
    return f"Funding: {pct:+.4f}%/8h  {tag}"


def format_oi(oi_usd: float | None) -> str:
    if oi_usd is None:
        return "OI: n/a"
    if oi_usd >= 1e9:
        return f"OI: ${oi_usd/1e9:.2f}B"
    if oi_usd >= 1e6:
        return f"OI: ${oi_usd/1e6:.1f}M"
    return f"OI: ${oi_usd:,.0f}"


# ═══════════════════════════════════════════════════════════════════
# ATR HISTORY
# ═══════════════════════════════════════════════════════════════════

def update_atr_history(state: dict, symbol: str, atr_pct: float):
    with _state_lock:
        h = state.setdefault("atr_history", {}).setdefault(symbol, [])
        h.append({"ts": int(time.time()), "atr_pct": atr_pct})
        if len(h) > ATR_HIST_DEPTH:
            state["atr_history"][symbol] = h[-ATR_HIST_DEPTH:]


def get_atr_percentile(state: dict, symbol: str, atr_pct: float) -> float | None:
    """
    Q6: Returns ATR percentile based on ATR_HIST_DEPTH (168) samples.
    Returns 0.5 (midpoint) when fewer than 30 samples have been collected,
    rather than None, to allow regime-aware adjustments to activate sooner.
    Returns None only when there is genuinely no history at all.
    """
    with _state_lock:
        hist = list(state.get("atr_history", {}).get(symbol, []))
    vals = sorted(e["atr_pct"] for e in hist)
    if not vals:
        return None
    # Q6: Graceful fallback — fewer than 30 samples → midpoint percentile
    if len(vals) < 30:
        return 0.5
    return sum(1 for v in vals if v < atr_pct) / len(vals)


# ═══════════════════════════════════════════════════════════════════
# BTC REGIME FILTER
# ═══════════════════════════════════════════════════════════════════

_btc_regime_cache: dict | None = None
_btc_regime_lock  = threading.Lock()


def compute_btc_regime(candles_1h: list[dict], candles_4h: list[dict],
                        candles_1d: list[dict] | None = None) -> dict:
    def _arr(cc): return [c["c"] for c in cc]

    c4h = _arr(candles_4h)
    c1h = _arr(candles_1h)

    ef4h = safe(ema(c4h, EMA_FAST)[-1])
    es4h = safe(ema(c4h, EMA_SLOW)[-1])
    ef1h = safe(ema(c1h, EMA_FAST)[-1])
    es1h = safe(ema(c1h, EMA_SLOW)[-1])
    btc_4h_mom = len(c4h) >= 6 and c4h[-2] > c4h[-5]

    btc_bullish       = (ef4h > es4h) and (ef1h > es1h) and btc_4h_mom
    btc_intra_bearish = (ef4h < es4h) and (ef1h < es1h)
    btc_daily_bearish = False

    if candles_1d and len(candles_1d) >= EMA_SLOW:
        c1d = _arr(candles_1d)
        ef1d = safe(ema(c1d, EMA_FAST)[-1])
        es1d = safe(ema(c1d, EMA_SLOW)[-1])
        btc_daily_bearish = ef1d < es1d

    btc_bearish         = btc_intra_bearish and btc_daily_bearish
    btc_intraday_only   = btc_intra_bearish and not btc_daily_bearish

    if btc_bullish:
        label = "BTC Regime: Bullish"
    elif btc_bearish:
        label = "BTC Regime: Bearish (confirmed)"
    elif btc_intraday_only:
        label = "BTC Regime: Mixed (4H dip, daily intact)"
    else:
        label = "BTC Regime: Mixed"

    return {
        "bullish":          btc_bullish,
        "bearish":          btc_bearish,
        "intraday_bearish": btc_intra_bearish,
        "label":            label,
    }


def set_btc_regime(regime: dict):
    global _btc_regime_cache
    with _btc_regime_lock:
        _btc_regime_cache = regime


def get_btc_regime() -> dict | None:
    with _btc_regime_lock:
        return _btc_regime_cache


# ── Q10: BTC Dominance Filter ────────────────────────────────────────────────

_BTC_DOMINANCE_URL = "https://api.coingecko.com/api/v3/global"
_btc_dominance_lock = threading.Lock()
_btc_dominance_last_fetch_ts: float = 0.0
_btc_dominance_cache: float | None = None
_BTC_DOMINANCE_TTL_S = 900.0   # refresh at most once per 15 min


def get_btc_dominance() -> float | None:
    """
    Q10: Fetch BTC dominance (BTC.D) percentage.

    Attempts the CoinGecko /global endpoint. Returns None gracefully if the
    fetch fails or the data is unavailable — callers must handle None by
    skipping the adjustment entirely (fail open, not fail closed).

    Caches for 15 minutes to avoid rate-limiting.
    """
    global _btc_dominance_last_fetch_ts, _btc_dominance_cache
    with _btc_dominance_lock:
        if (time.time() - _btc_dominance_last_fetch_ts) < _BTC_DOMINANCE_TTL_S:
            return _btc_dominance_cache
    try:
        resp = requests.get(_BTC_DOMINANCE_URL, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        btc_d = data.get("data", {}).get("market_cap_percentage", {}).get("btc")
        if btc_d is not None:
            btc_d = float(btc_d)
        with _btc_dominance_lock:
            _btc_dominance_cache = btc_d
            _btc_dominance_last_fetch_ts = time.time()
        return btc_d
    except Exception as e:
        print(f"  [BTC.D] dominance fetch failed (non-critical): {e}")
        with _btc_dominance_lock:
            _btc_dominance_last_fetch_ts = time.time()  # avoid retry-storm
        return None


def update_btc_dominance_history(state: dict, btc_d: float | None):
    """
    Q10: Append the latest BTC dominance reading to the persisted history
    (last 5 readings retained). Skips update if btc_d is None.
    """
    if btc_d is None:
        return
    with _state_lock:
        hist = state.setdefault("btc_dominance_history", [])
        hist.append(btc_d)
        state["btc_dominance_history"] = hist[-5:]


# ── Dynamic BTC correlation ──────────────────────────────────────

_dynamic_low_btc_corr: set[str] = set()
_dynamic_corr_lock = threading.Lock()


def update_dynamic_btc_correlation(symbol: str, candles_4h: list[dict],
                                    btc_candles_4h: list[dict] | None):
    # R4: LOW_BTC_CORR_BASELINE removed. After the first scan, dynamic values
    # are available for all scanned symbols. On first scan only, symbols with
    # no dynamic data receive neutral correlation assumption (no score adjustment).
    if not btc_candles_4h or len(btc_candles_4h) < LOW_BTC_CORR_LOOKBACK_BARS + 2:
        return
    if len(candles_4h) < LOW_BTC_CORR_LOOKBACK_BARS + 2:
        return
    n = LOW_BTC_CORR_LOOKBACK_BARS
    sc = [c["c"] for c in candles_4h[-(n + 1):]]
    bc = [c["c"] for c in btc_candles_4h[-(n + 1):]]
    sr = [(sc[i] - sc[i - 1]) / sc[i - 1] for i in range(1, len(sc)) if sc[i - 1] != 0]
    br = [(bc[i] - bc[i - 1]) / bc[i - 1] for i in range(1, len(bc)) if bc[i - 1] != 0]
    if len(sr) < 10 or len(br) < 10:
        return
    mn = min(len(sr), len(br))
    sr, br = sr[:mn], br[:mn]
    ms_ = sum(sr) / mn;  mb_ = sum(br) / mn
    cov   = sum((s - ms_) * (b - mb_) for s, b in zip(sr, br)) / mn
    vs_   = sum((s - ms_) ** 2 for s in sr) / mn
    vb_   = sum((b - mb_) ** 2 for b in br) / mn
    if vs_ <= 0 or vb_ <= 0:
        return
    corr = cov / (math.sqrt(vs_) * math.sqrt(vb_))
    with _dynamic_corr_lock:
        if abs(corr) < LOW_BTC_CORR_THRESHOLD:
            _dynamic_low_btc_corr.add(symbol)
        else:
            _dynamic_low_btc_corr.discard(symbol)


def get_low_btc_corr_set() -> set[str]:
    # R4: LOW_BTC_CORR_BASELINE removed. On the very first scan with no dynamic
    # data yet, returns an empty set — correlation adjustment is skipped for
    # all symbols until dynamic data is populated.
    # No correlation-based score adjustment for that symbol in the first scan only.
    with _dynamic_corr_lock:
        return _dynamic_low_btc_corr.copy()


# ── Dynamic pairwise correlation clustering ──────────────────────

_dynamic_corr_clusters: list[frozenset[str]] = []
_clusters_lock = threading.Lock()


def compute_pairwise_correlation_matrix(symbols: list[str],
                                         bundles: dict[str, tuple],
                                         candle_idx: int = 1,
                                         lookback: int = LOW_BTC_CORR_LOOKBACK_BARS,
                                         ) -> dict[tuple[str, str], float]:
    """4H candle-based pairwise Pearson correlation. bundles[sym][1] = 4H candles."""
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
            ma_ = sum(ra_) / n;  mb_ = sum(rb_) / n
            cov   = sum((x - ma_) * (y - mb_) for x, y in zip(ra_, rb_)) / n
            va_   = sum((x - ma_) ** 2 for x in ra_) / n
            vb_   = sum((y - mb_) ** 2 for y in rb_) / n
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


def set_dynamic_corr_clusters(clusters: list[set[str]]):
    global _dynamic_corr_clusters
    with _clusters_lock:
        _dynamic_corr_clusters = [frozenset(c) for c in clusters]


def get_dynamic_corr_clusters() -> list[frozenset[str]]:
    with _clusters_lock:
        return list(_dynamic_corr_clusters)


def group_of_dynamic(symbol: str) -> object:
    for cluster in get_dynamic_corr_clusters():
        if symbol in cluster:
            return cluster
    return symbol


def log_corr_clusters(clusters: list[set[str]]):
    multi = [sorted(c) for c in clusters if len(c) > 1]
    if multi:
        print(f"  [CORR] Dynamic clusters (threshold={DYNAMIC_CORR_CLUSTER_THRESHOLD}): {multi}")
    else:
        print(f"  [CORR] No pairs cleared correlation threshold — all singletons.")


def check_btc_regime_filter(direction: str, symbol: str,
                              signal_type: str = "") -> tuple[int, str]:
    if hl_coin(symbol) == "BTC":
        return 0, "BTC Regime: N/A (BTC itself)"
    regime = get_btc_regime()
    if regime is None:
        return 0, "BTC Regime: Unknown"
    label        = regime["label"]
    low_corr_set = get_low_btc_corr_set()

    if direction == "long" and regime["bearish"]:
        if symbol in low_corr_set:
            return 0, f"{label} — counter-trend (exempt, decorrelated)"
        with _rs_lock:
            rs_snap = dict(_rs_snapshot if _rs_snapshot is not None else _rs_scores)
        btc_ret  = rs_snap.get("BTCUSDT")
        coin_ret = rs_snap.get(symbol)
        if btc_ret is not None and coin_ret is not None:
            rs_vs_btc = coin_ret - btc_ret
            if rs_vs_btc >= RS_BEARISH_EXEMPT_PCT:
                return 0, f"{label} — counter-trend (exempt, RS {rs_vs_btc:+.1f}%)"
        return -1, f"{label} — counter-trend (-1)"

    if direction == "short" and regime["bullish"]:
        if symbol in low_corr_set:
            return 0, f"{label} — counter-trend (exempt, decorrelated)"
        with _rs_lock:
            rs_snap = dict(_rs_snapshot if _rs_snapshot is not None else _rs_scores)
        btc_ret  = rs_snap.get("BTCUSDT")
        coin_ret = rs_snap.get(symbol)
        if btc_ret is not None and coin_ret is not None:
            rs_vs_btc = coin_ret - btc_ret
            if rs_vs_btc <= -RS_BEARISH_EXEMPT_PCT:
                return 0, f"{label} — counter-trend (exempt, RS {rs_vs_btc:+.1f}%)"
        return -1, f"{label} — counter-trend (-1)"

    if direction == "long" and regime["bullish"]:
        return +1, f"{label} — tailwind (+1)"
    if direction == "short" and regime["bearish"]:
        return +1, f"{label} — tailwind (+1)"
    return 0, f"{label} — Mixed (0)"


# ═══════════════════════════════════════════════════════════════════
# MARKET BREADTH
# ═══════════════════════════════════════════════════════════════════

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
        return {"breadth_pct": 0.5, "label": "Market Breadth: Unknown"}
    pct = sum(1 for v in results.values() if v) / len(results)
    if pct < BREADTH_WEAK_LONG:
        lbl = f"Market Breadth: {pct*100:.0f}% > EMA50 (Weak)"
    elif pct > BREADTH_WEAK_SHORT:
        lbl = f"Market Breadth: {pct*100:.0f}% > EMA50 (Overbought)"
    else:
        lbl = f"Market Breadth: {pct*100:.0f}% > EMA50 (Healthy)"
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
            label += f" (-{abs(adj)}, {'crowded+weak RS' if rs_weak else 'crowded'})"
        elif pct < BREADTH_WEAK_LONG:
            adj = -1; label += " (-1, weak market)"
    elif direction == "short":
        if pct < BREADTH_EXTREME_SHORT:
            adj = -2; label += " (-2 extreme)"
        elif pct > BREADTH_WEAK_SHORT:
            rs_str = rs_pct is not None and rs_pct >= 0
            adj = -2 if rs_str else -1
            label += f" (-{abs(adj)}, {'crowded+strong RS' if rs_str else 'crowded'})"
    return adj, label


# ═══════════════════════════════════════════════════════════════════
# RELATIVE STRENGTH
# ═══════════════════════════════════════════════════════════════════

_rs_scores:   dict[str, float]       = {}
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
        return {"rs_pct": None, "percentile": None, "score_adj": 0,
                "label": "Relative Strength: N/A"}
    rs = coin_ret - btc_ret
    others = {k: v - btc_ret for k, v in scores.items() if k != "BTCUSDT"}
    all_rs = sorted(others.values())
    n = len(all_rs)
    if n == 0:
        return {"rs_pct": rs, "percentile": 0.5, "score_adj": 0,
                "label": f"Relative Strength: {rs:+.1f}%"}
    try:
        rank = next(i for i, v in enumerate(all_rs) if v >= rs)
        pct  = rank / max(n - 1, 1)
    except StopIteration:
        pct = 1.0

    if pct >= 1.0 - RS_TOP_PERCENTILE:
        adj = 1
    elif pct <= RS_BOTTOM_PERCENTILE:
        adj = -1
    else:
        adj = 0
    return {"rs_pct": rs, "percentile": pct, "score_adj": adj,
            "label": f"Relative Strength: {rs:+.1f}%"}


# ═══════════════════════════════════════════════════════════════════
# WIN-RATE ANALYTICS
# ═══════════════════════════════════════════════════════════════════

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
            "grade": grade,   # I10: multi-tier grade persisted for analytics
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
    for cls in ("Strong Bullish", "Bullish", "Neutral", "Bearish", "Strong Bearish"):
        sub = best_subset(lambda e, c=cls: e.get("daily_class") == c)
        wr, n = wr_for(sub)
        if n >= WIN_RATE_MIN_SAMPLE:
            wrs["by_daily_class"][cls] = {"wr": wr, "n": n}
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
        ("by_symbol",      symbol),
        ("by_type",        signal_type),
        ("by_direction",   direction),
        ("by_daily_class", daily_class),
    ]:
        entry = wrs.get(key, {}).get(lookup)
        if entry:
            wr, n = entry["wr"], entry["n"]
            # F10: Hard suppress for chronically underperforming setups (≥30 samples)
            if (n >= WIN_RATE_HARD_SUPPRESS_MIN_SAMPLE
                    and wr < WIN_RATE_HARD_SUPPRESS_THRESHOLD):
                return {"win_rate": wr, "sample_size": n, "score_adj": -3,
                        "label": (f"Win Rate HARD SUPPRESS: {wr*100:.0f}% "
                                  f"< {WIN_RATE_HARD_SUPPRESS_THRESHOLD*100:.0f}% "
                                  f"(n={n})")}
            if n < WIN_RATE_MIN_SAMPLE_FOR_ADJ:
                return {"win_rate": wr, "sample_size": n, "score_adj": 0,
                        "label": f"Win Rate: {wr*100:.0f}% (n={n}, insufficient for adj)"}
            adj = 1 if wr >= WIN_RATE_HIGH_THRESH else (-1 if wr <= WIN_RATE_LOW_THRESH else 0)
            return {"win_rate": wr, "sample_size": n, "score_adj": adj,
                    "label": f"Win Rate: {wr*100:.0f}%  n={n}  (30d weighted)"}
    return {"win_rate": None, "sample_size": 0, "score_adj": 0,
            "label": "Win Rate: Insufficient data"}


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
        cache = state.get("macro_calendar_cache", {})
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
                print(f"  [MACRO CAL] fetch failed: {e} — using cached/empty")
                return cache.get("events", [])
            time.sleep(min(10.0, 1.0 * 2 ** attempt) + random.uniform(0, 0.25))
    events = []
    for ev in raw:
        if str(ev.get("impact", "")).lower() != "high":
            continue
        # R3: keyword filter removed — impact == "high" is the sole gate.
        dt_utc = parse_ff_event_utc(ev.get("date", ""), ev.get("time", ""))
        if dt_utc:
            events.append({"name": ev.get("title", "?"), "datetime_utc": dt_utc.isoformat()})
    with _state_lock:
        state["macro_calendar_cache"] = {"fetched_at": int(time.time()), "events": events}
    print(f"  [MACRO CAL] Loaded {len(events)} high-impact events")
    return events


def apply_macro_filter(state: dict, atr_pct: float,
                        reference_ms: int | None = None) -> dict:
    events   = fetch_macro_calendar(state)
    ref_ts   = (reference_ms / 1000) if reference_ms is not None else time.time()
    now_utc  = datetime.fromtimestamp(ref_ts, tz=timezone.utc)
    nearest  = None
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
        return {"in_window": False, "score_adj": 0, "label": "Macro Risk: None", "hard_suppress": False}
    hard = atr_pct >= MACRO_HIGH_ATR_SUPPRESS
    if nearest_mins >= 0:
        lbl = f"⚠️ Macro: {nearest} in {int(nearest_mins)} min"
    else:
        lbl = f"⚠️ Macro: {nearest} {int(abs(nearest_mins))} min ago"
    return {"in_window": True, "score_adj": -1, "label": lbl, "hard_suppress": hard}


# ═══════════════════════════════════════════════════════════════════
# S/R LEVELS (using 4H candles now)
# ═══════════════════════════════════════════════════════════════════

SR_PIVOT_LEFT  = 3
SR_PIVOT_RIGHT = 3
SR_LOOKBACK    = 100   # 4H bars (~17 days)
SR_CLUSTER_ATR = 0.30


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
    lb, rb = SR_PIVOT_LEFT, SR_PIVOT_RIGHT
    window = candles_4h[max(0, len(candles_4h) - 1 - SR_LOOKBACK): -1]
    ph, pl = [], []
    for i in range(lb, len(window) - rb):
        h = window[i]["h"]; lo = window[i]["l"]
        if all(h > window[i - k]["h"] for k in range(1, lb + 1)) and \
           all(h > window[i + k]["h"] for k in range(1, rb + 1)):
            ph.append(h)
        if all(lo < window[i - k]["l"] for k in range(1, lb + 1)) and \
           all(lo < window[i + k]["l"] for k in range(1, rb + 1)):
            pl.append(lo)
    eff_atr = atr_val if atr_val and atr_val > 0 else (cur_c * 0.005)
    ph = _cluster_levels(ph, eff_atr, SR_CLUSTER_ATR)
    pl = _cluster_levels(pl, eff_atr, SR_CLUSTER_ATR)
    res = sorted([p for p in ph if p > cur_c], key=lambda x: x - cur_c)[:n_levels]
    sup = sorted([p for p in pl if p < cur_c], key=lambda x: cur_c - x)[:n_levels]
    return sup, res


# ═══════════════════════════════════════════════════════════════════
# SPREAD / LIQUIDITY FILTER
# ═══════════════════════════════════════════════════════════════════

_spread_lock = threading.Lock()


def is_spread_exempt(symbol: str) -> bool:
    # R2: Dynamic spread exemption removed — static SPREAD_EXEMPT set only.
    return symbol in SPREAD_EXEMPT


def update_spread_history_mem(symbol: str, spread_pct: float):
    pass  # R2: dynamic spread history removed; retained as no-op for call-site compatibility


def sync_spread_to_state(state: dict):
    pass  # R2: spread_history state key removed


def load_spread_from_state(state: dict):
    pass  # R2: spread_history state key removed


# ═══════════════════════════════════════════════════════════════════
# ── CORE: DAILY TREND CLASSIFICATION ──────────────────────────────
# This is the top of the 1D → 4H → 1H stack.
# ═══════════════════════════════════════════════════════════════════

def classify_daily_trend(candles_1d: list[dict]) -> dict:
    """
    Classify the daily trend using EMA21/50/200, ADX, ATR, and
    market structure (HH/HL for bull, LH/LL for bear).

    Returns:
        classification: str  — "Strong Bullish" | "Bullish" | "Neutral"
                                | "Bearish" | "Strong Bearish"
        score:          int  — 1..5 (used in scoring pipeline)
        details:        dict — individual component values for display
    """
    if len(candles_1d) < EMA_TREND + 5:
        return {
            "classification": "Neutral",
            "score": 2,
            "allows_long": True,
            "allows_short": True,
            "details": {"reason": "Insufficient daily data"},
        }

    ind = get_cached_indicators("__DAILY__", "1d", candles_1d)
    closes = ind["c"]
    highs  = ind["h"]
    lows   = ind["l"]
    cur    = closes[-1]

    ef  = safe(ind["ema_fast"][-1])
    es  = safe(ind["ema_slow"][-1])
    et  = safe(ind["ema_trend"][-1])
    adx = safe(ind["adx"][-1], 20.0)
    atr_val = safe(ind["atr"][-1], cur * 0.01)

    # Market structure — look at last MS_LOOKBACK_BARS daily bars
    lb    = min(MS_LOOKBACK_BARS, len(closes) - 1)
    h_sub = highs[-lb:]
    l_sub = lows[-lb:]

    # Find pivot highs and lows
    def pivots(arr, fn):  # fn: True for highs (greater), False for lows (less)
        pts = []
        for i in range(1, len(arr) - 1):
            if fn(arr[i], arr[i - 1]) and fn(arr[i], arr[i + 1]):
                pts.append(arr[i])
        return pts

    ph = pivots(h_sub, lambda a, b: a > b)
    pl = pivots(l_sub, lambda a, b: a < b)

    hh_hl = len(ph) >= 2 and ph[-1] > ph[-2] and len(pl) >= 2 and pl[-1] > pl[-2]
    lh_ll = len(ph) >= 2 and ph[-1] < ph[-2] and len(pl) >= 2 and pl[-1] < pl[-2]

    # EMA alignment
    bull_ema    = ef > es > et         # EMA21 > EMA50 > EMA200
    bear_ema    = ef < es < et         # EMA21 < EMA50 < EMA200
    mild_bull   = ef > es and cur > et # partial alignment bullish
    mild_bear   = ef < es and cur < et # partial alignment bearish

    strong_bull = bull_ema and hh_hl and adx >= DAILY_ADX_STRONG
    bullish     = (bull_ema or mild_bull) and (hh_hl or adx >= DAILY_ADX_WEAK) and ef > et
    strong_bear = bear_ema and lh_ll and adx >= DAILY_ADX_STRONG
    bearish     = (bear_ema or mild_bear) and (lh_ll or adx >= DAILY_ADX_WEAK) and ef < et

    if strong_bull:
        cls = "Strong Bullish"
        score = 3
    elif bullish:
        cls = "Bullish"
        score = 2
    elif strong_bear:
        cls = "Strong Bearish"
        score = -3
    elif bearish:
        cls = "Bearish"
        score = -2
    else:
        cls = "Neutral"
        score = 0

    # F1: Neutral daily opens both directions (with a base-score ceiling applied
    # downstream in compute_signals).  Bullish/Bearish remain exclusive.
    neutral_cap  = (cls == "Neutral")
    allows_long  = cls in ("Strong Bullish", "Bullish") or neutral_cap
    allows_short = cls in ("Strong Bearish", "Bearish") or neutral_cap

    return {
        "classification": cls,
        "score":          score,
        "allows_long":    allows_long,
        "allows_short":   allows_short,
        "neutral_cap":    neutral_cap,
        "atr_val":        atr_val,
        "adx":            adx,
        "ef":             ef,
        "es":             es,
        "et":             et,
        "hh_hl":          hh_hl,
        "lh_ll":          lh_ll,
        "details": {
            "ema21": ef, "ema50": es, "ema200": et, "adx": adx,
            "bull_ema": bull_ema, "bear_ema": bear_ema,
            "hh_hl": hh_hl, "lh_ll": lh_ll, "strong": strong_bull or strong_bear,
        },
    }


def score_daily_alignment(daily: dict, direction: str) -> tuple[int, str]:
    """
    Translate daily classification into a score contribution (1–3).
    Returns (score, label_for_breakdown)

    F1: Neutral daily returns 0 — there is no directional alignment
    bonus when the trend is unclear.  This prevents Neutral setups from
    inflating base_score; the neutral_cap ceiling in compute_signals()
    acts as a complementary guard.
    """
    cls = daily["classification"]
    if direction == "long":
        if cls == "Strong Bullish":
            return 3, "1D Strong Bull (+3)"
        elif cls == "Bullish":
            return 2, "1D Bullish (+2)"
        elif cls == "Neutral":
            return 0, "1D Neutral (0)"   # F1: no alignment bonus in consolidation
        else:
            return 1, f"1D {cls} (+1, baseline)"
    else:  # short
        if cls == "Strong Bearish":
            return 3, "1D Strong Bear (+3)"
        elif cls == "Bearish":
            return 2, "1D Bearish (+2)"
        elif cls == "Neutral":
            return 0, "1D Neutral (0)"   # F1: no alignment bonus in consolidation
        else:
            return 1, f"1D {cls} (+1, baseline)"


# ═══════════════════════════════════════════════════════════════════
# ── CORE: 4H SETUP DETECTION ───────────────────────────────────────
# Middle layer.  Detects continuation, pullback, or breakout setups.
# ═══════════════════════════════════════════════════════════════════

def detect_market_structure_4h(candles_4h: list[dict], direction: str) -> dict:
    """
    Q3: Market Structure Shift (MSS) and Break of Structure (BOS) detection on 4H.

    Uses a 3-bar pivot to identify the last 3 swing highs and 3 swing lows.

    For longs:
      • mss_bearish — most recent swing high is lower than the prior swing high
        (bearish MSS: uptrend losing steam)
      • bos_bearish — current close is below the most recent confirmed 4H swing low
        (BOS: structural support broken — suppresses entire setup)

    For shorts (mirrored):
      • mss_bullish — most recent swing low is higher than the prior swing low
      • bos_bullish — current close is above the most recent confirmed 4H swing high

    Returns a dict with boolean flags and descriptive labels.
    """
    if len(candles_4h) < 10:
        return {
            "mss_bearish": False, "mss_bullish": False,
            "bos_bearish": False, "bos_bullish": False,
            "label": "MSS/BOS: N/A (insufficient data)",
        }

    highs  = [c["h"] for c in candles_4h]
    lows   = [c["l"] for c in candles_4h]
    closes = [c["c"] for c in candles_4h]
    cur_c  = closes[-1]
    n      = len(candles_4h)

    # 3-bar pivot: pivot high at i if h[i] > h[i-1] and h[i] > h[i+1]
    swing_highs = [highs[i] for i in range(1, n - 1)
                   if highs[i] > highs[i - 1] and highs[i] > highs[i + 1]]
    swing_lows  = [lows[i]  for i in range(1, n - 1)
                   if lows[i]  < lows[i - 1]  and lows[i]  < lows[i + 1]]

    mss_bearish = mss_bullish = bos_bearish = bos_bullish = False

    if direction == "long":
        # MSS bearish: most recent swing high < prior swing high
        if len(swing_highs) >= 2:
            mss_bearish = swing_highs[-1] < swing_highs[-2]
        # BOS bearish: current close below the most recent confirmed swing low
        if swing_lows:
            bos_bearish = cur_c < swing_lows[-1]
    else:  # short
        # MSS bullish: most recent swing low > prior swing low
        if len(swing_lows) >= 2:
            mss_bullish = swing_lows[-1] > swing_lows[-2]
        # BOS bullish: current close above the most recent confirmed swing high
        if swing_highs:
            bos_bullish = cur_c > swing_highs[-1]

    label_parts = []
    if mss_bearish: label_parts.append("MSS_BEAR")
    if mss_bullish: label_parts.append("MSS_BULL")
    if bos_bearish: label_parts.append("BOS_BEAR")
    if bos_bullish: label_parts.append("BOS_BULL")

    # I2: Confirming MSS — MSS in the trade direction is a quality bonus.
    # A bullish MSS (swing lows stepping higher) confirms a long trade;
    # a bearish MSS (swing highs stepping lower) confirms a short trade.
    confirming_mss = False
    if ENABLE_MSS_CONFIRMING_BONUS:
        if direction == "long" and len(swing_lows) >= 2 and swing_lows[-1] > swing_lows[-2]:
            confirming_mss = True
            label_parts.append("MSS_BULL_CONFIRM(+1)")
        elif direction == "short" and len(swing_highs) >= 2 and swing_highs[-1] < swing_highs[-2]:
            confirming_mss = True
            label_parts.append("MSS_BEAR_CONFIRM(+1)")

    # I3: BOS level tracking — the specific price that was structurally broken.
    # Stores the confirmed swing level for SL refinement and display.
    bos_level    = None
    bos_direction_tag = None
    if ENABLE_BOS_LEVEL_TRACKING:
        if bos_bearish and swing_lows:
            bos_level         = swing_lows[-1]
            bos_direction_tag = "bearish"
        elif bos_bullish and swing_highs:
            bos_level         = swing_highs[-1]
            bos_direction_tag = "bullish"

    return {
        "mss_bearish":       mss_bearish,
        "mss_bullish":       mss_bullish,
        "bos_bearish":       bos_bearish,
        "bos_bullish":       bos_bullish,
        "confirming_mss":    confirming_mss,   # I2: quality bonus flag
        "bos_level":         bos_level,        # I3: structural price level
        "bos_direction":     bos_direction_tag,# I3: direction of the BOS
        "label": "MSS/BOS: " + (", ".join(label_parts) if label_parts else "clean"),
    }


def compute_fib_levels(candles_4h: list[dict], lookback: int = 20) -> dict:
    """
    F7: Compute standard Fibonacci retracement levels from the last
    `lookback` 4H bars.  Levels are expressed as absolute price values.

    Returns {"fib_382": float, "fib_500": float, "fib_618": float}.
    """
    if len(candles_4h) < lookback:
        return {}
    window = candles_4h[-lookback:]
    swing_high = max(c["h"] for c in window)
    swing_low  = min(c["l"] for c in window)
    rng = swing_high - swing_low
    if rng <= 0:
        return {}
    return {
        "fib_382": swing_high - rng * 0.382,
        "fib_500": swing_high - rng * 0.500,
        "fib_618": swing_high - rng * 0.618,
    }


def detect_liquidity_sweep(candles_4h: list[dict], direction: str,
                             atr_4h: float) -> dict:
    """
    I1: Detect a liquidity sweep — price wicks through a recent swing high/low
    and closes back inside, indicating institutional stop-hunt and reversal.

    For longs (bullish sweep):
      • Find the lowest swing low of the last SWEEP_LOOKBACK 4H bars
      • Current candle wicks below  swing_low − 0.3×ATR  (the sweep)
      • Current candle closes above swing_low               (the reversal)

    For shorts (bearish sweep): mirror logic with swing highs.

    Returns:
        type:        "SWEEP" | "NONE"
        score:       3 if sweep confirmed, else 0
        sweep_level: float — the structural level that was tapped
    """
    null = {"type": "NONE", "score": 0, "sweep_level": None}
    if not ENABLE_LIQUIDITY_SWEEPS:
        return null
    if len(candles_4h) < SWEEP_LOOKBACK + 2:
        return null

    window = candles_4h[-(SWEEP_LOOKBACK + 1):-1]  # exclude current bar
    cur    = candles_4h[-1]

    if direction == "long":
        swing_level = min(c["l"] for c in window)
        sweep_ok    = cur["l"] < swing_level - atr_4h * 0.3
        close_ok    = cur["c"] > swing_level
        if sweep_ok and close_ok:
            return {"type": "SWEEP", "score": 3, "sweep_level": swing_level}
    else:  # short
        swing_level = max(c["h"] for c in window)
        sweep_ok    = cur["h"] > swing_level + atr_4h * 0.3
        close_ok    = cur["c"] < swing_level
        if sweep_ok and close_ok:
            return {"type": "SWEEP", "score": 3, "sweep_level": swing_level}

    return null


def detect_vcp(atr_history: list[float], lookback: int = VCP_LOOKBACK) -> dict:
    """
    I4: Volatility Contraction Pattern — identifies multiple successive ATR
    peak contractions that precede high-probability breakouts.

    Requires at least VCP_MIN_STAGES (2) successive contractions where each
    ATR peak is lower than the prior ATR peak within the lookback window.

    Returns:
        vcp:    bool — True if a valid VCP is detected
        stages: int  — number of confirmed contraction stages
    """
    if not ENABLE_VCP or len(atr_history) < lookback:
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


def get_atr_regime(state: dict, symbol: str, atr_pct: float) -> dict:
    """
    I8: Classify current ATR into a volatility regime using percentile history.

    Returns:
        percentile: float | None
        regime:     "low" | "medium" | "high" | "extreme"
        reliable:   bool — True if enough history for statistical validity
    """
    if not ENABLE_DYNAMIC_ATR_REGIME:
        return {"percentile": None, "regime": "medium", "reliable": False}
    pctile = get_atr_percentile(state, symbol, atr_pct)
    if pctile is None:
        return {"percentile": None, "regime": "medium", "reliable": False}
    if pctile < ATR_REGIME_LOW_PCT:
        regime = "low"
    elif pctile < ATR_REGIME_MED_PCT:
        regime = "medium"
    elif pctile < ATR_REGIME_HIGH_PCT:
        regime = "high"
    else:
        regime = "extreme"
    with _state_lock:
        hist_len = len(state.get("atr_history", {}).get(symbol, []))
    return {"percentile": pctile, "regime": regime, "reliable": hist_len >= 48}


def detect_4h_setup(candles_4h: list[dict], daily: dict,
                    direction: str, symbol: str = "__4H__") -> dict:
    """
    Detect 4H setup type and quality.

    Setup types:
      CONT  — Trend continuation (EMA21 > EMA50, RSI healthy, vol rising)
      PULL  — Pullback entry   (price retraces to EMA21 or EMA50; EMA21 touch
               is the higher-probability setup and can reach score 3, EMA50
               touch is capped at score 2 — ported from MTF v0.1)
      BREAK — Breakout continuation (fresh break above/below key level)

    Returns:
        setup_type:  str  — "CONT" | "PULL" | "BREAK" | "NONE"
        score:       int  — 1..3
        details:     dict
    """
    if len(candles_4h) < EMA_SLOW + 5:
        return {"setup_type": "NONE", "score": 0, "details": {}}

    ind = get_cached_indicators(f"{symbol}_4h", "4h", candles_4h)
    c   = ind["c"]; h = ind["h"]; l_ = ind["l"]; v = ind["v"]
    ef  = safe(ind["ema_fast"][-1])
    es  = safe(ind["ema_slow"][-1])
    adx = safe(ind["adx"][-1], 20.0)
    rsi_ = safe(ind["rsi"][-1], 50.0)
    atr_ = safe(ind["atr"][-1], c[-1] * 0.01)
    vm   = safe(ind["vol_ma"][-1])
    cur_v = v[-1]
    cur_c = c[-1]

    # EMA structure
    h4_bull = ef > es
    h4_bear = ef < es

    # EMA trend matches daily
    ema_aligned = (direction == "long" and h4_bull) or (direction == "short" and h4_bear)

    # RSI health (not overbought for longs, not oversold for shorts)
    # Deliberately asymmetric: wide floor (H1_RSI_OS=25) on the permissive
    # side, tight ceiling (H4_RSI_OB=70) on the restrictive side. For longs,
    # this means: don't reject a pullback just because 4H RSI dipped toward
    # oversold (use the wider 1H floor), but DO reject if 4H RSI is already
    # overbought (use the tighter 4H ceiling) — avoids chasing an extended
    # move. Mirrored for shorts. This is intentional, not a constant mismatch.
    rsi_healthy = (direction == "long" and H1_RSI_OS < rsi_ < H4_RSI_OB) or \
                  (direction == "short" and H4_RSI_OS < rsi_ < H1_RSI_OB)

    # Volume vs average
    vol_ok = vm > 0 and cur_v >= vm * H4_VOL_MULT

    # ADX confirms trend exists
    adx_ok = adx >= H4_ADX_MIN
    adx_persistent = adx_persistence_bars(ind, H4_ADX_MIN, max_lookback=6) >= ADX_MIN_PERSISTENCE_BARS

    # ── I1: Liquidity Sweep — check before EMA/PULL/BREAK logic ────────
    # A sweep is the highest-priority PULL signal; bypass the standard
    # EMA proximity requirement when a sweep is confirmed.
    sweep = detect_liquidity_sweep(candles_4h, direction, atr_)
    if sweep["type"] == "SWEEP" and ema_aligned:
        return {
            "setup_type":  "PULL",
            "score":       sweep["score"],
            "ema_fast":    ef,
            "ema_slow":    es,
            "adx":         adx,
            "rsi":         rsi_,
            "atr_val":     atr_,
            "vol_ratio":   (cur_v / vm) if vm > 0 else None,
            "ema_aligned": ema_aligned,
            "breakout":    False,
            "near_ema":    True,
            "di_plus_4h":  safe(ind["di_plus"][-1],  25.0),
            "di_minus_4h": safe(ind["di_minus"][-1], 25.0),
            "struct_tags": ["SWEEP"],
            "sweep_level": sweep["sweep_level"],  # I1: for display
            "details": {"reason": f"Liquidity sweep @ {sweep['sweep_level']:.4f}"},
        }

    # ── I8: Dynamic ATR regime — widen pullback zone in low-ATR compression ──
    # In low-ATR regimes price hugs its EMAs; the standard 0.5×ATR zone is
    # too tight and will miss valid pullbacks. Widen to 0.7×ATR.
    # In extreme-ATR regimes, apply a score penalty later.
    atr_regime_now = "medium"
    if ENABLE_DYNAMIC_ATR_REGIME:
        # Use inline percentile since full state isn't available here; regime
        # is estimated from the current ATR value relative to recent bars.
        atr_arr_full = [safe(x) for x in ind["atr"] if not math.isnan(safe(x, float("nan")))]
        if len(atr_arr_full) >= 10:
            sorted_atr = sorted(atr_arr_full[-ATR_HIST_DEPTH:])
            rank = sum(1 for v in sorted_atr if v < atr_)
            pct_ = rank / len(sorted_atr)
            if pct_ < ATR_REGIME_LOW_PCT:
                atr_regime_now = "low"
            elif pct_ < ATR_REGIME_MED_PCT:
                atr_regime_now = "medium"
            elif pct_ < ATR_REGIME_HIGH_PCT:
                atr_regime_now = "high"
            else:
                atr_regime_now = "extreme"

    pull_zone = atr_ * (0.7 if atr_regime_now == "low" else 0.5)


    near_ef = abs(cur_c - ef) <= pull_zone
    near_es = abs(cur_c - es) <= pull_zone

    # Breakout — prev high broken for bulls, prev low for bears
    # Q4: Uses BREAK_LOOKBACK_BARS=30 (~5 days on 4H) instead of the
    # previous 10-bar (~1.7 day) lookback to filter noise signals.
    lookback = BREAK_LOOKBACK_BARS
    if len(c) > lookback + 2:
        prev_high = max(h[-lookback - 2: -1])
        prev_low  = min(l_[-lookback - 2: -1])
        long_breakout  = cur_c > prev_high and direction == "long"
        short_breakout = cur_c < prev_low  and direction == "short"
    else:
        long_breakout = short_breakout = False
    breakout = long_breakout or short_breakout

    # Q4: ATR compression pre-condition — boost BREAK score by 1 when ATR
    # contracted before the breakout (squeeze → expansion pattern).
    atr_arr = ind["atr"]
    contraction = False
    try:
        atr_prior_slice  = [safe(atr_arr[i]) for i in range(-lookback - 5, -lookback + 5)
                            if i < 0 and not math.isnan(atr_arr[i])]
        atr_recent_slice = [safe(atr_arr[i]) for i in range(-10, 0)
                            if not math.isnan(atr_arr[i])]
        if atr_prior_slice and atr_recent_slice:
            avg_prior  = sum(atr_prior_slice)  / len(atr_prior_slice)
            avg_recent = sum(atr_recent_slice) / len(atr_recent_slice)
            contraction = avg_recent < avg_prior * 0.8
    except (IndexError, ZeroDivisionError):
        contraction = False

    # Classify setup type
    if not ema_aligned:
        return {"setup_type": "NONE", "score": 0, "details": {
            "ema_aligned": False, "h4_bull": h4_bull, "h4_bear": h4_bear,
            "rsi": rsi_, "adx": adx, "near_ef": near_ef, "near_es": near_es,
        }}

    # Score setup quality 1–3
    if breakout and vol_ok and adx_ok:
        setup_type = "BREAK"
        score = 3
        # Q4: ATR compression bonus — squeeze before the breakout adds quality
        if contraction:
            score = min(3, score + 1)   # already at 3, cap holds; kept for clarity
        # I4: VCP check — use in-candle ATR series as proxy for the pattern
        if ENABLE_VCP:
            atr_vals_for_vcp = [safe(x) for x in ind["atr"][-VCP_LOOKBACK:] if not math.isnan(safe(x, float("nan")))]
            vcp_result = detect_vcp(atr_vals_for_vcp)
            if vcp_result["vcp"]:
                score = min(3, score + 1)  # VCP confirmed: quality boost (tag added below)
            else:
                score = max(1, score - 1)  # no contraction before breakout: quality penalty
    elif breakout and (vol_ok or adx_ok):
        setup_type = "BREAK"
        score = 2
        # Q4: ATR compression bonus
        if contraction:
            score = min(3, score + 1)
        # I4: VCP for lower-quality breakout
        if ENABLE_VCP:
            atr_vals_for_vcp = [safe(x) for x in ind["atr"][-VCP_LOOKBACK:] if not math.isnan(safe(x, float("nan")))]
            vcp_result = detect_vcp(atr_vals_for_vcp)
            if vcp_result["vcp"]:
                score = min(3, score + 1)

    # NOTE: near_ef and near_es are NOT mutually exclusive — if EMA21 and
    # EMA50 are close together (e.g. early trend, or low-vol chop), both can
    # be true simultaneously. Branch order matters: EMA21 is checked first
    # so it always wins when both are true, which is intentional (EMA21
    # pullback is the higher-quality setup). Do not reorder these branches.
    elif near_ef and rsi_healthy:
        # Pullback to EMA21 — the higher-probability setup in a trend-following
        # system (price hasn't given up much ground). Ported from MTF v0.1's
        # PULLBACK_EMA21 / PULLBACK_EMA50 quality split.
        setup_type = "PULL"
        score = 3 if adx_ok else 2
    elif near_es and rsi_healthy:
        # Pullback to EMA50 — deeper retracement, lower-probability than an
        # EMA21 touch, so it's capped below the score ceiling even with
        # strong ADX.
        setup_type = "PULL"
        score = 2
    elif ema_aligned and rsi_healthy and adx_ok and vol_ok:
        setup_type = "CONT"
        score = 3 if adx_persistent else 2   # single-bar ADX cross caps at 2
        # Q9: Consolidation pre-condition — CONT setups with no prior compression
        # are typically late-trend entries with lower win rates. Require price to
        # have been in a range < 2x ATR for at least 3 of the last 5 bars.
        if len(candles_4h) >= 5:
            recent_highs = [candles_4h[i]["h"] for i in range(-5, 0)]
            recent_lows  = [candles_4h[i]["l"] for i in range(-5, 0)]
            range_compressed = (max(recent_highs) - min(recent_lows)) < (atr_ * 2.0)
            bars_compressed  = sum(1 for i in range(-5, 0)
                                   if (candles_4h[i]["h"] - candles_4h[i]["l"]) < atr_ * 1.5)
            if not range_compressed or bars_compressed < 3:
                score = max(1, score - 1)   # no consolidation precursor — quality deduction
        # I6: Trend Acceleration — EMA slope + ADX velocity adjustment for CONT
        if ENABLE_TREND_ACCELERATION:
            ef_arr = ind.get("ema_fast", [])
            ema_accelerating = False
            if len(ef_arr) >= 6:
                ef_prev = safe(ef_arr[-6])
                if ef_prev != 0:
                    ema_slope = (safe(ef_arr[-1]) - ef_prev) / ef_prev
                    ema_accelerating = ema_slope > EMA_SLOPE_ACCEL_THRESHOLD
            adx_arr_4h = ind.get("adx", [])
            adx_velocity = 0.0
            if len(adx_arr_4h) >= 4:
                adx_velocity = safe(adx_arr_4h[-1]) - safe(adx_arr_4h[-4])
            adx_rising  = adx_velocity >  ADX_VELOCITY_RISING
            adx_falling = adx_velocity <  ADX_VELOCITY_FALLING
            if ema_accelerating and adx_rising:
                score = min(3, score + 1)   # acceleration bonus
            elif adx_falling and adx >= ADX_LATE_TREND_THRESHOLD:
                score = max(1, score - 1)   # late-trend deceleration penalty
    elif ema_aligned and rsi_healthy:
        setup_type = "CONT"
        score = 2
    elif ema_aligned:
        setup_type = "CONT"
        score = 1
    else:
        setup_type = "NONE"
        score = 0

    # F2: EMA200 PULL fallback — only activates when the full chain above
    # produced NONE (meaning: ema_aligned was False, no CONT path fired,
    # and neither EMA21 nor EMA50 zone was detected).  A price that holds
    # above EMA200 (long) or below EMA200 (short) with a clean RSI + ADX
    # remains a valid, if deeper, pullback setup.
    if setup_type == "NONE":
        et4h = safe(ind["ema_trend"][-1]) if "ema_trend" in ind else None
        near_et4h = (et4h is not None
                     and abs(cur_c - et4h) <= atr_ * 0.6
                     and rsi_healthy and adx_ok
                     and ((direction == "long"  and cur_c > et4h)
                          or (direction == "short" and cur_c < et4h)))
        if near_et4h:
            setup_type = "PULL"
            score = 2   # deeper pullback — valid but capped at 2 (PULL_EMA200)

    # F7: Fibonacci retracement PULL fallback — only activates when STILL NONE
    # after the EMA200 check.  Price near a 38.2%, 50%, or 61.8% fib level
    # in a healthy RSI state is a recognisable institutional entry point that
    # the EMA chain misses entirely.
    if setup_type == "NONE" and rsi_healthy:
        fibs = compute_fib_levels(candles_4h)
        near_fib = bool(fibs) and any(
            abs(cur_c - fib_lvl) <= atr_ * 0.4
            for fib_lvl in fibs.values()
        )
        if near_fib:
            setup_type = "PULL"
            score = 1   # Fib-only: lower confidence (PULL_FIB), score 1

    # ── Q3: Market Structure Shift / Break of Structure check ───────
    ms_struct = detect_market_structure_4h(candles_4h, direction)
    struct_tags: list[str] = []

    if direction == "long":
        if ms_struct["bos_bearish"]:
            # Hard suppress: structural support is broken — no long setup valid
            return {"setup_type": "NONE", "score": 0, "details": {
                "reason": "BOS_BEAR: close below 4H swing low — setup suppressed",
            }}
        if ms_struct["mss_bearish"] and setup_type != "NONE":
            score = max(0, score - 2)
            struct_tags.append("MSS_BEAR")
        # I2: Confirming MSS adds +1 quality bonus for longs
        if ms_struct.get("confirming_mss") and ENABLE_MSS_CONFIRMING_BONUS and setup_type != "NONE":
            score = min(3, score + 1)
            struct_tags.append("MSS_BULL_CONFIRM")
    else:  # short
        if ms_struct["bos_bullish"]:
            return {"setup_type": "NONE", "score": 0, "details": {
                "reason": "BOS_BULL: close above 4H swing high — setup suppressed",
            }}
        if ms_struct["mss_bullish"] and setup_type != "NONE":
            score = max(0, score - 2)
            struct_tags.append("MSS_BULL")
        # I2: Confirming MSS adds +1 quality bonus for shorts
        if ms_struct.get("confirming_mss") and ENABLE_MSS_CONFIRMING_BONUS and setup_type != "NONE":
            score = min(3, score + 1)
            struct_tags.append("MSS_BEAR_CONFIRM")

    # I8: Extreme ATR regime — reduce all setup scores by 1
    if atr_regime_now == "extreme" and setup_type != "NONE":
        score = max(1, score - 1)
        struct_tags.append("ATR_EXTREME")

    # Build VCP tag for BREAK setups
    vcp_tag = ""
    if setup_type == "BREAK" and ENABLE_VCP:
        atr_vals_vcp_tag = [safe(x) for x in ind["atr"][-VCP_LOOKBACK:] if not math.isnan(safe(x, float("nan")))]
        vcp_check = detect_vcp(atr_vals_vcp_tag)
        if vcp_check["vcp"]:
            vcp_tag = f"BREAK_VCP{vcp_check['stages']}"

    # Build acceleration tag for CONT setups
    accel_tag = ""
    if setup_type == "CONT" and ENABLE_TREND_ACCELERATION:
        ef_arr_tag = ind.get("ema_fast", [])
        if len(ef_arr_tag) >= 6:
            ef_prev_t = safe(ef_arr_tag[-6])
            if ef_prev_t != 0:
                slope_t = (safe(ef_arr_tag[-1]) - ef_prev_t) / ef_prev_t
                adx_arr_tag = ind.get("adx", [])
                adx_vel_tag = (safe(adx_arr_tag[-1]) - safe(adx_arr_tag[-4])) if len(adx_arr_tag) >= 4 else 0.0
                if slope_t > EMA_SLOPE_ACCEL_THRESHOLD and adx_vel_tag > ADX_VELOCITY_RISING:
                    accel_tag = "CONT_ACCEL"
                elif adx_vel_tag < ADX_VELOCITY_FALLING and adx >= ADX_LATE_TREND_THRESHOLD:
                    accel_tag = "CONT_DECEL"

    if vcp_tag:   struct_tags.append(vcp_tag)
    if accel_tag: struct_tags.append(accel_tag)

    return {
        "setup_type":  setup_type,
        "score":       score,
        "ema_fast":    ef,
        "ema_slow":    es,
        "adx":         adx,
        "rsi":         rsi_,
        "atr_val":     atr_,
        "vol_ratio":   (cur_v / vm) if vm > 0 else None,
        "ema_aligned": ema_aligned,
        "breakout":    breakout,
        "near_ema":    near_ef or near_es,
        # Q1: surface DI values for direction validation in score_adx()
        "di_plus_4h":  safe(ind["di_plus"][-1],  25.0),
        "di_minus_4h": safe(ind["di_minus"][-1], 25.0),
        # Q3: structure tags for display
        "struct_tags": struct_tags,
        # I3: BOS level for SL refinement and display
        "bos_level":   ms_struct.get("bos_level"),
        "bos_direction": ms_struct.get("bos_direction"),
        "sweep_level": None,   # populated by I1 path when applicable
        "atr_regime":  atr_regime_now,   # I8: for downstream use
        "details": {
            "h4_bull": h4_bull, "h4_bear": h4_bear, "adx_ok": adx_ok,
            "rsi_healthy": rsi_healthy, "vol_ok": vol_ok,
            "near_ef": near_ef, "near_es": near_es, "breakout": breakout,
        },
    }


# ═══════════════════════════════════════════════════════════════════
# ── SMART MONEY: ORDER BLOCKS & BREAKER BLOCKS ─────────────────────
# ═══════════════════════════════════════════════════════════════════

def detect_order_blocks(ind_4h: dict, direction: str,
                          max_age_bars: int = 20) -> list[dict]:
    """
    Scan the last `max_age_bars` 4H candles for valid Order Blocks.

    Bullish OB: last bear candle (c < o) immediately before a strong bull
    impulse candle (body > atr * 0.4, volume >= vol_ma * IMPULSE_VOL_MULT).
    Bearish OB: last bull candle (c > o) immediately before a strong bear
    impulse candle, same thresholds.

    Only OBs relevant to `direction` are returned (bullish OBs for
    direction="long", bearish OBs for direction="short").

    Returns a list of zones, most recent first:
        [{"high": float, "low": float, "bar_index": int, "violated": bool}]
    `bar_index` is the absolute index into ind_4h["c"]. The age-out bound
    `start = max(1, n - max_age_bars)` is the primary expiry mechanism —
    zones older than max_age_bars bars are simply never returned.
    """
    c = ind_4h["c"]; o_arr = ind_4h.get("o")
    h = ind_4h["h"]; l_ = ind_4h["l"]; v = ind_4h["v"]
    atr_arr = ind_4h["atr"]; vma_arr = ind_4h["vol_ma"]
    n = len(c)
    zones = []

    if o_arr is None:
        return zones  # open prices required — see _compute_all_indicators

    start = max(1, n - max_age_bars)
    for i in range(start, n):
        atr_i = safe(atr_arr[i], c[i] * 0.01)
        vma_i = safe(vma_arr[i])
        if atr_i <= 0:
            continue
        body_i = abs(c[i] - o_arr[i])
        is_impulse = body_i > atr_i * 0.4 and vma_i > 0 and v[i] >= vma_i * IMPULSE_VOL_MULT
        if not is_impulse:
            continue

        bull_impulse = c[i] > o_arr[i] and (c[i] - o_arr[i]) > atr_i * 0.4
        bear_impulse = c[i] < o_arr[i] and (o_arr[i] - c[i]) > atr_i * 0.4

        prev = i - 1
        if prev < 0:
            continue

        if direction == "long" and bull_impulse and c[prev] < o_arr[prev]:
            zones.append({"high": h[prev], "low": l_[prev],
                          "bar_index": prev, "violated": False})
        elif direction == "short" and bear_impulse and c[prev] > o_arr[prev]:
            zones.append({"high": h[prev], "low": l_[prev],
                          "bar_index": prev, "violated": False})

    # Mark violation: has price closed through the zone since it formed?
    for z in zones:
        for j in range(z["bar_index"] + 1, n):
            if direction == "long" and c[j] < z["low"]:
                z["violated"] = True
                break
            if direction == "short" and c[j] > z["high"]:
                z["violated"] = True
                break

    zones.sort(key=lambda z: z["bar_index"], reverse=True)
    return zones


def detect_breaker_blocks(ind_4h: dict, direction: str,
                            order_blocks: list[dict]) -> list[dict]:
    """
    A Breaker Block is a violated Order Block that price has since
    returned to retest from the opposite side — i.e. the zone's polarity
    has flipped.

    POLARITY CONVENTION (resolved):
    Standard ICT: a bearish breaker forms when a bullish OB is violated
    downward and then retested as resistance (from below). A bullish
    breaker forms when a bearish OB is violated upward and retested as
    support (from above).

    Concrete example: detect_order_blocks(direction="long") returns bullish
    OBs (last bear candle before a bull impulse). If that bullish OB is
    later violated (price closes below its low), and then price wicks back
    UP into the zone and closes back below — that is a BEARISH breaker
    (the old support is now acting as resistance), confirming the bearish
    move. So breaker blocks should be detected for the OPPOSITE direction
    from the original OBs: pass direction="short" to this function when
    feeding in bullish OBs (direction="long" from detect_order_blocks),
    and direction="long" when feeding in bearish OBs.

    In compute_signals() the caller should:
        ob_long  = detect_order_blocks(ind, "long")   # bullish OBs
        bb_short = detect_breaker_blocks(ind, "short", ob_long)  # bearish breakers from violated bullish OBs
        ob_short = detect_order_blocks(ind, "short")  # bearish OBs
        bb_long  = detect_breaker_blocks(ind, "long",  ob_short) # bullish breakers from violated bearish OBs
    Then combine OBs and BBs for the signal's trade direction.

    Returns zones in the same shape as detect_order_blocks(), with an
    added "is_breaker": True marker.
    """
    c = ind_4h["c"]; h = ind_4h["h"]; l_ = ind_4h["l"]
    n = len(c)
    breakers = []
    for z in order_blocks:
        if not z["violated"]:
            continue
        for j in range(z["bar_index"] + 1, n):
            if direction == "long" and l_[j] <= z["high"] and c[j] > z["high"]:
                # Price wicked into a violated bearish OB zone from below
                # and closed above — bullish breaker (zone flipped to support)
                breakers.append({**z, "is_breaker": True, "retest_bar": j})
                break
            if direction == "short" and h[j] >= z["low"] and c[j] < z["low"]:
                # Price wicked into a violated bullish OB zone from above
                # and closed below — bearish breaker (zone flipped to resistance)
                breakers.append({**z, "is_breaker": True, "retest_bar": j})
                break
    return breakers


def detect_ob_bb_tap(candles_1h: list[dict], ind_1h: dict, direction: str,
                       zones: list[dict]) -> dict:
    """
    Check whether the current (most recent closed) 1H candle taps into
    any of the given 4H OB/BB zones and shows a rejection in the trade
    direction — wick into the zone, close back outside it.

    Returns:
        tapped: bool
        zone: dict | None  — the zone that was tapped, if any
        label: str
    """
    if not zones:
        return {"tapped": False, "zone": None, "label": "No OB/BB zones in range"}

    cur = candles_1h[-1]
    for z in zones:
        if direction == "long":
            wicked_in = cur["l"] <= z["high"] and cur["l"] >= z["low"] * 0.998
            rejected  = cur["c"] > z["high"]
            if wicked_in and rejected:
                kind = "Breaker" if z.get("is_breaker") else "Order Block"
                return {"tapped": True, "zone": z,
                        "label": f"{kind} tap + reject (zone {z['low']:.4f}-{z['high']:.4f})"}
        else:
            wicked_in = cur["h"] >= z["low"] and cur["h"] <= z["high"] * 1.002
            rejected  = cur["c"] < z["low"]
            if wicked_in and rejected:
                kind = "Breaker" if z.get("is_breaker") else "Order Block"
                return {"tapped": True, "zone": z,
                        "label": f"{kind} tap + reject (zone {z['low']:.4f}-{z['high']:.4f})"}
    return {"tapped": False, "zone": None, "label": "No OB/BB tap"}


# ═══════════════════════════════════════════════════════════════════
# ── CORE: 1H ENTRY TRIGGER ─────────────────────────────────────────
# Bottom layer.  Confirms entry with candle structure, RSI, volume.
# ═══════════════════════════════════════════════════════════════════

def detect_1h_confirmation(candles_1h: list[dict], direction: str,
                            setup: dict, symbol: str = "__1H__") -> dict:
    """
    1H entry trigger.  All four conditions below must have at least ONE:
      • Bullish/Bearish engulfing candle
      • Strong impulse + volume expansion
      • Swing high/low break
      • RSI cross above/below 50

    Returns:
        confirmed: bool
        score: int  1..3
        triggers: list[str]
        details: dict
    """
    if len(candles_1h) < SWING_LOOKBACK + 5:
        return {"confirmed": False, "score": 0, "triggers": [], "details": {}}

    ind  = get_cached_indicators(f"{symbol}_1h", "1h", candles_1h)
    c    = ind["c"]; o = ind["o"]; h = ind["h"]; l_ = ind["l"]; v = ind["v"]
    rsi_ = safe(ind["rsi"][-1], 50.0)
    rsi_prev = safe(ind["rsi"][-2], 50.0)
    ef   = safe(ind["ema_fast"][-1])
    es   = safe(ind["ema_slow"][-1])
    atr_ = safe(ind["atr"][-1], c[-1] * 0.01)
    vm   = safe(ind["vol_ma"][-1])

    cur_c = c[-1]; cur_o = o[-1]; cur_h = h[-1]; cur_l = l_[-1]; cur_v = v[-1]
    prev_c = c[-2]; prev_o = o[-2]; prev_h = h[-2]; prev_l = l_[-2]

    triggers = []
    score    = 0

    # ── Engulfing ───────────────────────────────────────────────
    cur_body  = abs(cur_c  - cur_o)
    prev_body = abs(prev_c - prev_o)
    cur_range = cur_h - cur_l + 1e-10
    body_ratio = cur_body / cur_range

    if direction == "long":
        bull_engulf = (cur_c > cur_o and              # green
                       cur_c > prev_o and              # closes above prev open
                       cur_o < prev_c and              # opens below prev close
                       cur_body >= prev_body * 0.8 and # body covers prev body
                       body_ratio >= ENGULF_BODY_RATIO)
        if bull_engulf:
            triggers.append("Bullish engulfing")
            score += 1
    else:  # short
        bear_engulf = (cur_c < cur_o and
                       cur_c < prev_o and
                       cur_o > prev_c and
                       cur_body >= prev_body * 0.8 and
                       body_ratio >= ENGULF_BODY_RATIO)
        if bear_engulf:
            triggers.append("Bearish engulfing")
            score += 1

    # ── Strong impulse + volume expansion ───────────────────────
    impulse_vol = vm > 0 and cur_v >= vm * IMPULSE_VOL_MULT
    if direction == "long":
        strong_bull_impulse = (cur_c > cur_o and
                               (cur_c - cur_o) > atr_ * 0.4 and
                               impulse_vol)
        if strong_bull_impulse:
            triggers.append("Strong bull impulse + vol expansion")
            score += 1
    else:
        strong_bear_impulse = (cur_c < cur_o and
                               (cur_o - cur_c) > atr_ * 0.4 and
                               impulse_vol)
        if strong_bear_impulse:
            triggers.append("Strong bear impulse + vol expansion")
            score += 1

    # ── Swing high/low break ─────────────────────────────────────
    lb = min(SWING_LOOKBACK, len(c) - 2)
    if direction == "long":
        swing_high = max(h[-(lb + 2): -1])
        if cur_h > swing_high:
            triggers.append("Swing-high break")
            score += 1
    else:
        swing_low = min(l_[-(lb + 2): -1])
        if cur_l < swing_low:
            triggers.append("Swing-low break")
            score += 1

    # ── RSI confirms direction ───────────────────────────────────
    if direction == "long":
        rsi_ok = rsi_ > H1_RSI_BULL and rsi_ < H1_RSI_OB
        rsi_cross = rsi_prev < H1_RSI_BULL <= rsi_  # crossed above 50
        if rsi_ok:
            if rsi_cross:
                triggers.append("RSI crossed above 50")
            else:
                triggers.append(f"RSI bullish ({rsi_:.0f} > 50)")
            score += 1
    else:
        rsi_ok = rsi_ < H1_RSI_BEAR and rsi_ > H1_RSI_OS
        rsi_cross = rsi_prev > H1_RSI_BEAR >= rsi_
        if rsi_ok:
            if rsi_cross:
                triggers.append("RSI crossed below 50")
            else:
                triggers.append(f"RSI bearish ({rsi_:.0f} < 50)")
            score += 1

    # ── EMA alignment on 1H ──────────────────────────────────────
    h1_ema_aligned = (direction == "long" and ef > es) or (direction == "short" and ef < es)

    # At least one trigger must fire
    confirmed = len(triggers) >= 1 and rsi_ok if direction == "long" else len(triggers) >= 1 and rsi_ok

    # Cap score at 3
    score = min(score, 3)
    if score == 0:
        confirmed = False

    return {
        "confirmed":      confirmed,
        "score":          score,
        "triggers":       triggers,
        "rsi":            rsi_,
        "ema_fast":       ef,
        "ema_slow":       es,
        "h1_ema_aligned": h1_ema_aligned,
        "vol_ratio":      (cur_v / vm) if vm > 0 else None,
        "atr_val":        atr_,
        "details": {
            "rsi_ok": rsi_ok, "rsi": rsi_, "h1_ema_aligned": h1_ema_aligned,
        },
    }


# ═══════════════════════════════════════════════════════════════════
# ── VOLUME + ADX SCORING (cross-timeframe) ─────────────────────────
# ═══════════════════════════════════════════════════════════════════

def volume_rising(ind: dict, bars: int = 3) -> bool:
    """
    True if volume has been non-decreasing in aggregate over the last
    `bars` bars — i.e. the average of the most recent half is >= the
    average of the earlier half of the window. Avoids requiring strict
    bar-over-bar monotonicity, which is too strict for noisy volume data.
    """
    v = ind["v"]
    if len(v) < bars * 2:
        return False
    recent = v[-bars:]
    prior  = v[-bars * 2:-bars]
    if not prior:
        return False
    return (sum(recent) / len(recent)) >= (sum(prior) / len(prior))


def compute_rvol(current_volume: float, symbol: str,
                  current_hour: int, state: dict) -> float:
    """
    I5: Relative Volume normalised by time-of-day.

    Compares current_volume against the average volume observed at the same
    UTC hour from the persisted volume_profile.  Returns 1.0 (neutral) when
    no profile is available yet for the given symbol/hour.
    """
    if not ENABLE_RVOL:
        return 1.0
    profile = state.get("volume_profile", {}).get(symbol, {})
    typical_vol = profile.get(str(current_hour))
    if not typical_vol or typical_vol == 0:
        return 1.0
    return current_volume / typical_vol


def build_volume_profile(symbol: str, candles_1h: list[dict], state: dict):
    """
    I5: Build (or refresh) the per-symbol hourly volume profile from the
    last 7 days of 1H candles (up to 168 bars).  Keyed by hour-of-day string.
    Called once per day from main().
    """
    if not ENABLE_RVOL:
        return
    buckets: dict[str, list[float]] = {}
    for c in candles_1h[-168:]:
        hour_key = str(datetime.fromtimestamp(c["t"] / 1000, tz=timezone.utc).hour)
        buckets.setdefault(hour_key, []).append(c["v"])
    profile = {h: sum(vols) / len(vols) for h, vols in buckets.items()}
    with _state_lock:
        state.setdefault("volume_profile", {})[symbol] = profile


def score_volume(setup: dict, h1_conf: dict, ind_4h: dict | None = None,
                 rvol: float = 1.0) -> tuple[int, str]:
    """Volume score 1–2 based on 4H setup and 1H confirmation, plus a
    small bonus if 4H volume has been rising over the last 3 bars
    (not just elevated on the current bar).

    I5: When RVOL data is available the RVOL value takes precedence over the
    simple volume MA ratio — a spike at 3 AM UTC is weighted relative to that
    hour's typical volume, not the 20-bar SMA.
    """
    vr_4h = setup.get("vol_ratio")
    vr_1h = h1_conf.get("vol_ratio")
    both_strong = (vr_4h is not None and vr_4h >= 1.5 and
                   vr_1h is not None and vr_1h >= IMPULSE_VOL_MULT)
    one_strong  = (vr_4h is not None and vr_4h >= H4_VOL_MULT) or \
                  (vr_1h is not None and vr_1h >= 1.0)
    rising = bool(ind_4h) and volume_rising(ind_4h, bars=3)

    # I5: RVOL override — if we have a valid normalised reading, use it
    if ENABLE_RVOL and rvol != 1.0:
        if rvol >= RVOL_STRONG_THRESHOLD:
            return 2, f"RVOL strong ({rvol:.1f}x time-of-day avg) +2"
        elif rvol >= RVOL_MODERATE_THRESHOLD:
            return 1, f"RVOL moderate ({rvol:.1f}x time-of-day avg) +1"
        else:
            return 0, f"RVOL below average ({rvol:.1f}x time-of-day) (0)"

    if both_strong:
        return 2, f"Volume strong (4H {vr_4h:.1f}x | 1H {vr_1h:.1f}x) +2"
    elif one_strong and rising:
        vr = vr_4h or vr_1h
        return 2, f"Volume ok+rising ({vr:.1f}x, 3-bar uptrend) +2"
    elif one_strong:
        vr = vr_4h or vr_1h
        return 1, f"Volume ok ({vr:.1f}x) +1"
    else:
        return 0, "Volume weak (0)"




def adx_persistence_bars(ind_4h: dict, threshold: float, max_lookback: int = 6) -> int:
    """
    Count how many consecutive bars (most recent first) the 4H ADX has
    held at or above `threshold`. Capped at `max_lookback`.
    """
    adx_arr = ind_4h["adx"]
    count = 0
    for i in range(len(adx_arr) - 1, max(-1, len(adx_arr) - 1 - max_lookback), -1):
        v = adx_arr[i]
        if math.isnan(v) or v < threshold:
            break
        count += 1
    return count


def detect_divergence(ind_4h: dict, direction: str, lookback: int = 14) -> dict:
    """
    Q7: Swing-based RSI/price divergence detection on 4H over `lookback` bars.

    Previous implementation checked only the absolute last bar (if c_window[-1]
    == max(c_window)), which missed divergences that formed 2–5 bars ago — the
    most common institutional divergence pattern. This version uses 3-bar pivot
    swings for accurate detection.

    Bearish divergence (against longs): price makes higher high, RSI makes
    lower high at those same swing points.
    Bullish divergence (against shorts): price makes lower low, RSI makes
    higher low at those same swing points.

    Returns:
        divergent: bool
        label: str
    """
    closes  = ind_4h["c"]
    rsi_arr = ind_4h["rsi"]
    if len(closes) < lookback + 2:
        return {"divergent": False, "label": "Divergence: N/A (insufficient data)"}

    c_window = closes[-lookback:]
    r_window = rsi_arr[-lookback:]
    if any(math.isnan(r) for r in r_window):
        return {"divergent": False, "label": "Divergence: N/A (RSI not seeded)"}

    if direction == "long":
        # Bearish divergence: find 2 most recent swing highs (3-bar pivot)
        swing_highs = [i for i in range(1, len(c_window) - 1)
                       if c_window[i] > c_window[i - 1] and c_window[i] > c_window[i + 1]]
        if len(swing_highs) < 2:
            return {"divergent": False, "label": "Divergence: none (too few swing highs)"}
        sh1, sh2 = swing_highs[-2], swing_highs[-1]
        # Price made higher high, RSI made lower high — bearish divergence
        price_higher = c_window[sh2] > c_window[sh1]
        rsi_lower    = r_window[sh2] < r_window[sh1]
        divergent = price_higher and rsi_lower
        label = "Bearish divergence (price HH, RSI lower)" if divergent else "Divergence: none"
    else:
        # Bullish divergence: find 2 most recent swing lows (against shorts)
        swing_lows = [i for i in range(1, len(c_window) - 1)
                      if c_window[i] < c_window[i - 1] and c_window[i] < c_window[i + 1]]
        if len(swing_lows) < 2:
            return {"divergent": False, "label": "Divergence: none (too few swing lows)"}
        sl1, sl2 = swing_lows[-2], swing_lows[-1]
        # Price made lower low, RSI made higher low — bullish divergence (against short)
        price_lower  = c_window[sl2] < c_window[sl1]
        rsi_higher   = r_window[sl2] > r_window[sl1]
        divergent = price_lower and rsi_higher
        label = "Bullish divergence vs short (price LL, RSI higher)" if divergent else "Divergence: none"

    return {"divergent": divergent, "label": label}


def score_adx(daily: dict, setup: dict, direction: str = "long") -> tuple[int, str]:
    """ADX score 1–2 based on daily and 4H ADX readings.

    Q1: DI+ / DI- direction validation — ADX points are only awarded when
    the dominant DI on the 4H aligns with the trade direction. A rising ADX
    with DI- > DI+ (bearish trend) must not award points to a long trade.
    """
    d_adx  = daily.get("adx", 20.0)
    h4_adx = setup.get("adx", 20.0)

    # ── Q1: DI alignment gate ────────────────────────────────────────
    di_plus_4h  = safe(setup.get("di_plus_4h",  25.0))
    di_minus_4h = safe(setup.get("di_minus_4h", 25.0))
    di_aligned  = (direction == "long"  and di_plus_4h  > di_minus_4h) or \
                  (direction == "short" and di_minus_4h > di_plus_4h)
    if not di_aligned:
        return 0, (f"ADX unconfirmed by DI "
                   f"({di_plus_4h:.0f}+ vs {di_minus_4h:.0f}-)")

    if d_adx >= DAILY_ADX_STRONG and h4_adx >= H4_ADX_MIN:
        return 2, f"ADX strong (1D {d_adx:.0f} | 4H {h4_adx:.0f}) +2"
    elif (d_adx >= DAILY_ADX_WEAK and h4_adx >= H4_ADX_MIN) or d_adx >= DAILY_ADX_STRONG:
        return 1, f"ADX ok (1D {d_adx:.0f} | 4H {h4_adx:.0f}) +1"
    else:
        return 0, f"ADX weak (1D {d_adx:.0f} | 4H {h4_adx:.0f}) (0)"


# ═══════════════════════════════════════════════════════════════════
# SIGNAL RESULT  (data class equivalent)
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
        "score_adjustments",
        "trigger_list",
        "symbol",
        "divergence_label",
        "ob_zone_label",
        "grade",
        "confluence_factors",
        "_bos_level",
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
        self.ob_zone_label: str = ""
        self.grade: str = "C"                          # I10: A / B / C
        self.confluence_factors: list[str] = []        # I9: active factors
        self._bos_level: float | None = None           # I3: BOS structural price level


# ═══════════════════════════════════════════════════════════════════
# ── MAIN SIGNAL PIPELINE: compute_signals ──────────────────────────
# Orchestrates 1D → 4H → 1H → Scoring → Filters
# ═══════════════════════════════════════════════════════════════════

def check_confluence_factors(setup: dict, h1_result: dict,
                               vol_score: int, ob_bb_tapped: bool) -> dict:
    """
    I9: Multi-Factor Confluence — count how many of the key confirmation
    factors are simultaneously present.  A signal with only one dominant
    factor (e.g. excellent daily trend but weak everything else) scores high
    but tends to underperform vs. multi-factor confirmations.

    Factors:
      rsi_recovery    — 1H RSI crossed above/below 50
      volume_expansion — volume score ≥ 1
      ob_bb_tap       — Order Block or Breaker Block tap confirmed
      pullback_setup  — setup type is PULL (structure-based entry)
      adx_confirmed   — 4H ADX ≥ 25 (trend well established)

    Returns:
        factors: list[str] — names of active factors
        count:   int       — number of active factors
    """
    factors: list[str] = []
    # RSI recovery: check for a recent RSI cross (stored in h1_result triggers)
    triggers = h1_result.get("triggers", [])
    if any("crossed" in t.lower() for t in triggers):
        factors.append("rsi_recovery")
    if vol_score >= 1:
        factors.append("volume_expansion")
    if ob_bb_tapped:
        factors.append("ob_bb_tap")
    if setup.get("setup_type") == "PULL":
        factors.append("pullback_setup")
    if safe(setup.get("adx", 0)) >= 25:
        factors.append("adx_confirmed")
    return {"factors": factors, "count": len(factors)}


def compute_signals(symbol: str,
                    candles_1h:  list[dict],
                    candles_4h:  list[dict],
                    candles_1d:  list[dict],
                    state:       dict,
                    record_inputs: bool = True,
                    reference_ms: int | None = None,
                    funding_rate: float | None = None) -> SignalResult:
    """
    Full 1D → 4H → 1H signal pipeline.

    ┌─────────────────────────────────────────────────────────────┐
    │  Phase 1  —  1D Trend Filter                                │
    │    Classify daily trend, allow only aligned direction       │
    ├─────────────────────────────────────────────────────────────┤
    │  Phase 2  —  4H Setup Detection                             │
    │    Find CONT / PULL / BREAK setup on 4H                     │
    ├─────────────────────────────────────────────────────────────┤
    │  Phase 3  —  1H Entry Confirmation                          │
    │    Confirm with candle structure, RSI, volume               │
    ├─────────────────────────────────────────────────────────────┤
    │  Phase 4  —  Scoring (max 13)                               │
    │    Daily(1-3) + 4H(1-3) + 1H(1-3) + Vol(1-2) + ADX(1-2)   │
    ├─────────────────────────────────────────────────────────────┤
    │  Phase 5  —  Filters & Adjustments                          │
    │    BTC regime, breadth, RS, OI, funding, macro, spread      │
    ├─────────────────────────────────────────────────────────────┤
    │  Phase 6  —  Risk Model                                     │
    │    ATR-based TP1/TP2/SL with RR gate                        │
    └─────────────────────────────────────────────────────────────┘
    """
    res = SignalResult()
    res.symbol = symbol

    if not candles_1h or len(candles_1h) < 50:
        return res

    # ── Phase 1: Daily trend classification ─────────────────────
    daily = classify_daily_trend(candles_1d)

    # F8: BTC-regime directional bias for Neutral daily — rather than
    # opening both directions blindly in consolidation, use the confirmed
    # BTC regime to narrow direction when possible.
    if daily.get("neutral_cap"):
        _btc_r_f8 = get_btc_regime() or {}
        if _btc_r_f8.get("bearish"):
            # Neutral daily + BTC bear → shorts only
            daily["allows_long"]  = False
            daily["allows_short"] = True
        elif _btc_r_f8.get("bullish"):
            # Neutral daily + BTC bull → longs only
            daily["allows_long"]  = True
            daily["allows_short"] = False
        # else (BTC mixed/neutral): keep both open as F1 set

    if record_inputs:
        # Record breadth + RS from 4H data
        i4h = get_cached_indicators(symbol, "4h", candles_4h)
        c4h = i4h["c"]
        es4h = safe(i4h["ema_slow"][-1])
        record_breadth_result(symbol, c4h[-1] > es4h)
        if len(c4h) >= 42:
            ret7 = (c4h[-1] - c4h[-42]) / c4h[-42] * 100.0 if c4h[-42] != 0 else 0.0
        else:
            ret7 = 0.0
        record_rs_return(symbol, ret7)

    # Determine possible directions based on daily trend
    possible_directions = []
    if daily["allows_long"]:
        possible_directions.append("long")
    if daily["allows_short"]:
        possible_directions.append("short")

    if not possible_directions:
        # Neutral daily → no trade (core rule: only trend-aligned trades)
        return res

    # ── Phase 2: 4H setup ─────────────────────────────────────
    best_result: SignalResult | None = None

    for direction in possible_directions:
        setup = detect_4h_setup(candles_4h, daily, direction, symbol)
        if setup["setup_type"] == "NONE":
            continue

        # ── Entry-extension gate (PULL setups only) ──────────────────
        # detect_4h_setup() flags pullback proximity using the 4H close, but
        # the 1H trigger below can fire several hours later within the same
        # 4H bar, after price has already run away from the pullback level.
        # Re-check current 1H proximity to the same EMA before treating this
        # as a tight pullback entry.
        if setup["setup_type"] == "PULL":
            h1_ind_check = get_cached_indicators(symbol, "1h", candles_1h)
            cur_c_1h = h1_ind_check["c"][-1]
            atr_1h_check = safe(h1_ind_check["atr"][-1], cur_c_1h * 0.01)
            ref_ema = setup["ema_fast"] if abs(cur_c_1h - setup["ema_fast"]) <= abs(cur_c_1h - setup["ema_slow"]) else setup["ema_slow"]
            extension = abs(cur_c_1h - ref_ema) / atr_1h_check if atr_1h_check > 0 else 0.0
            if extension > PULL_MAX_EXTENSION_ATR:
                continue  # price has already left the pullback zone — this is a chase, not a pullback

        # ── Phase 3: 1H confirmation (two alternative paths) ────────
        h1_conf = detect_1h_confirmation(candles_1h, direction, setup, symbol)

        # ── OB/BB alternative entry path ─────────────────────────────
        # NOTE: this is intentionally NOT persisted in `state` across scans — a
        # zone that already produced a signal this scan won't fire again within
        # the same scan, but a fresh scan run later will re-detect the same zone
        # and could re-trigger if price taps it again. Persisting zone-consumption
        # across scans (so a zone is "used up" permanently after one signal) is a
        # reasonable future improvement but is out of scope for this patch — do
        # not attempt to add cross-scan persistence here without discussing the
        # state-schema implications first.
        i4h_ob = get_cached_indicators(symbol, "4h", candles_4h)
        # Detect OBs for this direction
        order_blocks = detect_order_blocks(i4h_ob, direction)
        # Detect breaker blocks: violated OBs from the OPPOSITE direction
        # (per polarity resolution in detect_breaker_blocks docstring)
        opp = "short" if direction == "long" else "long"
        ob_opposite = detect_order_blocks(i4h_ob, opp)
        breakers = detect_breaker_blocks(i4h_ob, direction, ob_opposite)
        all_zones = order_blocks + breakers

        i1h_ob = get_cached_indicators(symbol, "1h", candles_1h)
        ob_tap = detect_ob_bb_tap(candles_1h, i1h_ob, direction, all_zones)

        # Q8: Zone consumption guard — skip zones that already fired a signal
        # this run or in a prior scan (persisted in state["consumed_ob_zones"]).
        # A zone fingerprint is derived from its bar_index and price boundaries.
        if ob_tap["tapped"] and ob_tap.get("zone"):
            z = ob_tap["zone"]
            zone_id = f"{z['bar_index']}_{z['high']:.4f}_{z['low']:.4f}"
            consumed_zones = state.get("consumed_ob_zones", {}).get(symbol, {})
            if zone_id in consumed_zones:
                # Zone already consumed — suppress OB/BB contribution this scan
                ob_tap = {"tapped": False, "zone": None, "label": "OB/BB zone already consumed"}

        confirmed = h1_conf["confirmed"] or ob_tap["tapped"]
        if not confirmed:
            continue

        # Merge trigger lists so the alternative path is visible in output,
        # not silently absorbed — both paths firing simultaneously should be
        # visible as two entries, not collapsed into one.
        combined_triggers = list(h1_conf.get("triggers", []))
        if ob_tap["tapped"]:
            combined_triggers.append(ob_tap["label"])
            # Q8: Mark zone as consumed so it won't fire again on next scan
            z = ob_tap.get("zone")
            if z:
                bar_index_now_est = int(time.time() * 1000) // INTERVAL_MS["1h"]
                zone_id = f"{z['bar_index']}_{z['high']:.4f}_{z['low']:.4f}"
                with _state_lock:
                    sym_zones = state.setdefault("consumed_ob_zones", {}).setdefault(symbol, {})
                    sym_zones[zone_id] = bar_index_now_est

        combined_score = h1_conf.get("score", 0)
        if ob_tap["tapped"]:
            combined_score = max(combined_score, OB_BB_TAP_SCORE)
        h1_conf = {**h1_conf, "confirmed": True, "score": combined_score,
                   "triggers": combined_triggers}

        # ── Phase 4: Scoring ────────────────────────────────────
        d_score, d_label = score_daily_alignment(daily, direction)
        s_score          = setup["score"]
        h_score          = h1_conf["score"]
        i4h_for_vol      = get_cached_indicators(symbol, "4h", candles_4h)

        # I5: Compute RVOL for the current 1H bar's time-of-day
        cur_hour_utc = datetime.fromtimestamp(
            candles_1h[-1]["t"] / 1000, tz=timezone.utc
        ).hour
        cur_1h_vol = candles_1h[-1]["v"]
        rvol_val   = compute_rvol(cur_1h_vol, symbol, cur_hour_utc, state)

        v_score, v_label = score_volume(setup, h1_conf, i4h_for_vol, rvol=rvol_val)
        a_score, a_label = score_adx(daily, setup, direction)

        # I7: Regime-adaptive scoring weights
        if ENABLE_REGIME_SCORE_WEIGHTS:
            weights     = REGIME_SCORE_WEIGHTS.get(daily["classification"],
                                                    REGIME_SCORE_WEIGHTS["Neutral"])
            v_score_eff = min(2, round(v_score * weights["volume_weight"]))
            a_score_eff = min(2, round(a_score * weights["adx_weight"]))
        else:
            v_score_eff = v_score
            a_score_eff = a_score

        base_score = d_score + s_score + h_score + v_score_eff + a_score_eff
        # base_score is in range 2..13 (min: d=1,s=1,h=1,v=0,a=0 = 3 but
        # realistically if setup and h1 fire, minimum is higher)

        # F1: Neutral daily cap — daily_score is 0 but other components can
        # still accumulate; enforce a ceiling so consolidation setups never
        # pass as high-conviction signals.
        if daily.get("neutral_cap") and base_score > 10:
            continue  # neutral daily: effective base capped at 10

        if base_score < MIN_SIGNAL_SCORE:
            continue  # pre-filter before expensive adjustments

        # I9: Multi-Factor Confluence gate — require ≥ CONFLUENCE_MIN_FACTORS
        if ENABLE_CONFLUENCE_MODEL:
            ob_tapped_for_confluence = ob_tap.get("tapped", False)
            confluence = check_confluence_factors(
                setup, h1_conf, v_score_eff, ob_tapped_for_confluence
            )
            if confluence["count"] < CONFLUENCE_MIN_FACTORS:
                print(f"  [CONFLUENCE] {symbol} {direction.upper()} rejected — "
                      f"only {confluence['count']} factor(s): {confluence['factors']}")
                continue
            # Bonus for strong multi-factor confluence
            if confluence["count"] >= CONFLUENCE_BONUS_FACTORS:
                base_score += 1
        else:
            confluence = {"factors": [], "count": 0}

        # Build candidate result
        cand = SignalResult()
        cand.ob_zone_label = ob_tap["label"] if ob_tap["tapped"] else ""
        cand.symbol      = symbol
        cand.fire_long   = direction == "long"
        cand.fire_short  = direction == "short"
        cand.direction   = direction
        cand.signal_type = setup["setup_type"]
        cand.daily_class = daily["classification"]
        cand.daily_score = d_score
        cand.setup_type  = setup["setup_type"]
        cand.setup_score = s_score
        cand.h1_score    = h_score
        cand.vol_score   = v_score_eff
        cand.adx_score   = a_score_eff
        cand.base_score  = base_score
        cand.confluence_factors = confluence["factors"]  # I9
        cand.trigger_list = (h1_conf["triggers"] +
                              [f"4H {setup['setup_type']} ({d_label})"])

        # ATR from 1H for entry/SL precision
        h1_ind  = get_cached_indicators(symbol, "1h", candles_1h)
        atr_1h  = safe(h1_ind["atr"][-1], candles_1h[-1]["c"] * ATR_FALLBACK_PCT / 100)
        cur_c   = candles_1h[-1]["c"]
        atr_pct = atr_1h / cur_c * 100 if cur_c > 0 else 0.0

        if not (MIN_ATR_PCT <= atr_pct <= MAX_ATR_PCT):
            continue  # dead or manic market

        update_atr_history(state, symbol, atr_pct)

        cand.entry   = cur_c
        cand.atr_val = atr_1h
        cand.atr_pct = atr_pct

        # Prefer higher base score if multiple directions somehow qualify
        if best_result is None or base_score > best_result.base_score:
            best_result = cand
            # I3: carry BOS level through for SL refinement in phase 6
            best_result._bos_level = setup.get("bos_level")

    if best_result is None:
        return res

    res = best_result

    # ── Phase 5: Filters & Adjustments ────────────────────────
    res = _apply_filters_and_adjustments(
        res, state, symbol, candles_4h, daily, funding_rate, reference_ms
    )

    return res


def _apply_filters_and_adjustments(res: SignalResult,
                                     state: dict,
                                     symbol: str,
                                     candles_4h: list[dict],
                                     daily: dict,
                                     funding_rate: float | None,
                                     reference_ms: int | None) -> SignalResult:
    """Apply all score adjustments, filters, and compute risk levels."""
    direction = res.direction
    cur_c     = res.entry
    atr_val   = res.atr_val
    atr_pct   = res.atr_pct
    adjusted  = res.base_score
    adjs      = res.score_adjustments
    price_dir = "up" if res.fire_long else "down"

    # S/R levels from 4H
    sup, resis = find_sr_levels(candles_4h, cur_c, atr_val)
    res.supports    = sup
    res.resistances = resis

    # ── OI trend ─────────────────────────────────────────────────
    oi_data = compute_oi_trend(state, symbol, price_dir, direction)
    res.oi_data = oi_data
    if oi_data["score_adj"] != 0:
        adjusted += oi_data["score_adj"]
        adjs.append((f"OI ({oi_data['tag']})", oi_data["score_adj"], "oi_confirmation"))
    accel = oi_data.get("oi_acceleration")
    oi_contrib = oi_data["score_adj"]
    if accel is not None:
        if (accel > OI_ACCEL_MIN_THRESHOLD and oi_data["oi_trend"] == "rising"
                and oi_contrib > 0 and oi_contrib < OI_SCORE_CAP):
            adjusted  += 1; oi_contrib += 1
            adjs.append(("OI Acceleration ↑", +1, "oi_confirmation"))
        elif (accel < -OI_ACCEL_MIN_THRESHOLD and oi_data["oi_trend"] == "falling"
              and oi_contrib < 0 and oi_contrib > -OI_SCORE_CAP):
            adjusted  -= 1; oi_contrib -= 1
            adjs.append(("OI Acceleration ↓", -1, "oi_confirmation"))

    # ── BTC Regime ───────────────────────────────────────────────
    btc_adj, btc_label = check_btc_regime_filter(direction, symbol, res.signal_type)
    res.btc_regime_label = btc_label
    if btc_adj != 0:
        adjusted += btc_adj
        adjs.append((btc_label, btc_adj, "btc_regime"))

    # ── Q10: BTC Dominance filter ────────────────────────────────
    # Rising BTC.D = alts being rotated out (negative for alt longs).
    # Falling BTC.D = alt season (positive for alt longs).
    # BTC itself is exempt — dominance doesn't apply to BTC signals.
    # If data is unavailable, skip entirely (fail open).
    if symbol != "BTCUSDT":
        btc_d_hist = state.get("btc_dominance_history", [])
        if len(btc_d_hist) >= 2:
            if btc_d_hist[-1] > btc_d_hist[-2] and direction == "long":
                adjusted -= 1
                adjs.append(("BTC.D↑ alt rotation out", -1, "secondary"))
            elif btc_d_hist[-1] < btc_d_hist[-2] and direction == "long":
                adjusted += 1
                adjs.append(("BTC.D↓ alt season", +1, "secondary"))

    # ── RS ───────────────────────────────────────────────────────
    rs_data = compute_relative_strength(symbol)
    res.rs_data = rs_data
    if rs_data["score_adj"] != 0:
        adjusted += rs_data["score_adj"]
        adjs.append((rs_data["label"], rs_data["score_adj"], "rs"))

    # RS hard gate for breakouts
    if res.signal_type == "BREAK":
        rs_pct = rs_data.get("rs_pct")
        if rs_pct is not None and rs_pct < RS_BREAK_HARD_GATE_PCT:
            adjusted += RS_BREAK_HARD_PENALTY
            adjs.append((f"RS break gate ({rs_pct:+.1f}% < {RS_BREAK_HARD_GATE_PCT:.0f}%)",
                         RS_BREAK_HARD_PENALTY, "rs"))

    # ── Market Breadth ───────────────────────────────────────────
    breadth_adj, breadth_label = apply_breadth_adjustment(
        direction, rs_pct=rs_data.get("rs_pct")
    )
    res.breadth_label = breadth_label
    if breadth_adj != 0:
        adjusted += breadth_adj
        adjs.append((breadth_label, breadth_adj, "secondary"))

    # ── Win Rate ─────────────────────────────────────────────────
    wr_data = compute_wr_analytics(state, symbol, direction,
                                    res.signal_type, res.daily_class)
    res.wr_data = wr_data
    if wr_data["score_adj"] != 0:
        adjusted += wr_data["score_adj"]
        adjs.append((wr_data["label"], wr_data["score_adj"], "secondary"))

    # ── Macro filter ─────────────────────────────────────────────
    macro_data = apply_macro_filter(state, atr_pct, reference_ms)
    res.macro_data = macro_data
    if macro_data["hard_suppress"]:
        print(f"  [MACRO] {symbol} hard suppressed — elevated ATR + macro window")
        res.fire_long = res.fire_short = False
        return res
    if macro_data["score_adj"] != 0:
        adjusted += macro_data["score_adj"]
        adjs.append(("Macro risk window", macro_data["score_adj"], "secondary"))

    # ── Divergence ──────────────────────────────────────────────────
    i4h_div = get_cached_indicators(symbol, "4h", candles_4h)
    div_data = detect_divergence(i4h_div, direction)
    res.divergence_label = div_data["label"]
    if div_data["divergent"]:
        adjusted += DIVERGENCE_PENALTY
        adjs.append((div_data["label"], DIVERGENCE_PENALTY, "secondary"))

    # ── Q6: High-ATR regime penalty ────────────────────────────────
    # ATR percentile > 80th = elevated volatility → caution penalty.
    # The percentile is statistically reliable only with ATR_HIST_DEPTH=168
    # samples; Q6 increases the history depth to make this meaningful.
    atr_pctile_check = get_atr_percentile(state, symbol, atr_pct)
    if atr_pctile_check is not None and atr_pctile_check > ATR_HIGH_PERCENTILE:
        adjusted -= 1
        adjs.append((f"ATR high regime ({atr_pctile_check*100:.0f}th pct)", -1, "secondary"))

    # ── Funding ──────────────────────────────────────────────────
    if funding_rate is not None:
        rate     = funding_rate
        headwind = (rate > 0 and direction == "long") or (rate < 0 and direction == "short")
        tailwind = not headwind

        if tailwind and abs(rate) >= FUNDING_CARRY_POS_THRESHOLD:
            adjusted += FUNDING_CARRY_BONUS
            adjs.append((f"Funding tailwind ({rate*100:+.4f}%/8h)",
                         FUNDING_CARRY_BONUS, "secondary"))

        if headwind and abs(rate) >= FUNDING_HEADWIND_THRESHOLD:
            f_trend = get_funding_trend(state, symbol)
            penalty = -2 if f_trend == "rising" else -1
            adjusted += penalty
            adjs.append((f"Funding headwind ({rate*100:+.4f}%/8h)",
                         penalty, "secondary"))

    # ── Penalty / bonus caps (R5: simplified single-pass cap) ───────────────
    # Previous two-pass priority system removed — total caps applied directly.
    # In nearly all real scans total negatives never exceed 3 anyway; the
    # priority ordering only differed in rare edge cases.
    total_positive = sum(a for _, a, _ in adjs if a > 0)
    total_negative = sum(a for _, a, _ in adjs if a < 0)

    capped_positive = min(total_positive, MAX_POSITIVE_ADJUSTMENTS)
    capped_negative = max(total_negative, -MAX_NEGATIVE_ADJUSTMENTS)

    adjusted = res.base_score + capped_positive + capped_negative
    res.final_score = adjusted

    # ── Minimum score gate ────────────────────────────────────────
    btc_regime   = get_btc_regime()
    breadth_pct  = compute_market_breadth()["breadth_pct"]
    eff_min      = MIN_SIGNAL_SCORE
    if btc_regime and btc_regime.get("bearish") and breadth_pct > 0.75:
        eff_min += 1

    if adjusted < eff_min:
        print(f"  [SCORE FILTER] {symbol} {direction.upper()} suppressed: "
              f"base={res.base_score} final={adjusted} < {eff_min}")
        res.fire_long = res.fire_short = False
        return res

    # ── Phase 6: Risk model — ATR-based TP/SL ────────────────────
    atr_pctile = get_atr_percentile(state, symbol, atr_pct)

    # Per-setup-type base multipliers (ported from MTF v0.1): pullback entries
    # get tighter TP1/SL since they enter closer to a structural level than
    # trend-continuation or breakout entries.
    tp1_m, tp2_m, sl_m = SETUP_TP_SL_MULTS.get(
        res.signal_type, (TP1_MULT_CONT, TP2_MULT_CONT, SL_MULT_CONT)
    )

    # Regime-aware multiplier adjustments (applied on top of the setup-type base)
    if atr_pct > HIGH_ATR_THRESHOLD:
        sl_m = SL_HIGH_ATR_MULT
    if btc_regime:
        if btc_regime.get("bearish"):
            tp1_m *= REGIME_BEAR_TP1_MULT
        if btc_regime.get("bullish"):
            tp2_m *= REGIME_BULL_TP2_MULT
    if atr_pctile is not None:
        if atr_pctile > ATR_HIGH_PERCENTILE:
            sl_m *= REGIME_HIGHVOL_SL_MULT

    if res.fire_long:
        res.tp1 = cur_c + atr_val * tp1_m
        res.tp2 = cur_c + atr_val * tp2_m
        atr_based_sl = cur_c - atr_val * sl_m
        # Q5: Structure-based SL — tighten to just below the nearest 4H swing low
        # (if one is available and it produces a tighter stop than the ATR-based SL)
        if res.supports:
            structure_sl = max(res.supports) * 0.998   # 0.2% below the swing low
            res.sl = max(structure_sl, atr_based_sl)   # never wider than ATR-based
        else:
            res.sl = atr_based_sl
        # I3: BOS-level SL — if a bearish BOS level is known, place SL below it
        # (only when it produces a tighter stop than Q5/ATR)
        if ENABLE_BOS_LEVEL_TRACKING:
            bos_lvl = getattr(res, "_bos_level", None)
            if bos_lvl is not None and bos_lvl < cur_c:
                bos_sl = bos_lvl * 0.998
                res.sl = max(bos_sl, res.sl)  # tighter of BOS vs Q5
        # Snap TP1 to nearest resistance if tighter
        if res.resistances:
            nr = res.resistances[0]
            sr_dist = (nr - cur_c) / atr_val
            if 0.2 <= sr_dist < tp1_m:
                res.tp1 = nr
    else:
        res.tp1 = cur_c - atr_val * tp1_m
        res.tp2 = cur_c - atr_val * tp2_m
        atr_based_sl = cur_c + atr_val * sl_m
        # Q5: Structure-based SL — tighten to just above the nearest 4H swing high
        if res.resistances:
            structure_sl = min(res.resistances) * 1.002   # 0.2% above the swing high
            res.sl = min(structure_sl, atr_based_sl)      # never wider than ATR-based
        else:
            res.sl = atr_based_sl
        if res.supports:
            ns = res.supports[0]
            sr_dist = (cur_c - ns) / atr_val
            if 0.2 <= sr_dist < tp1_m:
                res.tp1 = ns

    # R:R gate
    tp1_dist = abs(res.tp1 - cur_c)
    sl_dist  = abs(res.sl  - cur_c)
    if sl_dist > 0:
        rr = tp1_dist / sl_dist
        if rr < MIN_RR_RATIO:
            print(f"  [RR FILTER] {symbol} {direction.upper()} "
                  f"R:R {rr:.2f} < {MIN_RR_RATIO} — suppressed")
            res.fire_long = res.fire_short = False
            return res

    # I10: Multi-tier signal grading
    if ENABLE_MULTI_TIER_GRADING:
        if res.final_score >= GRADE_A_SCORE:
            res.grade = "A"
        elif res.final_score >= GRADE_B_SCORE:
            res.grade = "B"
        else:
            res.grade = "C"

    return res


# ═══════════════════════════════════════════════════════════════════
# COOLDOWN
# ═══════════════════════════════════════════════════════════════════

def check_cooldown(state: dict, symbol: str, direction: str,
                   bar_index: int, candidate_score: int = 0) -> bool:
    with _state_lock:
        active        = list(state.get("active_signals", []))
        last_bar      = state.get("signal_cooldowns", {}).get(f"{symbol}_{direction}")
        last_sl_ts    = state.get("post_loss_cooldown", {}).get(f"{symbol}_{direction}")
        prev_outcome  = state.get("last_signal_outcome", {}).get(f"{symbol}_{direction}", "")

    # F5: Regime-adaptive concurrent cap — extend to 15 in BTC bull + strong breadth
    # (BREADTH_CROWDED_LONG = 0.75 is the "strong breadth" threshold used elsewhere)
    _btc_r_f5 = get_btc_regime() or {}
    _btc_bull_f5 = _btc_r_f5.get("bullish", False)
    _breadth_f5  = compute_market_breadth().get("breadth_pct", 0.0)
    effective_max_concurrent = (
        15 if (_btc_bull_f5 and _breadth_f5 >= BREADTH_CROWDED_LONG)
        else MAX_CONCURRENT_ACTIVE
    )
    active_count = sum(1 for s in active if not s.get("resolved", False))
    if active_count >= effective_max_concurrent:
        print(f"  [MAX CONCURRENT] {hl_coin(symbol)} blocked — "
              f"{active_count}/{effective_max_concurrent} active")
        return False

    # Same symbol+direction already active
    # F6: Allow re-entry if the active signal has expired (sideways outcome,
    # not a loss — the entry rationale may still be valid for another cycle)
    for sig in active:
        if (sig.get("symbol") == symbol and sig.get("direction") == direction
                and not sig.get("resolved")):
            age = bar_index - sig.get("bar_index", 0)
            if age >= SIGNAL_MAX_AGE_1H_BARS:
                continue  # F6: expired — don't block re-entry
            return False

    # Bar cooldown
    # F4: Premium-quality setups (score ≥ PREMIUM_SCORE) re-fire after 1 bar
    # I10: Grade-based cooldowns — Grade A: 1 bar, Grade B: 2 bars, Grade C: 3 bars
    if last_bar is not None:
        bars = bar_index - last_bar
        if prev_outcome in ("tp1", "tp2"):
            req = SIGNAL_COOLDOWN_POST_WIN
        elif ENABLE_MULTI_TIER_GRADING:
            # Determine grade from candidate_score
            if candidate_score >= GRADE_A_SCORE:
                req = 1
            elif candidate_score >= GRADE_B_SCORE:
                req = 2
            else:
                req = 3
        elif candidate_score >= PREMIUM_SCORE:
            req = 1   # F4: premium score → 1-bar cooldown (legacy path)
        else:
            req = SIGNAL_COOLDOWN_1H_BARS
        if bars < req:
            print(f"  [COOLDOWN] {hl_coin(symbol)} {direction.upper()} — "
                  f"{req - bars} bar(s) remaining")
            return False

    # Post-loss cooldown (in seconds)
    if last_sl_ts is not None:
        elapsed = int(time.time()) - last_sl_ts
        if elapsed < PULL_REENTRY_COOLDOWN_S:
            remaining = PULL_REENTRY_COOLDOWN_S - elapsed
            print(f"  [POST-LOSS COOLDOWN] {hl_coin(symbol)} {direction.upper()} — "
                  f"{remaining}s remaining")
            return False

    return True


def update_cooldown(state: dict, symbol: str, direction: str, bar_index: int):
    with _state_lock:
        state.setdefault("signal_cooldowns", {})[f"{symbol}_{direction}"] = bar_index


# ═══════════════════════════════════════════════════════════════════
# ACTIVE SIGNAL TRACKING  (1H candle-based)
# ═══════════════════════════════════════════════════════════════════

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
            "resolved":        False,
            "hist_id":         hist_id,
            "signal_type":     sig.signal_type,
            "entry":           sig.entry,
        })


def check_active_signals(state: dict, bar_index_now: int,
                          scan_reference_ms: int | None = None):
    """Check TP/SL hits using 1H candles."""
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
        tp1_hit   = sig.get("tp1_hit", False)
        last_ts   = sig.get("last_processed_candle_ts",
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
            still_active.append(sig)
            continue

        hist_id = sig.get("hist_id")

        def resolve(outcome: str):
            if hist_id:
                update_signal_result(state, hist_id, outcome)
            sig["resolved"] = True
            if outcome in ("tp1", "tp2", "sl"):
                key = f"{symbol}_{direction}"
                with _state_lock:
                    state.setdefault("last_signal_outcome", {})[key] = outcome
                    if outcome == "sl":
                        state.setdefault("post_loss_cooldown", {})[key] = int(time.time())

        for candle in new:
            ch, cl, co = candle["h"], candle["l"], candle["o"]
            last_ts = candle["t"]

            if direction == "long":
                if not tp1_hit:
                    if ch >= tp1 and cl <= sl_:
                        if abs(sl_ - co) < abs(tp1 - co):
                            react_to_message(msg_id, REACT_SL); resolve("sl"); break
                        else:
                            react_to_message(msg_id, REACT_TP1); tp1_hit = True; sig["tp1_hit"] = True
                    elif ch >= tp1:
                        react_to_message(msg_id, REACT_TP1); tp1_hit = True; sig["tp1_hit"] = True
                    elif cl <= sl_:
                        react_to_message(msg_id, REACT_SL); resolve("sl"); break
                if tp1_hit and ch >= tp2:
                    react_to_message(msg_id, REACT_TP2); resolve("tp2"); break
                if tp1_hit and not sig.get("resolved") and cl <= sl_:
                    resolve("tp1"); break
            else:
                if not tp1_hit:
                    if cl <= tp1 and ch >= sl_:
                        if abs(sl_ - co) < abs(tp1 - co):
                            react_to_message(msg_id, REACT_SL); resolve("sl"); break
                        else:
                            react_to_message(msg_id, REACT_TP1); tp1_hit = True; sig["tp1_hit"] = True
                    elif cl <= tp1:
                        react_to_message(msg_id, REACT_TP1); tp1_hit = True; sig["tp1_hit"] = True
                    elif ch >= sl_:
                        react_to_message(msg_id, REACT_SL); resolve("sl"); break
                if tp1_hit and cl <= tp2:
                    react_to_message(msg_id, REACT_TP2); resolve("tp2"); break
                if tp1_hit and not sig.get("resolved") and ch >= sl_:
                    resolve("tp1"); break

        if not sig.get("resolved"):
            sig["last_processed_candle_ts"] = last_ts
            still_active.append(sig)

    with _state_lock:
        state["active_signals"] = still_active


# ═══════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════

def _sanitize_error(e: Exception) -> str:
    import re
    msg = str(e)
    msg = re.sub(r'https?://\S+', '[URL]', msg)
    return f"{e.__class__.__name__}: {msg[:200]}"


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
                print(f"[TG ERROR] {_sanitize_error(e)}")
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
        print(f"  [REACT ERROR] {_sanitize_error(e)}")


def stars(score: int) -> str:
    capped = max(0, min(score, MAX_SCORE))
    filled = min(capped, 8)
    return "★" * filled + "☆" * max(0, 8 - filled)


RANK_MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}


def _leverage_for_risk(atr_pct: float, account_risk_pct: float,
                        signal_type: str | None = None,
                        grade: str = "C") -> float:
    """
    Approximate leverage sizing for display purposes only.

    I10: Grade-based leverage scaling —
      Grade A: 100% of computed leverage (full sizing)
      Grade B:  80% of computed leverage (standard sizing)
      Grade C:  60% of computed leverage (reduced sizing, monitor-only)

    NOTE: regime-aware SL overrides applied in _apply_filters_and_adjustments
    are not reflected here — display only.
    """
    _, _, sl_m = SETUP_TP_SL_MULTS.get(
        signal_type, (TP1_MULT_CONT, TP2_MULT_CONT, SL_MULT_CONT)
    )
    sl_pct = atr_pct * sl_m
    if sl_pct <= 0:
        return 1.0
    raw_lev = min(max(1.0, account_risk_pct / sl_pct), LEVERAGE_MAX)
    if ENABLE_MULTI_TIER_GRADING:
        factor = {
            "A": GRADE_A_LEVERAGE_FACTOR,
            "B": GRADE_B_LEVERAGE_FACTOR,
        }.get(grade, GRADE_C_LEVERAGE_FACTOR)
        raw_lev = max(1.0, raw_lev * factor)
    return raw_lev


def format_signal(symbol: str, sig: SignalResult,
                  engine_tag: str = "V1", rank: int = 0) -> str:
    # I10: Grade-based emoji and label
    if ENABLE_MULTI_TIER_GRADING and sig.grade == "A":
        base_emoji = "🟢" if sig.fire_long else "🔴"
        grade_tag  = " ⭐ GRADE A"
    elif ENABLE_MULTI_TIER_GRADING and sig.grade == "B":
        base_emoji = "🔵"
        grade_tag  = " GRADE B"
    else:
        base_emoji = "⚪"
        grade_tag  = " GRADE C (monitor)"
    direction = "▲ LONG" if sig.fire_long else "▼ SHORT"
    emoji     = base_emoji
    ts        = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    dir_str   = sig.direction

    def fmt(v):
        if v >= 1000:  return f"{v:,.2f}"
        if v >= 1:     return f"{v:.4f}"
        return f"{v:.6f}"

    lev      = _leverage_for_risk(sig.atr_pct, LEVERAGE_BASE_RISK_PCT,
                                   sig.signal_type, sig.grade)
    lev_lo   = _leverage_for_risk(sig.atr_pct, LEVERAGE_RANGE_LOW_PCT,
                                   sig.signal_type, sig.grade)
    lev_hi   = _leverage_for_risk(sig.atr_pct, LEVERAGE_RANGE_HIGH_PCT,
                                   sig.signal_type, sig.grade)
    lev_str  = f"{lev:.1f}x"
    lev_band = f"{int(round(lev_lo))}x–{int(round(lev_hi))}x"

    tp1_dist = abs(sig.tp1 - sig.entry)
    sl_dist  = abs(sig.sl  - sig.entry)
    rr       = tp1_dist / sl_dist if sl_dist > 0 else 0.0

    sr_block = ""
    if sig.resistances:
        sr_block += "🔴 Resistance: " + "  |  ".join(f"<code>{fmt(r)}</code>" for r in sig.resistances) + "\n"
    if sig.supports:
        sr_block += "🟢 Support:    " + "  |  ".join(f"<code>{fmt(s)}</code>" for s in sig.supports) + "\n"
    if sig.ob_zone_label:
        sr_block += f"📦 {sig.ob_zone_label}\n"
    # I1: Sweep level display
    sweep_lvl = getattr(sig, "_sweep_level", None)
    if sweep_lvl:
        sr_block += f"💧 Sweep level: <code>{fmt(sweep_lvl)}</code>\n"
    # I3: BOS level display
    bos_lvl = getattr(sig, "_bos_level", None)
    if bos_lvl:
        sr_block += f"⚡ BOS @ <code>{fmt(bos_lvl)}</code>\n"
    if sr_block:
        sr_block = "\n" + sr_block

    adj_trail = ""
    if sig.score_adjustments:
        parts = []
        for lbl, adj, _ in sig.score_adjustments:
            sign = "+" if adj > 0 else ""
            safe_lbl = lbl.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            parts.append(f"{safe_lbl}: {sign}{adj}")
        adj_trail = "\n<i>Adjustments: " + "  |  \n  ".join(parts) + "</i>"

    spread_line = ""
    if sig.spread_pct is not None:
        tag = "⚠️ elevated" if sig.spread_pct >= SPREAD_WARN_PCT else "✅ tight"
        spread_line = f"\nSpread: {sig.spread_pct:.3f}%  {tag}"

    triggers_str = " | ".join(sig.trigger_list) if sig.trigger_list else "N/A"

    # I9: Confluence factors display
    confluence_line = ""
    if ENABLE_CONFLUENCE_MODEL and sig.confluence_factors:
        confluence_line = f"\n<b>Confluence Factors:</b> {', '.join(sig.confluence_factors)}"

    medal    = RANK_MEDALS.get(rank, "")
    rank_tag = f"{medal} <b>Priority #{rank}</b>\n" if rank else ""

    premium_tag = " ⚡ PREMIUM" if sig.final_score >= PREMIUM_SCORE else ""

    # Score breakdown string
    score_bd = (f"1D:{sig.daily_score} + 4H:{sig.setup_score} + 1H:{sig.h1_score} + "
                f"VOL:{sig.vol_score} + ADX:{sig.adx_score} = {sig.base_score} base → {sig.final_score} final")

    return (
        f"{rank_tag}{emoji} <b>{direction} [{sig.signal_type}]{premium_tag}{grade_tag}</b>  {stars(sig.final_score)}\n"
        f"<b>Pair:</b>  {symbol}   |   <b>Daily:</b> {sig.daily_class}\n\n"
        f"<b>Entry:</b> <code>{fmt(sig.entry)}</code>\n"
        f"<b>TP1:</b>   <code>{fmt(sig.tp1)}</code>  (R:R {rr:.1f})\n"
        f"<b>TP2:</b>   <code>{fmt(sig.tp2)}</code>\n"
        f"<b>SL:</b>    <code>{fmt(sig.sl)}</code>   (ATR {sig.atr_pct:.2f}%)\n\n"
        f"<b>Leverage:</b> {lev_str}   <b>Range:</b> {lev_band}\n"
        f"<b>Score:</b> {sig.final_score}/{MAX_SCORE}{premium_tag}  [Grade {sig.grade}]\n"
        f"<i>{score_bd}</i>\n\n"
        f"<b>Entry Triggers:</b> {triggers_str}"
        f"{confluence_line}\n\n"
        f"<b>Signal Context</b>\n"
        f"{sig.oi_data.get('label', 'OI: Unknown')}\n"
        f"{sig.btc_regime_label or 'BTC Regime: Unknown'}\n"
        f"{sig.divergence_label or 'Divergence: none'}\n"
        f"{sig.breadth_label    or 'Market Breadth: Unknown'}\n"
        f"{sig.rs_data.get('label', 'RS: N/A')}\n"
        f"{sig.wr_data.get('label', 'Win Rate: N/A')}\n"
        f"{sig.macro_data.get('label', 'Macro: None') if sig.macro_data else 'Macro: None'}\n"
        f"{format_funding(sig.funding_rate, dir_str)}\n"
        f"{format_oi(sig.open_interest)}"
        f"{spread_line}"
        f"{adj_trail}"
        f"{sr_block}\n"
        f"<b>Pre-Trade Checklist</b>\n"
        f"✅ Daily trend confirmed ({sig.daily_class})\n"
        f"✅ 4H setup: {sig.setup_type}\n"
        f"✅ 1H trigger(s): {triggers_str}\n"
        f"✅ ATR-based SL set  ✅ R:R ≥ {MIN_RR_RATIO}\n\n"
        f"<i>Swing Engine {__version__} [1D/4H/1H] • Hyperliquid Perps • {ts}</i>"
    )


# ═══════════════════════════════════════════════════════════════════
# CORRELATION DEDUPLICATION
# ═══════════════════════════════════════════════════════════════════

def priority_score(sig: SignalResult) -> tuple:
    rs    = sig.rs_data.get("rs_pct") or 0.0
    wr    = sig.wr_data.get("win_rate") or 0.5
    oi_ok = sig.oi_data.get("score_adj", 0) > 0
    return (sig.final_score, round(wr, 2), int(oi_ok), rs, sig.symbol)


def deduplicate_correlated(signals: list[tuple]) -> list[tuple]:
    signals.sort(key=lambda t: priority_score(t[2]), reverse=True)
    seen:   set[tuple] = set()
    result: list[tuple] = []

    for tup in signals:
        sym, dirn, sig_ = tup
        group = group_of_dynamic(sym)
        key   = (group, dirn)
        if key not in seen:
            seen.add(key)
            result.append(tup)

    # Resolve opposite-direction conflicts inside same cluster
    by_group: dict = {}
    for tup in result:
        g = group_of_dynamic(tup[0])
        by_group.setdefault(g, []).append(tup)

    dropped: set[int] = set()
    for g, tuples in by_group.items():
        longs  = [t for t in tuples if t[1] == "long"]
        shorts = [t for t in tuples if t[1] == "short"]
        if longs and shorts:
            bl = max(longs,  key=lambda t: priority_score(t[2]))
            bs = max(shorts, key=lambda t: priority_score(t[2]))
            loser = bs if priority_score(bl[2]) >= priority_score(bs[2]) else bl
            dropped.add(id(loser))

    if dropped:
        result = [t for t in result if id(t) not in dropped]
    return result


# ═══════════════════════════════════════════════════════════════════
# DAILY SUMMARY
# ═══════════════════════════════════════════════════════════════════

def should_send_summary(state: dict) -> bool:
    now = datetime.now(timezone.utc)
    with _state_lock:
        last = state.get("last_summary_ts", 0)
    if now.hour == 8 and now.minute < 15 and (now.timestamp() - last) > 3600:
        with _state_lock:
            state["last_summary_ts"] = now.timestamp()
        return True
    return False


def send_summary(state: dict):
    cutoff = int(time.time()) - 86400
    with _state_lock:
        # R1: resolved_signals removed — query signal_history filtered to last 24h
        recent = [e for e in state.get("signal_history", [])
                  if e.get("result") in ("tp1", "tp2", "sl")
                  and e.get("timestamp", 0) >= cutoff]
        all_h  = [e for e in state.get("signal_history", [])
                  if e.get("result") in ("tp1", "tp2", "sl")]
    tp1s = sum(1 for e in recent if e.get("result") == "tp1")
    tp2s = sum(1 for e in recent if e.get("result") == "tp2")
    sls  = sum(1 for e in recent if e.get("result") == "sl")
    wins = tp1s + tp2s
    if wins == 0 and sls == 0:
        return
    lines = [
        f"📊 Swing Engine Daily Summary",
        f"✅ Winners: {wins} (🔥×{tp1s}  🏆×{tp2s})",
        f"❌ Losers:  {sls}",
    ]
    if len(all_h) >= WIN_RATE_MIN_SAMPLE:
        w = sum(1 for e in all_h if e.get("result") in ("tp1", "tp2"))
        lines.append(f"📈 Overall Win Rate: {w/len(all_h)*100:.0f}% ({len(all_h)} trades)")
    send_telegram("\n".join(lines))


# ═══════════════════════════════════════════════════════════════════
# SCAN
# ═══════════════════════════════════════════════════════════════════

def collect_market_inputs(symbol: str, state: dict,
                           reference_ms: int) -> tuple | None:
    """Fetch candles + fund meta, record breadth/RS."""
    data = fetch_all_candles(symbol, reference_ms=reference_ms)
    if data is None:
        return None
    candles_1h, candles_4h, candles_1d = data
    # Record breadth + RS inputs during Phase 1 parallel fetch
    ind4h = get_cached_indicators(symbol, "4h", candles_4h)
    c4h   = ind4h["c"]
    es4h  = safe(ind4h["ema_slow"][-1])
    record_breadth_result(symbol, c4h[-1] > es4h)
    if len(c4h) >= 42:
        ret7 = (c4h[-1] - c4h[-42]) / c4h[-42] * 100.0 if c4h[-42] != 0 else 0.0
    else:
        ret7 = 0.0
    record_rs_return(symbol, ret7)

    ctx = get_market_context(symbol)
    if ctx and ctx.get("funding") is not None:
        update_funding_history(state, symbol, ctx["funding"])
    return data


def scan_symbol(symbol: str, state: dict, bar_index_now: int,
                candle_bundle: tuple | None = None,
                reference_ms: int | None = None) -> list[tuple]:
    coin = hl_coin(symbol)

    if candle_bundle is None:
        data = fetch_all_candles(symbol, reference_ms=reference_ms)
        if data is None:
            print(f"    Skipping {coin}: insufficient candles")
            return []
    else:
        data = candle_bundle

    candles_1h, candles_4h, candles_1d = data

    # OI / funding context
    ctx = get_market_context(symbol)
    if ctx:
        # F9: Tiered OI minimum — smaller-cap pairs get a halved floor
        min_oi = MIN_OI_USD_SMALL_CAP if symbol in SMALL_CAP_PAIRS else MIN_OI_USD
        if ctx.get("open_interest") is not None and ctx["open_interest"] < min_oi:
            print(f"    Skipping {coin}: OI too low (${ctx['open_interest']:,.0f} < ${min_oi:,.0f})")
            return []
        update_oi_history(state, symbol, ctx.get("open_interest"))

    live_cache   = get_meta_and_asset_ctxs()
    live_funding = live_cache.get(hl_coin(symbol), {}).get("funding") if live_cache else None
    funding      = live_funding or (ctx.get("funding") if ctx else None)

    # Hard funding suppress
    if FUNDING_SUPPRESS_EXTREME and funding is not None:
        dir_check = None  # we check both
        # We don't know direction yet — check after signal
    # (full suppress is applied inside compute_signals funding block)

    sig = compute_signals(
        symbol, candles_1h, candles_4h, candles_1d,
        state, record_inputs=False,
        reference_ms=reference_ms,
        funding_rate=funding,
    )

    if not (sig.fire_long or sig.fire_short):
        print(f"    {coin}: no signal")
        return []

    direction = sig.direction

    # Funding extreme-headwind hard suppress (now direction is known)
    if funding is not None and FUNDING_SUPPRESS_EXTREME:
        hw = (funding > 0 and direction == "long") or (funding < 0 and direction == "short")
        if hw and abs(funding) >= FUNDING_SUPPRESS_EXTREME:
            print(f"    [FUNDING FILTER] {coin} {direction.upper()} suppressed — "
                  f"extreme headwind {funding*100:+.4f}%/8h")
            return []

    if not check_cooldown(state, symbol, direction, bar_index_now, sig.final_score):
        print(f"    {coin} signal suppressed by cooldown")
        return []

    # Attach market context
    if ctx:
        sig.funding_rate  = funding
        sig.open_interest = ctx.get("open_interest")

    # Spread filter
    if not is_spread_exempt(symbol):
        live_mark = live_cache.get(hl_coin(symbol), {}).get("mark_px") if live_cache else None
        if live_mark is not None and live_mark > 0:
            spread_pct = abs(live_mark - candles_1h[-1]["c"]) / candles_1h[-1]["c"] * 100.0
            sig.spread_pct = spread_pct
            update_spread_history_mem(symbol, spread_pct)
            if spread_pct >= SPREAD_SUPPRESS_PCT:
                print(f"    [SPREAD] {coin} hard suppressed — {spread_pct:.3f}%")
                return []
            elif spread_pct >= SPREAD_WARN_PCT:
                sig.final_score -= 1
                sig.score_adjustments.append(
                    (f"Spread warn ({spread_pct:.3f}%)", -1, "secondary"))
                if sig.final_score < MIN_SIGNAL_SCORE:
                    print(f"    [SPREAD] {coin} suppressed after spread penalty")
                    return []

    print(f"    🚀 SIGNAL: {coin} {direction.upper()} [{sig.signal_type}] "
          f"base={sig.base_score} final={sig.final_score} daily={sig.daily_class}")
    return [(symbol, direction, sig)]


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

_shutdown = False


def _handle_sigterm(signum, frame):
    global _shutdown
    _shutdown = True


os_signal.signal(os_signal.SIGTERM, _handle_sigterm)


def get_dynamic_max_signals(btc_regime: dict | None, breadth_pct: float) -> int:
    if btc_regime is None:
        return MAX_SIGNALS_PER_SCAN
    bullish = btc_regime.get("bullish", False)
    bearish = btc_regime.get("bearish", False)
    if (bullish and breadth_pct > BREADTH_BULL_THRESHOLD) or \
       (bearish and breadth_pct < (1 - BREADTH_BULL_THRESHOLD)):
        return MAX_SIGNALS_BULL_TREND
    return MAX_SIGNALS_PER_SCAN


def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Swing Engine v{__version__} starting…")
    print(f"Timeframe stack: 1D → 4H → 1H")
    print(f"Watchlist ({len(WATCHLIST)} pairs): {[hl_coin(s) for s in WATCHLIST]}")
    print(f"Score threshold: {MIN_SIGNAL_SCORE}/{MAX_SCORE}   Premium: {PREMIUM_SCORE}+")

    ref_ms        = int(time.time() * 1000)
    bar_index_now = ref_ms // INTERVAL_MS["1h"]
    state         = load_state()

    load_spread_from_state(state)
    prune_state(state)

    reset_breadth_cache()
    reset_rs_cache()
    reset_win_rates_cache()
    clear_indicator_cache()

    print("[INIT] Fetching market context…")
    get_meta_and_asset_ctxs()

    if _shutdown:
        save_state(state)
        sys.exit(0)

    print("[TRACK] Checking active signals (1H bars)…")
    check_active_signals(state, bar_index_now, ref_ms)
    save_state(state)

    if should_send_summary(state):
        send_summary(state)
        save_state(state)

    # ── Phase 1: Parallel candle collection + breadth/RS ─────────
    print("[PHASE 1] Collecting candles, market breadth, RS…")
    candle_bundles: dict[str, tuple] = {}

    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futs = {ex.submit(collect_market_inputs, sym, state, ref_ms): sym
                for sym in WATCHLIST}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                bundle = fut.result()
                if bundle is not None:
                    candle_bundles[sym] = bundle
            except Exception as e:
                print(f"    ERROR collecting {sym}: {e}")

    finalize_breadth_cache()
    finalize_rs_cache()
    print(f"  Breadth: {len(_breadth_snapshot or {})} symbols  |  "
          f"RS: {len(_rs_snapshot or {})} symbols")

    # ── I5: Volume profile refresh (once per day) ─────────────────
    if ENABLE_RVOL:
        last_vp_build = state.get("volume_profile_built_at", 0)
        if int(time.time()) - last_vp_build >= VOLUME_PROFILE_TTL_S:
            print("[I5] Rebuilding volume profiles (RVOL)…")
            for sym, bundle in candle_bundles.items():
                try:
                    build_volume_profile(sym, bundle[0], state)  # bundle[0] = 1H
                except Exception as e:
                    print(f"  [I5] Failed to build profile for {sym}: {e}")
            with _state_lock:
                state["volume_profile_built_at"] = int(time.time())
            print(f"  [I5] Volume profiles built for {len(candle_bundles)} symbols")

    # BTC dynamic correlation
    btc_bundle = candle_bundles.get("BTCUSDT")
    btc_4h     = btc_bundle[1] if btc_bundle else None  # bundle[1] = 4H
    if btc_4h:
        for sym, bundle in candle_bundles.items():
            if sym != "BTCUSDT":
                update_dynamic_btc_correlation(sym, bundle[1], btc_4h)

    # Pairwise correlation clustering
    try:
        _matrix   = compute_pairwise_correlation_matrix(WATCHLIST, candle_bundles, candle_idx=1)
        _clusters = cluster_by_correlation(WATCHLIST, _matrix)
        set_dynamic_corr_clusters(_clusters)
        log_corr_clusters(_clusters)
    except Exception as e:
        print(f"  [CORR] clustering failed, using singletons: {e}")
        set_dynamic_corr_clusters([{s} for s in WATCHLIST])

    # BTC regime
    print("[INIT] Computing BTC regime…")
    try:
        if btc_bundle is None:
            print("  [BTC REGIME] BTCUSDT unavailable")
        else:
            c1h, c4h_, c1d = btc_bundle
            regime = compute_btc_regime(c1h, c4h_, c1d)
            set_btc_regime(regime)
            print(f"  {regime['label']}")
    except Exception as e:
        print(f"  [BTC REGIME] failed: {e}")

    # Q10: BTC Dominance — fetch and persist
    try:
        btc_d = get_btc_dominance()
        if btc_d is not None:
            update_btc_dominance_history(state, btc_d)
            print(f"  [BTC.D] {btc_d:.1f}%")
        else:
            print("  [BTC.D] unavailable — skipping dominance filter this scan")
    except Exception as e:
        print(f"  [BTC.D] failed: {e}")

    if _shutdown:
        save_state(state)
        sys.exit(0)

    # ── Phase 2: Scan symbols ─────────────────────────────────────
    print("[PHASE 2] Scanning for signals (1D → 4H → 1H)…")
    get_meta_and_asset_ctxs()  # refresh live data before scan

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

    # Rank + deduplicate
    pending.sort(key=lambda t: priority_score(t[2]), reverse=True)
    deduped = deduplicate_correlated(pending)

    btc_regime_main  = get_btc_regime()
    breadth_pct_main = compute_market_breadth()["breadth_pct"]
    max_sigs         = get_dynamic_max_signals(btc_regime_main, breadth_pct_main)
    print(f"  [DYNAMIC SIGNALS] Max this scan: {max_sigs} "
          f"(BTC: {btc_regime_main['label'] if btc_regime_main else 'Unknown'}, "
          f"breadth: {breadth_pct_main*100:.0f}%)")

    top      = deduped[:max_sigs]
    dropped  = deduped[max_sigs:]

    if dropped:
        print(f"  [RANK] Dropped {len(dropped)} lower-priority signal(s): "
              f"{[f'{hl_coin(s)} {d.upper()}' for s, d, _ in dropped]}")
        for sym, dirn, sig_ in dropped:
            record_signal_history(
                state, sym, dirn, sig_.signal_type, sig_.final_score,
                sig_.funding_rate, sig_.atr_pct,
                sig_.oi_data.get("oi_change_pct"),
                sig_.daily_class, sent=False,
                grade=sig_.grade,
            )

    fired = 0
    for rank, (symbol, direction, sig) in enumerate(top, start=1):
        msg    = format_signal(symbol, sig, "SWING", rank=rank)
        msg_id = send_telegram(msg)

        hist_id = record_signal_history(
            state, symbol, direction, sig.signal_type, sig.final_score,
            sig.funding_rate, sig.atr_pct,
            sig.oi_data.get("oi_change_pct"),
            sig.daily_class, sent=True,
            grade=sig.grade,
        )

        if msg_id:
            update_cooldown(state, symbol, direction, bar_index_now)
            track_signal(state, symbol, direction, msg_id, sig, bar_index_now, hist_id)
            print(f"  [SENT] #{rank} {hl_coin(symbol)} {direction.upper()} "
                  f"score={sig.final_score}  TP1={sig.tp1:.4f}  TP2={sig.tp2:.4f}  SL={sig.sl:.4f}")
        else:
            print(f"  [TG FAIL] #{rank} {hl_coin(symbol)} — Telegram send failed")
        fired += 1
        time.sleep(0.5)

    sync_spread_to_state(state)
    save_state(state)
    print(f"Scan complete. {fired} signal(s) fired.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        try:
            send_telegram(f"🚨 Swing Engine crashed: {e}")
        except Exception:
            pass
        raise
    finally:
        _hl_session.close()
