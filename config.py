"""
Configuration for Bybit Open Interest LONG Scanner v2.
Detects birth of an uptrend via Price + OI confluence.
Signal types: Standard, OI Surge, Pullback Continuation, BB Squeeze.
"""
import os

# ---------- Telegram ----------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0"))

# ---------- Scanner core ----------
SCAN_INTERVAL_MIN = int(os.getenv("SCAN_INTERVAL_MIN", "10"))
ALERT_COOLDOWN_HOURS = int(os.getenv("ALERT_COOLDOWN_HOURS", "6"))
MAX_ALERTS_PER_SCAN = int(os.getenv("MAX_ALERTS_PER_SCAN", "6"))
MIN_STARS_TO_ALERT = int(os.getenv("MIN_STARS_TO_ALERT", "1"))

# ---------- Pre-filter ----------
MIN_AGE_DAYS = int(os.getenv("MIN_AGE_DAYS", "30"))
MIN_VOLUME_USD_24H = float(os.getenv("MIN_VOLUME_USD_24H", "3000000"))

# ---------- STANDARD signal (soft profile: earlier entries) ----------
PRICE_CHANGE_4H_MIN = float(os.getenv("PRICE_CHANGE_4H_MIN", "2.0"))
PRICE_CHANGE_4H_MAX = float(os.getenv("PRICE_CHANGE_4H_MAX", "10.0"))
OI_CHANGE_4H_MIN = float(os.getenv("OI_CHANGE_4H_MIN", "6.0"))
OI_CHANGE_24H_2STAR = float(os.getenv("OI_CHANGE_24H_2STAR", "15.0"))
VOLUME_SPIKE_MIN = float(os.getenv("VOLUME_SPIKE_MIN", "1.2"))
VOLUME_SPIKE_2STAR = float(os.getenv("VOLUME_SPIKE_2STAR", "1.8"))
RSI_4H_MIN = float(os.getenv("RSI_4H_MIN", "48"))
RSI_4H_MAX = float(os.getenv("RSI_4H_MAX", "72"))

# ---------- OI SURGE signal (softer to catch earlier impulse) ----------
ENABLE_OI_SURGE = os.getenv("ENABLE_OI_SURGE", "true").lower() == "true"
SURGE_PRICE_1H_MIN = float(os.getenv("SURGE_PRICE_1H_MIN", "1.0"))
SURGE_PRICE_1H_MAX = float(os.getenv("SURGE_PRICE_1H_MAX", "6.5"))
SURGE_OI_1H_MIN = float(os.getenv("SURGE_OI_1H_MIN", "3.0"))
SURGE_OI_24H_MIN = float(os.getenv("SURGE_OI_24H_MIN", "2.0"))
SURGE_RSI_1H_MAX = float(os.getenv("SURGE_RSI_1H_MAX", "70"))

# ---------- PULLBACK CONTINUATION signal (wider / earlier) ----------
ENABLE_PULLBACK = os.getenv("ENABLE_PULLBACK", "true").lower() == "true"
PULLBACK_RSI_1H_MIN = float(os.getenv("PULLBACK_RSI_1H_MIN", "42"))
PULLBACK_RSI_1H_MAX = float(os.getenv("PULLBACK_RSI_1H_MAX", "58"))
PULLBACK_EMA_DISTANCE_PCT = float(os.getenv("PULLBACK_EMA_DISTANCE_PCT", "2.0"))
PULLBACK_OI_24H_MIN = float(os.getenv("PULLBACK_OI_24H_MIN", "8.0"))
PULLBACK_OI_1H_MIN = float(os.getenv("PULLBACK_OI_1H_MIN", "-1.0"))

# ---------- BB SQUEEZE signal ----------
ENABLE_BB_SQUEEZE = os.getenv("ENABLE_BB_SQUEEZE", "true").lower() == "true"
BB_PERIOD = int(os.getenv("BB_PERIOD", "20"))
BB_MULT = float(os.getenv("BB_MULT", "2.0"))
# Squeeze: bandwidth in lower percentile of lookback OR below absolute max
BB_SQUEEZE_LOOKBACK = int(os.getenv("BB_SQUEEZE_LOOKBACK", "48"))  # 1h bars
BB_SQUEEZE_PERCENTILE = float(os.getenv("BB_SQUEEZE_PERCENTILE", "20"))  # bottom 20%
BB_SQUEEZE_MAX_BW = float(os.getenv("BB_SQUEEZE_MAX_BW", "3.5"))  # % hard cap
# After squeeze: close above upper band, then small pullback entry
BB_PULLBACK_MAX_PCT = float(os.getenv("BB_PULLBACK_MAX_PCT", "1.8"))
BB_PULLBACK_RSI_MAX = float(os.getenv("BB_PULLBACK_RSI_MAX", "62"))
BB_OI_24H_MIN = float(os.getenv("BB_OI_24H_MIN", "5.0"))

# ---------- EMA filter ----------
USE_EMA_FILTER = os.getenv("USE_EMA_FILTER", "true").lower() == "true"
EMA_PERIOD = int(os.getenv("EMA_PERIOD", "50"))
EMA_PULLBACK_PERIOD = int(os.getenv("EMA_PULLBACK_PERIOD", "21"))

# ---------- BTC filter ----------
BTC_MIN_1H_CHANGE = float(os.getenv("BTC_MIN_1H_CHANGE", "-0.5"))

# ---------- Trade parameters ----------
TP1_PCT = float(os.getenv("TP1_PCT", "2.0"))
TP2_PCT = float(os.getenv("TP2_PCT", "5.0"))
HARD_SL_PCT = float(os.getenv("HARD_SL_PCT", "10.0"))  # Aviation-style emergency only
OI_DROP_WARNING_PCT = float(os.getenv("OI_DROP_WARNING_PCT", "5.0"))  # warn if OI -5% in 1h
POSITION_TIMEOUT_HOURS = int(os.getenv("POSITION_TIMEOUT_HOURS", "24"))
POSITION_CHECK_INTERVAL_MIN = int(os.getenv("POSITION_CHECK_INTERVAL_MIN", "5"))

# Smart hold: when in +X% profit, monitor OI health intensively
SMART_HOLD_THRESHOLD_PCT = float(os.getenv("SMART_HOLD_THRESHOLD_PCT", "3.0"))

# ---------- Local ignore ----------
IGNORE_DURATION_HOURS = int(os.getenv("IGNORE_DURATION_HOURS", "24"))

# ---------- Storage ----------
DATA_DIR = os.getenv("DATA_DIR", "/data")
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except Exception:
    DATA_DIR = "."

POSITIONS_FILE = os.getenv("POSITIONS_FILE", os.path.join(DATA_DIR, "oi_positions.json"))
IGNORE_FILE = os.getenv("IGNORE_FILE", os.path.join(DATA_DIR, "oi_ignore.json"))
STATS_FILE = os.getenv("STATS_FILE", os.path.join(DATA_DIR, "oi_stats.json"))

# ---------- Daily report ----------
DAILY_REPORT_HOUR_UTC = int(os.getenv("DAILY_REPORT_HOUR_UTC", "20"))

# ---------- Blacklist ----------
BLACKLIST = {
    "BTC", "ETH", "XRP", "SOL", "BNB", "ADA", "DOGE", "TRX",
    "AVAX", "DOT", "LINK", "MATIC", "LTC", "BCH", "TON",
    "USDC", "USDT", "DAI", "TUSD", "FDUSD", "FOLKS"
}

# ============================================================
# AUTO-TRADING CONFIGURATION
# ============================================================

# Bybit API credentials — set in Railway Variables, NEVER hardcode
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
# demo: https://api-demo.bybit.com | mainnet: https://api.bybit.com
BYBIT_BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api-demo.bybit.com")

# ---------- Risk ----------
DEPOSIT_USD = float(os.getenv("DEPOSIT_USD", "500"))
LEVERAGE = float(os.getenv("LEVERAGE", "10"))
RISK_PCT_PER_TRADE = float(os.getenv("RISK_PCT_PER_TRADE", "1.5"))
POSITION_SIZE_USD = float(os.getenv("POSITION_SIZE_USD", "250"))

# ---------- Auto trade exits ----------
AUTO_TP_PCT = float(os.getenv("AUTO_TP_PCT", "3.0"))
AUTO_HARD_SL_PCT = float(os.getenv("AUTO_HARD_SL_PCT", "2.0"))

# Pullback auto trade
AUTO_PULLBACK_TP_PCT = float(os.getenv("AUTO_PULLBACK_TP_PCT", "1.8"))
AUTO_PULLBACK_SL_PCT = float(os.getenv("AUTO_PULLBACK_SL_PCT", "1.2"))

# BB Squeeze auto trade
AUTO_BB_TP_PCT = float(os.getenv("AUTO_BB_TP_PCT", "2.2"))
AUTO_BB_SL_PCT = float(os.getenv("AUTO_BB_SL_PCT", "1.4"))

# Limits
MAX_AUTO_POSITIONS = int(os.getenv("MAX_AUTO_POSITIONS", "2"))
DAILY_LOSS_LIMIT_USD = float(os.getenv("DAILY_LOSS_LIMIT_USD", "25"))
CONSECUTIVE_LOSS_BLOCK = int(os.getenv("CONSECUTIVE_LOSS_BLOCK", "3"))

# Which signal types are eligible for auto-trade
AUTO_TRADE_SIGNAL_TYPES = os.getenv(
    "AUTO_TRADE_SIGNAL_TYPES", "STANDARD,SURGE,PULLBACK,BB_SQUEEZE"
)

# Reconciliation interval (sec)
RECONCILE_INTERVAL_SEC = int(os.getenv("RECONCILE_INTERVAL_SEC", "30"))

# Post-trade cooldown
POST_TRADE_COOLDOWN_HOURS = int(os.getenv("POST_TRADE_COOLDOWN_HOURS", "48"))

# ---------- Trailing stop after TP1 ----------
AUTO_TRAIL_ENABLED = os.getenv("AUTO_TRAIL_ENABLED", "true").lower() == "true"
AUTO_TP1_TRIGGER_PCT = float(os.getenv("AUTO_TP1_TRIGGER_PCT", "1.5"))
AUTO_TRAIL_DISTANCE_PCT = float(os.getenv("AUTO_TRAIL_DISTANCE_PCT", "1.0"))
AUTO_TP1_TRIGGER_PCT_PB = float(os.getenv("AUTO_TP1_TRIGGER_PCT_PB", "0.9"))
AUTO_TRAIL_DISTANCE_PCT_PB = float(os.getenv("AUTO_TRAIL_DISTANCE_PCT_PB", "0.6"))
AUTO_TP1_TRIGGER_PCT_BB = float(os.getenv("AUTO_TP1_TRIGGER_PCT_BB", "1.1"))
AUTO_TRAIL_DISTANCE_PCT_BB = float(os.getenv("AUTO_TRAIL_DISTANCE_PCT_BB", "0.7"))

# BTC trend filter for auto-entry
BTC_FILTER_ENABLED = os.getenv("BTC_FILTER_ENABLED", "true").lower() == "true"
BTC_FILTER_15M_DROP_MAX = float(os.getenv("BTC_FILTER_15M_DROP_MAX", "0.8"))
BTC_FILTER_15M_PUMP_MAX = float(os.getenv("BTC_FILTER_15M_PUMP_MAX", "1.5"))
BTC_FILTER_1H_VOLATILITY_MAX = float(os.getenv("BTC_FILTER_1H_VOLATILITY_MAX", "1.2"))

# Storage
AUTO_STATE_FILE = os.getenv("AUTO_STATE_FILE", os.path.join(DATA_DIR, "auto_state.json"))
