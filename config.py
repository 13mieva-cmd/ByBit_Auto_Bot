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
SCAN_INTERVAL_MIN = int(os.getenv("SCAN_INTERVAL_MIN", "1"))  # 1 мин ≈ почти непрерывно
ALERT_COOLDOWN_HOURS = int(os.getenv("ALERT_COOLDOWN_HOURS", "6"))
MAX_ALERTS_PER_SCAN = int(os.getenv("MAX_ALERTS_PER_SCAN", "6"))
MIN_STARS_TO_ALERT = int(os.getenv("MIN_STARS_TO_ALERT", "1"))

# ---------- Pre-filter ----------
MIN_AGE_DAYS = int(os.getenv("MIN_AGE_DAYS", "30"))
MIN_VOLUME_USD_24H = float(os.getenv("MIN_VOLUME_USD_24H", "3000000"))

# ---------- STANDARD signal (soft profile: earlier entries) ----------
PRICE_CHANGE_4H_MIN = float(os.getenv("PRICE_CHANGE_4H_MIN", "2.0"))
PRICE_CHANGE_4H_MAX = float(os.getenv("PRICE_CHANGE_4H_MAX", "10.0"))
OI_CHANGE_4H_MIN = float(os.getenv("OI_CHANGE_4H_MIN", "4.0"))
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
PULLBACK_OI_24H_MIN = float(os.getenv("PULLBACK_OI_24H_MIN", "5.0"))
PULLBACK_OI_1H_MIN = float(os.getenv("PULLBACK_OI_1H_MIN", "-1.0"))

# ---------- BB SQUEEZE signal (15m, strict preset) ----------
ENABLE_BB_SQUEEZE = os.getenv("ENABLE_BB_SQUEEZE", "true").lower() == "true"
BB_PERIOD = int(os.getenv("BB_PERIOD", "20"))          # классика: Length 20
BB_MULT = float(os.getenv("BB_MULT", "2.0"))            # классика: Deviation 2.0
# После squeeze полосы должны начать расширяться (истинный пробой, не укол)
BB_REQUIRE_EXPANSION = os.getenv("BB_REQUIRE_EXPANSION", "true").lower() == "true"
# Отсев ложного пробоя: close снова внутри полос после укола upper
BB_REJECT_FALSE_BREAKOUT = os.getenv("BB_REJECT_FALSE_BREAKOUT", "true").lower() == "true"
# Squeeze on 15m: bandwidth in lower percentile of lookback OR below absolute max
BB_SQUEEZE_LOOKBACK = int(os.getenv("BB_SQUEEZE_LOOKBACK", "48"))  # 48×15m ≈ 12h
BB_SQUEEZE_PERCENTILE = float(os.getenv("BB_SQUEEZE_PERCENTILE", "20"))  # bottom 20%
BB_SQUEEZE_MAX_BW = float(os.getenv("BB_SQUEEZE_MAX_BW", "4.5"))  # % hard cap
# Squeeze must have been present in the last N bars (fresh, not stale)
BB_SQUEEZE_FRESH_BARS = int(os.getenv("BB_SQUEEZE_FRESH_BARS", "6"))  # 6×15m ≈ 1.5h
# Breakout volume: current 15m vol vs avg of prior 20 bars
BB_BREAKOUT_VOL_MIN = float(os.getenv("BB_BREAKOUT_VOL_MIN", "1.15"))
# After squeeze: close above upper band on 15m, then small pullback entry
BB_PULLBACK_MAX_PCT = float(os.getenv("BB_PULLBACK_MAX_PCT", "1.8"))
BB_PULLBACK_RSI_MAX = float(os.getenv("BB_PULLBACK_RSI_MAX", "65"))  # RSI 15m
BB_OI_24H_MIN = float(os.getenv("BB_OI_24H_MIN", "4.0"))
# Доп. подтверждение притока (не только 24h-всплеск / short cover)
BB_OI_4H_MIN = float(os.getenv("BB_OI_4H_MIN", "1.5"))
# Анти-параболика: макс. рост цены за последние 2×15m от локального low
BB_PARABOLIC_MAX_PCT = float(os.getenv("BB_PARABOLIC_MAX_PCT", "6.0"))
# Откат должен удерживаться выше mid BB (поддержка после пробоя)
BB_REQUIRE_ABOVE_MID = os.getenv("BB_REQUIRE_ABOVE_MID", "true").lower() == "true"
# Keltner Channels (TTM-style squeeze filter for BB)
KC_EMA_PERIOD = int(os.getenv("KC_EMA_PERIOD", "20"))
KC_ATR_PERIOD = int(os.getenv("KC_ATR_PERIOD", "10"))
KC_ATR_MULT = float(os.getenv("KC_ATR_MULT", "1.5"))
# BB inside KC recently = confirmed squeeze
BB_REQUIRE_KC_SQUEEZE = os.getenv("BB_REQUIRE_KC_SQUEEZE", "true").lower() == "true"
# Lookback bars for "was inside KC" (fresh squeeze)
BB_KC_SQUEEZE_BARS = int(os.getenv("BB_KC_SQUEEZE_BARS", "6"))
# Optional: breakout should clear Keltner upper too
BB_REQUIRE_KC_BREAKOUT = os.getenv("BB_REQUIRE_KC_BREAKOUT", "false").lower() == "true"

# ---------- BB LOWER: лонг от нижней полосы в 24h-аптренде ----------
ENABLE_BB_LOWER = os.getenv("ENABLE_BB_LOWER", "true").lower() == "true"
# Монета в восходящем тренде за 24ч (мин. рост цены %)
BB_LOWER_TREND_24H_MIN = float(os.getenv("BB_LOWER_TREND_24H_MIN", "3.0"))  # только явный 24h лонг-тренд
# Пробой нижней BB: close 15m ниже lower (не фитиль)
# Предыдущий close должен быть >= lower (первый пробой, не давно «под полосой»)
BB_LOWER_RSI_MAX = float(os.getenv("BB_LOWER_RSI_MAX", "45"))  # 15m не перекуплен
BB_LOWER_RSI_MIN = float(os.getenv("BB_LOWER_RSI_MIN", "20"))  # не мёртвый дамп
# Макс. «прокол» ниже lower — слишком глубоко = тренд ломается
BB_LOWER_MAX_BREAK_PCT = float(os.getenv("BB_LOWER_MAX_BREAK_PCT", "1.5"))

# ---------- EMA filter ----------
USE_EMA_FILTER = os.getenv("USE_EMA_FILTER", "true").lower() == "true"
EMA_PERIOD = int(os.getenv("EMA_PERIOD", "50"))
EMA_PULLBACK_PERIOD = int(os.getenv("EMA_PULLBACK_PERIOD", "21"))

# ---------- BTC filter ----------
BTC_MIN_1H_CHANGE = float(os.getenv("BTC_MIN_1H_CHANGE", "-1.5"))

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
AUTO_BB_TP_PCT = float(os.getenv("AUTO_BB_TP_PCT", "2.0"))
AUTO_BB_SL_PCT = float(os.getenv("AUTO_BB_SL_PCT", "1.2"))
# BB_LOWER: TP как у BB, SL 10% по запросу (осторожно при плече 10x)
AUTO_BB_LOWER_TP_PCT = float(os.getenv("AUTO_BB_LOWER_TP_PCT", "2.0"))
AUTO_BB_LOWER_SL_PCT = float(os.getenv("AUTO_BB_LOWER_SL_PCT", "10.0"))

# Limits
MAX_AUTO_POSITIONS = int(os.getenv("MAX_AUTO_POSITIONS", "2"))
DAILY_LOSS_LIMIT_USD = float(os.getenv("DAILY_LOSS_LIMIT_USD", "25"))
CONSECUTIVE_LOSS_BLOCK = int(os.getenv("CONSECUTIVE_LOSS_BLOCK", "3"))

# Which signal types are eligible for auto-trade
AUTO_TRADE_SIGNAL_TYPES = os.getenv(
    "AUTO_TRADE_SIGNAL_TYPES", "BB_LOWER"
)
# Авто только если монета в плюсе за 24ч (дубль-фильтр на всякий случай)
AUTO_REQUIRE_24H_UPTREND = os.getenv("AUTO_REQUIRE_24H_UPTREND", "true").lower() == "true"
AUTO_MIN_24H_CHANGE_PCT = float(os.getenv("AUTO_MIN_24H_CHANGE_PCT", "3.0"))

# Reconciliation interval (sec)
RECONCILE_INTERVAL_SEC = int(os.getenv("RECONCILE_INTERVAL_SEC", "15"))

# Post-trade cooldown
POST_TRADE_COOLDOWN_HOURS = int(os.getenv("POST_TRADE_COOLDOWN_HOURS", "48"))

# ---------- Trailing stop after TP1 ----------
AUTO_TRAIL_ENABLED = os.getenv("AUTO_TRAIL_ENABLED", "true").lower() == "true"
AUTO_TP1_TRIGGER_PCT = float(os.getenv("AUTO_TP1_TRIGGER_PCT", "1.5"))
AUTO_TRAIL_DISTANCE_PCT = float(os.getenv("AUTO_TRAIL_DISTANCE_PCT", "1.0"))
AUTO_TP1_TRIGGER_PCT_PB = float(os.getenv("AUTO_TP1_TRIGGER_PCT_PB", "0.9"))
AUTO_TRAIL_DISTANCE_PCT_PB = float(os.getenv("AUTO_TRAIL_DISTANCE_PCT_PB", "0.6"))
AUTO_TP1_TRIGGER_PCT_BB = float(os.getenv("AUTO_TP1_TRIGGER_PCT_BB", "1.0"))
AUTO_TRAIL_DISTANCE_PCT_BB = float(os.getenv("AUTO_TRAIL_DISTANCE_PCT_BB", "0.6"))
AUTO_TP1_TRIGGER_PCT_BB_LOWER = float(os.getenv("AUTO_TP1_TRIGGER_PCT_BB_LOWER", str(AUTO_TP1_TRIGGER_PCT_BB)))
AUTO_TRAIL_DISTANCE_PCT_BB_LOWER = float(os.getenv("AUTO_TRAIL_DISTANCE_PCT_BB_LOWER", str(AUTO_TRAIL_DISTANCE_PCT_BB)))

# Ранний безубыток (BE): после +X% переносим SL на цену входа (+ крошечный буфер)
AUTO_BE_ENABLED = os.getenv("AUTO_BE_ENABLED", "true").lower() == "true"
AUTO_BE_TRIGGER_PCT = float(os.getenv("AUTO_BE_TRIGGER_PCT", "0.6"))
AUTO_BE_BUFFER_PCT = float(os.getenv("AUTO_BE_BUFFER_PCT", "0.05"))  # SL чуть выше entry

# Выход по слому структуры: close ниже EMA50
STRUCTURE_EXIT_ENABLED = os.getenv("STRUCTURE_EXIT_ENABLED", "true").lower() == "true"
STRUCTURE_EXIT_EMA_1H = os.getenv("STRUCTURE_EXIT_EMA_1H", "true").lower() == "true"
STRUCTURE_EXIT_EMA_15M = os.getenv("STRUCTURE_EXIT_EMA_15M", "true").lower() == "true"

# BTC trend filter for auto-entry
BTC_FILTER_ENABLED = os.getenv("BTC_FILTER_ENABLED", "true").lower() == "true"
BTC_FILTER_15M_DROP_MAX = float(os.getenv("BTC_FILTER_15M_DROP_MAX", "0.8"))
BTC_FILTER_15M_PUMP_MAX = float(os.getenv("BTC_FILTER_15M_PUMP_MAX", "1.5"))
BTC_FILTER_1H_VOLATILITY_MAX = float(os.getenv("BTC_FILTER_1H_VOLATILITY_MAX", "1.2"))

# Storage
AUTO_STATE_FILE = os.getenv("AUTO_STATE_FILE", os.path.join(DATA_DIR, "auto_state.json"))
