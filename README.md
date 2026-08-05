# OI Scanner + BB_LOWER v2 Auto-Trader

## Railway files
scanner.py, config.py, auto_trade.py, trader.py, indicators.py, storage.py, visuals.py, backtest.py, Procfile, requirements.txt, runtime.txt

## Required Variables
TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, BYBIT_API_KEY, BYBIT_API_SECRET, BYBIT_BASE_URL, DATA_DIR=/data

## Recommended Variables
POSITION_SIZE_USD=250
LEVERAGE=10
DEPOSIT_USD=500
AUTO_TRADE_SIGNAL_TYPES=BB_LOWER
AUTO_BB_LOWER_TP_PCT=3.5
AUTO_BB_LOWER_SL_PCT=2.5
TELEGRAM_ALLOWED_IDS=<your_telegram_user_id>
RISK_SIZING_ENABLED=true
RISK_USD_PER_TRADE=6.25
SL_TRIGGER_BY=MarkPrice
PARTIAL_TP_ENABLED=true

## New safety features
- Atomic JSON saves (tmp + rename + flock)
- Telegram command whitelist (TELEGRAM_ALLOWED_IDS)
- Risk-based position sizing from SL distance
- SL trigger MarkPrice (fewer wick stops)
- Partial TP 50% at TP1, rest trailing
- Backtest models BE / trailing / structure exit

Volume: mount `/data`
