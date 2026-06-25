# Smart Money Bot v29 proper

Основа: старый v28 с сохранёнными кнопками, меню, статусом и логикой Telegram.
Добавлено: авто-открытие, авто-закрытие, SL/TP/Trailing через Bybit API.

## Render env
BOT_TOKEN=
CHAT_ID=
BYBIT_API_KEY=
BYBIT_API_SECRET=
LIVE_TRADING=false
AUTO_TRADING=true
ORDER_USDT=25
LEVERAGE=5
MAX_OPEN_TRADES=1
MIN_SCORE=70
MIN_TURNOVER_24H=5000000
MIN_RR=1.5
OI_ACCEL_THRESHOLD=0.3
TRAILING_STOP_PCT=1.2

## Telegram
Сохранены кнопки и команды старого бота: /start /status /test /scan /trades /reset
