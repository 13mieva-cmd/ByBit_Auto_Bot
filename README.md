# Bybit Smart Money Auto v29

Полностью автономный бот: сканер + авто-вход + авто-выход + SL/TP + trailing stop.

## Env
- BOT_TOKEN
- CHAT_ID
- BYBIT_API_KEY
- BYBIT_API_SECRET
- LIVE_TRADING=true
- ORDER_USDT=25
- LEVERAGE=5
- RISK_PERCENT=1
- MIN_SCORE=70
- MIN_TURNOVER_24H=5000000
- MAX_OPEN_TRADES=1
- REST_REFRESH_MS=30000

## Start
- npm start

## Notes
- При LIVE_TRADING=false бот только сканирует и шлёт сигналы.
- Для реальных сделок нужно сначала проверить на demo/testnet.
