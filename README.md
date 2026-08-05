# OI Scanner + BB_LOWER v2 Auto-Trader

## Railway deploy

1. Upload these files to the repo root:
   - `scanner.py` (entry point)
   - `config.py`, `auto_trade.py`, `trader.py`
   - `indicators.py`, `storage.py`, `visuals.py`, `backtest.py`
   - `Procfile`, `requirements.txt`, `runtime.txt`

2. Mount volume at `/data` (required — state survives redeploys).

3. Set Variables:

| Variable | Example |
|----------|---------|
| TELEGRAM_BOT_TOKEN | ... |
| TELEGRAM_CHAT_ID | ... |
| BYBIT_API_KEY | ... |
| BYBIT_API_SECRET | ... |
| BYBIT_BASE_URL | https://api-demo.bybit.com or https://api.bybit.com |
| DATA_DIR | /data |

Optional risk:
- `POSITION_SIZE_USD=250`
- `LEVERAGE=10`
- `AUTO_BB_LOWER_TP_PCT=3.5`
- `AUTO_BB_LOWER_SL_PCT=2.5`
- `AUTO_TRADE_SIGNAL_TYPES=BB_LOWER`

4. Start command is in Procfile: `worker: python scanner.py`

## Strategy (auto)

BB_LOWER v2 only:
- active liquid coins, 24h uptrend
- dip under lower Bollinger → reclaim close above
- OI / volume / funding / BTC 15m / chop filters
- TP = mid BB (fallback +3.5%), SL under structure (cap 2.5%)
- BE @ +0.6%, structure exit on 1h EMA50, trailing after TP1

## Backtest

```bash
python backtest.py --symbol PRLUSDT --days 30
python backtest.py --top 15 --days 14
python backtest.py --top 10 --days 20 --no-oi
```

Or in Telegram: `/backtest TOP 14` / `/backtest PRLUSDT 30`

## Commands

`/scan` `/auto` `/auto_on` `/auto_off` `/panic` `/resume`  
`/sig_on BB_LOWER` `/sig_off ...` `/backtest` `/settings` `/help`
