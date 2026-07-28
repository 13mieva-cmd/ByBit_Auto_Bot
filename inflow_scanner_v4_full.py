from config import *
import asyncio, json, logging, time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional
import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.utils.keyboard import InlineKeyboardBuilder
from storage import PositionStore, IgnoreStore, StatsStore, AutoStateStore
from trader import BybitTrader
from auto_trade import AutoTrader, check_btc_health
from indicators import calculate_rsi, calculate_ema
from visuals import progress_bar, sparkline, position_progress

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("scanner")
BYBIT_BASE = "https://api-demo.bybit.com"
last_alert: dict[str, float] = {}
SEM = asyncio.Semaphore(10)
positions = PositionStore(POSITIONS_FILE)
ignore = IgnoreStore(IGNORE_FILE)
stats = StatsStore(STATS_FILE)
auto_state = AutoStateStore(AUTO_STATE_FILE)
trader: Optional[BybitTrader] = None
auto_trader: Optional[AutoTrader] = None

async def fetch_json(session, url, params=None):
    async with session.get(url, params=params, timeout=30) as r:
        r.raise_for_status(); return await r.json()

async def get_instruments(session):
    instruments, cursor = [], ""
    while True:
        params = {"category": "linear", "limit": 1000}
        if cursor: params["cursor"] = cursor
        data = await fetch_json(session, f"{BYBIT_BASE}/v5/market/instruments-info", params)
        result = data.get("result", {})
        instruments.extend(result.get("list", []))
        cursor = result.get("nextPageCursor", "")
        if not cursor: break
    return instruments

async def get_tickers(session):
    data = await fetch_json(session, f"{BYBIT_BASE}/v5/market/tickers", {"category": "linear"})
    return {t["symbol"]: t for t in data.get("result", {}).get("list", [])}

async def get_klines(session, symbol, interval, limit):
    try:
        data = await fetch_json(session, f"{BYBIT_BASE}/v5/market/kline", {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit})
        return list(reversed(data.get("result", {}).get("list", [])))
    except Exception as e:
        log.warning(f"kline {symbol} {interval}: {e}")
        return []

async def get_oi_history(session, symbol, interval_time="5min", limit=60):
    try:
        data = await fetch_json(session, f"{BYBIT_BASE}/v5/market/open-interest", {"category": "linear", "symbol": symbol, "intervalTime": interval_time, "limit": limit})
        return list(reversed(data.get("result", {}).get("list", [])))
    except Exception as e:
        log.warning(f"OI fetch {symbol}: {e}")
        return []

async def get_current_price(session, symbol):
    try:
        data = await fetch_json(session, f"{BYBIT_BASE}/v5/market/tickers", {"category": "linear", "symbol": symbol})
        return float(data["result"]["list"][0]["lastPrice"])
    except Exception:
        return None

async def get_btc_1h_change(session):
    klines = await get_klines(session, "BTCUSDT", "60", 1)
    if not klines: return 0.0
    op, cl = float(klines[0][1]), float(klines[0][4])
    return (cl - op) / op * 100 if op > 0 else 0.0

def is_blacklisted(symbol: str) -> bool:
    return symbol.replace("USDT", "").replace("PERP", "") in BLACKLIST

async def analyze_coin(session, c: dict, btc_1h: float) -> Optional[dict]:
    symbol = c["symbol"]
    async with SEM:
        klines_4h = await get_klines(session, symbol, "240", 30)
        if len(klines_4h) < 20: return None
        closes_4h = [float(k[4]) for k in klines_4h]
        rsi_4h = calculate_rsi(closes_4h, 14)
        if rsi_4h is None: return None

        klines_1h = await get_klines(session, symbol, "60", max(EMA_PERIOD + 5, 30))
        if len(klines_1h) < 25: return None
        closes_1h = [float(k[4]) for k in klines_1h]
        current_price = closes_1h[-1]
        ema50 = calculate_ema(closes_1h, EMA_PERIOD)
        ema21 = calculate_ema(closes_1h, EMA_PULLBACK_PERIOD)
        oi_4h_history = await get_oi_history(session, symbol, "4h", 12)
        oi_1h_history = await get_oi_history(session, symbol, "1h", 24)

        oi_change_4h = oi_change_24h = oi_change_1h = 0.0
        if len(oi_4h_history) >= 2:
            try:
                oi_now_4h = float(oi_4h_history[-1]["openInterest"])
                oi_4h_ago = float(oi_4h_history[-2]["openInterest"])
                if oi_4h_ago > 0: oi_change_4h = (oi_now_4h - oi_4h_ago) / oi_4h_ago * 100
                if len(oi_4h_history) >= 7:
                    oi_24h_ago = float(oi_4h_history[-7]["openInterest"])
                    if oi_24h_ago > 0: oi_change_24h = (oi_now_4h - oi_24h_ago) / oi_24h_ago * 100
            except Exception:
                pass
        if len(oi_1h_history) >= 2:
            try:
                oi_now_1h = float(oi_1h_history[-1]["openInterest"])
                oi_1h_ago = float(oi_1h_history[-2]["openInterest"])
                if oi_1h_ago > 0: oi_change_1h = (oi_now_1h - oi_1h_ago) / oi_1h_ago * 100
            except Exception:
                pass

        price_sparkline_24h = sparkline(closes_1h[-24:], 12) if len(closes_1h) >= 24 else None
        oi_values_24h = []
        for item in oi_1h_history[-24:]:
            try: oi_values_24h.append(float(item["openInterest"]))
            except Exception: pass
        oi_24h_sparkline = sparkline(oi_values_24h, 12) if oi_values_24h else None

        last_4h = klines_4h[-1]
        try:
            op_4h, cl_4h, vol_4h = float(last_4h[1]), float(last_4h[4]), float(last_4h[5])
        except Exception:
            return None
        if op_4h <= 0: return None
        price_change_4h = (cl_4h - op_4h) / op_4h * 100
        prev_vols_4h = [float(k[5]) for k in klines_4h[:-1]]
        avg_vol_4h = sum(prev_vols_4h) / len(prev_vols_4h) if prev_vols_4h else 0
        vol_spike_4h = vol_4h / avg_vol_4h if avg_vol_4h > 0 else 0

        last_1h = klines_1h[-1]
        try:
            op_1h, vol_1h = float(last_1h[1]), float(last_1h[5])
        except Exception:
            return None
        price_change_1h = (current_price - op_1h) / op_1h * 100 if op_1h > 0 else 0
        prev_vols_1h = [float(k[5]) for k in klines_1h[-25:-1]]
        avg_vol_1h = sum(prev_vols_1h) / len(prev_vols_1h) if prev_vols_1h else 0
        vol_spike_1h = vol_1h / avg_vol_1h if avg_vol_1h > 0 else 0
        local_high_24h = 0.0
        if len(klines_1h) >= 25:
            prior_highs = [float(k[2]) for k in klines_1h[-25:-1]]
            if prior_highs: local_high_24h = max(prior_highs)
        rsi_1h = calculate_rsi(closes_1h, 14)
        if rsi_1h is None: return None

        d = {"symbol": symbol, "price": current_price, "price_change_4h": price_change_4h, "price_change_1h": price_change_1h, "oi_change_4h": oi_change_4h, "oi_change_24h": oi_change_24h, "oi_change_1h": oi_change_1h, "vol_spike_4h": vol_spike_4h, "vol_spike_1h": vol_spike_1h, "vol_24h": c["volume_24h"], "rsi_4h": rsi_4h, "rsi_1h": rsi_1h, "btc_1h": btc_1h, "age_days": c["age_days"], "ema50_1h": ema50, "ema21_1h": ema21, "local_high_24h": local_high_24h, "price_sparkline_24h": price_sparkline_24h, "oi_24h_sparkline": oi_24h_sparkline}
        if d["price"] < d["ema50_1h"] or d["ema21_1h"] < d["ema50_1h"]: return None
        if len(closes_1h) >= 15:
            ema50_old = calculate_ema(closes_1h[-15:-5], EMA_PERIOD)
            if ema50_old is not None and d["ema50_1h"] <= ema50_old * 1.002:
                return None
        distance_ema21 = abs(d["price"] - d["ema21_1h"]) / d["ema21_1h"] * 100
        if distance_ema21 > PULLBACK_EMA_DISTANCE_PCT: return None
        if not (PULLBACK_RSI_1H_MIN <= d["rsi_1h"] <= PULLBACK_RSI_1H_MAX): return None
        if d["oi_change_24h"] < PULLBACK_OI_24H_MIN or d["oi_change_1h"] < PULLBACK_OI_1H_MIN: return None
        if len(closes_1h) < 3 or closes_1h[-1] <= closes_1h[-2] or closes_1h[-2] <= closes_1h[-3]: return None
        local_min = min(closes_1h[-3:])
        if local_min <= 0: return None
        price_bounce = (d["price"] - local_min) / local_min * 100
        if price_bounce < 0.6: return None
        stars = 1
        if d["oi_change_24h"] >= PULLBACK_OI_24H_MIN * 1.6 and d["oi_change_1h"] > 2.0: stars = 2
        if stars == 2 and d["vol_spike_1h"] >= 1.4: stars = 3
        return {**d, "stars": stars, "signal_type": "PULLBACK"}

async def scan_once(session) -> list[dict]:
    btc_1h = await get_btc_1h_change(session)
    if btc_1h < BTC_MIN_1H_CHANGE: return []
    instruments = await get_instruments(session)
    tickers = await get_tickers(session)
    now_ms = int(time.time() * 1000)
    min_age_ms = MIN_AGE_DAYS * 86_400_000
    prefiltered = []
    for inst in instruments:
        symbol = inst.get("symbol", "")
        if not symbol.endswith("USDT"): continue
        if inst.get("contractType") != "LinearPerpetual": continue
        if inst.get("status") != "Trading": continue
        if is_blacklisted(symbol): continue
        if ignore.is_ignored(symbol): continue
        launch_time = int(inst.get("launchTime", 0) or 0)
        if launch_time == 0 or (now_ms - launch_time) < min_age_ms: continue
        t = tickers.get(symbol)
        if not t: continue
        try: turnover = float(t.get("turnover24h", 0))
        except Exception: continue
        if turnover < MIN_VOLUME_USD_24H: continue
        if (time.time() - last_alert.get(symbol, 0)) < ALERT_COOLDOWN_HOURS * 3600: continue
        prefiltered.append({"symbol": symbol, "volume_24h": turnover, "age_days": (now_ms - launch_time) // 86_400_000})
    results = await asyncio.gather(*[analyze_coin(session, c, btc_1h) for c in prefiltered], return_exceptions=True)
    scored = [r for r in results if not isinstance(r, Exception) and r and r["stars"] >= MIN_STARS_TO_ALERT]
    scored.sort(key=lambda x: (-x["stars"], -x["oi_change_4h"]))
    return scored
