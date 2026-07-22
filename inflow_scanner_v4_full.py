"""
Bybit OI LONG Scanner v2 — three signal types + smart hold + visuals.
- STANDARD: Price↑ 4h + OI↑ 4h + Volume
- SURGE: Price↑ 1h + OI↑ 1h (catches faster moves)
- PULLBACK: pullback to EMA21 in existing uptrend
"""
import asyncio
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import (
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    SCAN_INTERVAL_MIN, ALERT_COOLDOWN_HOURS, MAX_ALERTS_PER_SCAN, MIN_STARS_TO_ALERT,
    MIN_AGE_DAYS, MIN_VOLUME_USD_24H,
    PRICE_CHANGE_4H_MIN, PRICE_CHANGE_4H_MAX,
    OI_CHANGE_4H_MIN, OI_CHANGE_24H_2STAR,
    VOLUME_SPIKE_MIN, VOLUME_SPIKE_2STAR,
    RSI_4H_MIN, RSI_4H_MAX,
    ENABLE_OI_SURGE, SURGE_PRICE_1H_MIN, SURGE_PRICE_1H_MAX,
    SURGE_OI_1H_MIN, SURGE_OI_24H_MIN, SURGE_RSI_1H_MAX,
    ENABLE_PULLBACK, PULLBACK_RSI_1H_MIN, PULLBACK_RSI_1H_MAX,
    PULLBACK_EMA_DISTANCE_PCT, PULLBACK_OI_24H_MIN, PULLBACK_OI_1H_MIN,
    USE_EMA_FILTER, EMA_PERIOD, EMA_PULLBACK_PERIOD,
    BTC_MIN_1H_CHANGE,
    TP1_PCT, TP2_PCT, HARD_SL_PCT, OI_DROP_WARNING_PCT,
    POSITION_TIMEOUT_HOURS, POSITION_CHECK_INTERVAL_MIN,
    SMART_HOLD_THRESHOLD_PCT,
    IGNORE_DURATION_HOURS,
    POSITIONS_FILE, IGNORE_FILE, STATS_FILE,
    DAILY_REPORT_HOUR_UTC,
    BLACKLIST,
    BYBIT_API_KEY, BYBIT_API_SECRET,
    POSITION_SIZE_USD, AUTO_TP_PCT, AUTO_HARD_SL_PCT,
    AUTO_PULLBACK_TP_PCT, AUTO_PULLBACK_SL_PCT,
    MAX_AUTO_POSITIONS, DAILY_LOSS_LIMIT_USD, CONSECUTIVE_LOSS_BLOCK,
    AUTO_TRADE_SIGNAL_TYPES, AUTO_STATE_FILE,
    POST_TRADE_COOLDOWN_HOURS,
    BTC_FILTER_ENABLED, BTC_FILTER_15M_DROP_MAX,
    BTC_FILTER_15M_PUMP_MAX, BTC_FILTER_1H_VOLATILITY_MAX,
)
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
trader: Optional[BybitTrader] = None  # initialized in main if API keys present
auto_trader: Optional[AutoTrader] = None


# ---------- API ----------

async def fetch_json(session, url, params=None):
    async with session.get(url, params=params, timeout=30) as r:
        r.raise_for_status()
        return await r.json()


async def get_instruments(session):
    instruments, cursor = [], ""
    while True:
        params = {"category": "linear", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        data = await fetch_json(session, f"{BYBIT_BASE}/v5/market/instruments-info", params)
        result = data.get("result", {})
        instruments.extend(result.get("list", []))
        cursor = result.get("nextPageCursor", "")
        if not cursor:
            break
    return instruments


async def get_tickers(session):
    data = await fetch_json(session, f"{BYBIT_BASE}/v5/market/tickers", {"category": "linear"})
    return {t["symbol"]: t for t in data.get("result", {}).get("list", [])}


async def get_klines(session, symbol, interval, limit):
    try:
        data = await fetch_json(
            session, f"{BYBIT_BASE}/v5/market/kline",
            {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit},
        )
        return list(reversed(data.get("result", {}).get("list", [])))
    except Exception as e:
        log.warning(f"kline {symbol} {interval}: {e}")
        return []


async def get_oi_history(session, symbol, interval_time="5min", limit=60):
    try:
        data = await fetch_json(
            session, f"{BYBIT_BASE}/v5/market/open-interest",
            {"category": "linear", "symbol": symbol, "intervalTime": interval_time, "limit": limit},
        )
        return list(reversed(data.get("result", {}).get("list", [])))
    except Exception as e:
        log.warning(f"OI fetch {symbol}: {e}")
        return []


async def get_current_price(session, symbol):
    try:
        data = await fetch_json(
            session, f"{BYBIT_BASE}/v5/market/tickers",
            {"category": "linear", "symbol": symbol},
        )
        return float(data["result"]["list"][0]["lastPrice"])
    except Exception:
        return None


async def get_btc_1h_change(session):
    klines = await get_klines(session, "BTCUSDT", "60", 1)
    if not klines:
        return 0.0
    op, cl = float(klines[0][1]), float(klines[0][4])
    return (cl - op) / op * 100 if op > 0 else 0.0


# ---------- Analysis ----------

async def analyze_coin(session, c: dict, btc_1h: float) -> Optional[dict]:
    """Try all three signal types. Returns best match or None."""
    symbol = c["symbol"]

    async with SEM:
        # Fetch 4h klines (used by STANDARD and pullback context)
        klines_4h = await get_klines(session, symbol, "240", 30)
        if len(klines_4h) < 20:
            return None
        closes_4h = [float(k[4]) for k in klines_4h]
        rsi_4h = calculate_rsi(closes_4h, 14)
        if rsi_4h is None:
            return None

        # Fetch 1h klines (used by SURGE and pullback)
        klines_1h = await get_klines(session, symbol, "60", max(EMA_PERIOD + 5, 30))
        if len(klines_1h) < 25:
            return None
        closes_1h = [float(k[4]) for k in klines_1h]
        current_price = closes_1h[-1]

        # EMA50 on 1h
        ema50 = calculate_ema(closes_1h, EMA_PERIOD)
        ema21 = calculate_ema(closes_1h, EMA_PULLBACK_PERIOD)

        # OI history 4h and 1h
        oi_4h_history = await get_oi_history(session, symbol, "4h", 12)
        oi_1h_history = await get_oi_history(session, symbol, "1h", 24)

    # Build OI metrics
    oi_change_4h = None
    oi_change_24h = None
    oi_change_1h = None
    oi_24h_sparkline = None

    if len(oi_4h_history) >= 2:
        try:
            oi_now_4h = float(oi_4h_history[-1]["openInterest"])
            oi_4h_ago = float(oi_4h_history[-2]["openInterest"])
            if oi_4h_ago > 0:
                oi_change_4h = (oi_now_4h - oi_4h_ago) / oi_4h_ago * 100
            if len(oi_4h_history) >= 7:
                oi_24h_ago = float(oi_4h_history[-7]["openInterest"])
                if oi_24h_ago > 0:
                    oi_change_24h = (oi_now_4h - oi_24h_ago) / oi_24h_ago * 100
        except (KeyError, ValueError, TypeError):
            pass

    if len(oi_1h_history) >= 2:
        try:
            oi_now_1h = float(oi_1h_history[-1]["openInterest"])
            oi_1h_ago = float(oi_1h_history[-2]["openInterest"])
            if oi_1h_ago > 0:
                oi_change_1h = (oi_now_1h - oi_1h_ago) / oi_1h_ago * 100
            # Sparkline of OI for 24h (from 1h data)
            oi_values_24h = []
            for item in oi_1h_history[-24:]:
                try:
                    oi_values_24h.append(float(item["openInterest"]))
                except (KeyError, ValueError, TypeError):
                    pass
            if oi_values_24h:
                oi_24h_sparkline = sparkline(oi_values_24h, 12)
        except (KeyError, ValueError, TypeError):
            pass

    # Price sparkline 24h (from 1h closes)
    price_sparkline_24h = sparkline(closes_1h[-24:], 12) if len(closes_1h) >= 24 else None

    # Volume metrics (4h)
    last_4h = klines_4h[-1]
    try:
        op_4h = float(last_4h[1])
        cl_4h = float(last_4h[4])
        vol_4h = float(last_4h[5])
    except (ValueError, IndexError):
        return None
    if op_4h <= 0:
        return None
    price_change_4h = (cl_4h - op_4h) / op_4h * 100
    prev_vols_4h = [float(k[5]) for k in klines_4h[:-1]]
    avg_vol_4h = sum(prev_vols_4h) / len(prev_vols_4h) if prev_vols_4h else 0
    vol_spike_4h = vol_4h / avg_vol_4h if avg_vol_4h > 0 else 0

    # Price change 1h
    last_1h = klines_1h[-1]
    try:
        op_1h = float(last_1h[1])
        vol_1h = float(last_1h[5])
    except (ValueError, IndexError):
        return None
    price_change_1h = (current_price - op_1h) / op_1h * 100 if op_1h > 0 else 0

    # Volume spike 1h (current 1h vol vs average of previous 1h candles)
    prev_vols_1h = [float(k[5]) for k in klines_1h[-25:-1]]   # last 24 prior candles
    avg_vol_1h = sum(prev_vols_1h) / len(prev_vols_1h) if prev_vols_1h else 0
    vol_spike_1h = vol_1h / avg_vol_1h if avg_vol_1h > 0 else 0

    # 24h local high (excluding the latest candle — we want to check if NOW broke previous 24h max)
    local_high_24h = 0.0
    if len(klines_1h) >= 25:
        prior_highs = [float(k[2]) for k in klines_1h[-25:-1]]
        if prior_highs:
            local_high_24h = max(prior_highs)

    # RSI 1h
    rsi_1h = calculate_rsi(closes_1h, 14)
    if rsi_1h is None:
        return None

    base_data = {
        "symbol": symbol,
        "price": current_price,
        "price_change_4h": price_change_4h,
        "price_change_1h": price_change_1h,
        "oi_change_4h": oi_change_4h if oi_change_4h is not None else 0,
        "oi_change_24h": oi_change_24h if oi_change_24h is not None else 0,
        "oi_change_1h": oi_change_1h if oi_change_1h is not None else 0,
        "vol_spike_4h": vol_spike_4h,
        "vol_spike_1h": vol_spike_1h,
        "vol_24h": c["volume_24h"],
        "rsi_4h": rsi_4h,
        "rsi_1h": rsi_1h,
        "btc_1h": btc_1h,
        "age_days": c["age_days"],
        "ema50_1h": ema50,
        "ema21_1h": ema21,
        "local_high_24h": local_high_24h,
        "price_sparkline_24h": price_sparkline_24h,
        "oi_24h_sparkline": oi_24h_sparkline,
    }

    # ========== Try STANDARD signal ==========
    standard = try_standard(base_data)
    if standard:
        return standard

    # ========== Try SURGE signal ==========
    if ENABLE_OI_SURGE:
        surge = try_surge(base_data)
        if surge:
            return surge

    # ========== Try PULLBACK signal ==========
    if ENABLE_PULLBACK:
        pullback = try_pullback(base_data, closes_1h)
        if pullback:
            return pullback

    return None


def try_standard(d: dict) -> Optional[dict]:
    if d["price_change_4h"] < PRICE_CHANGE_4H_MIN or d["price_change_4h"] > PRICE_CHANGE_4H_MAX:
        return None
    if d["vol_spike_4h"] < VOLUME_SPIKE_MIN:
        return None
    if d["rsi_4h"] < RSI_4H_MIN or d["rsi_4h"] > RSI_4H_MAX:
        return None
    if USE_EMA_FILTER and d["ema50_1h"] is not None and d["price"] < d["ema50_1h"]:
        return None
    if d["oi_change_4h"] < OI_CHANGE_4H_MIN:
        return None

    stars = 1
    if d["oi_change_24h"] >= OI_CHANGE_24H_2STAR and d["vol_spike_4h"] >= VOLUME_SPIKE_2STAR:
        stars = 2
    # 3★: ⭐⭐ + breakout of 24h high + BTC not falling
    if stars == 2:
        broke_24h_high = d["local_high_24h"] > 0 and d["price"] > d["local_high_24h"]
        btc_supportive = d["btc_1h"] >= 0.0
        if broke_24h_high and btc_supportive:
            stars = 3

    return {**d, "stars": stars, "signal_type": "STANDARD"}


def try_surge(d: dict) -> Optional[dict]:
    """SURGE + защита от short squeeze.
    Резкий рост OI и цены часто = массовое закрытие шортов, а не приток лонгов.
    Требуем рост OI и на 4h, и уверенное положение выше EMA50 (не squeeze у сопротивления)."""
    if d["price_change_1h"] < SURGE_PRICE_1H_MIN or d["price_change_1h"] > SURGE_PRICE_1H_MAX:
        return None
    if d["oi_change_1h"] < SURGE_OI_1H_MIN:
        return None
    if d["oi_change_24h"] < SURGE_OI_24H_MIN:
        return None

    # === ФИЛЬТРЫ ПРОТИВ SQUEEZE ===
    if d["oi_change_4h"] is None or d["oi_change_4h"] < 4.0:
        return None
    # цена уверенно выше EMA50 (запас 0.5%), а не упирается в неё
    if USE_EMA_FILTER and d["ema50_1h"] is not None and d["price"] < d["ema50_1h"] * 1.005:
        return None
    if d["rsi_1h"] > SURGE_RSI_1H_MAX:
        return None

    stars = 1
    if d["oi_change_1h"] >= SURGE_OI_1H_MIN * 1.8 and d["oi_change_4h"] >= 8.0:
        stars = 2
    if stars == 2:
        vol4h_strong = d["vol_spike_4h"] >= 1.6
        btc_supportive = d["btc_1h"] >= -0.3
        if vol4h_strong and btc_supportive:
            stars = 3

    return {**d, "stars": stars, "signal_type": "SURGE"}


def try_pullback(d: dict, closes_1h: list[float]) -> Optional[dict]:
    """PULLBACK + защита от отскоков в конце тренда.
    Требуем РАСТУЩУЮ EMA50 (тренд живой) и подтверждённый отскок с силой."""
    if d["ema21_1h"] is None or d["ema50_1h"] is None:
        return None

    # Сильный общий тренд
    if d["price"] < d["ema50_1h"] or d["ema21_1h"] < d["ema50_1h"]:
        return None

    # EMA50 должна РАСТИ — иначе тренд выдыхается
    if len(closes_1h) >= 15:
        ema50_old = calculate_ema(closes_1h[-15:-5], EMA_PERIOD)
        if ema50_old is not None and d["ema50_1h"] <= ema50_old * 1.002:
            return None

    # Расстояние до EMA21
    distance_ema21 = abs(d["price"] - d["ema21_1h"]) / d["ema21_1h"] * 100
    if distance_ema21 > PULLBACK_EMA_DISTANCE_PCT:
        return None

    # RSI в зоне отката
    if d["rsi_1h"] < PULLBACK_RSI_1H_MIN or d["rsi_1h"] > PULLBACK_RSI_1H_MAX:
        return None

    # OI подтверждение
    if d["oi_change_24h"] < PULLBACK_OI_24H_MIN:
        return None
    if d["oi_change_1h"] < PULLBACK_OI_1H_MIN:
        return None

    # Две зелёные 1h свечи подряд
    if len(closes_1h) < 3 or closes_1h[-1] <= closes_1h[-2] or closes_1h[-2] <= closes_1h[-3]:
        return None

    # Отскок должен иметь СИЛУ: минимум +0.6% от локального минимума
    local_min = min(closes_1h[-3:])
    if local_min <= 0:
        return None
    price_bounce = (d["price"] - local_min) / local_min * 100
    if price_bounce < 0.6:
        return None

    stars = 1
    if d["oi_change_24h"] >= PULLBACK_OI_24H_MIN * 1.6 and d["oi_change_1h"] > 2.0:
        stars = 2
    if stars == 2 and d["vol_spike_1h"] >= 1.4:
        stars = 3

    return {**d, "stars": stars, "signal_type": "PULLBACK"}


def is_blacklisted(symbol: str) -> bool:
    base = symbol.replace("USDT", "").replace("PERP", "")
    return base in BLACKLIST


# ---------- Scan ----------

async def scan_once(session) -> list[dict]:
    log.info("=== SCAN START ===")
    btc_1h = await get_btc_1h_change(session)
    log.info(f"BTC 1h: {btc_1h:+.2f}%")

    if btc_1h < BTC_MIN_1H_CHANGE:
        log.info(f"BTC dropping ({btc_1h:.2f}%) — skip.")
        return []

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
        try:
            turnover = float(t.get("turnover24h", 0))
        except (ValueError, TypeError):
            continue
        if turnover < MIN_VOLUME_USD_24H: continue
        if (time.time() - last_alert.get(symbol, 0)) < ALERT_COOLDOWN_HOURS * 3600: continue
        prefiltered.append({
            "symbol": symbol,
            "volume_24h": turnover,
            "age_days": (now_ms - launch_time) // 86_400_000,
        })

    log.info(f"Pre-filtered: {len(prefiltered)}")

    tasks = [analyze_coin(session, c, btc_1h) for c in prefiltered]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    scored = []
    for r in results:
        if isinstance(r, Exception) or r is None: continue
        if r["stars"] < MIN_STARS_TO_ALERT: continue
        scored.append(r)
    scored.sort(key=lambda x: (-x["stars"], -x["oi_change_4h"]))

    by_type = defaultdict(int)
    for s in scored:
        by_type[s["signal_type"]] += 1
    log.info(f"=== SCAN END: {len(scored)} alerts | "
             f"STD:{by_type['STANDARD']} SURGE:{by_type['SURGE']} PB:{by_type['PULLBACK']} ===")
    return scored


# ---------- Alert formatting ----------

def make_keyboard(symbol: str):
    builder = InlineKeyboardBuilder()
    builder.button(text="📱 Открыть в Bybit", url=f"https://www.bybit.com/trade/usdt/{symbol}")
    builder.button(text="✅ Я в лонге", callback_data=f"in:{symbol}")
    builder.button(text="📊 График OI", callback_data=f"oi:{symbol}")
    builder.button(text="❌ Игнорировать 24ч", callback_data=f"ign:{symbol}")
    builder.adjust(1)
    return builder.as_markup()


def make_tp1_keyboard(symbol: str):
    builder = InlineKeyboardBuilder()
    builder.button(text="🔒 Перенести SL в безубыток", callback_data=f"be:{symbol}")
    builder.button(text="✅ Закрыл 50%", callback_data=f"ack_tp1:{symbol}")
    builder.button(text="❌ Закрыл всё", callback_data=f"close:{symbol}")
    builder.adjust(1)
    return builder.as_markup()


SIGNAL_HEADERS = {
    "STANDARD": "🟢 <b>LONG</b> · STANDARD",
    "SURGE":    "⚡ <b>LONG</b> · OI SURGE",
    "PULLBACK": "↩️ <b>LONG</b> · PULLBACK",
}

SIGNAL_LOGIC = {
    "STANDARD": "Цена↑ 4ч + OI↑ 4ч + объём = свежие деньги входят",
    "SURGE":    "Цена↑ 1ч + OI↑ 1ч = очень ранний старт тренда",
    "PULLBACK": "Тренд вверх + откат к EMA21 + OI растёт = вход на ретесте",
}


def format_alert(s: dict) -> str:
    base = s["symbol"].replace("USDT", "")
    star_emoji = "⭐" * s["stars"]
    star_label = {1: "СЛАБЫЙ", 2: "ХОРОШИЙ", 3: "ПРЕМИУМ"}[s["stars"]]
    price = s["price"]
    tp1 = price * (1 + TP1_PCT / 100)
    tp2 = price * (1 + TP2_PCT / 100)
    hard_sl = price * (1 - HARD_SL_PCT / 100)
    vol_m = s["vol_24h"] / 1e6
    header = SIGNAL_HEADERS.get(s["signal_type"], "🟢 <b>LONG</b>")
    logic = SIGNAL_LOGIC.get(s["signal_type"], "")

    # Sparklines block
    sparkline_block = ""
    if s.get("price_sparkline_24h"):
        sparkline_block += f"Цена 24ч: <code>{s['price_sparkline_24h']}</code>\n"
    if s.get("oi_24h_sparkline"):
        sparkline_block += f"OI 24ч:    <code>{s['oi_24h_sparkline']}</code>\n"

    return (
        f"{header} {star_emoji} <b>{star_label}</b> — <b>{base}</b>\n\n"
        f"💵 Цена: <code>${price:.6g}</code>\n"
        f"📈 Цена: 1ч <b>{s['price_change_1h']:+.2f}%</b> | 4ч <b>{s['price_change_4h']:+.2f}%</b>\n"
        f"💰 OI: 1ч <b>{s['oi_change_1h']:+.1f}%</b> | "
        f"4ч <b>{s['oi_change_4h']:+.1f}%</b> | "
        f"24ч <b>{s['oi_change_24h']:+.1f}%</b>\n"
        f"📊 Объём: 4ч ×{s['vol_spike_4h']:.1f} | 24ч ${vol_m:.0f}M\n"
        f"📈 RSI: 1h {s['rsi_1h']:.0f} | 4h {s['rsi_4h']:.0f}\n"
        f"₿ BTC 1ч: {s['btc_1h']:+.2f}%\n"
        f"📅 Возраст: {s['age_days']}д\n\n"
        f"{sparkline_block}\n"
        f"<b>Логика:</b> {logic}\n\n"
        f"🎯 <b>TP1 (+{TP1_PCT}%):</b> <code>${tp1:.6g}</code>  — закрой 50%\n"
        f"🎯 <b>TP2 (+{TP2_PCT}%):</b> <code>${tp2:.6g}</code>  — закрой остаток\n"
        f"🛑 <b>Hard SL (−{HARD_SL_PCT}%):</b> <code>${hard_sl:.6g}</code> <i>(аварийный)</i>\n"
        f"⏱ Тайм-стоп: {POSITION_TIMEOUT_HOURS}ч\n\n"
        f"<i>Стратегия: следим за OI. При падении OI бот предупредит — это сигнал выхода.</i>"
    )


async def scan_and_alert(bot: Bot):
    async with aiohttp.ClientSession() as session:
        try:
            results = await scan_once(session)
        except Exception as e:
            log.exception("Scan failed")
            return

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        stats.reset_daily_if_needed(today)

        for s in results[:MAX_ALERTS_PER_SCAN]:
            try:
                await bot.send_message(
                    TELEGRAM_CHAT_ID, format_alert(s),
                    reply_markup=make_keyboard(s["symbol"]),
                    disable_web_page_preview=True,
                )
                last_alert[s["symbol"]] = time.time()
                stats.incr_alert(s["stars"], s["signal_type"])
                await asyncio.sleep(0.3)
            except Exception as e:
                log.warning(f"send {s['symbol']}: {e}")

            # Auto-trade hook: try to enter if eligible
            if auto_trader is not None:
                try:
                    await auto_trader.handle_signal(s)
                except Exception as e:
                    log.exception(f"auto_trade {s['symbol']}: {e}")


async def periodic_scanner(bot: Bot):
    while True:
        await scan_and_alert(bot)
        await asyncio.sleep(SCAN_INTERVAL_MIN * 60)


# ---------- Position monitor: TP / Hard SL / OI health / smart hold ----------

async def check_oi_health(session, symbol: str) -> Optional[float]:
    """Returns OI 1h change %, or None."""
    hist = await get_oi_history(session, symbol, "5min", 12)
    if len(hist) < 12: return None
    try:
        oi_now = float(hist[-1]["openInterest"])
        oi_1h_ago = float(hist[0]["openInterest"])
        if oi_1h_ago <= 0: return None
        return (oi_now - oi_1h_ago) / oi_1h_ago * 100
    except (KeyError, ValueError, TypeError):
        return None


async def check_positions(bot: Bot):
    if not positions.data: return
    async with aiohttp.ClientSession() as session:
        now = time.time()
        for symbol, pos in list(positions.data.items()):
            price = await get_current_price(session, symbol)
            if price is None: continue
            entry = pos["entry_price"]
            pnl_pct = (price - entry) / entry * 100
            age_h = (now - pos["opened_at"]) / 3600
            base = symbol.replace("USDT", "")

            # TP1
            if not pos.get("tp1_hit") and price >= pos["tp1"]:
                pnl_dollar = pnl_pct * 10  # if $1000 position
                bar = position_progress(entry, price, entry, pos["tp1"], pos["tp2"])
                try:
                    await bot.send_message(
                        TELEGRAM_CHAT_ID,
                        f"🎯 <b>{base}</b> достиг <b>TP1 (+{TP1_PCT}%)</b>\n\n"
                        f"Вход: <code>${entry:.6g}</code>\n"
                        f"Текущая: <code>${price:.6g}</code> ({pnl_pct:+.2f}%)\n"
                        f"Если торговал на $1000 → <b>+${pnl_dollar:.0f}</b>\n\n"
                        f"<code>{bar}</code>\n"
                        f"вход─TP1─────────TP2\n\n"
                        f"<b>Что делать?</b>",
                        reply_markup=make_tp1_keyboard(symbol),
                    )
                    positions.mark(symbol, "tp1_hit")
                    stats.incr("tp1_hits")
                except Exception as e:
                    log.warning(f"tp1 {symbol}: {e}")

            # TP2
            if not pos.get("tp2_hit") and price >= pos["tp2"]:
                pnl_dollar = pnl_pct * 10
                try:
                    await bot.send_message(
                        TELEGRAM_CHAT_ID,
                        f"🎯🎯 <b>{base}</b> достиг <b>TP2 (+{TP2_PCT}%)</b>\n\n"
                        f"Вход: <code>${entry:.6g}</code>\n"
                        f"Текущая: <code>${price:.6g}</code> ({pnl_pct:+.2f}%)\n"
                        f"Если торговал на $1000 → <b>+${pnl_dollar:.0f}</b>\n\n"
                        f"💡 <b>Можно:</b>\n"
                        f"  • Закрыть полностью (зафиксировать)\n"
                        f"  • Поднять SL в текущую цену (трейлинг)\n"
                        f"  • Держать пока OI растёт\n\n"
                        f"После закрытия: <code>/remove {base}</code>"
                    )
                    positions.mark(symbol, "tp2_hit")
                    stats.incr("tp2_hits")
                except Exception as e:
                    log.warning(f"tp2 {symbol}: {e}")

            # Hard SL — emergency
            if not pos.get("hard_sl_hit") and price <= pos["hard_sl"]:
                try:
                    await bot.send_message(
                        TELEGRAM_CHAT_ID,
                        f"🚨 <b>АВАРИЙНЫЙ SL</b> — <b>{base}</b>\n\n"
                        f"Цена пробила Hard SL (−{HARD_SL_PCT}%)\n"
                        f"Вход: <code>${entry:.6g}</code>\n"
                        f"Сейчас: <code>${price:.6g}</code> ({pnl_pct:+.2f}%)\n\n"
                        f"<b>Срочно закрой позицию по рынку.</b>\n"
                        f"<code>/remove {base}</code>"
                    )
                    positions.mark(symbol, "hard_sl_hit")
                    stats.incr("sl_hits")
                except Exception as e:
                    log.warning(f"hardsl {symbol}: {e}")

            # Break-even check (if user activated)
            if pos.get("be_active") and pos.get("be_price") and not pos.get("hard_sl_hit"):
                if price <= pos["be_price"]:
                    try:
                        await bot.send_message(
                            TELEGRAM_CHAT_ID,
                            f"🔒 <b>{base}</b> вернулся к безубытку\n\n"
                            f"Цена: <code>${price:.6g}</code> (вход)\n"
                            f"Рекомендую <b>закрыть</b> — твой плановый exit точка.\n"
                            f"<code>/remove {base}</code>"
                        )
                        positions.mark(symbol, "hard_sl_hit")  # treat as exit
                    except Exception as e:
                        log.warning(f"be {symbol}: {e}")

            # OI watchdog — warn if OI dropping
            warned_count = pos.get("warned_oi_drop_count", 0)
            if age_h > 0.5 and not pos.get("hard_sl_hit"):
                oi_1h = await check_oi_health(session, symbol)
                if oi_1h is not None:
                    if oi_1h <= -OI_DROP_WARNING_PCT and warned_count < 3:
                        try:
                            await bot.send_message(
                                TELEGRAM_CHAT_ID,
                                f"⚠️ <b>{base}</b>: OI ПАДАЕТ\n\n"
                                f"OI 1ч: <b>{oi_1h:+.1f}%</b>\n"
                                f"P&L сейчас: <b>{pnl_pct:+.2f}%</b>\n\n"
                                f"<b>Это сигнал выхода.</b> Тренд истощается, "
                                f"деньги уходят. Рассмотри закрытие руками."
                            )
                            positions.mark(symbol, "warned_oi_drop_count", warned_count + 1)
                        except Exception as e:
                            log.warning(f"oi warn {symbol}: {e}")
                    # Smart hold: OI растёт и мы в плюсе
                    elif pnl_pct >= SMART_HOLD_THRESHOLD_PCT and oi_1h > 0 and not pos.get("smart_hold_active"):
                        try:
                            await bot.send_message(
                                TELEGRAM_CHAT_ID,
                                f"💚 <b>{base}</b>: smart hold\n\n"
                                f"P&L: <b>{pnl_pct:+.2f}%</b>\n"
                                f"OI 1ч: <b>{oi_1h:+.1f}%</b> (растёт!)\n\n"
                                f"<i>Тренд здоровый. Можно держать дальше — "
                                f"бот предупредит когда OI начнёт падать.</i>"
                            )
                            positions.mark(symbol, "smart_hold_active")
                        except Exception as e:
                            log.warning(f"smart hold {symbol}: {e}")

            # Timeout
            if age_h >= POSITION_TIMEOUT_HOURS and not pos.get("warned_timeout"):
                try:
                    await bot.send_message(
                        TELEGRAM_CHAT_ID,
                        f"⏰ <b>{base}</b> в позиции {age_h:.0f}ч — тайм-стоп\n\n"
                        f"Цена: <code>${price:.6g}</code> ({pnl_pct:+.2f}%)\n\n"
                        f"Рекомендую закрыть. <code>/remove {base}</code>"
                    )
                    positions.mark(symbol, "warned_timeout")
                    stats.incr("timeouts")
                except Exception as e:
                    log.warning(f"timeout {symbol}: {e}")


async def position_monitor_loop(bot: Bot):
    while True:
        try:
            await check_positions(bot)
        except Exception as e:
            log.exception(f"position monitor: {e}")
        await asyncio.sleep(POSITION_CHECK_INTERVAL_MIN * 60)


# ---------- Daily report ----------

async def daily_report_loop(bot: Bot):
    sent_today = None
    while True:
        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")
        if now.hour == DAILY_REPORT_HOUR_UTC and sent_today != today:
            try:
                stats.reset_daily_if_needed(today)
                by_type = stats.data.get("alerts_by_type", {})
                by_star = stats.data.get("alerts_by_star", {})
                msg = (
                    f"📊 <b>Дневной отчёт</b>\n\n"
                    f"Алертов сегодня: <b>{stats.data['alerts_today']}</b>\n"
                    f"  ⭐ {by_star.get('1', 0)} | ⭐⭐ {by_star.get('2', 0)} | ⭐⭐⭐ {by_star.get('3', 0)}\n\n"
                    f"<b>Типы сигналов (всего):</b>\n"
                    f"  STANDARD: {by_type.get('STANDARD', 0)}\n"
                    f"  SURGE: {by_type.get('SURGE', 0)}\n"
                    f"  PULLBACK: {by_type.get('PULLBACK', 0)}\n\n"
                    f"Открытых позиций: <b>{len(positions.data)}</b>\n\n"
                    f"<b>Итого:</b> {stats.data['alerts_total']} алертов, "
                    f"TP1: {stats.data.get('tp1_hits', 0)}, "
                    f"TP2: {stats.data.get('tp2_hits', 0)}, "
                    f"SL: {stats.data.get('sl_hits', 0)}"
                )
                await bot.send_message(TELEGRAM_CHAT_ID, msg)
                sent_today = today
            except Exception as e:
                log.warning(f"daily report: {e}")
        await asyncio.sleep(300)


# ---------- Commands ----------

dp = Dispatcher()


@dp.message(Command("start", "help"))
async def cmd_start(msg: types.Message):
    await msg.answer(
        "✅ <b>OI LONG Scanner v2</b>\n\n"
        "<b>3 типа сигналов:</b>\n"
        "🟢 STANDARD — цена↑ + OI↑ 4ч + объём\n"
        "⚡ SURGE — цена↑ + OI↑ 1ч (ранний вход)\n"
        "↩️ PULLBACK — откат к EMA21 в тренде\n\n"
        "<b>Логика выхода:</b>\n"
        "🎯 TP1 +2% (закрыть 50%)\n"
        "🎯 TP2 +5% (закрыть остаток)\n"
        f"🛑 Hard SL −{HARD_SL_PCT}% (аварийный)\n"
        "⚠️ OI watchdog: при падении OI бот алертит — выходи руками\n"
        "💚 Smart hold: при растущем OI бот скажет «держи»\n\n"
        "<b>Команды:</b>\n"
        "/scan /settings /positions /stats\n"
        "/top_oi /active /ignored /unignore SYM\n"
        "/add SYM PRICE /remove SYM\n\n"
        "<b>🤖 Авто-торговля:</b>\n"
        "/auto — статус\n"
        "/auto_on — включить\n"
        "/auto_off — выключить\n"
        "/sig_off TYPE — выключить тип (PULLBACK / SURGE / STANDARD)\n"
        "/sig_on TYPE — включить тип\n"
        "/cooldown — список монет в пост-сделочном блоке\n"
        "/cooldown_clear SYM — снять блок с монеты\n"
        "/panic — закрыть всё + блок\n"
        "/resume — снять блок"
    )


@dp.message(Command("scan"))
async def cmd_scan(msg: types.Message):
    await msg.answer("🔍 Сканирую...")
    await scan_and_alert(msg.bot)
    await msg.answer("✅ Готово.")


@dp.message(Command("settings"))
async def cmd_settings(msg: types.Message):
    await msg.answer(
        f"<b>Сканер:</b>\n"
        f"• Интервал: {SCAN_INTERVAL_MIN} мин\n"
        f"• Кулдаун: {ALERT_COOLDOWN_HOURS}ч\n"
        f"• Возраст: ≥{MIN_AGE_DAYS}д\n"
        f"• Объём 24ч: ≥${MIN_VOLUME_USD_24H/1e6:.0f}M\n\n"
        f"<b>🟢 STANDARD:</b>\n"
        f"• Цена 4ч: +{PRICE_CHANGE_4H_MIN}…+{PRICE_CHANGE_4H_MAX}%\n"
        f"• OI 4ч: ≥+{OI_CHANGE_4H_MIN}%\n"
        f"• Объём 4ч: ≥×{VOLUME_SPIKE_MIN}\n"
        f"• RSI 4ч: {RSI_4H_MIN}–{RSI_4H_MAX}\n"
        f"• ⭐⭐: OI 24ч ≥+{OI_CHANGE_24H_2STAR}% И объём ×{VOLUME_SPIKE_2STAR}\n"
        f"• ⭐⭐⭐: ⭐⭐ + пробой 24ч максимума + BTC 1ч ≥ 0%\n\n"
        f"<b>⚡ SURGE:</b> {'ON' if ENABLE_OI_SURGE else 'OFF'}\n"
        f"• Цена 1ч: +{SURGE_PRICE_1H_MIN}…+{SURGE_PRICE_1H_MAX}%\n"
        f"• OI 1ч: ≥+{SURGE_OI_1H_MIN}%\n"
        f"• OI 24ч: ≥+{SURGE_OI_24H_MIN}% (защита от short squeeze)\n"
        f"• RSI 1ч: ≤{SURGE_RSI_1H_MAX}\n"
        f"• ⭐⭐⭐: ⭐⭐ + объём 4ч ×1.5 + BTC 1ч ≥ 0%\n\n"
        f"<b>↩️ PULLBACK:</b> {'ON' if ENABLE_PULLBACK else 'OFF'}\n"
        f"• Цена &gt; EMA50, EMA21 &gt; EMA50\n"
        f"• Расстояние до EMA21: ≤{PULLBACK_EMA_DISTANCE_PCT}%\n"
        f"• RSI 1ч: {PULLBACK_RSI_1H_MIN}–{PULLBACK_RSI_1H_MAX}\n"
        f"• OI 24ч: ≥+{PULLBACK_OI_24H_MIN}%\n"
        f"• OI 1ч: ≥+{PULLBACK_OI_1H_MIN}% (не падает)\n"
        f"• 2 зелёные свечи подряд на 1h\n"
        f"• ⭐⭐⭐: ⭐⭐ + OI 1ч ≥+3% + объём 1ч ×1.5\n\n"
        f"<b>Ручная сделка (трекер):</b>\n"
        f"• TP1: +{TP1_PCT}% / TP2: +{TP2_PCT}%\n"
        f"• Hard SL: −{HARD_SL_PCT}%\n"
        f"• Тайм-стоп: {POSITION_TIMEOUT_HOURS}ч\n\n"
        f"<b>Авто-торговля:</b> /auto"
    )


@dp.message(Command("top_oi"))
async def cmd_top_oi(msg: types.Message):
    await msg.answer("📊 Считаю топ OI 4ч...")
    async with aiohttp.ClientSession() as session:
        instruments = await get_instruments(session)
        tickers = await get_tickers(session)
        candidates = []
        for inst in instruments[:300]:
            symbol = inst.get("symbol", "")
            if not symbol.endswith("USDT") or is_blacklisted(symbol): continue
            if inst.get("contractType") != "LinearPerpetual": continue
            t = tickers.get(symbol)
            if not t: continue
            try:
                if float(t.get("turnover24h", 0)) < MIN_VOLUME_USD_24H: continue
            except (ValueError, TypeError):
                continue
            candidates.append(symbol)

        async def calc(symbol):
            async with SEM:
                hist = await get_oi_history(session, symbol, "4h", 2)
            if len(hist) < 2: return None
            try:
                now_oi = float(hist[-1]["openInterest"])
                prev = float(hist[-2]["openInterest"])
                if prev <= 0: return None
                return symbol, (now_oi - prev) / prev * 100
            except (KeyError, ValueError, TypeError):
                return None

        results = await asyncio.gather(*[calc(s) for s in candidates])
        valid = [r for r in results if r is not None]
        valid.sort(key=lambda x: -x[1])

    if not valid:
        await msg.answer("Нет данных.")
        return
    lines = ["<b>Топ-10 по OI 4ч:</b>\n"]
    for sym, ch in valid[:10]:
        base = sym.replace("USDT", "")
        lines.append(f"• {base}: <b>{ch:+.1f}%</b>")
    await msg.answer("\n".join(lines))


@dp.message(Command("active"))
async def cmd_active(msg: types.Message):
    now = time.time()
    active = [(s, ts) for s, ts in last_alert.items() if (now - ts) < ALERT_COOLDOWN_HOURS * 3600]
    if not active:
        await msg.answer("В кулдауне нет.")
        return
    lines = [f"<b>В кулдауне ({len(active)}):</b>"]
    for sym, ts in sorted(active, key=lambda x: -x[1])[:15]:
        base = sym.replace("USDT", "")
        mins = int((ALERT_COOLDOWN_HOURS * 3600 - (now - ts)) / 60)
        lines.append(f"• {base}: {mins} мин")
    await msg.answer("\n".join(lines))


@dp.message(Command("ignored"))
async def cmd_ignored(msg: types.Message):
    a = ignore.list_active()
    if not a:
        await msg.answer("Игнор-лист пуст.")
        return
    lines = ["<b>Игнорируемые:</b>"]
    for sym, ts in a.items():
        base = sym.replace("USDT", "")
        h = int((ts - time.time()) / 3600)
        lines.append(f"• {base}: {h}ч")
    await msg.answer("\n".join(lines))


@dp.message(Command("unignore"))
async def cmd_unignore(msg: types.Message):
    parts = (msg.text or "").split()
    if len(parts) != 2:
        await msg.answer("Использование: <code>/unignore SYMBOL</code>")
        return
    if ignore.remove(parts[1]):
        await msg.answer(f"✅ {parts[1].upper()} убран.")
    else:
        await msg.answer(f"⚠️ Не в игноре.")


@dp.message(Command("stats"))
async def cmd_stats(msg: types.Message):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    stats.reset_daily_if_needed(today)
    tp1 = stats.data.get("tp1_hits", 0)
    sl = stats.data.get("sl_hits", 0)
    total_closed = tp1 + sl
    winrate = (tp1 / total_closed * 100) if total_closed > 0 else 0
    by_type = stats.data.get("alerts_by_type", {})
    by_star = stats.data.get("alerts_by_star", {})
    await msg.answer(
        f"📊 <b>Статистика</b>\n\n"
        f"<b>Сегодня:</b> {stats.data['alerts_today']} алертов\n"
        f"⭐{by_star.get('1',0)} ⭐⭐{by_star.get('2',0)} ⭐⭐⭐{by_star.get('3',0)}\n\n"
        f"<b>Типы (всего):</b>\n"
        f"STANDARD: {by_type.get('STANDARD',0)}\n"
        f"SURGE: {by_type.get('SURGE',0)}\n"
        f"PULLBACK: {by_type.get('PULLBACK',0)}\n\n"
        f"<b>Всего:</b>\n"
        f"Алертов: {stats.data['alerts_total']}\n"
        f"TP1: {tp1} | TP2: {stats.data.get('tp2_hits',0)}\n"
        f"SL: {sl} | Timeouts: {stats.data.get('timeouts',0)}\n"
        f"Винрейт (TP1 vs SL): <b>{winrate:.1f}%</b>\n\n"
        f"Открытых позиций: <b>{len(positions.data)}</b>"
    )


@dp.message(Command("add"))
async def cmd_add(msg: types.Message):
    parts = (msg.text or "").split()
    if len(parts) != 3:
        await msg.answer("Использование: <code>/add SYMBOL PRICE</code>")
        return
    symbol = parts[1].upper()
    try:
        price = float(parts[2])
    except ValueError:
        await msg.answer("❌ Цена должна быть числом.")
        return
    tp1 = price * (1 + TP1_PCT / 100)
    tp2 = price * (1 + TP2_PCT / 100)
    sl = price * (1 - HARD_SL_PCT / 100)
    if positions.add(symbol, price, tp1, tp2, sl, "MANUAL"):
        base = symbol.replace("USDT", "")
        await msg.answer(
            f"✅ <b>{base} LONG</b>\n"
            f"Вход: <code>${price:.6g}</code>\n"
            f"TP1: <code>${tp1:.6g}</code> / TP2: <code>${tp2:.6g}</code>\n"
            f"Hard SL: <code>${sl:.6g}</code>"
        )
    else:
        await msg.answer(f"⚠️ {symbol} уже в трекере.")


@dp.message(Command("positions"))
async def cmd_positions(msg: types.Message):
    manual_positions = positions.list_all()
    auto_positions = auto_state.active_positions if auto_trader else {}

    if not manual_positions and not auto_positions:
        await msg.answer("Открытых позиций нет.")
        return

    lines = ["<b>📊 Открытые позиции:</b>"]
    total_pnl_pct = 0.0
    total_pnl_usd = 0.0

    async with aiohttp.ClientSession() as session:
        # ---------- Auto-positions (Bybit-managed) ----------
        if auto_positions:
            lines.append("\n<b>🤖 Авто-торговля:</b>")
            for symbol, pos in auto_positions.items():
                base = symbol.replace("USDT", "")
                price = await get_current_price(session, symbol)
                age_h = (time.time() - pos["opened_at"]) / 3600
                if price is None:
                    lines.append(f"\n• <b>{base}</b> — нет цены")
                    continue
                entry = pos["entry_price"]
                tp_price = pos["tp_price"]
                sl_price = pos["sl_price"]
                pnl_pct = (price - entry) / entry * 100
                pnl_usd = pnl_pct / 100 * POSITION_SIZE_USD
                total_pnl_pct += pnl_pct
                total_pnl_usd += pnl_usd

                # Status icon
                if price >= tp_price:
                    icon = "✅"
                elif price <= sl_price:
                    icon = "🛑"
                else:
                    icon = "⏳"

                # Progress bar: from entry to TP
                if price >= tp_price:
                    progress = "██████████"
                    label = "TP достигнут"
                elif price >= entry:
                    progress = progress_bar(price, entry, tp_price)
                    label = f"до TP +{((tp_price - entry) / entry * 100):.2f}%"
                else:
                    # In drawdown — show progress from SL to entry (reverse risk meter)
                    progress = progress_bar(price, sl_price, entry)
                    label = f"в просадке (до SL: {((price - sl_price) / sl_price * 100):.2f}%)"

                sig_label = f"{pos['signal_type']} {'⭐' * pos['stars']}"
                lines.append(
                    f"\n{icon} <b>{base}</b>  <i>{sig_label}</i>\n"
                    f"  <code>${entry:.6g}</code> → <code>${price:.6g}</code>  "
                    f"<b>{pnl_pct:+.2f}%</b> (<b>${pnl_usd:+.2f}</b>) | {age_h:.1f}ч\n"
                    f"  <code>[{progress}]</code> {label}\n"
                    f"  🎯 <code>${tp_price:.6g}</code> | 🛑 <code>${sl_price:.6g}</code> | "
                    f"плечо {pos['leverage']:.0f}x"
                )

        # ---------- Manual positions (legacy tracker) ----------
        if manual_positions:
            lines.append("\n<b>✋ Ручные (трекер):</b>")
            for symbol, pos in manual_positions.items():
                base = symbol.replace("USDT", "")
                price = await get_current_price(session, symbol)
                age_h = (time.time() - pos["opened_at"]) / 3600
                if price is None:
                    lines.append(f"\n• <b>{base}</b> — нет цены")
                    continue
                entry = pos["entry_price"]
                pnl_pct = (price - entry) / entry * 100
                total_pnl_pct += pnl_pct

                if pos.get("tp2_hit"):
                    icon = "✅✅"
                elif pos.get("tp1_hit"):
                    icon = "✅"
                elif pos.get("hard_sl_hit"):
                    icon = "🛑"
                elif age_h >= POSITION_TIMEOUT_HOURS:
                    icon = "⏰"
                else:
                    icon = "⏳"

                if not pos.get("tp1_hit"):
                    progress = progress_bar(price, entry, pos["tp1"])
                    target_label = "до TP1"
                elif not pos.get("tp2_hit"):
                    progress = progress_bar(price, pos["tp1"], pos["tp2"])
                    target_label = "до TP2"
                else:
                    progress = "██████████"
                    target_label = "TP2 достигнут"

                be_marker = " 🔒BE" if pos.get("be_active") else ""
                lines.append(
                    f"\n{icon} <b>{base}</b>{be_marker}\n"
                    f"  <code>${entry:.6g}</code> → <code>${price:.6g}</code> "
                    f"<b>{pnl_pct:+.2f}%</b> ({age_h:.1f}ч)\n"
                    f"  <code>[{progress}]</code> {target_label}"
                )

    summary = f"\n<b>Σ P&amp;L: {total_pnl_pct:+.2f}%</b>"
    if auto_positions:
        summary += f"  (≈ <b>${total_pnl_usd:+.2f}</b>)"
    lines.append(summary)
    await msg.answer("\n".join(lines))


@dp.message(Command("remove"))
async def cmd_remove(msg: types.Message):
    parts = (msg.text or "").split()
    if len(parts) != 2:
        await msg.answer("<code>/remove SYMBOL</code>")
        return
    if positions.remove(parts[1]):
        await msg.answer(f"✅ {parts[1].upper()} удалён.")
    else:
        await msg.answer(f"⚠️ Нет в трекере.")


# ---------- Auto-trade commands ----------

@dp.message(Command("auto"))
async def cmd_auto(msg: types.Message):
    if auto_trader is None:
        await msg.answer(
            "🤖 Авто-торговля <b>не настроена</b>.\n"
            "BYBIT_API_KEY / BYBIT_API_SECRET не заданы в Railway Variables."
        )
        return
    enabled = auto_state.is_enabled()
    blocked = auto_state.is_blocked()
    reason = auto_state.blocked_reason
    apos = auto_state.active_positions
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    auto_state.maybe_reset_day(today)

    status_line = "🟢 ВКЛ" if enabled else "🔴 ВЫКЛ"
    if blocked:
        status_line += f" (🚫 заблокировано: {reason})"

    # Per-signal-type status
    sig_status_lines = []
    for sig_type in ["STANDARD", "SURGE", "PULLBACK"]:
        on = auto_state.get_signal_toggle(sig_type)
        emoji = "🟢" if on else "🔴"
        if sig_type == "PULLBACK":
            params = f"TP +{AUTO_PULLBACK_TP_PCT}% / SL −{AUTO_PULLBACK_SL_PCT}%"
        else:
            params = f"TP +{AUTO_TP_PCT}% / SL −{AUTO_HARD_SL_PCT}%"
        sig_status_lines.append(f"  {emoji} {sig_type}: {params}")
    sig_status_block = "\n".join(sig_status_lines)

    # BTC filter status
    btc_filter_active = BTC_FILTER_ENABLED and auto_state.is_btc_filter_enabled()
    btc_filter_line = "🟢 ВКЛ" if btc_filter_active else "🔴 ВЫКЛ"

    pos_lines = []
    if apos:
        for sym, p in apos.items():
            base = sym.replace("USDT", "")
            age_min = (time.time() - p["opened_at"]) / 60
            pos_lines.append(
                f"• <b>{base}</b> {p['signal_type']} {'⭐'*p['stars']}\n"
                f"  Вход <code>${p['entry_price']:.6g}</code> | "
                f"TP <code>${p['tp_price']:.6g}</code> | "
                f"SL <code>${p['sl_price']:.6g}</code>\n"
                f"  Плечо {p['leverage']:.0f}x | {age_min:.0f} мин"
            )
    pos_block = "\n".join(pos_lines) if pos_lines else "<i>Нет открытых авто-позиций</i>"

    await msg.answer(
        f"🤖 <b>Auto-trading status</b>\n\n"
        f"Общее состояние: {status_line}\n"
        f"Размер позиции: <b>${POSITION_SIZE_USD}</b>\n"
        f"Max позиций: {len(apos)}/{MAX_AUTO_POSITIONS}\n"
        f"BTC-фильтр: {btc_filter_line}\n\n"
        f"<b>По типам сигналов:</b>\n{sig_status_block}\n\n"
        f"<b>Сегодня (UTC):</b>\n"
        f"P&L: <b>${auto_state.daily_pnl:+.2f}</b> (лимит −${DAILY_LOSS_LIMIT_USD})\n"
        f"Подряд убытков: {auto_state.consecutive_losses}/{CONSECUTIVE_LOSS_BLOCK}\n\n"
        f"<b>Открытые позиции:</b>\n{pos_block}\n\n"
        f"<b>Команды:</b>\n"
        f"/auto_on — включить общий\n"
        f"/auto_off — выключить общий\n"
        f"/sig_off TYPE — выключить тип (STANDARD/SURGE/PULLBACK)\n"
        f"/sig_on TYPE — включить тип\n"
        f"/btc_filter — статус BTC-фильтра\n"
        f"/cooldown — список монет в кулдауне ({POST_TRADE_COOLDOWN_HOURS}ч)\n"
        f"/panic — закрыть всё + блок\n"
        f"/resume — снять блок"
    )


@dp.message(Command("auto_on"))
async def cmd_auto_on(msg: types.Message):
    if auto_trader is None:
        await msg.answer("❌ Авто-торговля не настроена (нет API ключей).")
        return
    if auto_state.is_blocked():
        await msg.answer(
            f"🚫 Заблокировано ({auto_state.blocked_reason}). "
            f"Сначала /resume чтобы снять блок."
        )
        return
    # Verify API keys work by querying balance
    balance = await trader.get_wallet_balance_usdt()
    if balance is None:
        await msg.answer(
            "❌ Не смог получить баланс. Проверь API ключи и permissions.\n"
            "Нужны права: Contract Orders + Contract Positions."
        )
        return
    auto_state.set_enabled(True)
    await msg.answer(
        f"🟢 <b>Авто-торговля ВКЛЮЧЕНА</b>\n\n"
        f"Баланс USDT: <b>${balance:.2f}</b>\n"
        f"Сигналы: <code>{AUTO_TRADE_SIGNAL_TYPES}</code>\n"
        f"Размер позиции: ${POSITION_SIZE_USD}\n\n"
        f"Бот будет автоматически входить при сигналах."
    )


@dp.message(Command("auto_off"))
async def cmd_auto_off(msg: types.Message):
    auto_state.set_enabled(False)
    apos = len(auto_state.active_positions)
    await msg.answer(
        f"🔴 <b>Авто-торговля ВЫКЛЮЧЕНА</b>\n\n"
        f"Новые сигналы — не входим.\n"
        f"Открытых авто-позиций: <b>{apos}</b>\n"
        f"Они продолжают торговаться (TP/SL уже на бирже).\n"
        f"Закрыть всё мгновенно: /panic"
    )


@dp.message(Command("panic"))
async def cmd_panic(msg: types.Message):
    if auto_trader is None:
        await msg.answer("❌ Авто-торговля не настроена.")
        return
    apos = len(auto_state.active_positions)
    if apos == 0:
        auto_state.block_panic()
        await msg.answer("🚫 Открытых авто-позиций нет. Авто-торговля заблокирована.")
        return
    await msg.answer(f"🚨 PANIC: закрываю {apos} позиций по рынку...")
    ok, fail = await auto_trader.panic_close_all()
    await msg.answer(
        f"🚨 <b>PANIC завершён</b>\n\n"
        f"Закрыто успешно: {ok}\n"
        f"Ошибок: {fail}\n\n"
        f"Авто-торговля заблокирована. /resume чтобы вернуть."
    )


@dp.message(Command("resume"))
async def cmd_resume(msg: types.Message):
    if auto_trader is None:
        await msg.answer("❌ Авто-торговля не настроена.")
        return
    if not auto_state.is_blocked():
        await msg.answer("ℹ️ Блокировки нет.")
        return
    reason = auto_state.blocked_reason
    auto_state.unblock()
    await msg.answer(
        f"✅ Блокировка снята (была: <b>{reason}</b>)\n"
        f"Счётчик убытков обнулён.\n"
        f"Авто-торговля: {'🟢 ВКЛ' if auto_state.is_enabled() else '🔴 ВЫКЛ'}\n"
        f"Чтобы включить: /auto_on"
    )


@dp.message(Command("sig_on"))
async def cmd_sig_on(msg: types.Message):
    parts = (msg.text or "").split()
    if len(parts) != 2:
        await msg.answer(
            "Использование: <code>/sig_on TYPE</code>\n"
            "TYPE: STANDARD, SURGE или PULLBACK\n"
            "Пример: <code>/sig_on PULLBACK</code>"
        )
        return
    sig_type = parts[1].upper()
    if sig_type not in ("STANDARD", "SURGE", "PULLBACK"):
        await msg.answer("❌ TYPE должен быть STANDARD, SURGE или PULLBACK.")
        return
    auto_state.set_signal_toggle(sig_type, True)
    if sig_type == "PULLBACK":
        params = f"TP +{AUTO_PULLBACK_TP_PCT}% / SL −{AUTO_PULLBACK_SL_PCT}%"
    else:
        params = f"TP +{AUTO_TP_PCT}% / SL −{AUTO_HARD_SL_PCT}%"
    await msg.answer(
        f"🟢 <b>{sig_type}</b> авто-торговля ВКЛЮЧЕНА\n"
        f"Параметры: {params}\n\n"
        f"Общий статус: /auto"
    )


@dp.message(Command("sig_off"))
async def cmd_sig_off(msg: types.Message):
    parts = (msg.text or "").split()
    if len(parts) != 2:
        await msg.answer(
            "Использование: <code>/sig_off TYPE</code>\n"
            "TYPE: STANDARD, SURGE или PULLBACK\n"
            "Пример: <code>/sig_off PULLBACK</code>"
        )
        return
    sig_type = parts[1].upper()
    if sig_type not in ("STANDARD", "SURGE", "PULLBACK"):
        await msg.answer("❌ TYPE должен быть STANDARD, SURGE или PULLBACK.")
        return
    auto_state.set_signal_toggle(sig_type, False)
    await msg.answer(
        f"🔴 <b>{sig_type}</b> авто-торговля ВЫКЛЮЧЕНА\n"
        f"Сигналы продолжат приходить в Telegram, но бот не будет автоматически входить.\n"
        f"Другие типы сигналов работают как обычно.\n\n"
        f"Общий статус: /auto"
    )


@dp.message(Command("cooldown"))
async def cmd_cooldown(msg: types.Message):
    cooldowns = auto_state.list_post_trade_cooldowns()
    if not cooldowns:
        await msg.answer(
            f"🔓 Монет в пост-сделочном кулдауне нет.\n\n"
            f"После закрытия любой сделки монета блокируется "
            f"на <b>{POST_TRADE_COOLDOWN_HOURS}ч</b> от авто-входа."
        )
        return
    now = time.time()
    lines = [f"🔒 <b>В кулдауне после сделок ({len(cooldowns)}):</b>\n"]
    for symbol, ts in sorted(cooldowns.items(), key=lambda x: x[1]):
        base = symbol.replace("USDT", "")
        hours_left = (ts - now) / 3600
        if hours_left >= 1:
            time_str = f"{hours_left:.1f}ч"
        else:
            time_str = f"{int(hours_left * 60)}мин"
        lines.append(f"• <b>{base}</b>: ещё {time_str}")
    lines.append(
        f"\n<i>Алерты по этим монетам продолжают приходить, "
        f"но бот не будет автоматически в них входить.</i>\n\n"
        f"Снять блок вручную: <code>/cooldown_clear SYMBOL</code>"
    )
    await msg.answer("\n".join(lines))


@dp.message(Command("cooldown_clear"))
async def cmd_cooldown_clear(msg: types.Message):
    parts = (msg.text or "").split()
    if len(parts) != 2:
        await msg.answer(
            "Использование: <code>/cooldown_clear SYMBOL</code>\n"
            "Пример: <code>/cooldown_clear ARB</code>"
        )
        return
    symbol = parts[1].upper()
    if not symbol.endswith("USDT"):
        symbol += "USDT"
    if auto_state.remove_post_trade_cooldown(symbol):
        base = symbol.replace("USDT", "")
        await msg.answer(
            f"🔓 <b>{base}</b> снят с пост-сделочного кулдауна.\n"
            f"Теперь может быть авто-куплен при следующем сигнале."
        )
    else:
        await msg.answer(f"⚠️ {symbol} не в кулдауне.")


@dp.message(Command("btc_filter"))
async def cmd_btc_filter(msg: types.Message):
    """Show current BTC market state and whether filter passes."""
    enabled_global = BTC_FILTER_ENABLED
    enabled_user = auto_state.is_btc_filter_enabled()
    actually_active = enabled_global and enabled_user

    state_text = "🟢 ВКЛ" if actually_active else "🔴 ВЫКЛ"
    if enabled_global and not enabled_user:
        state_text = "🔴 ВЫКЛ (отключено командой)"
    elif not enabled_global:
        state_text = "🔴 ВЫКЛ (в конфиге BTC_FILTER_ENABLED=false)"

    await msg.answer("📊 Проверяю состояние BTC...")
    health = await check_btc_health()
    ok_icon = "✅" if health["is_ok"] else "⛔"
    ok_text = "проходит" if health["is_ok"] else "БЛОКИРУЕТ авто-вход"
    reason_line = f"\nПричина блока: <b>{health['reason']}</b>" if not health["is_ok"] else ""

    await msg.answer(
        f"📊 <b>BTC Filter Status</b>\n\n"
        f"Состояние фильтра: {state_text}\n\n"
        f"<b>Текущие значения BTC:</b>\n"
        f"• 15м изменение: <b>{health['change_15m']:+.2f}%</b> "
        f"(лимит: −{BTC_FILTER_15M_DROP_MAX}% / +{BTC_FILTER_15M_PUMP_MAX}%)\n"
        f"• 1ч волатильность: <b>{health['volatility_1h']:.2f}%</b> "
        f"(лимит: {BTC_FILTER_1H_VOLATILITY_MAX}%)\n\n"
        f"{ok_icon} Сейчас фильтр {ok_text}{reason_line}\n\n"
        f"<b>Команды:</b>\n"
        f"/btc_filter_on — включить\n"
        f"/btc_filter_off — выключить"
    )


@dp.message(Command("btc_filter_on"))
async def cmd_btc_filter_on(msg: types.Message):
    auto_state.set_btc_filter(True)
    await msg.answer(
        f"🟢 BTC-фильтр для авто-торговли ВКЛЮЧЕН\n\n"
        f"Авто-вход блокируется когда:\n"
        f"• BTC падает быстрее <b>−{BTC_FILTER_15M_DROP_MAX}%</b> за 15м\n"
        f"• BTC растёт быстрее <b>+{BTC_FILTER_15M_PUMP_MAX}%</b> за 15м\n"
        f"• Волатильность 1ч выше <b>{BTC_FILTER_1H_VOLATILITY_MAX}%</b>"
    )


@dp.message(Command("btc_filter_off"))
async def cmd_btc_filter_off(msg: types.Message):
    auto_state.set_btc_filter(False)
    await msg.answer(
        f"🔴 BTC-фильтр ВЫКЛЮЧЕН\n\n"
        f"⚠️ Бот будет авто-входить в любые сигналы независимо от состояния BTC.\n"
        f"Включить обратно: /btc_filter_on"
    )


# ---------- Callbacks ----------

@dp.callback_query(F.data.startswith("in:"))
async def cb_in_long(cb: types.CallbackQuery):
    symbol = cb.data.split(":")[1]
    async with aiohttp.ClientSession() as session:
        price = await get_current_price(session, symbol)
    if price is None:
        await cb.answer("Не получил цену.", show_alert=True)
        return
    tp1 = price * (1 + TP1_PCT / 100)
    tp2 = price * (1 + TP2_PCT / 100)
    sl = price * (1 - HARD_SL_PCT / 100)
    if positions.add(symbol, price, tp1, tp2, sl):
        base = symbol.replace("USDT", "")
        await cb.message.answer(
            f"✅ <b>{base} LONG</b> записан\n"
            f"Вход: <code>${price:.6g}</code>\n"
            f"TP1: <code>${tp1:.6g}</code> | TP2: <code>${tp2:.6g}</code>\n"
            f"Hard SL: <code>${sl:.6g}</code>"
        )
        await cb.answer("Записано")
    else:
        await cb.answer("Уже в трекере.", show_alert=True)


@dp.callback_query(F.data.startswith("oi:"))
async def cb_oi_chart(cb: types.CallbackQuery):
    symbol = cb.data.split(":")[1]
    base = symbol.replace("USDT", "")
    async with aiohttp.ClientSession() as session:
        hist = await get_oi_history(session, symbol, "1h", 24)
    if not hist or len(hist) < 6:
        await cb.answer("Нет данных.", show_alert=True)
        return
    try:
        values = [float(h["openInterest"]) for h in hist]
    except (KeyError, ValueError):
        await cb.answer("Ошибка.", show_alert=True)
        return
    vmin, vmax = min(values), max(values)
    if vmax == vmin:
        await cb.message.answer(f"📊 OI {base}: без изменений")
        await cb.answer()
        return
    lines = [f"📊 <b>OI {base} 24ч:</b>\n<pre>"]
    bar_w = 18
    for i, v in enumerate(values):
        n = (v - vmin) / (vmax - vmin)
        bars = int(n * bar_w)
        ts = int(hist[i].get("timestamp", 0)) // 1000
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M")
        lines.append(f"{dt} {'█'*bars}{'░'*(bar_w-bars)}")
    lines.append("</pre>")
    change = (values[-1] - values[0]) / values[0] * 100 if values[0] > 0 else 0
    lines.append(f"\n<b>Изменение 24ч: {change:+.1f}%</b>")
    await cb.message.answer("\n".join(lines))
    await cb.answer()


@dp.callback_query(F.data.startswith("ign:"))
async def cb_ignore(cb: types.CallbackQuery):
    symbol = cb.data.split(":")[1]
    ignore.add(symbol, IGNORE_DURATION_HOURS)
    base = symbol.replace("USDT", "")
    await cb.message.answer(f"❌ <b>{base}</b> в игноре {IGNORE_DURATION_HOURS}ч")
    await cb.answer("Игнор")


@dp.callback_query(F.data.startswith("be:"))
async def cb_breakeven(cb: types.CallbackQuery):
    symbol = cb.data.split(":")[1]
    pos = positions.get(symbol)
    if not pos:
        await cb.answer("Позиции нет в трекере.", show_alert=True)
        return
    positions.activate_breakeven(symbol)
    base = symbol.replace("USDT", "")
    await cb.message.answer(
        f"🔒 <b>{base}</b>: SL поднят в безубыток\n"
        f"Новый exit: <code>${pos['entry_price']:.6g}</code> (вход)\n"
        f"Дальше можно держать без риска."
    )
    await cb.answer("Безубыток активен")


@dp.callback_query(F.data.startswith("ack_tp1:"))
async def cb_ack_tp1(cb: types.CallbackQuery):
    symbol = cb.data.split(":")[1]
    base = symbol.replace("USDT", "")
    await cb.message.answer(f"✅ Закрыл 50% по {base}. Остаток до TP2 — бот следит.")
    await cb.answer()


@dp.callback_query(F.data.startswith("close:"))
async def cb_close(cb: types.CallbackQuery):
    symbol = cb.data.split(":")[1]
    if positions.remove(symbol):
        base = symbol.replace("USDT", "")
        await cb.message.answer(f"✅ <b>{base}</b> закрыт и удалён из трекера.")
    await cb.answer("Закрыто")


# ---------- Entry ----------

async def main():
    global trader, auto_trader

    bot = Bot(
        token=TELEGRAM_BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    log.info("OI Scanner v2 starting...")

    # Initialize auto-trading if API keys present
    auto_status = "🔴 Авто-торговля: НЕ настроена (нет API ключей)"
    if BYBIT_API_KEY and BYBIT_API_SECRET:
        trader = BybitTrader(BYBIT_API_KEY, BYBIT_API_SECRET)
        # Verify connection
        balance = await trader.get_wallet_balance_usdt()
        if balance is None:
            auto_status = (
                "🔴 Авто-торговля: <b>ОШИБКА ПОДКЛЮЧЕНИЯ</b>\n"
                "Проверь BYBIT_API_KEY и BYBIT_API_SECRET в Variables.\n"
                "Нужны permissions: Contract Orders + Contract Positions."
            )
            trader = None
        else:
            await trader.get_instruments_cached()  # warm up
            auto_trader = AutoTrader(bot, trader, auto_state)
            enabled = "🟢 ВКЛ" if auto_state.is_enabled() else "🔴 ВЫКЛ"
            blocked = " (🚫 blocked)" if auto_state.is_blocked() else ""
            auto_status = (
                f"🤖 Авто-торговля: настроена\n"
                f"  Состояние: {enabled}{blocked}\n"
                f"  Баланс USDT: <b>${balance:.2f}</b>\n"
                f"  Размер сделки: ${POSITION_SIZE_USD}\n"
                f"  Сигналы: {AUTO_TRADE_SIGNAL_TYPES}\n"
                f"  Управление: /auto"
            )

    try:
        await bot.send_message(
            TELEGRAM_CHAT_ID,
            "🚀 <b>OI Scanner v2</b> запущен\n"
            "3 типа сигналов: STANDARD / SURGE / PULLBACK\n"
            f"TP1 +{TP1_PCT}% / TP2 +{TP2_PCT}% / Hard SL −{HARD_SL_PCT}%\n\n"
            f"{auto_status}\n\n"
            "/help"
        )
    except Exception as e:
        log.error(f"startup: {e}")

    asyncio.create_task(periodic_scanner(bot))
    asyncio.create_task(position_monitor_loop(bot))
    asyncio.create_task(daily_report_loop(bot))
    if auto_trader is not None:
        asyncio.create_task(auto_trader.reconcile_loop())
        log.info("Auto-trader reconcile loop started")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
