"""
inflow_scanner_v4_full.py

EvA Bot - Long-only breakout/retest Telegram trading bot (MULTI-SYMBOL).

Data: Binance Futures (volume, klines, CVD proxy, OI)
Execution: Bybit v5 (orders, TP/SL, native trailing stop)
Interface: Telegram via Aiogram 3

Symbol universe: auto-refreshed list of all Binance USDT-M perpetual futures
with 24h quote volume > MIN_24H_VOLUME_USD (default 5,000,000 USDT).

Deploy on Railway:
- Set env vars: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, BYBIT_API_KEY, BYBIT_API_SECRET
- Start command: python inflow_scanner_v4_full.py (see Procfile / railway.json)
"""

import asyncio
import datetime
import logging
import os

import ccxt
import numpy as np
import pandas as pd
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("eva_bot")

# ---------------- CONFIG ----------------
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])
BYBIT_API_KEY = os.environ["BYBIT_API_KEY"]
BYBIT_API_SECRET = os.environ["BYBIT_API_SECRET"]

TIMEFRAME = os.environ.get("TIMEFRAME", "15m")
MARGIN_USD = float(os.environ.get("MARGIN_USD", 50))
LEVERAGE = float(os.environ.get("LEVERAGE", 10))
MAX_DAILY_TRADES = int(os.environ.get("MAX_DAILY_TRADES", 2))
MAX_SIMULTANEOUS_TRADES = int(os.environ.get("MAX_SIMULTANEOUS_TRADES", 1))
TRAIL_CALLBACK_PCT = float(os.environ.get("TRAIL_CALLBACK_PCT", 0.4))
MIN_24H_VOLUME_USD = float(os.environ.get("MIN_24H_VOLUME_USD", 5_000_000))
SYMBOL_REFRESH_MINUTES = int(os.environ.get("SYMBOL_REFRESH_MINUTES", 60))
MAX_SYMBOLS_PER_SCAN = int(os.environ.get("MAX_SYMBOLS_PER_SCAN", 300))
REQUEST_DELAY_SEC = float(os.environ.get("REQUEST_DELAY_SEC", 0.15))

VOL_MA_LEN, VOL_MULT = 20, 2.0
EMA_FAST, EMA_SLOW = 21, 50
RSI_LEN, RSI_MAX = 14, 75
MAX_UPPER_WICK_PCT = 0.30
FIB_LOW, FIB_HIGH = 0.382, 0.5
SL_BUFFER = 0.001
TP1_RR = 1.0
ATR_LEN = 14
MAX_CANDLE_ATR_MULT = float(os.environ.get("MAX_CANDLE_ATR_MULT", 2.5))
POLL_SECONDS = 60  # check every minute, act only on new closed M15 candle

# ---------------- STATE ----------------
class BotState:
    running = True
    trades_today = 0
    last_reset_date = datetime.datetime.now(datetime.UTC).date()
    last_candle_ts = {}          # per-symbol last processed candle ts
    open_position = None         # dict with entry/sl/tp/trailing info
    symbols = []                 # active universe of symbols (Bybit format, e.g. BTCUSDT)
    symbols_last_refresh = None
    trade_history = []           # closed trades log for /history command

state = BotState()

# ---------------- EXCHANGES ----------------
binance = ccxt.binance({"options": {"defaultType": "future"}})
bybit = ccxt.bybit({
    "apiKey": BYBIT_API_KEY,
    "secret": BYBIT_API_SECRET,
    "options": {"defaultType": "future"},
})

# ---------------- TELEGRAM ----------------
bot = Bot(token=TELEGRAM_BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

def main_keyboard():
    label = "⏸ Пауза" if state.running else "▶️ Старт"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=label, callback_data="toggle")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")],
    ])

async def notify(text: str):
    try:
        await bot.send_message(TELEGRAM_CHAT_ID, text, reply_markup=main_keyboard())
    except Exception as e:
        log.error(f"Telegram send failed: {e}")

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "🤖 <b>EvA Bot запущен.</b>\n\n"
        f"📈 Монет в сканере: <b>{len(state.symbols)}</b> (объём 24ч &gt; {MIN_24H_VOLUME_USD:,.0f}$)\n"
        f"⏱ Таймфрейм: {TIMEFRAME}\n"
        f"💰 Маржа: {MARGIN_USD}$ x{int(LEVERAGE)} = <b>{MARGIN_USD*LEVERAGE:.0f}$</b> объём сделки\n"
        f"🎯 TP1: 1:1 (закрытие 50%) → трейлинг {TRAIL_CALLBACK_PCT}%\n"
        f"🔒 Лимиты: {MAX_SIMULTANEOUS_TRADES} позиция одновременно / {MAX_DAILY_TRADES} сделки в день\n\n"
        "Команды:\n"
        "/status — текущий статус и открытая позиция\n"
        "/symbols — список сканируемых монет\n"
        "/history — история последних сделок\n"
        "/pause — поставить на паузу\n"
        "/resume — снять с паузы",
        reply_markup=main_keyboard()
    )

@dp.message(Command("symbols"))
async def cmd_symbols(message: Message):
    if not state.symbols:
        await message.answer("Список монет пока не загружен.")
        return
    chunk = ", ".join(state.symbols[:100])
    await message.answer(f"📋 <b>Сканируется {len(state.symbols)} монет:</b>\n{chunk}...")

@dp.message(Command("status"))
async def cmd_status(message: Message):
    reset_if_new_day()
    pos = state.open_position
    if pos:
        text = format_position_card(pos)
    else:
        text = "Открытых позиций нет. Бот сканирует рынок."
    await message.answer(text, reply_markup=main_keyboard())

@dp.message(Command("history"))
async def cmd_history(message: Message):
    if not state.trade_history:
        await message.answer("История сделок пока пуста.")
        return
    lines = []
    for t in state.trade_history[-10:]:
        emoji = "✅" if t["pnl_usd"] >= 0 else "❌"
        lines.append(
            f"{emoji} {t['symbol']} | вход {t['entry']:.4f} → выход {t['exit']:.4f} | "
            f"P&L: {t['pnl_usd']:+.2f}$ ({t['pnl_pct']:+.2f}%)"
        )
    await message.answer("📜 <b>Последние сделки:</b>\n" + "\n".join(lines))

@dp.message(Command("pause"))
async def cmd_pause(message: Message):
    state.running = False
    await message.answer("⏸ Сканирование поставлено на паузу.", reply_markup=main_keyboard())

@dp.message(Command("resume"))
async def cmd_resume(message: Message):
    state.running = True
    await message.answer("▶️ Сканирование запущено.", reply_markup=main_keyboard())

@dp.callback_query(F.data == "toggle")
async def cb_toggle(callback):
    state.running = not state.running
    status = "▶️ Сканирование запущено" if state.running else "⏸ Сканирование на паузе"
    await callback.message.edit_text(status, reply_markup=main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "stats")
async def cb_stats(callback):
    reset_if_new_day()
    pos = state.open_position
    pos_text = format_position_card(pos) if pos else "нет открытой позиции"
    text = (
        f"📊 <b>Статистика</b>\n"
        f"Монет в сканере: {len(state.symbols)}\n"
        f"Сделок сегодня: {state.trades_today}/{MAX_DAILY_TRADES}\n"
        f"Статус: {'работает' if state.running else 'на паузе'}\n\n"
        f"{pos_text}"
    )
    await callback.answer()
    await callback.message.answer(text, reply_markup=main_keyboard())

def format_position_card(pos):
    if not pos:
        return "Открытых позиций нет."
    risk_usd = abs(pos["entry"] - pos["sl"]) / pos["entry"] * pos["notional"]
    reward_usd = abs(pos["tp1"] - pos["entry"]) / pos["entry"] * pos["notional"]
    trail_status = "🟢 активен" if pos["trailing_active"] else "⚪ не активирован"
    return (
        f"💼 <b>Открытая позиция: {pos['symbol']}</b>\n"
        f"Вход: {pos['entry']:.6f}\n"
        f"Стоп: {pos['sl']:.6f} (риск ~{risk_usd:.2f}$)\n"
        f"TP1: {pos['tp1']:.6f} (потенциал ~{reward_usd:.2f}$)\n"
        f"Объём позиции: {pos['notional']:.0f}$\n"
        f"Трейлинг: {trail_status}"
    )

# ---------------- SYMBOL UNIVERSE (Binance 24h volume > threshold) ----------------
async def refresh_symbol_universe():
    try:
        tickers = binance.fapiPublicGetTicker24hr()
        df = pd.DataFrame(tickers)
        df["quoteVolume"] = df["quoteVolume"].astype(float)
        df = df[df["symbol"].str.endswith("USDT")]
        df = df[df["quoteVolume"] > MIN_24H_VOLUME_USD]
        df = df.sort_values("quoteVolume", ascending=False)
        symbols = df["symbol"].tolist()[:MAX_SYMBOLS_PER_SCAN]

        bybit_markets = bybit.load_markets()
        bybit_symbols = set(bybit_markets.keys())
        filtered = []
        for s in symbols:
            bybit_ccxt_symbol = f"{s[:-4]}/USDT:USDT"
            if bybit_ccxt_symbol in bybit_symbols or s in bybit_symbols:
                filtered.append(s)

        state.symbols = filtered
        state.symbols_last_refresh = datetime.datetime.now(datetime.UTC)
        log.info(f"Symbol universe refreshed: {len(filtered)} symbols (volume > {MIN_24H_VOLUME_USD:,.0f}$)")
        await notify(
            f"🔄 <b>Список монет обновлён.</b>\n"
            f"Активно: {len(filtered)} пар с объёмом 24ч > {MIN_24H_VOLUME_USD:,.0f}$"
        )
    except Exception as e:
        log.error(f"Symbol universe refresh failed: {e}")

def symbols_need_refresh():
    if state.symbols_last_refresh is None:
        return True
    elapsed = (datetime.datetime.now(datetime.UTC) - state.symbols_last_refresh).total_seconds() / 60
    return elapsed >= SYMBOL_REFRESH_MINUTES

# ---------------- DATA FETCH ----------------
def fetch_klines_with_taker(symbol_raw, limit=100):
    resp = binance.fapiPublicGetKlines({
        "symbol": symbol_raw, "interval": TIMEFRAME, "limit": limit
    })
    cols = ["ts","open","high","low","close","volume","close_time","quote_vol",
            "trades","taker_buy_base","taker_buy_quote","ignore"]
    df = pd.DataFrame(resp, columns=cols)
    for c in ["open","high","low","close","volume","taker_buy_base"]:
        df[c] = df[c].astype(float)
    df["ts"] = df["ts"].astype(np.int64)
    df["cvd_delta"] = 2*df["taker_buy_base"] - df["volume"]
    return df

def fetch_oi_recent(symbol_raw, limit=5):
    try:
        resp = binance.fapiDataGetOpenInterestHist({
            "symbol": symbol_raw, "period": TIMEFRAME, "limit": limit
        })
        oi = pd.DataFrame(resp)
        oi["sumOpenInterest"] = oi["sumOpenInterest"].astype(float)
        return oi
    except Exception as e:
        log.warning(f"OI fetch failed for {symbol_raw}: {e}")
        return None

def add_indicators(df):
    df["vol_ma20"] = df["volume"].rolling(VOL_MA_LEN).mean()
    df["ema21"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["ema50"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()
    delta = df["close"].diff()
    gain = delta.clip(lower=0).rolling(RSI_LEN).mean()
    loss = (-delta.clip(upper=0)).rolling(RSI_LEN).mean()
    rs = gain / loss
    df["rsi"] = 100 - 100/(1+rs)

    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs()
    ], axis=1).max(axis=1)
    df["atr14"] = tr.rolling(ATR_LEN).mean()
    return df

# ---------------- SIGNAL LOGIC ----------------
def check_signal(df, oi_df):
    """Returns (signal_dict_or_None, reason, debug_dict).
    debug_dict always contains latest diagnostic values for logging/notifications."""
    c1, c2, c3 = df.iloc[-4], df.iloc[-3], df.iloc[-2]

    debug = {
        "c1_volume": c1["volume"], "vol_ma20": c1["vol_ma20"],
        "vol_ratio": c1["volume"]/c1["vol_ma20"] if c1["vol_ma20"] else 0,
        "ema21": c3["ema21"], "ema50": c3["ema50"], "close": c3["close"],
        "rsi": c3["rsi"], "cvd1": c1["cvd_delta"], "cvd2": c2["cvd_delta"], "cvd3": c3["cvd_delta"],
    }

    if c1["volume"] < VOL_MULT * c1["vol_ma20"]:
        return None, "нет аномалии объёма", debug
    if not (c1["close"]>c1["open"] and c2["close"]>c2["open"] and c3["close"]>c3["open"]):
        return None, "не все свечи зелёные", debug
    if not (c2["close"]>c1["high"] and c3["close"]>c2["high"]):
        return None, "нет последовательного роста хаев", debug
    c3_range = c3["high"]-c3["low"]
    if c3_range>0 and (c3["high"]-c3["close"])/c3_range > MAX_UPPER_WICK_PCT:
        return None, "верхняя тень 3-й свечи слишком длинная", debug

    atr_val = c3["atr14"]
    debug["atr14"] = atr_val
    debug["c3_range"] = c3_range
    debug["c3_atr_ratio"] = c3_range/atr_val if atr_val else None
    if not np.isnan(atr_val) and atr_val > 0 and c3_range > MAX_CANDLE_ATR_MULT * atr_val:
        return None, f"3-я свеча аномально большая ({c3_range/atr_val:.1f}x ATR, лимит {MAX_CANDLE_ATR_MULT}x)", debug
    c1_range = c1["high"]-c1["low"]
    c2_range = c2["high"]-c2["low"]
    if not np.isnan(atr_val) and atr_val > 0:
        if c1_range > MAX_CANDLE_ATR_MULT * atr_val or c2_range > MAX_CANDLE_ATR_MULT * atr_val:
            return None, f"1-я или 2-я свеча аномально большая (>{MAX_CANDLE_ATR_MULT}x ATR)", debug

    if not (c3["close"]>c3["ema21"]>c3["ema50"]):
        return None, "нет тренда (EMA21/50)", debug
    cvds = [c1["cvd_delta"], c2["cvd_delta"], c3["cvd_delta"]]
    if not (cvds[0] < cvds[1] < cvds[2]) or cvds[2] <= 0:
        return None, "CVD не растёт", debug
    local_high = df["high"].iloc[-30:-4].max()
    debug["local_high"] = local_high
    if not (c2["close"]>local_high or c3["close"]>local_high):
        return None, "нет пробоя локального уровня", debug
    if np.isnan(c3["rsi"]) or c3["rsi"] > RSI_MAX:
        return None, f"RSI перегрет ({c3['rsi']:.1f})", debug
    oi_growth_pct = None
    if oi_df is not None and len(oi_df) >= 3:
        oi_vals = oi_df["sumOpenInterest"].iloc[-3:].values
        if not (oi_vals[0] < oi_vals[1] < oi_vals[2]):
            return None, "OI не растёт стабильно", debug
        oi_growth_pct = (oi_vals[-1]-oi_vals[0])/oi_vals[0]*100 if oi_vals[0] else 0
    debug["oi_growth_pct"] = oi_growth_pct

    impulse_low = min(c1["low"], c2["low"], c3["low"])
    impulse_high = c3["high"]
    rng = impulse_high - impulse_low
    entry_high = impulse_high - FIB_LOW*rng
    entry_low = impulse_high - FIB_HIGH*rng
    entry_price = (entry_high+entry_low)/2
    stop_ref = min(c1["low"], c2["low"])
    stop_loss = stop_ref*(1-SL_BUFFER)
    risk = entry_price - stop_loss
    if risk <= 0:
        return None, "некорректный риск", debug
    tp1 = entry_price + risk*TP1_RR

    signal = {
        "entry_low": entry_low, "entry_high": entry_high, "entry_price": entry_price,
        "stop_loss": stop_loss, "tp1": tp1, "risk_pct": risk/entry_price*100,
        "impulse_low": impulse_low, "impulse_high": impulse_high,
        "c1_volume": c1["volume"], "vol_ma20": c1["vol_ma20"],
        "vol_ratio": debug["vol_ratio"], "rsi": c3["rsi"],
        "ema21": c3["ema21"], "ema50": c3["ema50"],
        "oi_growth_pct": oi_growth_pct, "local_high": local_high,
        "atr14": atr_val, "c3_atr_ratio": debug["c3_atr_ratio"],
    }
    return signal, "OK", debug

# ---------------- RISK / DAILY CONTROL ----------------
def reset_if_new_day():
    today = datetime.datetime.now(datetime.UTC).date()
    if today > state.last_reset_date:
        state.trades_today = 0
        state.last_reset_date = today

def trading_allowed():
    reset_if_new_day()
    if state.trades_today >= MAX_DAILY_TRADES:
        return False, "дневной лимит сделок исчерпан"
    if state.open_position is not None:
        return False, "позиция уже открыта"
    return True, "OK"

# ---------------- EXECUTION (Bybit v5) ----------------
def position_size(entry_price):
    notional = MARGIN_USD * LEVERAGE
    qty = notional / entry_price
    return round(qty, 4), notional

def build_signal_card(symbol_bybit, signal, qty, notional):
    """Builds a detailed Telegram card explaining WHY the bot entered, with all numbers."""
    risk_pct = signal["risk_pct"]
    risk_usd = risk_pct/100 * notional
    reward_usd = risk_usd * TP1_RR  # TP1 at 1:1
    tp2_estimate_usd = risk_usd * 3  # per strategy: final target ~1:3 after trailing
    vol_ratio = signal["vol_ratio"]
    oi_txt = f"{signal['oi_growth_pct']:+.2f}% за 3 свечи" if signal["oi_growth_pct"] is not None else "н/д (фильтр отключен)"

    card = (
        f"🚀 <b>СИГНАЛ НА ВХОД: {symbol_bybit}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Почему бот входит:</b>\n"
        f"📊 Объём аномалии: {signal['c1_volume']:.0f} (в {vol_ratio:.1f}x больше средней за 20 свечей)\n"
        f"📈 Тренд: цена {signal['ema21']:.6f} EMA21 &gt; {signal['ema50']:.6f} EMA50 ✅\n"
        f"🔥 Открытый интерес: {oi_txt}\n"
        f"🎯 RSI(14): {signal['rsi']:.1f} (не перегрет, лимит {RSI_MAX})\n"
        f"⛰ Пробит локальный уровень: {signal['local_high']:.6f}\n"
        f"📏 Размер 3-й свечи: {signal['c3_atr_ratio']:.1f}x ATR14 (лимит {MAX_CANDLE_ATR_MULT}x, ретест безопасен) ✅\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Параметры сделки:</b>\n"
        f"💵 Цена входа (ретест): <b>{signal['entry_price']:.6f}</b>\n"
        f"   Зона лимитки: {signal['entry_low']:.6f} — {signal['entry_high']:.6f}\n"
        f"🛑 Стоп-лосс: <b>{signal['stop_loss']:.6f}</b> (-{risk_pct:.2f}%)\n"
        f"🎯 Take Profit 1 (50% позиции, 1:1): <b>{signal['tp1']:.6f}</b> (+{risk_pct*TP1_RR:.2f}%)\n"
        f"📐 После TP1 включится трейлинг {TRAIL_CALLBACK_PCT}% на остаток позиции\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Деньги:</b>\n"
        f"💰 Маржа: {MARGIN_USD}$ x{int(LEVERAGE)} плечо\n"
        f"📦 Объём позиции: <b>{notional:.0f}$</b> ({qty:.4f} монет)\n"
        f"❌ Риск при срабатывании стопа: <b>-{risk_usd:.2f}$</b>\n"
        f"✅ Прибыль на TP1 (50% позиции): <b>+{reward_usd/2:.2f}$</b>\n"
        f"✅ Потенциал при движении до 1:3 (трейлинг): <b>+{tp2_estimate_usd:.2f}$</b> (ориентировочно)\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 Сделок сегодня: {state.trades_today+1}/{MAX_DAILY_TRADES}"
    )
    return card

async def place_entry(symbol_bybit, signal):
    qty, notional = position_size(signal["entry_price"])
    try:
        order = bybit.create_order(
            symbol=symbol_bybit, type="limit", side="buy",
            amount=qty, price=signal["entry_price"],
            params={"timeInForce": "GTC", "positionIdx": 0}
        )
        bybit.private_post_v5_position_trading_stop({
            "category": "linear", "symbol": symbol_bybit,
            "takeProfit": str(round(signal["tp1"], 6)),
            "stopLoss": str(round(signal["stop_loss"], 6)),
            "tpslMode": "Partial",
            "tpSize": str(round(qty*0.5, 4)),
            "slSize": str(round(qty, 4)),
            "positionIdx": 0
        })
        state.open_position = {
            "symbol": symbol_bybit, "entry": signal["entry_price"],
            "sl": signal["stop_loss"], "tp1": signal["tp1"], "qty": qty,
            "notional": notional, "trailing_active": False, "order_id": order.get("id"),
            "opened_at": datetime.datetime.now(datetime.UTC),
        }
        state.trades_today += 1
        card = build_signal_card(symbol_bybit, signal, qty, notional)
        await notify(card)
    except Exception as e:
        log.error(f"Order placement failed for {symbol_bybit}: {e}")
        await notify(f"⚠️ <b>Ошибка при выставлении ордера ({symbol_bybit}):</b>\n{e}")

async def activate_trailing():
    pos = state.open_position
    if pos is None or pos["trailing_active"]:
        return
    try:
        bybit.private_post_v5_position_trading_stop({
            "category": "linear", "symbol": pos["symbol"],
            "trailingStop": str(TRAIL_CALLBACK_PCT),
            "activePrice": str(round(pos["tp1"], 6)),
            "positionIdx": 0
        })
        pos["trailing_active"] = True
        risk_usd = abs(pos["entry"]-pos["sl"])/pos["entry"]*pos["notional"]
        await notify(
            f"✅ <b>TP1 достигнут: {pos['symbol']}</b>\n"
            f"Зафиксирована прибыль: <b>+{risk_usd/2:.2f}$</b> (закрыто 50% позиции)\n"
            f"Трейлинг-стоп активирован (откат {TRAIL_CALLBACK_PCT}%) на остаток."
        )
    except Exception as e:
        log.error(f"Trailing activation failed: {e}")
        await notify(f"⚠️ <b>Ошибка активации трейлинга:</b>\n{e}")

async def check_position_status():
    if state.open_position is None:
        return
    symbol = state.open_position["symbol"]
    pos_state = state.open_position
    try:
        positions = bybit.fetch_positions([symbol])
        active = [p for p in positions if float(p.get("contracts", 0) or 0) > 0]
        if not active:
            try:
                ticker = bybit.fetch_ticker(symbol)
                exit_price = ticker["last"]
            except Exception:
                exit_price = pos_state["entry"]
            pnl_pct = (exit_price - pos_state["entry"]) / pos_state["entry"] * 100
            pnl_usd = pnl_pct/100 * pos_state["notional"]
            state.trade_history.append({
                "symbol": symbol, "entry": pos_state["entry"], "exit": exit_price,
                "pnl_pct": pnl_pct, "pnl_usd": pnl_usd,
                "closed_at": datetime.datetime.now(datetime.UTC),
            })
            emoji = "✅" if pnl_usd >= 0 else "❌"
            await notify(
                f"🏁 <b>Позиция закрыта: {symbol}</b>\n"
                f"Вход: {pos_state['entry']:.6f} → Выход: {exit_price:.6f}\n"
                f"{emoji} Итоговый результат: <b>{pnl_usd:+.2f}$</b> ({pnl_pct:+.2f}%)\n"
                f"Слот свободен для новых сигналов."
            )
            state.open_position = None
            return
        pos = active[0]
        current_size = float(pos.get("contracts", 0) or 0)
        original_qty = pos_state["qty"]
        if current_size <= original_qty * 0.55 and not pos_state["trailing_active"]:
            await activate_trailing()
    except Exception as e:
        log.error(f"Position check failed: {e}")

# ---------------- MAIN SCAN LOOP (multi-symbol) ----------------
async def scan_symbol(symbol_raw):
    try:
        df = fetch_klines_with_taker(symbol_raw, 100)
        if len(df) < 40:
            return
        latest_closed_ts = int(df.iloc[-2]["ts"])
        if state.last_candle_ts.get(symbol_raw) == latest_closed_ts:
            return
        state.last_candle_ts[symbol_raw] = latest_closed_ts

        df = add_indicators(df)
        allowed, reason = trading_allowed()
        if not allowed:
            return

        oi_df = fetch_oi_recent(symbol_raw, 5)
        signal, reason, debug = check_signal(df, oi_df)
        if signal is None:
            log.info(f"{symbol_raw}: no signal ({reason})")
            return

        await place_entry(symbol_raw, signal)
    except Exception as e:
        log.warning(f"scan_symbol error {symbol_raw}: {e}")

async def scan_loop():
    while True:
        try:
            if not state.running:
                await asyncio.sleep(POLL_SECONDS)
                continue

            if symbols_need_refresh():
                await refresh_symbol_universe()

            await check_position_status()

            allowed, _ = trading_allowed()
            if not allowed or not state.symbols:
                await asyncio.sleep(POLL_SECONDS)
                continue

            for symbol_raw in state.symbols:
                if state.open_position is not None:
                    break
                await scan_symbol(symbol_raw)
                await asyncio.sleep(REQUEST_DELAY_SEC)

        except Exception as e:
            log.error(f"Scan loop error: {e}")
            await notify(f"⚠️ <b>Ошибка в основном цикле:</b>\n{e}")

        await asyncio.sleep(POLL_SECONDS)

async def main():
    await refresh_symbol_universe()
    asyncio.create_task(scan_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
