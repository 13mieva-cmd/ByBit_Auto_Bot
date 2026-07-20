#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EVA v4 — EMA CROSSOVER STRATEGY (полная замена импульсной стратегии v3)
=========================================================================
ДАННЫЕ: Binance Futures (объёмы, taker buy volume -> CVD как доп. инфо)
ЦЕНЫ/ТОРГОВЛЯ: Bybit (сверка цены + исполнение, символ должен быть на Bybit)
ИСПОЛНЕНИЕ: PAPER по умолчанию (AUTO_TRADE=0), реальные ордера Bybit demo/live при AUTO_TRADE=1

СТРАТЕГИЯ (по спеке пользователя — "Улучшенный EMA Crossover Strategy"):
Тренд-фолловинг на 15M/1H. Поддерживает LONG и SHORT (старая версия была только LONG).

1. EMA9 x EMA21 crossover на сигнальной (последней закрытой) свече.
2. Тренд-фильтр: цена > SMA200 для Long, цена < SMA200 для Short.
3. Флэт-фильтр: кроссовер не считается валидным, если случился слишком близко к SMA200
   (по умолчанию < 0.5*ATR — типичный шум боковика).
4. RSI(14): > 45 для Long, < 55 для Short.
5. ADX(14) > 20 и растёт — ОПЦИОНАЛЬНО (ADX_REQUIRED=0 по умолчанию, как в спеке "опционально").
6. Подтверждение свечой: close в сторону сигнала (бычья для Long, медвежья для Short).
7. Объёмный фильтр: объём сигнальной свечи >= среднего (SMA20) * VOL_CONFIRM_MULT.
8. Риск-менеджмент: SL = 1.5*ATR, TP1 = 2*ATR (закрыть 50%, RR 1:2), остаток — трейлинг
   1.5*ATR от пика (без фиксированного TP2 — "остаток trailing stop" по спеке).
9. До MAX_CONCURRENT одновременных позиций, дневной лимит убытка через /pause (ручной триггер).

ДЕПЛОЙ: Railway, переменные окружения — те же, что в v3 (см. ниже блок КОНФИГ).
Start Command: python <этот файл>.py
"""

import os, time, json, csv, math, threading, asyncio
import aiohttp

state_lock = threading.RLock()
import datetime as dt
import urllib.request, urllib.parse, urllib.error, hmac, hashlib

# ============================== КОНФИГ ==============================
TG_TOKEN = os.environ.get("TG_TOKEN", "")
DATA_DIR = os.environ.get("DATA_DIR", "/data")
DEPOSIT = float(os.environ.get("DEPOSIT_USD", 500))
MARGIN = float(os.environ.get("MARGIN_USD", 50))
LEVERAGE = float(os.environ.get("LEVERAGE", 10))
NOTIONAL = MARGIN * LEVERAGE

MAX_CONCURRENT = int(os.environ.get("MAX_OPEN_POSITIONS", os.environ.get("MAX_CONCURRENT", 2)))  # спека: 1-2 позиции
MAX_OPEN_POSITIONS = MAX_CONCURRENT
MAX_DAILY_TRADES = int(os.environ.get("MAX_DAILY_TRADES", 0))
DAILY_LOSS_LIMIT_PCT = float(os.environ.get("DAILY_LOSS_LIMIT_PCT", "6.0"))  # спека: пауза при убытке >5-7%/день

# --- таймфрейм ---
TF = os.environ.get("TF", "15m")
VOL_MA_LEN = 20
ATR_LEN = 14
RSI_LEN = 14

# ================= EMA CROSSOVER STRATEGY v2 (по спеке пользователя) =================
EMA_CROSS_FAST = int(os.environ.get("EMA_CROSS_FAST", "9"))
EMA_CROSS_SLOW = int(os.environ.get("EMA_CROSS_SLOW", "21"))
TREND_SMA_LEN = int(os.environ.get("TREND_SMA_LEN", "200"))          # SMA200 тренд-фильтр
RSI_LONG_MIN = float(os.environ.get("RSI_LONG_MIN", "45"))           # RSI > 45 для Long
RSI_SHORT_MAX = float(os.environ.get("RSI_SHORT_MAX", "55"))         # RSI < 55 для Short
ADX_LEN = int(os.environ.get("ADX_LEN", "14"))
ADX_MIN = float(os.environ.get("ADX_MIN", "20"))                     # только в сильном тренде
ADX_REQUIRED = int(os.environ.get("ADX_REQUIRED", "0"))              # 0 = опционально (по спеке)
VOL_CONFIRM_MULT = float(os.environ.get("VOL_CONFIRM_MULT", "1.0"))  # объём >= среднего
FLAT_ZONE_ATR = float(os.environ.get("FLAT_ZONE_ATR", "0.5"))        # защита от флэта у SMA200
CANDLE_CONFIRM_REQUIRED = int(os.environ.get("CANDLE_CONFIRM_REQUIRED", "1"))

# --- ATR-риск-менеджмент (по спеке: SL 1-1.5xATR, TP RR 1:2+, остаток trailing) ---
ATR_SL_MULT_EMA = float(os.environ.get("ATR_SL_MULT_EMA", "1.5"))
ATR_TP1_MULT_EMA = float(os.environ.get("ATR_TP1_MULT_EMA", "2.0"))
ATR_TRAIL_MULT_EMA = float(os.environ.get("ATR_TRAIL_MULT_EMA", "1.5"))
FEE_MAKER = 0.0002
FEE_TAKER = 0.00055

# --- вселенная (глобальный скан ликвидных пар) ---
MAX_COINS = int(os.environ.get("MAX_COINS", "500"))
MIN_QUOTE_VOL24 = float(os.environ.get("MIN_QUOTE_VOL24", "3000000"))
SCAN_EVERY_SEC = 5
MANAGE_EVERY_SEC = 5

# --- файлы (на volume) ---
def ensure_dirs():
    try: os.makedirs(DATA_DIR, exist_ok=True)
    except Exception: pass
ensure_dirs()
STATE_FILE = os.path.join(DATA_DIR, "v4_state.json")
TRADES_FILE = os.path.join(DATA_DIR, "v4_trades.csv")
SIGNALS_FILE = os.path.join(DATA_DIR, "v4_signals.csv")
CHAT_FILE = os.path.join(DATA_DIR, "v4_chat.txt")

BINANCE = "https://fapi.binance.com"
BYBIT_LIVE = "https://api.bybit.com"
BYBIT_DEMO = "https://api-demo.bybit.com"

BYBIT_API_KEY = os.environ.get("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", "")
BYBIT_USE_DEMO = os.environ.get("BYBIT_USE_DEMO", "1") == "1"
AUTO_TRADE = os.environ.get("AUTO_TRADE", "0") == "1"
BYBIT = BYBIT_DEMO if BYBIT_USE_DEMO else BYBIT_LIVE
CATEGORY = "linear"

# ============================== HTTP/TG ==============================
def http_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "eva-v4"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())

def tg(method, _timeout=35, **kw):
    if not TG_TOKEN: return None
    try:
        data = urllib.parse.urlencode(kw).encode()
        req = urllib.request.Request(f"https://api.telegram.org/bot{TG_TOKEN}/{method}", data=data)
        with urllib.request.urlopen(req, timeout=_timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print("tg err:", e); return None

def tg_send(chat, text):
    if not chat: return
    tg("sendMessage", chat_id=chat, text=text, parse_mode="HTML", disable_web_page_preview=True)

def load_chat():
    try:
        with open(CHAT_FILE) as f: return f.read().strip()
    except Exception: return None

def save_chat(cid):
    try:
        with open(CHAT_FILE, "w") as f: f.write(str(cid))
    except Exception: pass

# ============================== ДАННЫЕ ==============================
_uni_cache = {"ts": 0, "coins": []}
_RATE_SEM = asyncio.Semaphore(20)

async def _fetch_json_async(session, url):
    async with _RATE_SEM:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                return await r.json()
        except Exception as e:
            print("async fetch err:", url, e)
            return None

async def build_universe_async():
    async with aiohttp.ClientSession() as session:
        tick_task = _fetch_json_async(session, f"{BINANCE}/fapi/v1/ticker/24hr")
        bybit_task = _fetch_json_async(session, f"{BYBIT}/v5/market/tickers?category=linear")
        tick, by = await asyncio.gather(tick_task, bybit_task)
    if not tick or not by:
        return _uni_cache["coins"]
    binance = {}
    for t in tick:
        s = t.get("symbol", "")
        if not s.endswith("USDT"): continue
        qv = float(t.get("quoteVolume", 0) or 0)
        if qv < MIN_QUOTE_VOL24: continue
        binance[s] = qv
    bybit_syms = set()
    for x in by.get("result", {}).get("list", []):
        bybit_syms.add(x["symbol"])
    coins = [s for s in sorted(binance, key=lambda k: -binance[k]) if s in bybit_syms][:MAX_COINS]
    _uni_cache["coins"] = coins; _uni_cache["ts"] = time.time()
    print(f"Universe updated: {len(coins)} coins (liquidity >= {MIN_QUOTE_VOL24:,.0f}$)")
    return coins

def universe():
    if time.time() - _uni_cache["ts"] < 3600 and _uni_cache["coins"]:
        return _uni_cache["coins"]
    try:
        return asyncio.run(build_universe_async())
    except Exception as e:
        print("universe err:", e)
        return _uni_cache["coins"]

async def fetch_klines_batch(symbols, need_bars):
    async with aiohttp.ClientSession() as session:
        async def one(sym):
            k_url = f"{BINANCE}/fapi/v1/klines?symbol={sym}&interval={TF}&limit={need_bars}"
            k_data = await _fetch_json_async(session, k_url)
            if not k_data or len(k_data) < need_bars - 5:
                return sym, None
            o = [float(x[1]) for x in k_data]; h = [float(x[2]) for x in k_data]
            l = [float(x[3]) for x in k_data]; c = [float(x[4]) for x in k_data]
            v = [float(x[5]) for x in k_data]; ct = [int(x[0]) for x in k_data]
            return sym, (o, h, l, c, v, ct)
        results = await asyncio.gather(*[one(s) for s in symbols], return_exceptions=False)
    return {sym: data for sym, data in results if data is not None}

def bybit_price(symbol):
    try:
        d = http_json(f"{BYBIT}/v5/market/tickers?category=linear&symbol={symbol}")
        return float(d["result"]["list"][0]["lastPrice"])
    except Exception:
        return None

# ============================== BYBIT АВТО-ТОРГОВЛЯ (v5 API) ==============================
def _bybit_signed(method, path, body=None, params=None):
    if not BYBIT_API_KEY or not BYBIT_API_SECRET:
        return {"retCode": -1, "retMsg": "no api keys"}
    ts = str(int(time.time() * 1000))
    recv = "5000"
    body_str = json.dumps(body, separators=(",", ":")) if body else ""
    qs = urllib.parse.urlencode(params) if params else ""
    prehash = ts + BYBIT_API_KEY + recv + (qs if method == "GET" else body_str)
    sign = hmac.new(BYBIT_API_SECRET.encode(), prehash.encode(), hashlib.sha256).hexdigest()
    headers = {
        "X-BAPI-API-KEY": BYBIT_API_KEY, "X-BAPI-SIGN": sign, "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv, "Content-Type": "application/json",
    }
    url = f"{BYBIT}{path}" + (f"?{qs}" if qs and method == "GET" else "")
    req = urllib.request.Request(url, data=body_str.encode() if method != "GET" else None,
                                  headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try: return json.loads(e.read().decode())
        except Exception: return {"retCode": -1, "retMsg": str(e)}
    except Exception as e:
        return {"retCode": -1, "retMsg": str(e)}

_instr_cache = {}
def bybit_instrument_info(symbol):
    if symbol in _instr_cache: return _instr_cache[symbol]
    try:
        d = http_json(f"{BYBIT}/v5/market/instruments-info?category={CATEGORY}&symbol={symbol}")
        info = d["result"]["list"][0]
        _instr_cache[symbol] = info
        return info
    except Exception:
        return None

def _round_step(value, step):
    if step <= 0: return value
    return math.floor(value / step) * step

def bybit_round_price(symbol, price):
    info = bybit_instrument_info(symbol)
    if not info: return round(price, 6)
    tick = float(info["priceFilter"]["tickSize"])
    dec = max(0, len(str(tick).split(".")[-1])) if "." in str(tick) else 0
    return round(_round_step(price, tick), dec)

def bybit_round_qty(symbol, qty):
    info = bybit_instrument_info(symbol)
    if not info: return round(qty, 3)
    step = float(info["lotSizeFilter"]["qtyStep"])
    dec = max(0, len(str(step).split(".")[-1])) if "." in str(step) else 0
    return round(_round_step(qty, step), dec)

def bybit_set_leverage(symbol, leverage):
    return _bybit_signed("POST", "/v5/position/set-leverage", body={
        "category": CATEGORY, "symbol": symbol,
        "buyLeverage": str(leverage), "sellLeverage": str(leverage),
    })

def bybit_market_long(symbol, qty):
    qty_r = bybit_round_qty(symbol, qty)
    return _bybit_signed("POST", "/v5/order/create", body={
        "category": CATEGORY, "symbol": symbol, "side": "Buy",
        "orderType": "Market", "qty": str(qty_r), "timeInForce": "IOC",
    })

def bybit_market_short(symbol, qty):
    """SHORT-вход — добавлено для EMA-crossover стратегии v2 (старая версия была только LONG)."""
    qty_r = bybit_round_qty(symbol, qty)
    return _bybit_signed("POST", "/v5/order/create", body={
        "category": CATEGORY, "symbol": symbol, "side": "Sell",
        "orderType": "Market", "qty": str(qty_r), "timeInForce": "IOC",
    })

def bybit_cancel_order(symbol, order_id):
    return _bybit_signed("POST", "/v5/order/cancel", body={
        "category": CATEGORY, "symbol": symbol, "orderId": order_id,
    })

def bybit_set_stop(symbol, sl_price=None, tp_price=None):
    body = {"category": CATEGORY, "symbol": symbol, "positionIdx": 0}
    if sl_price is not None: body["stopLoss"] = str(bybit_round_price(symbol, sl_price))
    if tp_price is not None: body["takeProfit"] = str(bybit_round_price(symbol, tp_price))
    return _bybit_signed("POST", "/v5/position/trading-stop", body=body)

def bybit_reduce_limit(symbol, qty, price, side="long"):
    """side-aware: для long закрываем Sell-лимиткой, для short — Buy-лимиткой."""
    qty_r = bybit_round_qty(symbol, qty)
    price_r = bybit_round_price(symbol, price)
    close_side = "Sell" if side == "long" else "Buy"
    return _bybit_signed("POST", "/v5/order/create", body={
        "category": CATEGORY, "symbol": symbol, "side": close_side,
        "orderType": "Limit", "qty": str(qty_r), "price": str(price_r),
        "timeInForce": "GTC", "reduceOnly": True,
    })

def bybit_close_market(symbol, qty, side="long"):
    """side-aware: для long закрываем Sell-маркетом, для short — Buy-маркетом."""
    qty_r = bybit_round_qty(symbol, qty)
    close_side = "Sell" if side == "long" else "Buy"
    return _bybit_signed("POST", "/v5/order/create", body={
        "category": CATEGORY, "symbol": symbol, "side": close_side,
        "orderType": "Market", "qty": str(qty_r), "timeInForce": "IOC", "reduceOnly": True,
    })

def bybit_cancel_all(symbol):
    return _bybit_signed("POST", "/v5/order/cancel-all", body={"category": CATEGORY, "symbol": symbol})

def bybit_wallet_balance():
    d = _bybit_signed("GET", "/v5/account/wallet-balance", params={"accountType": "UNIFIED"})
    try:
        return float(d["result"]["list"][0]["totalEquity"])
    except Exception:
        return None

# ============================== ИНДИКАТОРЫ ==============================
def ema_series(v, span):
    if not v: return []
    a = 2 / (span + 1); out = [v[0]]
    for x in v[1:]: out.append(a * x + (1 - a) * out[-1])
    return out

def sma_series(v, period):
    if not v: return []
    if len(v) < period: return [sum(v) / len(v)] * len(v)
    out = []
    for i in range(len(v)):
        if i < period - 1:
            out.append(sum(v[:i + 1]) / (i + 1))
        else:
            out.append(sum(v[i - period + 1:i + 1]) / period)
    return out

def rsi(closes, period=14):
    if len(closes) < period + 1: return 50.0
    g = l = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        if d >= 0: g += d
        else: l -= d
    ag, al = g / period, l / period
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (period - 1) + max(d, 0)) / period
        al = (al * (period - 1) + max(-d, 0)) / period
    if al == 0: return 100.0
    return 100 - 100 / (1 + ag / al)

def atr(h, l, c, period=14):
    n = len(c)
    if n < period + 2: return 0.0
    trs = []
    for i in range(1, n):
        trs.append(max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])))
    a = sum(trs[:period]) / period
    for x in trs[period:]:
        a = (a * (period - 1) + x) / period
    return a

def adx_series(h, l, c, period=14):
    """Wilder ADX. Возвращает список той же длины, что h/l/c."""
    n = len(c)
    if n < period * 2 + 2:
        return [0.0] * n
    plus_dm = [0.0] * n; minus_dm = [0.0] * n; trs = [0.0] * n
    for i in range(1, n):
        up = h[i] - h[i - 1]; down = l[i - 1] - l[i]
        plus_dm[i] = up if (up > down and up > 0) else 0.0
        minus_dm[i] = down if (down > up and down > 0) else 0.0
        trs[i] = max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1]))

    def wilder_smooth(vals):
        out = [0.0] * n
        s = sum(vals[1:period + 1])
        out[period] = s
        for i in range(period + 1, n):
            s = out[i - 1] - out[i - 1] / period + vals[i]
            out[i] = s
        return out

    tr_sm = wilder_smooth(trs); plus_sm = wilder_smooth(plus_dm); minus_sm = wilder_smooth(minus_dm)
    dx = [0.0] * n
    for i in range(period, n):
        if tr_sm[i] <= 0: continue
        pdi = 100 * plus_sm[i] / tr_sm[i]; mdi = 100 * minus_sm[i] / tr_sm[i]
        if (pdi + mdi) > 0:
            dx[i] = 100 * abs(pdi - mdi) / (pdi + mdi)
    adx = [0.0] * n
    start = period * 2
    if start >= n: return adx
    adx[start] = sum(dx[period:start + 1]) / (start + 1 - period)
    for i in range(start + 1, n):
        adx[i] = (adx[i - 1] * (period - 1) + dx[i]) / period
    return adx

# ============================== СИГНАЛ: EMA9/21 CROSSOVER ==============================
def detect_signal(o, h, l, c, v, ct):
    """EMA9 x EMA21 crossover + SMA200 тренд + RSI + ADX(опц.) + объём + подтверждение свечой.
    Возвращает (ok, dict|причина). dict содержит side='long'|'short'."""
    n = len(c)
    min_hist = max(TREND_SMA_LEN, ADX_LEN * 3, VOL_MA_LEN) + 5
    if n < min_hist:
        return False, "мало истории"
    i1 = n - 1

    ema_fast = ema_series(c, EMA_CROSS_FAST)
    ema_slow = ema_series(c, EMA_CROSS_SLOW)
    sma_trend = sma_series(c, TREND_SMA_LEN)
    a = atr(h[:i1 + 1], l[:i1 + 1], c[:i1 + 1], ATR_LEN)
    if a <= 0:
        return False, "нет ATR для риск-менеджмента"

    cross_up = ema_fast[i1 - 1] <= ema_slow[i1 - 1] and ema_fast[i1] > ema_slow[i1]
    cross_down = ema_fast[i1 - 1] >= ema_slow[i1 - 1] and ema_fast[i1] < ema_slow[i1]
    if not (cross_up or cross_down):
        return False, "нет кроссовера EMA9/21"
    side = "long" if cross_up else "short"

    trend_ok = (c[i1] > sma_trend[i1]) if side == "long" else (c[i1] < sma_trend[i1])
    if not trend_ok:
        return False, f"против SMA{TREND_SMA_LEN} тренда"

    dist_to_sma = abs(c[i1] - sma_trend[i1])
    if dist_to_sma < FLAT_ZONE_ATR * a:
        return False, f"кроссовер близко к SMA{TREND_SMA_LEN} (флэт, {dist_to_sma/a:.2f}x<{FLAT_ZONE_ATR}x ATR)"

    r = rsi(c[-(RSI_LEN * 6):], RSI_LEN)
    if side == "long" and not (r > RSI_LONG_MIN):
        return False, f"RSI {r:.0f} <= {RSI_LONG_MIN} (слабо для Long)"
    if side == "short" and not (r < RSI_SHORT_MAX):
        return False, f"RSI {r:.0f} >= {RSI_SHORT_MAX} (слабо для Short)"

    adx_vals = adx_series(h[:i1 + 1], l[:i1 + 1], c[:i1 + 1], ADX_LEN)
    adx_now = adx_vals[i1] if i1 < len(adx_vals) else 0.0
    adx_rising = i1 > 0 and adx_now > adx_vals[i1 - 1]
    adx_ok = adx_now > ADX_MIN and adx_rising
    if ADX_REQUIRED and not adx_ok:
        return False, f"ADX {adx_now:.0f} слабый/падает (нужно >{ADX_MIN} и растёт)"

    candle_confirm = (c[i1] > o[i1]) if side == "long" else (c[i1] < o[i1])
    if CANDLE_CONFIRM_REQUIRED and not candle_confirm:
        return False, "нет подтверждения свечой"

    if len(v) < VOL_MA_LEN + 1:
        return False, "мало объёмной базы"
    vma = sum(v[i1 - VOL_MA_LEN:i1]) / VOL_MA_LEN
    vol_ratio = (v[i1] / vma) if vma > 0 else 0.0
    if not (vma > 0 and vol_ratio >= VOL_CONFIRM_MULT):
        return False, f"объём слабее среднего (x{vol_ratio:.1f}<{VOL_CONFIRM_MULT}x)"

    entry = c[i1]
    if side == "long":
        sl = entry - ATR_SL_MULT_EMA * a
        tp1 = entry + ATR_TP1_MULT_EMA * a
        risk_per_unit = entry - sl
    else:
        sl = entry + ATR_SL_MULT_EMA * a
        tp1 = entry - ATR_TP1_MULT_EMA * a
        risk_per_unit = sl - entry
    if risk_per_unit <= 0:
        return False, "риск на единицу <= 0"
    risk_pct = risk_per_unit / entry

    return True, dict(
        side=side, entry=entry, sl=sl, tp1=tp1, atr=a, risk_pct=risk_pct,
        rsi=r, adx=adx_now, adx_ok=adx_ok, ema_fast=ema_fast[i1], ema_slow=ema_slow[i1],
        sma_trend=sma_trend[i1], vol_ratio=vol_ratio, close3=c[i1],
    )

# ============================== СОСТОЯНИЕ/ЛИМИТЫ ==============================
def _default_state():
    return dict(day=str(dt.datetime.now(dt.timezone.utc).date()),
                trades_today=0, paused=False, day_start_equity=DEPOSIT,
                pendings={}, positions={})

def load_state():
    try:
        with open(STATE_FILE) as f: st = json.load(f)
    except Exception: return _default_state()
    if "pendings" not in st: st["pendings"] = {}
    if "positions" not in st: st["positions"] = {}
    if "day_start_equity" not in st: st["day_start_equity"] = DEPOSIT
    return st

def save_state(st):
    try:
        with state_lock:
            with open(STATE_FILE, "w") as f: json.dump(st, f)
    except Exception as e: print("state save err:", e)

def utc_day():
    return str(dt.datetime.now(dt.timezone.utc).date())

def roll_day(st, chat=None):
    d = utc_day()
    if d != st.get("day"):
        st["day"] = d; st["trades_today"] = 0; st["day_start_equity"] = DEPOSIT
        save_state(st)
        if chat: tg_send(chat, f"\U0001F305 Новый день (UTC) — счётчик сделок обнулён ({daily_txt(st)}).")

def slots_used(st):
    return len(st.get("pendings", {})) + len(st.get("positions", {}))

def engaged_syms(st):
    return set(st.get("pendings", {})) | set(st.get("positions", {}))

def daily_txt(st):
    n = st.get("trades_today", 0)
    return f"{n}/{MAX_DAILY_TRADES}" if MAX_DAILY_TRADES > 0 else f"{n} (дневного лимита сделок нет)"

def open_risk_usd(st):
    total = 0.0
    for pos in st.get("positions", {}).values():
        if not pos.get("half_closed"):
            total += abs(pos["entry"] - pos["sl"]) * pos["qty"]
    return total

def today_pnl(st):
    if not os.path.exists(TRADES_FILE): return 0.0
    day = st.get("day")
    total = 0.0
    try:
        for r in csv.DictReader(open(TRADES_FILE)):
            if r["ts_open"][:10] == day or r["ts_close"][:10] == day:
                total += float(r["pnl_usd"])
    except Exception:
        pass
    return total

def trading_allowed(st):
    if st.get("paused"): return False, "пауза"
    if MAX_DAILY_TRADES > 0 and st.get("trades_today", 0) >= MAX_DAILY_TRADES:
        return False, f"дневной лимит {MAX_DAILY_TRADES} исчерпан"
    if slots_used(st) >= MAX_CONCURRENT:
        return False, f"заняты все слоты ({MAX_CONCURRENT}/{MAX_CONCURRENT})"
    day_pnl = today_pnl(st)
    if day_pnl < 0 and abs(day_pnl) / DEPOSIT * 100 >= DAILY_LOSS_LIMIT_PCT:
        return False, f"дневной убыток {abs(day_pnl)/DEPOSIT*100:.1f}% >= лимита {DAILY_LOSS_LIMIT_PCT}% (пауза по спеке)"
    return True, ""

# ============================== ЖУРНАЛЫ ==============================
def log_signal(coin, price, side):
    try:
        new = not os.path.exists(SIGNALS_FILE)
        with open(SIGNALS_FILE, "a", newline="") as f:
            w = csv.writer(f)
            if new: w.writerow(["ts", "coin", "side", "price"])
            w.writerow([dt.datetime.now().isoformat(timespec="seconds"), coin, side, price])
    except Exception as e: print("log_signal err:", e)

def log_trade(row):
    try:
        new = not os.path.exists(TRADES_FILE)
        with open(TRADES_FILE, "a", newline="") as f:
            w = csv.writer(f)
            if new: w.writerow(["ts_open", "ts_close", "coin", "side", "entry", "exit", "qty", "part", "pnl_usd", "r_mult", "reason"])
            w.writerow(row)
    except Exception as e: print("log_trade err:", e)

# ============================== PAPER/LIVE ДВИЖОК ==============================
def _profit_scenarios_ema(entry, sl, tp1, qty, side="long"):
    """TP1 закрывает 50% (RR 1:2), остаток идёт в трейлинг (без фикс. TP2 — по спеке)."""
    fee_in = NOTIONAL * FEE_MAKER
    sign = 1 if side == "long" else -1
    def leg(exit_px, q, fee_share):
        return sign * (exit_px - entry) * q - exit_px * q * FEE_TAKER - fee_share
    half = qty / 2
    stop_full = leg(sl, qty, fee_in)
    tp1_half = leg(tp1, half, fee_in / 2)
    be_after_tp1 = tp1_half + leg(entry, half, fee_in / 2)
    return stop_full, tp1_half, be_after_tp1

def open_market_position(st, sym, d, chat):
    side = d["side"]
    qty = NOTIONAL / d["entry"]
    fee_in = NOTIONAL * FEE_MAKER
    risk_all = open_risk_usd(st)
    risk_usd = NOTIONAL * d["risk_pct"]
    stop_full, tp1_half, be_after_tp1 = _profit_scenarios_ema(d["entry"], d["sl"], d["tp1"], qty, side)
    by = bybit_price(sym)
    entry_px = d["entry"]

    live_note = "PAPER (комиссии учтены)"
    if AUTO_TRADE:
        live_note = "LIVE" + (" DEMO" if BYBIT_USE_DEMO else " РЕАЛ") + " (Bybit)"
        bybit_set_leverage(sym, LEVERAGE)
        r_order = bybit_market_long(sym, qty) if side == "long" else bybit_market_short(sym, qty)
        if r_order.get("retCode") != 0:
            tg_send(chat, f"\u26A0\uFE0F {sym}: ОШИБКА рыночного входа на Bybit: {r_order.get('retMsg')}")
            return
        if by: entry_px = by
        r_stop = bybit_set_stop(sym, sl_price=d["sl"], tp_price=None)
        if r_stop.get("retCode") != 0:
            tg_send(chat, f"\u26A0\uFE0F {sym}: SL не выставлен на Bybit: {r_stop.get('retMsg')}")
        r_tp1 = bybit_reduce_limit(sym, qty / 2, d["tp1"], side=side)
        tp1_order_id = r_tp1.get("result", {}).get("orderId") if r_tp1.get("retCode") == 0 else None
        if r_tp1.get("retCode") != 0:
            tg_send(chat, f"\u26A0\uFE0F {sym}: TP1-лимитка не выставлена: {r_tp1.get('retMsg')}")
    else:
        tp1_order_id = None

    st["positions"][sym] = dict(sym=sym, side=side, entry=entry_px, sl=d["sl"], tp1=d["tp1"],
                                 atr=d["atr"], qty=qty, qty_init=qty, fee_in=fee_in,
                                 half_closed=False, be_moved=False, peak=0.0,
                                 tp1_order_id=tp1_order_id,
                                 opened=dt.datetime.now().isoformat(timespec="seconds"))
    st["trades_today"] = st.get("trades_today", 0) + 1
    save_state(st)

    warn = ""
    if risk_usd > DEPOSIT * 0.02:
        warn = (f"\n\u26A0\uFE0F Риск {risk_usd:.0f}$ = {risk_usd/DEPOSIT*100:.1f}% депозита — "
                f"выше правила 1-2%. Маржа {MARGIN:.0f}$ x{LEVERAGE:.0f} — агрессивно на реале.")
    side_ru = "LONG (Buy)" if side == "long" else "SHORT (Sell)"
    emoji_side = "\U0001F4C8" if side == "long" else "\U0001F4C9"
    L = [
        f"{emoji_side} {sym} \u00b7 СИГНАЛ: EMA{EMA_CROSS_FAST}/{EMA_CROSS_SLOW} crossover \u2192 {side_ru} \u2014 {live_note}",
        f"\U0001F4B5 Вход МАРКЕТОМ на открытии новой свечи: ${entry_px:.6g}" + (f" \u00b7 Binance close ${d['close3']:.6g}" if by else ""),
        "",
        "\U0001F9E0 ПОЧЕМУ ВХОЖУ — чек-лист:",
        f"\u2705 Кроссовер EMA{EMA_CROSS_FAST} (${d['ema_fast']:.6g}) x EMA{EMA_CROSS_SLOW} (${d['ema_slow']:.6g})",
        f"\u2705 Тренд-фильтр: цена {'>' if side=='long' else '<'} SMA{TREND_SMA_LEN} (${d['sma_trend']:.6g})",
        f"\u2705 RSI {d['rsi']:.0f} ({'>' + str(RSI_LONG_MIN) if side=='long' else '<' + str(RSI_SHORT_MAX)})",
        (f"\u2705 ADX {d['adx']:.0f} (>{ADX_MIN} и растёт)" if d['adx_ok'] else f"\u2139\uFE0F ADX {d['adx']:.0f} (слабый, но не обязателен)"),
        f"\u2705 Объём сигнальной свечи x{d['vol_ratio']:.1f} от среднего (\u2265{VOL_CONFIRM_MULT}x)",
        "",
        "\U0001F4CB ПЛАН СДЕЛКИ (TP1 50% + трейлинг остатка):",
        f"\U0001F4E6 Объём: ${NOTIONAL:.0f} = {qty:.4g} {sym.replace('USDT','')} (маржа {MARGIN:.0f}$ \u00d7 плечо {LEVERAGE:.0f})",
        f"\U0001F6D1 Стоп: ${d['sl']:.6g} ({ATR_SL_MULT_EMA}\u00d7ATR, \u2212{d['risk_pct']*100:.2f}%) \u2192 потеря {stop_full:+.2f}$",
        f"\U0001F3AF TP1: ${d['tp1']:.6g} ({ATR_TP1_MULT_EMA}\u00d7ATR, RR 1:{ATR_TP1_MULT_EMA/ATR_SL_MULT_EMA:.1f}) \u2192 закрываю 50% \u2192 {tp1_half:+.2f}$",
        f"\U0001F513 После TP1: SL остатка \u2192 БУ ({be_after_tp1:+.2f}$), далее трейлинг {ATR_TRAIL_MULT_EMA}\u00d7ATR",
        f"{warn}",
        "",
        (f"\U0001F517 Суммарный риск занятых слотов: \u2248{risk_all:.2f}$ ({risk_all/DEPOSIT*100:.1f}% депозита)"
         if slots_used(st) > 1 else None),
        f"\U0001F9EA Слоты: {slots_used(st)}/{MAX_CONCURRENT} заняты \u00b7 сделок сегодня: {st.get('trades_today',0)}",
        "Команды: /pos \u00b7 /stats \u00b7 /pause \u00b7 /help",
    ]
    tg_send(chat, "\n".join(x for x in L if x is not None))
    log_signal(sym, d["close3"], side)


def close_part(st, chat, pos, price, part, reason):
    side = pos.get("side", "long")
    sign = 1 if side == "long" else -1
    qty_close = pos["qty"] * part
    if AUTO_TRADE:
        if part >= 0.999:
            r = bybit_close_market(pos["sym"], qty_close, side=side)
            if r.get("retCode") != 0:
                tg_send(chat, f"\u26A0\uFE0F {pos['sym']}: ошибка закрытия на Bybit: {r.get('retMsg')}")
            bybit_cancel_all(pos["sym"])
        else:
            if "СТОП" in reason.upper():
                r = bybit_close_market(pos["sym"], qty_close, side=side)
                if r.get("retCode") != 0:
                    tg_send(chat, f"\u26A0\uFE0F {pos['sym']}: ошибка частичного закрытия на Bybit: {r.get('retMsg')}")
        if pos.get("be_moved"):
            r_sl = bybit_set_stop(pos["sym"], sl_price=pos["sl"], tp_price=None)
            if r_sl.get("retCode") != 0:
                tg_send(chat, f"\u26A0\uFE0F {pos['sym']}: SL в БУ не обновлён на Bybit: {r_sl.get('retMsg')}")
    gross = sign * (price - pos["entry"]) * qty_close
    fee_exit = price * qty_close * FEE_TAKER
    fee_in_share = pos.get("fee_in", 0.0) * (qty_close / pos.get("qty_init", qty_close))
    pnl = gross - fee_exit - fee_in_share
    risk_per_unit = abs(pos["entry"] - pos["sl"])
    r_mult = (sign * (price - pos["entry"]) / risk_per_unit) if risk_per_unit > 0 else 0
    log_trade([pos["opened"], dt.datetime.now().isoformat(timespec="seconds"),
               pos["sym"], side, f"{pos['entry']:.8g}", f"{price:.8g}", f"{qty_close:.8g}",
               f"{part:.2f}", f"{pnl:.2f}", f"{r_mult:.2f}", reason])
    pos["qty"] -= qty_close
    emoji = "\U0001F4B0" if pnl >= 0 else "\U0001F53B"
    tg_send(chat, f"{emoji} {pos['sym']} ({side}): {reason} по ${price:.6g}\n"
                  f"PnL части: {pnl:+.2f}$ ({r_mult:+.2f}R, комиссии учтены)")
    if pos["qty"] <= 1e-12 or part >= 0.999:
        st["positions"].pop(pos["sym"], None)
        tg_send(chat, f"\U0001F4CB {pos['sym']} закрыта полностью. Слот освободился "
                      f"({slots_used(st)}/{MAX_CONCURRENT} занято). Сегодня сделок: {daily_txt(st)}.")
    save_state(st)


def manage_position(st, chat):
    """side-aware менеджмент: TP1 закрывает 50%, SL остатка -> БУ, трейлинг ATR до полного выхода
    (без фиксированного TP2 — остаток идёт по трейлингу, как в спеке)."""
    for sym, pos in list(st.get("positions", {}).items()):
        price = bybit_price(sym); time.sleep(0.05)
        if price is None: continue
        side = pos.get("side", "long")
        long_side = side == "long"
        if not pos["half_closed"]:
            hit_sl = price <= pos["sl"] if long_side else price >= pos["sl"]
            hit_tp1 = price >= pos["tp1"] if long_side else price <= pos["tp1"]
            if hit_sl:
                close_part(st, chat, pos, pos["sl"], 1.0, "СТОП-ЛОСС"); continue
            if hit_tp1:
                close_part(st, chat, pos, pos["tp1"], 0.5, f"ТЕЙК-ПРОФИТ 1 ({ATR_TP1_MULT_EMA}\u00d7ATR)")
                if sym in st.get("positions", {}):
                    pos["half_closed"] = True; pos["be_moved"] = True
                    pos["sl"] = pos["entry"]; pos["peak"] = price; save_state(st)
                    tg_send(chat, f"\U0001F513 {sym}: SL остатка \u2192 БУ (${pos['entry']:.6g}), "
                                  f"трейлинг {ATR_TRAIL_MULT_EMA}\u00d7ATR.")
                continue
        else:
            if long_side:
                if price > pos["peak"]: pos["peak"] = price; save_state(st)
                trail_stop = pos["peak"] - ATR_TRAIL_MULT_EMA * pos["atr"]
                if price <= trail_stop:
                    close_part(st, chat, pos, trail_stop, 1.0, "ТРЕЙЛИНГ-СТОП (ATR)"); continue
                if price <= pos["sl"]:
                    close_part(st, chat, pos, pos["sl"], 1.0, "СТОП В БУ")
            else:
                if pos["peak"] == 0 or price < pos["peak"]: pos["peak"] = price; save_state(st)
                trail_stop = pos["peak"] + ATR_TRAIL_MULT_EMA * pos["atr"]
                if price >= trail_stop:
                    close_part(st, chat, pos, trail_stop, 1.0, "ТРЕЙЛИНГ-СТОП (ATR)"); continue
                if price >= pos["sl"]:
                    close_part(st, chat, pos, pos["sl"], 1.0, "СТОП В БУ")

# ============================== СТАТИСТИКА ==============================
def pos_text(st):
    poss = dict(st.get("positions", {}))
    margin_used = MARGIN * slots_used(st)
    head = (f"\U0001F4CA Слоты: {slots_used(st)}/{MAX_CONCURRENT} \u00b7 "
            f"сделок сегодня {daily_txt(st)} \u00b7 маржа занята {margin_used:.0f}$/{DEPOSIT:.0f}$")
    if not poss:
        return head + "\nВсе слоты свободны — сканирую рынок (EMA9/21 crossover)."
    L = [head]
    for sym, pos in poss.items():
        pr = bybit_price(sym) or pos["entry"]
        side = pos.get("side", "long")
        sign = 1 if side == "long" else -1
        upnl = sign * (pr - pos["entry"]) * pos["qty"]
        stage = "трейлинг (стоп в плюсе)" if pos["half_closed"] else "жду TP1/SL"
        L.append(f"\U0001F4CC {sym} ({side}): вход ${pos['entry']:.6g} \u2192 сейчас ${pr:.6g} "
                 f"({upnl:+.2f}$) \u00b7 {stage}")
    return "\n".join(L)

def stats_text():
    if not os.path.exists(TRADES_FILE):
        return "\U0001F4CA Сделок ещё нет."
    rows = list(csv.DictReader(open(TRADES_FILE)))
    if not rows: return "\U0001F4CA Сделок ещё нет."
    n = len(rows)
    pnls = [float(r["pnl_usd"]) for r in rows]
    rs = [float(r["r_mult"]) for r in rows]
    wins = sum(1 for x in pnls if x > 0)
    total = sum(pnls)
    return ("\U0001F4CA СТАТИСТИКА (EMA Crossover v2, с комиссиями)\n"
            f"Закрытий: {n} \u00b7 в плюсе: {wins} ({wins/n*100:.0f}%)\n"
            f"Средний R: {sum(rs)/n:+.2f} \u00b7 Сумма PnL: {total:+.2f}$\n"
            f"Депозит {DEPOSIT:.0f}$ \u2192 {'\u2705' if total>=0 else '\u274C'} {total/DEPOSIT*100:+.1f}%")

# ============================== ОСНОВНОЙ ЦИКЛ ==============================
last_processed_candle_time = {}
_reject_stats = {}
_scan_counter = {"total": 0, "last_reset": time.time()}
BT_RUNNING = {"on": False}

def scan_once(st, chat):
    if BT_RUNNING["on"]: return
    ok_allowed, why = trading_allowed(st)
    if not ok_allowed: return
    busy = engaged_syms(st)
    coins = [s for s in universe() if s not in busy]
    if not coins: return

    need_bars = max(TREND_SMA_LEN, ADX_LEN * 3, VOL_MA_LEN) + 20
    try:
        batch = asyncio.run(fetch_klines_batch(coins, need_bars))
    except Exception as e:
        print("batch fetch err:", e); return

    for sym, (o, h, l, c, v, ct) in batch.items():
        o, h, l, c, v, ct = o[:-1], h[:-1], l[:-1], c[:-1], v[:-1], ct[:-1]
        if len(c) < need_bars - 5: continue
        current_candle_time = ct[-1]
        if current_candle_time <= last_processed_candle_time.get(sym, 0): continue
        last_processed_candle_time[sym] = current_candle_time

        _scan_counter["total"] += 1
        ok, d = detect_signal(o, h, l, c, v, ct)
        if not ok:
            reason_key = str(d).split(" (")[0].split(" x")[0]
            _reject_stats[reason_key] = _reject_stats.get(reason_key, 0) + 1
            continue
        allowed, why = trading_allowed(st)
        if not allowed: return
        open_market_position(st, sym, d, chat)
        busy.add(sym)
        if slots_used(st) >= MAX_CONCURRENT: return

def debug_text():
    elapsed_h = (time.time() - _scan_counter["last_reset"]) / 3600
    total = _scan_counter["total"]
    if total == 0:
        return "\U0001F50D Пока нет данных: сканирование только запустилось."
    lines = [f"\U0001F50D Диагностика за {elapsed_h:.1f}ч \u00b7 проверок закрытых свечей: {total}", ""]
    for reason, cnt in sorted(_reject_stats.items(), key=lambda x: -x[1])[:15]:
        lines.append(f"\u2022 {reason}: {cnt} ({cnt/total*100:.1f}%)")
    return "\n".join(lines)

# ============================== БЭКТЕСТЕР (упрощённый, EMA v2, long+short) ==============================
def bt_klines(symbol, days):
    need_bars_per_day = {"15m": 96, "1h": 24}.get(TF, 96)
    need = int(days * need_bars_per_day) + max(TREND_SMA_LEN, ADX_LEN * 3, VOL_MA_LEN) + 40
    out = []; end = None
    while len(out) < need:
        url = f"{BINANCE}/fapi/v1/klines?symbol={symbol}&interval={TF}&limit=1500"
        if end: url += f"&endTime={end}"
        d = http_json(url); time.sleep(0.15)
        if not d: break
        out = d + out
        end = int(d[0][0]) - 1
        if len(d) < 1500: break
    out = out[-need:]
    o = [float(x[1]) for x in out]; h = [float(x[2]) for x in out]
    l = [float(x[3]) for x in out]; c = [float(x[4]) for x in out]
    v = [float(x[5]) for x in out]; ct = [int(x[6]) for x in out]
    return o, h, l, c, v, ct

def _bt_leg(pos, price, part):
    sign = 1 if pos["side"] == "long" else -1
    qty_close = pos["qty"] * part
    gross = sign * (price - pos["entry"]) * qty_close
    fee_exit = price * qty_close * FEE_TAKER
    fee_in_share = pos["fee_in"] * (qty_close / pos["qty_init"])
    pnl = gross - fee_exit - fee_in_share
    pos["qty"] -= qty_close
    pos["pnl"] += pnl
    return pnl

def bt_simulate_coin(sym, o, h, l, c, v, ct, diag=None):
    W = max(TREND_SMA_LEN, ADX_LEN * 3, VOL_MA_LEN) + 20
    positions = []; pos = None
    for i in range(W, len(c)):
        bar_h, bar_l = h[i], l[i]
        if pos:
            long_side = pos["side"] == "long"
            if not pos["half"]:
                hit_sl = bar_l <= pos["sl"] if long_side else bar_h >= pos["sl"]
                hit_tp1 = bar_h >= pos["tp1"] if long_side else bar_l <= pos["tp1"]
                if hit_sl:
                    _bt_leg(pos, pos["sl"], 1.0); pos["close_ts"] = ct[i]; positions.append(pos); pos = None
                elif hit_tp1:
                    _bt_leg(pos, pos["tp1"], 0.5)
                    pos["half"] = True; pos["sl"] = pos["entry"]
                    pos["peak"] = pos["tp1"]
            if pos and pos["half"]:
                if long_side:
                    pos["peak"] = max(pos.get("peak", 0), bar_h)
                    trail = pos["peak"] - ATR_TRAIL_MULT_EMA * pos["atr"]
                    if bar_l <= trail:
                        _bt_leg(pos, trail, 1.0); pos["close_ts"] = ct[i]; positions.append(pos); pos = None; continue
                    if bar_l <= pos["sl"]:
                        _bt_leg(pos, pos["sl"], 1.0); pos["close_ts"] = ct[i]; positions.append(pos); pos = None
                else:
                    pos["peak"] = min(pos.get("peak", 1e18), bar_l)
                    trail = pos["peak"] + ATR_TRAIL_MULT_EMA * pos["atr"]
                    if bar_h >= trail:
                        _bt_leg(pos, trail, 1.0); pos["close_ts"] = ct[i]; positions.append(pos); pos = None; continue
                    if bar_h >= pos["sl"]:
                        _bt_leg(pos, pos["sl"], 1.0); pos["close_ts"] = ct[i]; positions.append(pos); pos = None
        if pos is None:
            if diag is not None: diag["evals"] += 1
            ok, d = detect_signal(o[i + 1 - W:i + 1], h[i + 1 - W:i + 1], l[i + 1 - W:i + 1],
                                   c[i + 1 - W:i + 1], v[i + 1 - W:i + 1], ct[i + 1 - W:i + 1])
            if ok:
                if diag is not None: diag["signals"] += 1
                qty = NOTIONAL / d["entry"]
                pos = dict(sym=sym, side=d["side"], entry=d["entry"], sl=d["sl"], tp1=d["tp1"],
                           atr=d["atr"], qty=qty, qty_init=qty, fee_in=NOTIONAL * FEE_MAKER,
                           half=False, peak=0.0, pnl=0.0, open_ts=ct[i])
            elif diag is not None:
                diag["reasons"][str(d)] = diag["reasons"].get(str(d), 0) + 1
    return positions

def bt_portfolio(all_pos, deposit):
    taken = []
    for p in sorted(all_pos, key=lambda x: x["open_ts"]):
        active = [t for t in taken if t["close_ts"] > p["open_ts"]]
        if len(active) < MAX_CONCURRENT:
            taken.append(p)
    taken.sort(key=lambda x: x["close_ts"])
    eq = [deposit]
    for t in taken: eq.append(eq[-1] + t["pnl"])
    return taken, eq

def run_backtest(chat, days=14, ncoins=30):
    if BT_RUNNING["on"]:
        tg_send(chat, "\u23F3 Бэктест уже идёт."); return
    BT_RUNNING["on"] = True
    try:
        days = max(3, min(days, 30)); ncoins = max(5, min(ncoins, 60))
        tg_send(chat, f"\U0001F9EA Бэктест EMA{EMA_CROSS_FAST}/{EMA_CROSS_SLOW} crossover: {days} дн \u00d7 до {ncoins} монет.")
        try:
            tick = http_json(f"{BINANCE}/fapi/v1/ticker/24hr", timeout=15)
        except Exception:
            tg_send(chat, "\u274C Не удалось получить список монет."); return
        cands = sorted(
            [(t["symbol"], float(t.get("quoteVolume", 0) or 0)) for t in tick if t.get("symbol", "").endswith("USDT")],
            key=lambda x: -x[1])
        coins = [s for s, qv in cands if qv >= MIN_QUOTE_VOL24][:ncoins]
        all_pos = []; diag = dict(evals=0, signals=0, reasons={})
        for k, sym in enumerate(coins, 1):
            try:
                o, h, l, c, v, ct = bt_klines(sym, days)
                if len(c) < max(TREND_SMA_LEN, ADX_LEN * 3) + 60: continue
                all_pos += bt_simulate_coin(sym, o[:-1], h[:-1], l[:-1], c[:-1], v[:-1], ct[:-1], diag=diag)
            except Exception as e:
                print(f"bt {sym} err:", e)
            if k % 10 == 0:
                tg_send(chat, f"\u2699\uFE0F {k}/{len(coins)} монет \u00b7 сигналов {diag['signals']} \u00b7 сделок {len(all_pos)}")
        taken, eq = bt_portfolio(all_pos, DEPOSIT)
        if not taken:
            tg_send(chat, f"\U0001F4ED За {days} дн по {len(coins)} монетам сделок не было (сигналов: {diag['signals']}).")
            return
        n = len(taken); wins = sum(1 for t in taken if t["pnl"] > 0)
        total = eq[-1] - DEPOSIT
        peak = DEPOSIT; dd = 0.0
        for x in eq:
            peak = max(peak, x); dd = max(dd, (peak - x) / peak)
        longs = sum(1 for t in taken if t["side"] == "long")
        shorts = n - longs
        tg_send(chat, (f"\U0001F9EA БЭКТЕСТ EMA{EMA_CROSS_FAST}/{EMA_CROSS_SLOW}: {days} дн \u00d7 {len(coins)} монет\n"
                       f"Сделок: {n} (Long {longs} \u00b7 Short {shorts})\n"
                       f"В плюсе: {wins} ({wins/n*100:.0f}%)\n"
                       f"Итог: {total:+.2f}$ ({total/DEPOSIT*100:+.1f}% депо) \u00b7 макс.просадка {dd*100:.1f}%\n"
                       f"\u26A0\uFE0F Комиссии учтены; спред/проскальзывание нет; пессимизм: в спорном баре стоп раньше тейка."))
    finally:
        BT_RUNNING["on"] = False

# ============================== TELEGRAM LOOP ==============================
def tg_loop(st):
    offset = 0
    while True:
        try:
            r = tg("getUpdates", _timeout=35, timeout=25, offset=offset)
            if not r or not r.get("ok"):
                time.sleep(2); continue
            for u in r["result"]:
                offset = u["update_id"] + 1
                msg = u.get("message") or {}
                text = (msg.get("text") or "").strip()
                cid = msg.get("chat", {}).get("id")
                if not cid: continue
                save_chat(cid)
                if text.startswith("/help"):
                    tg_send(cid,
                        "\U0001F4D6 Команды EVA v4 (EMA Crossover):\n"
                        "/start — запуск и краткая сводка\n"
                        "/pos — текущие позиции: цена, PnL, стадия\n"
                        "/stats — статистика: win rate, средний R, PnL\n"
                        "/debug — топ причин отказа сигналов\n"
                        "/backtest [дней] [монет] — прогон по истории (long+short)\n"
                        "/pause — пауза (позиции ведутся, новые не ищутся)\n"
                        "/resume — возобновить сканирование\n"
                        "/help — эта справка")
                elif text.startswith("/start"):
                    st["paused"] = False; save_state(st)
                    tg_send(cid, "\U0001F916 EVA v4 — EMA9/21 Crossover (Long+Short)\n"
                             "Данные: Binance \u00b7 Цены/исполнение: Bybit\n"
                             f"Лимиты: до {MAX_CONCURRENT} позиций \u00b7 "
                             f"{'дневной лимит сделок ' + str(MAX_DAILY_TRADES) if MAX_DAILY_TRADES>0 else 'без дневного лимита сделок'} \u00b7 "
                             f"дневной стоп по убытку {DAILY_LOSS_LIMIT_PCT}% \u00b7 "
                             f"Объём ${NOTIONAL:.0f} (маржа {MARGIN:.0f}$ x{LEVERAGE:.0f})\n"
                             "Команды: /pos \u00b7 /stats \u00b7 /backtest \u00b7 /pause \u00b7 /resume \u00b7 /help")
                elif text.startswith("/pause"):
                    st["paused"] = True; save_state(st)
                    tg_send(cid, "\u23F8 Пауза: новые сигналы не ищу.")
                elif text.startswith("/resume"):
                    st["paused"] = False; save_state(st)
                    tg_send(cid, "\u25B6\uFE0F Сканирование возобновлено.")
                elif text.startswith("/pos"):
                    tg_send(cid, pos_text(st))
                elif text.startswith("/stats"):
                    tg_send(cid, stats_text())
                elif text.startswith("/debug"):
                    tg_send(cid, debug_text())
                elif text.startswith("/backtest"):
                    parts = text.split()[1:]
                    nums = [p for p in parts if p.isdigit()]
                    bd = int(nums[0]) if len(nums) > 0 else 14
                    bc = int(nums[1]) if len(nums) > 1 else 30
                    threading.Thread(target=run_backtest, args=(cid, bd, bc), daemon=True).start()
        except Exception as e:
            print("tg_loop err:", e); time.sleep(3)

def main():
    st = load_state()
    chat = load_chat()
    print("EVA v4 (EMA Crossover Long+Short) запущен. chat:", "есть" if chat else "нет")
    threading.Thread(target=tg_loop, args=(st,), daemon=True).start()
    last_scan = last_manage = 0
    while True:
        try:
            chat = load_chat()
            roll_day(st, chat)
            now = time.time()
            if now - last_manage >= MANAGE_EVERY_SEC:
                last_manage = now
                manage_position(st, chat)
            if now - last_scan >= SCAN_EVERY_SEC:
                last_scan = now
                scan_once(st, chat)
                print(f"[scan] слоты {slots_used(st)}/{MAX_CONCURRENT} \u00b7 сделок сегодня {st.get('trades_today',0)}")
        except Exception as e:
            print("main err:", e)
        time.sleep(2)

if __name__ == "__main__":
    main()
