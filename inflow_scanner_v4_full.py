#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EVA v3 — ИМПУЛЬСНЫЙ БОТ (полная переделка по спеке)
=====================================================
ДАННЫЕ:  Binance Futures (объёмы больше, есть taker buy volume -> честный CVD)
ЦЕНЫ/ТОРГОВЛЯ: Bybit (сверка цены, символ должен существовать на Bybit)
ИСПОЛНЕНИЕ v1: PAPER-режим — бот ведёт виртуальные сделки с учётом комиссий
и пишет каждую в журнал. Реальное исполнение (Bybit demo API) — v2,
только после проверки логики на данных. Реальные деньги — только
после положительной статистики. Это красная линия.

СТРАТЕГИЯ (чек-лист из спеки, LONG) — БЕЗ УСЛОВИЯ "3 ЗЕЛЁНЫХ СВЕЧИ":
1. Затишье: объёмы ровные относительно Volume MA20
2. Триггер: всплеск объёма на M15 >= 2.5x MA20 (импульсная свеча)
3. Импульс: цена пробивает high предыдущих баров, тело в сторону движения
4. Размер импульсной свечи ограничен ATR (не "паранормальный бар")
5. Деньги: OI растёт устойчиво + CVD (дельта) положительна на импульсе
6. Тренд: close > EMA21 > EMA50 (M15)
7. Логика: пробит локальный уровень (max high за сутки до импульса)
8. Безопасность: RSI14(M15) < 75
9. ВХОД: МАРКЕТ-ордер сразу на закрытии сигнальной свечи (без лимитки/ретеста)
10. SL: entry - 1.5*ATR14 (динамический, под текущую волатильность)
11. TP1 (entry+2.0*ATR): закрыть 50%, SL остатка -> БУ немедленно + ТРЕЙЛИНГ 1.5*ATR до TP2 (entry+4.5*ATR)
12. ЛИМИТЫ: до 2 позиций ОДНОВРЕМЕННО; закрылась — слот сразу освобождается;
дневной лимит опционален (ENV MAX_DAILY_TRADES, 0 = выключен)

ДЕПЛОЙ: Railway, переменные окружения:
TG_TOKEN — токен телеграм-бота
DATA_DIR — /data (volume), по умолчанию /data
DEPOSIT_USD — 500
MARGIN_USD — 50
LEVERAGE — 10
Start Command: python inflow_scanner_v4_full.py
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

MAX_CONCURRENT = int(os.environ.get("MAX_OPEN_POSITIONS", os.environ.get("MAX_CONCURRENT", 5)))  # слоты одновременных позиций
MAX_OPEN_POSITIONS = MAX_CONCURRENT  # алиас под спеку
MAX_DAILY_TRADES = int(os.environ.get("MAX_DAILY_TRADES", 0))

# --- сигнал (спека) ---
TF = "15m"
VOL_MA_LEN = 20
VOL_SPIKE_MIN = 2.45     # Volume Spike: Current Volume > 2.0 * SMA20 (объём "просыпается")
ATR_MIN_MOVE_MULT = 1.5 # Price Action: (Close - PrevClose) > 1.5*ATR14, реальный импульс, не шум
BREAKOUT_LOOKBACK = 3   # пробой считаем не только над high пред. свечи, а над max(high) последних N свечей (шире, реже false-negative)
QUIET_BARS = 7          # строго 8 чистых баров затишья перед импульсом
QUIET_MAX = 2.05         # жёсткий порог шума в полке накопления
QUIET_ALLOW = 0         # ноль толерантности к шуму в зоне накопления
QUIET_REQUIRED = int(os.environ.get("QUIET_REQUIRED", 0))  # 0=OPTIONAL (не блокирует сигнал, только влияет на score/details), 1=обязательное отбрасывание как раньше
WICK_MAX = 0.38
ATR_LEN = 14
BAR_ATR_MAX = 3.2       # FOMO CAP: строго. High-Low сигнальной свечи > 2.5*ATR -> сигнал отбрасывается целиком
OI_MIN_GROW = 0.014      # OI-ПОДТВЕРЖДЕНИЕ: (OI_now - OI_prev)/OI_prev < 2% -> сигнал отбрасывается (фейковый объём без реального интереса)
RSI_LEN = 14
RSI_MAX = 78.0          # поднято с 75: на сильных пампах RSI летит быстро
CVD_MODE = "all"
LEVEL_LOOKBACK = 96
EMA_FAST, EMA_SLOW = 21, 50

# --- вход/выход (МАРКЕТ на открытии новой свечи сразу после сигнала; лимитка/ретест отключены) ---
FIB_RETRACE = 0.382      # 0.0: лимитка-ретест на Фибо больше не используется (плохие исполнения на затухающих пампах)
ENTRY_TTL_BARS = 8     # не используется при маркет-входе (оставлено для совместимости состояния)
FEE_MAKER = 0.0002
FEE_TAKER = 0.00055

# --- ATR-риск-менеджмент: частичная фиксация TP1/TP2 (position scaling) ---
ATR_SL_MULT = 1.8      # SL = entry - 1.5*ATR
ATR_TP1_MULT = 2.2     # TP1 = entry + 2.0*ATR -> закрыть 50% позиции
ATR_TP2_MULT = 4.5     # TP2 = entry + 4.5*ATR -> закрыть оставшиеся 50%
ATR_TRAIL_MULT = 1.4   # после TP1: SL остатка -> БУ, трейлинг 1.5*ATR от пика до TP2

# --- VALID_ENTRY: контроль качества входа относительно уровня пробоя ---
ENTRY_MAX_EXT_ATR = float(os.environ.get("ENTRY_MAX_EXT_ATR", "1.2"))       # не входим дальше 1.2*ATR от уровня
ENTRY_MIN_PULLBACK_ATR = float(os.environ.get("ENTRY_MIN_PULLBACK_ATR", "0.8"))  # должен быть откат к breakout+0.8*ATR

# --- вселенная (ГЛОБАЛЬНЫЙ СКАНЕР: без статичного топ-N, до 500+ монет одновременно) ---
MAX_COINS = int(os.environ.get("MAX_COINS", "500"))
MIN_QUOTE_VOL24 = float(os.environ.get("MIN_QUOTE_VOL24", "3000000"))  # 3M USDT — отсекаем только мёртвую ликвидность
SCAN_EVERY_SEC = 5      # тик проверки каждые 5с, но сигнал считается ТОЛЬКО на закрытии новой 15м-свечи
MANAGE_EVERY_SEC = 5    # быстрый менеджмент позиций: TP1 -> БУ + трейлинг проверяются каждые 5с

# --- файлы (на volume) ---
def ensure_dirs():
    try: os.makedirs(DATA_DIR, exist_ok=True)
    except Exception: pass
ensure_dirs()
STATE_FILE = os.path.join(DATA_DIR, "v3_state.json")
TRADES_FILE = os.path.join(DATA_DIR, "v3_trades.csv")
SIGNALS_FILE = os.path.join(DATA_DIR, "v3_signals.csv")
CHAT_FILE = os.path.join(DATA_DIR, "v3_chat.txt")

BINANCE = "https://fapi.binance.com"
BYBIT_LIVE = "https://api.bybit.com"
BYBIT_DEMO = "https://api-demo.bybit.com"

# --- Bybit авто-торговля (реальные ордера вместо paper) ---
BYBIT_API_KEY = os.environ.get("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", "")
BYBIT_USE_DEMO = os.environ.get("BYBIT_USE_DEMO", "1") == "1"   # 1 = demo-счёт (виртуальный баланс, реальные цены)
AUTO_TRADE = os.environ.get("AUTO_TRADE", "0") == "1"           # 0 = paper (как раньше), 1 = реальные ордера на Bybit
BYBIT = BYBIT_DEMO if BYBIT_USE_DEMO else BYBIT_LIVE
CATEGORY = "linear"

# ============================== HTTP/TG ==============================
def http_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "eva-v3"})
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
    tg("sendMessage", chat_id=chat, text=text, parse_mode="HTML",
       disable_web_page_preview=True)

def load_chat():
    try:
        with open(CHAT_FILE) as f: return f.read().strip()
    except Exception: return None

def save_chat(cid):
    try:
        with open(CHAT_FILE, "w") as f: f.write(str(cid))
    except Exception: pass

# ============================== ДАННЫЕ ==============================
_uni_cache = {"ts": 0, "coins": [], "snapshot": {}}

UNIV_MIN_OI_GROWTH = float(os.environ.get("UNIV_MIN_OI_GROWTH", "0.02"))    # OI рост >2% на сигнальной свече (см. detect_signal)
UNIV_MIN_VOL_GROWTH = float(os.environ.get("UNIV_MIN_VOL_GROWTH", "0.0"))   # доп. фильтр разгона объёма (0 = не используется на этапе вселенной)
UNIV_MIN_PRICE_CHG = float(os.environ.get("UNIV_MIN_PRICE_CHG", "0.0"))     # цена за 24ч не в минусе (лонговые деньги, не шорт-памп)

_RATE_SEM = asyncio.Semaphore(20)   # ограничитель параллелизма, чтобы не попасть под rate-limit Binance/Bybit

async def _fetch_json_async(session, url):
    async with _RATE_SEM:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                return await r.json()
        except Exception as e:
            print("async fetch err:", url, e)
            return None

async def build_universe_async():
    """ГЛОБАЛЬНЫЙ СКАНЕР: тянем ВСЕ линейные USDT-фьючерсы с Binance + Bybit параллельно (asyncio.gather),
    без статичного топ-N. Отсекаем только мёртвую ликвидность по MIN_QUOTE_VOL24 (3M USDT).
    Возвращает до MAX_COINS (500+) символов, торгуемых на обеих биржах одновременно."""
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
        pchg = float(t.get("priceChangePercent", 0) or 0) / 100.0
        binance[s] = {"vol": qv, "pchg": pchg}

    bybit_oi = {}
    for x in by.get("result", {}).get("list", []):
        try:
            bybit_oi[x["symbol"]] = float(x.get("openInterest", 0) or 0)
        except Exception:
            continue

    prev_snap = _uni_cache.get("snapshot", {})
    now_snap = {}
    scored = []
    for s, b in binance.items():
        if s not in bybit_oi: continue
        oi_now = bybit_oi[s]
        now_snap[s] = {"vol": b["vol"], "oi": oi_now}
        prev = prev_snap.get(s)
        base_score = b["vol"]  # без истории по умолчанию ранжируем по ликвидности (первый цикл)
        if prev and prev.get("oi", 0) > 0 and prev.get("vol", 0) > 0:
            oi_growth = (oi_now - prev["oi"]) / prev["oi"]
            vol_growth = (b["vol"] - prev["vol"]) / prev["vol"]
            if b["pchg"] < UNIV_MIN_PRICE_CHG: continue
            if vol_growth < UNIV_MIN_VOL_GROWTH: continue
            base_score = oi_growth + vol_growth + b["pchg"]
        scored.append((s, base_score))

    scored.sort(key=lambda x: -x[1])
    coins = [s for s, _ in scored][:MAX_COINS]
    _uni_cache["snapshot"] = now_snap
    _uni_cache["coins"] = coins; _uni_cache["ts"] = time.time()
    print(f"Universe updated (async global scan): {len(coins)} coins из {len(binance)} по ликвидности \u2265{MIN_QUOTE_VOL24:,.0f}$")
    return coins

def universe():
    """Синхронная обёртка для остального (синхронного) кода бота: раз в час запускает async
    build_universe_async() внутри отдельного event loop и кеширует результат."""
    if time.time() - _uni_cache["ts"] < 3600 and _uni_cache["coins"]:
        return _uni_cache["coins"]
    try:
        return asyncio.run(build_universe_async())
    except Exception as e:
        print("universe err:", e)
        return _uni_cache["coins"]

async def fetch_klines_oi_batch(symbols):
    """ОПТИМИЗАЦИЯ ПОД МАСШТАБ: параллельно (asyncio.gather + семафор) тянем 15м-свечи и OI-историю
    для ВСЕХ символов вселенной за один проход, вместо последовательного for-цикла с time.sleep.
    Возвращает dict symbol -> (klines_tuple, oi_list) или None при ошибке."""
    async with aiohttp.ClientSession() as session:
        async def one(sym):
            k_url = f"{BINANCE}/fapi/v1/klines?symbol={sym}&interval={TF}&limit={LEVEL_LOOKBACK + 40}"
            oi_url = f"{BINANCE}/futures/data/openInterestHist?symbol={sym}&period=15m&limit=12"
            k_data, oi_data = await asyncio.gather(
                _fetch_json_async(session, k_url), _fetch_json_async(session, oi_url))
            if not k_data or len(k_data) < LEVEL_LOOKBACK + 30:
                return sym, None
            o = [float(x[1]) for x in k_data]; h = [float(x[2]) for x in k_data]
            l = [float(x[3]) for x in k_data]; c = [float(x[4]) for x in k_data]
            v = [float(x[5]) for x in k_data]; tb = [float(x[9]) for x in k_data]
            ct = [int(x[0]) for x in k_data]
            oi = [float(x["sumOpenInterest"]) for x in (oi_data or [])]
            return sym, ((o, h, l, c, v, tb, ct), oi)

        results = await asyncio.gather(*[one(s) for s in symbols], return_exceptions=False)
    return {sym: data for sym, data in results if data is not None}

_klines_cache = {}

def klines15(symbol, limit=200):
    now = time.time()
    cache_key = (symbol, limit)
    if cache_key in _klines_cache:
        data, ts = _klines_cache[cache_key]
        if now - ts < 300:
            return data
    try:
        d = http_json(f"{BINANCE}/fapi/v1/klines?symbol={symbol}&interval={TF}&limit={limit}")
        o = [float(x[1]) for x in d]; h = [float(x[2]) for x in d]
        l = [float(x[3]) for x in d]; c = [float(x[4]) for x in d]
        v = [float(x[5]) for x in d]; tb = [float(x[9]) for x in d]
        ct = [int(x[6]) for x in d]
        result = (o, h, l, c, v, tb, ct)
        _klines_cache[cache_key] = (result, now)
        return result
    except Exception as e:
        print(f"klines {symbol} err:", e)
        if cache_key in _klines_cache:
            return _klines_cache[cache_key][0]
        raise

def oi_hist(symbol, limit=12):
    try:
        d = http_json(f"{BINANCE}/futures/data/openInterestHist?symbol={symbol}&period=15m&limit={limit}")
        return [float(x["sumOpenInterest"]) for x in d]
    except Exception:
        return []

def bybit_price(symbol):
    try:
        d = http_json(f"{BYBIT}/v5/market/tickers?category=linear&symbol={symbol}")
        return float(d["result"]["list"][0]["lastPrice"])
    except Exception:
        return None

# ============================== BYBIT АВТО-ТОРГОВЛЯ (v5 API) ==============================
def _bybit_signed(method, path, body=None, params=None):
    """Подписанный запрос к Bybit v5 (HMAC-SHA256). Работает и с demo, и с live через BYBIT (base_url)."""
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
    import math
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
    """Рыночный LONG сразу на открытии новой свечи после закрытия сигнальной (без лимитки/ретеста)."""
    qty_r = bybit_round_qty(symbol, qty)
    return _bybit_signed("POST", "/v5/order/create", body={
        "category": CATEGORY, "symbol": symbol, "side": "Buy",
        "orderType": "Market", "qty": str(qty_r), "timeInForce": "IOC",
    })

def bybit_cancel_order(symbol, order_id):
    return _bybit_signed("POST", "/v5/order/cancel", body={
        "category": CATEGORY, "symbol": symbol, "orderId": order_id,
    })

def bybit_set_stop(symbol, sl_price=None, tp_price=None):
    """Устанавливает/обновляет SL и/или TP для ВСЕЙ текущей позиции (position-level stop)."""
    body = {"category": CATEGORY, "symbol": symbol, "positionIdx": 0}
    if sl_price is not None: body["stopLoss"] = str(bybit_round_price(symbol, sl_price))
    if tp_price is not None: body["takeProfit"] = str(bybit_round_price(symbol, tp_price))
    return _bybit_signed("POST", "/v5/position/trading-stop", body=body)

def bybit_reduce_limit(symbol, qty, price):
    """Reduce-only лимитка на частичное закрытие (например, 50% на TP1)."""
    qty_r = bybit_round_qty(symbol, qty)
    price_r = bybit_round_price(symbol, price)
    return _bybit_signed("POST", "/v5/order/create", body={
        "category": CATEGORY, "symbol": symbol, "side": "Sell",
        "orderType": "Limit", "qty": str(qty_r), "price": str(price_r),
        "timeInForce": "GTC", "reduceOnly": True,
    })

def bybit_close_market(symbol, qty):
    """Reduce-only маркет на закрытие qty контрактов (например, полный стоп/выход по трейлингу)."""
    qty_r = bybit_round_qty(symbol, qty)
    return _bybit_signed("POST", "/v5/order/create", body={
        "category": CATEGORY, "symbol": symbol, "side": "Sell",
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

# ============================== СИГНАЛ (по спеке, БЕЗ "3 зелёных") ==============================
def detect_signal(o, h, l, c, v, tb, oi):
    """EVA v4 — сбалансированная версия"""
    n = len(c)
    if n < LEVEL_LOOKBACK + 30: return False, "мало истории"
    i1, i2, i3 = n - 3, n - 2, n - 1

    # 1. Структура (разумный минимум)
    green_count = sum(1 for i in (i1, i2, i3) if c[i] > o[i])
    strong_momentum = (c[i2] > h[i1] or c[i3] > h[i2])
    if green_count < 2 or not strong_momentum:
        return False, "слабая структура"

    # 2. Затишье + всплеск
    base = v[i1 - VOL_MA_LEN:i1]
    if len(base) < VOL_MA_LEN: return False, "мало базы"
    vma = sum(base) / len(base)
    if vma <= 0: return False, "нулевая база"
    
    noisy = sum(1 for x in v[i1 - QUIET_BARS:i1] if x > vma * QUIET_MAX)
    if noisy > QUIET_ALLOW: return False, f"не было затишья ({noisy})"
    
    spike = v[i1] / vma
    if spike < VOL_SPIKE_MIN: return False, f"слабый всплеск x{spike:.1f}"

    # 3. Фитиль
    rng3 = h[i3] - l[i3]
    if rng3 > 0 and (h[i3] - c[i3]) / rng3 > WICK_MAX:
        return False, "длинный фитиль"

    # 4. CVD
    deltas = [2 * tb[i] - v[i] for i in (i1, i2, i3)]
    if sum(deltas) <= 0 or deltas[-1] <= 0:
        return False, "CVD слабый"

    # 5. OI
    oi_ok = False
    if len(oi) >= 5:
        oi_chg = (oi[-1] / oi[-5] - 1) if oi[-5] > 0 else 0
        oi_ok = oi_chg >= OI_MIN_GROW
    if not oi_ok: return False, "OI слабый"

    # 6. EMA + RSI + Уровень
    e21 = ema_series(c, EMA_FAST)[-1]
    if c[i3] <= e21: return False, "нет аптренда EMA"

    r = rsi(c[-(RSI_LEN * 6):], RSI_LEN)
    if r > RSI_MAX: return False, f"RSI {r:.0f} перегрет"

    level = max(h[i1 - LEVEL_LOOKBACK:i1])
    if c[i3] <= level: return False, "уровень не пробит"

    # Расчёт входа
    impulse = h[i3] - l[i1]
    if impulse <= 0: return False, "нет импульса"

    entry = h[i3] - FIB_RETRACE * impulse
    sl = l[i1] * (1 - SL_BUFFER)
    if entry <= sl: return False, "вход ниже стопа"

    risk_pct = (entry - sl) / entry
    tp1 = entry + (entry - sl) * TP1_RR

    return True, dict(
        spike=spike, deltas=deltas, oi_chg=oi_chg if 'oi_chg' in locals() else 0, rsi=r,
        e21=e21, e50=ema_series(c, EMA_SLOW)[-1], level=level, 
        low1=l[i1], high3=h[i3], entry=entry, sl=sl, tp1=tp1, 
        risk_pct=risk_pct, wick=(h[i3]-c[i3])/rng3 if rng3 > 0 else 0, close3=c[i3]
    )


# ==============================================================
# PROP-STYLE ENTRY LOGIC (встроено по спеке пользователя как есть,
# функции management переименованы с префиксом prop_, чтобы не
# конфликтовать с существующими manage_position/open_market_position
# бота — сама логика и пороги НЕ изменены)
# ==============================================================

def check_long_entry(signal):
    score = 0
    reasons = []

    vol_ratio = signal["volume"] / signal["vol_ma20"]

    # 1. Volume impulse
    if vol_ratio >= 2.5:
        score += 2
        reasons.append("Volume spike")
    else:
        return False, "No volume impulse"

    # 2. Breakout
    if signal["close"] > signal["prev_high"]:
        score += 2
        reasons.append("Breakout")
    else:
        return False, "No breakout"

    # 3. Trend alignment
    if signal["close"] > signal["ema21"] > signal["ema50"]:
        score += 1
        reasons.append("Trend aligned")

    # 4. Smart money
    if signal["oi_delta"] > 0 and signal["cvd_delta"] > 0:
        score += 2
        reasons.append("Smart money")
    else:
        return False, "No smart money"

    # 5. RSI filter
    if signal["rsi"] < 75:
        score += 1
    else:
        return False, "Overbought"

    # 6. Candle sanity check
    candle_size = signal["high"] - signal["close"]
    if signal["atr"] * 0.5 < candle_size < signal["atr"] * 2.5:
        score += 1
    else:
        return False, "Bad candle"

    if score >= 6:
        return True, f"ENTRY OK | score={score} | {' | '.join(reasons)}"

    return False, f"Weak setup score={score}"


# =========================
# POSITION OPEN
# =========================

def open_position(price, atr):
    return {
        "entry": price,
        "sl": price - 1.5 * atr,
        "tp1": price + 2.0 * atr,
        "tp2": price + 4.5 * atr,
        "size": 1.0,
        "half_closed": False,
        "trail_active": False,
        "trail_sl": None,
        "status": "OPEN"
    }


# =========================
# POSITION MANAGEMENT (переименовано в prop_manage_position:
# у бота уже есть своя manage_position(st, chat) для живой торговли
# через Bybit — эта версия работает с локальным dict pos, как в спеке)
# =========================

def prop_manage_position(pos, price, atr):
    if pos["status"] != "OPEN":
        return pos, None

    # STOP LOSS
    if price <= pos["sl"]:
        pos["status"] = "CLOSED"
        return pos, "STOP LOSS"

    # TP1
    if not pos["half_closed"] and price >= pos["tp1"]:
        pos["half_closed"] = True
        pos["size"] = 0.5
        pos["sl"] = pos["entry"]

        pos["trail_active"] = True
        pos["trail_sl"] = price - 1.5 * atr

        return pos, "TP1 HIT"

    # TRAILING
    if pos["trail_active"]:
        new_trail = price - 1.5 * atr

        if new_trail > pos["trail_sl"]:
            pos["trail_sl"] = new_trail

        if price <= pos["trail_sl"]:
            pos["status"] = "CLOSED"
            return pos, "TRAIL STOP"

    # TP2
    if price >= pos["tp2"]:
        pos["status"] = "CLOSED"
        return pos, "TP2 HIT"

    return pos, None


# =========================
# 🔌 INTEGRATION В ТВОЙ LOOP
# =========================


# ==============================================================
# PROP-STRATEGY: ПОЛНОСТЬЮ ПАРАЛЛЕЛЬНЫЙ НЕЗАВИСИМЫЙ ЦИКЛ
# Работает в своём потоке, со своим состоянием (prop_state.json),
# своими слотами и своим тикером — НЕ пересекается с основной
# стратегией (detect_signal / scan_once / manage_position) и не
# делит с ней слоты MAX_CONCURRENT. Реальных ордеров НЕ шлёт —
# режим PAPER (виртуальные позиции), чтобы можно было безопасно
# сравнить обе стратегии на одном живом потоке данных.
# ==============================================================

PROP_ENABLED = os.environ.get("PROP_STRATEGY_ENABLED", "0") == "1"
PROP_SCAN_EVERY_SEC = int(os.environ.get("PROP_SCAN_EVERY_SEC", "5"))
PROP_MANAGE_EVERY_SEC = int(os.environ.get("PROP_MANAGE_EVERY_SEC", "5"))
PROP_MAX_POSITIONS = int(os.environ.get("PROP_MAX_POSITIONS", "5"))
PROP_NOTIONAL = float(os.environ.get("PROP_NOTIONAL_USD", str(NOTIONAL)))
PROP_STATE_FILE = os.path.join(DATA_DIR, "prop_state.json")

_prop_last_bar = {}  # sym -> timestamp последней обработанной ЗАКРЫТОЙ свечи (свой guard, независимый от основного бота)

def prop_load_state():
    try:
        with open(PROP_STATE_FILE) as f:
            d = json.load(f)
            d.setdefault("positions", [])
            return d
    except Exception:
        return {"positions": []}

def prop_save_state(state):
    try:
        with open(PROP_STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        print("prop_save_state err:", e)

def prop_build_signal(sym, kl, oi):
    """Собирает dict 'signal' в формате, который ожидает check_long_entry(), из тех же
    живых данных Binance (свечи+CVD) / Bybit-OI, что использует основной сканер."""
    o, h, l, c, v, tb, ct = kl
    o, h, l, c, v, tb, ct = o[:-1], h[:-1], l[:-1], c[:-1], v[:-1], tb[:-1], ct[:-1]
    n = len(c)
    if n < VOL_MA_LEN + 5 or n < ATR_LEN + 5:
        return None
    i1 = n - 1
    current_candle_time = ct[-1]

    vol_ma20 = sum(v[i1 - VOL_MA_LEN:i1]) / VOL_MA_LEN
    if vol_ma20 <= 0:
        return None
    a = atr(h[:i1], l[:i1], c[:i1], ATR_LEN)
    if a <= 0:
        return None
    e21 = ema_series(c, EMA_FAST)[-1]
    e50 = ema_series(c, EMA_SLOW)[-1]
    r = rsi(c[-(RSI_LEN * 6):], RSI_LEN) if n > RSI_LEN * 6 else 50.0
    delta = 2 * tb[i1] - v[i1]  # CVD-дельта на закрытой свече (та же формула, что и в detect_signal)

    oi_delta = 0.0
    if oi and len(oi) >= 2 and oi[-2] > 0:
        oi_delta = oi[-1] - oi[-2]

    signal = dict(
        sym=sym, ts=current_candle_time,
        close=c[i1], high=h[i1], low=l[i1], prev_high=h[i1 - 1],
        volume=v[i1], vol_ma20=vol_ma20,
        ema21=e21, ema50=e50, rsi=r, atr=a,
        oi_delta=oi_delta, cvd_delta=delta,
    )
    return signal

def prop_scan_once(state, chat):
    """Независимый скан: тот же список монет из universe() и тот же параллельный батч-фетчер
    fetch_klines_oi_batch, что у основного бота (переиспользуем инфраструктуру данных),
    но решение о входе принимает ИСКЛЮЧИТЕЛЬНО check_long_entry() из prop-модуля."""
    if len(state["positions"]) >= PROP_MAX_POSITIONS:
        return
    coins = universe()
    if not coins:
        return
    try:
        batch = asyncio.run(fetch_klines_oi_batch(coins))
    except Exception as e:
        print("prop_scan_once batch err:", e)
        return

    open_syms = {p["sym"] for p in state["positions"] if p["status"] == "OPEN"}
    for sym, (kl, oi) in batch.items():
        if sym in open_syms:
            continue
        if len(state["positions"]) - sum(1 for p in state["positions"] if p["status"] != "OPEN") >= PROP_MAX_POSITIONS:
            break
        signal = prop_build_signal(sym, kl, oi[-8:] if oi else [])
        if signal is None:
            continue
        if _prop_last_bar.get(sym) == signal["ts"]:
            continue
        _prop_last_bar[sym] = signal["ts"]

        ok, reason = check_long_entry(signal)
        if not ok:
            continue
        pos = open_position(signal["close"], signal["atr"])
        pos["sym"] = sym
        state["positions"].append(pos)
        prop_save_state(state)
        tg_send(chat, f"\U0001F680 [PROP] {sym}: {reason} @ ${signal['close']:.6g} "
                      f"(PAPER, независимая стратегия, слот {len(open_syms)+1}/{PROP_MAX_POSITIONS})")

def prop_manage_all(state, chat):
    """Опрашивает цену на Bybit для каждой открытой prop-позиции каждые PROP_MANAGE_EVERY_SEC
    и прогоняет через prop_manage_position() — TP1->БУ->трейлинг->TP2, как в спеке."""
    changed = False
    for pos in state["positions"]:
        if pos["status"] != "OPEN":
            continue
        sym = pos["sym"]
        price = bybit_price(sym); time.sleep(0.05)
        if price is None:
            continue
        atr_now = pos.get("atr", (pos["tp1"] - pos["entry"]) / ATR_TP1_MULT)
        pos_before_half = pos["half_closed"]
        pos, event = prop_manage_position(pos, price, atr_now)
        if event:
            changed = True
            emoji = "\U0001F4B0" if event in ("TP1 HIT", "TP2 HIT") else "\U0001F53B" if event == "STOP LOSS" else "\U0001F512"
            tg_send(chat, f"{emoji} [PROP] {sym}: {event} @ ${price:.6g}")
    if changed:
        prop_save_state(state)

def prop_loop():
    """Полностью САМОСТОЯТЕЛЬНЫЙ поток: своя частота скана/менеджмента, своё состояние,
    свои слоты. Запускается из main() отдельным threading.Thread и не влияет на основной
    бот (detect_signal/scan_once/manage_position) при отключении через PROP_STRATEGY_ENABLED=0."""
    if not PROP_ENABLED:
        print("[PROP] отключена (PROP_STRATEGY_ENABLED=0) — не запускаю параллельный цикл.")
        return
    state = prop_load_state()
    chat = load_chat()
    print("[PROP] параллельная стратегия запущена (PAPER, независимо от основного бота)")
    last_scan = last_manage = 0
    while True:
        try:
            chat = load_chat()
            now = time.time()
            if now - last_manage >= PROP_MANAGE_EVERY_SEC:
                last_manage = now
                prop_manage_all(state, chat)
            if now - last_scan >= PROP_SCAN_EVERY_SEC:
                last_scan = now
                prop_scan_once(state, chat)
                open_n = sum(1 for p in state["positions"] if p["status"] == "OPEN")
                print(f"[PROP scan] открытых позиций {open_n}/{PROP_MAX_POSITIONS}")
        except Exception as e:
            print("[PROP] loop err:", e)
        time.sleep(2)

def process_signal(state, signal):
    price = signal["close"]
    atr = signal["atr"]

    # ENTRY
    ok, reason = check_long_entry(signal)

    if ok:
        pos = open_position(price, atr)
        state["positions"].append(pos)
        print(f"\U0001F680 {reason} @ {price}")

    # EXIT / MANAGEMENT
    for pos in state["positions"]:
        pos, event = prop_manage_position(pos, price, atr)

        if event:
            print(f"\u26A1 {event} @ {price}")

# ============================== СОСТОЯНИЕ/ЛИМИТЫ ==============================
def _default_state():
    return dict(day=str(dt.datetime.now(dt.timezone.utc).date()),
                trades_today=0, paused=False,
                pendings={}, positions={})

def load_state():
    try:
        with open(STATE_FILE) as f: st = json.load(f)
    except Exception: return _default_state()
    if "pendings" not in st:
        st["pendings"] = {}
        p = st.pop("pending", None)
        if p: st["pendings"][p["sym"]] = p
    if "positions" not in st:
        st["positions"] = {}
        p = st.pop("position", None)
        if p: st["positions"][p["sym"]] = p
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
        st["day"] = d; st["trades_today"] = 0
        save_state(st)
        if chat: tg_send(chat, f"\U0001F305 Новый день (UTC) — счётчик сделок обнулён ({daily_txt(st)}).")

def slots_used(st):
    return len(st.get("pendings", {})) + len(st.get("positions", {}))

def engaged_syms(st):
    return set(st.get("pendings", {})) | set(st.get("positions", {}))

def daily_txt(st):
    n = st.get("trades_today", 0)
    return f"{n}/{MAX_DAILY_TRADES}" if MAX_DAILY_TRADES > 0 else f"{n} (дневного лимита нет)"

def open_risk_usd(st):
    total = 0.0
    for p in st.get("pendings", {}).values():
        total += (p["entry"] - p["sl"]) * (NOTIONAL / p["entry"])
    for pos in st.get("positions", {}).values():
        if not pos.get("half_closed"):
            total += (pos["entry"] - pos["sl"]) * pos["qty"]
    return total

def trading_allowed(st):
    if st.get("paused"): return False, "пауза"
    if MAX_DAILY_TRADES > 0 and st.get("trades_today", 0) >= MAX_DAILY_TRADES:
        return False, f"дневной лимит {MAX_DAILY_TRADES} исчерпан"
    if slots_used(st) >= MAX_CONCURRENT:
        return False, f"заняты все слоты ({MAX_CONCURRENT}/{MAX_CONCURRENT})"
    return True, ""

# ============================== ЖУРНАЛЫ ==============================
def log_signal(coin, price):
    try:
        new = not os.path.exists(SIGNALS_FILE)
        with open(SIGNALS_FILE, "a", newline="") as f:
            w = csv.writer(f)
            if new: w.writerow(["ts", "coin", "type", "price", "btc_price"])
            b = bybit_price("BTCUSDT") or ""
            w.writerow([dt.datetime.now().isoformat(timespec="seconds"), coin, "impulse", price, b])
    except Exception as e: print("log_signal err:", e)

def log_trade(row):
    try:
        new = not os.path.exists(TRADES_FILE)
        with open(TRADES_FILE, "a", newline="") as f:
            w = csv.writer(f)
            if new: w.writerow(["ts_open", "ts_close", "coin", "entry", "exit", "qty", "part", "pnl_usd", "r_mult", "reason"])
            w.writerow(row)
    except Exception as e: print("log_trade err:", e)

# ============================== PAPER-ДВИЖОК ==============================
def _profit_scenarios(entry, sl, tp1, tp2, qty, atr):
    """Частичная фиксация: TP1=entry+2*ATR (50%), TP2=entry+4.5*ATR (50%).
    После TP1 остаток переводится в БУ, трейлинг 1.5*ATR от пика до TP2."""
    fee_in = NOTIONAL * FEE_MAKER
    def leg(exit_px, q, fee_share):
        return (exit_px - entry) * q - exit_px * q * FEE_TAKER - fee_share
    half = qty / 2
    stop_full = leg(sl, qty, fee_in)                       # полный стоп до TP1
    tp1_half = leg(tp1, half, fee_in / 2)                  # закрытие 50% на TP1
    be_after_tp1 = tp1_half + leg(entry, half, fee_in / 2)  # остаток закрылся по БУ (комиссии в минус)
    tp2_full = tp1_half + leg(tp2, half, fee_in / 2)       # остаток дошёл до TP2
    trail_min = tp1_half + leg(tp1, half, fee_in / 2)      # минимум сразу после переноса в БУ (трейлинг ещё не дал профит)
    return stop_full, tp1_half, be_after_tp1, tp2_full, trail_min

def open_market_position(st, sym, d, chat):
    """Маркет-вход на открытии новой свечи сразу после закрытия сигнальной (FIB_RETRACE=0.0).
    FOMO CAP уже отработал внутри detect_signal (BAR_ATR_MAX=2.5): сигналы на перерастянутых
    свечах сюда не попадают, поэтому маркет-ордер не покупает абсолютный хай импульса."""
    qty = NOTIONAL / d["entry"]
    fee_in = NOTIONAL * FEE_MAKER
    risk_all = open_risk_usd(st)
    risk_usd = NOTIONAL * d["risk_pct"]
    stop_full, tp1_half, be_after_tp1, tp2_full, trail_min = _profit_scenarios(
        d["entry"], d["sl"], d["tp1"], d["tp2"], qty, d["atr"])
    by = bybit_price(sym)
    entry_px = d["entry"]

    live_note = "PAPER (комиссии учтены)"
    if AUTO_TRADE:
        live_note = "LIVE" + (" DEMO" if BYBIT_USE_DEMO else " РЕАЛ") + " (Bybit)"
        bybit_set_leverage(sym, LEVERAGE)
        r_order = bybit_market_long(sym, qty)
        if r_order.get("retCode") != 0:
            tg_send(chat, f"\u26A0\uFE0F {sym}: ОШИБКА рыночного входа на Bybit: {r_order.get('retMsg')}")
            return
        if by: entry_px = by  # фактическая цена исполнения на Bybit, если удалось получить
        r_stop = bybit_set_stop(sym, sl_price=d["sl"], tp_price=None)
        if r_stop.get("retCode") != 0:
            tg_send(chat, f"\u26A0\uFE0F {sym}: SL не выставлен на Bybit: {r_stop.get('retMsg')}")
        r_tp1 = bybit_reduce_limit(sym, qty / 2, d["tp1"])
        tp1_order_id = r_tp1.get("result", {}).get("orderId") if r_tp1.get("retCode") == 0 else None
        if r_tp1.get("retCode") != 0:
            tg_send(chat, f"\u26A0\uFE0F {sym}: TP1-лимитка не выставлена: {r_tp1.get('retMsg')}")
    else:
        tp1_order_id = None

    st["positions"][sym] = dict(sym=sym, entry=entry_px, sl=d["sl"], tp1=d["tp1"], tp2=d["tp2"],
                                 atr=d["atr"], qty=qty, qty_init=qty, fee_in=fee_in,
                                 half_closed=False, be_moved=False, peak=0.0,
                                 tp1_order_id=tp1_order_id,
                                 opened=dt.datetime.now().isoformat(timespec="seconds"))
    st["trades_today"] = st.get("trades_today", 0) + 1
    save_state(st)

    impulse_pct = (d["high3"] / d["low1"] - 1) * 100
    warn = ""
    if risk_usd > DEPOSIT * 0.02:
        warn = (f"\n\u26A0\uFE0F Риск {risk_usd:.0f}$ = {risk_usd/DEPOSIT*100:.1f}% депозита — "
                f"выше правила 1-2%. Твои параметры (маржа {MARGIN:.0f}$ x{LEVERAGE:.0f}), но на реале это агрессивно.")
    L = [
        f"\U0001F680 {sym} · СИГНАЛ: импульсный пробой \u2014 {live_note}",
        f"\U0001F4B5 Вход МАРКЕТОМ на открытии новой свечи: ${entry_px:.6g}" + (f" \u00b7 Binance close ${d['close3']:.6g}" if by else ""),
        "",
        "\U0001F9E0 ПОЧЕМУ ВХОЖУ — весь чек-лист (факты):",
        f"\u2705 Затишье было ({QUIET_BARS} баров, строго без исключений), затем ВСПЛЕСК объёма \u00d7{d['spike']:.1f} от MA20 (порог \u2265{VOL_SPIKE_MIN}x)",
        f"\u2705 Импульс: {d['low1']:.6g} \u2192 {d['high3']:.6g} (+{impulse_pct:.1f}%)",
        f"\u2705 FOMO-кап пройден: свеча \u2264 {BAR_ATR_MAX}\u00d7ATR (не перерастянута)",
        f"\u2705 Фитиль {d['wick']*100:.0f}% (\u226430%) — продавец не гасит",
        f"\u2705 CVD растёт: дельта покупок положительна (+{d['delta']:,.0f})",
        f"\u2705 Тренд: цена > EMA21 (${d['e21']:.6g}) > EMA50 (${d['e50']:.6g})",
        f"\u2705 Пробит суточный уровень ${d['level']:.6g}",
        f"\u2705 RSI {d['rsi']:.0f} (<{RSI_MAX:.0f}) — не перегрет",
        "",
        "\U0001F4CB ПЛАН СДЕЛКИ (частичная фиксация TP1/TP2):",
        f"\U0001F4E6 Объём: ${NOTIONAL:.0f} = {qty:.4g} {sym.replace('USDT','')} (маржа {MARGIN:.0f}$ \u00d7 плечо {LEVERAGE:.0f})",
        f"\U0001F6D1 Стоп: ${d['sl']:.6g} (entry \u2212 {ATR_SL_MULT}\u00d7ATR, \u2212{d['risk_pct']*100:.2f}%) \u2192 потеря {stop_full:+.2f}$",
        f"\U0001F3AF TP1: ${d['tp1']:.6g} (entry + {ATR_TP1_MULT}\u00d7ATR) \u2192 закрываю 50% \u2192 {tp1_half:+.2f}$ в карман",
        f"\U0001F3AF TP2: ${d['tp2']:.6g} (entry + {ATR_TP2_MULT}\u00d7ATR) \u2192 остаток 50% \u2192 {tp2_full:+.2f}$ суммарно",
        f"\U0001F513 Как только TP1 срабатывает: SL остатка \u2192 БУ немедленно (покрывает комиссии), трейлинг {ATR_TRAIL_MULT}\u00d7ATR от пика до TP2",
        "",
        "\U0001F4B0 СЦЕНАРИИ ИТОГА (с комиссиями):",
        f"\u2022 полный стоп-лосс (до TP1): {stop_full:+.2f}$",
        f"\u2022 TP1, затем БУ-стоп по остатку: {be_after_tp1:+.2f}$",
        f"\u2022 TP1 + TP2 (полный ход): {tp2_full:+.2f}$",
        f"{warn}",
        "",
        (f"\U0001F517 Суммарный риск занятых слотов: \u2248{risk_all:.2f}$ ({risk_all/DEPOSIT*100:.1f}% депозита) — две лонг-позиции = удвоенная ставка на рынок"
         if slots_used(st) > 1 else None),
        f"\U0001F9EA Слоты: {slots_used(st)}/{MAX_CONCURRENT} заняты \u00b7 сделок сегодня: {st.get('trades_today',0)}",
        "Команды: /pos \u00b7 /stats \u00b7 /pause \u00b7 /help",
    ]
    tg_send(chat, "\n".join(x for x in L if x is not None))
    log_signal(sym, d["close3"])

def close_part(st, chat, pos, price, part, reason):
    qty_close = pos["qty"] * part
    if AUTO_TRADE:
        if part >= 0.999:
            r = bybit_close_market(pos["sym"], qty_close)
            if r.get("retCode") != 0:
                tg_send(chat, f"\u26A0\uFE0F {pos['sym']}: ошибка закрытия на Bybit: {r.get('retMsg')}")
            bybit_cancel_all(pos["sym"])
        else:
            if "СТОП" not in reason.upper():
                # TP1 уже стоит лимиткой на бирже (выставлена в fill_pending) — здесь просто фиксируем в state
                pass
            else:
                r = bybit_close_market(pos["sym"], qty_close)
                if r.get("retCode") != 0:
                    tg_send(chat, f"\u26A0\uFE0F {pos['sym']}: ошибка частичного закрытия на Bybit: {r.get('retMsg')}")
        if pos.get("be_moved"):
            r_sl = bybit_set_stop(pos["sym"], sl_price=pos["sl"], tp_price=None)
            if r_sl.get("retCode") != 0:
                tg_send(chat, f"\u26A0\uFE0F {pos['sym']}: SL в БУ не обновлён на Bybit: {r_sl.get('retMsg')}")
    gross = (price - pos["entry"]) * qty_close
    fee_exit = price * qty_close * FEE_TAKER
    fee_in_share = pos.get("fee_in", 0.0) * (qty_close / pos.get("qty_init", qty_close))
    pnl = gross - fee_exit - fee_in_share
    risk_per_unit = pos["entry"] - pos["sl"]
    r_mult = ((price - pos["entry"]) / risk_per_unit) if risk_per_unit > 0 else 0
    log_trade([pos["opened"], dt.datetime.now().isoformat(timespec="seconds"),
               pos["sym"], f"{pos['entry']:.8g}", f"{price:.8g}", f"{qty_close:.8g}",
               f"{part:.2f}", f"{pnl:.2f}", f"{r_mult:.2f}", reason])
    pos["qty"] -= qty_close
    emoji = "\U0001F4B0" if pnl >= 0 else "\U0001F53B"
    tg_send(chat, f"{emoji} {pos['sym']}: {reason} по ${price:.6g}\n"
                  f"PnL части: {pnl:+.2f}$ ({r_mult:+.2f}R, комиссии учтены)")
    if pos["qty"] <= 1e-12 or part >= 0.999:
        st["positions"].pop(pos["sym"], None)
        tg_send(chat, f"\U0001F4CB {pos['sym']} закрыта полностью. Слот освободился "
                      f"({slots_used(st)}/{MAX_CONCURRENT} занято) — могу открывать следующую. "
                      f"Сегодня сделок: {daily_txt(st)}.")
    save_state(st)

def manage_position(st, chat):
    """Частичная фиксация: TP1 (entry+2*ATR) закрывает 50%, сразу переносит SL остатка в БУ.
    TP2 (entry+4.5*ATR) закрывает финальные 50%. Между TP1 и TP2 — трейлинг 1.5*ATR от пика."""
    for sym, pos in list(st.get("positions", {}).items()):
        price = bybit_price(sym); time.sleep(0.05)
        if price is None: continue
        if not pos["half_closed"]:
            if price <= pos["sl"]:
                close_part(st, chat, pos, pos["sl"], 1.0, "СТОП-ЛОСС"); continue
            if price >= pos["tp1"]:
                close_part(st, chat, pos, pos["tp1"], 0.5, "ТЕЙК-ПРОФИТ 1 (2\u00d7ATR)")
                if sym in st.get("positions", {}):
                    pos["half_closed"] = True; pos["be_moved"] = True
                    pos["sl"] = pos["entry"]; pos["peak"] = price; save_state(st)
                    tg_send(chat, f"\U0001F513 {sym}: SL остатка \u2192 БУ (${pos['entry']:.6g}), "
                                  f"трейлинг {ATR_TRAIL_MULT}\u00d7ATR до TP2.")
                continue
        else:
            if price >= pos["tp2"]:
                close_part(st, chat, pos, pos["tp2"], 1.0, "ТЕЙК-ПРОФИТ 2 (4.5\u00d7ATR)"); continue
            if price > pos["peak"]:
                pos["peak"] = price; save_state(st)
            trail_stop = pos["peak"] - ATR_TRAIL_MULT * pos["atr"]
            if price <= trail_stop:
                close_part(st, chat, pos, trail_stop, 1.0, "ТРЕЙЛИНГ-СТОП (ATR)"); continue
            if price <= pos["sl"]:
                close_part(st, chat, pos, pos["sl"], 1.0, "СТОП В БУ")

# ============================== СТАТИСТИКА ==============================
def pos_text(st):
    pens, poss = {}, {}
    for _ in range(5):
        try:
            pens = dict(st.get("pendings", {})); poss = dict(st.get("positions", {}))
            break
        except RuntimeError:
            time.sleep(0.02)
    margin_used = MARGIN * slots_used(st)
    head = (f"\U0001F4CA Слоты: {slots_used(st)}/{MAX_CONCURRENT} \u00b7 "
            f"сделок сегодня {daily_txt(st)} \u00b7 "
            f"маржа занята {margin_used:.0f}$/{DEPOSIT:.0f}$")
    if not pens and not poss:
        return head + "\nВсе слоты свободны — сканирую рынок."
    L = [head]
    for sym, pos in poss.items():
        pr = bybit_price(sym) or pos["entry"]
        upnl = (pr - pos["entry"]) * pos["qty"]
        stage = "трейлинг (стоп в плюсе)" if pos["half_closed"] else "жду TP1/SL"
        L.append(f"\U0001F4CC {sym}: вход ${pos['entry']:.6g} \u2192 сейчас ${pr:.6g} "
                 f"({upnl:+.2f}$) \u00b7 {stage}")
    for sym, p in pens.items():
        L.append(f"\u23F3 {sym}: жду ретеста ${p['entry']:.6g} (осталось {p['ttl']} свечей)")
    return "\n".join(L)

def stats_text():
    if not os.path.exists(TRADES_FILE):
        return "\U0001F4CA Сделок ещё нет. PAPER-движок копит статистику."
    rows = list(csv.DictReader(open(TRADES_FILE)))
    if not rows: return "\U0001F4CA Сделок ещё нет."
    n = len(rows)
    pnls = [float(r["pnl_usd"]) for r in rows]
    rs = [float(r["r_mult"]) for r in rows]
    wins = sum(1 for x in pnls if x > 0)
    total = sum(pnls)
    return ("\U0001F4CA PAPER-статистика (честная, с комиссиями)\n"
            f"Закрытий: {n} \u00b7 в плюсе: {wins} ({wins/n*100:.0f}%)\n"
            f"Средний R: {sum(rs)/n:+.2f} \u00b7 Сумма PnL: {total:+.2f}$\n"
            f"Депозит {DEPOSIT:.0f}$ \u2192 {'\u2705' if total>=0 else '\u274C'} {total/DEPOSIT*100:+.1f}%\n\n"
            "Правда о стратегии = эти цифры на дистанции, а не красота сигналов. "
            "Реальные деньги — только если тут устойчивый плюс.")

# ============================== ОСНОВНОЙ ЦИКЛ ==============================
last_processed_candle_time = {}   # sym -> timestamp последней ОБРАБОТАННОЙ закрытой свечи (anti mid-candle guard)
_reject_stats = {}
_scan_counter = {"total": 0, "last_reset": time.time()}

def scan_once(st, chat):
    """ГЛОБАЛЬНЫЙ СКАНЕР: до 500+ монет за проход, данные тянутся ПАРАЛЛЕЛЬНО (asyncio.gather)
    через fetch_klines_oi_batch, а не последовательным циклом с time.sleep — это убирает
    rate-limit и лаг при масштабе. Guard last_processed_candle_time гарантирует ровно ОДНУ
    оценку стратегии за жизнь свечи (мид-свечные входы исключены)."""
    if BT_RUNNING["on"]:
        return
    ok_allowed, why = trading_allowed(st)
    if not ok_allowed:
        return
    busy = engaged_syms(st)
    coins = [s for s in universe() if s not in busy]
    if not coins:
        return

    try:
        batch = asyncio.run(fetch_klines_oi_batch(coins))
    except Exception as e:
        print("batch fetch err:", e)
        return

    for sym, (kl, oi) in batch.items():
        o, h, l, c, v, tb, ct = kl
        o, h, l, c, v, tb, ct = o[:-1], h[:-1], l[:-1], c[:-1], v[:-1], tb[:-1], ct[:-1]
        if len(c) < LEVEL_LOOKBACK + 30: continue
        current_candle_time = ct[-1]  # timestamp последней ЗАКРЫТОЙ свечи

        # --- ЖЁСТКИЙ GUARD: одна оценка на свечу, никакого мид-свечного пересчёта ---
        if current_candle_time <= last_processed_candle_time.get(sym, 0):
            continue
        last_processed_candle_time[sym] = current_candle_time  # фиксируем ДО обработки

        _scan_counter["total"] += 1
        ok, d = detect_signal(o, h, l, c, v, tb, oi[-8:] if oi else [])
        if not ok:
            reason_key = str(d).split(" (")[0].split(" x")[0]
            _reject_stats[reason_key] = _reject_stats.get(reason_key, 0) + 1
            continue
        allowed, why = trading_allowed(st)
        if not allowed: return
        open_market_position(st, sym, d, chat)   # МАРКЕТ на открытии новой свечи (лимитка/ретест отключены)
        busy.add(sym)
        if slots_used(st) >= MAX_CONCURRENT:
            return


def debug_text():
    elapsed_h = (time.time() - _scan_counter["last_reset"]) / 3600
    total = _scan_counter["total"]
    if total == 0:
        return "\U0001F50D Пока нет данных: сканирование только запустилось."
    lines = [f"\U0001F50D Диагностика за {elapsed_h:.1f}ч \u00b7 всего проверок закрытых свечей: {total}", ""]
    sorted_reasons = sorted(_reject_stats.items(), key=lambda x: -x[1])
    for reason, cnt in sorted_reasons[:15]:
        pct = cnt / total * 100
        lines.append(f"\u2022 {reason}: {cnt} ({pct:.1f}%)")
    lines.append("")
    lines.append("\U0001F4A1 Самая частая причина отказа = где именно фильтр слишком строгий.")
    return "\n".join(lines)

# ============================== БЭКТЕСТЕР ==============================
BT_RUNNING = {"on": False}

def _cvd_cast(s):
    s = str(s).lower()
    if s not in ("all", "sum"): raise ValueError("cvd: all|sum")
    return s

BT_PARAMS = {
    "spike": ("VOL_SPIKE_MIN", lambda s: float(s)),
    "quiet": ("QUIET_MAX", lambda s: float(s)),
    "qbars": ("QUIET_BARS", lambda s: int(float(s))),
    "qallow": ("QUIET_ALLOW", lambda s: int(float(s))),
    "wick": ("WICK_MAX", lambda s: float(s)),
    "atr": ("BAR_ATR_MAX", lambda s: float(s)),
    "oi": ("OI_MIN_GROW", lambda s: float(s)),
    "rsi": ("RSI_MAX", lambda s: float(s)),
    "cvd": ("CVD_MODE", _cvd_cast),
    "slmult": ("ATR_SL_MULT", lambda s: float(s)),
    "tp1mult": ("ATR_TP1_MULT", lambda s: float(s)),
    "tp2mult": ("ATR_TP2_MULT", lambda s: float(s)),
    "trailmult": ("ATR_TRAIL_MULT", lambda s: float(s)),
}

BT_PRESETS = {
    "soft": {"quiet": "2.2", "qallow": "2", "spike": "2.0",
             "atr": "3.5", "wick": "0.35", "cvd": "sum"},
}

def _bt_apply_overrides(overrides):
    applied, saved = {}, {}
    for k, raw in (overrides or {}).items():
        if k in BT_PARAMS:
            gname, cast = BT_PARAMS[k]
            try:
                val = cast(raw)
                saved[gname] = globals()[gname]
                globals()[gname] = val
                applied[k] = val
            except Exception:
                pass
    return applied, saved

def _bt_restore(saved):
    for g, v in saved.items():
        globals()[g] = v

def _ov_str(applied):
    return " ".join(f"{k}={v}" for k, v in applied.items()) if applied else "базовые (как в живом боте)"

def _parse_bt_args(text):
    parts = text.split()[1:]
    nums = [p for p in parts if p.isdigit()]
    days = int(nums[0]) if len(nums) > 0 else 14
    ncoins = int(nums[1]) if len(nums) > 1 else 30
    ov = {}
    for p in parts:
        if p.lower() in BT_PRESETS:
            ov.update(BT_PRESETS[p.lower()])
    for p in parts:
        if "=" in p:
            k, _, val = p.partition("=")
            ov[k.strip().lower()] = val.strip()
    return days, ncoins, ov

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MPL = True
except Exception:
    HAS_MPL = False

def tg_photo(chat, path, caption=""):
    if not TG_TOKEN or not chat: return
    try:
        with open(path, "rb") as f: img = f.read()
        b = "----evabt" + str(int(time.time()))
        parts = []
        for k, val in (("chat_id", str(chat)), ("caption", caption[:1000])):
            parts.append((f"--{b}\r\nContent-Disposition: form-data; "
                          f"name=\"{k}\"\r\n\r\n{val}\r\n").encode())
        parts.append((f"--{b}\r\nContent-Disposition: form-data; name=\"photo\"; "
                      f"filename=\"bt.png\"\r\nContent-Type: image/png\r\n\r\n").encode() + img + b"\r\n")
        parts.append(f"--{b}--\r\n".encode())
        body = b"".join(parts)
        req = urllib.request.Request(f"https://api.telegram.org/bot{TG_TOKEN}/sendPhoto", data=body,
                                      headers={"Content-Type": f"multipart/form-data; boundary={b}"})
        urllib.request.urlopen(req, timeout=60).read()
    except Exception as e:
        print("tg_photo err:", e)

async def bt_fetch_hourly_batch(symbols, days):
    """Параллельно (asyncio.gather) тянем ЧАСОВЫЕ свечи + часовую историю OI за весь период
    бэктеста для списка монет-кандидатов. Используется для построения momentum-вселенной
    на исторических данных вместо live-снимков."""
    bars = min(int(days * 24) + 2, 1000)
    async with aiohttp.ClientSession() as session:
        async def one(sym):
            k_url = f"{BINANCE}/fapi/v1/klines?symbol={sym}&interval=1h&limit={bars}"
            oi_url = f"{BINANCE}/futures/data/openInterestHist?symbol={sym}&period=1h&limit={bars}"
            k_data, oi_data = await asyncio.gather(
                _fetch_json_async(session, k_url), _fetch_json_async(session, oi_url))
            if not k_data or len(k_data) < 30 or not oi_data or len(oi_data) < 30:
                return sym, None
            vol = [float(x[5]) for x in k_data]
            close = [float(x[4]) for x in k_data]
            oi = [float(x["sumOpenInterest"]) for x in oi_data]
            n = min(len(vol), len(oi))
            return sym, (vol[-n:], close[-n:], oi[-n:])
        results = await asyncio.gather(*[one(s) for s in symbols], return_exceptions=False)
    return {sym: data for sym, data in results if data is not None}


def bt_build_universe(days, ncoins):
    """Строит вселенную бэктеста НА ИСТОРИЧЕСКИХ ЧАСОВЫХ СВЕЧАХ объёма/OI за окно [days],
    а не на live-снимке текущего момента. Логика идентична live universe(): монета считается
    'момент-положительной' в конкретный час, если OI и объём выросли по сравнению с предыдущим
    часом (>= UNIV_MIN_OI_GROWTH / UNIV_MIN_VOL_GROWTH) и цена за 24ч в плюсе (>= UNIV_MIN_PRICE_CHG).
    Ранжируем монеты по количеству таких 'моментум-часов' за весь период -> берём топ-ncoins.
    Это даёт честную симуляцию: в бэктесте участвуют именно те монеты, которые ИСТОРИЧЕСКИ
    показывали приток объёма/OI в этот период, а не текущий топ по ликвидности."""
    try:
        tick = http_json(f"{BINANCE}/fapi/v1/ticker/24hr", timeout=15)
    except Exception as e:
        print("bt_build_universe ticker err:", e)
        return []
    candidates = []
    for t in tick:
        s = t.get("symbol", "")
        if not s.endswith("USDT"): continue
        qv = float(t.get("quoteVolume", 0) or 0)
        if qv < MIN_QUOTE_VOL24: continue
        candidates.append((s, qv))
    candidates.sort(key=lambda x: -x[1])
    pool = [s for s, _ in candidates[:max(ncoins * 4, 120)]]  # берём пул кандидатов шире, чем итоговый ncoins

    try:
        batch = asyncio.run(bt_fetch_hourly_batch(pool, days))
    except Exception as e:
        print("bt_build_universe batch err:", e)
        return pool[:ncoins]

    scored = []
    for sym, (vol, close, oi) in batch.items():
        n = len(vol)
        if n < 26: continue
        momentum_hours = 0
        score_sum = 0.0
        for i in range(24, n):
            if oi[i - 1] <= 0 or vol[i - 1] <= 0 or close[i - 24] <= 0: continue
            oi_growth = (oi[i] - oi[i - 1]) / oi[i - 1]
            vol_growth = (vol[i] - vol[i - 1]) / vol[i - 1]
            price_chg = (close[i] - close[i - 24]) / close[i - 24]
            if price_chg < UNIV_MIN_PRICE_CHG: continue
            if oi_growth < UNIV_MIN_OI_GROWTH: continue
            if vol_growth < UNIV_MIN_VOL_GROWTH: continue
            momentum_hours += 1
            score_sum += oi_growth + vol_growth + price_chg
        if momentum_hours > 0:
            scored.append((sym, momentum_hours, score_sum))

    scored.sort(key=lambda x: (-x[1], -x[2]))
    coins = [s for s, _, _ in scored][:ncoins]
    print(f"bt_build_universe: {len(coins)}/{len(pool)} монет прошли momentum-фильтр за {days}д")
    return coins

def bt_klines(symbol, days):
    need = int(days * 96) + LEVEL_LOOKBACK + 40
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
    v = [float(x[5]) for x in out]; tb = [float(x[9]) for x in out]
    ct = [int(x[6]) for x in out]
    return o, h, l, c, v, tb, ct

def bt_oi(symbol, days):
    need = min(days, 30) * 96
    out = []; end = None
    while len(out) < need:
        url = f"{BINANCE}/futures/data/openInterestHist?symbol={symbol}&period=15m&limit=500"
        if end: url += f"&endTime={end}"
        try:
            d = http_json(url); time.sleep(0.15)
        except Exception:
            break
        if not d: break
        out = d + out
        end = int(d[0]["timestamp"]) - 1
        if len(d) < 500: break
    return [(int(x["timestamp"]), float(x["sumOpenInterest"])) for x in out]

def _bt_leg(pos, price, part):
    qty_close = pos["qty"] * part
    gross = (price - pos["entry"]) * qty_close
    fee_exit = price * qty_close * FEE_TAKER
    fee_in_share = pos["fee_in"] * (qty_close / pos["qty_init"])
    pnl = gross - fee_exit - fee_in_share
    pos["qty"] -= qty_close
    pos["pnl"] += pnl
    return pnl

def _bt_reason(msg):
    m = str(msg)
    for sub, label in (("не зелёная", "импульсная свеча не зелёная"),
                       ("пробоя", "нет пробоя high пред. свечи"),
                       ("затишья", "не было затишья (объём шумел)"),
                       ("всплеск", "всплеск слабее порога"),
                       ("фитиль", "длинный фитиль импульса"),
                       ("параболик", "свеча-параболик (ATR-кап)"),
                       ("дельта", "CVD: дельта не растёт"),
                       ("OI", "OI не растёт устойчиво"),
                       ("аптренда", "нет аптренда EMA"),
                       ("RSI", "RSI перегрет (>75)"),
                       ("слишком далеко от уровня", "VALID_ENTRY: вход слишком далеко от уровня (>1.2x ATR)"),
                       ("нет отката", "VALID_ENTRY: нет отката к уровню (>0.8x ATR)"),
                       ("не удержан", "VALID_ENTRY: уровень не удержан (low < breakout)"),
                       ("продолжения", "VALID_ENTRY: нет подтверждения продолжения"),
                       ("не подтверждает продолжение", "VALID_ENTRY: объём не подтверждает продолжение"),
                       ("уровень", "уровень не пробит"),
                       ("истории", "мало истории"),
                       ("базы", "мало/ноль объёмной базы")):
        if sub in m: return label
    return "прочее"

def bt_simulate_coin(sym, o, h, l, c, v, tb, ct, oi_ts, diag=None):
    """Симуляция маркет-входа на открытии новой свечи сразу после сигнальной (FIB_RETRACE=0.0):
    FOMO CAP (BAR_ATR_MAX=2.5) уже отсекает перерастянутые свечи внутри detect_signal.
    TP1=entry+2*ATR (закрыть 50%, SL остатка -> БУ), TP2=entry+4.5*ATR (закрыть остаток),
    между TP1 и TP2 трейлинг 1.5*ATR от пика."""
    W = LEVEL_LOOKBACK + 40
    positions = []; pos = None
    j = 0; oi_vals = []
    for i in range(W, len(c)):
        while j < len(oi_ts) and oi_ts[j][0] <= ct[i]:
            oi_vals.append(oi_ts[j][1]); j += 1
        bar_h, bar_l, bar_c = h[i], l[i], c[i]
        if pos:
            if not pos["half"]:
                if bar_l <= pos["sl"]:
                    _bt_leg(pos, pos["sl"], 1.0)
                    pos["close_ts"] = ct[i]; positions.append(pos); pos = None
                elif bar_h >= pos["tp1"]:
                    _bt_leg(pos, pos["tp1"], 0.5)
                    pos["half"] = True; pos["sl"] = pos["entry"]
                    pos["peak"] = max(pos["tp1"], bar_h)
            if pos and pos["half"]:
                if bar_h >= pos["tp2"]:
                    _bt_leg(pos, pos["tp2"], 1.0)
                    pos["close_ts"] = ct[i]; positions.append(pos); pos = None
                    continue
                pos["peak"] = max(pos.get("peak", 0), bar_h)
                trail = pos["peak"] - ATR_TRAIL_MULT * pos["atr"]
                if bar_l <= trail:
                    _bt_leg(pos, trail, 1.0)
                    pos["close_ts"] = ct[i]; positions.append(pos); pos = None
                    continue
                if bar_l <= pos["sl"]:
                    _bt_leg(pos, pos["sl"], 1.0)
                    pos["close_ts"] = ct[i]; positions.append(pos); pos = None
        if pos is None:
            if len(oi_vals) < 5:
                if diag is not None: diag["no_oi"] += 1
                continue
            if diag is not None: diag["evals"] += 1
            ok, d = detect_signal(o[i+1-W:i+1], h[i+1-W:i+1], l[i+1-W:i+1],
                                   c[i+1-W:i+1], v[i+1-W:i+1], tb[i+1-W:i+1], oi_vals[-8:])
            if ok:
                if diag is not None: diag["signals"] += 1
                qty = NOTIONAL / d["entry"]
                pos = dict(sym=sym, entry=d["entry"], sl=d["sl"], tp1=d["tp1"], tp2=d["tp2"],
                           atr=d["atr"], qty=qty, qty_init=qty, fee_in=NOTIONAL * FEE_MAKER,
                           half=False, peak=0.0, pnl=0.0, open_ts=ct[i])
            elif diag is not None:
                lbl = _bt_reason(d)
                diag["reasons"][lbl] = diag["reasons"].get(lbl, 0) + 1
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


# ==============================================================
# GRID SEARCH: перебор комбинаций параметров стратегии по
# заранее закешированным историческим данным (без повторных
# сетевых запросов на каждую комбинацию — иначе перебор из
# 50-100 комбинаций растянулся бы на часы). Данные (klines+OI)
# по каждой монете скачиваются ОДИН раз, а detect_signal с
# разными порогами прогоняется по ним в памяти много раз.
# ==============================================================

import itertools
import numpy as np
import pandas as pd

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MPL_GRID = True
except Exception:
    HAS_MPL_GRID = False

# Доп. алиасы параметров для grid_search (сверх тех, что уже есть в BT_PARAMS):
# volume_threshold -> тот же VOL_SPIKE_MIN, что и "spike"
# break_lookback    -> LEVEL_LOOKBACK (глубина поиска уровня пробоя)
# oi_strength       -> тот же OI_MIN_GROW, что и "oi"
BT_PARAMS["volume_threshold"] = ("VOL_SPIKE_MIN", lambda s: float(s))
BT_PARAMS["break_lookback"] = ("LEVEL_LOOKBACK", lambda s: int(float(s)))
BT_PARAMS["oi_strength"] = ("OI_MIN_GROW", lambda s: float(s))

def _bt_prefetch(days, ncoins):
    """Строит momentum-вселенную и один раз скачивает klines+OI по каждой монете.
    Возвращает список (sym, o,h,l,c,v,tb,ct, oi_ts) для многократного переиспользования
    без повторных сетевых запросов на каждую комбинацию параметров."""
    coins = bt_build_universe(days, ncoins)
    cached = []
    for sym in coins:
        try:
            o, h, l, c, v, tb, ct = bt_klines(sym, days)
            if len(c) < LEVEL_LOOKBACK + 60:
                continue
            oi_ts = bt_oi(sym, days)
            if len(oi_ts) < 20:
                continue
            cached.append((sym, o, h, l, c, v, tb, ct, oi_ts))
        except Exception as e:
            print(f"grid prefetch {sym} err:", e)
    return cached

def _bt_run_once(config, cached, deposit):
    """Прогоняет ОДНУ комбинацию параметров (config) по уже закешированным данным.
    Возвращает (signals, trades, pf, ret, dd) — сигнатура, которую ожидает
    пользовательский grid_search()."""
    applied, saved = _bt_apply_overrides(config)
    try:
        all_pos = []
        diag = dict(evals=0, no_oi=0, signals=0, reasons={})
        for sym, o, h, l, c, v, tb, ct, oi_ts in cached:
            all_pos += bt_simulate_coin(
                sym, o[1:], h[1:], l[1:], c[1:], v[1:], tb[1:], ct[1:],
                oi_ts, diag=diag
            )
        taken, eq = bt_portfolio(all_pos, deposit)
        n = len(taken)
        wins = [t for t in taken if t["pnl"] > 0]
        losses = [t for t in taken if t["pnl"] <= 0]
        gross_win = sum(t["pnl"] for t in wins)
        gross_loss = abs(sum(t["pnl"] for t in losses))
        pf = (gross_win / gross_loss) if gross_loss > 0 else float("inf")
        ret = (eq[-1] / deposit - 1.0) if eq else 0.0
        peak = deposit; dd = 0.0
        for x in eq:
            peak = max(peak, x)
            dd = max(dd, (peak - x) / peak if peak > 0 else 0.0)
        return diag["signals"], n, pf, ret, dd
    finally:
        _bt_restore(saved)

def grid_search(run_backtest_fn, param_grid):
    """Ваша функция — сигнатура и логика без изменений."""
    keys = list(param_grid.keys())
    combinations = list(itertools.product(*param_grid.values()))

    results = []

    for values in combinations:
        config = dict(zip(keys, values))

        signals, trades, pf, ret, dd = run_backtest_fn(config)

        results.append({
            **config,
            "signals": signals,
            "trades": trades,
            "pf": pf,
            "return": ret,
            "drawdown": dd
        })

        print(f"\u2705 {config} \u2192 PF={pf:.2f}, trades={trades}")

    return pd.DataFrame(results)

def plot_heatmap(df, x, y, metric="pf", save_path=None):
    """Ваша функция — сигнатура и логика без изменений, кроме plt.show()->savefig,
    т.к. на сервере (Railway) нет дисплея — картинка сохраняется в файл для отправки
    через tg_photo(), plt.show() там просто ничего не сделает."""
    if not HAS_MPL_GRID:
        return None
    pivot = df.pivot_table(
        index=y,
        columns=x,
        values=metric,
        aggfunc="mean"
    )

    plt.figure()
    plt.imshow(pivot, aspect='auto')
    plt.colorbar(label=metric)

    plt.xticks(range(len(pivot.columns)), pivot.columns)
    plt.yticks(range(len(pivot.index)), pivot.index)

    plt.xlabel(x)
    plt.ylabel(y)
    plt.title(f"{metric} heatmap")

    path = save_path or os.path.join(DATA_DIR, f"heatmap_{x}_{y}_{metric}.png")
    plt.savefig(path, dpi=110, bbox_inches="tight")
    plt.close()
    return path

def stability_score(df, group_cols, metric="pf"):
    """Ваша функция — без изменений: устойчивость = среднее / (std + eps),
    высокий скор = комбинация стабильно хорошая, а не разово удачная."""
    grouped = df.groupby(group_cols)[metric]

    stability = grouped.mean() / (grouped.std() + 1e-6)

    return stability.sort_values(ascending=False)

def run_grid_search_telegram(chat, days=14, ncoins=30, param_grid=None):
    """Обёртка для запуска через Telegram: /gridsearch [days] [ncoins].
    Скачивает данные один раз, перебирает сетку параметров (ваш grid_search()),
    строит хитмапы, считает stability_score, фильтрует по trades/pf/drawdown,
    сохраняет CSV и шлёт итог + картинки обратно в чат."""
    if BT_RUNNING["on"]:
        tg_send(chat, "\u23F3 Бэктест/грид уже идёт — дождись окончания."); return
    BT_RUNNING["on"] = True
    try:
        if param_grid is None:
            param_grid = {
                "volume_threshold": [1.2, 1.5, 2.0, 2.5],
                "break_lookback": [1, 2, 3, 5],
                "oi_strength": [0.5, 0.8, 1.0],
            }
        n_combos = 1
        for v in param_grid.values():
            n_combos *= len(v)
        tg_send(chat, f"\U0001F52C Grid search: {days} дн \u00d7 до {ncoins} монет, "
                      f"{n_combos} комбинаций параметров ({', '.join(param_grid.keys())}). "
                      f"Скачиваю данные один раз...")
        cached = _bt_prefetch(days, ncoins)
        if not cached:
            tg_send(chat, "\U0001F4ED Momentum-вселенная пуста за этот период — увеличь days/ncoins.")
            return
        tg_send(chat, f"\u2705 Данных по {len(cached)} монетам. Прогоняю {n_combos} комбинаций (это может занять несколько минут)...")

        run_backtest_fn = lambda cfg: _bt_run_once(cfg, cached, DEPOSIT)
        df = grid_search(run_backtest_fn, param_grid)

        # Полный необработанный результат — сохраняем сразу, до фильтров
        raw_csv = os.path.join(DATA_DIR, "grid_results.csv")
        df.to_csv(raw_csv, index=False)

        keys = list(param_grid.keys())
        x_param, y_param = keys[0], keys[1] if len(keys) > 1 else keys[0]

        heatmap_paths = []
        p1 = plot_heatmap(df, x_param, y_param, "pf")
        if p1: heatmap_paths.append((p1, f"PF heatmap: {x_param} \u00d7 {y_param}"))
        p2 = plot_heatmap(df, x_param, y_param, "return")
        if p2: heatmap_paths.append((p2, f"Return heatmap: {x_param} \u00d7 {y_param}"))

        stability = stability_score(df, keys, metric="pf")

        # Фильтр по надёжности: достаточно сделок, PF>1.2, просадка не критичная (<30%)
        df_filtered = df[(df["trades"] > 30) & (df["pf"] > 1.2) & (df["drawdown"] < 0.3)]

        lines = [f"\U0001F3C6 Grid search готов: {n_combos} комбинаций \u00d7 {len(cached)} монет.",
                 f"Прошли фильтр (trades>30, PF>1.2, dd<30%): {len(df_filtered)} из {len(df)}.",
                 "", "\U0001F4CA Топ-10 по стабильности (mean(PF)/std(PF)):"]
        for idx, val in stability.head(10).items():
            key_str = idx if isinstance(idx, str) else ", ".join(f"{k}={v}" for k, v in zip(keys, idx if isinstance(idx, tuple) else [idx]))
            lines.append(f"\u2022 {key_str} \u2192 стабильность={val:.2f}")

        if len(df_filtered) > 0:
            best = df_filtered.sort_values("pf", ascending=False).iloc[0]
            best_str = ", ".join(f"{k}={best[k]}" for k in keys)
            lines.append("")
            lines.append(f"\U0001F947 Лучшая надёжная комбинация: {best_str}")
            lines.append(f"PF={best['pf']:.2f} \u00b7 trades={int(best['trades'])} \u00b7 "
                          f"return={best['return']*100:.1f}% \u00b7 dd={best['drawdown']*100:.1f}%")
            filtered_csv = os.path.join(DATA_DIR, "grid_results_filtered.csv")
            df_filtered.to_csv(filtered_csv, index=False)

        tg_send(chat, "\n".join(lines))
        for path, caption in heatmap_paths:
            tg_photo(chat, path, caption)
    except Exception as e:
        tg_send(chat, f"\u26A0\uFE0F grid search err: {e}")
    finally:
        BT_RUNNING["on"] = False


# ==============================================================
# DEMO / SANITY-CHECK МОДУЛЬ НА СИНТЕТИЧЕСКИХ ДАННЫХ
# Внимание: этот блок работает на случайных ценах (np.random),
# а не на реальных котировках Binance/Bybit — он НЕ связан с
# живым detect_signal и НЕ участвует в реальной торговле бота.
# Его смысл — быстрая проверка логики grid_search/heatmap/
# stability_score на синтетике перед тем, как гонять их на
# реальных исторических данных через /gridsearch.
# Все функции даны как есть, но с префиксом demo_, чтобы не
# конфликтовать с уже существующими в файле grid_search(),
# plot_heatmap(), stability_score() — у них другие сигнатуры
# (работают с реальными run_backtest_fn/monetary PF), и простое
# совпадение имён привело бы к перезаписи рабочих версий более
# новыми определениями ниже в файле — тогда команда /gridsearch
# сломалась бы (TypeError: grid_search() missing 1 required
# positional argument, т.к. demo-версия принимает только 1 аргумент).
# ==============================================================

def demo_backtest(prices, signals):
    df = pd.DataFrame({
        "price": prices,
        "signal": signals
    })

    df["returns"] = df["price"].pct_change().fillna(0)
    df["strategy"] = df["returns"] * df["signal"].shift(1).fillna(0)
    df["equity"] = (1 + df["strategy"]).cumprod()

    return df


def demo_compute_metrics(df):
    total_return = df["equity"].iloc[-1] - 1

    wins = (df["strategy"] > 0).sum()
    losses = (df["strategy"] < 0).sum()
    winrate = wins / (wins + losses) if (wins + losses) else 0

    cum_max = df["equity"].cummax()
    drawdown = (df["equity"] - cum_max) / cum_max
    max_dd = drawdown.min()

    sharpe = df["strategy"].mean() / (df["strategy"].std() + 1e-9)

    return total_return, winrate, max_dd, sharpe


def demo_generate_signals(prices, config):
    df = pd.DataFrame({"price": prices})
    df["ret"] = df["price"].pct_change()

    lb = config["break_lookback"]

    df["roll_high"] = df["price"].rolling(lb).max()

    cond = (
        (df["price"] > df["roll_high"].shift(1)) &
        (df["ret"] > 0)
    )

    df["signal"] = 0
    df.loc[cond, "signal"] = 1
    df["signal"] = df["signal"].replace(0, method="ffill").fillna(0)

    return df["signal"]


def demo_extract_trades(df):
    trades = []
    pos = 0
    entry_price = 0

    for i in range(1, len(df)):
        sig = df["signal"].iloc[i]
        price = df["price"].iloc[i]

        if pos == 0 and sig == 1:
            pos = 1
            entry_price = price

        elif pos == 1 and sig == 0:
            pnl = price / entry_price - 1
            trades.append(pnl)
            pos = 0

    return np.array(trades)


def demo_trade_stats(trades):
    if len(trades) == 0:
        return 0, 0, 0

    wins = trades[trades > 0]
    losses = trades[trades <= 0]

    winrate = len(wins) / len(trades)
    pf = abs(wins.sum() / (losses.sum() + 1e-9))

    return winrate, pf, len(trades)


def demo_run_backtest_fn(config):
    np.random.seed(42)
    prices = pd.Series(np.cumprod(1 + np.random.normal(0, 0.01, 1000)))

    signals = demo_generate_signals(prices, config)
    df = demo_backtest(prices, signals)

    ret, winrate, dd, sharpe = demo_compute_metrics(df)

    trades_arr = demo_extract_trades(df)
    winrate_t, pf, trades = demo_trade_stats(trades_arr)

    return {
        "signals": int((df["signal"].diff() != 0).sum()),
        "trades": trades,
        "pf": pf,
        "return": ret,
        "dd": dd
    }


def demo_grid_search(param_grid):
    keys = list(param_grid.keys())
    combos = list(itertools.product(*param_grid.values()))

    results = []

    for values in combos:
        config = dict(zip(keys, values))

        res = demo_run_backtest_fn(config)

        results.append({**config, **res})

        print(config, "-> PF:", round(res["pf"], 2), "trades:", res["trades"])

    return pd.DataFrame(results)


def demo_plot_heatmap(df, x, y, metric="pf", save_path=None):
    """plt.show() заменён на savefig — на Railway нет дисплея, картинка нужна как файл."""
    pivot = df.pivot_table(index=y, columns=x, values=metric)

    plt.figure()
    plt.imshow(pivot, aspect='auto')
    plt.colorbar()
    plt.xticks(range(len(pivot.columns)), pivot.columns)
    plt.yticks(range(len(pivot.index)), pivot.index)
    plt.title(metric)
    plt.xlabel(x)
    plt.ylabel(y)
    path = save_path or os.path.join(DATA_DIR, f"demo_heatmap_{x}_{y}_{metric}.png")
    plt.savefig(path, dpi=110, bbox_inches="tight")
    plt.close()
    return path


def demo_stability_score(df, cols):
    g = df.groupby(cols)["pf"]
    return (g.mean() / (g.std() + 1e-6)).sort_values(ascending=False)


def demo_auto_optimize(base_config):
    config = base_config.copy()

    steps = [
        ("break_lookback", [1, 2, 3, 5]),
    ]

    best = None

    for name, values in steps:
        print(f"\n\U0001F527 optimizing {name}")

        for v in values:
            config[name] = v
            res = demo_run_backtest_fn(config)

            print(name, v, res)

            if best is None or res["pf"] > best["pf"]:
                best = {**config, **res}

    return best


def demo_selfcheck():
    """Запуск синтетической проверки — эквивалент вашего if __name__ блока,
    но как вызываемая функция (не выполняется автоматически при импорте файла).
    Можно вызвать вручную из консоли Railway или временно из main() для проверки,
    что grid_search/heatmap/stability_score логически работают корректно."""
    base_config = {"break_lookback": 1}

    print("\n\U0001F680 AUTO OPTIMIZATION (synthetic)")
    best = demo_auto_optimize(base_config)
    print("\nBEST:", best)

    print("\n\U0001F4CA GRID SEARCH (synthetic)")
    param_grid = {"break_lookback": [1, 2, 3, 5]}

    df = demo_grid_search(param_grid)
    df = df[(df["trades"] > 5) & (df["pf"] > 1)]

    print("\nTOP:")
    print(df.sort_values("pf", ascending=False).head())

    print("\n\U0001F525 HEATMAP (synthetic)")
    hm_path = demo_plot_heatmap(df, "break_lookback", "break_lookback") if len(df) else None

    print("\n\U0001F9E0 STABILITY (synthetic)")
    stab = demo_stability_score(df, ["break_lookback"]) if len(df) else None
    print(stab.head() if stab is not None else "no data")

    return best, df, hm_path, stab


def run_selfcheck_telegram(chat):
    """Обёртка demo_selfcheck() для команды /selfcheck — прогоняет синтетическую
    проверку логики grid_search/heatmap/stability на случайных данных (без сети,
    без затрагивания реального detect_signal и живых позиций) и шлёт итог в чат."""
    try:
        tg_send(chat, "\U0001F9EA Synthetic self-check: проверяю grid_search/heatmap/stability на случайных данных (без сети)...")
        best, df, hm_path, stab = demo_selfcheck()
        lines = [f"\u2705 Self-check пройден. Комбинаций: {len(df)}.",
                  f"Лучшая (синтетика): {best}"]
        if stab is not None and len(stab):
            lines.append("Stability top: " + ", ".join(f"{k}={v:.2f}" for k, v in stab.head(3).items()))
        tg_send(chat, "\n".join(lines))
        if hm_path:
            tg_photo(chat, hm_path, "Synthetic heatmap (self-check)")
    except Exception as e:
        tg_send(chat, f"\u26A0\uFE0F self-check err: {e}")


# ==============================================================
# LIVE-DATA BACKTEST FN: та же идея, что и demo_run_backtest_fn,
# но данные берутся из РЕАЛЬНЫХ котировок Binance (klines + OI),
# а не из синтетики. Переписано с requests+pandas.merge_asof на
# http_json() (уже есть в файле, urllib-based) — чтобы не тащить
# в requirements.txt ещё одну HTTP-библиотеку (requests) при
# наличии готовой инфраструктуры запросов. Названия функций с
# префиксом live_, чтобы не конфликтовать с demo_* и с реальным
# run_backtest()/bt_klines()/bt_oi(), которые работают с MOMENTUM-
# вселенной и учитывают лимит слотов портфеля — этот блок проще:
# считает метрики по ОДНОЙ монете за раз, без портфельных лимитов.
# ==============================================================

def live_get_klines(symbol="BTCUSDT", interval="1h", limit=500):
    url = f"{BINANCE}/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
    data = http_json(url)

    df = pd.DataFrame(data, columns=[
        "time", "open", "high", "low", "close", "volume",
        "close_time", "qav", "trades", "taker_base", "taker_quote", "ignore"
    ])
    df["time"] = pd.to_datetime(df["time"], unit="ms")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)

    return df[["time", "open", "high", "low", "close", "volume"]]

def live_get_oi(symbol="BTCUSDT", interval="5m", limit=500):
    url = f"{BINANCE}/futures/data/openInterestHist?symbol={symbol}&period={interval}&limit={limit}"
    data = http_json(url)

    df = pd.DataFrame(data)
    df["time"] = pd.to_datetime(df["timestamp"], unit="ms")
    df["oi"] = df["sumOpenInterest"].astype(float)

    return df[["time", "oi"]]

def live_load_market_data(symbol="BTCUSDT"):
    df_price = live_get_klines(symbol)
    df_oi = live_get_oi(symbol)

    df = pd.merge_asof(
        df_price.sort_values("time"),
        df_oi.sort_values("time"),
        on="time"
    )
    df["oi"] = df["oi"].ffill()  # fillna(method=) устарел в новых pandas

    return df

def live_generate_signals(df, config):
    df = df.copy()
    df["ret"] = df["close"].pct_change()

    df["vol_ma"] = df["volume"].rolling(20).mean()
    df["vol_spike"] = df["volume"] / df["vol_ma"]

    df["oi_delta"] = df["oi"].pct_change()

    lb = config["break_lookback"]
    df["high_roll"] = df["high"].rolling(lb).max()

    signal = (
        (df["vol_spike"] > config["volume_threshold"]) &
        (df["oi_delta"] > config["oi_threshold"]) &
        (df["close"] > df["high_roll"].shift(1))
    )

    df["signal"] = 0
    df.loc[signal, "signal"] = 1
    df["signal"] = df["signal"].mask(df["signal"] == 0).ffill().fillna(0)  # replace(method=) устарел

    return df

def live_run_backtest_fn(config, symbol="BTCUSDT"):
    """Метрики стратегии по ОДНОЙ реальной монете за раз (для сравнения между
    символами — см. live_multi_symbol_report ниже). Использует demo_backtest/
    demo_compute_metrics (та же математика equity/PF/Sharpe), но на живых данных."""
    df = live_load_market_data(symbol)
    df = live_generate_signals(df, config)

    prices = df["close"]
    bt = demo_backtest(prices, df["signal"])
    ret, winrate, dd, sharpe = demo_compute_metrics(bt)

    trades = int((df["signal"].diff() != 0).sum())

    pos_sum = bt.loc[bt["strategy"] > 0, "strategy"].sum()
    neg_sum = bt.loc[bt["strategy"] < 0, "strategy"].sum()
    pf = abs(pos_sum / (neg_sum + 1e-9))

    return {
        "signals": trades,
        "trades": trades,
        "pf": pf,
        "return": ret,
        "dd": dd,
        "winrate": winrate,
        "sharpe": sharpe,
    }

def live_multi_symbol_report(chat, config=None, symbols=None):
    """Команда /crosscheck — прогоняет один и тот же конфиг сигналов по нескольким
    символам сразу (BTC/ETH/SOL/BNB по умолчанию) на реальных данных Binance и
    присылает сравнительную таблицу PF/return/winrate/Sharpe по каждой монете."""
    if config is None:
        config = {"break_lookback": 3, "volume_threshold": 1.5, "oi_threshold": 0.001}
    if symbols is None:
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]

    results = []
    for s in symbols:
        try:
            res = live_run_backtest_fn(config, s)
            res["symbol"] = s
            results.append(res)
        except Exception as e:
            print(f"live_multi_symbol_report {s} err:", e)
            tg_send(chat, f"\u26A0\uFE0F {s}: err {e}")

    if not results:
        tg_send(chat, "\U0001F4ED Не удалось получить данные ни по одной монете.")
        return None

    df = pd.DataFrame(results)
    csv_path = os.path.join(DATA_DIR, "cross_symbol_report.csv")
    df.to_csv(csv_path, index=False)

    lines = [f"\U0001F310 Cross-symbol check: {config}", ""]
    for _, row in df.iterrows():
        lines.append(f"\u2022 {row['symbol']}: PF={row['pf']:.2f} \u00b7 trades={row['trades']} \u00b7 "
                      f"return={row['return']*100:.1f}% \u00b7 winrate={row['winrate']*100:.0f}% \u00b7 "
                      f"dd={row['dd']*100:.1f}% \u00b7 sharpe={row['sharpe']:.2f}")
    tg_send(chat, "\n".join(lines))
    return df

def run_crosscheck_telegram(chat):
    try:
        tg_send(chat, "\U0001F310 Прогоняю сигналы по BTC/ETH/SOL/BNB на реальных данных Binance...")
        live_multi_symbol_report(chat)
    except Exception as e:
        tg_send(chat, f"\u26A0\uFE0F crosscheck err: {e}")

def run_backtest(chat, days=14, ncoins=30, overrides=None):
    if BT_RUNNING["on"]:
        tg_send(chat, "\u23F3 Бэктест уже идёт — дождись окончания."); return
    BT_RUNNING["on"] = True
    applied, saved = _bt_apply_overrides(overrides)
    try:
        days = max(3, min(days, 30)); ncoins = max(5, min(ncoins, 60))
        tg_send(chat, f"\U0001F9EA Бэктест запущен: {days} дн \u00d7 до {ncoins} momentum-монет (по историческому росту OI/объёма).\n"
                      f"\u2699\uFE0F Параметры: {_ov_str(applied)}\n"
                      f"Живой скан на паузе до конца бэктеста (чтобы временные параметры не протекли).\n"
                      f"Займёт несколько минут — пришлю прогресс и итог с графиком.")
        tg_send(chat, f"\U0001F52C Строю momentum-вселенную на ЧАСОВЫХ исторических данных за {days} дн (объём/OI), это может занять минуту...")
        coins = bt_build_universe(days, ncoins)
        if not coins:
            tg_send(chat, "\U0001F4ED За этот период ни одна монета не прошла momentum-фильтр (рост OI/объёма/цены) — попробуй увеличить период или ослабить UNIV_MIN_* пороги.")
            return
        all_pos = []
        diag = dict(evals=0, no_oi=0, signals=0, reasons={})
        for k, sym in enumerate(coins, 1):
            try:
                o, h, l, c, v, tb, ct = bt_klines(sym, days)
                if len(c) < LEVEL_LOOKBACK + 60: continue
                oi_ts = bt_oi(sym, days)
                if len(oi_ts) < 20: continue
                all_pos += bt_simulate_coin(sym, o[:-1], h[:-1], l[:-1], c[:-1],
                                             v[:-1], tb[:-1], ct[:-1], oi_ts, diag=diag)
            except Exception as e:
                print(f"bt {sym} err:", e)
            if k % 10 == 0:
                tg_send(chat, f"\u2699\uFE0F Бэктест: {k}/{len(coins)} монет \u00b7 сигналов {diag['signals']} \u00b7 исполнено {len(all_pos)}")

        def _funnel_text():
            if not diag["evals"] and not diag["no_oi"]: return None
            top = sorted(diag["reasons"].items(), key=lambda x: -x[1])[:8]
            L = [f"\U0001F52C ВОРОНКА ОТСЕВА — что рубит чек-лист чаще всего "
                 f"(проверок: {diag['evals']:,}, сигналов: {diag['signals']}):"]
            for name, cnt in top:
                L.append(f"\u2022 {name}: {cnt:,} ({cnt/max(diag['evals'],1)*100:.1f}%)")
            if diag["no_oi"]:
                L.append(f"\u2022 нет OI-истории (оценка пропущена): {diag['no_oi']:,}")
            L.append("Если сигналов слишком мало — ослабляем ВЕРХНЕЕ условие воронки, по данным, а не наугад.")
            return "\n".join(L)

        taken, eq = bt_portfolio(all_pos, DEPOSIT)
        if not taken:
            tg_send(chat, f"\U0001F4ED Бэктест [{_ov_str(applied)}]: за {days} дн по {len(coins)} монетам "
                          f"чек-лист не дал ни одной сделки (сигналов было: {diag['signals']}, "
                          f"исполнилось на ретесте: 0). Смотри воронку ниже — она скажет, что именно рубит.")
            ft = _funnel_text()
            if ft: tg_send(chat, ft)
            return
        n = len(taken); wins = sum(1 for t in taken if t["pnl"] > 0)
        total = eq[-1] - DEPOSIT
        peak = DEPOSIT; dd = 0.0
        for x in eq:
            peak = max(peak, x); dd = max(dd, (peak - x) / peak)
        skipped = len(all_pos) - n
        txt = (f"\U0001F9EA БЭКТЕСТ: {days} дн \u00d7 {len(coins)} монет\n"
               f"\u2699\uFE0F Параметры: {_ov_str(applied)}\n"
               f"Сделок взято: {n} (пропущено из-за 2 слотов: {skipped})\n"
               f"В плюсе: {wins} ({wins/n*100:.0f}%)\n"
               f"Итог: {total:+.2f}$ ({total/DEPOSIT*100:+.1f}% депо) \u00b7 макс.просадка {dd*100:.1f}%\n"
               f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
               f"\u26A0\uFE0F Комиссии учтены; спред/проскальзывание НЕТ; трейлинг по 15м-барам; "
               f"в спорном баре стоп раньше тейка (пессимизм). Это ориентир на малой выборке — "
               f"судья по-прежнему форвардный paper (/stats).")
        tg_send(chat, txt)
        ft = _funnel_text()
        if ft: tg_send(chat, ft)
        if HAS_MPL:
            try:
                plt.figure(figsize=(10, 5))
                plt.plot(eq, linewidth=1.6)
                plt.title(f"EVA v4 · эквити бэктеста ({days} дн, {len(coins)} монет, {n} сделок)")
                plt.xlabel("Сделки"); plt.ylabel("Капитал $"); plt.grid(True, alpha=0.4)
                p = "/tmp/bt_equity.png"
                plt.savefig(p, dpi=110, bbox_inches="tight"); plt.close()
                tg_photo(chat, p, caption="Кривая капитала (paper-математика, с комиссиями)")
            except Exception as e:
                print("bt chart err:", e)
        else:
            tg_send(chat, "\U0001F5BC matplotlib не установлен — график пропущен (добавь в requirements.txt).")
    finally:
        _bt_restore(saved)
        BT_RUNNING["on"] = False

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
                            "\U0001F4D6 Команды EVA v4:\n"
                            "/start — запуск и краткая сводка\n"
                            "/pos — текущая позиция/лимитка: цена, PnL, стадия\n"
                            "/stats — PAPER-статистика: win rate, средний R, PnL $ и %\n"
                            "/debug — почему сигналов нет: топ причин отказа по чек-листу\n"
                            "/backtest [дней] [монет] [ключ=знач ...] — прогон по истории + воронка + график.\n"
                            "   Калибровка порогов (живой бот не трогается): spike= quiet= qbars= wick= atr= oi= rsi=\n"
                            "   ATR-риск (частичная фиксация): slmult= (SL) tp1mult= (TP1 50%) tp2mult= (TP2 50%) trailmult= (трейлинг после TP1)\n"
                            "   Пример: /backtest 30 60 spike=1.5 quiet=2.5 qbars=5 slmult=1.5 tp1mult=2.0 tp2mult=4.5 \u00b7 или пресет: /backtest 30 60 soft\n"
                            "/gridsearch [дней] [монет] — перебор volume_threshold\u00d7break_lookback\u00d7oi_strength, хитмапы PF+return, stability score, CSV\n"
                            "/selfcheck — синтетическая проверка grid_search/heatmap/stability (без сети, без реальных данных)\n"
                            "/crosscheck — сравнение сигналов по BTC/ETH/SOL/BNB на реальных данных (PF/return/winrate/sharpe)\n"
                            "/pause — пауза (новые сигналы не ищутся, позиция ведётся)\n"
                            "/resume — возобновить сканирование\n"
                            "/help — эта справка")
                elif text.startswith("/start"):
                    st["paused"] = False; save_state(st)
                    tg_send(cid, "\U0001F916 EVA v4 — импульсный бот (PAPER)\n"
                                 "Данные: Binance \u00b7 Цены: Bybit \u00b7 Исполнение: виртуальное с честным учётом\n"
                                 f"Лимиты: до {MAX_CONCURRENT} позиций одновременно (скользящие слоты) \u00b7 "
                                 f"{'дневной предохранитель ' + str(MAX_DAILY_TRADES) if MAX_DAILY_TRADES>0 else 'без дневного лимита'} \u00b7 "
                                 f"Объём ${NOTIONAL:.0f} (маржа {MARGIN:.0f}$ x{LEVERAGE:.0f})\n"
                                 "Команды: /pos \u00b7 /stats \u00b7 /backtest \u00b7 /pause \u00b7 /resume \u00b7 /help")
                elif text.startswith("/pause"):
                    st["paused"] = True; save_state(st)
                    tg_send(cid, "\u23F8 Пауза: новые сигналы не ищу (открытая позиция ведётся).")
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
                    bd, bc, ov = _parse_bt_args(text)
                    threading.Thread(target=run_backtest, args=(cid, bd, bc, ov), daemon=True).start()
                elif text.startswith("/gridsearch"):
                    bd, bc, _ov = _parse_bt_args(text)
                    threading.Thread(target=run_grid_search_telegram, args=(cid, bd, bc, None), daemon=True).start()
                elif text.startswith("/selfcheck"):
                    threading.Thread(target=run_selfcheck_telegram, args=(cid,), daemon=True).start()
                elif text.startswith("/crosscheck"):
                    threading.Thread(target=run_crosscheck_telegram, args=(cid,), daemon=True).start()
        except Exception as e:
            print("tg_loop err:", e); time.sleep(3)

def main():
    st = load_state()
    chat = load_chat()
    print("EVA v4 запущен (PAPER, без условия 3 зелёных). chat:", "есть" if chat else "нет")
    threading.Thread(target=tg_loop, args=(st,), daemon=True).start()
    threading.Thread(target=prop_loop, daemon=True).start()  # PROP-стратегия: полностью параллельный независимый цикл
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
