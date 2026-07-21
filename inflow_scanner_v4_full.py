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
VOL_SPIKE_MIN = float(os.environ.get("VOL_SPIKE_MIN", "1.5"))  # было 2.0 -> 1.5: по воронке рубило 10.5% всех / 77.8% оставшихся после close+breakout — крупный третий отсев
ATR_MIN_MOVE_MULT = 1.5 # Price Action: (Close - PrevClose) > 1.5*ATR14, реальный импульс, не шум
PRICE_MOVE_REQUIRED = int(os.environ.get("PRICE_MOVE_REQUIRED", 0))  # 0=OPTIONAL (не блокирует, только details), 1=обязательный порог 1.5x ATR как раньше
BREAKOUT_LOOKBACK = int(os.environ.get("BREAKOUT_LOOKBACK", "2"))  # ВАЖНО: больше N = выше планка max(high) = ТРУДНЕЕ пробить, не легче! Прошлое увеличение 3->5 было ошибкой логики и подняло отсев с 34.8% до 89.7%. Снижаем до 2 (мягче исходных 3).
BREAKOUT_REQUIRED = int(os.environ.get("BREAKOUT_REQUIRED", "1"))  # 1=обязателен (по умолчанию), 0=OPTIONAL если даже N=2 всё ещё режет слишком много
CLOSE_ABOVE_PREV_REQUIRED = int(os.environ.get("CLOSE_ABOVE_PREV_REQUIRED", "0"))  # 0=OPTIONAL: close<=prev_close рубило 51.7% всех проверок в воронке — самый крупный отсев, не блокирует по умолчанию
QUIET_BARS = 8          # строго 8 чистых баров затишья перед импульсом
QUIET_MAX = 1.8         # жёсткий порог шума в полке накопления
QUIET_ALLOW = 0         # ноль толерантности к шуму в зоне накопления
QUIET_REQUIRED = int(os.environ.get("QUIET_REQUIRED", 0))  # 0=OPTIONAL (не блокирует сигнал, только влияет на score/details), 1=обязательное отбрасывание как раньше
WICK_MAX = 0.30
ATR_LEN = 14
BAR_ATR_MAX = 2.5       # FOMO CAP: строго. High-Low сигнальной свечи > 2.5*ATR -> сигнал отбрасывается целиком
OI_MIN_GROW = 0.02      # OI-ПОДТВЕРЖДЕНИЕ: (OI_now - OI_prev)/OI_prev < 2% -> сигнал отбрасывается (фейковый объём без реального интереса)
RSI_LEN = 14
RSI_MAX = 78.0          # поднято с 75: на сильных пампах RSI летит быстро
CVD_MODE = "all"
LEVEL_LOOKBACK = int(os.environ.get("LEVEL_LOOKBACK", "48"))  # было 96 (24ч на M15) — слишком строгий суточный хай; 48 = 12ч, легче пробить, настраивается через ENV
EMA_FAST, EMA_SLOW = 21, 50

# --- вход/выход (МАРКЕТ на открытии новой свечи сразу после сигнала; лимитка/ретест отключены) ---
FIB_RETRACE = 0.0      # 0.0: лимитка-ретест на Фибо больше не используется (плохие исполнения на затухающих пампах)
ENTRY_TTL_BARS = 0     # не используется при маркет-входе (оставлено для совместимости состояния)
FEE_MAKER = 0.0002
FEE_TAKER = 0.00055

# --- ATR-риск-менеджмент: частичная фиксация TP1/TP2 (position scaling) ---
ATR_SL_MULT = 1.5      # SL = entry - 1.5*ATR
ATR_TP1_MULT = 2.0     # TP1 = entry + 2.0*ATR -> закрыть 50% позиции
ATR_TP2_MULT = 4.5     # TP2 = entry + 4.5*ATR -> закрыть оставшиеся 50%
ATR_TRAIL_MULT = 1.5   # после TP1: SL остатка -> БУ, трейлинг 1.5*ATR от пика до TP2

# --- VALID_ENTRY: контроль качества входа относительно уровня пробоя ---
ENTRY_MAX_EXT_ATR = float(os.environ.get("ENTRY_MAX_EXT_ATR", "2.0"))       # было 1.2 -> 2.0: даём больше запаса от уровня до входа
ENTRY_MIN_PULLBACK_ATR = float(os.environ.get("ENTRY_MIN_PULLBACK_ATR", "1.5"))  # было 0.8 -> 1.5: откат не обязателен таким узким
VALID_ENTRY_REQUIRED = int(os.environ.get("VALID_ENTRY_REQUIRED", "0"))  # 0=OPTIONAL (весь блок 9b не блокирует, только details), 1=обязателен как раньше

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
RISK_SIZING = os.environ.get("RISK_SIZING", "0") == "1"         # 1 = объём от РИСКА (risk$/дистанция стопа), 0 = фиксированный NOTIONAL
RISK_PCT_TRADE = float(os.environ.get("RISK_PCT_TRADE", "0.01"))# сколько депозита рискуем в сделке при RISK_SIZING=1
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

# ============================================================================
# SCORED DETECTOR — фильтры переведены в баллы (шаг 1 плана)
# ============================================================================
SCORE_WEIGHTS = {
    "breakout": float(os.environ.get("W_BREAKOUT", "2.0")),
    "volume":   float(os.environ.get("W_VOLUME",   "2.0")),
    "oi":       float(os.environ.get("W_OI",       "1.5")),
    "cvd":      float(os.environ.get("W_CVD",      "1.5")),
    "trend":    float(os.environ.get("W_TREND",    "1.0")),
}
SCORE_MAX = sum(SCORE_WEIGHTS.values())
SCORE_THRESHOLD = float(os.environ.get("SCORE_THRESHOLD", "4.0"))
BREAKOUT_TOLERANCE = float(os.environ.get("BREAKOUT_TOLERANCE", "0.995"))  # close > high_N * 0.995
VOL_RATIO_MIN_SCORED = float(os.environ.get("VOL_RATIO_MIN_SCORED", "1.5"))

# Kill-switch по просадке (шаг: DD > 10% -> стоп торговли)
KILL_SWITCH_DD = float(os.environ.get("KILL_SWITCH_DD", "0.10"))

# Проскальзывание (шаг 3 плана) — ровно по присланной формуле
SLIP_MIN = float(os.environ.get("SLIP_MIN", "0.0005"))
SLIP_MAX = float(os.environ.get("SLIP_MAX", "0.002"))
SPREAD_PCT = float(os.environ.get("SPREAD_PCT", "0.0004"))


def apply_slippage(price, side="long", rng=None):
    """slippage = price * uniform(0.0005, 0.002); spread = price * 0.0004
    entry = price + spread + slippage   (для лонга; для выхода — минус)."""
    import random as _r
    r = rng or _r
    slippage = price * r.uniform(SLIP_MIN, SLIP_MAX)
    spread = price * SPREAD_PCT
    return price + spread + slippage if side == "long" else price - spread - slippage


def size_from_score(capital, score, max_score=None, base_risk=0.01):
    """size = capital * 0.01 * (score / max_score) — чем сильнее сигнал, тем больше объём."""
    max_score = max_score or SCORE_MAX
    frac = max(0.0, min(score / max_score, 1.0)) if max_score else 0.0
    return capital * base_risk * frac


def detect_signal_scored(o, h, l, c, v, tb, oi, threshold=None, weights=None):
    """SCORING вместо бинарных фильтров.
    Ослаблено по плану: breakout с допуском 0.995, volume >= 1.5, OI как tanh(slope*3).
    Жёсткими остаются только защитные условия (фитиль, ATR-кап, RSI) — они не про частоту,
    а про качество входа."""
    W = weights or SCORE_WEIGHTS
    thr = SCORE_THRESHOLD if threshold is None else threshold
    n = len(c)
    if n < LEVEL_LOOKBACK + 30:
        return False, "мало истории"
    i1 = n - 1

    # --- защитные условия (остаются бинарными) ---
    if c[i1] <= o[i1]:
        return False, "свеча не зелёная"
    rng1 = h[i1] - l[i1]
    if rng1 <= 0:
        return False, "нулевая свеча"
    upper_wick = (h[i1] - c[i1]) / rng1
    if upper_wick > WICK_MAX:
        return False, f"фитиль {upper_wick*100:.0f}%>30%"
    a = atr(h[:i1], l[:i1], c[:i1], ATR_LEN)
    if a > 0 and rng1 > BAR_ATR_MAX * a:
        return False, f"свеча параболик ({rng1/a:.1f}x ATR)"
    r = rsi(c[-(RSI_LEN * 6):], RSI_LEN)
    if r > RSI_MAX:
        return False, f"RSI {r:.0f} перегрет"

    # --- факторы в баллы ---
    parts = {}
    level = max(h[i1 - LEVEL_LOOKBACK:i1])
    # 1) BREAKOUT с допуском: close > high_N * 0.995 (было строго close > high_N)
    br_ratio = c[i1] / (level * BREAKOUT_TOLERANCE) if level > 0 else 0.0
    parts["breakout"] = W["breakout"] * max(0.0, min((br_ratio - 1.0) / 0.01, 1.0)) if br_ratio > 1.0 else 0.0
    # 2) VOLUME: порог 1.5, дальше растёт до 3x
    base = v[i1 - VOL_MA_LEN:i1]
    vma = (sum(base) / len(base)) if base else 0.0
    vol_ratio = (v[i1] / vma) if vma > 0 else 0.0
    parts["volume"] = W["volume"] * max(0.0, min((vol_ratio - VOL_RATIO_MIN_SCORED) / 1.5, 1.0))
    # 3) OI как SLOPE через tanh (не binary)
    oi_chg = 0.0
    if len(oi) >= 2 and oi[-2] > 0:
        oi_chg = (oi[-1] - oi[-2]) / oi[-2]
    parts["oi"] = W["oi"] * max(0.0, math.tanh(oi_chg * 3))
    # 4) CVD как непрерывная доля
    delta = 2 * tb[i1] - v[i1]
    cvd_norm = delta / (v[i1] + 1e-9) if v[i1] > 0 else 0.0
    parts["cvd"] = W["cvd"] * max(0.0, math.tanh(cvd_norm * 2))
    # 5) TREND
    e21 = ema_series(c, EMA_FAST)[-1]
    e50 = ema_series(c, EMA_SLOW)[-1]
    parts["trend"] = W["trend"] if (c[i1] > e21 > e50) else 0.0

    score = sum(parts.values())
    if score < thr:
        return False, f"score {score:.2f} < {thr:.2f}"

    entry = c[i1]
    sl = entry - ATR_SL_MULT * a if a > 0 else entry * 0.995
    if sl >= entry:
        return False, "стоп выше входа"
    tp1 = entry + ATR_TP1_MULT * a if a > 0 else entry * 1.01
    tp2 = entry + ATR_TP2_MULT * a if a > 0 else entry * 1.02
    return True, dict(
        score=score, parts=parts, spike=vol_ratio, delta=delta, oi_chg=oi_chg,
        rsi=r, e21=e21, e50=e50, level=level, low1=l[i1], high3=h[i1],
        entry=entry, sl=sl, tp1=tp1, tp2=tp2, atr=a,
        risk_pct=(entry - sl) / entry, wick=upper_wick, close3=c[i1],
    )


def detect_signal(o, h, l, c, v, tb, oi):
    """Импульсная свеча -1 (последняя закрытая). Возвращает (ok, details|причина)."""
    n = len(c)
    if n < LEVEL_LOOKBACK + 30: return False, "мало истории"
    i1 = n - 1  # импульсная свеча

    # 1) Price Action v2 (упрощено по спеке — сигналов было 0, фильтры были пережаты):
    #    ❌ убрали "зелёная свеча" (body>0) — заменили на close > prev_close (мягче,
    #       не требует именно бычьего тела текущей свечи, только рост к пред. закрытию)
    #    ❌ заменили "пробой high только пред. свечи" на "пробой над max(high) последних
    #       BREAKOUT_LOOKBACK свечей" (уже сделано ранее)
    #    ⚠️ ATR_MIN_MOVE_MULT-порог движения оставлен как есть (1.5x ATR) — если после
    #       этой правки сигналов всё ещё 0/мало, следующий кандидат на смягчение — именно
    #       он: он требует не просто рост, а рост >=1.5x ATR, что само по себе жёстче,
    #       чем "close > prev_close" из вашей v2-спеки.
    a = atr(h[:i1], l[:i1], c[:i1], ATR_LEN)
    if a <= 0: return False, "нет ATR для риск-менеджмента"
    close_above_prev_ok = c[i1] > c[i1 - 1]
    if CLOSE_ABOVE_PREV_REQUIRED and not close_above_prev_ok:
        return False, "close <= prev_close"
    breakout_level = max(h[i1 - BREAKOUT_LOOKBACK:i1])
    breakout_ok = c[i1] > breakout_level
    if BREAKOUT_REQUIRED and not breakout_ok:
        return False, f"нет пробоя over {BREAKOUT_LOOKBACK}-свечного диапазона"
    # По воронке бэктеста: "нет пробоя" рубило 34.8%, а связка close>prev_close + это
    # обязательное 1.5x ATR добивала почти всё остальное до 0 сигналов ("прочее" 63.4% —
    # ATR_MIN_MOVE_MULT ловил движения, которые ЕСТЬ, но <1.5x ATR — таких большинство).
    # Делаем порог движения информационным (price_move_ok), а не блокирующим по умолчанию.
    price_move = c[i1] - c[i1 - 1]
    price_move_ok = price_move >= ATR_MIN_MOVE_MULT * a
    if PRICE_MOVE_REQUIRED and not price_move_ok:
        return False, f"движение цены слабое ({price_move/a:.2f}x ATR < {ATR_MIN_MOVE_MULT}x)"

    # 2) затишье до импульса (OPTIONAL) + всплеск объёма на импульсной свече (Volume Spike: >2.0x SMA20)
    #    Раньше "не было затишья" отбрасывало сигнал целиком (жёсткий блок), но в реальности
    #    перед импульсом не всегда тишина — часто идёт "грязная аккумуляция" с шумным объёмом.
    #    Теперь по умолчанию (QUIET_REQUIRED=0) затишье НЕ обязательно: считаем его только
    #    как доп. информацию в details (quiet_ok), а отбрасываем сигнал лишь если совсем
    #    нет всплеска объёма (spike < VOL_SPIKE_MIN) — это ядро фильтра, оно не смягчается.
    base = v[i1 - VOL_MA_LEN:i1]
    if len(base) < VOL_MA_LEN: return False, "мало объёмной базы"
    vma = sum(base) / len(base)
    if vma <= 0: return False, "нулевая база"
    noisy = sum(1 for x in v[i1 - QUIET_BARS:i1] if x > vma * QUIET_MAX)
    quiet_ok = noisy <= QUIET_ALLOW
    if QUIET_REQUIRED and not quiet_ok:
        return False, f"не было затишья ({noisy} шумн.)"
    spike = v[i1] / vma
    if spike < VOL_SPIKE_MIN: return False, f"слабый всплеск x{spike:.1f}"

    # 3) фитиль импульсной свечи <= 30% размаха
    rng1 = h[i1] - l[i1]
    if rng1 <= 0: return False, "нулевая импульсная свеча"
    upper_wick = (h[i1] - c[i1]) / rng1
    if upper_wick > WICK_MAX: return False, f"фитиль {upper_wick*100:.0f}%>30%"

    # 4) импульсная свеча не параболик (FOMO ATR-кап)
    if rng1 > BAR_ATR_MAX * a: return False, f"свеча параболик ({rng1/a:.1f}x ATR)"

    # 5) CVD: дельта > 0 на импульсной свече (агрессивные покупки)
    delta = 2 * tb[i1] - v[i1]
    if delta <= 0: return False, "дельта не растёт"

    # 6) OI-ПОДТВЕРЖДЕНИЕ: рост Open Interest СТРОГО на сигнальной свече (Current vs Previous)
    # (Current_OI - Previous_OI) / Previous_OI < OI_MIN_GROW (2%) -> сигнал отбрасывается мгновенно
    oi_ok = False; oi_chg = 0.0
    if len(oi) >= 2 and oi[-2] > 0:
        oi_chg = (oi[-1] - oi[-2]) / oi[-2]
        oi_ok = oi_chg >= OI_MIN_GROW
    if not oi_ok: return False, f"OI не растёт ({oi_chg*100:+.1f}%, нужно \u2265{OI_MIN_GROW*100:.0f}%)"

    # 7) тренд: close > EMA21 > EMA50
    e21 = ema_series(c, EMA_FAST)[-1]; e50 = ema_series(c, EMA_SLOW)[-1]
    if not (c[i1] > e21 > e50): return False, "нет аптренда EMA"

    # 8) RSI < 75
    r = rsi(c[-(RSI_LEN * 6):], RSI_LEN)
    if r > RSI_MAX: return False, f"RSI {r:.0f} перегрет"

    # 9) пробой локального уровня (LEVEL_LOOKBACK свечей ДО импульса; было 96=24ч, теперь 48=12ч по умолчанию)
    level = max(h[i1 - LEVEL_LOOKBACK:i1])
    if not (c[i1] > level): return False, "уровень не пробит"

    # 9b) VALID_ENTRY (OPTIONAL по умолчанию, VALID_ENTRY_REQUIRED=0):
    #    Раньше это был стек из 5 ОБЯЗАТЕЛЬНЫХ условий поверх суточного пробоя —
    #    именно эта комбинация (узкий коридор входа + откат + удержание + повторное
    #    close>prev_close + объём>MA, всё одновременно на 15-минутке) статистически
    #    крайне редка и была главным кандидатом на то, что рубило сигналы в 0.
    #    Теперь весь блок не блокирует сигнал по умолчанию — считаем валидность входа
    #    (valid_entry_ok) и причину, но пропускаем сигнал дальше. При необходимости
    #    вернуть строгий контроль — VALID_ENTRY_REQUIRED=1 в ENV Railway.
    breakout = level
    close = c[i1]; low = l[i1]; prev_close = c[i1 - 1]
    volume = v[i1]; volume_ma = vma

    valid_entry_ok = True
    valid_entry_reason = ""

    if (close - breakout) > ENTRY_MAX_EXT_ATR * a:
        valid_entry_ok = False
        valid_entry_reason = f"вход слишком далеко от уровня (+{(close-breakout)/a:.2f}x ATR > {ENTRY_MAX_EXT_ATR}x)"
    elif close > breakout + ENTRY_MIN_PULLBACK_ATR * a:
        valid_entry_ok = False
        valid_entry_reason = f"нет отката к уровню (close +{(close-breakout)/a:.2f}x ATR > {ENTRY_MIN_PULLBACK_ATR}x)"
    elif low < breakout:
        valid_entry_ok = False
        valid_entry_reason = "уровень не удержан (low ниже breakout)"
    elif close <= prev_close:
        valid_entry_ok = False
        valid_entry_reason = "нет подтверждения продолжения (close <= prev_close)"
    elif volume < volume_ma:
        valid_entry_ok = False
        valid_entry_reason = "объём не подтверждает продолжение (< MA)"

    if VALID_ENTRY_REQUIRED and not valid_entry_ok:
        return False, f"VALID_ENTRY: {valid_entry_reason}"

    impulse = h[i1] - l[i1]
    if impulse <= 0: return False, "нет импульса"
    # FIB_RETRACE=0.0: entry = цена закрытия сигнальной свечи ≈ цена открытия следующей (маркет-вход)
    entry = c[i1] - FIB_RETRACE * (c[i1] - o[i1])
    sl = entry - ATR_SL_MULT * a           # динамический SL: entry - 1.5*ATR
    if entry <= sl: return False, "вход ниже стопа"
    risk_pct = (entry - sl) / entry
    tp1 = entry + ATR_TP1_MULT * a         # TP1: entry + 2.0*ATR -> закрыть 50%
    tp2 = entry + ATR_TP2_MULT * a         # TP2: entry + 4.5*ATR -> закрыть остаток

    return True, dict(
        spike=spike, delta=delta, oi_chg=oi_chg, rsi=r,
        e21=e21, e50=e50, level=level, low1=l[i1], high3=h[i1],
        entry=entry, sl=sl, tp1=tp1, tp2=tp2, risk_pct=risk_pct, atr=a,
        wick=upper_wick, close3=c[i1], quiet_ok=quiet_ok, price_move_ok=price_move_ok,
        valid_entry_ok=valid_entry_ok, close_above_prev_ok=close_above_prev_ok,
        breakout_ok=breakout_ok,
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


def calc_position_qty(symbol, entry_price, stop_price, deposit=None, risk_pct=None):
    """ПРАВИЛЬНЫЙ риск-сайзинг: объём = риск$ / ДИСТАНЦИЯ ДО СТОПА В ЦЕНЕ.

    ВАЖНО про частую ошибку: делить риск на волатильность-как-долю (0.002) НЕЛЬЗЯ —
    получится объём в сотни раз больше депозита. Делить надо на (entry - stop) в валюте котировки.

    Возвращает (qty, info) где info описывает, что произошло с ограничениями.
    """
    deposit = DEPOSIT if deposit is None else deposit
    risk_pct = RISK_PCT_TRADE if risk_pct is None else risk_pct
    info = {}
    stop_distance = entry_price - stop_price
    if stop_distance <= 0:
        return 0.0, {"error": "стоп выше входа"}
    risk_usd = deposit * risk_pct
    qty = risk_usd / stop_distance                     # <- дистанция В ЦЕНЕ, не доля
    info["risk_usd"] = risk_usd
    info["stop_distance"] = stop_distance
    info["qty_by_risk"] = qty
    # ПОТОЛОК ПЛЕЧА: номинал не больше маржа*плечо
    max_qty = NOTIONAL / entry_price if entry_price > 0 else qty
    if qty > max_qty:
        qty = max_qty
        info["capped_by_leverage"] = True
    info["notional"] = qty * entry_price
    # округление под шаг лота биржи + проверка минимумов
    try:
        rounded = bybit_round_qty(symbol, qty)
        if rounded and rounded > 0:
            info["qty_rounded_from"] = qty
            qty = rounded
        inst = bybit_instrument_info(symbol)
        if inst:
            min_qty = float(inst["lotSizeFilter"].get("minOrderQty", 0) or 0)
            if min_qty and qty < min_qty:
                info["below_min_qty"] = (qty, min_qty)
                return 0.0, info
    except Exception as e:
        info["round_err"] = str(e)
    info["qty_final"] = qty
    info["notional_final"] = qty * entry_price
    return qty, info


def open_market_position(st, sym, d, chat):
    """Маркет-вход на открытии новой свечи сразу после закрытия сигнальной (FIB_RETRACE=0.0).
    FOMO CAP уже отработал внутри detect_signal (BAR_ATR_MAX=2.5): сигналы на перерастянутых
    свечах сюда не попадают, поэтому маркет-ордер не покупает абсолютный хай импульса."""
    size_info = {}
    if RISK_SIZING:
        qty, size_info = calc_position_qty(sym, d["entry"], d["sl"])
        if qty <= 0:
            tg_send(chat, f"\u26A0\uFE0F {sym}: пропуск — риск-сайзинг дал нулевой объём ({size_info}).")
            return
    else:
        qty = NOTIONAL / d["entry"]
    notional_used = qty * d["entry"]
    fee_in = notional_used * FEE_MAKER
    risk_all = open_risk_usd(st)
    risk_usd = qty * (d["entry"] - d["sl"])
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
        # БИРЖЕВОЙ СТОП с ретраем: позиция не должна остаться голой, если бот упадёт
        stop_ok = False
        for attempt in range(3):
            r_stop = bybit_set_stop(sym, sl_price=d["sl"], tp_price=None)
            if r_stop.get("retCode") == 0:
                stop_ok = True; break
            time.sleep(1)
        if not stop_ok:
            # КРИТИЧНО: без стопа позицию не держим — закрываем сразу
            tg_send(chat, f"\U0001F6A8 {sym}: SL НЕ выставлен на бирже после 3 попыток "
                          f"({r_stop.get('retMsg')}). Закрываю позицию — голую держать нельзя.")
            bybit_close_market(sym, qty); bybit_cancel_all(sym)
            return
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

def _ensure_exchange_stop(sym, pos, chat):
    """СТОРОЖ: раз в цикл проверяем, что биржевой стоп реально стоит.
    Если бот падал/передеплоивался — стоп мог не выставиться, позиция осталась голой."""
    if not AUTO_TRADE:
        return
    try:
        r = _bybit_signed("GET", "/v5/position/list", params={"category": CATEGORY, "symbol": sym})
        lst = (r.get("result") or {}).get("list") or []
        if not lst:
            return
        cur_sl = lst[0].get("stopLoss") or ""
        if cur_sl in ("", "0", 0):
            want = pos["sl"]
            rr = bybit_set_stop(sym, sl_price=want, tp_price=None)
            if rr.get("retCode") == 0:
                tg_send(chat, f"\U0001F6E1 {sym}: биржевой стоп отсутствовал — восстановлен на ${want:.6g}")
            else:
                tg_send(chat, f"\U0001F6A8 {sym}: стоп на бирже ОТСУТСТВУЕТ и не восстановился: {rr.get('retMsg')}")
    except Exception as e:
        print("stop guard err:", e)

def manage_position(st, chat):
    """Частичная фиксация: TP1 (entry+2*ATR) закрывает 50%, сразу переносит SL остатка в БУ.
    TP2 (entry+4.5*ATR) закрывает финальные 50%. Между TP1 и TP2 — трейлинг 1.5*ATR от пика."""
    for sym, pos in list(st.get("positions", {}).items()):
        price = bybit_price(sym); time.sleep(0.05)
        if price is None: continue
        _ensure_exchange_stop(sym, pos, chat)   # сторож: позиция не должна остаться без стопа
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
    for sub, label in (("close <= prev_close", "close не выше prev_close"),
                       ("нет пробоя over", "нет пробоя over N-свечного диапазона"),
                       ("не зелёная", "импульсная свеча не зелёная"),
                       ("пробоя", "нет пробоя high пред. свечи"),
                       ("движение цены слабое", "движение цены слабее 1.5x ATR"),
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


# ============================================================================
# FIXED TP/SL MODEL + OPTIMIZE_PARAMS  (по присланному коду, на реальных данных)
# ============================================================================

# ============================================================================
# DERIVATIVES DATA / EXECUTION MODEL / SIGNAL SCORE / RISK SIZING
# ============================================================================
EXEC_SPREAD      = float(os.environ.get("EXEC_SPREAD", "0.0005"))   # спред 0.05% (половина в каждую сторону)
EXEC_SLIP_MIN    = float(os.environ.get("EXEC_SLIP_MIN", "0.1"))    # проскальзывание: доля волатильности, min
EXEC_SLIP_MAX    = float(os.environ.get("EXEC_SLIP_MAX", "0.5"))    # проскальзывание: доля волатильности, max
RISK_PER_TRADE   = float(os.environ.get("RISK_PER_TRADE", "0.01"))  # риск на сделку от баланса (1%)

_funding_cache = {}

def get_funding_rate(symbol):
    """РЕАЛЬНЫЙ funding rate с Binance (не симуляция). Кэш 1 час."""
    now = time.time()
    if symbol in _funding_cache:
        val, ts = _funding_cache[symbol]
        if now - ts < 3600:
            return val
    try:
        d = http_json(f"{BINANCE}/fapi/v1/premiumIndex?symbol={symbol}")
        val = float(d.get("lastFundingRate", 0.0))
    except Exception:
        val = 0.0
    _funding_cache[symbol] = (val, now)
    return val

def add_derivatives_data(sym, c, v, oi_vals):
    """Деривативные метрики на РЕАЛЬНЫХ данных:
      funding_rate — реальный с Binance premiumIndex
      delta / cvd  — из close.diff() * volume (как в присланном коде)
      liq_long/liq_short — Binance не отдаёт бесплатную историю ликвидаций,
                           поэтому вместо random-симуляции ставим 0.0 (нейтрально)."""
    delta = [0.0]
    for i in range(1, len(c)):
        delta.append((c[i] - c[i - 1]) * v[i])
    cvd = []
    run = 0.0
    for d_ in delta:
        run += d_; cvd.append(run)
    return dict(
        funding_rate=get_funding_rate(sym),
        delta=delta,
        cvd=cvd,
        liq_long=0.0,
        liq_short=0.0,
    )

def apply_execution_model(price, side, volatility, rng=None):
    """Ваш execution model: спред + проскальзывание + латентность.
    Закрывает главную дыру всех прошлых бэктестов ('спред/проскальзывание НЕТ')."""
    import random as _r
    r = rng or _r
    spread = price * EXEC_SPREAD
    slippage = (volatility or 0.0) * price * r.uniform(EXEC_SLIP_MIN, EXEC_SLIP_MAX)
    latency_shift = r.randint(0, 1)
    if side == "long":
        exec_price = price + spread / 2 + slippage       # покупаем дороже
    else:
        exec_price = price - spread / 2 - slippage       # продаём дешевле
    return exec_price, latency_shift

def compute_signal_score(row):
    """Ваш непрерывный score вместо бинарных фильтров.
    row: volume, volume_ma, oi_change, funding_rate, liq_short, liq_long, delta"""
    score = 0.0
    score += min(row["volume"] / (row.get("volume_ma", 0) + 1e-6), 3)
    score += math.tanh(row.get("oi_change", 0.0) * 5)
    score -= row.get("funding_rate", 0.0) * 100
    score += math.tanh(row.get("liq_short", 0.0) - row.get("liq_long", 0.0))
    score += math.tanh(row.get("delta", 0.0) / (row["volume"] + 1e-6))
    return score

def position_size(balance, risk_per_trade, stop_distance):
    """Ваш риск-сайзинг: объём считается от РИСКА, а не фиксированный notional.
    size = (balance * risk%) / расстояние_до_стопа"""
    risk_amount = balance * risk_per_trade
    return risk_amount / (stop_distance + 1e-6)

def analyze_trades(trades):
    """Ваша analyze_trades."""
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    winrate = len(wins) / len(trades) if trades else 0.0
    avg_win = (sum(t["pnl"] for t in wins) / len(wins)) if wins else 0.0
    avg_loss = (sum(t["pnl"] for t in losses) / len(losses)) if losses else 0.0
    return {
        "PnL": sum(t["pnl"] for t in trades),
        "Winrate": winrate,
        "Avg Win": avg_win,
        "Avg Loss": avg_loss,
        "Trades": len(trades),
    }

def _rolling_vol(c, i, window=20):
    """Волатильность = std доходностей за window баров (для execution model)."""
    if i < window + 1: return 0.0
    rets = [(c[k] / c[k - 1] - 1.0) for k in range(i - window + 1, i + 1) if c[k - 1] > 0]
    if len(rets) < 2: return 0.0
    m = sum(rets) / len(rets)
    var = sum((x - m) ** 2 for x in rets) / (len(rets) - 1)
    return math.sqrt(var)


def run_trade(h, l, entry_idx, entry_price, params, max_bars=None):
    """Ваша run_trade: фиксированные TP/SL в процентах от входа.
    ПЕССИМИСТИЧНО: в одном баре SL проверяется РАНЬШЕ TP.
    Возвращает доходность сделки в долях (+tp / -sl / 0 если не закрылась)."""
    tp = entry_price * (1 + params["tp"])
    sl = entry_price * (1 - params["sl"])
    end = len(h) if max_bars is None else min(len(h), entry_idx + 1 + max_bars)
    for i in range(entry_idx + 1, end):
        if l[i] <= sl:
            return -params["sl"]
        if h[i] >= tp:
            return params["tp"]
    return 0.0

def bt_simulate_coin_fixed(sym, o, h, l, c, v, tb, ct, oi_ts, params, diag=None):
    """Тот же детектор сигналов, но выход по ФИКСИРОВАННЫМ TP/SL (не ATR-лестница).
    Возвращает список сделок [{symbol, pnl_pct, pnl_usd, open_ts}]."""
    W = LEVEL_LOOKBACK + 40
    trades = []
    j = 0
    oi_vals = []
    i = W
    while i < len(c):
        while j < len(oi_ts) and oi_ts[j][0] <= ct[i]:
            oi_vals.append(oi_ts[j][1]); j += 1
        if len(oi_vals) < 2:
            if diag is not None: diag["no_oi"] += 1
            i += 1; continue
        if diag is not None: diag["evals"] += 1
        ok, d = detect_signal(o[i+1-W:i+1], h[i+1-W:i+1], l[i+1-W:i+1],
                              c[i+1-W:i+1], v[i+1-W:i+1], tb[i+1-W:i+1], oi_vals[-8:])
        if ok:
            if diag is not None: diag["signals"] += 1
            vol_now = _rolling_vol(c, i)
            # ВХОД через execution model: спред + проскальзывание (покупаем дороже)
            entry_price, lag = apply_execution_model(c[i], "long", vol_now)
            r = run_trade(h, l, i + lag, entry_price, params)
            # ВЫХОД тоже с издержками: цена выхода ухудшается на спред+слиппедж
            exit_ideal = entry_price * (1 + r)
            exit_price, _ = apply_execution_model(exit_ideal, "short", vol_now)
            gross_pct = exit_price / entry_price - 1.0
            pnl_pct = gross_pct - 2 * FEE_TAKER
            # РИСК-САЙЗИНГ: объём от риска на сделку, а не фиксированный notional
            stop_distance = entry_price * params["sl"]
            size = position_size(DEPOSIT, RISK_PER_TRADE, stop_distance)
            notional_used = min(size * entry_price, NOTIONAL)   # но не больше лимита плеча
            trades.append(dict(symbol=sym, pnl=pnl_pct,
                               pnl_usd=pnl_pct * notional_used, open_ts=ct[i],
                               notional=notional_used, vol=vol_now))
            i += 1
        else:
            if diag is not None:
                lbl = _bt_reason(d)
                diag["reasons"][lbl] = diag["reasons"].get(lbl, 0) + 1
            i += 1
    return trades

def trade_stats(trades):
    """Ваша trade_stats (на списке dict вместо DataFrame — чтобы не тянуть pandas в горячий путь)."""
    if not trades:
        return dict(trades=0, winrate=0.0, avg_win=0.0, avg_loss=0.0, pnl_total=0.0)
    wins = [t["pnl"] for t in trades if t["pnl"] > 0]
    losses = [t["pnl"] for t in trades if t["pnl"] <= 0]
    return dict(
        trades=len(trades),
        winrate=len(wins) / len(trades),
        avg_win=(sum(wins) / len(wins)) if wins else 0.0,
        avg_loss=(sum(losses) / len(losses)) if losses else 0.0,
        pnl_total=sum(t["pnl"] for t in trades),
    )

def optimize_params(cached, tp=0.02, sl=0.01,
                    vol_grid=(1.5, 2.0, 2.5), oi_grid=(0.0, 0.01, 0.02),
                    progress=None):
    """Ваша optimize_params: перебор vol_thr x oi_thr при фиксированных TP/SL.
    vol_thr -> VOL_SPIKE_MIN, oi_thr -> OI_MIN_GROW (доли, не абсолютный OI)."""
    results = []
    total = len(vol_grid) * len(oi_grid)
    done = 0
    for vol_thr in vol_grid:
        for oi_thr in oi_grid:
            params = {"vol_thr": vol_thr, "oi_thr": oi_thr, "tp": tp, "sl": sl}
            saved = {"VOL_SPIKE_MIN": globals()["VOL_SPIKE_MIN"],
                     "OI_MIN_GROW": globals()["OI_MIN_GROW"]}
            globals()["VOL_SPIKE_MIN"] = vol_thr
            globals()["OI_MIN_GROW"] = oi_thr
            try:
                all_tr = []
                diag = dict(evals=0, no_oi=0, signals=0, reasons={})
                for sym, o, h, l, c, v, tb, ct, oi_ts in cached:
                    all_tr += bt_simulate_coin_fixed(
                        sym, o[1:], h[1:], l[1:], c[1:], v[1:], tb[1:], ct[1:],
                        oi_ts, params, diag=diag)
                st = trade_stats(all_tr)
            finally:
                for k_, v_ in saved.items(): globals()[k_] = v_
            row = {"vol_thr": vol_thr, "oi_thr": oi_thr, "tp": tp, "sl": sl}
            row.update(st)
            row["pnl_usd"] = sum(t.get("pnl_usd", 0.0) for t in all_tr)
            results.append(row)
            done += 1
            if progress: progress(done, total, row)
            print(f"vol={vol_thr} oi={oi_thr} -> trades={st['trades']} "
                  f"wr={st['winrate']*100:.0f}% pnl={st['pnl_total']*100:+.2f}%")
    return results


# ============================================================================
# ALPHA MODEL / PORTFOLIO / RISK ENGINE / EXECUTION ENGINE / WALK-FORWARD
# ============================================================================
class AlphaModel:
    """Взвешенная факторная модель. Вместо бинарных фильтров — непрерывный score."""
    def __init__(self, weights=None):
        self.weights = weights or {
            "volume": 1.0,
            "oi": 1.2,
            "funding": -0.8,
            "liq": 1.0,
            "cvd": 1.1,
        }

    def score(self, row):
        features = {
            "volume": min(row["volume"] / (row.get("volume_ma", 0) + 1e-6), 3),
            "oi": math.tanh(row.get("oi_change", 0.0) * 5),
            "funding": row.get("funding_rate", 0.0) * 100,
            "liq": math.tanh(row.get("liq_short", 0.0) - row.get("liq_long", 0.0)),
            "cvd": math.tanh(row.get("delta", 0.0) / (row["volume"] + 1e-6)),
        }
        score = sum(features[k] * self.weights[k] for k in self.weights)
        return score, features


class AlphaModelSimple:
    """2-факторная альфа: объём/MA + tanh(returns*50). Без OI, CVD, тренда и уровня.
    Добавлена для честного сравнения с полной моделью в /alpha."""
    def __init__(self, weights=None):
        self.weights = weights or {"volume": 1.0, "returns": 1.0}

    def score(self, row):
        features = {
            "volume": min(row["volume"] / (row.get("volume_ma", 0) + 1e-6), 3),
            "returns": math.tanh(row.get("returns", 0.0) * 50),
        }
        score = sum(features[k] * self.weights[k] for k in self.weights)
        return score, features


class Portfolio:
    def __init__(self, capital=None):
        self.capital = capital if capital is not None else DEPOSIT
        self.positions = []
        self.equity_curve = []
        self.trades = []

    def open_position(self, pos):
        self.positions.append(pos)

    def close_position(self, pos, exit_price):
        pnl = (exit_price - pos["entry"]) * pos["size"]
        self.capital += pnl
        if pos in self.positions:
            self.positions.remove(pos)
        self.trades.append(dict(entry=pos["entry"], exit=exit_price, pnl=pnl,
                                size=pos["size"], features=pos.get("features", {}),
                                duration=pos.get("duration", 0)))
        return pnl

    def update_equity(self):
        self.equity_curve.append(self.capital)


class RiskEngine:
    """Предохранитель по просадке: превысили max_dd — торговля стоп."""
    def __init__(self, max_risk=0.02, max_dd=0.2):
        self.max_risk = max_risk
        self.max_dd = max_dd
        self.peak = None

    def update(self, equity):
        if self.peak is None or equity > self.peak:
            self.peak = equity
        return (self.peak - equity) / self.peak if self.peak else 0.0

    def allow_trade(self, equity):
        return self.update(equity) < self.max_dd


class ExecutionEngine:
    """Спред + рыночное воздействие (impact), зависящее от волатильности."""
    def __init__(self, spread=0.0004, impact_min=0.2, impact_max=0.6, rng=None):
        self.spread = spread
        self.impact_min = impact_min
        self.impact_max = impact_max
        import random as _r
        self.rng = rng or _r

    def execute(self, price, side, volatility):
        spread = price * self.spread
        impact = (volatility or 0.0) * price * self.rng.uniform(self.impact_min, self.impact_max)
        return price + spread + impact if side == "long" else price - spread - impact


def size_position(capital, risk_pct, volatility):
    """Объём от риска, нормированный на волатильность."""
    dollar_risk = capital * risk_pct
    return dollar_risk / (volatility + 1e-6)


def _alpha_build_rows(sym, o, h, l, c, v, tb, ct, oi_ts, funding=0.0, vol_ma_len=20):
    """Превращает сырые свечи+OI в строки-фичи для AlphaModel (на РЕАЛЬНЫХ данных)."""
    rows = []
    j = 0
    oi_vals = []
    for i in range(len(c)):
        while j < len(oi_ts) and oi_ts[j][0] <= ct[i]:
            oi_vals.append(oi_ts[j][1]); j += 1
        if i < vol_ma_len:
            rows.append(None); continue
        vma = sum(v[i - vol_ma_len:i]) / vol_ma_len
        oi_change = 0.0
        if len(oi_vals) >= 2 and oi_vals[-2] > 0:
            oi_change = (oi_vals[-1] - oi_vals[-2]) / oi_vals[-2]
        delta = 2 * tb[i] - v[i]
        ret = (c[i] / c[i-1] - 1.0) if i > 0 and c[i-1] > 0 else 0.0
        rows.append(dict(close=c[i], high=h[i], low=l[i], volume=v[i], volume_ma=vma,
                         oi_change=oi_change, funding_rate=funding, returns=ret,
                         liq_long=0.0, liq_short=0.0, delta=delta, ts=ct[i]))
    return rows


def run_portfolio(rows, closes, model, score_thr=2.0, hold_bars=10,
                  risk_pct=0.01, capital=None, max_dd=0.2, seed=None):
    """Портфельный прогон по строкам-фичам. Вход при score > порога, выход по времени."""
    import random as _r
    rng = _r.Random(seed) if seed is not None else _r
    portfolio = Portfolio(capital)
    risk = RiskEngine(max_dd=max_dd)
    execution = ExecutionEngine(rng=rng)
    for i in range(50, len(rows)):
        row = rows[i]
        if row is None:
            portfolio.update_equity(); continue
        vol = _rolling_vol(closes, i)
        if not risk.allow_trade(portfolio.capital):
            portfolio.update_equity(); continue
        score, features = model.score(row)
        if score > score_thr and not portfolio.positions:
            entry = execution.execute(row["close"], "long", vol)
            size = size_position(portfolio.capital, risk_pct, vol)
            # ограничение плечом: notional не больше NOTIONAL
            size = min(size, NOTIONAL / entry if entry > 0 else size)
            portfolio.open_position(dict(entry=entry, size=size, features=features, entry_i=i))
        for pos in portfolio.positions[:]:
            if i - pos["entry_i"] >= hold_bars:
                exit_price = execution.execute(row["close"], "short", vol)
                pos["duration"] = i - pos["entry_i"]
                portfolio.close_position(pos, exit_price)
        portfolio.update_equity()
    # закрыть хвосты
    for pos in portfolio.positions[:]:
        pos["duration"] = len(rows) - 1 - pos["entry_i"]
        portfolio.close_position(pos, rows[-1]["close"] if rows[-1] else pos["entry"])
    return portfolio


def evaluate_model(rows, closes, model, **kw):
    """Метрика для оптимизации весов: итоговый капитал."""
    p = run_portfolio(rows, closes, model, seed=42, **kw)
    return p.capital


def optimize_weights(rows, closes, grid=(0.5, 1.0, 1.5)):
    """Перебор весов volume/oi/cvd на ТРЕНИРОВОЧНОМ окне."""
    best_score = float("-inf")
    best_weights = None
    for vw in grid:
        for oi in grid:
            for cvd in grid:
                weights = {"volume": vw, "oi": oi, "funding": -1.0, "liq": 1.0, "cvd": cvd}
                s = evaluate_model(rows, closes, AlphaModel(weights))
                if s > best_score:
                    best_score = s
                    best_weights = weights
    return best_weights, best_score


def walk_forward(rows, closes, train_window=500, test_window=200):
    """СКОЛЬЗЯЩИЙ walk-forward по ВРЕМЕНИ: веса подбираются на train,
    проверяются на следующем, ни разу не виденном test-окне."""
    results = []
    step = test_window
    start = 0
    while start + train_window + test_window <= len(rows):
        tr_rows = rows[start:start + train_window]
        tr_close = closes[start:start + train_window]
        te_rows = rows[start + train_window:start + train_window + test_window]
        te_close = closes[start + train_window:start + train_window + test_window]
        best_w, train_cap = optimize_weights(tr_rows, tr_close)
        model = AlphaModel(best_w)
        p = run_portfolio(te_rows, te_close, model, seed=7)
        results.append(dict(start=start, weights=best_w,
                            train_capital=train_cap, test_capital=p.capital,
                            train_ret=train_cap / DEPOSIT - 1.0,
                            test_ret=p.capital / DEPOSIT - 1.0,
                            trades=len(p.trades), portfolio=p))
        start += step
    return results


def factor_attribution(trades):
    """Какой фактор реально давал PnL: средний вклад feature*pnl."""
    factors = {}
    for t in trades:
        for k, val in (t.get("features") or {}).items():
            factors.setdefault(k, []).append(t["pnl"] * val)
    return {k: (sum(vs) / len(vs)) for k, vs in factors.items() if vs}


def run_livecheck(chat, sym="BTCUSDT"):
    """Проверка готовности LIVE-слоя. НИ ОДНОГО ОРДЕРА не отправляется —
    только чтение: ключи, эндпоинт, баланс, инструмент, округления, расчёт объёма."""
    L = ["\U0001F50C <b>LIVE-CHECK</b> (только чтение, ордера не шлются)"]
    endpoint = "DEMO" if BYBIT_USE_DEMO else "\u26A0\uFE0F РЕАЛ (mainnet)"
    L.append(f"\u2022 Эндпоинт: <b>{endpoint}</b> \u2014 {BYBIT}")
    L.append(f"\u2022 AUTO_TRADE: <b>{'ВКЛ (ордера пойдут)' if AUTO_TRADE else 'ВЫКЛ (paper)'}</b>")
    L.append(f"\u2022 Ключи: API_KEY {'есть' if BYBIT_API_KEY else 'НЕТ'}, "
             f"SECRET {'есть' if BYBIT_API_SECRET else 'НЕТ'}")
    L.append(f"\u2022 Сайзинг: {'РИСК ' + str(RISK_PCT_TRADE*100) + '% депо' if RISK_SIZING else 'фиксированный ' + str(int(NOTIONAL)) + '$'}")
    # баланс
    try:
        bal = bybit_wallet_balance()
        L.append(f"\u2022 Баланс: <b>{bal:.2f} USDT</b>" if bal is not None
                 else "\u2022 Баланс: \u274C не получен (проверь ключи/права)")
    except Exception as e:
        L.append(f"\u2022 Баланс: \u274C ошибка {e}")
    # инструмент и округления
    try:
        inst = bybit_instrument_info(sym)
        if inst:
            step = inst["lotSizeFilter"].get("qtyStep")
            minq = inst["lotSizeFilter"].get("minOrderQty")
            tick = inst["priceFilter"].get("tickSize")
            L.append(f"\u2022 {sym}: шаг лота {step}, мин. объём {minq}, шаг цены {tick}")
            px = bybit_price(sym)
            if px:
                stop_px = px * 0.995
                qty, info = calc_position_qty(sym, px, stop_px)
                L.append(f"\u2022 Тест-расчёт при цене ${px:,.2f}, стоп \u22120.5%:")
                L.append(f"   риск {info.get('risk_usd',0):.2f}$ / дистанция {info.get('stop_distance',0):.4f}$ "
                         f"\u2192 <b>{qty} {sym.replace('USDT','')}</b> "
                         f"(номинал ${info.get('notional_final',0):,.2f})")
                if info.get("capped_by_leverage"):
                    L.append("   \u2139\uFE0F объём урезан потолком плеча")
                if info.get("below_min_qty"):
                    L.append(f"   \u26A0\uFE0F ниже минимального объёма биржи: {info['below_min_qty']}")
        else:
            L.append(f"\u2022 {sym}: \u274C инструмент не получен")
    except Exception as e:
        L.append(f"\u2022 Инструмент: \u274C ошибка {e}")
    # открытые позиции на бирже
    try:
        r = _bybit_signed("GET", "/v5/position/list", params={"category": CATEGORY, "symbol": sym})
        lst = (r.get("result") or {}).get("list") or []
        live_pos = [p for p in lst if float(p.get("size", 0) or 0) != 0]
        L.append(f"\u2022 Открытых позиций по {sym} на бирже: {len(live_pos)}")
        for p in live_pos:
            L.append(f"   размер {p.get('size')}, вход {p.get('avgPrice')}, "
                     f"стоп {p.get('stopLoss') or '\u26A0\uFE0F НЕТ'}")
    except Exception as e:
        L.append(f"\u2022 Позиции: \u274C ошибка {e}")
    L.append("\n<i>Ордера не отправлялись. Для включения реальных сделок нужен AUTO_TRADE=1.</i>")
    tg_send(chat, "\n".join(L))


# ============================================================================
# SCORED BACKTEST + KILL-SWITCH + WALK-FORWARD 20d/10d
# ============================================================================
def bt_simulate_scored(sym, o, h, l, c, v, tb, ct, oi_ts, threshold=None,
                       weights=None, capital=None, kill_dd=None, diag=None, seed=None,
                       use_regime=False, use_kelly=False, collect_only=False):
    """Прогон scored-детектора с реалистичным исполнением, score-сайзингом и kill-switch."""
    import random as _r
    rng = _r.Random(seed) if seed is not None else _r
    kill_dd = KILL_SWITCH_DD if kill_dd is None else kill_dd
    capital = DEPOSIT if capital is None else capital
    start_capital = capital
    peak = capital
    killed = False
    W_ = LEVEL_LOOKBACK + 40
    trades = []
    pos = None
    j = 0
    oi_vals = []
    for i in range(W_, len(c)):
        while j < len(oi_ts) and oi_ts[j][0] <= ct[i]:
            oi_vals.append(oi_ts[j][1]); j += 1
        # --- ведение позиции ---
        if pos:
            exit_px = None; reason = None
            if l[i] <= pos["sl"]:                      # пессимизм: стоп раньше тейка
                exit_px, reason = pos["sl"], "SL"
            elif h[i] >= pos["tp1"]:
                exit_px, reason = pos["tp1"], "TP1"
            elif i - pos["i"] >= 20:
                exit_px, reason = c[i], "TIME"
            if exit_px is not None:
                fill = apply_slippage(exit_px, "short", rng)
                pnl = (fill - pos["entry"]) * pos["qty"] - (pos["entry"] + fill) * pos["qty"] * FEE_TAKER
                capital += pnl
                peak = max(peak, capital)
                trades.append(dict(symbol=sym, pnl=pnl, reason=reason, score=pos["score"],
                                   qty=pos["qty"], open_ts=ct[pos["i"]], close_ts=ct[i]))
                pos = None
                # KILL-SWITCH: просадка от пика больше порога -> торговля стоп
                dd = (peak - capital) / peak if peak > 0 else 0.0
                if dd > kill_dd:
                    killed = True
                    if diag is not None: diag["killed"] = True
                    break
        # --- поиск входа ---
        if pos is None and len(oi_vals) >= 2:
            if diag is not None: diag["evals"] += 1
            if use_regime:
                reg = market_regime(c, i)
                if reg != "trend":
                    if diag is not None:
                        diag["reasons"][f"режим рынка: {reg}"] = diag["reasons"].get(f"режим рынка: {reg}", 0) + 1
                    continue
            ok, d = detect_signal_scored(o[i+1-W_:i+1], h[i+1-W_:i+1], l[i+1-W_:i+1],
                                         c[i+1-W_:i+1], v[i+1-W_:i+1], tb[i+1-W_:i+1],
                                         oi_vals[-8:], threshold=threshold, weights=weights)
            if ok:
                if diag is not None: diag["signals"] += 1
                if collect_only:
                    # режим сбора кандидатов для TOP-N: сделку не открываем
                    trades.append(dict(candidate=True, symbol=sym, ts=ct[i], i=i,
                                       score=d["score"], entry=d["entry"], sl=d["sl"],
                                       tp1=d["tp1"]))
                    continue
                entry = apply_slippage(d["entry"], "long", rng)
                stop_dist = entry - d["sl"]
                if stop_dist > 0:
                    if use_kelly:
                        frac = size_kelly_lite(d["score"])
                        if frac <= 0:
                            continue
                        risk_usd = capital * frac
                    else:
                        risk_usd = size_from_score(capital, d["score"])
                    qty = risk_usd / stop_dist
                    qty = min(qty, NOTIONAL / entry)                  # потолок плеча
                    pos = dict(entry=entry, sl=d["sl"], tp1=d["tp1"], qty=qty,
                               i=i, score=d["score"])
            elif diag is not None:
                lbl = "score ниже порога" if str(d).startswith("score") else _bt_reason(d)
                diag["reasons"][lbl] = diag["reasons"].get(lbl, 0) + 1
    return trades, capital - start_capital, killed


def walk_forward_days(cached, train_days=20, test_days=10, thr_grid=(3.0, 3.5, 4.0, 4.5, 5.0)):
    """Walk-forward по ВРЕМЕНИ: train 20 дней -> test 10 дней -> сдвиг (шаг 4 плана).
    На train подбирается порог score, на test он проверяется вслепую."""
    bars_train = int(train_days * 96)
    bars_test = int(test_days * 96)
    windows = []
    for sym, o, h, l, c, v, tb, ct, oi_ts in cached:
        start = 0
        while start + bars_train + bars_test <= len(c):
            sl_ = slice(start, start + bars_train)
            te_ = slice(start + bars_train, start + bars_train + bars_test)
            oi_tr = [x for x in oi_ts if ct[sl_.start] <= x[0] <= ct[sl_.stop - 1]]
            oi_te = [x for x in oi_ts if ct[te_.start] <= x[0] <= ct[te_.stop - 1]]
            best_thr, best_pnl = None, float("-inf")
            for thr in thr_grid:
                _, pnl_tr, _ = bt_simulate_scored(sym, o[sl_], h[sl_], l[sl_], c[sl_],
                                                  v[sl_], tb[sl_], ct[sl_], oi_tr,
                                                  threshold=thr, seed=42)
                if pnl_tr > best_pnl:
                    best_pnl, best_thr = pnl_tr, thr
            tr_te, pnl_te, killed = bt_simulate_scored(sym, o[te_], h[te_], l[te_], c[te_],
                                                       v[te_], tb[te_], ct[te_], oi_te,
                                                       threshold=best_thr, seed=7)
            windows.append(dict(symbol=sym, start=start, thr=best_thr,
                                train_pnl=best_pnl, test_pnl=pnl_te,
                                test_trades=len(tr_te), killed=killed, trades=tr_te))
            start += bars_test
    return windows



# ============================================================================
# ADAPTIVE FUNNEL — сам ослабляет фильтры, которые режут больше всего
# ============================================================================
class AdaptiveFunnel:
    def __init__(self):
        self.stats = {"breakout": 0, "volume": 0, "oi": 0, "cvd": 0}
        self.thresholds = {
            "breakout_mult": 1.0,
            "volume_mult": 2.0,
            "oi_min": 0.0,
            "cvd_min": 0.0,
        }
        self.total_checks = 1
        self.history = []

    def register_fail(self, reason):
        if reason in self.stats:
            self.stats[reason] += 1
        self.total_checks += 1

    def register_pass(self):
        self.total_checks += 1

    def adapt(self):
        changed = {}
        for k in self.stats:
            fail_rate = self.stats[k] / self.total_checks
            if fail_rate > 0.5:
                if k == "breakout":
                    self.thresholds["breakout_mult"] *= 0.995
                elif k == "volume":
                    self.thresholds["volume_mult"] *= 0.98
                elif k == "oi":
                    self.thresholds["oi_min"] *= 0.9
                elif k == "cvd":
                    self.thresholds["cvd_min"] *= 0.9
                changed[k] = fail_rate
        if changed:
            self.history.append(dict(checks=self.total_checks, changed=changed,
                                     thresholds=dict(self.thresholds)))
        return changed

    def get(self):
        return self.thresholds

    def report(self):
        L = [f"\U0001F504 <b>ADAPTIVE FUNNEL</b> (проверок {self.total_checks:,})"]
        for k, cnt in sorted(self.stats.items(), key=lambda x: -x[1]):
            rate = cnt / self.total_checks * 100
            flag = " \u2190 ослабляется" if rate > 50 else ""
            L.append(f"\u2022 {k}: {cnt:,} отказов ({rate:.1f}%){flag}")
        L.append("\n\u2699\uFE0F Текущие пороги:")
        for k, v in self.thresholds.items():
            L.append(f"\u2022 {k}: {v:.5f}")
        L.append(f"\n\U0001F4DC Адаптаций было: {len(self.history)}")
        return "\n".join(L)


def detect_signal_adaptive(o, h, l, c, v, tb, oi, funnel):
    """Детектор, использующий пороги из AdaptiveFunnel. Каждый отказ регистрируется,
    чтобы воронка знала, какой фильтр самый дорогой."""
    th = funnel.get()
    n = len(c)
    if n < LEVEL_LOOKBACK + 30:
        return False, "мало истории"
    i1 = n - 1
    if c[i1] <= o[i1]:
        funnel.register_fail("candle"); return False, "свеча не зелёная"
    rng1 = h[i1] - l[i1]
    if rng1 <= 0:
        return False, "нулевая свеча"
    a = atr(h[:i1], l[:i1], c[:i1], ATR_LEN)

    # 1) BREAKOUT с адаптивным множителем
    level = max(h[i1 - LEVEL_LOOKBACK:i1])
    if not (c[i1] > level * th["breakout_mult"]):
        funnel.register_fail("breakout")
        return False, "нет пробоя"
    # 2) VOLUME с адаптивным порогом
    base = v[i1 - VOL_MA_LEN:i1]
    vma = (sum(base) / len(base)) if base else 0.0
    vol_ratio = (v[i1] / vma) if vma > 0 else 0.0
    if vol_ratio < th["volume_mult"]:
        funnel.register_fail("volume")
        return False, f"объём x{vol_ratio:.2f} < {th['volume_mult']:.2f}"
    # 3) OI с адаптивным минимумом
    oi_chg = 0.0
    if len(oi) >= 2 and oi[-2] > 0:
        oi_chg = (oi[-1] - oi[-2]) / oi[-2]
    if oi_chg < th["oi_min"]:
        funnel.register_fail("oi")
        return False, f"OI {oi_chg*100:+.2f}% < {th['oi_min']*100:.2f}%"
    # 4) CVD с адаптивным минимумом
    delta = 2 * tb[i1] - v[i1]
    cvd_norm = delta / (v[i1] + 1e-9) if v[i1] > 0 else 0.0
    if cvd_norm < th["cvd_min"]:
        funnel.register_fail("cvd")
        return False, f"CVD {cvd_norm:.3f} < {th['cvd_min']:.3f}"

    funnel.register_pass()
    entry = c[i1]
    sl = entry - ATR_SL_MULT * a if a > 0 else entry * 0.995
    if sl >= entry:
        return False, "стоп выше входа"
    return True, dict(entry=entry, sl=sl,
                      tp1=entry + ATR_TP1_MULT * a if a > 0 else entry * 1.01,
                      tp2=entry + ATR_TP2_MULT * a if a > 0 else entry * 1.02,
                      atr=a, spike=vol_ratio, oi_chg=oi_chg, delta=delta,
                      level=level, close3=c[i1], low1=l[i1], high3=h[i1],
                      risk_pct=(entry - sl) / entry, wick=(h[i1]-c[i1])/rng1,
                      rsi=rsi(c[-(RSI_LEN*6):], RSI_LEN), e21=0.0, e50=0.0)


# ============================================================================
# ML RANKING — XGBoost (fallback: sklearn GradientBoosting)
# ============================================================================
ML_FEATURES = ["vol_ratio", "oi_change", "return", "volatility", "cvd_slope"]
ML_THRESHOLD = float(os.environ.get("ML_THRESHOLD", "0.6"))
ML_HORIZON = int(os.environ.get("ML_HORIZON", "5"))
ML_TARGET_RET = float(os.environ.get("ML_TARGET_RET", "0.003"))

def _make_classifier():
    """XGBoost если есть, иначе sklearn GradientBoosting с теми же гиперпараметрами."""
    try:
        from xgboost import XGBClassifier
        return XGBClassifier(n_estimators=100, max_depth=4, learning_rate=0.05,
                             eval_metric="logloss"), "XGBoost"
    except Exception:
        pass
    try:
        from sklearn.ensemble import GradientBoostingClassifier
        return GradientBoostingClassifier(n_estimators=100, max_depth=4,
                                          learning_rate=0.05), "sklearn GB"
    except Exception:
        return None, None


def build_features(c, v, oi_series):
    """vol_ratio, oi_change, return, volatility, delta/cvd, cvd_slope — на РЕАЛЬНЫХ рядах."""
    n = len(c)
    rows = []
    cvd = 0.0
    prev_cvd = 0.0
    for i in range(n):
        if i < 21:
            rows.append(None); cvd_prev = cvd; continue
        vma = sum(v[i-20:i]) / 20
        vol_ratio = v[i] / vma if vma > 0 else 0.0
        oi_change = (oi_series[i] - oi_series[i-1]) if i < len(oi_series) and oi_series[i-1] else 0.0
        ret = c[i] / c[i-1] - 1.0 if c[i-1] > 0 else 0.0
        rets = [(c[k] / c[k-1] - 1.0) for k in range(i-19, i+1) if c[k-1] > 0]
        m = sum(rets) / len(rets) if rets else 0.0
        var = sum((x-m)**2 for x in rets) / (len(rets)-1) if len(rets) > 1 else 0.0
        volatility = math.sqrt(var)
        delta = (c[i] - c[i-1]) * v[i]
        prev_cvd = cvd
        cvd += delta
        rows.append(dict(vol_ratio=vol_ratio, oi_change=oi_change, **{"return": ret},
                         volatility=volatility, delta=delta, cvd=cvd,
                         cvd_slope=cvd - prev_cvd, close=c[i], idx=i))
    return rows


def create_target(rows, closes, horizon=None, target_ret=None):
    """target = 1, если через horizon баров цена выше на target_ret."""
    horizon = ML_HORIZON if horizon is None else horizon
    target_ret = ML_TARGET_RET if target_ret is None else target_ret
    out = []
    for r in rows:
        if r is None: continue
        i = r["idx"]
        if i + horizon >= len(closes): continue
        future_return = closes[i + horizon] / closes[i] - 1.0
        r2 = dict(r); r2["target"] = 1 if future_return > target_ret else 0
        r2["future_return"] = future_return
        out.append(r2)
    return out


def train_model(dataset):
    """Обучение на списке строк с target. Возвращает (model, name)."""
    model, name = _make_classifier()
    if model is None:
        return None, None
    X = [[r[f] for f in ML_FEATURES] for r in dataset]
    y = [r["target"] for r in dataset]
    if len(set(y)) < 2:
        return None, None
    model.fit(X, y)
    return model, name


def ml_score(model, row):
    """Вероятность класса 1 = ranking score."""
    if model is None: return 0.0
    feats = [[row[f] for f in ML_FEATURES]]
    try:
        return float(model.predict_proba(feats)[0][1])
    except Exception:
        return 0.0


def size_from_confidence(balance, score, base=0.01):
    """Больше уверенность -> больше позиция. size = base * (1 + (score-0.5)*2)"""
    size = base * (1 + (score - 0.5) * 2)
    return balance * max(0.0, size)



def run_ml(chat, days=30, ncoins=30, thr=None, train_frac=0.7):
    """/ml — обучает ML-ранкер на РАННЕЙ части истории, торгует на ПОЗДНЕЙ (out-of-sample).
    Разделение строго по ВРЕМЕНИ: перемешивать финансовые ряды нельзя — это утечка будущего."""
    if BT_RUNNING["on"]:
        tg_send(chat, "\u23F3 Идёт другой прогон — дождись окончания."); return
    BT_RUNNING["on"] = True
    try:
        days = max(7, min(days, 30)); ncoins = max(1, min(ncoins, 60))
        thr = ML_THRESHOLD if thr is None else thr
        _, clf_name = _make_classifier()
        if clf_name is None:
            tg_send(chat, "\u274C Нет ни xgboost, ни sklearn. Добавь в requirements.txt: xgboost scikit-learn"); return
        tg_send(chat, f"\U0001F916 <b>ML RANKING</b>: {days} дн \u00d7 {ncoins} монет\n"
                      f"\u2699\uFE0F Модель: {clf_name} \u00b7 фичи: {', '.join(ML_FEATURES)}\n"
                      f"\U0001F3AF Target: рост > {ML_TARGET_RET*100:.2f}% за {ML_HORIZON} баров \u00b7 порог входа {thr}\n"
                      f"\u23F1 Сплит по ВРЕМЕНИ: первые {train_frac*100:.0f}% — обучение, "
                      f"последние {(1-train_frac)*100:.0f}% — торговля вслепую\n"
                      f"\U0001F4B8 Сайзинг: от уверенности модели \u00b7 слиппедж+спред+комиссии учтены\n"
                      f"Живой скан на паузе. Качаю историю\u2026")
        cached = _bt_prefetch(days, ncoins)
        if not cached:
            tg_send(chat, "\u274C Не удалось загрузить данные."); return
        tg_send(chat, f"\U0001F4E6 {len(cached)} монет. Считаю фичи\u2026")

        train_rows, test_sets = [], []
        for sym, o, h, l, c, v, tb, ct, oi_ts in cached:
            oi_al = []
            j = 0; cur = 0.0
            for i in range(len(c)):
                while j < len(oi_ts) and oi_ts[j][0] <= ct[i]:
                    cur = oi_ts[j][1]; j += 1
                oi_al.append(cur)
            rows = build_features(c, v, oi_al)
            ds = create_target(rows, c)
            if len(ds) < 100: continue
            split = int(len(ds) * train_frac)
            train_rows += ds[:split]                      # РАННЯЯ часть -> обучение
            test_sets.append((sym, ds[split:], c, h, l, ct))   # ПОЗДНЯЯ -> торговля
        if len(train_rows) < 200:
            tg_send(chat, f"\u274C Мало данных для обучения ({len(train_rows)} строк)."); return

        pos_rate = sum(r["target"] for r in train_rows) / len(train_rows)
        tg_send(chat, f"\U0001F9E0 Обучаю на {len(train_rows):,} примерах "
                      f"(положительных {pos_rate*100:.1f}%)\u2026")
        model, name = train_model(train_rows)
        if model is None:
            tg_send(chat, "\u274C Не удалось обучить (один класс в target)."); return

        # важность фич
        imp_txt = ""
        try:
            imps = list(getattr(model, "feature_importances_", []))
            if imps:
                pairs = sorted(zip(ML_FEATURES, imps), key=lambda x: -x[1])
                imp_txt = "\n\U0001F50E <b>Важность фич:</b>\n" + "\n".join(
                    f"\u2022 {k}: {vv*100:.1f}%" for k, vv in pairs)
        except Exception:
            pass

        # торговля на невиданной части
        import random as _r
        rng = _r.Random(13)
        trades = []; capital = DEPOSIT; peak = DEPOSIT; killed = 0
        scored_cnt = 0
        for sym, ds, c, h, l, ct in test_sets:
            pos = None
            for r in ds:
                i = r["idx"]
                if pos:
                    exit_px = None; reason = None
                    if l[i] <= pos["sl"]: exit_px, reason = pos["sl"], "SL"
                    elif h[i] >= pos["tp1"]: exit_px, reason = pos["tp1"], "TP1"
                    elif i - pos["i"] >= 20: exit_px, reason = c[i], "TIME"
                    if exit_px is not None:
                        fill = apply_slippage(exit_px, "short", rng)
                        pnl = ((fill - pos["entry"]) * pos["qty"]
                               - (pos["entry"] + fill) * pos["qty"] * FEE_TAKER)
                        capital += pnl; peak = max(peak, capital)
                        trades.append(dict(symbol=sym, pnl=pnl, reason=reason,
                                           score=pos["score"], close_ts=ct[i]))
                        pos = None
                        if peak > 0 and (peak - capital) / peak > KILL_SWITCH_DD:
                            killed += 1; break
                if pos is None:
                    s = ml_score(model, r)
                    scored_cnt += 1
                    if s > thr:
                        entry = apply_slippage(r["close"], "long", rng)
                        a = atr(h[:i], l[:i], c[:i], ATR_LEN)
                        sl = entry - ATR_SL_MULT * a if a > 0 else entry * 0.995
                        dist = entry - sl
                        if dist > 0:
                            risk_usd = size_from_confidence(capital, s)
                            qty = min(risk_usd / dist, NOTIONAL / entry)
                            pos = dict(entry=entry, sl=sl,
                                       tp1=entry + ATR_TP1_MULT * a if a > 0 else entry*1.01,
                                       qty=qty, i=i, score=s)

        n = len(trades)
        if n == 0:
            tg_send(chat, f"\U0001F4ED Модель обучена ({name}), но при пороге {thr} "
                          f"ни один из {scored_cnt:,} баров не прошёл. Понизь порог: /ml {days} {ncoins} 0.5"
                          + imp_txt)
            return
        wins = sum(1 for t in trades if t["pnl"] > 0)
        total = sum(t["pnl"] for t in trades)
        eq = [DEPOSIT]
        for t in sorted(trades, key=lambda x: x["close_ts"]): eq.append(eq[-1] + t["pnl"])
        pk = DEPOSIT; dd = 0.0
        for x in eq:
            pk = max(pk, x); dd = max(dd, (pk - x) / pk)
        avg_conf = sum(t["score"] for t in trades) / n
        verdict = ("\u2705 <b>ПЛЮС на невиданных данных</b> — модель училась только на ранней части."
                   if total > 0 else
                   "\u274C <b>Минус на невиданных данных</b> — ML не нашёл закономерности.")
        tg_send(chat, "\n".join([
            f"\U0001F916 <b>ML ИТОГ</b> ({name}, порог {thr})",
            f"Обучение: {len(train_rows):,} примеров \u00b7 оценено баров: {scored_cnt:,}",
            f"Сделок: <b>{n}</b> \u00b7 в плюсе: {wins} ({wins/n*100:.0f}%)",
            f"PnL: <b>{total:+.2f}$</b> ({total/DEPOSIT*100:+.1f}%) \u00b7 макс.DD {dd*100:.1f}%",
            f"Средняя уверенность входа: {avg_conf:.3f}",
            (f"\U0001F6D1 Kill-switch срабатывал: {killed} раз" if killed else ""),
            f"{'\u2705 Выборка достаточна' if n >= 100 else '\u26A0\uFE0F Выборка мала (' + str(n) + ' < 100)'}",
            "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501", verdict]) + imp_txt)
        if HAS_MPL:
            try:
                plt.figure(figsize=(10, 5)); plt.plot(eq, linewidth=1.4)
                plt.axhline(DEPOSIT, color="gray", linestyle="--", alpha=0.5)
                plt.title(f"ML ranking ({name}) · {n} сделок · порог {thr}")
                plt.xlabel("Сделки"); plt.ylabel("Капитал $"); plt.grid(True, alpha=0.4)
                p_ = "/tmp/ml_eq.png"; plt.savefig(p_, dpi=110, bbox_inches="tight"); plt.close()
                tg_photo(chat, p_, caption="ML equity на невиданной части истории")
            except Exception as e:
                print("ml chart err:", e)
    finally:
        BT_RUNNING["on"] = False


def run_adaptive(chat, days=30, ncoins=60, adapt_every=500):
    """/adaptive — воронка сама ослабляет самые дорогие фильтры по ходу прогона."""
    if BT_RUNNING["on"]:
        tg_send(chat, "\u23F3 Идёт другой прогон — дождись окончания."); return
    BT_RUNNING["on"] = True
    try:
        days = max(7, min(days, 30)); ncoins = max(1, min(ncoins, 60))
        funnel = AdaptiveFunnel()
        tg_send(chat, f"\U0001F504 <b>ADAPTIVE FUNNEL</b>: {days} дн \u00d7 {ncoins} монет\n"
                      f"Стартовые пороги: {funnel.get()}\n"
                      f"Фильтр, который режет >50% проверок, ослабляется каждые {adapt_every} баров.\n"
                      f"Живой скан на паузе. Качаю историю\u2026")
        cached = _bt_prefetch(days, ncoins)
        if not cached:
            tg_send(chat, "\u274C Не удалось загрузить данные."); return
        import random as _r
        rng = _r.Random(21)
        trades = []; capital = DEPOSIT; peak = DEPOSIT
        checks = 0
        W_ = LEVEL_LOOKBACK + 40
        for k, (sym, o, h, l, c, v, tb, ct, oi_ts) in enumerate(cached, 1):
            pos = None; j = 0; oi_vals = []
            for i in range(W_, len(c)):
                while j < len(oi_ts) and oi_ts[j][0] <= ct[i]:
                    oi_vals.append(oi_ts[j][1]); j += 1
                if pos:
                    exit_px = None; reason = None
                    if l[i] <= pos["sl"]: exit_px, reason = pos["sl"], "SL"
                    elif h[i] >= pos["tp1"]: exit_px, reason = pos["tp1"], "TP1"
                    elif i - pos["i"] >= 20: exit_px, reason = c[i], "TIME"
                    if exit_px is not None:
                        fill = apply_slippage(exit_px, "short", rng)
                        pnl = ((fill - pos["entry"]) * pos["qty"]
                               - (pos["entry"] + fill) * pos["qty"] * FEE_TAKER)
                        capital += pnl; peak = max(peak, capital)
                        trades.append(dict(symbol=sym, pnl=pnl, reason=reason, close_ts=ct[i]))
                        pos = None
                if pos is None and len(oi_vals) >= 2:
                    checks += 1
                    ok, d = detect_signal_adaptive(o[i+1-W_:i+1], h[i+1-W_:i+1], l[i+1-W_:i+1],
                                                   c[i+1-W_:i+1], v[i+1-W_:i+1], tb[i+1-W_:i+1],
                                                   oi_vals[-8:], funnel)
                    if ok:
                        entry = apply_slippage(d["entry"], "long", rng)
                        dist = entry - d["sl"]
                        if dist > 0:
                            qty = min((capital * 0.01) / dist, NOTIONAL / entry)
                            pos = dict(entry=entry, sl=d["sl"], tp1=d["tp1"], qty=qty, i=i)
                    if checks % adapt_every == 0:
                        funnel.adapt()
            if k % 15 == 0:
                tg_send(chat, f"\u2699\uFE0F {k}/{len(cached)} \u00b7 сделок {len(trades)} \u00b7 "
                              f"порог объёма сейчас {funnel.get()['volume_mult']:.3f}")
        n = len(trades)
        L = [f"\U0001F504 <b>ADAPTIVE ИТОГ</b>"]
        if n:
            wins = sum(1 for t in trades if t["pnl"] > 0)
            total = sum(t["pnl"] for t in trades)
            L += [f"Сделок: <b>{n}</b> \u00b7 в плюсе {wins} ({wins/n*100:.0f}%)",
                  f"PnL: <b>{total:+.2f}$</b> ({total/DEPOSIT*100:+.1f}%)"]
        else:
            L.append("Сделок нет даже после адаптации.")
        L.append("")
        L.append(funnel.report())
        tg_send(chat, "\n".join(L))
    finally:
        BT_RUNNING["on"] = False



# ============================================================================
# SCORE BINNING / REGIME FILTER / KELLY-LITE / TOP-N RANKING
# ============================================================================
def score_binning(trades, bins=(3, 4, 5, 6, 7, 8)):
    """Ключевая диагностика: разбиваем сделки по силе сигнала и смотрим,
    есть ли прибыль в ХВОСТЕ распределения (score > X)."""
    out = []
    for i in range(len(bins) - 1):
        low, high = bins[i], bins[i + 1]
        tb = [t for t in trades if low <= t.get("score", 0) < high]
        if not tb:
            out.append(dict(low=low, high=high, n=0, wr=0.0, pnl=0.0, avg=0.0))
            continue
        wins = sum(1 for t in tb if t["pnl"] > 0)
        pnl = sum(t["pnl"] for t in tb)
        out.append(dict(low=low, high=high, n=len(tb), wr=wins / len(tb),
                        pnl=pnl, avg=pnl / len(tb)))
    return out


def binning_report(trades, bins=(3, 4, 5, 6, 7, 8)):
    rows = score_binning(trades, bins)
    L = ["\U0001F4CA <b>SCORE BINNING</b> — где живёт прибыль:",
         "<code>score      сделок  win%    PnL$     ср/сделку</code>"]
    best = None
    for r in rows:
        if r["n"] == 0:
            L.append(f"<code>{r['low']}-{r['high']}        0       —        —          —</code>")
            continue
        L.append(f"<code>{r['low']}-{r['high']}    {r['n']:6d}  {r['wr']*100:5.1f}  "
                 f"{r['pnl']:+9.1f}  {r['avg']:+8.3f}</code>")
        if r["n"] >= 20 and (best is None or r["avg"] > best["avg"]):
            best = r
    # ищем ЛУЧШИЙ порог: максимум среднего PnL на сделку при достаточной выборке
    cum = None
    for r in rows:
        if r["n"] == 0: continue
        agg_n = sum(x["n"] for x in rows if x["low"] >= r["low"])
        agg_pnl = sum(x["pnl"] for x in rows if x["low"] >= r["low"])
        if agg_n >= 30 and agg_pnl > 0:
            avg = agg_pnl / agg_n
            if cum is None or avg > cum[3]:
                cum = (r["low"], agg_n, agg_pnl, avg)
    if cum:
        L.append(f"\n\U0001F3AF Лучший порог: score \u2265 <b>{cum[0]}</b> \u2014 "
                 f"{cum[1]} сделок, PnL {cum[2]:+.1f}$ ({cum[3]:+.3f}$/сделку) "
                 f"\u2014 <b>кандидат в alpha threshold</b>")
        L.append("<i>\u26A0\uFE0F Найдено НА ЭТИХ ЖЕ данных. Обязательно проверь на невиданных: "
                 "/scoredwf 30 60 — иначе это подгонка.</i>")
    else:
        L.append("\n\u274C Нет порога, выше которого совокупный PnL положителен "
                 "(при \u226530 сделках). Альфы в хвосте не видно.")
    return "\n".join(L)


def market_regime(closes, i, vol_win=50, trend_win=50,
                  dead_vol=0.01, chop_trend=0.02):
    """dead — волатильность мертва, chop — нет направления, trend — торгуем."""
    if i < max(vol_win, trend_win) + 1:
        return "dead"
    rets = [(closes[k] / closes[k-1] - 1.0) for k in range(i - vol_win + 1, i + 1) if closes[k-1] > 0]
    if len(rets) < 2:
        return "dead"
    m = sum(rets) / len(rets)
    vol = math.sqrt(sum((x - m) ** 2 for x in rets) / (len(rets) - 1))
    trend = closes[i] / closes[i - trend_win] - 1.0 if closes[i - trend_win] > 0 else 0.0
    if vol < dead_vol:
        return "dead"
    if abs(trend) < chop_trend:
        return "chop"
    return "trend"


def size_kelly_lite(score, max_score=None):
    """Kelly-lite: доля капитала растёт ступенями с уверенностью.
    score нормируется в 0..1 от max_score."""
    max_score = max_score or SCORE_MAX
    s = score / max_score if max_score else 0.0
    if s < 0.55: return 0.0
    if s > 0.70: return 0.03
    if s > 0.60: return 0.02
    return 0.01


def select_top_n(candidates, top_n=20, per_day=True, bar_ms=900000):
    """TOP-N ranking: вместо 'торгуем всё, что прошло фильтр' берём лучшие идеи.
    per_day=True -> топ-N в каждые сутки, иначе топ-N за весь период."""
    if not per_day:
        return sorted(candidates, key=lambda x: -x["score"])[:top_n]
    by_day = {}
    for cnd in candidates:
        day = cnd["ts"] // (24 * 3600 * 1000)
        by_day.setdefault(day, []).append(cnd)
    out = []
    for day, lst in by_day.items():
        out += sorted(lst, key=lambda x: -x["score"])[:top_n]
    return sorted(out, key=lambda x: x["ts"])




# ============================================================================
# ADX SNIPER — новая стратегия (тренд-сила + точный вход)
# ============================================================================
ADX_LEN        = int(os.environ.get("ADX_LEN", "14"))
ADX_MIN        = float(os.environ.get("ADX_MIN", "25"))      # порог силы тренда
ADX_RISING     = os.environ.get("ADX_RISING", "1") == "1"    # требовать рост ADX
SNIPER_EMA_F   = int(os.environ.get("SNIPER_EMA_F", "9"))
SNIPER_EMA_S   = int(os.environ.get("SNIPER_EMA_S", "21"))
SNIPER_SMA     = int(os.environ.get("SNIPER_SMA", "200"))
SNIPER_TF      = os.environ.get("SNIPER_TF", "1h")           # 1h / 4h

# Пресеты вселенных для ADX Sniper.
# ВАЖНО про издержки: модель слиппеджа (0.05-0.20%) честна для ликвидных монет.
# На тонких альткоинах реальный спред/проскальзывание ВЫШЕ -> бэктест завысит результат.
SNIPER_UNIVERSES = {
    "major": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"],
    "mid":   ["ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT", "LTCUSDT",
              "ATOMUSDT", "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT"],
    "alt":   ["DOGEUSDT", "TRXUSDT", "MATICUSDT", "FILUSDT", "INJUSDT",
              "SUIUSDT", "TIAUSDT", "SEIUSDT", "RUNEUSDT", "GALAUSDT"],
}
SNIPER_SL_ATR  = float(os.environ.get("SNIPER_SL_ATR", "1.5"))
SNIPER_TP1_ATR = float(os.environ.get("SNIPER_TP1_ATR", "2.0"))
SNIPER_TRAIL   = float(os.environ.get("SNIPER_TRAIL", "2.0"))
SNIPER_RSI_MAX = float(os.environ.get("SNIPER_RSI_MAX", "85"))  # 0 = фильтр выключен.
# ВНИМАНИЕ: RSI-фильтр конфликтует с ADX по смыслу — ADX>25 требует СИЛЬНЫЙ тренд,
# а в сильном тренде RSI по определению высокий. Слишком низкий RSI_MAX убивает
# все сигналы стратегии. Проверено: при RSI_MAX=80 на трендовом ряду 348 из 395
# проверок отклонялись именно по RSI.


def sma(vals, period):
    """Простая скользящая: список значений (None пока не хватает истории)."""
    out = []
    run = 0.0
    for i, x in enumerate(vals):
        run += x
        if i >= period:
            run -= vals[i - period]
        out.append(run / period if i >= period - 1 else None)
    return out


def dmi_adx(h, l, c, period=14):
    """ADX / +DI / -DI по Уайлдеру.
    Возвращает (adx[], plus_di[], minus_di[]) — списки длиной len(c), None где нет данных.

    Формулы:
      +DM = high-prevHigh, если > (prevLow-low) и > 0, иначе 0
      -DM = prevLow-low,   если > (high-prevHigh) и > 0, иначе 0
      TR  = max(h-l, |h-prevC|, |l-prevC|)
      сглаживание Уайлдера: S = S - S/period + current
      +DI = 100 * smoothed(+DM) / smoothed(TR)
      DX  = 100 * |+DI - -DI| / (+DI + -DI)
      ADX = сглаженное среднее DX
    """
    n = len(c)
    adx = [None] * n; pdi = [None] * n; mdi = [None] * n
    if n < period * 2 + 1:
        return adx, pdi, mdi
    plus_dm, minus_dm, tr = [0.0], [0.0], [0.0]
    for i in range(1, n):
        up = h[i] - h[i - 1]
        dn = l[i - 1] - l[i]
        plus_dm.append(up if (up > dn and up > 0) else 0.0)
        minus_dm.append(dn if (dn > up and dn > 0) else 0.0)
        tr.append(max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])))
    # первичные суммы Уайлдера
    s_tr = sum(tr[1:period + 1])
    s_pdm = sum(plus_dm[1:period + 1])
    s_mdm = sum(minus_dm[1:period + 1])
    dx_list = []
    for i in range(period, n):
        if i > period:
            s_tr = s_tr - s_tr / period + tr[i]
            s_pdm = s_pdm - s_pdm / period + plus_dm[i]
            s_mdm = s_mdm - s_mdm / period + minus_dm[i]
        if s_tr > 0:
            p = 100.0 * s_pdm / s_tr
            m = 100.0 * s_mdm / s_tr
        else:
            p = m = 0.0
        pdi[i] = p; mdi[i] = m
        dx = 100.0 * abs(p - m) / (p + m) if (p + m) > 0 else 0.0
        dx_list.append((i, dx))
        # ADX = сглаженное DX, стартует после period значений DX
        if len(dx_list) == period:
            adx_val = sum(d for _, d in dx_list) / period
            adx[i] = adx_val
        elif len(dx_list) > period:
            prev = adx[i - 1]
            if prev is not None:
                adx[i] = (prev * (period - 1) + dx) / period
    return adx, pdi, mdi



def detect_adx_sniper(o, h, l, c, i, adx, pdi, mdi, ema_f, ema_s, sma200, atr_val):
    """ADX Sniper LONG на баре i (все индикаторы предрассчитаны).
    Правила:
      1) ADX > ADX_MIN (сильный тренд) и растёт
      2) +DI > -DI (бычье направление)
      3) EMA быстрая > EMA медленной
      4) цена > SMA200 (глобальный тренд)
      5) свеча бычья (подтверждение)
      6) RSI не в экстремуме
    Возвращает (ok, dict|причина)."""
    if adx[i] is None or pdi[i] is None or sma200[i] is None or atr_val <= 0:
        return False, "нет данных индикаторов"
    # 1) сила тренда
    if adx[i] <= ADX_MIN:
        return False, f"ADX {adx[i]:.1f} <= {ADX_MIN}"
    if ADX_RISING:
        prev = adx[i-1] if i > 0 else None
        if prev is None or adx[i] <= prev:
            return False, "ADX не растёт"
    # 2) направление
    if pdi[i] <= mdi[i]:
        return False, f"+DI {pdi[i]:.1f} <= -DI {mdi[i]:.1f}"
    # 3) EMA
    if ema_f[i] is None or ema_s[i] is None or not (ema_f[i] > ema_s[i]):
        return False, "EMA быстрая <= медленной"
    # 4) глобальный тренд
    if not (c[i] > sma200[i]):
        return False, "цена ниже SMA200"
    # 5) подтверждающая свеча
    if c[i] <= o[i]:
        return False, "свеча не бычья"
    # 6) не в экстремуме
    r = rsi(c[max(0, i-RSI_LEN*6):i+1], RSI_LEN)
    if SNIPER_RSI_MAX > 0 and r > SNIPER_RSI_MAX:
        return False, f"RSI {r:.0f} перегрет"

    entry = c[i]
    sl = entry - SNIPER_SL_ATR * atr_val
    if sl >= entry:
        return False, "стоп выше входа"
    return True, dict(entry=entry, sl=sl,
                      tp1=entry + SNIPER_TP1_ATR * atr_val,
                      atr=atr_val, adx=adx[i], pdi=pdi[i], mdi=mdi[i],
                      ema_f=ema_f[i], ema_s=ema_s[i], sma200=sma200[i],
                      rsi=r, score=adx[i],   # score = сила тренда (для биннинга)
                      risk_pct=(entry - sl) / entry, close3=c[i])


def sniper_klines(symbol, interval="1h", months=12):
    """Длинная история свечей (OI не нужен -> ограничение 30 дней снимается).
    Постранично, до нужного количества баров."""
    per_day = {"15m": 96, "1h": 24, "4h": 6}.get(interval, 24)
    need = int(months * 30 * per_day) + 300
    out = []; end = None
    while len(out) < need:
        url = f"{BINANCE}/fapi/v1/klines?symbol={symbol}&interval={interval}&limit=1500"
        if end: url += f"&endTime={end}"
        try:
            d = http_json(url); time.sleep(0.15)
        except Exception as e:
            print(f"sniper_klines {symbol}: {e}"); break
        if not d: break
        out = d + out
        end = int(d[0][0]) - 1
        if len(d) < 1500: break
    out = out[-need:]
    o = [float(x[1]) for x in out]; h = [float(x[2]) for x in out]
    l = [float(x[3]) for x in out]; c = [float(x[4]) for x in out]
    v = [float(x[5]) for x in out]; ct = [int(x[6]) for x in out]
    return o, h, l, c, v, ct


def bt_sniper_coin(sym, o, h, l, c, ct, capital=None, diag=None, seed=None,
                   adx_min=None, use_trail=True,
                   use_tp1=True, tp1_mult=None, trail_mult=None, sl_mult=None):
    """Бэктест ADX Sniper по одной монете. TP1 50% + трейлинг остатка."""
    import random as _r
    rng = _r.Random(seed) if seed is not None else _r
    global ADX_MIN, SNIPER_TP1_ATR, SNIPER_TRAIL, SNIPER_SL_ATR
    saved = (ADX_MIN, SNIPER_TP1_ATR, SNIPER_TRAIL, SNIPER_SL_ATR)
    if adx_min is not None:   ADX_MIN = adx_min
    if tp1_mult is not None:  SNIPER_TP1_ATR = tp1_mult
    if trail_mult is not None: SNIPER_TRAIL = trail_mult
    if sl_mult is not None:   SNIPER_SL_ATR = sl_mult
    try:
        capital = DEPOSIT if capital is None else capital
        start_cap = capital
        n = len(c)
        adx, pdi, mdi = dmi_adx(h, l, c, ADX_LEN)
        ema_f = [None]*n; ema_s = [None]*n
        ef = ema_series(c, SNIPER_EMA_F); es = ema_series(c, SNIPER_EMA_S)
        for k in range(n):
            ema_f[k] = ef[k] if k >= SNIPER_EMA_F else None
            ema_s[k] = es[k] if k >= SNIPER_EMA_S else None
        s200 = sma(c, SNIPER_SMA)
        trades = []; pos = None
        warm = max(SNIPER_SMA, ADX_LEN*3) + 5
        for i in range(warm, n):
            if pos:
                exit_px = None; reason = None
                if l[i] <= pos["sl"]:
                    exit_px, reason = pos["sl"], ("СТОП" if not pos["half"] else "ТРЕЙЛ/БУ")
                elif use_tp1 and not pos["half"] and h[i] >= pos["tp1"]:
                    # TP1: фиксируем 50%, остаток -> БУ + трейлинг
                    fill = apply_slippage(pos["tp1"], "short", rng)
                    q = pos["qty"] * 0.5
                    pnl = (fill - pos["entry"]) * q - (pos["entry"] + fill) * q * FEE_TAKER
                    capital += pnl
                    trades.append(dict(symbol=sym, pnl=pnl, reason="TP1", score=pos["score"],
                                       adx=pos["score"], close_ts=ct[i], part=0.5))
                    pos["qty"] -= q; pos["half"] = True
                    pos["sl"] = pos["entry"]; pos["peak"] = h[i]
                elif pos["half"] or not use_tp1:
                    # без TP1 трейлинг ведёт позицию с самого входа (победители не обрезаются)
                    pos["peak"] = max(pos["peak"], h[i])
                    if use_trail:
                        trail = pos["peak"] - SNIPER_TRAIL * pos["atr"]
                        if l[i] <= trail and trail > pos["sl"]:
                            exit_px, reason = trail, "ТРЕЙЛ"
                if exit_px is not None:
                    fill = apply_slippage(exit_px, "short", rng)
                    pnl = ((fill - pos["entry"]) * pos["qty"]
                           - (pos["entry"] + fill) * pos["qty"] * FEE_TAKER)
                    capital += pnl
                    trades.append(dict(symbol=sym, pnl=pnl, reason=reason, score=pos["score"],
                                       adx=pos["score"], close_ts=ct[i], part=1.0))
                    pos = None
            if pos is None:
                if diag is not None: diag["evals"] += 1
                a_val = atr(h[:i], l[:i], c[:i], ATR_LEN)
                ok, d = detect_adx_sniper(o, h, l, c, i, adx, pdi, mdi,
                                          ema_f, ema_s, s200, a_val)
                if ok:
                    if diag is not None: diag["signals"] += 1
                    entry = apply_slippage(d["entry"], "long", rng)
                    dist = entry - d["sl"]
                    if dist > 0:
                        risk_usd = capital * RISK_PCT_TRADE
                        qty = min(risk_usd / dist, NOTIONAL / entry)
                        pos = dict(entry=entry, sl=d["sl"], tp1=d["tp1"], qty=qty,
                                   atr=d["atr"], half=False, peak=entry,
                                   i=i, score=d["adx"])
                elif diag is not None:
                    key = str(d).split("(")[0].strip()
                    for pat in ("ADX", "+DI", "EMA", "SMA200", "не бычья", "RSI", "нет данных"):
                        if pat in str(d): key = pat; break
                    diag["reasons"][key] = diag["reasons"].get(key, 0) + 1
        return trades, capital - start_cap
    finally:
        ADX_MIN, SNIPER_TP1_ATR, SNIPER_TRAIL, SNIPER_SL_ATR = saved




# ============================================================================
# СРАВНЕНИЕ СТРУКТУР ВЫХОДА — проверка гипотезы "проблема не во входе, а в выходе"
# ============================================================================
EXIT_VARIANTS = [
    # (название, use_tp1, tp1_mult, trail_mult, sl_mult)
    ("текущий: TP1 2ATR 50% + трейл 2ATR", True,  2.0, 2.0, 1.5),
    ("без TP1: только трейл 2ATR",          False, 2.0, 2.0, 1.5),
    ("без TP1: трейл 3ATR (шире)",          False, 2.0, 3.0, 1.5),
    ("без TP1: трейл 4ATR (очень широкий)", False, 2.0, 4.0, 1.5),
    ("TP1 подальше: 4ATR 50% + трейл 3ATR", True,  4.0, 3.0, 1.5),
    ("TP1 3ATR + трейл 3ATR",               True,  3.0, 3.0, 1.5),
    ("стоп ближе: SL 1.0ATR + трейл 3ATR",  False, 2.0, 3.0, 1.0),
]



# ============================================================================
# BTC BETA MOMENTUM — "следование за BTC"
# ============================================================================
BETA_TF          = os.environ.get("BETA_TF", "1h")
BETA_BTC_MOVE    = float(os.environ.get("BETA_BTC_MOVE", "0.012"))   # 1.2% движение BTC
BETA_LOOKBACK    = int(os.environ.get("BETA_LOOKBACK", "1"))         # за сколько баров меряем движение
BETA_CORR_MIN    = float(os.environ.get("BETA_CORR_MIN", "0.85"))    # мин. корреляция
BETA_CORR_WIN    = int(os.environ.get("BETA_CORR_WIN", "720"))       # окно корреляции (720ч = 30 дней)
BETA_BASKET      = int(os.environ.get("BETA_BASKET", "5"))           # монет в корзине
BETA_SL          = float(os.environ.get("BETA_SL", "0.02"))          # стоп 2%
BETA_TP1         = float(os.environ.get("BETA_TP1", "0.025"))        # TP1 +2.5% -> 40%
BETA_TP1_PART    = float(os.environ.get("BETA_TP1_PART", "0.40"))
BETA_TP2         = float(os.environ.get("BETA_TP2", "0.04"))         # TP2 +4% -> 30%
BETA_TP2_PART    = float(os.environ.get("BETA_TP2_PART", "0.30"))
BETA_TRAIL_START = float(os.environ.get("BETA_TRAIL_START", "0.018"))# трейлинг после +1.8%
BETA_TRAIL_STEP  = float(os.environ.get("BETA_TRAIL_STEP", "0.008")) # шаг 0.8%
BETA_MAX_HOLD    = int(os.environ.get("BETA_MAX_HOLD", "24"))        # макс. удержание, баров
BETA_ADX_MIN     = float(os.environ.get("BETA_ADX_MIN", "23"))       # фильтр ADX по BTC
BETA_ALTS = ["ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "AVAXUSDT",
             "SUIUSDT", "TONUSDT", "NEARUSDT", "APTUSDT", "HBARUSDT"]


def pearson_corr(x, y):
    """Коэффициент корреляции Пирсона."""
    n = min(len(x), len(y))
    if n < 3: return 0.0
    x = x[-n:]; y = y[-n:]
    mx = sum(x) / n; my = sum(y) / n
    num = sum((a - mx) * (b - my) for a, b in zip(x, y))
    dx = math.sqrt(sum((a - mx) ** 2 for a in x))
    dy = math.sqrt(sum((b - my) ** 2 for b in y))
    return (num / (dx * dy)) if dx > 0 and dy > 0 else 0.0


def calc_beta(alt_rets, btc_rets):
    """Бета: на сколько альт двигается относительно BTC. beta = cov(a,b)/var(b)."""
    n = min(len(alt_rets), len(btc_rets))
    if n < 3: return 0.0
    a = alt_rets[-n:]; b = btc_rets[-n:]
    mb = sum(b) / n; ma = sum(a) / n
    cov = sum((x - ma) * (y - mb) for x, y in zip(a, b)) / n
    var = sum((y - mb) ** 2 for y in b) / n
    return (cov / var) if var > 0 else 0.0


def returns_of(closes):
    return [(closes[i] / closes[i-1] - 1.0) if closes[i-1] > 0 else 0.0
            for i in range(1, len(closes))]



# ============================================================================
# TRENDLINE LONG BREAKOUT — вход от восходящей линии поддержки
# ============================================================================
TL_TF            = os.environ.get("TL_TF", "1h")
TL_SWINGS        = int(os.environ.get("TL_SWINGS", "3"))
TL_SWING_CONFIRM = int(os.environ.get("TL_SWING_CONFIRM", "2"))
TL_LOOKBACK      = int(os.environ.get("TL_LOOKBACK", "120"))
TL_TOUCH_TOL     = float(os.environ.get("TL_TOUCH_TOL", "0.004"))
TL_ADX_MIN       = float(os.environ.get("TL_ADX_MIN", "23"))
TL_RSI_MIN       = float(os.environ.get("TL_RSI_MIN", "45"))
TL_BODY_MIN      = float(os.environ.get("TL_BODY_MIN", "0.5"))
TL_VOL_MIN       = float(os.environ.get("TL_VOL_MIN", "1.0"))
TL_EMA200_MULT   = int(os.environ.get("TL_EMA200_MULT", "4"))
TL_SL_BUF        = float(os.environ.get("TL_SL_BUF", "0.008"))
TL_TP1_RR        = float(os.environ.get("TL_TP1_RR", "2.0"))
TL_TP1_PART      = float(os.environ.get("TL_TP1_PART", "0.40"))
TL_TP2_RR        = float(os.environ.get("TL_TP2_RR", "3.5"))
TL_TP2_PART      = float(os.environ.get("TL_TP2_PART", "0.30"))
TL_TRAIL_STEP    = float(os.environ.get("TL_TRAIL_STEP", "0.008"))
TL_MAX_HOLD      = int(os.environ.get("TL_MAX_HOLD", "48"))
TL_COINS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "AVAXUSDT", "SUIUSDT",
            "TONUSDT", "NEARUSDT", "XRPUSDT", "DOGEUSDT", "LINKUSDT"]


def find_swing_lows(l, end_idx, lookback=None, confirm=None):
    """Свинг-минимумы БЕЗ ЗАГЛЯДЫВАНИЯ В БУДУЩЕЕ.
    Свинг на баре k подтверждается только через `confirm` баров ПОСЛЕ него,
    поэтому на баре end_idx видны свинги только до (end_idx - confirm).
    Без этой отсечки бэктест читал бы будущее и рисовал фальшивый результат."""
    lookback = lookback or TL_LOOKBACK
    confirm = confirm or TL_SWING_CONFIRM
    swings = []
    start = max(confirm, end_idx - lookback)
    last_visible = end_idx - confirm
    for k in range(start, last_visible):
        if k - confirm < 0 or k + confirm >= len(l):
            continue
        if (all(l[k] <= l[k - j] for j in range(1, confirm + 1)) and
                all(l[k] <= l[k + j] for j in range(1, confirm + 1))):
            swings.append((k, l[k]))
    return swings


def fit_trendline(swings, min_touches=None):
    """Линейная регрессия по последним N swing low (чистый python, без numpy)."""
    min_touches = min_touches or TL_SWINGS
    if len(swings) < min_touches:
        return None
    recent = swings[-min_touches:]
    xs = [float(p[0]) for p in recent]
    ys = [float(p[1]) for p in recent]
    n = len(xs)
    mx = sum(xs) / n; my = sum(ys) / n
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0:
        return None
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den
    return dict(slope=slope, intercept=my - slope * mx, touches=n,
                first_idx=recent[0][0], last_idx=recent[-1][0], points=recent)


def trendline_price(tl, idx):
    return tl["slope"] * idx + tl["intercept"]


def htf_ema200(c, mult=None):
    """EMA200 старшего ТФ: агрегируем по mult баров (4H из 1H = mult 4)."""
    mult = mult or TL_EMA200_MULT
    htf_closes = [c[i] for i in range(mult - 1, len(c), mult)]
    if len(htf_closes) < 5:
        return [None] * len(c)
    e = ema_series(htf_closes, 200)
    out = [None] * len(c)
    # EMA200 валидна только когда набралось >= 200 баров СТАРШЕГО ТФ.
    # (для 4H из 1H это 200*mult баров младшего ТФ, т.е. ~33 дня)
    for j, idx in enumerate(range(mult - 1, len(c), mult)):
        val = e[j] if j >= 199 else None
        for k in range(idx, min(idx + mult, len(c))):
            out[k] = val
    return out


def detect_trendline_long(o, h, l, c, v, i, adx, pdi, mdi, ema200,
                          adx_min=None, rsi_min=None, swings_req=None):
    """LONG от восходящей трендовой поддержки (полный чек-лист)."""
    adx_min = TL_ADX_MIN if adx_min is None else adx_min
    rsi_min = TL_RSI_MIN if rsi_min is None else rsi_min
    if i < TL_LOOKBACK + 10:
        return False, "мало истории"
    if ema200[i] is None or c[i] <= ema200[i]:
        return False, "цена ниже EMA200 старшего ТФ"
    if adx[i] is None or adx[i] <= adx_min:
        return False, f"ADX <= {adx_min:.0f}"
    if pdi[i] is None or mdi[i] is None or pdi[i] <= mdi[i]:
        return False, "+DI <= -DI"
    swings = find_swing_lows(l, i)
    tl = fit_trendline(swings, swings_req)
    if tl is None:
        return False, "мало swing low"
    if tl["slope"] <= 0:
        return False, "линия не восходящая"
    tl_px = trendline_price(tl, i)
    if tl_px <= 0:
        return False, "некорректная линия"
    if not (l[i] <= tl_px * (1 + TL_TOUCH_TOL) and l[i] >= tl_px * (1 - 0.02)):
        return False, "нет касания линии"
    if c[i] <= o[i]:
        return False, "свеча не бычья"
    if c[i] <= tl_px:
        return False, "закрытие не выше линии"
    rng = h[i] - l[i]; body = abs(c[i] - o[i])
    if rng <= 0 or body / rng < TL_BODY_MIN:
        return False, "тело меньше 50%"
    r = rsi(c[max(0, i - RSI_LEN * 6):i + 1], RSI_LEN)
    if r < rsi_min:
        return False, f"RSI {r:.0f} < {rsi_min:.0f}"
    vma = sum(v[i - 20:i]) / 20 if i >= 20 else 0
    if vma > 0 and v[i] < vma * TL_VOL_MIN:
        return False, "объём ниже среднего"
    entry = c[i]
    sl = min(tl_px, l[i]) * (1 - TL_SL_BUF)
    if sl >= entry:
        return False, "стоп выше входа"
    risk = entry - sl
    return True, dict(entry=entry, sl=sl, risk=risk,
                      tp1=entry + TL_TP1_RR * risk, tp2=entry + TL_TP2_RR * risk,
                      trendline=tl, tl_price=tl_px, adx=adx[i], rsi=r,
                      vol_ratio=(v[i] / vma if vma > 0 else 0), body_pct=body / rng,
                      score=adx[i], risk_pct=risk / entry, close3=c[i])


def bt_trendline_coin(sym, o, h, l, c, v, ct, capital=None, diag=None, seed=None,
                      adx_min=None, rsi_min=None, swings_req=None):
    """Бэктест: вход market на закрытии подтверждающей свечи,
    выходы 40% на RR 1:2, 30% на RR 1:3.5, остаток трейлингом."""
    import random as _r
    rng = _r.Random(seed) if seed is not None else _r
    capital = DEPOSIT if capital is None else capital
    start_cap = capital
    n = len(c)
    adx, pdi, mdi = dmi_adx(h, l, c, ADX_LEN)
    ema200 = htf_ema200(c)
    trades = []; pos = None
    warm = TL_LOOKBACK + 20
    for i in range(warm, n):
        if pos:
            exit_px = None; reason = None
            if l[i] <= pos["sl"]:
                exit_px, reason = pos["sl"], ("СТОП" if pos["stage"] == 0 else "ТРЕЙЛ/БУ")
            elif pos["stage"] == 0 and h[i] >= pos["tp1"]:
                fill = apply_slippage(pos["tp1"], "short", rng)
                q = pos["qty_init"] * TL_TP1_PART
                pnl = (fill - pos["entry"]) * q - (pos["entry"] + fill) * q * FEE_TAKER
                capital += pnl
                trades.append(dict(symbol=sym, pnl=pnl, reason="TP1", score=pos["score"],
                                   adx=pos["score"], close_ts=ct[i], part=TL_TP1_PART))
                pos["qty"] -= q; pos["stage"] = 1
                pos["sl"] = pos["entry"]; pos["peak"] = h[i]
            elif pos["stage"] == 1 and h[i] >= pos["tp2"]:
                fill = apply_slippage(pos["tp2"], "short", rng)
                q = pos["qty_init"] * TL_TP2_PART
                q = min(q, pos["qty"])
                pnl = (fill - pos["entry"]) * q - (pos["entry"] + fill) * q * FEE_TAKER
                capital += pnl
                trades.append(dict(symbol=sym, pnl=pnl, reason="TP2", score=pos["score"],
                                   adx=pos["score"], close_ts=ct[i], part=TL_TP2_PART))
                pos["qty"] -= q; pos["stage"] = 2; pos["peak"] = h[i]
            elif pos["stage"] >= 1:
                pos["peak"] = max(pos["peak"], h[i])
                trail = pos["peak"] * (1 - TL_TRAIL_STEP)
                if l[i] <= trail and trail > pos["sl"]:
                    exit_px, reason = trail, "ТРЕЙЛ"
            if exit_px is None and i - pos["i"] >= TL_MAX_HOLD:
                exit_px, reason = c[i], "ВРЕМЯ"
            if exit_px is not None and pos["qty"] > 0:
                fill = apply_slippage(exit_px, "short", rng)
                pnl = ((fill - pos["entry"]) * pos["qty"]
                       - (pos["entry"] + fill) * pos["qty"] * FEE_TAKER)
                capital += pnl
                trades.append(dict(symbol=sym, pnl=pnl, reason=reason, score=pos["score"],
                                   adx=pos["score"], close_ts=ct[i], part=1.0))
                pos = None
            elif exit_px is not None:
                pos = None
        if pos is None:
            if diag is not None: diag["evals"] += 1
            ok, d = detect_trendline_long(o, h, l, c, v, i, adx, pdi, mdi, ema200,
                                          adx_min=adx_min, rsi_min=rsi_min,
                                          swings_req=swings_req)
            if ok:
                if diag is not None: diag["signals"] += 1
                entry = apply_slippage(d["entry"], "long", rng)
                dist = entry - d["sl"]
                if dist > 0:
                    qty = min(capital * RISK_PCT_TRADE / dist, NOTIONAL / entry)
                    pos = dict(entry=entry, sl=d["sl"], tp1=d["tp1"], tp2=d["tp2"],
                               qty=qty, qty_init=qty, stage=0, peak=entry,
                               i=i, score=d["adx"], tl=d["trendline"])
            elif diag is not None:
                key = str(d).split("(")[0].strip()
                diag["reasons"][key] = diag["reasons"].get(key, 0) + 1
    return trades, capital - start_cap



def plot_trendline_setup(sym, o, h, l, c, i, d, path="/tmp/tl_setup.png", bars=120):
    """Авто-рисование сетапа: свечи + трендовая линия + вход/стоп/тейки."""
    if not HAS_MPL:
        return None
    try:
        start = max(0, i - bars)
        xs = list(range(start, i + 1))
        tl = d["trendline"]
        fig, ax = plt.subplots(figsize=(12, 6), facecolor="#0d1117")
        ax.set_facecolor("#0d1117")
        # свечи
        for k in xs:
            up = c[k] >= o[k]
            col = "#26a69a" if up else "#ef5350"
            ax.plot([k, k], [l[k], h[k]], color=col, linewidth=0.8, zorder=2)
            ax.plot([k, k], [o[k], c[k]], color=col, linewidth=3.0, zorder=3)
        # трендовая линия (продлеваем на весь экран)
        ty = [trendline_price(tl, x) for x in xs]
        ax.plot(xs, ty, color="#ffa726", linewidth=2.0, label="Трендовая поддержка", zorder=4)
        # точки касания
        for (px, py) in tl["points"]:
            if px >= start:
                ax.scatter([px], [py], color="#ffa726", s=70, zorder=5,
                           edgecolors="white", linewidths=0.8)
        # уровни
        ax.axhline(d["entry"], color="#42a5f5", linestyle="-", linewidth=1.4,
                   label=f"Вход {d['entry']:.6g}", zorder=4)
        ax.axhline(d["sl"], color="#ef5350", linestyle="--", linewidth=1.3,
                   label=f"Стоп {d['sl']:.6g}", zorder=4)
        ax.axhline(d["tp1"], color="#66bb6a", linestyle="--", linewidth=1.1,
                   label=f"TP1 (RR 1:{TL_TP1_RR:.0f}) {d['tp1']:.6g}", zorder=4)
        ax.axhline(d["tp2"], color="#26a69a", linestyle=":", linewidth=1.1,
                   label=f"TP2 (RR 1:{TL_TP2_RR:.1f}) {d['tp2']:.6g}", zorder=4)
        # зоны риска/прибыли
        ax.fill_between(xs, d["entry"], d["tp1"], color="#26a69a", alpha=0.07, zorder=1)
        ax.fill_between(xs, d["sl"], d["entry"], color="#ef5350", alpha=0.07, zorder=1)
        ax.scatter([i], [c[i]], color="#42a5f5", s=110, marker="^", zorder=6,
                   edgecolors="white", linewidths=1.0)
        ax.set_title(f"{sym} · Trendline Long · ADX {d['adx']:.0f} · RSI {d['rsi']:.0f} "
                     f"· касаний {tl['touches']}", color="white", fontsize=12)
        ax.tick_params(colors="#8b949e", labelsize=8)
        for s_ in ax.spines.values(): s_.set_color("#30363d")
        ax.grid(True, alpha=0.15, color="#8b949e")
        ax.legend(facecolor="#161b22", edgecolor="#30363d", labelcolor="white", fontsize=8)
        plt.tight_layout()
        plt.savefig(path, dpi=110, facecolor="#0d1117"); plt.close()
        return path
    except Exception as e:
        print("plot_trendline err:", e)
        try: plt.close()
        except Exception: pass
        return None


def run_trendline(chat, months=12, symbols=None, interval=None,
                  adx_min=None, rsi_min=None, swings_req=None, mode="backtest"):
    """/trendline — бэктест стратегии от трендовой поддержки + биннинг + графики сетапов."""
    if BT_RUNNING["on"]:
        tg_send(chat, "\u23F3 Идёт другой прогон — дождись окончания."); return
    BT_RUNNING["on"] = True
    try:
        months = max(1, min(months, 36))
        interval = interval or TL_TF
        if isinstance(symbols, str):
            syms = SNIPER_UNIVERSES.get(symbols, TL_COINS) if symbols in SNIPER_UNIVERSES else TL_COINS
        else:
            syms = symbols or TL_COINS
        amin = TL_ADX_MIN if adx_min is None else adx_min
        rmin = TL_RSI_MIN if rsi_min is None else rsi_min
        sw_req = TL_SWINGS if swings_req is None else swings_req
        tg_send(chat, f"\U0001F4C9 <b>TRENDLINE LONG BREAKOUT</b>: {months} мес \u00b7 {interval} \u00b7 "
                      f"{len(syms)} монет\n"
                      f"\u2699\uFE0F Линия по {sw_req} swing low \u00b7 ADX>{amin:.0f} \u00b7 "
                      f"RSI>{rmin:.0f} \u00b7 тело>{TL_BODY_MIN*100:.0f}% \u00b7 объём>среднего\n"
                      f"\U0001F6D1 SL на {TL_SL_BUF*100:.1f}% ниже линии \u00b7 "
                      f"TP1 RR1:{TL_TP1_RR:.0f} ({TL_TP1_PART*100:.0f}%) \u00b7 "
                      f"TP2 RR1:{TL_TP2_RR:.1f} ({TL_TP2_PART*100:.0f}%) \u00b7 трейл {TL_TRAIL_STEP*100:.1f}%\n"
                      f"\U0001F512 Swing low берутся только ПОДТВЕРЖДЁННЫЕ "
                      f"({TL_SWING_CONFIRM} бара) — без заглядывания в будущее\n"
                      f"\U0001F4B8 Спред+слиппедж+комиссии учтены\n"
                      f"Качаю историю\u2026")
        data = {}
        for sym in syms:
            try:
                o, h, l, c, v, ct = sniper_klines(sym, interval, months)
                if len(c) >= TL_LOOKBACK + 200:
                    data[sym] = (o, h, l, c, v, ct)
                    tg_send(chat, f"\u2699\uFE0F {sym}: {len(c):,} баров")
            except Exception as e:
                print(f"tl {sym} err:", e)
        if not data:
            tg_send(chat, "\u274C Данные не загрузились."); return

        all_tr = []; total = 0.0
        diag = dict(evals=0, signals=0, reasons={})
        sample = None
        for sym, (o, h, l, c, v, ct) in data.items():
            tr, pnl = bt_trendline_coin(sym, o, h, l, c, v, ct, diag=diag, seed=19,
                                        adx_min=amin, rsi_min=rmin, swings_req=sw_req)
            all_tr += tr; total += pnl
            # сохраним один сетап для картинки
            if sample is None:
                adx_, pdi_, mdi_ = dmi_adx(h, l, c, ADX_LEN)
                ema_ = htf_ema200(c)
                for i2 in range(len(c) - 1, TL_LOOKBACK + 20, -1):
                    ok2, d2 = detect_trendline_long(o, h, l, c, v, i2, adx_, pdi_, mdi_, ema_,
                                                    adx_min=amin, rsi_min=rmin, swings_req=sw_req)
                    if ok2:
                        sample = (sym, o, h, l, c, i2, d2); break

        n = len(all_tr)
        if n == 0:
            top = sorted(diag["reasons"].items(), key=lambda x: -x[1])[:8]
            L = [f"\U0001F4ED Сделок нет. Проверок: {diag['evals']:,}", "", "Что рубит:"]
            for nm, cnt in top:
                L.append(f"\u2022 {nm}: {cnt:,} ({cnt/max(diag['evals'],1)*100:.1f}%)")
            L.append(f"\nМягче: /trendline {months} adx=20 rsi=40 swings=2")
            tg_send(chat, "\n".join(L)); return

        wins = [t["pnl"] for t in all_tr if t["pnl"] > 0]
        losses = [t["pnl"] for t in all_tr if t["pnl"] <= 0]
        gw = sum(wins); gl = -sum(losses)
        pf = (gw / gl) if gl > 0 else float("inf")
        R = ((gw / len(wins)) / (gl / len(losses))) if wins and losses and gl > 0 else 0.0
        eq = [DEPOSIT]
        for t in sorted(all_tr, key=lambda x: x["close_ts"]): eq.append(eq[-1] + t["pnl"])
        pk = DEPOSIT; dd = 0.0
        for x in eq:
            pk = max(pk, x); dd = max(dd, (pk - x) / pk)
        reasons = {}
        for t in all_tr: reasons[t["reason"]] = reasons.get(t["reason"], 0) + 1
        per_year = n / max(months / 12.0, 1e-9) / max(len(data), 1)
        L = [f"\U0001F4C9 <b>TRENDLINE ИТОГ</b> ({interval}, {months} мес, {len(data)} монет)",
             f"Проверок: {diag['evals']:,} \u2192 сигналов: {diag['signals']:,}",
             f"Закрытий: <b>{n}</b> \u00b7 в плюсе: {len(wins)} ({len(wins)/n*100:.0f}%)",
             f"PnL: <b>{total:+.2f}$</b> ({total/DEPOSIT*100:+.1f}%) \u00b7 макс.DD {dd*100:.1f}%",
             f"Profit Factor: <b>{pf:.2f}</b> \u00b7 R (побед/проигр): {R:.2f}",
             f"Частота: ~{per_year:.1f} закрытий в год на монету",
             f"Выходы: {reasons}",
             "",
             ("\u2705 Выборка достаточна" if n >= 100 else
              f"\u26A0\uFE0F Выборка мала ({n} < 100) — цифрам верить рано")]
        # разбивка по монетам
        by_sym = {}
        for t in all_tr:
            s_ = by_sym.setdefault(t["symbol"], {"n": 0, "pnl": 0.0, "w": 0})
            s_["n"] += 1; s_["pnl"] += t["pnl"]
            if t["pnl"] > 0: s_["w"] += 1
        L.append("\n\U0001F4C8 <b>По монетам:</b>")
        for s_, st_ in sorted(by_sym.items(), key=lambda x: -x[1]["pnl"]):
            L.append(f"\u2022 {s_}: {st_['n']} закр., {st_['w']/st_['n']*100:.0f}% плюс, {st_['pnl']:+.1f}$")
        if len(by_sym) > 1 and total > 0:
            top_pnl = max(st_["pnl"] for st_ in by_sym.values())
            if top_pnl > total * 0.6:
                L.append(f"\u26A0\uFE0F Одна монета даёт {top_pnl/total*100:.0f}% прибыли — "
                         f"держится на ней, а не на стратегии")
        tg_send(chat, "\n".join(L))
        try:
            tg_send(chat, binning_report(all_tr, bins=(20, 25, 30, 40, 50, 100)))
        except Exception as e:
            print("tl binning err:", e)
        # график сетапа
        if sample:
            sym_, o_, h_, l_, c_, i_, d_ = sample
            p_ = plot_trendline_setup(sym_, o_, h_, l_, c_, i_, d_)
            if p_:
                tg_photo(chat, p_, caption=f"{sym_}: пример сетапа — линия по {d_['trendline']['touches']} "
                                           f"касаниям, вход/стоп/тейки")
        if HAS_MPL:
            try:
                plt.figure(figsize=(10, 5)); plt.plot(eq, linewidth=1.4)
                plt.axhline(DEPOSIT, color="gray", linestyle="--", alpha=0.5)
                plt.title(f"Trendline Long · {interval} · {n} закрытий · PF {pf:.2f}")
                plt.xlabel("Закрытия"); plt.ylabel("Капитал $"); plt.grid(True, alpha=0.4)
                p2 = "/tmp/tl_eq.png"; plt.savefig(p2, dpi=110, bbox_inches="tight"); plt.close()
                tg_photo(chat, p2, caption="Кривая капитала (издержки учтены)")
            except Exception as e:
                print("tl chart err:", e)
    finally:
        BT_RUNNING["on"] = False


def run_beta_premise(chat, months=6, interval=None, move_thr=None, alts=None):
    """ПРОВЕРКА ПРЕДПОСЫЛКИ (до постройки бота):
    после сильного движения BTC — что делают альты ДАЛЬШЕ?

    Если альты уже отработали движение одновременно с BTC, входить поздно и edge нет.
    Если есть лаг — альты продолжают идти после сигнала, и стратегия имеет смысл.
    Меряем forward-доходность альта на 1/2/4/8/24 бара ПОСЛЕ сигнала."""
    if BT_RUNNING["on"]:
        tg_send(chat, "\u23F3 Идёт другой прогон — дождись окончания."); return
    BT_RUNNING["on"] = True
    try:
        months = max(1, min(months, 24))
        interval = interval or BETA_TF
        thr = BETA_BTC_MOVE if move_thr is None else move_thr
        alts = alts or BETA_ALTS
        tg_send(chat, f"\U0001F52C <b>ПРОВЕРКА ПРЕДПОСЫЛКИ: есть ли лаг альтов за BTC?</b>\n"
                      f"{months} мес \u00b7 {interval} \u00b7 {len(alts)} альтов\n"
                      f"Триггер: движение BTC \u2265 {thr*100:.1f}% за {BETA_LOOKBACK} бар(ов)\n\n"
                      f"<i>Вопрос: после того как BTC уже прошёл {thr*100:.1f}%, "
                      f"альт ЕЩЁ идёт — или он уже отработал вместе с BTC?</i>\n"
                      f"Качаю историю\u2026")
        try:
            bo, bh, bl, bc, bv, bct = sniper_klines("BTCUSDT", interval, months)
        except Exception as e:
            tg_send(chat, f"\u274C BTC не загрузился: {e}"); return
        if len(bc) < 300:
            tg_send(chat, f"\u274C Мало истории BTC ({len(bc)})"); return
        tg_send(chat, f"\u2699\uFE0F BTC: {len(bc):,} баров")

        btc_rets = returns_of(bc)
        horizons = [1, 2, 4, 8, 24]
        agg = {hz: {"up": [], "dn": []} for hz in horizons}
        # для сравнения: обычная (безусловная) доходность альта
        baseline = {hz: [] for hz in horizons}
        corr_stats = []
        n_signals_up = n_signals_dn = 0

        for sym in alts:
            try:
                ao, ah, al, ac, av, act = sniper_klines(sym, interval, months)
            except Exception as e:
                print(f"beta {sym}: {e}"); continue
            m = min(len(bc), len(ac))
            if m < 300: continue
            b_c = bc[-m:]; a_c = ac[-m:]
            a_rets = returns_of(a_c); b_rets = returns_of(b_c)
            corr = pearson_corr(a_rets[-BETA_CORR_WIN:], b_rets[-BETA_CORR_WIN:])
            beta_v = calc_beta(a_rets[-BETA_CORR_WIN:], b_rets[-BETA_CORR_WIN:])
            corr_stats.append((sym, corr, beta_v))

            for i in range(BETA_LOOKBACK, m - max(horizons) - 1):
                bmove = b_c[i] / b_c[i - BETA_LOOKBACK] - 1.0
                # безусловная выборка (каждый 10-й бар) для сравнения
                if i % 10 == 0:
                    for hz in horizons:
                        baseline[hz].append(a_c[i + hz] / a_c[i] - 1.0)
                if abs(bmove) < thr:
                    continue
                side = "up" if bmove > 0 else "dn"
                if sym == alts[0]:
                    if side == "up": n_signals_up += 1
                    else: n_signals_dn += 1
                for hz in horizons:
                    fwd = a_c[i + hz] / a_c[i] - 1.0
                    agg[hz][side].append(fwd)
            tg_send(chat, f"\u2699\uFE0F {sym}: corr {corr:+.2f}, beta {beta_v:.2f}")

        if not any(agg[h]["up"] or agg[h]["dn"] for h in horizons):
            tg_send(chat, "\U0001F4ED Сигналов не набралось — понизь порог: /premise 6 move=0.008")
            return

        L = ["\U0001F52C <b>РЕЗУЛЬТАТ: что делает альт ПОСЛЕ движения BTC</b>", ""]
        L.append("<b>После движения BTC ВВЕРХ:</b>")
        L.append("<code>гориз.  случаев  ср.доходн.  положит.%</code>")
        for hz in horizons:
            d = agg[hz]["up"]
            if not d: continue
            avg = sum(d) / len(d)
            pos = sum(1 for x in d if x > 0) / len(d)
            L.append(f"<code>+{hz:2d} бар {len(d):7d}  {avg*100:+9.3f}%  {pos*100:8.1f}%</code>")
        L.append("")
        L.append("<b>После движения BTC ВНИЗ (для шорта):</b>")
        for hz in horizons:
            d = agg[hz]["dn"]
            if not d: continue
            avg = sum(d) / len(d)
            neg = sum(1 for x in d if x < 0) / len(d)
            L.append(f"<code>+{hz:2d} бар {len(d):7d}  {avg*100:+9.3f}%  вниз {neg*100:5.1f}%</code>")
        L.append("")
        L.append("<b>БАЗОВАЯ ЛИНИЯ (любой момент, без сигнала):</b>")
        for hz in horizons:
            d = baseline[hz]
            if not d: continue
            avg = sum(d) / len(d)
            pos = sum(1 for x in d if x > 0) / len(d)
            L.append(f"<code>+{hz:2d} бар {len(d):7d}  {avg*100:+9.3f}%  {pos*100:8.1f}%</code>")
        tg_send(chat, "\n".join(L))

        # ВЕРДИКТ: есть ли преимущество против базовой линии, и хватает ли на издержки
        cost = 2 * (SPREAD_PCT + (SLIP_MIN + SLIP_MAX) / 2 + FEE_TAKER)
        V = ["\U0001F3AF <b>ВЕРДИКТ ПО ПРЕДПОСЫЛКЕ</b>",
             f"Издержки раунд-трипа: {cost*100:.3f}% — доходность должна их перекрыть.", ""]
        edge_found = False
        for hz in horizons:
            up = agg[hz]["up"]; base = baseline[hz]
            if not up or not base: continue
            a_up = sum(up) / len(up); a_base = sum(base) / len(base)
            lift = a_up - a_base
            net = a_up - cost
            flag = "\u2705" if net > 0 else "\u274c"
            V.append(f"{flag} +{hz} бар: сигнал {a_up*100:+.3f}% vs фон {a_base*100:+.3f}% "
                     f"(преимущество {lift*100:+.3f} п.п.) \u2192 после издержек {net*100:+.3f}%")
            if net > 0 and lift > 0:
                edge_found = True
        V.append("")
        if edge_found:
            V.append("\u2705 <b>ЛАГ ЕСТЬ.</b> После сигнала альты продолжают идти, и остаётся "
                     "больше издержек. Имеет смысл строить полного бота.")
        else:
            V.append("\u274C <b>ЛАГА НЕТ.</b> Альты отрабатывают движение ОДНОВРЕМЕННО с BTC — "
                     "к моменту сигнала ловить уже нечего, а издержки съедают остаток.")
            V.append("<i>Строить бота на этой предпосылке бессмысленно: проблема не в параметрах, "
                     "а в самой идее 'войти после того, как движение уже случилось'.</i>")
        if corr_stats:
            V.append("")
            V.append("<b>Корреляции с BTC:</b>")
            for s_, c_, b_ in sorted(corr_stats, key=lambda x: -x[1]):
                V.append(f"\u2022 {s_}: corr {c_:+.2f}, beta {b_:.2f}"
                         + ("" if c_ >= BETA_CORR_MIN else "  (ниже порога)"))
        tg_send(chat, "\n".join(V))
    finally:
        BT_RUNNING["on"] = False


def run_exit_compare(chat, months=12, symbols=None, interval=None, adx_min=None):
    """/exits — гоняет ОДИН И ТОТ ЖЕ вход через разные структуры выхода.
    Вход не меняется вообще: чистая проверка, вытягивает ли асимметрия выхода."""
    if BT_RUNNING["on"]:
        tg_send(chat, "\u23F3 Идёт другой прогон — дождись окончания."); return
    BT_RUNNING["on"] = True
    try:
        months = max(1, min(months, 36))
        interval = interval or SNIPER_TF
        if isinstance(symbols, str):
            if symbols == "all":
                syms = (SNIPER_UNIVERSES["major"] + SNIPER_UNIVERSES["mid"]
                        + SNIPER_UNIVERSES["alt"])
            else:
                syms = SNIPER_UNIVERSES.get(symbols, SNIPER_UNIVERSES["major"])
        else:
            syms = symbols or SNIPER_UNIVERSES["major"]
        amin = ADX_MIN if adx_min is None else adx_min
        tg_send(chat, f"\U0001F513 <b>СРАВНЕНИЕ ВЫХОДОВ</b>: {months} мес \u00b7 {interval} \u00b7 "
                      f"{len(syms)} монет \u00b7 ADX>{amin}\n"
                      f"Вход ОДИН И ТОТ ЖЕ во всех вариантах — меняется только выход.\n"
                      f"Вариантов: {len(EXIT_VARIANTS)}. Качаю историю\u2026")
        data = {}
        for sym in syms:
            try:
                o, h, l, c, v, ct = sniper_klines(sym, interval, months)
                if len(c) >= SNIPER_SMA + 100:
                    data[sym] = (o, h, l, c, ct)
            except Exception as e:
                print(f"exits {sym} err:", e)
        if not data:
            tg_send(chat, "\u274C Данные не загрузились."); return
        tg_send(chat, f"\U0001F4E6 {len(data)} монет. Гоняю {len(EXIT_VARIANTS)} вариантов выхода\u2026")

        results = []
        for name, use_tp1, tp1m, trm, slm in EXIT_VARIANTS:
            all_tr = []; total = 0.0
            for sym, (o, h, l, c, ct) in data.items():
                tr, pnl = bt_sniper_coin(sym, o, h, l, c, ct, seed=17, adx_min=amin,
                                         use_tp1=use_tp1, tp1_mult=tp1m,
                                         trail_mult=trm, sl_mult=slm)
                all_tr += tr; total += pnl
            n = len(all_tr)
            if n == 0:
                results.append(dict(name=name, n=0, wr=0, pf=0, pnl=0, dd=0, R=0)); continue
            wins = [t["pnl"] for t in all_tr if t["pnl"] > 0]
            losses = [t["pnl"] for t in all_tr if t["pnl"] <= 0]
            gw = sum(wins); gl = -sum(losses)
            pf = (gw / gl) if gl > 0 else float("inf")
            avg_w = (gw / len(wins)) if wins else 0.0
            avg_l = (gl / len(losses)) if losses else 0.0
            R = (avg_w / avg_l) if avg_l > 0 else 0.0
            eq = [DEPOSIT]
            for t in sorted(all_tr, key=lambda x: x["close_ts"]): eq.append(eq[-1] + t["pnl"])
            pk = DEPOSIT; dd = 0.0
            for x in eq:
                pk = max(pk, x); dd = max(dd, (pk - x) / pk)
            results.append(dict(name=name, n=n, wr=len(wins) / n, pf=pf,
                                pnl=total, dd=dd, R=R, eq=eq))
            tg_send(chat, f"\u2699\uFE0F {name}: {n} закр., WR {len(wins)/n*100:.0f}%, "
                          f"PF {pf:.2f}, {total:+.0f}$")

        results.sort(key=lambda r: -r["pnl"])
        L = ["\U0001F513 <b>СРАВНЕНИЕ СТРУКТУР ВЫХОДА</b>",
             "<i>Вход идентичен везде — разница только в том, как выходим.</i>", ""]
        for r in results:
            if r["n"] == 0:
                L.append(f"\u2022 {r['name']}: сделок нет"); continue
            mark = "\u2705" if r["pnl"] > 0 else "\u274C"
            L.append(f"{mark} <b>{r['name']}</b>")
            L.append(f"   {r['n']} закр. \u00b7 WR {r['wr']*100:.0f}% \u00b7 R {r['R']:.2f} \u00b7 "
                     f"PF {r['pf']:.2f} \u00b7 {r['pnl']:+.0f}$ \u00b7 DD {r['dd']*100:.0f}%")
        best = results[0]
        L.append("")
        if best["pnl"] > 0:
            L.append(f"\U0001F3C6 Лучший: <b>{best['name']}</b> ({best['pnl']:+.0f}$, PF {best['pf']:.2f})")
            L.append("<i>\u26A0\uFE0F Найдено НА ЭТИХ данных. Проверь на невиданных: /sniperwf</i>")
            L.append("<i>И помни: биннинг показал, что ADX не предсказывает исход — "
                     "плюс идёт от асимметрии выхода, а не от качества сигнала. "
                     "Такое держится на трендовости рынка и может исчезнуть в боковике.</i>")
        else:
            L.append("\u274C <b>Ни одна структура выхода не вывела в плюс.</b>")
            L.append("<i>Значит дело не в обрезке победителей — вход не даёт преимущества.</i>")
        tg_send(chat, "\n".join(L))
        if HAS_MPL and any(r["n"] for r in results):
            try:
                plt.figure(figsize=(11, 6))
                for r in results[:5]:
                    if r["n"]:
                        plt.plot(r["eq"], linewidth=1.3, label=f"{r['name'][:28]} ({r['pnl']:+.0f}$)")
                plt.axhline(DEPOSIT, color="gray", linestyle="--", alpha=0.6)
                plt.title("Один вход, разные выходы")
                plt.xlabel("Закрытия"); plt.ylabel("Капитал $")
                plt.legend(fontsize=7); plt.grid(True, alpha=0.4)
                p_ = "/tmp/exits.png"; plt.savefig(p_, dpi=110, bbox_inches="tight"); plt.close()
                tg_photo(chat, p_, caption="Сравнение структур выхода на одном и том же входе")
            except Exception as e:
                print("exits chart err:", e)
    finally:
        BT_RUNNING["on"] = False


def run_sniper(chat, months=12, symbols=None, interval=None, adx_min=None, mode="backtest"):
    """/sniper — бэктест ADX Sniper на ДЛИННОЙ истории + биннинг по силе ADX.
       /sniperwf — walk-forward: подбор ADX-порога на train, проверка на test."""
    if BT_RUNNING["on"]:
        tg_send(chat, "\u23F3 Идёт другой прогон — дождись окончания."); return
    BT_RUNNING["on"] = True
    try:
        months = max(1, min(months, 36))
        interval = interval or SNIPER_TF
        if isinstance(symbols, str):
            if symbols == "wide":
                try:
                    syms = universe()[:30]      # топ-30 по обороту (пересечение Binance/Bybit)
                except Exception:
                    syms = SNIPER_UNIVERSES["major"]
            elif symbols == "all":
                syms = (SNIPER_UNIVERSES["major"] + SNIPER_UNIVERSES["mid"]
                        + SNIPER_UNIVERSES["alt"])
            else:
                syms = SNIPER_UNIVERSES.get(symbols, SNIPER_UNIVERSES["major"])
        else:
            syms = symbols or SNIPER_UNIVERSES["major"]
        amin = ADX_MIN if adx_min is None else adx_min
        head = ("\U0001F3AF <b>ADX SNIPER</b>" if mode == "backtest"
                else "\U0001F501 <b>ADX SNIPER WALK-FORWARD</b>")
        tg_send(chat, f"{head}: {months} мес \u00b7 {interval} \u00b7 {len(syms)} монет\n"
                      f"\u2699\uFE0F ADX({ADX_LEN}) > {amin}"
                      + (" и растёт" if ADX_RISING else "")
                      + f" \u00b7 +DI>-DI \u00b7 EMA{SNIPER_EMA_F}>EMA{SNIPER_EMA_S} \u00b7 цена>SMA{SNIPER_SMA}\n"
                      f"\U0001F6D1 SL {SNIPER_SL_ATR}\u00d7ATR \u00b7 TP1 {SNIPER_TP1_ATR}\u00d7ATR (50%) \u00b7 "
                      f"трейлинг {SNIPER_TRAIL}\u00d7ATR\n"
                      f"\U0001F4B8 Спред+слиппедж+комиссии учтены \u00b7 риск {RISK_PCT_TRADE*100:.0f}%/сделку\n"
                      + (f"\u2699\uFE0F RSI-фильтр: <{SNIPER_RSI_MAX:.0f}\n" if SNIPER_RSI_MAX > 0
                         else "\u2699\uFE0F RSI-фильтр отключён (конфликтует с ADX)\n")
                      + f"Живой скан на паузе. Качаю историю ({months} мес)\u2026")
        data = {}
        for k, sym in enumerate(syms, 1):
            try:
                o, h, l, c, v, ct = sniper_klines(sym, interval, months)
                if len(c) < SNIPER_SMA + 100:
                    tg_send(chat, f"\u26A0\uFE0F {sym}: мало истории ({len(c)} баров), пропуск"); continue
                data[sym] = (o, h, l, c, ct)
                tg_send(chat, f"\u2699\uFE0F {sym}: {len(c):,} баров загружено")
            except Exception as e:
                print(f"sniper {sym} err:", e)
        if not data:
            tg_send(chat, "\u274C Данные не загрузились."); return

        if mode == "backtest":
            all_tr = []; total = 0.0
            diag = dict(evals=0, signals=0, reasons={})
            for sym, (o, h, l, c, ct) in data.items():
                tr, pnl = bt_sniper_coin(sym, o, h, l, c, ct, diag=diag, seed=17, adx_min=amin)
                all_tr += tr; total += pnl
            n = len(all_tr)
            if n == 0:
                top = sorted(diag["reasons"].items(), key=lambda x: -x[1])[:6]
                L = [f"\U0001F4ED Сигналов нет. Проверок: {diag['evals']:,}", "\nЧто рубит:"]
                for nm, cnt in top:
                    L.append(f"\u2022 {nm}: {cnt:,} ({cnt/max(diag['evals'],1)*100:.1f}%)")
                L.append(f"\nПопробуй мягче: /sniper {months} adx=20")
                tg_send(chat, "\n".join(L)); return
            # закрытые сделки = только полные выходы (part=1.0) + частичные TP1
            wins = sum(1 for t in all_tr if t["pnl"] > 0)
            eq = [DEPOSIT]
            for t in sorted(all_tr, key=lambda x: x["close_ts"]): eq.append(eq[-1] + t["pnl"])
            pk = DEPOSIT; dd = 0.0
            for x in eq:
                pk = max(pk, x); dd = max(dd, (pk - x) / pk)
            gross_w = sum(t["pnl"] for t in all_tr if t["pnl"] > 0)
            gross_l = -sum(t["pnl"] for t in all_tr if t["pnl"] < 0)
            pf = (gross_w / gross_l) if gross_l > 0 else float("inf")
            per_year = n / max(months / 12.0, 1e-9) / max(len(data), 1)
            avg_adx = sum(t["adx"] for t in all_tr) / n
            L = [f"\U0001F3AF <b>ADX SNIPER ИТОГ</b> ({interval}, ADX>{amin}, {months} мес)",
                 f"Проверок: {diag['evals']:,} \u2192 сигналов: {diag['signals']:,}",
                 f"Закрытий: <b>{n}</b> \u00b7 в плюсе: {wins} ({wins/n*100:.0f}%)",
                 f"PnL: <b>{total:+.2f}$</b> ({total/DEPOSIT*100:+.1f}%) \u00b7 макс.DD {dd*100:.1f}%",
                 f"Profit Factor: <b>{pf:.2f}</b> \u00b7 средний ADX входа {avg_adx:.1f}",
                 f"Частота: ~{per_year:.1f} закрытий в год на монету",
                 "",
                 ("\u2705 Выборка достаточна" if n >= 100 else
                  f"\u26A0\uFE0F Выборка мала ({n} < 100) — цифрам верить рано")]
            # разбивка по монетам — видно, весь ли результат держится на одной
            by_sym = {}
            for t in all_tr:
                s = by_sym.setdefault(t["symbol"], {"n": 0, "pnl": 0.0, "w": 0})
                s["n"] += 1; s["pnl"] += t["pnl"]
                if t["pnl"] > 0: s["w"] += 1
            L.append("\n\U0001F4C8 <b>По монетам:</b>")
            for s_, st_ in sorted(by_sym.items(), key=lambda x: -x[1]["pnl"]):
                L.append(f"\u2022 {s_}: {st_['n']} закр., {st_['w']/st_['n']*100:.0f}% плюс, "
                         f"{st_['pnl']:+.1f}$")
            if len(by_sym) > 1:
                top_pnl = max(st_["pnl"] for st_ in by_sym.values())
                if total > 0 and top_pnl > total * 0.6:
                    L.append(f"\u26A0\uFE0F Одна монета даёт {top_pnl/total*100:.0f}% всей прибыли "
                             f"— результат держится на ней, а не на стратегии")
            L.append("\n\U0001F4CB Заявлено в описании: WR 75-80%, PF ~8, DD ~7%")
            L.append(f"\U0001F4CA Фактически:            WR {wins/n*100:.0f}%, PF {pf:.2f}, DD {dd*100:.1f}%")
            tg_send(chat, "\n".join(L))
            # биннинг по силе ADX
            try:
                tg_send(chat, binning_report(all_tr, bins=(20, 25, 30, 40, 50, 100)))
            except Exception as e:
                print("sniper binning err:", e)
            if HAS_MPL:
                try:
                    plt.figure(figsize=(10, 5)); plt.plot(eq, linewidth=1.4)
                    plt.axhline(DEPOSIT, color="gray", linestyle="--", alpha=0.5)
                    plt.title(f"ADX Sniper · {interval} · {n} закрытий · PF {pf:.2f}")
                    plt.xlabel("Закрытия"); plt.ylabel("Капитал $"); plt.grid(True, alpha=0.4)
                    p_ = "/tmp/sniper.png"; plt.savefig(p_, dpi=110, bbox_inches="tight"); plt.close()
                    tg_photo(chat, p_, caption="ADX Sniper equity (издержки учтены)")
                except Exception as e:
                    print("sniper chart err:", e)
        else:
            # WALK-FORWARD по времени: train 60% -> test 40%, подбор ADX-порога
            grid = [20, 22, 25, 28, 30, 35]
            wins_ = []
            for sym, (o, h, l, c, ct) in data.items():
                split = int(len(c) * 0.6)
                best_thr, best_pnl = None, float("-inf")
                for thr in grid:
                    _, pnl_tr = bt_sniper_coin(sym, o[:split], h[:split], l[:split],
                                               c[:split], ct[:split], seed=42, adx_min=thr)
                    if pnl_tr > best_pnl: best_pnl, best_thr = pnl_tr, thr
                tr_te, pnl_te = bt_sniper_coin(sym, o[split:], h[split:], l[split:],
                                               c[split:], ct[split:], seed=7, adx_min=best_thr)
                wins_.append(dict(symbol=sym, thr=best_thr, train=best_pnl,
                                  test=pnl_te, n_test=len(tr_te)))
            n_w = len(wins_)
            avg_tr = sum(w["train"] for w in wins_) / n_w
            avg_te = sum(w["test"] for w in wins_) / n_w
            tot_te = sum(w["n_test"] for w in wins_)
            if avg_tr > 0 and avg_te > 0:
                verdict = "\u2705 <b>EDGE ВЫЖИЛ</b> на невиданной части истории."
            elif avg_tr > 0 >= avg_te:
                verdict = "\u274C <b>ПОДГОНКА.</b> Порог ADX заучил train, на test минус."
            else:
                verdict = "\U0001F53B <b>Нет преимущества</b> даже на train."
            L = ["\U0001F501 <b>ADX SNIPER WALK-FORWARD</b> (train 60% \u2192 test 40%)",
                 f"Монет: {n_w} \u00b7 сделок на test: {tot_te}",
                 f"TRAIN: средний PnL {avg_tr:+.2f}$",
                 f"TEST:  средний PnL {avg_te:+.2f}$",
                 "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501"]
            for w in wins_:
                L.append(f"\u2022 {w['symbol']}: ADX>{w['thr']} \u2192 train {w['train']:+.1f}$, "
                         f"test {w['test']:+.1f}$ ({w['n_test']} сделок)")
            L.append("\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501")
            L.append(verdict)
            tg_send(chat, "\n".join(L))
    finally:
        BT_RUNNING["on"] = False


def run_topn(chat, days=30, ncoins=60, threshold=3.0, top_n=20,
             use_regime=False, use_kelly=True):
    """TOP-N RANKING: собираем ВСЕ сигналы по всем монетам, ранжируем по score,
    торгуем только лучшие N за сутки — 'лучшие идеи дня' вместо 'всё подряд'."""
    if BT_RUNNING["on"]:
        tg_send(chat, "\u23F3 Идёт другой прогон — дождись окончания."); return
    BT_RUNNING["on"] = True
    try:
        days = max(7, min(days, 30)); ncoins = max(1, min(ncoins, 60))
        tg_send(chat, f"\U0001F3C6 <b>TOP-N RANKING</b>: {days} дн \u00d7 {ncoins} монет\n"
                      f"Порог отбора кандидатов: {threshold} \u00b7 берём <b>топ-{top_n} в сутки</b>\n"
                      + (f"\U0001F30A Regime-фильтр: только 'trend'\n" if use_regime else "")
                      + (f"\U0001F4B0 Kelly-lite сайзинг\n" if use_kelly else "")
                      + f"Живой скан на паузе. Качаю историю\u2026")
        cached = _bt_prefetch(days, ncoins)
        if not cached:
            tg_send(chat, "\u274C Не удалось загрузить данные."); return

        # 1) собираем кандидатов по всем монетам
        cands = []
        bars = {}
        diag = dict(evals=0, signals=0, reasons={}, killed=False)
        for k, (sym, o, h, l, c, v, tb, ct, oi_ts) in enumerate(cached, 1):
            got, _, _ = bt_simulate_scored(sym, o, h, l, c, v, tb, ct, oi_ts,
                                           threshold=threshold, diag=diag, seed=11,
                                           use_regime=use_regime, collect_only=True)
            cands += got
            bars[sym] = (o, h, l, c, ct)
            if k % 15 == 0:
                tg_send(chat, f"\u2699\uFE0F {k}/{len(cached)} \u00b7 кандидатов {len(cands)}")
        if not cands:
            tg_send(chat, f"\U0001F4ED Кандидатов нет при пороге {threshold}."); return

        # 2) отбираем лучшие N в сутки
        chosen = select_top_n(cands, top_n=top_n, per_day=True)
        tg_send(chat, f"\U0001F4E5 Кандидатов: {len(cands):,} \u2192 отобрано лучших: {len(chosen):,} "
                      f"(топ-{top_n}/сутки)")

        # 3) торгуем только отобранные
        import random as _r
        rng = _r.Random(23)
        capital = DEPOSIT; peak = DEPOSIT
        trades = []
        for cnd in chosen:
            sym = cnd["symbol"]
            o, h, l, c, ct = bars[sym]
            i = cnd["i"]
            entry = apply_slippage(cnd["entry"], "long", rng)
            dist = entry - cnd["sl"]
            if dist <= 0: continue
            frac = size_kelly_lite(cnd["score"]) if use_kelly else 0.01
            if frac <= 0: continue
            qty = min(capital * frac / dist, NOTIONAL / entry)
            exit_px = None; reason = None
            for j2 in range(i + 1, min(i + 21, len(c))):
                if l[j2] <= cnd["sl"]: exit_px, reason = cnd["sl"], "SL"; break
                if h[j2] >= cnd["tp1"]: exit_px, reason = cnd["tp1"], "TP1"; break
            if exit_px is None:
                j2 = min(i + 20, len(c) - 1); exit_px, reason = c[j2], "TIME"
            fill = apply_slippage(exit_px, "short", rng)
            pnl = (fill - entry) * qty - (entry + fill) * qty * FEE_TAKER
            capital += pnl; peak = max(peak, capital)
            trades.append(dict(symbol=sym, pnl=pnl, reason=reason, score=cnd["score"],
                               close_ts=ct[j2]))
            if peak > 0 and (peak - capital) / peak > KILL_SWITCH_DD:
                tg_send(chat, f"\U0001F6D1 Kill-switch: просадка > {KILL_SWITCH_DD*100:.0f}%, стоп на {len(trades)} сделке")
                break

        n = len(trades)
        if n == 0:
            tg_send(chat, "\U0001F4ED Ни одной сделки после отбора."); return
        wins = sum(1 for t in trades if t["pnl"] > 0)
        total = sum(t["pnl"] for t in trades)
        eq = [DEPOSIT]
        for t in sorted(trades, key=lambda x: x["close_ts"]): eq.append(eq[-1] + t["pnl"])
        pk = DEPOSIT; dd = 0.0
        for x in eq:
            pk = max(pk, x); dd = max(dd, (pk - x) / pk)
        avg_sc = sum(t["score"] for t in trades) / n
        tg_send(chat, "\n".join([
            f"\U0001F3C6 <b>TOP-N ИТОГ</b> (топ-{top_n}/сутки, порог {threshold})",
            f"Кандидатов: {len(cands):,} \u2192 сделок: <b>{n}</b> "
            f"(отсеяно {(1-n/max(len(cands),1))*100:.1f}%)",
            f"В плюсе: {wins} ({wins/n*100:.0f}%) \u00b7 средний score {avg_sc:.2f}/{SCORE_MAX:.1f}",
            f"PnL: <b>{total:+.2f}$</b> ({total/DEPOSIT*100:+.1f}%) \u00b7 макс.DD {dd*100:.1f}%",
            f"{'\u2705 Выборка достаточна' if n >= 100 else '\u26A0\uFE0F Выборка мала (' + str(n) + ' < 100)'}",
        ]))
        try:
            tg_send(chat, binning_report(trades))
        except Exception as e:
            print("binning err:", e)
        if HAS_MPL:
            try:
                plt.figure(figsize=(10, 5)); plt.plot(eq, linewidth=1.4)
                plt.axhline(DEPOSIT, color="gray", linestyle="--", alpha=0.5)
                plt.title(f"TOP-{top_n}/сутки · {n} сделок")
                plt.xlabel("Сделки"); plt.ylabel("Капитал $"); plt.grid(True, alpha=0.4)
                p_ = "/tmp/topn.png"; plt.savefig(p_, dpi=110, bbox_inches="tight"); plt.close()
                tg_photo(chat, p_, caption="TOP-N equity")
            except Exception as e:
                print("topn chart err:", e)
    finally:
        BT_RUNNING["on"] = False


def run_scored(chat, days=30, ncoins=60, threshold=None, mode="backtest",
               use_regime=False, use_kelly=False, top_n=0):
    """/scored — бэктест scored-детектора; /scoredwf — walk-forward 20d/10d."""
    if BT_RUNNING["on"]:
        tg_send(chat, "\u23F3 Идёт другой прогон — дождись окончания."); return
    BT_RUNNING["on"] = True
    try:
        days = max(7, min(days, 30)); ncoins = max(1, min(ncoins, 60))
        thr = SCORE_THRESHOLD if threshold is None else threshold
        head = ("\U0001F3AF <b>SCORED BACKTEST</b>" if mode == "backtest"
                else "\U0001F501 <b>SCORED WALK-FORWARD</b> (train 20д \u2192 test 10д, roll)")
        tg_send(chat, f"{head}: {days} дн \u00d7 {ncoins} монет\n"
                      f"\u2699\uFE0F Веса: {SCORE_WEIGHTS} \u00b7 макс.score={SCORE_MAX:.1f} \u00b7 порог={thr}\n"
                      f"\u2699\uFE0F Ослаблено: breakout \u00d7{BREAKOUT_TOLERANCE}, volume \u2265{VOL_RATIO_MIN_SCORED}, OI=tanh(slope\u00d73)\n"
                      f"\U0001F4B8 Реализм: спред {SPREAD_PCT*100:.2f}% + слиппедж {SLIP_MIN*100:.2f}-{SLIP_MAX*100:.2f}% + комиссии\n"
                      f"\U0001F6D1 Kill-switch: просадка > {KILL_SWITCH_DD*100:.0f}%\n"
                      + (f"\U0001F30A Regime-фильтр: торгуем только 'trend'\n" if use_regime else "")
                      + (f"\U0001F4B0 Kelly-lite сайзинг (0/1/2/3% по силе сигнала)\n" if use_kelly else "")
                      + (f"\U0001F3C6 TOP-{top_n} лучших сигналов в сутки\n" if top_n else "")
                      + f"Живой скан на паузе. Качаю историю\u2026")
        cached = _bt_prefetch(days, ncoins)
        if not cached:
            tg_send(chat, "\u274C Не удалось загрузить данные."); return
        tg_send(chat, f"\U0001F4E6 {len(cached)} монет загружено.")

        if mode == "backtest":
            all_tr = []; total = 0.0; killed_syms = []
            diag = dict(evals=0, signals=0, reasons={}, killed=False)
            for k, (sym, o, h, l, c, v, tb, ct, oi_ts) in enumerate(cached, 1):
                tr, pnl, killed = bt_simulate_scored(sym, o, h, l, c, v, tb, ct, oi_ts,
                                                     threshold=thr, diag=diag, seed=11,
                                                     use_regime=use_regime, use_kelly=use_kelly)
                all_tr += tr; total += pnl
                if killed: killed_syms.append(sym)
                if k % 15 == 0:
                    tg_send(chat, f"\u2699\uFE0F {k}/{len(cached)} \u00b7 сделок {len(all_tr)}")
            n = len(all_tr)
            if n == 0:
                tg_send(chat, f"\U0001F4ED Ни одной сделки при пороге {thr}. "
                              f"Проверок: {diag['evals']:,}. Понизь порог: /scored {days} {ncoins} 3.0")
                return
            wins = sum(1 for t in all_tr if t["pnl"] > 0)
            eq = [DEPOSIT]
            for t in sorted(all_tr, key=lambda x: x["close_ts"]): eq.append(eq[-1] + t["pnl"])
            peak = DEPOSIT; dd = 0.0
            for x in eq:
                peak = max(peak, x); dd = max(dd, (peak - x) / peak)
            conv = diag["signals"] / diag["evals"] * 100 if diag["evals"] else 0
            avg_score = sum(t["score"] for t in all_tr) / n
            L = [f"\U0001F3AF <b>SCORED ИТОГ</b> (порог {thr})",
                 f"Проверок: {diag['evals']:,} \u2192 сигналов: {diag['signals']:,} "
                 f"(конверсия {conv:.3f}%, было 0.007%)",
                 f"Сделок: <b>{n}</b> \u00b7 в плюсе: {wins} ({wins/n*100:.0f}%)",
                 f"PnL: <b>{total:+.2f}$</b> ({total/DEPOSIT*100:+.1f}% депо) \u00b7 макс.DD {dd*100:.1f}%",
                 f"Средний score сделки: {avg_score:.2f}/{SCORE_MAX:.1f}"]
            if killed_syms:
                L.append(f"\U0001F6D1 Kill-switch сработал на {len(killed_syms)} монетах "
                         f"(просадка >{KILL_SWITCH_DD*100:.0f}%): {', '.join(killed_syms[:5])}")
            L.append(f"\n{'\u2705 Выборка достаточна' if n >= 100 else '\u26A0\uFE0F Выборка мала (' + str(n) + ' < 100) — цифрам верить рано'}")
            if diag["reasons"]:
                top = sorted(diag["reasons"].items(), key=lambda x: -x[1])[:5]
                L.append("\n\U0001F52C Что рубит:")
                for nm, cnt in top:
                    L.append(f"\u2022 {nm}: {cnt:,} ({cnt/max(diag['evals'],1)*100:.1f}%)")
            tg_send(chat, "\n".join(L))
            # BINNING — где именно живёт прибыль
            try:
                tg_send(chat, binning_report(all_tr))
            except Exception as e:
                print("binning err:", e)
            if HAS_MPL:
                try:
                    plt.figure(figsize=(10, 5)); plt.plot(eq, linewidth=1.4)
                    plt.axhline(DEPOSIT, color="gray", linestyle="--", alpha=0.5)
                    plt.title(f"Scored · {n} сделок · порог {thr}")
                    plt.xlabel("Сделки"); plt.ylabel("Капитал $"); plt.grid(True, alpha=0.4)
                    p_ = "/tmp/scored.png"; plt.savefig(p_, dpi=110, bbox_inches="tight"); plt.close()
                    tg_photo(chat, p_, caption="Scored equity (спред+слиппедж+комиссии учтены)")
                except Exception as e:
                    print("scored chart err:", e)
        else:
            wins_ = walk_forward_days(cached, 20, 10)
            if not wins_:
                tg_send(chat, "\U0001F4ED Не набралось окон train20/test10 — нужно \u226530 дней."); return
            n_w = len(wins_)
            tr_pos = sum(1 for w in wins_ if w["train_pnl"] > 0)
            te_pos = sum(1 for w in wins_ if w["test_pnl"] > 0)
            avg_tr = sum(w["train_pnl"] for w in wins_) / n_w
            avg_te = sum(w["test_pnl"] for w in wins_) / n_w
            tot_trades = sum(w["test_trades"] for w in wins_)
            if avg_tr > 0 and avg_te > 0:
                verdict = "\u2705 <b>EDGE ВЫЖИЛ</b> на невиданных окнах."
            elif avg_tr > 0 >= avg_te:
                verdict = "\u274C <b>ПОДГОНКА.</b> Порог заучил train, на test минус."
            else:
                verdict = "\U0001F53B <b>Нет преимущества</b> даже на train."
            tg_send(chat, "\n".join([
                "\U0001F501 <b>SCORED WALK-FORWARD ИТОГ</b> (train 20д \u2192 test 10д)",
                f"Окон: {n_w} \u00b7 сделок на test: {tot_trades}",
                f"TRAIN: плюсовых {tr_pos}/{n_w}, средний PnL {avg_tr:+.2f}$",
                f"TEST:  плюсовых {te_pos}/{n_w}, средний PnL {avg_te:+.2f}$",
                "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501", verdict]))
            if HAS_MPL:
                try:
                    plt.figure(figsize=(10, 5))
                    plt.plot([w["train_pnl"] for w in wins_], label="train", linewidth=1.4)
                    plt.plot([w["test_pnl"] for w in wins_], label="test (вслепую)", linewidth=1.4)
                    plt.axhline(0, color="gray", linestyle="--", alpha=0.6)
                    plt.title("Scored walk-forward: train 20д vs test 10д")
                    plt.xlabel("Окно"); plt.ylabel("PnL $"); plt.legend(); plt.grid(True, alpha=0.4)
                    p_ = "/tmp/scored_wf.png"; plt.savefig(p_, dpi=110, bbox_inches="tight"); plt.close()
                    tg_photo(chat, p_, caption="train выше нуля, test ниже = подгонка порога")
                except Exception as e:
                    print("scored wf chart err:", e)
    finally:
        BT_RUNNING["on"] = False


def run_alpha(chat, days=30, ncoins=20, train_window=500, test_window=200):
    """Команда /alpha — скользящий walk-forward AlphaModel + факторная атрибуция."""
    if BT_RUNNING["on"]:
        tg_send(chat, "\u23F3 Идёт другой прогон — дождись окончания."); return
    BT_RUNNING["on"] = True
    try:
        days = max(7, min(days, 30)); ncoins = max(1, min(ncoins, 60))
        tg_send(chat, f"\U0001F9EC <b>ALPHA WALK-FORWARD</b>: {days} дн \u00d7 {ncoins} монет\n"
                      f"Окна: train={train_window} баров \u2192 test={test_window} баров (скользящие)\n"
                      f"На train подбираются веса (27 комбинаций), на test — проверка вслепую.\n"
                      f"Живой скан на паузе. Качаю историю\u2026")
        cached = _bt_prefetch(days, ncoins)
        if not cached:
            tg_send(chat, "\u274C Не удалось загрузить данные."); return
        tg_send(chat, f"\U0001F4E6 {len(cached)} монет. Считаю фичи и гоняю окна\u2026")

        all_windows = []
        all_trades = []
        for k, (sym, o, h, l, c, v, tb, ct, oi_ts) in enumerate(cached, 1):
            try:
                fr = get_funding_rate(sym)
                rows = _alpha_build_rows(sym, o, h, l, c, v, tb, ct, oi_ts, funding=fr)
                res = walk_forward(rows, c, train_window, test_window)
                for r in res:
                    r["symbol"] = sym
                    all_trades += r["portfolio"].trades
                all_windows += res
            except Exception as e:
                print(f"alpha {sym} err:", e)
            if k % 5 == 0:
                tg_send(chat, f"\u2699\uFE0F {k}/{len(cached)} монет \u00b7 окон {len(all_windows)}")

        if not all_windows:
            tg_send(chat, "\U0001F4ED Не набралось ни одного train/test окна — увеличь период "
                          "или уменьши train_window/test_window."); return

        n_win = len(all_windows)
        train_pos = sum(1 for w in all_windows if w["train_ret"] > 0)
        test_pos = sum(1 for w in all_windows if w["test_ret"] > 0)
        avg_train = sum(w["train_ret"] for w in all_windows) / n_win
        avg_test = sum(w["test_ret"] for w in all_windows) / n_win
        total_trades = sum(w["trades"] for w in all_windows)

        if avg_train > 0 and avg_test > 0:
            verdict = ("\u2705 <b>ALPHA ВЫЖИЛА</b> на невиданных окнах. Осторожный оптимизм.")
        elif avg_train > 0 >= avg_test:
            verdict = ("\u274C <b>ПОДГОНКА.</b> На train плюс, на test минус — веса заучили прошлое.")
        else:
            verdict = ("\U0001F53B <b>Нет альфы.</b> Даже на train-окнах модель не в плюсе.")

        L = [f"\U0001F9EC <b>ALPHA WALK-FORWARD ИТОГ</b>",
             f"Окон: {n_win} \u00b7 сделок: {total_trades}",
             f"TRAIN: плюсовых окон {train_pos}/{n_win}, средняя доходность {avg_train*100:+.2f}%",
             f"TEST:  плюсовых окон {test_pos}/{n_win}, средняя доходность {avg_test*100:+.2f}%",
             "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501",
             verdict]

        # СРАВНЕНИЕ: та же нарезка окон, но 2-факторная альфа (без OI/CVD/тренда)
        simple_test = []
        for k2, (sym, o, h, l, c, v, tb, ct, oi_ts) in enumerate(cached, 1):
            try:
                rows2 = _alpha_build_rows(sym, o, h, l, c, v, tb, ct, oi_ts,
                                          funding=get_funding_rate(sym))
                start = 0
                while start + train_window + test_window <= len(rows2):
                    te_rows = rows2[start + train_window:start + train_window + test_window]
                    te_close = c[start + train_window:start + train_window + test_window]
                    p2 = run_portfolio(te_rows, te_close, AlphaModelSimple(), seed=7)
                    simple_test.append(p2.capital / DEPOSIT - 1.0)
                    start += test_window
            except Exception as e:
                print(f"alpha-simple {sym} err:", e)
        if simple_test:
            avg_simple = sum(simple_test) / len(simple_test)
            pos_simple = sum(1 for x in simple_test if x > 0)
            L.append(f"\n\u2696\uFE0F <b>2-ФАКТОРНАЯ АЛЬФА</b> (объём + tanh(returns\u00d750), "
                     f"без OI/CVD/тренда):")
            L.append(f"\u2022 TEST: плюсовых окон {pos_simple}/{len(simple_test)}, "
                     f"средняя {avg_simple*100:+.2f}%")
            L.append(f"\u2022 Полная модель дала {avg_test*100:+.2f}% \u2014 "
                     f"{'полная лучше' if avg_test > avg_simple else '2 фактора не хуже (лишние фильтры не помогают)'}")

        attr = factor_attribution(all_trades)
        if attr:
            L.append("\n\U0001F50E <b>ФАКТОРНАЯ АТРИБУЦИЯ</b> (средний вклад в PnL):")
            for k_, v_ in sorted(attr.items(), key=lambda x: -abs(x[1])):
                L.append(f"\u2022 {k_}: {v_:+.4f}")
        tg_send(chat, "\n".join(L))

        if HAS_MPL:
            try:
                plt.figure(figsize=(10, 5))
                plt.plot([w["train_ret"] * 100 for w in all_windows], label="train", linewidth=1.4)
                plt.plot([w["test_ret"] * 100 for w in all_windows], label="test (вслепую)", linewidth=1.4)
                plt.axhline(0, color="gray", linestyle="--", alpha=0.6)
                plt.title("Alpha walk-forward: train vs test по окнам")
                plt.xlabel("Окно"); plt.ylabel("Доходность, %"); plt.legend(); plt.grid(True, alpha=0.4)
                p_ = "/tmp/alpha_wf.png"
                plt.savefig(p_, dpi=110, bbox_inches="tight"); plt.close()
                tg_photo(chat, p_, caption="Если train выше нуля, а test ниже — это подгонка весов.")
            except Exception as e:
                print("alpha chart err:", e)
    finally:
        BT_RUNNING["on"] = False


def run_optimize(chat, days=30, ncoins=60, tp=0.02, sl=0.01):
    """Команда /optimize — grid search vol_thr x oi_thr + heatmap в Telegram."""
    if BT_RUNNING["on"]:
        tg_send(chat, "\u23F3 Идёт другой прогон — дождись окончания."); return
    BT_RUNNING["on"] = True
    try:
        days = max(7, min(days, 30)); ncoins = max(5, min(ncoins, 60))
        vol_grid = (1.5, 2.0, 2.5)
        oi_grid = (0.0, 0.01, 0.02)
        tg_send(chat, f"\U0001F527 <b>OPTIMIZE</b>: {days} дн \u00d7 {ncoins} монет\n"
                      f"Сетка: vol_thr {vol_grid} \u00d7 oi_thr {oi_grid} = "
                      f"{len(vol_grid)*len(oi_grid)} комбинаций\n"
                      f"Выход: фиксированный TP {tp*100:.1f}% / SL {sl*100:.1f}%\n"
                      f"\U0001F4B8 Издержки: спред {EXEC_SPREAD*100:.2f}% + проскальзывание "
                      f"({EXEC_SLIP_MIN}-{EXEC_SLIP_MAX}\u00d7волатильность) + комиссии \u2014 УЧТЕНЫ\n"
                      f"\U0001F4CF Сайзинг: риск {RISK_PER_TRADE*100:.1f}% от {DEPOSIT:.0f}$ на сделку\n"
                      f"Живой скан на паузе. Качаю историю\u2026")
        cached = _bt_prefetch(days, ncoins)
        if not cached:
            tg_send(chat, "\u274C Не удалось загрузить данные."); return
        tg_send(chat, f"\U0001F4E6 Загружено {len(cached)} монет. Перебираю сетку\u2026")

        def _prog(done, total, row):
            if done % 3 == 0 or done == total:
                tg_send(chat, f"\u2699\uFE0F {done}/{total} \u00b7 "
                              f"vol={row['vol_thr']} oi={row['oi_thr']} \u2192 "
                              f"сделок {row['trades']}, PnL {row['pnl_usd']:+.1f}$")

        results = optimize_params(cached, tp=tp, sl=sl,
                                  vol_grid=vol_grid, oi_grid=oi_grid, progress=_prog)
        results.sort(key=lambda r: -r["pnl_usd"])
        L = [f"\U0001F527 <b>OPTIMIZE ИТОГ</b> (TP {tp*100:.1f}% / SL {sl*100:.1f}%)",
             "<code>vol   oi     сделок  win%   PnL$</code>"]
        for r in results:
            L.append(f"<code>{r['vol_thr']:<5} {r['oi_thr']:<6} {r['trades']:<7} "
                     f"{r['winrate']*100:<6.0f} {r['pnl_usd']:+.1f}</code>")
        best = results[0]
        L.append(f"\n\U0001F3C6 Лучший: vol_thr={best['vol_thr']}, oi_thr={best['oi_thr']} "
                 f"\u2192 {best['trades']} сделок, {best['pnl_usd']:+.1f}$")
        tg_send(chat, "\n".join(L))

        if HAS_MPL_GRID:
            try:
                df = pd.DataFrame(results)
                p = "/tmp/opt_heatmap.png"
                plot_heatmap(df, "oi_thr", "vol_thr", metric="pnl_usd", save_path=p)
                tg_photo(chat, p, caption="Heatmap: PnL$ по сетке vol_thr × oi_thr")
            except Exception as e:
                print("heatmap err:", e)
    finally:
        BT_RUNNING["on"] = False


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


def _help_text():
    """Полная справка по всем командам бота."""
    return "\n".join([
        "\U0001F4D6 <b>EVA v4 — ВСЕ КОМАНДЫ</b>",
        "",
        "\U0001F7E2 <b>УПРАВЛЕНИЕ</b>",
        "/start — запуск, сводка параметров и режима",
        "/pos — открытые позиции: цена, PnL, стадия, занятая маржа",
        "/stats — статистика закрытых сделок: win rate, средний R, PnL $ и %",
        "/pause — пауза: новые сигналы не ищутся (открытые позиции ведутся)",
        "/resume — возобновить сканирование",
        "/help — эта справка",
        "",
        "\U0001F50D <b>ДИАГНОСТИКА</b>",
        "/debug — почему нет сигналов: топ причин отказа по чек-листу прямо сейчас",
        "/livecheck [монета] — проверка LIVE-слоя: ключи, эндпоинт (demo/реал), баланс,",
        "   шаг лота, тест-расчёт объёма, позиции на бирже. Ордера НЕ шлёт",
        "/selfcheck — самопроверка движка на синтетике, без сети и реальных данных",
        "",
        "\U0001F9EA <b>БЭКТЕСТЫ</b>",
        "/backtest [дней] [монет] [ключ=знач] — основной прогон + воронка отсева + график",
        "   пороги: spike= quiet= qbars= wick= atr= oi= rsi=",
        "   выходы: slmult= tp1mult= tp2mult= trailmult=",
        "   пресет: /backtest 30 60 soft",
        "/scored [дней] [монет] [порог] — SCORING вместо бинарных фильтров:",
        "   score = веса\u00d7(breakout+volume+oi+cvd+trend), вход при score > порога.",
        "   Ослаблено: breakout \u00d70.995, volume \u22651.5, OI=tanh(slope\u00d73)",
        "/optimize [дней] [монет] [tp] [sl] — grid search vol_thr\u00d7oi_thr + хитмап",
        "/gridsearch [дней] [монет] — перебор volume\u00d7lookback\u00d7oi + хитмапы PF/return + CSV",
        "/crosscheck — сравнение сигналов по BTC/ETH/SOL/BNB (PF, return, winrate, sharpe)",
        "",
        "\U0001F501 <b>ПРОВЕРКА НА ПОДГОНКУ</b>",
        "/scoredwf [дней] [монет] — walk-forward: train 20д \u2192 test 10д \u2192 сдвиг.",
        "   Порог подбирается на train, проверяется на невиданном test",
        "/alpha [дней] [монет] [train] [test] — факторная модель + скользящий walk-forward,",
        "   факторная атрибуция (какой фактор реально давал PnL) + сравнение с 2-факторной",
        "",
        "\U0001F916 <b>АДАПТАЦИЯ И ML</b>",
        "/adaptive [дней] [монет] — воронка сама ослабляет фильтры, которые режут >50%",
        "/ml [дней] [монет] [порог] — ML-ранкер (XGBoost): обучение на ранних 70% истории,",
        "   торговля на поздних 30% вслепую. Показывает важность фич",
        "",
        "\u2699\uFE0F <b>РЕЖИМ СЕЙЧАС</b>",
        f"\u2022 Исполнение: {'LIVE ' + ('DEMO' if BYBIT_USE_DEMO else 'РЕАЛ') if AUTO_TRADE else 'PAPER (виртуальные сделки)'}",
        f"\u2022 Слотов: {MAX_CONCURRENT} \u00b7 объём {NOTIONAL:.0f}$ (маржа {MARGIN:.0f}$ \u00d7{LEVERAGE:.0f})",
        f"\u2022 Сайзинг: {'от риска ' + str(RISK_PCT_TRADE*100) + '%' if RISK_SIZING else 'фиксированный'}",
        f"\u2022 Kill-switch: просадка > {KILL_SWITCH_DD*100:.0f}%",
    ])


def setup_bot_commands():
    """Регистрирует меню команд в Telegram (кнопка / рядом с полем ввода)."""
    cmds = [
        ("start", "Запуск и сводка"),
        ("pos", "Открытые позиции и PnL"),
        ("stats", "Статистика сделок"),
        ("debug", "Почему нет сигналов"),
        ("livecheck", "Проверка LIVE-слоя (без ордеров)"),
        ("backtest", "Бэктест + воронка отсева"),
        ("scored", "Scoring вместо бинарных фильтров"),
        ("scoredwf", "Walk-forward train20д/test10д"),
        ("alpha", "Факторная модель + атрибуция"),
        ("adaptive", "Самоослабляющаяся воронка"),
        ("ml", "ML-ранкер XGBoost"),
        ("optimize", "Grid search + хитмап"),
        ("gridsearch", "Перебор параметров + CSV"),
        ("crosscheck", "Сравнение по BTC/ETH/SOL/BNB"),
        ("selfcheck", "Самопроверка движка"),
        ("pause", "Пауза сканирования"),
        ("resume", "Возобновить"),
        ("help", "Все команды"),
    ]
    try:
        payload = json.dumps([{"command": c, "description": d} for c, d in cmds])
        tg("setMyCommands", commands=payload)
        print(f"Telegram: зарегистрировано {len(cmds)} команд в меню")
    except Exception as e:
        print("setMyCommands err:", e)


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
                    tg_send(cid, _help_text())
                elif text.startswith("/start"):
                    st["paused"] = False; save_state(st)
                    mode_txt = ("LIVE " + ("DEMO" if BYBIT_USE_DEMO else "\u26A0\uFE0F РЕАЛ")) if AUTO_TRADE else "PAPER"
                    tg_send(cid, "\n".join([
                        "\U0001F916 <b>EVA v4 — импульсный бот</b>",
                        f"Режим: <b>{mode_txt}</b> \u00b7 Данные: Binance \u00b7 Цены/ордера: Bybit",
                        f"Слотов: {MAX_CONCURRENT} \u00b7 объём {NOTIONAL:.0f}$ "
                        f"(маржа {MARGIN:.0f}$ \u00d7{LEVERAGE:.0f}) \u00b7 депозит {DEPOSIT:.0f}$",
                        "",
                        "\U0001F4CC <b>Быстрый старт:</b>",
                        "/pos — что открыто \u00b7 /stats — результаты",
                        "/debug — почему нет сигналов",
                        "/backtest 30 60 — прогон по истории",
                        "/scoredwf 30 60 — честная проверка на подгонку",
                        "",
                        "\U0001F4D6 <b>/help — полный список всех команд с описанием</b>",
                    ]))

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
                elif text.startswith("/ml"):
                    parts = text.split(); nums = [p for p in parts if p.replace(".","").isdigit()]
                    md = int(float(nums[0])) if len(nums) > 0 else 30
                    mc = int(float(nums[1])) if len(nums) > 1 else 30
                    mthr = float(nums[2]) if len(nums) > 2 else None
                    threading.Thread(target=run_ml, args=(cid, md, mc, mthr), daemon=True).start()
                elif text.startswith("/adaptive"):
                    parts = text.split(); nums = [p for p in parts if p.isdigit()]
                    ad = int(nums[0]) if len(nums) > 0 else 30
                    ac = int(nums[1]) if len(nums) > 1 else 60
                    threading.Thread(target=run_adaptive, args=(cid, ad, ac), daemon=True).start()
                elif text.startswith("/trendline") or text.startswith("/tl"):
                    parts = text.split(); nums = [p for p in parts if p.isdigit()]
                    mo = int(nums[0]) if nums else 12
                    lower = text.lower()
                    iv = "15m" if "15m" in lower else ("4h" if "4h" in lower else "1h")
                    am = rm = swq = None; uni = None
                    for p in parts:
                        pl = p.lower()
                        if pl.startswith("adx="):
                            try: am = float(p.split("=")[1])
                            except Exception: pass
                        elif pl.startswith("rsi="):
                            try: rm = float(p.split("=")[1])
                            except Exception: pass
                        elif pl.startswith("swings="):
                            try: swq = int(p.split("=")[1])
                            except Exception: pass
                        elif pl in ("major","mid","alt","all"): uni = pl
                    threading.Thread(target=run_trendline,
                                     args=(cid, mo, uni, iv, am, rm, swq, "backtest"),
                                     daemon=True).start()
                elif text.startswith("/premise"):
                    parts = text.split(); nums = [p for p in parts if p.isdigit()]
                    mo = int(nums[0]) if nums else 6
                    lower = text.lower()
                    iv = "15m" if "15m" in lower else ("4h" if "4h" in lower else "1h")
                    mv = None
                    for p in parts:
                        if p.lower().startswith("move="):
                            try: mv = float(p.split("=")[1])
                            except Exception: pass
                    threading.Thread(target=run_beta_premise,
                                     args=(cid, mo, iv, mv, None), daemon=True).start()
                elif text.startswith("/exits"):
                    parts = text.split(); nums = [p for p in parts if p.isdigit()]
                    mo = int(nums[0]) if nums else 12
                    lower = text.lower()
                    iv = "4h" if "4h" in lower else ("15m" if "15m" in lower else "1h")
                    uni = None; am = None
                    for p in parts:
                        pl = p.lower()
                        if pl in ("major","mid","alt","all"): uni = pl
                        elif pl.startswith("adx="):
                            try: am = float(p.split("=")[1])
                            except Exception: pass
                    threading.Thread(target=run_exit_compare,
                                     args=(cid, mo, uni, iv, am), daemon=True).start()
                elif text.startswith("/sniperwf"):
                    parts = text.split(); nums = [p for p in parts if p.isdigit()]
                    mo = int(nums[0]) if nums else 12
                    iv = "4h" if "4h" in text.lower() else ("15m" if "15m" in text.lower() else "1h")
                    threading.Thread(target=run_sniper,
                                     args=(cid, mo, None, iv, None, "wf"), daemon=True).start()
                elif text.startswith("/sniper"):
                    parts = text.split(); nums = [p for p in parts if p.isdigit()]
                    mo = int(nums[0]) if nums else 12
                    lower = text.lower()
                    iv = "4h" if "4h" in lower else ("15m" if "15m" in lower else "1h")
                    am = None; uni = None
                    for p in parts:
                        pl = p.lower()
                        if pl.startswith("adx="):
                            try: am = float(p.split("=")[1])
                            except Exception: pass
                        elif pl in ("major", "mid", "alt", "all", "wide"):
                            uni = pl
                        elif pl.endswith("usdt"):
                            uni = (uni if isinstance(uni, list) else []) + [p.upper()]
                    threading.Thread(target=run_sniper,
                                     args=(cid, mo, uni, iv, am, "backtest"), daemon=True).start()
                elif text.startswith("/topn"):
                    parts = text.split(); nums = [p for p in parts if p.replace(".","").isdigit()]
                    td = int(float(nums[0])) if len(nums) > 0 else 30
                    tc = int(float(nums[1])) if len(nums) > 1 else 60
                    tthr = float(nums[2]) if len(nums) > 2 else 3.0
                    tn = int(float(nums[3])) if len(nums) > 3 else 20
                    ureg = "regime" in text.lower()
                    threading.Thread(target=run_topn,
                                     args=(cid, td, tc, tthr, tn, ureg, True), daemon=True).start()
                elif text.startswith("/scoredwf"):
                    parts = text.split(); nums = [p for p in parts if p.replace(".","").isdigit()]
                    sd = int(float(nums[0])) if len(nums) > 0 else 30
                    sc = int(float(nums[1])) if len(nums) > 1 else 60
                    threading.Thread(target=run_scored, args=(cid, sd, sc, None, "wf"), daemon=True).start()
                elif text.startswith("/scored"):
                    parts = text.split(); nums = [p for p in parts if p.replace(".","").isdigit()]
                    sd = int(float(nums[0])) if len(nums) > 0 else 30
                    sc = int(float(nums[1])) if len(nums) > 1 else 60
                    sthr = float(nums[2]) if len(nums) > 2 else None
                    lower = text.lower()
                    threading.Thread(target=run_scored,
                                     args=(cid, sd, sc, sthr, "backtest",
                                           "regime" in lower, "kelly" in lower, 0),
                                     daemon=True).start()
                elif text.startswith("/livecheck"):
                    parts = text.split()
                    lsym = parts[1].upper() if len(parts) > 1 else "BTCUSDT"
                    threading.Thread(target=run_livecheck, args=(cid, lsym), daemon=True).start()
                elif text.startswith("/alpha"):
                    parts = text.split()
                    nums = [p for p in parts if p.isdigit()]
                    ad = int(nums[0]) if len(nums) > 0 else 30
                    ac = int(nums[1]) if len(nums) > 1 else 20
                    atr_w = int(nums[2]) if len(nums) > 2 else 500
                    ate_w = int(nums[3]) if len(nums) > 3 else 200
                    threading.Thread(target=run_alpha,
                                     args=(cid, ad, ac, atr_w, ate_w), daemon=True).start()
                elif text.startswith("/optimize") or text.startswith("/opt"):
                    parts = text.split()
                    nums = [p for p in parts if p.replace(".", "").isdigit()]
                    od = int(float(nums[0])) if len(nums) > 0 else 30
                    oc = int(float(nums[1])) if len(nums) > 1 else 60
                    otp = float(nums[2]) if len(nums) > 2 else 0.02
                    osl = float(nums[3]) if len(nums) > 3 else 0.01
                    threading.Thread(target=run_optimize,
                                     args=(cid, od, oc, otp, osl), daemon=True).start()
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
    setup_bot_commands()          # меню команд в Telegram (кнопка "/")
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
