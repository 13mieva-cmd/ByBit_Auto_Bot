#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EVA v3 — ИМПУЛЬСНЫЙ БОТ (РЕАЛЬНОЕ ИСПОЛНЕНИЕ через Bybit v5)
=====================================================
ДАННЫЕ: Binance Futures (объёмы, taker buy volume -> честный CVD)
ЦЕНЫ/ТОРГОВЛЯ: Bybit v5 (реальные лимитные ордера, TP/SL, трейлинг-стоп)
ИСПОЛНЕНИЕ: реальные сделки на Bybit (Demo Trading или Live — через ENV LIVE_TRADING/USE_DEMO_TRADING)

СТРАТЕГИЯ (чек-лист, LONG):
1. Затишье: объёмы ровные относительно Volume MA20
2. Триггер: всплеск объёма на M15 >= 2.5x MA20 (1-я свеча импульса)
3. Структура: 3 зелёные свечи подряд, без длинной верхней тени у 3-й (<=30%)
4. Размер каждой из 3 свечей ограничен ATR (не "паранормальный бар")
5. Соразмерность тел свечей (не 1 гигант + 2 карлика)
6. Деньги: OI растёт устойчиво + CVD (дельта) растёт на всех 3 свечах
7. Тренд: close > EMA21 > EMA50 (M15)
8. Логика: пробит локальный уровень (max high за сутки до импульса)
9. Безопасность: RSI14(M15) < 75
10. ВХОД: лимитка на ретесте (фибо 0.382 от импульса)
11. SL: под Low 1-й свечи с отступом 0.1%
12. TP1 (1:1): закрыть 50%, включить ТРЕЙЛИНГ (откат 0.4%) на остаток
13. ЛИМИТЫ: до MAX_CONCURRENT позиций одновременно; дневной лимит опционален

ДЕПЛОЙ: Railway
Переменные окружения:
  TG_TOKEN, DATA_DIR, DEPOSIT_USD, MARGIN_USD, LEVERAGE,
  MAX_CONCURRENT, MAX_DAILY_TRADES,
  BYBIT_API_KEY, BYBIT_API_SECRET, USE_DEMO_TRADING (true/false), LIVE_TRADING (true/false)
Start Command: python inflow_scanner_v2_render.py
"""

import os, time, json, csv, math, threading, hashlib, hmac
import datetime as dt
import urllib.request, urllib.parse

# ============================== КОНФИГ ==============================
TG_TOKEN = os.environ.get("TG_TOKEN", "")
DATA_DIR = os.environ.get("DATA_DIR", "/data")
DEPOSIT = float(os.environ.get("DEPOSIT_USD", 500))
MARGIN = float(os.environ.get("MARGIN_USD", 50))
LEVERAGE = float(os.environ.get("LEVERAGE", 10))
NOTIONAL = MARGIN * LEVERAGE

MAX_CONCURRENT = int(os.environ.get("MAX_CONCURRENT", 2))
MAX_DAILY_TRADES = int(os.environ.get("MAX_DAILY_TRADES", 0))

BYBIT_API_KEY = os.environ.get("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", "")
USE_DEMO_TRADING = os.environ.get("USE_DEMO_TRADING", "true").lower() == "true"
LIVE_TRADING = os.environ.get("LIVE_TRADING", "false").lower() == "true"
BYBIT_TRADE_BASE = "https://api-demo.bybit.com" if USE_DEMO_TRADING else "https://api.bybit.com"
RECV_WINDOW = "5000"

# --- сигнал (спека) — все пороги настраиваются через ENV в Railway ---
TF = os.environ.get("TF", "15m")
VOL_MA_LEN = int(os.environ.get("VOL_MA_LEN", 20))
VOL_SPIKE_MIN = float(os.environ.get("VOL_SPIKE_MIN", 2.5))
QUIET_BARS = int(os.environ.get("QUIET_BARS", 12))
QUIET_MAX = float(os.environ.get("QUIET_MAX", 1.8))
WICK_MAX = float(os.environ.get("WICK_MAX", 0.30))
ATR_LEN = int(os.environ.get("ATR_LEN", 14))
BAR3_ATR_MAX = float(os.environ.get("BAR3_ATR_MAX", 2.5))
BODY_RATIO_MAX = float(os.environ.get("BODY_RATIO_MAX", 3.0))
OI_MIN_GROW = float(os.environ.get("OI_MIN_GROW", 0.01))
RSI_LEN = int(os.environ.get("RSI_LEN", 14))
RSI_MAX = float(os.environ.get("RSI_MAX", 75.0))
LEVEL_LOOKBACK = int(os.environ.get("LEVEL_LOOKBACK", 96))
EMA_FAST = int(os.environ.get("EMA_FAST", 21))
EMA_SLOW = int(os.environ.get("EMA_SLOW", 50))

# --- вход/выход — настраиваются через ENV ---
FIB_RETRACE = float(os.environ.get("FIB_RETRACE", 0.382))
ENTRY_TTL_BARS = int(os.environ.get("ENTRY_TTL_BARS", 8))
SL_BUFFER = float(os.environ.get("SL_BUFFER", 0.001))
TP1_RR = float(os.environ.get("TP1_RR", 1.0))
TRAIL_CALLBACK = float(os.environ.get("TRAIL_CALLBACK", 0.004))
FEE_MAKER = float(os.environ.get("FEE_MAKER", 0.0002))
FEE_TAKER = float(os.environ.get("FEE_TAKER", 0.00055))

# --- вселенная — настраиваются через ENV ---
MAX_COINS = int(os.environ.get("MAX_COINS", 120))
MIN_QUOTE_VOL24 = float(os.environ.get("MIN_QUOTE_VOL24", 5_000_000))
SCAN_EVERY_SEC = int(os.environ.get("SCAN_EVERY_SEC", 90))
MANAGE_EVERY_SEC = int(os.environ.get("MANAGE_EVERY_SEC", 45))

def ensure_dirs():
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
    except Exception:
        pass
ensure_dirs()

STATE_FILE = os.path.join(DATA_DIR, "v3_state.json")
TRADES_FILE = os.path.join(DATA_DIR, "v3_trades.csv")
SIGNALS_FILE = os.path.join(DATA_DIR, "v3_signals.csv")
CHAT_FILE = os.path.join(DATA_DIR, "v3_chat.txt")

BINANCE = "https://fapi.binance.com"
BYBIT = "https://api.bybit.com"

# ============================== HTTP/TG ==============================
def http_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "eva-v3"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())

def tg(method, _timeout=35, **kw):
    if not TG_TOKEN:
        return None
    try:
        data = urllib.parse.urlencode(kw).encode()
        req = urllib.request.Request(f"https://api.telegram.org/bot{TG_TOKEN}/{method}", data=data)
        with urllib.request.urlopen(req, timeout=_timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print("tg err:", e)
        return None

def tg_send(chat, text):
    if not chat:
        return
    tg("sendMessage", chat_id=chat, text=text, parse_mode="HTML", disable_web_page_preview=True)

def load_chat():
    try:
        with open(CHAT_FILE) as f:
            return f.read().strip()
    except Exception:
        return None

def save_chat(cid):
    try:
        with open(CHAT_FILE, "w") as f:
            f.write(str(cid))
    except Exception:
        pass

# ============================== ДАННЫЕ ==============================
_uni_cache = {"ts": 0, "coins": []}
_klines_cache = {}
_last_bar_scanned = {}

def universe():
    if time.time() - _uni_cache["ts"] < 3600 and _uni_cache["coins"]:
        return _uni_cache["coins"]
    try:
        time.sleep(0.2)
        tick = http_json(f"{BINANCE}/fapi/v1/ticker/24hr", timeout=15)
        binance = {}
        for t in tick:
            s = t.get("symbol", "")
            if not s.endswith("USDT"):
                continue
            qv = float(t.get("quoteVolume", 0) or 0)
            if qv >= MIN_QUOTE_VOL24:
                binance[s] = qv
        time.sleep(0.3)
        by = http_json(f"{BYBIT}/v5/market/tickers?category=linear", timeout=15)
        bybit_syms = {x["symbol"] for x in by.get("result", {}).get("list", [])}
        coins = [s for s in binance if s in bybit_syms]
        coins.sort(key=lambda s: -binance[s])
        _uni_cache["coins"] = coins[:MAX_COINS]
        _uni_cache["ts"] = time.time()
        print(f"Universe updated: {len(coins)} coins")
    except Exception as e:
        print("universe err:", e)
    return _uni_cache.get("coins", [])

def klines15(symbol, limit=200):
    now = time.time()
    cache_key = (symbol, limit)
    if cache_key in _klines_cache:
        data, ts = _klines_cache[cache_key]
        if now - ts < 300:
            return data
    try:
        d = http_json(f"{BINANCE}/fapi/v1/klines?symbol={symbol}&interval={TF}&limit={limit}")
        o = [float(x[1]) for x in d]
        h = [float(x[2]) for x in d]
        l = [float(x[3]) for x in d]
        c = [float(x[4]) for x in d]
        v = [float(x[5]) for x in d]
        tb = [float(x[9]) for x in d]
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

# ============================== ИНДИКАТОРЫ ==============================
def ema_series(v, span):
    if not v:
        return []
    a = 2 / (span + 1)
    out = [v[0]]
    for x in v[1:]:
        out.append(a * x + (1 - a) * out[-1])
    return out

def rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    g = l = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        if d >= 0:
            g += d
        else:
            l -= d
    ag, al = g / period, l / period
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (period - 1) + max(d, 0)) / period
        al = (al * (period - 1) + max(-d, 0)) / period
    if al == 0:
        return 100.0
    return 100 - 100 / (1 + ag / al)

def atr(h, l, c, period=14):
    n = len(c)
    if n < period + 2:
        return 0.0
    trs = []
    for i in range(1, n):
        trs.append(max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])))
    a = sum(trs[:period]) / period
    for x in trs[period:]:
        a = (a * (period - 1) + x) / period
    return a

# ============================== СИГНАЛ ==============================
def detect_signal(o, h, l, c, v, tb, oi):
    n = len(c)
    if n < LEVEL_LOOKBACK + 30:
        return False, "мало истории"
    i1, i2, i3 = n - 3, n - 2, n - 1

    green = all(c[i] > o[i] for i in (i1, i2, i3))
    if not green:
        return False, "нет 3 зелёных"
    if not (c[i2] > h[i1] or c[i3] > h[i2]):
        return False, "нет закрытий выше high"

    base = v[i1 - VOL_MA_LEN:i1]
    if len(base) < VOL_MA_LEN:
        return False, "мало объёмной базы"
    vma = sum(base) / len(base)
    if vma <= 0:
        return False, "нулевая база"
    quiet = all(x <= vma * QUIET_MAX for x in v[i1 - QUIET_BARS:i1])
    if not quiet:
        return False, "не было затишья"
    spike = v[i1] / vma
    if spike < VOL_SPIKE_MIN:
        return False, f"слабый всплеск x{spike:.1f}"

    rng3 = h[i3] - l[i3]
    if rng3 <= 0:
        return False, "нулевая 3-я свеча"
    upper_wick = (h[i3] - c[i3]) / rng3
    if upper_wick > WICK_MAX:
        return False, f"фитиль {upper_wick*100:.0f}%>30%"

    a = atr(h[:i3], l[:i3], c[:i3], ATR_LEN)
    if a > 0:
        for idx, lbl in ((i1, "1-я"), (i2, "2-я"), (i3, "3-я")):
            rng_i = h[idx] - l[idx]
            if rng_i > BAR3_ATR_MAX * a:
                return False, f"{lbl} свеча параболик ({rng_i/a:.1f}x ATR)"

    bodies = [abs(c[idx] - o[idx]) for idx in (i1, i2, i3)]
    if min(bodies) > 0:
        ratio = max(bodies) / min(bodies)
        if ratio > BODY_RATIO_MAX:
            return False, f"свечи неравномерны (тела различаются в {ratio:.1f}x)"

    deltas = [2 * tb[i] - v[i] for i in (i1, i2, i3)]
    if not all(d > 0 for d in deltas):
        return False, "дельта не растёт"

    oi_ok = False
    oi_chg = 0.0
    if len(oi) >= 5:
        oi_before = oi[-5]
        oi_chg = (oi[-1] / oi_before - 1) if oi_before > 0 else 0
        oi_ok = oi_chg >= OI_MIN_GROW and all(oi[i] >= oi[i-1] * 0.995 for i in range(-3, 0))
    if not oi_ok:
        return False, f"OI не растёт ({oi_chg*100:+.1f}%)"

    e21 = ema_series(c, EMA_FAST)[-1]
    e50 = ema_series(c, EMA_SLOW)[-1]
    if not (c[i3] > e21 > e50):
        return False, "нет аптренда EMA"

    r = rsi(c[-(RSI_LEN * 6):], RSI_LEN)
    if r > RSI_MAX:
        return False, f"RSI {r:.0f} перегрет"

    level = max(h[i1 - LEVEL_LOOKBACK:i1])
    if not (c[i2] > level or c[i3] > level):
        return False, "уровень не пробит"

    impulse = h[i3] - l[i1]
    if impulse <= 0:
        return False, "нет импульса"
    entry = h[i3] - FIB_RETRACE * impulse
    sl = l[i1] * (1 - SL_BUFFER)
    if entry <= sl:
        return False, "вход ниже стопа"
    risk_pct = (entry - sl) / entry
    tp1 = entry + (entry - sl) * TP1_RR

    return True, dict(
        spike=spike, deltas=deltas, oi_chg=oi_chg, rsi=r,
        e21=e21, e50=e50, level=level, low1=l[i1], high3=h[i3],
        entry=entry, sl=sl, tp1=tp1, risk_pct=risk_pct,
        wick=upper_wick, close3=c[i3],
    )

# ============================== BYBIT REAL EXECUTION ==============================
def _bybit_sign(payload_str, ts):
    raw = f"{ts}{BYBIT_API_KEY}{RECV_WINDOW}{payload_str}"
    return hmac.new(BYBIT_API_SECRET.encode(), raw.encode(), hashlib.sha256).hexdigest()

def bybit_signed_get(path, params=None):
    params = params or {}
    qs = urllib.parse.urlencode(params)
    ts = str(int(time.time() * 1000))
    sign = _bybit_sign(qs, ts)
    url = f"{BYBIT_TRADE_BASE}{path}"
    if qs:
        url += f"?{qs}"
    req = urllib.request.Request(url, method="GET", headers={
        "X-BAPI-API-KEY": BYBIT_API_KEY, "X-BAPI-SIGN": sign, "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": ts, "X-BAPI-RECV-WINDOW": RECV_WINDOW, "User-Agent": "eva-v3",
    })
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())

def bybit_signed_post(path, body):
    body_str = json.dumps(body, separators=(",", ":"))
    ts = str(int(time.time() * 1000))
    sign = _bybit_sign(body_str, ts)
    url = f"{BYBIT_TRADE_BASE}{path}"
    req = urllib.request.Request(url, method="POST", data=body_str.encode(), headers={
        "X-BAPI-API-KEY": BYBIT_API_KEY, "X-BAPI-SIGN": sign, "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": ts, "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "Content-Type": "application/json", "User-Agent": "eva-v3",
    })
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())

_instr_cache = {}
def bybit_instrument_filters(symbol):
    if symbol in _instr_cache:
        return _instr_cache[symbol]
    try:
        d = http_json(f"{BYBIT}/v5/market/instruments-info?category=linear&symbol={symbol}")
        info = d["result"]["list"][0]
        qty_step = float(info["lotSizeFilter"]["qtyStep"])
        tick_size = float(info["priceFilter"]["tickSize"])
        _instr_cache[symbol] = (qty_step, tick_size)
        return qty_step, tick_size
    except Exception as e:
        print(f"instrument_filters {symbol} err:", e)
        return 0.001, 0.0001

def _round_step(value, step):
    if step <= 0:
        return value
    return math.floor(value / step) * step

def _fmt(value):
    return f"{value:.8f}".rstrip("0").rstrip(".")

def bybit_place_limit_buy(symbol, qty, price):
    qty_step, tick_size = bybit_instrument_filters(symbol)
    qty_r = _round_step(qty, qty_step)
    price_r = _round_step(price, tick_size)
    body = {
        "category": "linear", "symbol": symbol, "side": "Buy", "orderType": "Limit",
        "qty": _fmt(qty_r), "price": _fmt(price_r), "timeInForce": "GTC", "positionIdx": 0,
    }
    try:
        resp = bybit_signed_post("/v5/order/create", body)
        if resp.get("retCode") != 0:
            print(f"bybit order err {symbol}:", resp.get("retMsg"))
            return None
        return resp["result"]["orderId"], qty_r, price_r
    except Exception as e:
        print(f"bybit_place_limit_buy {symbol} err:", e)
        return None

def bybit_cancel_order(symbol, order_id):
    try:
        resp = bybit_signed_post("/v5/order/cancel", {"category": "linear", "symbol": symbol, "orderId": order_id})
        return resp.get("retCode") == 0
    except Exception as e:
        print(f"bybit_cancel_order {symbol} err:", e)
        return False

def bybit_set_trading_stop(symbol, stop_loss=None, take_profit=None, tp_size=None, trailing_stop=None, active_price=None):
    _, tick_size = bybit_instrument_filters(symbol)
    body = {"category": "linear", "symbol": symbol, "positionIdx": 0}
    if stop_loss is not None:
        body["stopLoss"] = _fmt(_round_step(stop_loss, tick_size))
    if take_profit is not None:
        body["takeProfit"] = _fmt(_round_step(take_profit, tick_size))
        body["tpslMode"] = "Partial" if tp_size else "Full"
        if tp_size:
            body["tpSize"] = _fmt(tp_size)
    if trailing_stop is not None:
        body["trailingStop"] = str(trailing_stop)
        if active_price is not None:
            body["activePrice"] = _fmt(_round_step(active_price, tick_size))
    try:
        resp = bybit_signed_post("/v5/position/trading-stop", body)
        if resp.get("retCode") != 0:
            print(f"trading_stop err {symbol}:", resp.get("retMsg"))
            return False
        return True
    except Exception as e:
        print(f"bybit_set_trading_stop {symbol} err:", e)
        return False

def bybit_get_position(symbol):
    try:
        resp = bybit_signed_get("/v5/position/list", {"category": "linear", "symbol": symbol})
        lst = resp.get("result", {}).get("list", [])
        for p in lst:
            size = float(p.get("size", 0) or 0)
            if size > 0:
                return size, float(p.get("avgPrice", 0) or 0), float(p.get("unrealisedPnl", 0) or 0)
        return None
    except Exception as e:
        print(f"bybit_get_position {symbol} err:", e)
        return None

def bybit_close_market(symbol, qty):
    qty_step, _ = bybit_instrument_filters(symbol)
    qty_r = _round_step(qty, qty_step)
    if qty_r <= 0:
        return False
    body = {
        "category": "linear", "symbol": symbol, "side": "Sell", "orderType": "Market",
        "qty": _fmt(qty_r), "timeInForce": "IOC", "reduceOnly": True, "positionIdx": 0,
    }
    try:
        resp = bybit_signed_post("/v5/order/create", body)
        return resp.get("retCode") == 0
    except Exception as e:
        print(f"bybit_close_market {symbol} err:", e)
        return False

def bybit_get_open_order(symbol, order_id):
    try:
        resp = bybit_signed_get("/v5/order/realtime", {"category": "linear", "symbol": symbol, "orderId": order_id})
        lst = resp.get("result", {}).get("list", [])
        return lst[0] if lst else None
    except Exception as e:
        print(f"bybit_get_open_order {symbol} err:", e)
        return None

# ============================== СОСТОЯНИЕ/ЛИМИТЫ ==============================
def _default_state():
    return dict(day=str(dt.datetime.now(dt.timezone.utc).date()),
                trades_today=0, paused=False, pendings={}, positions={})

def load_state():
    try:
        with open(STATE_FILE) as f:
            st = json.load(f)
    except Exception:
        return _default_state()
    if "pendings" not in st:
        st["pendings"] = {}
        p = st.pop("pending", None)
        if p:
            st["pendings"][p["sym"]] = p
    if "positions" not in st:
        st["positions"] = {}
        p = st.pop("position", None)
        if p:
            st["positions"][p["sym"]] = p
    return st

def save_state(st):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(st, f)
    except Exception as e:
        print("state save err:", e)

def utc_day():
    return str(dt.datetime.now(dt.timezone.utc).date())

def roll_day(st, chat=None):
    d = utc_day()
    if d != st.get("day"):
        st["day"] = d
        st["trades_today"] = 0
        save_state(st)
        if chat:
            tg_send(chat, f"🌅 Новый день (UTC) — счётчик сделок обнулён ({daily_txt(st)}).")

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
    if st.get("paused"):
        return False, "пауза"
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
            if new:
                w.writerow(["ts", "coin", "type", "price", "btc_price"])
            b = bybit_price("BTCUSDT") or ""
            w.writerow([dt.datetime.now().isoformat(timespec="seconds"), coin, "impulse", price, b])
    except Exception as e:
        print("log_signal err:", e)

def log_trade(row):
    try:
        new = not os.path.exists(TRADES_FILE)
        with open(TRADES_FILE, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["ts_open", "ts_close", "coin", "entry", "exit", "qty", "part", "pnl_usd", "r_mult", "reason"])
            w.writerow(row)
    except Exception as e:
        print("log_trade err:", e)

# ============================== РЕАЛЬНЫЙ ТОРГОВЫЙ ДВИЖОК ==============================
def _profit_scenarios(entry, sl, tp1, qty):
    fee_in = NOTIONAL * FEE_MAKER
    def leg(exit_px, q, fee_share):
        return (exit_px - entry) * q - exit_px * q * FEE_TAKER - fee_share
    half = qty / 2
    risk = entry - sl
    stop_full = leg(sl, qty, fee_in)
    tp1_half = leg(tp1, half, fee_in / 2)
    trail_at_t1 = tp1_half + leg(tp1 * (1 - TRAIL_CALLBACK), half, fee_in / 2)
    r2 = tp1_half + leg(entry + 2 * risk, half, fee_in / 2)
    r3 = tp1_half + leg(entry + 3 * risk, half, fee_in / 2)
    return stop_full, tp1_half, trail_at_t1, r2, r3

def open_pending(st, sym, d, chat):
    """Выставляет РЕАЛЬНУЮ лимитную заявку на Bybit и сохраняет её в state."""
    qty = NOTIONAL / d["entry"]
    result = bybit_place_limit_buy(sym, qty, d["entry"])
    if result is None:
        tg_send(chat, f"⚠️ {sym}: ошибка выставления ордера на Bybit — сигнал пропущен.")
        return
    order_id, qty_r, price_r = result

    st["pendings"][sym] = dict(sym=sym, entry=price_r, sl=d["sl"], tp1=d["tp1"],
                                low1=d["low1"], high3=d["high3"], ttl=ENTRY_TTL_BARS,
                                order_id=order_id, qty=qty_r, born=time.time())
    save_state(st)

    risk_all = open_risk_usd(st)
    risk_usd = NOTIONAL * d["risk_pct"]
    stop_full, tp1_half, trail_min, r2, r3 = _profit_scenarios(price_r, d["sl"], d["tp1"], qty_r)
    by = bybit_price(sym)
    impulse_pct = (d["high3"] / d["low1"] - 1) * 100
    dsum = sum(d.get("deltas", [])) or 0
    mode_txt = "🔴 LIVE (реальные деньги)" if LIVE_TRADING and not USE_DEMO_TRADING else "🟡 DEMO TRADING (виртуальный баланс Bybit)"
    warn = ""
    if risk_usd > DEPOSIT * 0.02:
        warn = (f"\n⚠️ Риск {risk_usd:.0f}$ = {risk_usd/DEPOSIT*100:.1f}% депозита — "
                f"выше правила 1-2%. Параметры (маржа {MARGIN:.0f}$ x{LEVERAGE:.0f}) агрессивны.")
    L = [
        f"🚀 {sym} · СИГНАЛ: импульс 3 свечей [{mode_txt}]",
        f"💵 Цена: ${d['close3']:.6g} (Binance)" + (f" · ${by:.6g} (Bybit)" if by else ""),
        "",
        "🧠 ПОЧЕМУ ВХОЖУ — весь чек-лист (факты):",
        f"✅ Затишье было, затем ВСПЛЕСК объёма ×{d['spike']:.1f} от MA20 (порог ≥{VOL_SPIKE_MIN}x)",
        f"✅ 3 зелёные свечи подряд: импульс {d['low1']:.6g} → {d['high3']:.6g} (+{impulse_pct:.1f}%)",
        f"✅ Фитиль 3-й свечи {d['wick']*100:.0f}% (≤30%) — продавец не гасит",
        f"✅ Все 3 свечи в норме ATR и соразмерны (не 1 гигант + 2 карлика)",
        f"✅ CVD растёт: дельта покупок положительна на всех 3 свечах (+{dsum:,.0f})",
        f"✅ OI {d['oi_chg']*100:+.1f}% — заходят НОВЫЕ деньги (не шорт-сквиз)",
        f"✅ Тренд: цена > EMA21 (${d['e21']:.6g}) > EMA50 (${d['e50']:.6g})",
        f"✅ Пробит суточный уровень ${d['level']:.6g}",
        f"✅ RSI {d['rsi']:.0f} (<{RSI_MAX:.0f}) — не перегрет",
        "",
        "📋 ПЛАН СДЕЛКИ (ордер выставлен на Bybit):",
        f"📌 Лимитка BUY выставлена: ${price_r:.6g} (order_id: {order_id})",
        f"📦 Объём: ${NOTIONAL:.0f} = {qty_r:.4g} {sym.replace('USDT','')} (маржа {MARGIN:.0f}$ × плечо {LEVERAGE:.0f})",
        f"🛑 Стоп: ${d['sl']:.6g} (под Low 1-й свечи, −{d['risk_pct']*100:.2f}%) → потеря {stop_full:+.2f}$",
        f"🎯 TP1 (1:1): ${d['tp1']:.6g} → закрою 50% → {tp1_half:+.2f}$ в карман",
        f"🔓 После TP1: трейлинг {TRAIL_CALLBACK*100:.1f}% на остаток 50%",
        "",
        "💰 СЦЕНАРИИ ИТОГА (с комиссиями):",
        f"• стоп-лосс: {stop_full:+.2f}$",
        f"• TP1 + трейлинг сразу: {trail_min:+.2f}$ (минимум после TP1)",
        f"• тренд до 2R: {r2:+.2f}$",
        f"• тренд до 3R: {r3:+.2f}$",
        f"{warn}",
        "",
        f"⏳ Жду филла лимитки максимум {ENTRY_TTL_BARS} свечей (2ч). Если не исполнится — отмена ордера на Bybit.",
        (f"🔗 Суммарный риск занятых слотов: ≈{risk_all:.2f}$ ({risk_all/DEPOSIT*100:.1f}% депозита)"
         if slots_used(st) > 1 else None),
        f"🧪 Слоты: {slots_used(st)}/{MAX_CONCURRENT} заняты · сделок сегодня: {st.get('trades_today',0)}",
        "Команды: /pos · /stats · /pause · /help",
    ]
    tg_send(chat, "\n".join(x for x in L if x is not None))
    log_signal(sym, d["close3"])

def cancel_pending(st, chat, sym, reason):
    p = st.get("pendings", {}).pop(sym, None)
    if not p:
        return
    order_id = p.get("order_id")
    if order_id:
        bybit_cancel_order(sym, order_id)
    tg_send(chat, f"❌ {sym}: лимитка отменена на Bybit — {reason}. Слот свободен ({slots_used(st)}/{MAX_CONCURRENT}).")
    save_state(st)

def fill_pending(st, chat, sym):
    """Проверяет, исполнился ли реальный ордер на Bybit; если да — ставит TP/SL."""
    p = st["pendings"][sym]
    order_id = p.get("order_id")
    order = bybit_get_open_order(sym, order_id) if order_id else None
    status = order.get("orderStatus") if order else None
    if status != "Filled":
        return False

    p = st["pendings"].pop(sym)
    qty = p["qty"]
    fee_in = NOTIONAL * FEE_MAKER

    ok_tp = bybit_set_trading_stop(sym, stop_loss=p["sl"], take_profit=p["tp1"], tp_size=qty * 0.5)

    st["positions"][sym] = dict(sym=sym, entry=p["entry"], sl=p["sl"], tp1=p["tp1"],
                                 qty=qty, qty_init=qty, fee_in=fee_in,
                                 half_closed=False, peak=0.0,
                                 opened=dt.datetime.now().isoformat(timespec="seconds"))
    st["trades_today"] = st.get("trades_today", 0) + 1
    save_state(st)
    stop_full, tp1_half, trail_min, r2, r3 = _profit_scenarios(p["entry"], p["sl"], p["tp1"], qty)
    tp_txt = "✅ TP/SL установлены на Bybit" if ok_tp else "⚠️ Ошибка установки TP/SL на Bybit — проверь вручную!"
    tg_send(chat,
        f"✅ {p['sym']}: ВХОД ИСПОЛНЕН на Bybit (ретест сработал)\n"
        f"💵 Цена входа: ${p['entry']:.6g}\n"
        f"📦 Куплено: {qty:.4g} {p['sym'].replace('USDT','')} на ${NOTIONAL:.0f} (маржа {MARGIN:.0f}$ ×{LEVERAGE:.0f})\n"
        f"🛑 Стоп ${p['sl']:.6g} → {stop_full:+.2f}$ · 🎯 TP1 ${p['tp1']:.6g} → {tp1_half:+.2f}$ за 50%\n"
        f"{tp_txt}\n"
        f"🔓 После TP1 — трейлинг {TRAIL_CALLBACK*100:.1f}%: тренд до 2R даст {r2:+.2f}$, до 3R — {r3:+.2f}$\n"
        f"📅 Сделка №{st['trades_today']} сегодня · Слоты: {slots_used(st)}/{MAX_CONCURRENT}\n"
        f"Команды: /pos · /stats")
    return True

def close_part(st, chat, pos, price, part, reason, real_close=True):
    """part: 0.5 или 1.0 от ТЕКУЩЕГО остатка. real_close=True -> реальный market close на Bybit."""
    qty_close = pos["qty"] * part
    if real_close:
        bybit_close_market(pos["sym"], qty_close)

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
    emoji = "💰" if pnl >= 0 else "🔻"
    tg_send(chat, f"{emoji} {pos['sym']}: {reason} по ${price:.6g}\nPnL части: {pnl:+.2f}$ ({r_mult:+.2f}R, комиссии учтены)")
    if pos["qty"] <= 1e-12 or part >= 0.999:
        st["positions"].pop(pos["sym"], None)
        tg_send(chat, f"📋 {pos['sym']} закрыта полностью. Слот освободился "
                      f"({slots_used(st)}/{MAX_CONCURRENT} занято). Сегодня сделок: {daily_txt(st)}.")
    save_state(st)

def manage_position(st, chat):
    """Управление КАЖДОЙ открытой позицией: проверка реальной позиции на Bybit + трейлинг."""
    for sym, pos in list(st.get("positions", {}).items()):
        price = bybit_price(sym)
        time.sleep(0.05)
        if price is None:
            continue

        live_pos = bybit_get_position(sym)
        if live_pos is None:
            # позиция на Bybit закрыта (сработал SL/TP биржей) — фиксируем как закрытую
            close_part(st, chat, pos, price, 1.0, "ЗАКРЫТА НА BYBIT (SL/TP сработал биржей)", real_close=False)
            continue

        if not pos["half_closed"]:
            if price >= pos["tp1"]:
                close_part(st, chat, pos, pos["tp1"], 0.5, "ТЕЙК-ПРОФИТ 1 (1:1)")
                if sym in st.get("positions", {}):
                    pos["half_closed"] = True
                    pos["peak"] = price
                    bybit_set_trading_stop(sym, trailing_stop=round(pos["entry"] * TRAIL_CALLBACK, 6))
                    save_state(st)
                    tg_send(chat, f"🔓 {sym}: трейлинг включён на Bybit (откат {TRAIL_CALLBACK*100:.1f}% от пика).")
            continue
        else:
            if price > pos["peak"]:
                pos["peak"] = price
                save_state(st)
            # трейлинг ведёт сама биржа через bybit_set_trading_stop; здесь просто следим за статусом

def check_pending(st, chat):
    """Проверка каждой лимитки: филл (реальный) / отмена по стопу / TTL."""
    for sym, p in list(st.get("pendings", {}).items()):
        try:
            filled = fill_pending(st, chat, sym)
            if filled:
                continue
        except Exception as e:
            print(f"fill_pending {sym} err:", e)

        try:
            o, h, l, c, v, tb, ct = klines15(sym, limit=3)
            time.sleep(0.08)
        except Exception:
            continue
        lo, cl = l[-2], c[-2]
        if sym not in st.get("pendings", {}):
            continue
        p = st["pendings"][sym]
        if cl < p["sl"]:
            cancel_pending(st, chat, sym, "закрытие ниже стопа до входа (структура сломана)")
            continue
        p["ttl"] -= 1
        if p["ttl"] <= 0:
            cancel_pending(st, chat, sym, f"ретеста не было за {ENTRY_TTL_BARS} свечей")
        else:
            save_state(st)

# ============================== СТАТИСТИКА ==============================
def pos_text(st):
    pens = st.get("pendings", {})
    poss = st.get("positions", {})
    margin_used = MARGIN * slots_used(st)
    head = (f"📊 Слоты: {slots_used(st)}/{MAX_CONCURRENT} · сделок сегодня {daily_txt(st)} · "
            f"маржа занята {margin_used:.0f}$/{DEPOSIT:.0f}$")
    if not pens and not poss:
        return head + "\nВсе слоты свободны — сканирую рынок."
    L = [head]
    for sym, pos in poss.items():
        pr = bybit_price(sym) or pos["entry"]
        upnl = (pr - pos["entry"]) * pos["qty"]
        stage = "трейлинг (стоп в плюсе)" if pos["half_closed"] else "жду TP1/SL"
        L.append(f"📌 {sym}: вход ${pos['entry']:.6g} → сейчас ${pr:.6g} ({upnl:+.2f}$) · {stage}")
    for sym, p in pens.items():
        L.append(f"⏳ {sym}: жду филла ${p['entry']:.6g} (осталось {p['ttl']} свечей)")
    return "\n".join(L)

def stats_text():
    if not os.path.exists(TRADES_FILE):
        return "📊 Сделок ещё нет."
    rows = list(csv.DictReader(open(TRADES_FILE)))
    if not rows:
        return "📊 Сделок ещё нет."
    n = len(rows)
    pnls = [float(r["pnl_usd"]) for r in rows]
    rs = [float(r["r_mult"]) for r in rows]
    wins = sum(1 for x in pnls if x > 0)
    total = sum(pnls)
    return ("📊 Статистика (реальные сделки, честно с комиссиями)\n"
            f"Закрытий: {n} · в плюсе: {wins} ({wins/n*100:.0f}%)\n"
            f"Средний R: {sum(rs)/n:+.2f} · Сумма PnL: {total:+.2f}$\n"
            f"Депозит {DEPOSIT:.0f}$ → {'✅' if total>=0 else '❌'} {total/DEPOSIT*100:+.1f}%")

# ============================== ОСНОВНОЙ ЦИКЛ ==============================
def scan_once(st, chat):
    ok_allowed, why = trading_allowed(st)
    if not ok_allowed:
        return
    busy = engaged_syms(st)
    for sym in universe():
        if sym in busy:
            continue
        try:
            o, h, l, c, v, tb, ct = klines15(sym, limit=LEVEL_LOOKBACK + 40)
            time.sleep(0.08)
        except Exception:
            continue
        if len(c) < LEVEL_LOOKBACK + 30:
            continue
        o, h, l, c, v, tb, ct = o[:-1], h[:-1], l[:-1], c[:-1], v[:-1], tb[:-1], ct[:-1]
        bar_id = ct[-1]
        if _last_bar_scanned.get(sym) == bar_id:
            continue
        _last_bar_scanned[sym] = bar_id
        oi = oi_hist(sym, limit=8)
        time.sleep(0.05)
        ok, d = detect_signal(o, h, l, c, v, tb, oi)
        if not ok:
            continue
        allowed, why = trading_allowed(st)
        if not allowed:
            return
        open_pending(st, sym, d, chat)
        busy.add(sym)
        if slots_used(st) >= MAX_CONCURRENT:
            return

def tg_loop(st):
    offset = 0
    while True:
        try:
            r = tg("getUpdates", _timeout=35, timeout=25, offset=offset)
            if not r or not r.get("ok"):
                time.sleep(2)
                continue
            for u in r["result"]:
                offset = u["update_id"] + 1
                msg = u.get("message") or {}
                text = (msg.get("text") or "").strip()
                cid = msg.get("chat", {}).get("id")
                if not cid:
                    continue
                save_chat(cid)
                if text.startswith("/help"):
                    tg_send(cid,
                        "📖 Команды EVA v3:\n"
                        "/start — запуск и краткая сводка\n"
                        "/pos — текущая позиция/лимитка: цена, PnL, стадия\n"
                        "/stats — статистика: win rate, средний R, PnL $ и %\n"
                        "/pause — пауза (новые сигналы не ищутся, позиция ведётся)\n"
                        "/resume — возобновить сканирование\n"
                        "/help — эта справка")
                elif text.startswith("/start"):
                    st["paused"] = False
                    save_state(st)
                    mode_txt = "🔴 LIVE (реальные деньги)" if LIVE_TRADING and not USE_DEMO_TRADING else "🟡 DEMO TRADING"
                    tg_send(cid, f"🤖 EVA v3 — импульсный бот [{mode_txt}]\n"
                        "Данные: Binance · Цены/Исполнение: Bybit v5 (реальные ордера)\n"
                        f"Лимиты: до {MAX_CONCURRENT} позиций одновременно · "
                        f"{'дневной предохранитель ' + str(MAX_DAILY_TRADES) if MAX_DAILY_TRADES>0 else 'без дневного лимита'} · "
                        f"Объём ${NOTIONAL:.0f} (маржа {MARGIN:.0f}$ x{LEVERAGE:.0f})\n"
                        "Команды: /pos · /stats · /pause · /resume · /help")
                elif text.startswith("/pause"):
                    st["paused"] = True
                    save_state(st)
                    tg_send(cid, "⏸ Пауза: новые сигналы не ищу (открытая позиция ведётся).")
                elif text.startswith("/resume"):
                    st["paused"] = False
                    save_state(st)
                    tg_send(cid, "▶️ Сканирование возобновлено.")
                elif text.startswith("/pos"):
                    tg_send(cid, pos_text(st))
                elif text.startswith("/stats"):
                    tg_send(cid, stats_text())
        except Exception as e:
            print("tg_loop err:", e)
            time.sleep(3)

def main():
    st = load_state()
    chat = load_chat()
    mode_txt = "LIVE" if LIVE_TRADING and not USE_DEMO_TRADING else "DEMO"
    print(f"EVA v3 запущен ({mode_txt}). chat:", "есть" if chat else "нет")
    if not BYBIT_API_KEY or not BYBIT_API_SECRET:
        print("WARNING: BYBIT_API_KEY/SECRET не заданы — реальное исполнение не будет работать!")
    threading.Thread(target=tg_loop, args=(st,), daemon=True).start()
    last_scan = last_manage = 0
    while True:
        try:
            chat = load_chat()
            roll_day(st, chat)
            now = time.time()
            if now - last_manage >= MANAGE_EVERY_SEC:
                last_manage = now
                check_pending(st, chat)
                manage_position(st, chat)
            if now - last_scan >= SCAN_EVERY_SEC:
                last_scan = now
                scan_once(st, chat)
        except Exception as e:
            print("main err:", e)
        time.sleep(2)

if __name__ == "__main__":
    main()
