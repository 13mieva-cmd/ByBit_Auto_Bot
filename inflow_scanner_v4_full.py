#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EVA v6 — BETA MOMENTUM STRATEGY ("Следование за BTC") — полная замена SuperTrend+ADX v5
=========================================================================================
ДАННЫЕ: Binance Futures (klines) для BTC и альткоинов, корреляция считается на дневных барах
ЦЕНЫ/ТОРГОВЛЯ: Bybit (сверка цены + исполнение)
ИСПОЛНЕНИЕ: PAPER по умолчанию (AUTO_TRADE=0), реальные ордера Bybit demo/live при AUTO_TRADE=1

СТРАТЕГИЯ (по спеке пользователя — "Beta Momentum Strategy"):
Бот отслеживает сильные движения BTC и открывает КОРЗИНУ позиций в самых коррелирующих
и волатильных альткоинах в ТОМ ЖЕ направлении (альты обычно бета > 1, двигаются сильнее BTC).

РЕЖИМЫ (переключаются через ENV MODE=conservative|aggressive):
  conservative (по умолчанию):
    TF=1h, BTC_THRESHOLD=1.2% за 60 мин, MIN_CORR=0.85 (30 дн), монет в корзине 3-5,
    LEVERAGE=6x, риск на корзину 0.75-1% депо, SL корзины 1.8-2.2%,
    TP: 40% при +2.5%, 30% при +4%, остаток trailing (после +1.8%, шаг 0.8%),
    фильтры ADX(BTC,14)>23, RSI(BTC) не в экстремуме, глобальный СТОП корзины -3%.
  aggressive:
    TF=15m, BTC_THRESHOLD=0.8% за 30-45 мин, MIN_CORR=0.78, монет в корзине 6-10,
    LEVERAGE=10x, риск на корзину 1.5-2% депо, SL 1.4-1.8%,
    TP: 50% при +2%, остаток trailing (после +1.2%), ADX(BTC)>20, глобальный СТОП -4.5%.

Максимальное время удержания позиции: 12-24ч (закрытие по таймауту, чтобы не висеть в развороте).
Список альткоинов-кандидатов — фиксированный шорт-лист высокой ликвидности (по спеке), из
которого бот динамически выбирает 3-10 самых коррелирующих с BTC на данный момент.

ДЕПЛОЙ: Railway, переменные окружения — см. блок КОНФИГ ниже.
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

MODE = os.environ.get("MODE", "conservative").lower()  # "conservative" | "aggressive"
IS_CONSERVATIVE = MODE != "aggressive"

# --- параметры режима (по спеке, дефолты — середина диапазона) ---
if IS_CONSERVATIVE:
    TF = os.environ.get("TF", "1h")
    BTC_LOOKBACK_MIN = int(os.environ.get("BTC_LOOKBACK_MIN", "60"))
    BTC_THRESHOLD_PCT = float(os.environ.get("BTC_THRESHOLD_PCT", "1.2"))
    MIN_CORR = float(os.environ.get("MIN_CORR", "0.85"))
    BASKET_SIZE = int(os.environ.get("BASKET_SIZE", "4"))          # 3-5
    LEVERAGE = float(os.environ.get("LEVERAGE", "6"))               # 5-7x
    BASKET_RISK_PCT = float(os.environ.get("BASKET_RISK_PCT", "0.9"))   # 0.75-1%
    SL_PCT = float(os.environ.get("SL_PCT", "2.0"))                 # 1.8-2.2%
    TP1_PCT, TP1_PART = float(os.environ.get("TP1_PCT", "2.5")), float(os.environ.get("TP1_PART", "0.40"))
    TP2_PCT, TP2_PART = float(os.environ.get("TP2_PCT", "4.0")), float(os.environ.get("TP2_PART", "0.30"))
    TRAIL_ACTIVATE_PCT = float(os.environ.get("TRAIL_ACTIVATE_PCT", "1.8"))
    TRAIL_STEP_PCT = float(os.environ.get("TRAIL_STEP_PCT", "0.8"))
    ADX_MIN = float(os.environ.get("ADX_MIN", "23"))
    BASKET_GLOBAL_SL_PCT = float(os.environ.get("BASKET_GLOBAL_SL_PCT", "3.0"))
else:
    TF = os.environ.get("TF", "15m")
    BTC_LOOKBACK_MIN = int(os.environ.get("BTC_LOOKBACK_MIN", "40"))
    BTC_THRESHOLD_PCT = float(os.environ.get("BTC_THRESHOLD_PCT", "0.8"))
    MIN_CORR = float(os.environ.get("MIN_CORR", "0.78"))
    BASKET_SIZE = int(os.environ.get("BASKET_SIZE", "8"))          # 6-10
    LEVERAGE = float(os.environ.get("LEVERAGE", "10"))               # 8-12x
    BASKET_RISK_PCT = float(os.environ.get("BASKET_RISK_PCT", "1.75"))  # 1.5-2%
    SL_PCT = float(os.environ.get("SL_PCT", "1.6"))                  # 1.4-1.8%
    TP1_PCT, TP1_PART = float(os.environ.get("TP1_PCT", "2.0")), float(os.environ.get("TP1_PART", "0.50"))
    TP2_PCT, TP2_PART = 0.0, 0.0   # спека агрессивного режима: только один TP на 50%, остаток trailing
    TRAIL_ACTIVATE_PCT = float(os.environ.get("TRAIL_ACTIVATE_PCT", "1.2"))
    TRAIL_STEP_PCT = float(os.environ.get("TRAIL_STEP_PCT", "0.8"))
    ADX_MIN = float(os.environ.get("ADX_MIN", "20"))
    BASKET_GLOBAL_SL_PCT = float(os.environ.get("BASKET_GLOBAL_SL_PCT", "4.5"))

ADX_LEN = 14
RSI_LEN = 14
RSI_EXTREME_LOW = float(os.environ.get("RSI_EXTREME_LOW", "20"))
RSI_EXTREME_HIGH = float(os.environ.get("RSI_EXTREME_HIGH", "80"))
CORR_LOOKBACK_DAYS = int(os.environ.get("CORR_LOOKBACK_DAYS", "30"))
MAX_HOLD_HOURS = float(os.environ.get("MAX_HOLD_HOURS", "18" if IS_CONSERVATIVE else "14"))  # 12-24ч по спеке
MAX_CONCURRENT_BASKETS = int(os.environ.get("MAX_CONCURRENT_BASKETS", "1"))  # одна активная корзина за раз
SCAN_EVERY_SEC = int(os.environ.get("SCAN_EVERY_SEC", "300"))  # спека псевдокода: sleep(300)
MANAGE_EVERY_SEC = 10
FEE_MAKER = 0.0002
FEE_TAKER = 0.00055

# --- фиксированный шорт-лист альткоинов-кандидатов (спека) ---
CANDIDATE_ALTS = ["ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "AVAXUSDT", "SUIUSDT",
                  "TONUSDT", "NEARUSDT", "APTUSDT", "HBARUSDT"]
CANDIDATE_ALTS_AGGRESSIVE_EXTRA = ["WIFUSDT", "POPCATUSDT", "PNUTUSDT"]  # только в агрессивном режиме
if not IS_CONSERVATIVE:
    CANDIDATE_ALTS = CANDIDATE_ALTS + CANDIDATE_ALTS_AGGRESSIVE_EXTRA

def ensure_dirs():
    try: os.makedirs(DATA_DIR, exist_ok=True)
    except Exception: pass
ensure_dirs()
STATE_FILE = os.path.join(DATA_DIR, "v6_state.json")
TRADES_FILE = os.path.join(DATA_DIR, "v6_trades.csv")
SIGNALS_FILE = os.path.join(DATA_DIR, "v6_signals.csv")
CHAT_FILE = os.path.join(DATA_DIR, "v6_chat.txt")

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
    req = urllib.request.Request(url, headers={"User-Agent": "eva-v6"})
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
_RATE_SEM = asyncio.Semaphore(20)

async def _fetch_json_async(session, url):
    async with _RATE_SEM:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                return await r.json()
        except Exception as e:
            print("async fetch err:", url, e)
            return None

async def fetch_klines_batch(symbols, tf, need_bars):
    async with aiohttp.ClientSession() as session:
        async def one(sym):
            k_url = f"{BINANCE}/fapi/v1/klines?symbol={sym}&interval={tf}&limit={need_bars}"
            k_data = await _fetch_json_async(session, k_url)
            if not k_data or len(k_data) < need_bars - 5:
                return sym, None
            o = [float(x[1]) for x in k_data]; h = [float(x[2]) for x in k_data]
            l = [float(x[3]) for x in k_data]; c = [float(x[4]) for x in k_data]
            v = [float(x[5]) for x in k_data]; ct = [int(x[0]) for x in k_data]
            return sym, (o, h, l, c, v, ct)
        results = await asyncio.gather(*[one(s) for s in symbols], return_exceptions=False)
    return {sym: data for sym, data in results if data is not None}

def klines_sync(symbol, tf, limit):
    d = http_json(f"{BINANCE}/fapi/v1/klines?symbol={symbol}&interval={tf}&limit={limit}")
    o = [float(x[1]) for x in d]; h = [float(x[2]) for x in d]
    l = [float(x[3]) for x in d]; c = [float(x[4]) for x in d]
    v = [float(x[5]) for x in d]; ct = [int(x[0]) for x in d]
    return o, h, l, c, v, ct

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
    qty_r = bybit_round_qty(symbol, qty)
    return _bybit_signed("POST", "/v5/order/create", body={
        "category": CATEGORY, "symbol": symbol, "side": "Sell",
        "orderType": "Market", "qty": str(qty_r), "timeInForce": "IOC",
    })

def bybit_close_market(symbol, qty, side="long"):
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

def adx_series(h, l, c, period=14):
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

def pct_change_over(closes, bars_back):
    if len(closes) < bars_back + 1: return 0.0
    a, b = closes[-1 - bars_back], closes[-1]
    if a == 0: return 0.0
    return (b - a) / a * 100.0

def pearson_corr(x, y):
    n = min(len(x), len(y))
    if n < 5: return 0.0
    x = x[-n:]; y = y[-n:]
    mx = sum(x) / n; my = sum(y) / n
    sx = sum((xi - mx) ** 2 for xi in x) ** 0.5
    sy = sum((yi - my) ** 2 for yi in y) ** 0.5
    if sx == 0 or sy == 0: return 0.0
    cov = sum((x[i] - mx) * (y[i] - my) for i in range(n))
    return cov / (sx * sy)

def daily_returns(closes):
    return [(closes[i] - closes[i - 1]) / closes[i - 1] for i in range(1, len(closes)) if closes[i - 1] != 0]

# ============================== КОРРЕЛЯЦИЯ И ВЫБОР МОНЕТ ==============================
_corr_cache = {"ts": 0, "data": {}}

def compute_correlations():
    """Дневные свечи BTC + кандидатов за CORR_LOOKBACK_DAYS, корреляция returns с BTC.
    Кэш на 1 час — пересчёт корреляции каждый цикл избыточен (спека: 30-дн окно почти не меняется внутри дня)."""
    if time.time() - _corr_cache["ts"] < 3600 and _corr_cache["data"]:
        return _corr_cache["data"]
    need = CORR_LOOKBACK_DAYS + 5
    try:
        _, _, _, btc_c, _, _ = klines_sync("BTCUSDT", "1d", need)
    except Exception as e:
        print("corr btc fetch err:", e)
        return _corr_cache["data"]
    btc_ret = daily_returns(btc_c)
    result = {}
    for sym in CANDIDATE_ALTS:
        try:
            _, _, _, c, v, _ = klines_sync(sym, "1d", need)
            ret = daily_returns(c)
            corr = pearson_corr(btc_ret, ret)
            avg_vol24 = sum(v[-7:]) / 7 if len(v) >= 7 else sum(v) / max(len(v), 1)
            beta = 0.0
            btc_var = sum(r * r for r in btc_ret) / len(btc_ret) if btc_ret else 0.0
            if btc_var > 0 and len(ret) >= len(btc_ret):
                n = min(len(ret), len(btc_ret))
                cov = sum(ret[-n:][i] * btc_ret[-n:][i] for i in range(n)) / n
                beta = cov / btc_var
            result[sym] = dict(corr=corr, beta=beta, vol24=avg_vol24)
        except Exception as e:
            print(f"corr {sym} err:", e)
        time.sleep(0.1)
    _corr_cache["data"] = result; _corr_cache["ts"] = time.time()
    return result

def get_top_correlated_coins(num, min_correlation):
    corrs = compute_correlations()
    cands = [(s, d["corr"], d["beta"], d["vol24"]) for s, d in corrs.items() if d["corr"] >= min_correlation]
    cands.sort(key=lambda x: (-x[1], -x[2]))
    return [c[0] for c in cands[:num]]

# ============================== СИГНАЛ: BTC MOMENTUM ==============================
def get_btc_signal():
    """Возвращает (ok, direction|None, details). Проверяет движение BTC, ADX, RSI (без экстремума)."""
    need = max(BTC_LOOKBACK_MIN // {"15m": 15, "1h": 60}.get(TF, 60) + 5, ADX_LEN * 3 + 5, RSI_LEN * 6 + 5)
    try:
        o, h, l, c, v, ct = klines_sync("BTCUSDT", TF, need + 10)
    except Exception as e:
        return False, None, f"btc fetch err: {e}"
    o, h, l, c, v, ct = o[:-1], h[:-1], l[:-1], c[:-1], v[:-1], ct[:-1]
    bar_minutes = {"15m": 15, "1h": 60}.get(TF, 60)
    bars_back = max(1, round(BTC_LOOKBACK_MIN / bar_minutes))
    chg = pct_change_over(c, bars_back)
    if abs(chg) < BTC_THRESHOLD_PCT:
        return False, None, f"BTC движение {chg:+.2f}% < порога {BTC_THRESHOLD_PCT}%"
    direction = "long" if chg > 0 else "short"

    adx_vals = adx_series(h, l, c, ADX_LEN)
    adx_now = adx_vals[-1] if adx_vals else 0.0
    if adx_now <= ADX_MIN:
        return False, None, f"ADX(BTC) {adx_now:.0f} <= {ADX_MIN} (слабый тренд)"

    r = rsi(c[-(RSI_LEN * 6):], RSI_LEN)
    if r <= RSI_EXTREME_LOW or r >= RSI_EXTREME_HIGH:
        return False, None, f"RSI(BTC) {r:.0f} в экстремальной зоне"

    return True, direction, dict(chg=chg, adx=adx_now, rsi=r, price=c[-1])

# ============================== СОСТОЯНИЕ/ЛИМИТЫ ==============================
def _default_state():
    return dict(day=str(dt.datetime.now(dt.timezone.utc).date()),
                trades_today=0, paused=False, day_start_equity=DEPOSIT,
                baskets={})

def load_state():
    try:
        with open(STATE_FILE) as f: st = json.load(f)
    except Exception: return _default_state()
    if "baskets" not in st: st["baskets"] = {}
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
        if chat: tg_send(chat, f"\U0001F305 Новый день (UTC) — счётчик сделок обнулён.")

def active_baskets(st):
    return {k: v for k, v in st.get("baskets", {}).items() if v.get("status") == "open"}

def trading_allowed(st):
    if st.get("paused"): return False, "пауза"
    if len(active_baskets(st)) >= MAX_CONCURRENT_BASKETS:
        return False, f"активна корзина ({MAX_CONCURRENT_BASKETS} макс)"
    return True, ""

# ============================== ЖУРНАЛЫ ==============================
def log_signal(direction, btc_chg, coins):
    try:
        new = not os.path.exists(SIGNALS_FILE)
        with open(SIGNALS_FILE, "a", newline="") as f:
            w = csv.writer(f)
            if new: w.writerow(["ts", "direction", "btc_chg", "coins"])
            w.writerow([dt.datetime.now().isoformat(timespec="seconds"), direction, btc_chg, ",".join(coins)])
    except Exception as e: print("log_signal err:", e)

def log_trade(row):
    try:
        new = not os.path.exists(TRADES_FILE)
        with open(TRADES_FILE, "a", newline="") as f:
            w = csv.writer(f)
            if new: w.writerow(["ts_open", "ts_close", "basket_id", "coin", "side", "entry", "exit",
                                 "qty", "part", "pnl_usd", "reason"])
            w.writerow(row)
    except Exception as e: print("log_trade err:", e)

# ============================== PAPER/LIVE ДВИЖОК (КОРЗИНА) ==============================
def open_basket(st, chat, direction, coins, btc_info):
    """Открывает корзину позиций по всем coins в направлении direction (long/short).
    Риск на ВСЮ корзину = BASKET_RISK_PCT% депозита, делится равными частями между монетами."""
    basket_id = str(int(time.time()))
    basket_risk_usd = DEPOSIT * (BASKET_RISK_PCT / 100.0)
    per_coin_risk_usd = basket_risk_usd / max(len(coins), 1)
    margin_per_coin = MARGIN  # позиция на монету — как обычно, ограничена MARGIN*LEVERAGE
    notional_per_coin = margin_per_coin * LEVERAGE

    positions = {}
    opened_syms = []
    for sym in coins:
        price = bybit_price(sym)
        if price is None:
            continue
        qty = notional_per_coin / price
        sl_dist = price * (SL_PCT / 100.0)
        sl = price - sl_dist if direction == "long" else price + sl_dist
        tp1 = price + price * (TP1_PCT / 100.0) if direction == "long" else price - price * (TP1_PCT / 100.0)
        tp2 = None
        if TP2_PCT > 0:
            tp2 = price + price * (TP2_PCT / 100.0) if direction == "long" else price - price * (TP2_PCT / 100.0)

        live_note = "PAPER"
        if AUTO_TRADE:
            live_note = "LIVE" + (" DEMO" if BYBIT_USE_DEMO else " РЕАЛ")
            bybit_set_leverage(sym, LEVERAGE)
            r_order = bybit_market_long(sym, qty) if direction == "long" else bybit_market_short(sym, qty)
            if r_order.get("retCode") != 0:
                tg_send(chat, f"\u26A0\uFE0F {sym}: ошибка входа в корзину: {r_order.get('retMsg')}")
                continue
            by = bybit_price(sym)
            if by: price = by

        fee_in = qty * price * FEE_MAKER
        positions[sym] = dict(sym=sym, side=direction, entry=price, sl=sl, tp1=tp1, tp2=tp2,
                               qty=qty, qty_init=qty, fee_in=fee_in,
                               tp1_done=False, tp2_done=(tp2 is None), trail_active=False,
                               peak=price, opened=dt.datetime.now().isoformat(timespec="seconds"))
        opened_syms.append(sym)

    if not positions:
        return

    st["baskets"][basket_id] = dict(
        id=basket_id, direction=direction, status="open",
        opened_ts=time.time(), coins=positions,
        equity_start=sum(p["qty"] * p["entry"] / LEVERAGE for p in positions.values()),
        btc_chg=btc_info.get("chg", 0.0),
    )
    st["trades_today"] = st.get("trades_today", 0) + len(positions)
    save_state(st)
    log_signal(direction, btc_info.get("chg", 0.0), opened_syms)

    dir_ru = "LONG (BTC растёт)" if direction == "long" else "SHORT (BTC падает)"
    emoji = "\U0001F4C8" if direction == "long" else "\U0001F4C9"
    L = [
        f"{emoji} НОВАЯ КОРЗИНА \u2014 {dir_ru}",
        f"\U0001F9E0 Триггер: BTC {btc_info.get('chg', 0):+.2f}% за {BTC_LOOKBACK_MIN} мин \u00b7 "
        f"ADX(BTC) {btc_info.get('adx', 0):.0f} \u00b7 RSI(BTC) {btc_info.get('rsi', 0):.0f}",
        f"\U0001F4CB Монеты ({len(opened_syms)}): {', '.join(s.replace('USDT','') for s in opened_syms)}",
        f"\U0001F4B0 Риск корзины: {BASKET_RISK_PCT}% депо (${basket_risk_usd:.2f}) \u00b7 плечо {LEVERAGE:.0f}x",
        f"\U0001F6D1 SL по каждой монете: {SL_PCT}% от входа \u00b7 глобальный стоп корзины \u2212{BASKET_GLOBAL_SL_PCT}%",
        f"\U0001F3AF TP1 {TP1_PCT}% \u2192 {TP1_PART*100:.0f}%" + (f" \u00b7 TP2 {TP2_PCT}% \u2192 {TP2_PART*100:.0f}%" if TP2_PCT > 0 else "") +
        f" \u00b7 остаток trailing (после +{TRAIL_ACTIVATE_PCT}%, шаг {TRAIL_STEP_PCT}%)",
        f"\u23F1 Макс. время удержания: {MAX_HOLD_HOURS:.0f}ч",
        f"Режим: {'консервативный' if IS_CONSERVATIVE else 'агрессивный'} \u00b7 Команды: /status \u00b7 /close_all \u00b7 /pause",
    ]
    tg_send(chat, "\n".join(L))


def close_coin_position(st, chat, basket, sym, price, part, reason):
    pos = basket["coins"][sym]
    side = pos["side"]
    sign = 1 if side == "long" else -1
    qty_close = pos["qty"] * part
    if AUTO_TRADE and qty_close > 0:
        r = bybit_close_market(sym, qty_close, side=side)
        if r.get("retCode") != 0:
            tg_send(chat, f"\u26A0\uFE0F {sym}: ошибка закрытия части корзины: {r.get('retMsg')}")
        if part >= 0.999:
            bybit_cancel_all(sym)
    gross = sign * (price - pos["entry"]) * qty_close
    fee_exit = price * qty_close * FEE_TAKER
    fee_in_share = pos["fee_in"] * (qty_close / pos["qty_init"])
    pnl = gross - fee_exit - fee_in_share
    pos["qty"] -= qty_close
    log_trade([pos["opened"], dt.datetime.now().isoformat(timespec="seconds"),
               basket["id"], sym, side, f"{pos['entry']:.8g}", f"{price:.8g}",
               f"{qty_close:.8g}", f"{part:.2f}", f"{pnl:.2f}", reason])
    return pnl


def manage_baskets(st, chat):
    for bid, basket in list(st.get("baskets", {}).items()):
        if basket.get("status") != "open": continue
        direction = basket["direction"]
        sign = 1 if direction == "long" else -1
        hold_hours = (time.time() - basket["opened_ts"]) / 3600.0

        total_pnl = 0.0; total_margin = 0.0
        for sym, pos in list(basket["coins"].items()):
            if pos["qty"] <= 1e-12: continue
            price = bybit_price(sym); time.sleep(0.03)
            if price is None: continue
            total_margin += pos["qty_init"] * pos["entry"] / LEVERAGE
            unrealized = sign * (price - pos["entry"]) * pos["qty"]
            total_pnl += unrealized

        basket_pnl_pct = (total_pnl / total_margin * 100.0) if total_margin > 0 else 0.0

        force_close_all = False; close_reason = ""
        if basket_pnl_pct <= -BASKET_GLOBAL_SL_PCT:
            force_close_all = True; close_reason = f"ГЛОБАЛЬНЫЙ СТОП КОРЗИНЫ ({basket_pnl_pct:+.1f}%)"
        elif hold_hours >= MAX_HOLD_HOURS:
            force_close_all = True; close_reason = f"ТАЙМ-АУТ ({hold_hours:.1f}ч >= {MAX_HOLD_HOURS:.0f}ч)"

        if force_close_all:
            realized = 0.0
            for sym, pos in list(basket["coins"].items()):
                if pos["qty"] <= 1e-12: continue
                price = bybit_price(sym) or pos["entry"]
                realized += close_coin_position(st, chat, basket, sym, price, 1.0, close_reason)
            basket["status"] = "closed"; basket["closed_ts"] = time.time()
            save_state(st)
            emoji = "\U0001F4B0" if realized >= 0 else "\U0001F53B"
            tg_send(chat, f"{emoji} Корзина закрыта целиком: {close_reason}\nPnL корзины: {realized:+.2f}$")
            continue

        any_alive = False
        for sym, pos in list(basket["coins"].items()):
            if pos["qty"] <= 1e-12: continue
            price = bybit_price(sym); time.sleep(0.03)
            if price is None: continue
            any_alive = True
            move_pct = sign * (price - pos["entry"]) / pos["entry"] * 100.0

            hit_sl = price <= pos["sl"] if direction == "long" else price >= pos["sl"]
            if hit_sl:
                pnl = close_coin_position(st, chat, basket, sym, pos["sl"], 1.0, "СТОП-ЛОСС")
                tg_send(chat, f"\U0001F53B {sym}: стоп по корзине ${pos['sl']:.6g} \u00b7 PnL {pnl:+.2f}$")
                save_state(st); continue

            if not pos["tp1_done"]:
                hit_tp1 = price >= pos["tp1"] if direction == "long" else price <= pos["tp1"]
                if hit_tp1:
                    pnl = close_coin_position(st, chat, basket, sym, pos["tp1"], TP1_PART, f"TP1 +{TP1_PCT}%")
                    pos["tp1_done"] = True
                    tg_send(chat, f"\U0001F3AF {sym}: TP1 +{TP1_PCT}% \u2192 закрыл {TP1_PART*100:.0f}% \u00b7 PnL {pnl:+.2f}$")
                    save_state(st)

            if pos["tp1_done"] and not pos["tp2_done"] and TP2_PCT > 0:
                hit_tp2 = price >= pos["tp2"] if direction == "long" else price <= pos["tp2"]
                if hit_tp2:
                    pnl = close_coin_position(st, chat, basket, sym, pos["tp2"], TP2_PART, f"TP2 +{TP2_PCT}%")
                    pos["tp2_done"] = True
                    tg_send(chat, f"\U0001F3AF {sym}: TP2 +{TP2_PCT}% \u2192 закрыл {TP2_PART*100:.0f}% \u00b7 PnL {pnl:+.2f}$")
                    save_state(st)

            if pos["tp2_done"] and pos["qty"] > 1e-12:
                if not pos["trail_active"] and move_pct >= TRAIL_ACTIVATE_PCT:
                    pos["trail_active"] = True
                    pos["peak"] = price
                    save_state(st)
                if pos["trail_active"]:
                    if direction == "long":
                        pos["peak"] = max(pos["peak"], price)
                        trail_stop = pos["peak"] * (1 - TRAIL_STEP_PCT / 100.0)
                        if price <= trail_stop:
                            pnl = close_coin_position(st, chat, basket, sym, trail_stop, 1.0, "ТРЕЙЛИНГ-СТОП")
                            tg_send(chat, f"\U0001F512 {sym}: трейлинг-стоп ${trail_stop:.6g} \u00b7 PnL {pnl:+.2f}$")
                            save_state(st)
                    else:
                        pos["peak"] = min(pos["peak"], price) if pos["peak"] else price
                        trail_stop = pos["peak"] * (1 + TRAIL_STEP_PCT / 100.0)
                        if price >= trail_stop:
                            pnl = close_coin_position(st, chat, basket, sym, trail_stop, 1.0, "ТРЕЙЛИНГ-СТОП")
                            tg_send(chat, f"\U0001F512 {sym}: трейлинг-стоп ${trail_stop:.6g} \u00b7 PnL {pnl:+.2f}$")
                            save_state(st)

        remaining_qty = sum(p["qty"] for p in basket["coins"].values())
        if remaining_qty <= 1e-9:
            basket["status"] = "closed"; basket["closed_ts"] = time.time(); save_state(st)
            tg_send(chat, f"\U0001F4CB Корзина {bid} полностью закрыта (все монеты вышли по TP/трейлингу).")

def close_all_baskets(st, chat):
    closed_any = False
    for bid, basket in list(st.get("baskets", {}).items()):
        if basket.get("status") != "open": continue
        realized = 0.0
        for sym, pos in list(basket["coins"].items()):
            if pos["qty"] <= 1e-12: continue
            price = bybit_price(sym) or pos["entry"]
            realized += close_coin_position(st, chat, basket, sym, price, 1.0, "РУЧНОЕ ЗАКРЫТИЕ /close_all")
        basket["status"] = "closed"; basket["closed_ts"] = time.time()
        closed_any = True
        tg_send(chat, f"\U0001F6D1 Корзина {bid} закрыта вручную. PnL: {realized:+.2f}$")
    save_state(st)
    if not closed_any:
        tg_send(chat, "Нет активных корзин.")

# ============================== СТАТИСТИКА ==============================
def status_text(st):
    baskets = active_baskets(st)
    head = f"\U0001F4CA Режим: {'консервативный' if IS_CONSERVATIVE else 'агрессивный'} \u00b7 активных корзин: {len(baskets)}/{MAX_CONCURRENT_BASKETS}"
    if not baskets:
        return head + "\nЖду сильного движения BTC для входа."
    L = [head]
    for bid, b in baskets.items():
        hold_h = (time.time() - b["opened_ts"]) / 3600.0
        L.append(f"\n\U0001F9FA Корзина {bid} ({b['direction']}) \u00b7 держим {hold_h:.1f}ч из {MAX_HOLD_HOURS:.0f}ч:")
        for sym, pos in b["coins"].items():
            if pos["qty"] <= 1e-12: continue
            pr = bybit_price(sym) or pos["entry"]
            sign = 1 if pos["side"] == "long" else -1
            upnl = sign * (pr - pos["entry"]) * pos["qty"]
            stage = "trailing" if pos["trail_active"] else ("после TP1" if pos["tp1_done"] else "жду TP1/SL")
            L.append(f"  \U0001F4CC {sym}: ${pos['entry']:.6g}\u2192${pr:.6g} ({upnl:+.2f}$) \u00b7 {stage}")
    return "\n".join(L)

def stats_text():
    if not os.path.exists(TRADES_FILE):
        return "\U0001F4CA Сделок ещё нет."
    rows = list(csv.DictReader(open(TRADES_FILE)))
    if not rows: return "\U0001F4CA Сделок ещё нет."
    n = len(rows)
    pnls = [float(r["pnl_usd"]) for r in rows]
    wins = sum(1 for x in pnls if x > 0)
    total = sum(pnls)
    return ("\U0001F4CA СТАТИСТИКА (Beta Momentum, с комиссиями)\n"
            f"Закрытий позиций: {n} \u00b7 в плюсе: {wins} ({wins/n*100:.0f}%)\n"
            f"Сумма PnL: {total:+.2f}$\n"
            f"Депозит {DEPOSIT:.0f}$ \u2192 {'\u2705' if total>=0 else '\u274C'} {total/DEPOSIT*100:+.1f}%")

# ============================== ОСНОВНОЙ ЦИКЛ ==============================
BT_RUNNING = {"on": False}
_last_btc_signal_ts = {"ts": 0}

def scan_once(st, chat):
    if BT_RUNNING["on"]: return
    ok_allowed, why = trading_allowed(st)
    if not ok_allowed: return

    ok, direction, info = get_btc_signal()
    if not ok:
        return
    if isinstance(info, str):
        return

    coins = get_top_correlated_coins(BASKET_SIZE, MIN_CORR)
    if not coins:
        return
    open_basket(st, chat, direction, coins, info)

# ============================== БЭКТЕСТЕР (упрощённый, корзина, long+short) ==============================
def bt_klines(symbol, tf, days):
    need_bars_per_day = {"15m": 96, "1h": 24}.get(tf, 24)
    need = int(days * need_bars_per_day) + 60
    out = []; end = None
    while len(out) < need:
        url = f"{BINANCE}/fapi/v1/klines?symbol={symbol}&interval={tf}&limit=1500"
        if end: url += f"&endTime={end}"
        d = http_json(url); time.sleep(0.12)
        if not d: break
        out = d + out
        end = int(d[0][0]) - 1
        if len(d) < 1500: break
    out = out[-need:]
    o = [float(x[1]) for x in out]; h = [float(x[2]) for x in out]
    l = [float(x[3]) for x in out]; c = [float(x[4]) for x in out]
    v = [float(x[5]) for x in out]; ct = [int(x[6]) for x in out]
    return o, h, l, c, v, ct

def run_backtest(chat, days=14, ncoins=None):
    if BT_RUNNING["on"]:
        tg_send(chat, "\u23F3 Бэктест уже идёт."); return
    BT_RUNNING["on"] = True
    try:
        days = max(3, min(days, 30))
        ncoins = ncoins or BASKET_SIZE
        tg_send(chat, f"\U0001F9EA Бэктест Beta Momentum ({MODE}): {days} дн, корзина до {ncoins} монет.")
        try:
            corrs = compute_correlations()
        except Exception as e:
            tg_send(chat, f"\u274C Ошибка расчёта корреляций: {e}"); return
        coins_all = [s for s, d in sorted(corrs.items(), key=lambda x: -x[1]["corr"]) if d["corr"] >= MIN_CORR][:ncoins]
        if not coins_all:
            tg_send(chat, "\u274C Нет монет с достаточной корреляцией к BTC."); return

        try:
            bo, bh, bl, bc, bv, bct = bt_klines("BTCUSDT", TF, days)
        except Exception as e:
            tg_send(chat, f"\u274C Ошибка загрузки BTC: {e}"); return

        alt_data = {}
        for sym in coins_all:
            try:
                alt_data[sym] = bt_klines(sym, TF, days)
            except Exception:
                pass

        bar_minutes = {"15m": 15, "1h": 60}.get(TF, 60)
        bars_back = max(1, round(BTC_LOOKBACK_MIN / bar_minutes))
        adx_vals = adx_series(bh, bl, bc, ADX_LEN)
        W = max(ADX_LEN * 3, RSI_LEN * 6, bars_back) + 10

        all_trades = []; basket_open = None
        n = len(bc)
        for i in range(W, n):
            if basket_open:
                hold_h = (bct[i] - basket_open["open_ts"]) / 1000.0 / 3600.0
                direction = basket_open["direction"]; sign = 1 if direction == "long" else -1
                total_pnl = 0.0; total_margin = 0.0; all_closed = True
                for sym, pos in basket_open["coins"].items():
                    if pos["qty"] <= 1e-12: continue
                    all_closed = False
                    ad = alt_data.get(sym)
                    if not ad: continue
                    idx = pos["idx_map"].get(i)
                    if idx is None: continue
                    bar_h, bar_l, bar_c = ad[1][idx], ad[2][idx], ad[3][idx]
                    total_margin += pos["qty_init"] * pos["entry"] / LEVERAGE
                    if not pos["tp1_done"]:
                        hit_sl = bar_l <= pos["sl"] if direction == "long" else bar_h >= pos["sl"]
                        hit_tp1 = bar_h >= pos["tp1"] if direction == "long" else bar_l <= pos["tp1"]
                        if hit_sl:
                            pnl = sign * (pos["sl"] - pos["entry"]) * pos["qty"] - pos["sl"] * pos["qty"] * FEE_TAKER - pos["fee_in"]
                            pos["qty"] = 0; all_trades.append(pnl); continue
                        if hit_tp1:
                            part_qty = pos["qty"] * TP1_PART
                            pnl = sign * (pos["tp1"] - pos["entry"]) * part_qty - pos["tp1"] * part_qty * FEE_TAKER - pos["fee_in"] * TP1_PART
                            pos["qty"] -= part_qty; all_trades.append(pnl); pos["tp1_done"] = True
                            if TP2_PCT <= 0: pos["tp2_done"] = True
                    if pos["tp1_done"] and not pos.get("tp2_done") and TP2_PCT > 0:
                        hit_tp2 = bar_h >= pos["tp2"] if direction == "long" else bar_l <= pos["tp2"]
                        if hit_tp2:
                            part_qty = pos["qty"] * (TP2_PART / (1 - TP1_PART))
                            part_qty = min(part_qty, pos["qty"])
                            pnl = sign * (pos["tp2"] - pos["entry"]) * part_qty - pos["tp2"] * part_qty * FEE_TAKER - pos["fee_in"] * TP2_PART
                            pos["qty"] -= part_qty; all_trades.append(pnl); pos["tp2_done"] = True
                    if pos.get("tp2_done", TP2_PCT <= 0 and pos["tp1_done"]) and pos["qty"] > 1e-12:
                        move_pct = sign * (bar_c - pos["entry"]) / pos["entry"] * 100.0
                        if not pos["trail_active"] and move_pct >= TRAIL_ACTIVATE_PCT:
                            pos["trail_active"] = True; pos["peak"] = bar_c
                        if pos["trail_active"]:
                            if direction == "long":
                                pos["peak"] = max(pos["peak"], bar_h)
                                trail = pos["peak"] * (1 - TRAIL_STEP_PCT / 100.0)
                                if bar_l <= trail:
                                    pnl = sign * (trail - pos["entry"]) * pos["qty"] - trail * pos["qty"] * FEE_TAKER - pos["fee_in"] * (pos["qty"]/pos["qty_init"])
                                    pos["qty"] = 0; all_trades.append(pnl)
                            else:
                                pos["peak"] = min(pos["peak"], bar_l) if pos["peak"] else bar_l
                                trail = pos["peak"] * (1 + TRAIL_STEP_PCT / 100.0)
                                if bar_h >= trail:
                                    pnl = sign * (trail - pos["entry"]) * pos["qty"] - trail * pos["qty"] * FEE_TAKER - pos["fee_in"] * (pos["qty"]/pos["qty_init"])
                                    pos["qty"] = 0; all_trades.append(pnl)
                basket_pnl_pct = 0.0
                if total_margin > 0:
                    live_pnl = sum(sign * (alt_data[s][3][pos["idx_map"].get(i, 0)] - pos["entry"]) * pos["qty"]
                                   for s, pos in basket_open["coins"].items() if pos["qty"] > 1e-12 and s in alt_data)
                    basket_pnl_pct = live_pnl / total_margin * 100.0 if total_margin else 0.0
                timeout = hold_h >= MAX_HOLD_HOURS
                global_stop = basket_pnl_pct <= -BASKET_GLOBAL_SL_PCT
                remaining = sum(p["qty"] for p in basket_open["coins"].values())
                if remaining <= 1e-9 or timeout or global_stop:
                    if timeout or global_stop:
                        for sym, pos in basket_open["coins"].items():
                            if pos["qty"] <= 1e-12: continue
                            ad = alt_data.get(sym)
                            if not ad: continue
                            idx = pos["idx_map"].get(i)
                            if idx is None: continue
                            exit_px = ad[3][idx]
                            pnl = sign * (exit_px - pos["entry"]) * pos["qty"] - exit_px * pos["qty"] * FEE_TAKER - pos["fee_in"] * (pos["qty"]/pos["qty_init"])
                            all_trades.append(pnl)
                    basket_open = None
                continue

            if i < len(adx_vals) and adx_vals[i] > ADX_MIN:
                chg = pct_change_over(bc[:i + 1], bars_back)
                if abs(chg) >= BTC_THRESHOLD_PCT:
                    r = rsi(bc[max(0, i - RSI_LEN * 6):i + 1], RSI_LEN)
                    if RSI_EXTREME_LOW < r < RSI_EXTREME_HIGH:
                        direction = "long" if chg > 0 else "short"
                        sign = 1 if direction == "long" else -1
                        coins_pos = {}
                        for sym in coins_all:
                            ad = alt_data.get(sym)
                            if not ad or len(ad[3]) <= i: continue
                            entry = ad[3][i]
                            sl = entry * (1 - SL_PCT/100) if direction == "long" else entry * (1 + SL_PCT/100)
                            tp1 = entry * (1 + TP1_PCT/100) if direction == "long" else entry * (1 - TP1_PCT/100)
                            tp2 = None
                            if TP2_PCT > 0:
                                tp2 = entry * (1 + TP2_PCT/100) if direction == "long" else entry * (1 - TP2_PCT/100)
                            notional = MARGIN * LEVERAGE
                            qty = notional / entry
                            idx_map = {j: j for j in range(i, min(i + int(MAX_HOLD_HOURS * 60 / bar_minutes) + 5, len(ad[3])))}
                            coins_pos[sym] = dict(entry=entry, sl=sl, tp1=tp1, tp2=tp2, qty=qty, qty_init=qty,
                                                   fee_in=qty*entry*FEE_MAKER, tp1_done=False,
                                                   tp2_done=(tp2 is None), trail_active=False, peak=entry,
                                                   idx_map=idx_map)
                        if coins_pos:
                            basket_open = dict(direction=direction, open_ts=bct[i], coins=coins_pos)

        if not all_trades:
            tg_send(chat, f"\U0001F4ED За {days} дн сделок не было (порог движения BTC не пробит или ADX/RSI фильтры блокировали)."); return
        n_trades = len(all_trades)
        wins = sum(1 for x in all_trades if x > 0)
        total = sum(all_trades)
        eq = [DEPOSIT]
        for x in all_trades: eq.append(eq[-1] + x)
        peak = DEPOSIT; dd = 0.0
        for x in eq: peak = max(peak, x); dd = max(dd, (peak - x) / peak)
        tg_send(chat, (f"\U0001F9EA БЭКТЕСТ Beta Momentum ({MODE}): {days} дн \u00d7 {len(coins_all)} альтов\n"
                       f"Частичных закрытий: {n_trades}\n"
                       f"В плюсе: {wins} ({wins/n_trades*100:.0f}%)\n"
                       f"Итог: {total:+.2f}$ ({total/DEPOSIT*100:+.1f}% депо) \u00b7 макс.просадка {dd*100:.1f}%\n"
                       f"\u26A0\uFE0F Комиссии учтены; спред/проскальзывание нет; корзина упрощена до одной активной за раз."))
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
                        "\U0001F4D6 Команды EVA v6 (Beta Momentum \u2014 следование за BTC):\n"
                        "/start — запуск и краткая сводка\n"
                        "/status — активные корзины: монеты, PnL, стадия\n"
                        "/stats — статистика: win rate, PnL\n"
                        "/backtest [дней] — прогон по истории\n"
                        "/close_all — закрыть все корзины немедленно\n"
                        "/pause — пауза (новые корзины не открываются)\n"
                        "/resume — возобновить сканирование\n"
                        "/help — эта справка")
                elif text.startswith("/start"):
                    st["paused"] = False; save_state(st)
                    tg_send(cid, f"\U0001F916 EVA v6 — Beta Momentum (следование за BTC), режим: {'консервативный' if IS_CONSERVATIVE else 'агрессивный'}\n"
                             f"TF={TF} \u00b7 порог BTC {BTC_THRESHOLD_PCT}%/{BTC_LOOKBACK_MIN}мин \u00b7 корр>{MIN_CORR} \u00b7 "
                             f"корзина до {BASKET_SIZE} монет \u00b7 плечо {LEVERAGE:.0f}x\n"
                             f"Риск корзины {BASKET_RISK_PCT}% депо \u00b7 SL {SL_PCT}% \u00b7 глобальный стоп \u2212{BASKET_GLOBAL_SL_PCT}%\n"
                             "Команды: /status \u00b7 /stats \u00b7 /backtest \u00b7 /close_all \u00b7 /pause \u00b7 /resume")
                elif text.startswith("/pause"):
                    st["paused"] = True; save_state(st)
                    tg_send(cid, "\u23F8 Пауза: новые корзины не открываю.")
                elif text.startswith("/resume"):
                    st["paused"] = False; save_state(st)
                    tg_send(cid, "\u25B6\uFE0F Сканирование возобновлено.")
                elif text.startswith("/status"):
                    tg_send(cid, status_text(st))
                elif text.startswith("/stats"):
                    tg_send(cid, stats_text())
                elif text.startswith("/close_all"):
                    close_all_baskets(st, cid)
                elif text.startswith("/backtest"):
                    parts = text.split()[1:]
                    nums = [p for p in parts if p.isdigit()]
                    bd = int(nums[0]) if len(nums) > 0 else 14
                    threading.Thread(target=run_backtest, args=(cid, bd), daemon=True).start()
        except Exception as e:
            print("tg_loop err:", e); time.sleep(3)

def main():
    st = load_state()
    chat = load_chat()
    print(f"EVA v6 (Beta Momentum, режим={MODE}) запущен. chat:", "есть" if chat else "нет")
    threading.Thread(target=tg_loop, args=(st,), daemon=True).start()
    last_scan = last_manage = 0
    while True:
        try:
            chat = load_chat()
            roll_day(st, chat)
            now = time.time()
            if now - last_manage >= MANAGE_EVERY_SEC:
                last_manage = now
                manage_baskets(st, chat)
            if now - last_scan >= SCAN_EVERY_SEC:
                last_scan = now
                scan_once(st, chat)
                print(f"[scan] активных корзин {len(active_baskets(st))}/{MAX_CONCURRENT_BASKETS} \u00b7 сделок сегодня {st.get('trades_today',0)}")
        except Exception as e:
            print("main err:", e)
        time.sleep(2)

if __name__ == "__main__":
    main()
