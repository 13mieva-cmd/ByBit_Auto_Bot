# -*- coding: utf-8 -*-
"""
СКАНЕР ВЛИВАНИЙ v3 — ЛОНГ + СОПРОВОЖДЕНИЕ ПОЗИЦИИ (Telegram, для Railway)
========================================================================
Бот НЕ торгует сам. Он:
1) подсвечивает ЛОНГ-сетапы (OI↑ + объём↑ + тренд вверх), расписывая логику;
   ЕСЛИ дополнительно найден паттерн ТРЕУГОЛЬНИК (ready/breakout) — шлёт
   ОТДЕЛЬНУЮ вторую карточку "СЕТАП ТРЕУГОЛЬНИК" (тот же полный набор фильтров);
2) по кнопке "✅ Я вошёл" ведёт твою позицию: показывает P&L и комментирует
   "держать" (деньги ещё заходят) или "подумай о выходе" (приток выдыхается);
3) по кнопке "❌ Выйти" фиксирует сделку в журнал с P&L (команда /log);
4) КАЖДЫЙ отправленный сигнал (не только закрытые сделки) логируется в
   SIGNALS_FILE — это нужно для честной статистики "сигнал → что было дальше",
   без которой пороги фильтров — просто гадание. Смотри /stats.

ЧЕСТНО: комментарии бота — ОПИСАНИЕ текущего состояния, не предсказание.
Edge направления мы измеряли — его нет. Решение и риск всегда на тебе.
Журнал входов/выходов и журнал сигналов нужны, чтобы посчитать реальную
статистику: сколько сигналов было прибыльными через 4ч/24ч в среднем.

ИСПРАВЛЕНО в этой версии:
- баг: цена всегда подписывалась "(Binance)", хотя ВСЕ данные идут с Bybit API
  (klines, open_interest, ticker_info). Подпись исправлена на "(Bybit)".
- баг: штраф "всплеск ликвидаций" (liq_spike) никогда не мог сработать, т.к.
  enrich() не вычисляет это поле (Bybit публичный REST не даёт данных по
  ликвидациям) — мёртвый код удалён, чтобы не создавать иллюзию защиты.
- добавлено логирование сигналов (SIGNALS_FILE) + команда /stats для расчёта
  форвардной статистики по сигналам (win rate и средний % через 4ч/24ч).

Ключи через Environment: TG_TOKEN. Команды: /start /scan /log /pos /watch /bybit /stats
"""
import os, time, json, csv
import datetime as dt
import numpy as np
import requests

BYBIT="https://api.bybit.com"; QUOTE="USDT"
MAX_COINS=300; SCAN_EVERY_MIN=5; MAX_ALERTS=8
CHECK_POS_MIN=2; CALM_UPDATE_MIN=30
OI_4H_MIN=0.05; VOL_SPIKE_MIN=1.5; KNIFE_DD=-0.40; THIN_TURN=30_000_000
SPREAD_MAX=0.003            # спред >0.3% = тонкий стакан, сигнал понижаем/блокируем
VOL_PREV_MIN=0.8            # предыдущая свеча тоже >= 0.8x нормы (не однобарный фитиль)
NIGHT_VOL_MULT=1.3         # в тихие часы UTC порог объёма выше (ночь/выходные шумят)
OI_PRICE_IMBALANCE=3.0     # OI растёт в 3x быстрее цены = плечо копится, риск каскада
BTC_DUMP_1H=-0.02; HI_CORR=0.8
# --- зеркальный ШОРТОВЫЙ набор для BTC (рубильник лонгов по альтам) ---
BTC_DROP_4H=-0.02      # падение BTC за 4ч больше 2%
BTC_OI_DROP_4H=-0.05   # отток OI по BTC за 4ч (деньги уходят)
BTC_VOL_SPIKE=1.5      # растущий объём на падении
BTC_RSI_OVERSOLD=30    # перепроданность
BTC_RISK_MIN_HITS=2    # сколько признаков = блок лонгов
LAST_BTC_WARN=0        # антиспам предупреждения при автоскане
PRICE_UP_4H_MIN=0.005
OI_1H_MIN=0.02           # БЫСТРЫЙ триггер: OI +2% за 1 час (приток только начался)
SPIKE_FAST_MIN=2.0       # объём последних 1-2 свечей >= 2x нормы (свежий всплеск)
MAX_EXT_ENTRY=0.05       # ПОЗДНО: цена >5% выше EMA21 — движение выдохлось, сигнал НЕ шлём
MAX_MOVE_4H=0.10         # ПОЗДНО: уже +10% за 4ч — конец движения, не гонимся
RSI_MAX=78
MIN_BARS=200
MIN_AGE_DAYS=180        # монете не меньше полугода (отсекаем свежие листинги)
COOLDOWN_H=4
FUNDING_CUTOFF=0.0005   # 0.05% за интервал — перегрев лонгами, жёсткий отказ
ATR_MIN_RATIO=0.6       # текущий ATR(14) < 60% средней за 30 баров = сжатие/чоп, сигнал не идёт
# --- РАННИЙ СИГНАЛ (экспериментальный, тестируется на данных) ---
EARLY_ENABLED=True          # ранний сигнал по пробою сжатия
EARLY15_ENABLED=True        # раннее обнаружение на 15м (движение началось) -> час подтверждает
LAST_EARLY15={}             # coin -> ts последнего 15м-раннего сигнала
EARLY_WATCH={}              # coin -> {sym, ts, v0, lsr0, fund0, price0} — живое отслеживание развития
EARLY_WATCH_HOURS=6        # сколько следим за развитием после 15м-всплеска
EARLY_WATCH_CHECK_SEC=60   # как часто проверять развитие
EARLY15_COOLDOWN_H=2
# --- РАЗВОРОТНЫЙ сигнал (контр-тренд, экспериментальный, отдельная статистика) ---
REVERSAL_ENABLED=True
REVERSAL_COOLDOWN_H=4
REVERSAL_VOL_MIN=1.8            # объём >= 1.8x нормы
REVERSAL_OI_MIN=0.02           # рост OI >=2% (отличает от шорт-сквиза)
REVERSAL_LOOKBACK_DOWN=10
REVERSAL_NO_LOW_BARS=3
REVERSAL_NO_LOW_BASE=8
LAST_REVERSAL={}
EARLY_COMPRESS_MAX=0.7      # текущий диапазон < 70% медианного = сжатие
EARLY_RVOL_MIN=3.0          # RVOL: объём >= 3x нормы = начало крупного движения (не мелкий шум)
EARLY_VOL_MIN=2.5           # объём пробойной свечи >= 2.5x среднего (было 1.2 — ловило шум)
EARLY_RSI_MAX=68            # строже обычного (78): не ловим сжатие перед разворотом вниз
EARLY_COOLDOWN_H=4          # антиспам по раннему сигналу
LAST_EARLY={}               # coin -> ts последнего раннего сигнала
LOSS_COOLDOWN_MULT=3    # во сколько раз дольше кулдаун по монете после убыточной сделки
RECENT_LOSSES={}        # coin -> ts последнего стоп-лосса (для удлинённого кулдауна)
LAST_ALERT={}
WATCH={}
TRI_ALERT={} # coin -> {sym, top, ts} - активно ждём момент пробоя крышки треугольника (стадия ready)
TRI_ALERT_HOURS=6 # сколько часов держим монету на активном ожидании пробоя после ready
WATCH_HOURS=12
WATCH_CHECK_SEC=15
RETEST_NEED_BOUNCE=True
TRADES=os.environ.get("TRADES_FILE","/tmp/scanner_trades.csv")
CHAT_FILE=os.environ.get("CHAT_FILE","/tmp/scanner_chat.txt")
SIGNALS_FILE=os.environ.get("SIGNALS_FILE","/tmp/scanner_signals.csv")
BLOCKS_FILE=os.environ.get("BLOCKS_FILE","/data/scanner_blocks.csv")
TG_TOKEN=""
SYM_CACHE={}
POSITIONS={}

# ---------- Telegram ----------
def tg(method, **p):
    try:
        return requests.get(f"https://api.telegram.org/bot{TG_TOKEN}/{method}",params=p,timeout=40).json()
    except requests.exceptions.ReadTimeout:
        return {}
    except Exception as e:
        print("TG:",e); return {}

def kb(rows): return json.dumps({"inline_keyboard":rows})

def tg_send(cid,t,buttons=None):
    p={"chat_id":cid,"text":t,"parse_mode":"HTML"}
    if buttons: p["reply_markup"]=kb(buttons)
    tg("sendMessage",**p)

def tg_answer(qid,text=""): tg("answerCallbackQuery",callback_query_id=qid,text=text)

def tg_send_doc(cid,path,caption=""):
    try:
        with open(path,"rb") as f:
            requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendDocument",
                data={"chat_id":cid,"caption":caption},files={"document":f},timeout=60)
    except Exception as e: print("doc:",e)

# ---------- Bybit ----------
def bybit_price(coin):
    try:
        r=requests.get("https://api.bybit.com/v5/market/tickers",
            params={"category":"linear","symbol":coin+"USDT"}, timeout=8)
        if r.status_code!=200: return None
        j=r.json()
        lst=(j.get("result") or {}).get("list") or []
        return float(lst[0]["lastPrice"]) if lst else None
    except Exception:
        return None

def bget(path, params):
    r=requests.get(f"{BYBIT}{path}", params=params, timeout=20)
    if r.status_code!=200: raise RuntimeError(f"HTTP {r.status_code}")
    j=r.json()
    if j.get("retCode")!=0: raise RuntimeError(f"Bybit retCode {j.get('retCode')}: {j.get('retMsg')}")
    return j["result"]

_tickers_cache={"ts":0,"data":[]}
def all_tickers():
    if time.time()-_tickers_cache["ts"]<60 and _tickers_cache["data"]:
        return _tickers_cache["data"]
    res=bget("/v5/market/tickers", {"category":"linear"})
    _tickers_cache["data"]=res["list"]; _tickers_cache["ts"]=time.time()
    return res["list"]

def universe():
    rows=[x for x in all_tickers() if x["symbol"].endswith("USDT")]
    rows.sort(key=lambda x: float(x.get("turnover24h",0) or 0), reverse=True)
    seen=set(); out=[]
    for x in rows:
        b=x["symbol"][:-4]
        if b and b not in seen: seen.add(b); out.append((b,x["symbol"]))
    return out[:MAX_COINS]

def klines_tf(symbol, interval, limit=200):
    res=bget("/v5/market/kline", {"category":"linear","symbol":symbol,"interval":interval,"limit":limit})
    k=res["list"][::-1]
    return ([float(x[4]) for x in k],[float(x[2]) for x in k],
            [float(x[3]) for x in k],[float(x[5]) for x in k])

def klines(symbol, limit=200):
    res=bget("/v5/market/kline", {"category":"linear","symbol":symbol,"interval":"60","limit":limit})
    k=res["list"][::-1]
    closes=[float(x[4]) for x in k]; highs=[float(x[2]) for x in k]
    lows=[float(x[3]) for x in k]; vols=[float(x[5]) for x in k]
    return closes,highs,lows,vols

def open_interest(symbol, limit=50):
    res=bget("/v5/market/open-interest", {"category":"linear","symbol":symbol,
        "intervalTime":"1h","limit":limit})
    oi=res["list"][::-1]
    return [float(x["openInterest"]) for x in oi]

def stop_map(m):
    """Оценка зон скопления стопов (ориентир, не точная карта — публичный API её не даёт).
    Стопы ЛОНГИСТОВ стоят ниже цены (под поддержкой), стопы ШОРТИСТОВ — выше (над сопротивлением).
    Опираемся на уровни, что бот уже считает: ближайшую поддержку снизу и сопротивление сверху."""
    price=m["price"]; levels=m.get("levels") or []
    below=[lv for lv in levels if lv[0]<price*0.998]   # уровни ниже цены
    above=[lv for lv in levels if lv[0]>price*1.002]   # уровни выше цены
    # стопы лонгистов — под ближайшей сильной поддержкой (строго ниже цены)
    if below: long_stops=max(below, key=lambda x:x[1])[0]
    else:
        cb=m.get("consol_base")
        long_stops=cb if (cb and cb<price) else None
    # стопы шортистов — над ближайшим сопротивлением (строго ВЫШЕ цены)
    if above: short_stops=min(above, key=lambda x:x[0])[0]
    else:
        oh=m.get("old_high")
        short_stops=oh if (oh and oh>price) else None
    return long_stops, short_stops

def liq_zones(price, funding=0.0):
    """Оценка зон ликвидации через популярное плечо (метод как у CoinGlass/Hyblock):
    при плече L ликвидация лонга ≈ на 1/L ниже входа. Берём 10x и 25x — самые
    популярные. funding уточняет, с какой стороны кластер плотнее (знак перекоса).
    Возвращает dict с зонами лонг-ликвидаций (ниже) и шорт-ликвидаций (выше)."""
    k=0.005  # коэф. поддержания маржи (типовой)
    out={}
    for L in (10,25):
        out[f"long_{L}x"]=price*(1-(1.0/L)+k)   # лонги ликвидируются НИЖЕ
        out[f"short_{L}x"]=price*(1+(1.0/L)-k)  # шорты ликвидируются ВЫШЕ
    # какая сторона перегружена (по funding): + = перегруз лонгами -> ниже плотнее
    if funding>=0.0003: out["heavy"]="long"     # лонги перегружены -> магнит вниз
    elif funding<=-0.0003: out["heavy"]="short" # шорты перегружены -> магнит вверх
    else: out["heavy"]=None
    return out

def long_short_ratio(symbol):
    """Соотношение лонг/шорт аккаунтов с Bybit (account-ratio). None если недоступно.
    ratio>1 = лонгистов больше (толпа в лонге), <1 = шортистов больше."""
    try:
        res=bget("/v5/market/account-ratio", {"category":"linear","symbol":symbol,
                                               "period":"1h","limit":1})
        lst=res.get("list") or []
        if not lst: return None
        b=float(lst[0].get("buyRatio",0)); s=float(lst[0].get("sellRatio",0))
        if s<=0: return None
        return b/s
    except Exception:
        return None

def ticker_info(symbol):
    for t in all_tickers():
        if t["symbol"]==symbol:
            bid=float(t.get("bid1Price",0) or 0); ask=float(t.get("ask1Price",0) or 0)
            spread=(ask-bid)/((ask+bid)/2) if (bid>0 and ask>0) else 0.0
            return dict(price=float(t["lastPrice"]),
                funding=float(t.get("fundingRate",0) or 0),
                turnover=float(t.get("turnover24h",0) or 0),
                spread=spread)
    return None

def rsi(closes, period=14):
    if len(closes)<period+1: return 50.0
    d=[closes[i]-closes[i-1] for i in range(1,len(closes))]
    g=[x if x>0 else 0 for x in d]; l=[-x if x<0 else 0 for x in d]
    ag=sum(g[:period])/period; al=sum(l[:period])/period
    for i in range(period,len(d)):
        ag=(ag*(period-1)+g[i])/period; al=(al*(period-1)+l[i])/period
    if al==0: return 100.0
    return 100-100/(1+ag/al)

def ema(v,span):
    a=2/(span+1); e=v[0]
    for x in v[1:]: e=a*x+(1-a)*e
    return e

def ema_series(v, span):
    """EMA как ряд (нужно для MACD)."""
    a=2/(span+1); out=[v[0]]
    for x in v[1:]: out.append(a*x+(1-a)*out[-1])
    return out

def macd_hist(closes, fast=12, slow=26, signal=9):
    """MACD-гистограмма (12/26/9) на часовых. >0 = бычий момент, <0 = медвежий.
    ЛАГОВЫЙ индикатор (производная цены) — используется ТОЛЬКО как справка-подтверждение,
    не как сигнал входа и не как ворота."""
    if len(closes)<slow+signal+2: return 0.0
    ef=ema_series(closes,fast); es=ema_series(closes,slow)
    macd_line=[ef[i]-es[i] for i in range(len(closes))]
    sig=ema_series(macd_line[slow:], signal)   # сигнальная по стабильной части
    if not sig: return 0.0
    return macd_line[-1]-sig[-1]               # гистограмма = MACD - сигнальная

def roc(closes, period=12):
    """Rate of Change за period часов, %. >0 = цена росла. Справка-подтверждение."""
    if len(closes)<period+1 or closes[-period-1]==0: return 0.0
    return (closes[-1]/closes[-period-1]-1)*100

def corr(a,b):
    n=min(len(a),len(b))
    if n<10: return 0.0
    ra=np.diff(a[-n:]); rb=np.diff(b[-n:])
    if ra.std()==0 or rb.std()==0: return 0.0
    return float(np.corrcoef(ra,rb)[0,1])

def find_levels(highs, lows, closes, price, min_touches=3):
    if len(closes)<40: return []
    tol=price*0.010
    away=price*0.015
    H=highs[-150:]; L=lows[-150:]
    cand=[]
    for i in range(2,len(H)-2):
        if H[i]>=max(H[i-2:i+3]): cand.append(H[i])
        if L[i]<=min(L[i-2:i+3]): cand.append(L[i])
    levels=[]
    used=[False]*len(cand)
    for i,base in enumerate(cand):
        if used[i]: continue
        cluster=[base]; used[i]=True
        for j in range(i+1,len(cand)):
            if not used[j] and abs(cand[j]-base)<=tol:
                cluster.append(cand[j]); used[j]=True
        lvl=sum(cluster)/len(cluster)
        touches=0; state="away"
        for c in closes[-150:]:
            if abs(c-lvl)<=tol and state=="away":
                touches+=1; state="near"
            elif abs(c-lvl)>away:
                state="away"
        if touches>=min_touches:
            levels.append((lvl,touches))
    levels.sort(key=lambda x:-x[1]); out=[]
    for lv in levels:
        if all(abs(lv[0]-o[0])>tol*2 for o in out): out.append(lv)
    return out[:4]

def nearest_level(levels, price):
    if not levels: return None
    lv=min(levels, key=lambda x:abs(x[0]-price))
    return dict(price=lv[0], touches=lv[1], dist=(price-lv[0])/price)

def _bar(frac, n=5):
    frac=max(0.0,min(1.0,frac))
    filled=int(round(frac*n))
    return "\u25A0"*filled + "\u25A1"*(n-filled)


def _swing_points(vals, is_high, win=3):
    """Находит локальные экстремумы (свинги) в массиве."""
    pts=[]
    for i in range(win, len(vals)-win):
        seg=vals[i-win:i+win+1]
        if is_high and vals[i]==max(seg): pts.append((i,vals[i]))
        if not is_high and vals[i]==min(seg): pts.append((i,vals[i]))
    return pts

def _fit_line(pts):
    """Линейная регрессия через точки [(x,y),...]. Возвращает (slope, intercept) или None."""
    if len(pts)<2: return None
    xs=np.array([p[0] for p in pts], dtype=float)
    ys=np.array([p[1] for p in pts], dtype=float)
    if xs.std()==0: return None
    slope,intercept=np.polyfit(xs,ys,1)
    return float(slope), float(intercept)

def detect_triangle(highs, lows, closes, price, win=45, swing_win=3):
    """ВОСХОДЯЩИЙ треугольник по классике (LiteFinance): плоский горизонтальный
    ВЕРХ (сопротивление, к которому цена подходит несколько раз) + РАСТУЩЕЕ дно
    (минимумы повышаются). Верх НЕ должен расти — иначе это восходящий КЛИН
    (разворотный, нам не нужен). Тренд вверх уже гарантируют ворота long_ok.
    Стадии: forming -> ready (у сопротивления) -> breakout (пробой).
    Возвращает (tri, tri_top, res_now, sup_now)."""
    n=len(closes)
    if n<win+5: return None,price,price,price
    H=highs[-win:]; L=lows[-win:]
    half=win//2
    # --- ВЕРХ: сопротивление (максимумы обеих половин примерно на одном уровне = плоский) ---
    top_early=max(H[:half]); top_late=max(H[half:-1] or H[half:])
    res_now=max(top_early, top_late)
    # плоский верх: поздний максимум НЕ выше раннего более чем на 1.5% (иначе верх растёт = клин)
    flat_top = top_late <= top_early*1.015
    # --- ДНО: поддержка растёт (минимум поздней половины ВЫШЕ минимума ранней) ---
    bot_early=min(L[:half]); bot_late=min(L[half:-1] or L[half:])
    rising_bottom = bot_late > bot_early*1.003          # дно поднялось хотя бы на 0.3%
    sup_now=bot_late
    # --- сужение (следствие плоского верха + растущего дна) ---
    w_early=top_early-bot_early; w_late=top_late-bot_late
    contracting = w_early>0 and w_late < w_early*0.9
    # все три классических признака восходящего треугольника
    if not (flat_top and rising_bottom and contracting):
        return None,price,price,price
    # стадии
    if price>res_now:
        return "breakout",res_now,res_now,sup_now
    dist=(res_now-price)/price if price>0 else 1
    if dist<=0.02:
        return "ready",res_now,res_now,sup_now
    return "forming",res_now,res_now,sup_now


def atr(highs, lows, closes, period=14):
    """Average True Range — мера волатильности. Для фильтра рыночного режима:
    если текущий ATR аномально низкий относительно средней за 30 периодов,
    рынок в фазе сжатия/чопа — сигналы там чаще дают ложные пробои."""
    n=len(closes)
    if n<period+2: return 0.0
    trs=[]
    for i in range(1,n):
        tr=max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        trs.append(tr)
    if len(trs)<period: return 0.0
    a=sum(trs[:period])/period
    for x in trs[period:]:
        a=(a*(period-1)+x)/period
    return a

def atr_ratio(highs, lows, closes):
    """Текущий ATR(14) / средний ATR(14) за последние 30 баров. <1 = волатильность
    ниже нормы (сжатие диапазона, флэт)."""
    n=len(closes)
    if n<60: return 1.0
    cur=atr(highs[-20:],lows[-20:],closes[-20:],period=14)
    hist=[]
    for i in range(30):
        end=n-i; start=end-20
        if start<14: break
        hist.append(atr(highs[start:end],lows[start:end],closes[start:end],period=14))
    if not hist: return 1.0
    avg=sum(hist)/len(hist)
    if avg==0: return 1.0
    return cur/avg

def coin_age_days(symbol):
    """Возраст монеты в днях по числу доступных ДНЕВНЫХ свечей. Свежие листинги
    (< полугода) отсекаем — у них нет истории, паттерны/уровни недостоверны."""
    try:
        res=bget("/v5/market/kline", {"category":"linear","symbol":symbol,
                 "interval":"D","limit":400})
        return len(res.get("list") or [])
    except Exception:
        return 999   # если не смогли проверить — не блокируем

def vol_acceleration(vols):
    """ДИНАМИКА объёма (а не статичный порог): ускоряется ли объём ПРЯМО СЕЙЧАС.
    Отличает реальный разгон (свеча за свечой растёт) от разового выброса-шума.
    Возвращает (accelerating, slope_ratio):
      accelerating — 3 последние свечи объёма растут последовательно;
      slope_ratio — во сколько раз объём свежих 3 свечей выше предыдущих 3."""
    if len(vols)<9: return False, 1.0
    v=vols[-9:]
    recent=sum(v[-3:])/3          # свежие 3 свечи
    prev=sum(v[-6:-3])/3          # предыдущие 3
    older=sum(v[-9:-6])/3         # ещё раньше
    slope_ratio = recent/prev if prev>0 else 1.0
    # ускорение: объём растёт ступенями recent>prev>older (разгон, а не выброс)
    accelerating = recent>prev>older and slope_ratio>=1.3
    return accelerating, slope_ratio

def detect_early_15m(c15, h15, l15, v15):
    """РАННЕЕ обнаружение на 15м: движение только НАЧАЛОСЬ — всплеск объёма на 15м +
    цена растёт за последние 15м-свечи. Ловит старт на 1-2 свече, ДО того как
    наберётся часовая картина. Подтверждение приходит позже часовым long_ok.
    Возвращает (started: bool, move_pct)."""
    if not v15 or len(v15)<30 or len(c15)<8: return False,0
    vb=sum(v15[-16:-4])/12 if len(v15)>=16 else (sum(v15)/len(v15))
    vspike=(sum(v15[-4:])/4)/vb if vb>0 else 0     # объём последних 4х15м vs среднего
    move=c15[-1]/c15[-6]-1 if len(c15)>=6 else 0    # рост за ~1.5ч на 15м
    # 3 ЗЕЛЁНЫЕ свечи подряд на 15м (движение реальное, а не одна свеча-выброс)
    # зелёная = close выше close предыдущей свечи
    three_green = len(c15)>=4 and c15[-1]>c15[-2]>c15[-3]>c15[-4]
    accel, slope = vol_acceleration(v15)   # динамика: разгоняется ли объём
    rvol_thr=EARLY_RVOL_MIN*night_mult()   # ночью/выходные порог выше
    # старт = (сильный RVOL ИЛИ объём ускоряется) + движение цены + 3 зелёные
    vol_ok = vspike>=rvol_thr or accel
    started = vol_ok and 0.008<=move<=0.08 and three_green
    return started, move, vspike, slope, three_green

def detect_compression(highs, lows, closes, vols):
    """РАННИЙ детектор: цена сжалась в узкий диапазон, и последняя свеча только что
    пробила его вверх на подросшем объёме. Ловит ПЕРВУЮ свечу движения — ДО того,
    как OI/объём раздуются (то есть ДО обычного лонг-сигнала).
    ЧЕСТНО: это НЕ подтверждение деньгами, а ставка на то, что сжатие разрешится
    вверх. Пробой сжатия вверх и вниз почти равновероятны — отсюда доп. фильтры.
    Возвращает (just_broke_up: bool, zone_hi, zone_lo)."""
    n=len(closes)
    if n<60: return False,0,0
    # диапазон последних 24 свечей БЕЗ самой последней (окно сжатия)
    win_h=highs[-25:-1]; win_l=lows[-25:-1]
    zone_hi=max(win_h); zone_lo=min(win_l)
    cur_range=zone_hi-zone_lo
    if cur_range<=0: return False,0,0
    # исторические диапазоны за предыдущие 30 окон по 24 свечи
    hist=[]
    for i in range(1,31):
        e=n-1-i; s=e-24
        if s<0: break
        seg_h=highs[s:e]; seg_l=lows[s:e]
        if seg_h and seg_l: hist.append(max(seg_h)-min(seg_l))
    if len(hist)<10: return False,0,0
    med=sorted(hist)[len(hist)//2]
    if med<=0: return False,0,0
    compressed = cur_range < med*EARLY_COMPRESS_MAX          # сейчас уже нормы = сжатие
    last=closes[-1]
    broke_up = last>zone_hi                                   # пробой вверх за границу сжатия
    vb=sum(vols[-25:-1])/24 if len(vols)>=25 else (sum(vols)/len(vols))
    vol_ok = vols[-1] >= vb*EARLY_VOL_MIN if vb>0 else False
    return (compressed and broke_up and vol_ok), zone_hi, zone_lo

def core(coin,closes,highs,lows,vols,oic,btc,btc_p4=0.0,tri_mtf=None,turn24=None):
    if len(closes)<MIN_BARS or len(oic)<25: return None
    price=closes[-1]
    p4=price/closes[-5]-1 if len(closes)>=5 else 0
    oi1=oic[-1]/oic[-2]-1 if oic[-2]>0 else 0
    oi4=oic[-1]/oic[-5]-1 if oic[-5]>0 else 0
    oi24=oic[-1]/oic[-25]-1 if oic[-25]>0 else 0
    vr=sum(vols[-4:]); vb=(sum(vols[-28:-4])/24*4) if len(vols)>=28 else vr
    spike=vr/vb if vb>0 else 0
    # СВЕЖИЙ всплеск: последние 1-2 свечи против нормы (ловит приток, начавшийся ЧАС назад)
    vb1=vb/4 if vb>0 else 0                      # норма на одну свечу
    spike_fast=(sum(vols[-2:])/2)/vb1 if vb1>0 else 0
    e21=ema(closes[-60:],21); e50=ema(closes[-60:],50)
    uptrend=price>e50 and e21>e50
    ext=(price-e21)/e21 if e21>0 else 0
    consol_base=min(lows[-8:]) if len(lows)>=8 else min(lows)
    old_high=max(highs[-72:-4]) if len(highs)>76 else max(highs[:-4] or highs)
    extended = ext>0.05

    levels=find_levels(highs,lows,closes,price,min_touches=3)
    lvl=nearest_level(levels,price)
    tri,tri_top,tri_res_now,tri_sup_now = detect_triangle(highs,lows,closes,price)

    flag=None; flag_top=price
    if len(closes)>=30:
        imp = closes[-15]/closes[-25]-1
        pull = closes[-1]/closes[-15]-1
        pull_range = (max(highs[-12:])-min(lows[-12:]))/price
        if imp>=0.05 and -0.06<=pull<=0.01 and pull_range<0.06:
            flag_top=max(highs[-12:-1])
            flag = "breakout" if price>flag_top else "forming"

    hi7=max(highs[-168:]) if len(highs)>=168 else max(highs)
    dd=price/hi7-1
    # оборот за сутки: точный turnover24h с биржи (если передан), иначе оценка
    turn = turn24 if (turn24 and turn24>0) else sum(vols[-24:])*price
    # СУТОЧНЫЙ RVOL: сегодняшний 24ч-объём против среднего за доступную историю
    vol24_now=sum(vols[-24:])
    if len(vols)>=48:
        # средний 24ч-объём по скользящим блокам (в монетах), исключая последние сутки
        blocks=[sum(vols[i:i+24]) for i in range(0, len(vols)-24, 24)]
        vol24_avg=sum(blocks)/len(blocks) if blocks else vol24_now
    else:
        vol24_avg=vol24_now
    daily_rvol = vol24_now/vol24_avg if vol24_avg>0 else 1.0
    cor=corr(btc,closes)
    r=rsi(closes[-40:],14)
    btc_beta = cor>=HI_CORR and btc_p4>0 and abs(p4-btc_p4)<0.01
    tf=sum([oi1>0.01, oi4>=OI_4H_MIN, oi24>0.10])
    brk=price>max(highs[-168:-1]) if len(highs)>168 else False
    atrr=atr_ratio(highs,lows,closes)   # режим рынка: <1 = сжатие/чоп
    mh=macd_hist(closes)                # MACD-гистограмма (справка-подтверждение)
    rc=roc(closes,12)                   # ROC за 12ч, % (справка)
    return dict(coin=coin,price=price,p4=p4,oi1=oi1,oi4=oi4,oi24=oi24,spike=spike,spike_fast=spike_fast,
        uptrend=uptrend,dd=dd,turn=turn,cor=cor,tf=tf,brk=brk,rsi=r,btc_beta=btc_beta,
        e21=e21,ext=ext,consol_base=consol_base,old_high=old_high,extended=extended,
        tri=tri,tri_top=tri_top,tri_res_now=tri_res_now,tri_sup_now=tri_sup_now,
        flag=flag,flag_top=flag_top,levels=levels,lvl=lvl,atrr=atrr,tri_mtf=tri_mtf,
        macd_h=mh,roc=rc,daily_rvol=daily_rvol)

def long_ok(m):
    """Лонг-сигнал. ДВА пути входа:
      1) БЫСТРЫЙ — свежий приток (OI +2% за 1ч + объём последних свечей >=2x): ловим
         движение через ~1 час после старта, а не через 4.
      2) ОБЫЧНЫЙ — накопленная 4ч-картина (OI 4ч + объём 4ч).
    И ВОРОТА 'ПОЗДНО': если движение уже выдохлось (цена далеко от EMA21 или уже +10%
    за 4ч) — сигнал НЕ шлём, чтобы не звать на конце движения."""
    # приток: быстрый ИЛИ накопленный
    fast_inflow = m.get("oi1",0)>=OI_1H_MIN and m.get("spike_fast",0)>=SPIKE_FAST_MIN
    slow_inflow = m["oi4"]>=OI_4H_MIN and m["spike"]>=VOL_SPIKE_MIN
    inflow = fast_inflow or slow_inflow
    # ПОЗДНО? движение уже прошло основную часть — не гонимся за свечой
    too_late = m.get("ext",0)>MAX_EXT_ENTRY or m["p4"]>MAX_MOVE_4H
    return (inflow and m["uptrend"]
        and m["dd"]>KNIFE_DD and m["turn"]>=THIN_TURN
        and m["p4"]>=PRICE_UP_4H_MIN
        and m["rsi"]<=RSI_MAX
        and m.get("atrr",1.0)>=ATR_MIN_RATIO
        and not too_late)

def _score(m, ex):
    """Скоринг ТОЛЬКО из полей, которые бот реально вычисляет. Штраф за
    ликвидации убран — нет источника данных (Bybit public REST их не даёт),
    оставлять его было бы созданием иллюзии несуществующей защиты."""
    s=0
    s+= 2 if m["oi4"]>=0.10 else (1 if m["oi4"]>=0.05 else 0)
    s+= 2 if m["spike"]>=3 else (1 if m["spike"]>=1.5 else 0)
    s+= m["tf"]
    s+= 1 if m["brk"] else 0
    s+= 1 if 50<=m.get("rsi",50)<=70 else 0
    s+= 1 if not m.get("btc_beta") else 0
    s+= 1 if m["turn"]>=20_000_000 else 0
    if ex.get("funding",0)>0.01: s-=1
    return max(0,min(10,s))

# ---------- КАРТОЧКА 1: чистый лонг-сетап (без блока треугольника) ----------
def card_long(m, ex):
    cautions=[]
    if m.get("btc_beta"): cautions.append("движение в основном ЗА БИТКОМ — не её собственный приток")
    elif m["cor"]>=HI_CORR: cautions.append(f"сильно ходит за биткоином (корреляция {m['cor']*100:.0f}%)")
    if ex.get("funding",0)>0.01: cautions.append(f"повышенный funding ({ex.get('funding',0)*100:.3f}%) — плечо копится")
    if m.get("extended"): cautions.append("вход на пике импульса \u2014 лучше ждать откат")
    # OI/цена дисбаланс: плечо копится быстрее движения -> риск каскада
    if m["p4"]>0 and m["oi4"]>m["p4"]*OI_PRICE_IMBALANCE:
        cautions.append(f"OI растёт быстрее цены (OI +{m['oi4']*100:.0f}% vs цена +{m['p4']*100:.1f}%) \u2014 плечо копится, риск резкого разворота")
    # спред: тонкий стакан -> цена может лететь через дырки ликвидности
    sp=ex.get("spread",0)
    if sp>=SPREAD_MAX:
        cautions.append(f"широкий спред {sp*100:.2f}% \u2014 тонкий стакан, движение может быть проколом через редкие лимитки")
    # экстремальный funding в ЛЮБУЮ сторону = перегрев
    fund=ex.get("funding",0)
    if fund<=-0.01:
        cautions.append(f"сильно отрицательный funding ({fund*100:.3f}%) \u2014 перегрев шортами, риск каскада против толпы")
    if m.get("btc_weak") and m["cor"]>=0.3:
        cautions.append(f"\U0001F7E1 BTC слабеет по факту ({m['btc_weak']}) — при корреляции {m['cor']*100:.0f}% риск потянуть альт вниз (реакция, не прогноз)")

    sc=_score(m,ex)
    head = "\U0001F7E2" if not cautions else "\U0001F7E1"
    arrow = "\u25B2" if m["p4"]>=0 else "\u25BC"
    rsi_v=int(m.get("rsi",50))
    tf_txt={3:"1ч+4ч+24ч \u2705",2:"2 интервала",1:"1 интервал \u26A0\uFE0F"}.get(m["tf"],"")

    table=(
        f"\U0001F4B0 Приток OI {m['oi4']*100:+.0f}% {_bar(m['oi4']/0.20,5)}\n"
        f"\U0001F4C8 Объём \u00d7{m['spike']:.1f} {_bar(m['spike']/5,5)}\n"
        f"\U0001F321 RSI {rsi_v} {_bar(rsi_v/100,5)}\n"
        f"\U0001F4A7 Ликвидн. ${m['turn']/1e6:.0f}M/сутки {_bar(min(m['turn']/100e6,1),5)}\n"
        f"\U0001F525 Объём сегодня \u00d7{m.get('daily_rvol',1):.1f} от обычного {_bar(min(m.get('daily_rvol',1)/10,1),5)}\n"
        f"\u26A1 Волатильность {m.get('atrr',1.0)*100:.0f}% от нормы {_bar(min(m.get('atrr',1.0),1.5)/1.5,5)}\n"
        f"\U0001F517 Корр. с BTC {m['cor']*100:.0f}% {_bar(abs(m['cor']),5)}"
    )
    # --- КАРТА СТОПОВ: перекос толпы + зоны стопов обеих сторон ---
    lsr=m.get("ls_ratio")
    long_stops,short_stops=stop_map(m)
    stop_lines=["", "\U0001F5FA <b>Карта стопов</b> (ориентир, не точная):"]
    if lsr:
        if lsr>=1.5: perekos=f"перекос в ЛОНГ ×{lsr:.1f} — толпа в лонге, риск слива за их стопами"
        elif lsr<=0.67: perekos=f"перекос в ШОРТ (Л/Ш {lsr:.2f}) — шортов много, их стопы = топливо вверх"
        else: perekos=f"баланс (Л/Ш {lsr:.2f})"
        stop_lines.append(f"\u2696\uFE0F Толпа: {perekos}")
    if short_stops:
        stop_lines.append(f"\U0001F3AF Стопы шортистов: над <b>${short_stops:.5g}</b> — топливо для рывка вверх")
    if long_stops:
        stop_lines.append(f"\U0001F6D1 Стопы лонгистов: под <b>${long_stops:.5g}</b> — риск слива за ними")
    # зоны ликвидации через плечо (метод CoinGlass-стайл: 10x/25x)
    lz=liq_zones(m["price"], ex.get("funding",0.0))
    stop_lines.append(f"\U0001F525 Ликвидации лонгов (плечо): ~${lz['long_25x']:.5g} (25x) / ~${lz['long_10x']:.5g} (10x) — магнит вниз")
    stop_lines.append(f"\U0001F525 Ликвидации шортов (плечо): ~${lz['short_25x']:.5g} (25x) / ~${lz['short_10x']:.5g} (10x) — магнит вверх")
    if lz["heavy"]=="long":
        stop_lines.append("\u2022 <i>funding+ : рынок перегружен лонгами \u2014 нижний кластер плотнее (риск слива вниз)</i>")
    elif lz["heavy"]=="short":
        stop_lines.append("\u2022 <i>funding\u2212 : рынок перегружен шортами \u2014 верхний кластер плотнее (топливо вверх)</i>")
    # УМНЫЙ СТОП: ставить ЗА кластером лонг-ликвидаций, а не внутри (защита от стоп-ханта)
    # ближайший к цене кластер СНИЗУ (чтобы стоп был тесным, а не за тридевять земель)
    cands=[z for z in (lz["long_25x"], lz["long_10x"], long_stops) if z and z<m["price"]]
    long_cluster = max(cands) if cands else lz["long_25x"]   # ближайший снизу
    smart_stop = long_cluster*0.995                    # чуть НИЖЕ кластера
    risk=(m["price"]-smart_stop)/m["price"]*100
    stop_lines.append(f"\U0001F6E1 <b>Умный стоп: под ${smart_stop:.5g}</b> (за кластером лонг-ликвидаций ${long_cluster:.5g}, риск ~{risk:.1f}%)")
    stop_lines.append("\u2022 <i>стоп ЗА кластером, а не внутри — чтобы не выбило на стоп-ханте перед разворотом</i>")
    stop_lines.append("<i>точных стопов публичный API не даёт \u2014 это оценка по уровням + типовому плечу (как CoinGlass)</i>")

    # --- MACD/ROC: вспомогательное подтверждение момента (НЕ сигнал, НЕ ворота) ---
    mh=m.get("macd_h",0); rc=m.get("roc",0)
    macd_txt="бычий \u2713" if mh>0 else "медвежий \u2717"
    roc_txt=f"{rc:+.1f}%"
    stop_lines.append("")
    stop_lines.append(f"\U0001F4C9 Момент (справка): MACD {macd_txt}  \u00b7  ROC(12ч) {roc_txt}")
    if mh>0 and rc>0:
        stop_lines.append("\u2022 <i>MACD и ROC оба за рост — момент подтверждён (но это лаговые индикаторы, не гарантия)</i>")
    elif mh<=0 or rc<=0:
        stop_lines.append("\u2022 <i>момент по MACD/ROC смешанный — подтверждения нет, будь осторожнее</i>")

    by=m.get("bybit")
    if by:
        spread=(by-m["price"])/m["price"]*100
        rel = "вровень" if abs(spread)<0.15 else (f"выше +{spread:.1f}%" if spread>0 else f"ниже {spread:.1f}%")
        price_line=f"\U0001F4B5 ${m['price']:.5g} (Bybit) {arrow} {m['p4']*100:+.1f}% за 4ч, сверка: {rel}"
    else:
        price_line=f"\U0001F4B5 ${m['price']:.5g} (Bybit) {arrow} {m['p4']*100:+.1f}% за 4ч"

    lines=[
        f"{head} {m['coin']} \u00b7 ЛОНГ-СЕТАП",
        price_line, "",
        f"\U0001F4AA Сила сетапа: {sc}/10 {_bar(sc/10,5)}", "",
        table,
        f"\U0001F4CA Подтверждение: {tf_txt}",
        (f"\u26A1 <b>БЫСТРЫЙ триггер</b>: OI +{m.get('oi1',0)*100:.1f}% за 1ч, объём свежих свечей \u00d7{m.get('spike_fast',0):.1f} \u2014 приток только начался"
         if (m.get('oi1',0)>=OI_1H_MIN and m.get('spike_fast',0)>=SPIKE_FAST_MIN)
         else f"\U0001F551 Обычный триггер: накопленная 4ч-картина (OI +{m['oi4']*100:.0f}%, объём \u00d7{m['spike']:.1f})"),
        f"\U0001F4CF Цена выше EMA21 на {m.get('ext',0)*100:.1f}% (ворота 'поздно': >{MAX_EXT_ENTRY*100:.0f}% \u2014 сигнал не шлём)",
    ] + stop_lines

    reasons=[]
    reasons.append("деньги активно заходят" if m["oi4"]>=0.10 else "деньги заходят")
    reasons.append("тренд вверх (>EMA50)")
    if m["brk"]: reasons.append("пробой 7д-максимума")
    if 50<=rsi_v<=70: reasons.append("RSI здоровый")
    lines.append("\u2705 " + ", ".join(reasons) + ".")

    if cautions:
        lines.append("")
        lines.append("\U0001F6E1 Учти риски:")
        for c in cautions: lines.append("\u26A0\uFE0F "+c)
    else:
        lines.append("\U0001F6E1 Риски: чисто \u2705 (не нож, ликвидность ок)")

    e21=m.get("e21",m["price"]); base=m.get("consol_base",m["price"]); oh=m.get("old_high",m["price"])
    ext=m.get("ext",0)

    fl=m.get("flag"); ft=m.get("flag_top",m["price"])
    if fl:
        lines.append("")
        if fl=="forming":
            lines.append("\U0001F6A9 Флаг: откат после импульса")
            lines.append("\u2022 был сильный импульс вверх, сейчас неглубокий откат-консолидация (флажок)")
            lines.append(f"\u2022 верх флага: ${ft:.5g}")
            lines.append(f"\u2022 классически цель \u2014 продолжение вверх при выходе за ${ft:.5g}")
            lines.append("\u2022 вход выгоднее у низа отката, чем на выходе")
        elif fl=="breakout":
            lines.append("\U0001F6A9\U0001F680 Флаг: пробой вверх")
            lines.append(f"\u2022 цена вышла из флага выше ${ft:.5g} \u2014 импульс продолжается")
            lines.append("\u2022 \u26A0\uFE0F подтверждение: удержание выше уровня; ложные выходы тоже бывают")

    lv=m.get("lvl")
    if lv:
        pos = "цена НА уровне" if abs(lv["dist"])<0.012 else ("цена НАД уровнем (уровень стал поддержкой)" if lv["dist"]>0 else "цена ПОД уровнем (уровень = сопротивление сверху)")
        lines.append("")
        strength = "очень сильный" if lv['touches']>=6 else ("сильный" if lv['touches']>=4 else "заметный")
        lines.append(f"\U0001F4CF Уровень ${lv['price']:.5g} \u2014 {strength}: цена подходила к нему {lv['touches']} раз(а)")
        lines.append(f"\u2022 {pos}")
        lines.append("\u2022 чем больше подходов, тем важнее уровень (рынок его «уважает»)")
        if abs(lv["dist"])<0.012:
            lines.append("\u2022 цена наторговывает у уровня \u2014 это твоя зона, но вход ТОЛЬКО на ретесте с отбоем")

    lines.append("")
    lines.append("\U0001F6D1 Правило входа: НЕ по рынку сейчас. Вход только на РЕТЕСТЕ уровня/зоны с отбоем (зелёная свеча). Бот позовёт.")

    if m.get("btc_weak") and m["cor"]>=0.3:
        lines.append("")
        lines.append(f"\U0001F7E1 BTC слабеет ({m['btc_weak']}) — при корреляции {m['cor']*100:.0f}% риск потянуть вниз")
    if m.get("watching"):
        zlo,zhi=m["watching"]; wk=m.get("watch_kind","зоне")
        lines.append("")
        lines.append(f"\u23F3 Взял на отслеживание \u2014 позову на ретесте к {wk} ${zlo:.5g}\u2013${zhi:.5g}")

    lines.append("\U0001F4CD Где входить:")
    if m.get("extended"):
        lines.append(f"\u26A0\uFE0F цена на +{ext*100:.0f}% выше EMA21 \u2014 не гонись за свечой")
    lines.append(f"\u2022 зона отката (лимитка): ${e21:.5g} (EMA21) \u2013 ${base:.5g} (база наторговки)")
    hi_note = " \u2014 пробивается \U0001F680" if m["price"]>oh else " \u2014 цель"
    lines.append(f"\u2022 старый хай (уровень): ${oh:.5g}{hi_note}")
    lines.append("\u2022 выгоднее лимитка в зоне отката, чем по рынку на пике")

    ch=chain_line(m["coin"],"long")
    if ch: lines += ["", ch, "<i>по монете уже были сигналы — движение развивается по этапам (подтверждение)</i>"]
    _n,_w=track_record("long")
    if _n>0:
        lines += ["", f"\U0001F4C8 Трек-рекорд ЛОНГ-сигналов: измерено {_n}, в плюсе через 24ч {_w} ({_w/_n*100:.0f}%)"]
    lines += ["", "\u2501"*16,
        "\u26A0\uFE0F Подсветка, не приказ. Пойдёт ли вверх \u2014 не гарантия. "
        "Стоп на Bybit \u2014 обязателен."]
    return "\n".join(lines)

# ---------- КАРТОЧКА 2: сетап ТРЕУГОЛЬНИК (те же фильтры long_ok, + tri) ----------
def card_triangle(m, ex):
    tri=m.get("tri")
    if tri not in ("ready","breakout"):
        return None
    tt=m.get("tri_top", m["price"])
    sc=_score(m,ex)
    rsi_v=int(m.get("rsi",50))

    lines=[
        f"\U0001F53A {m['coin']} \u00b7 СЕТАП ТРЕУГОЛЬНИК",
        f"\U0001F4B5 ${m['price']:.5g} (Bybit)", "",
        f"\U0001F4AA Сила сетапа: {sc}/10 {_bar(sc/10,5)}",
        f"\U0001F4B0 Приток OI {m['oi4']*100:+.0f}%   \U0001F4C8 Объём \u00d7{m['spike']:.1f}   RSI {rsi_v}",
        f"\U0001F517 Корреляция с BTC {m['cor']*100:.0f}% {_bar(abs(m['cor']),5)}",
        "",
    ]
    # мультитаймфрейм-статус треугольника (справка): 15м / 1ч / 4ч
    mtf=m.get("tri_mtf")
    if mtf:
        def _mk(v): return "\u2705" if v in ("ready","breakout","forming") else "\u2014"
        n_active=sum(1 for tf in ("15м","1ч","4ч") if mtf.get(tf) in ("ready","breakout","forming"))
        lines.append(f"\U0001F553 Треугольник по ТФ: 15м {_mk(mtf.get('15м'))}  1ч {_mk(mtf.get('1ч'))}  4ч {_mk(mtf.get('4ч'))}")
        if n_active>=2:
            lines.append(f"\u2022 <i>виден на {n_active} ТФ \u2014 структура подтверждена на нескольких горизонтах (надёжнее)</i>")
        else:
            lines.append("\u2022 <i>виден только на одном ТФ \u2014 слабее подтверждён</i>")
        lines.append("")
    if tri=="ready":
        lines.append("\u26A1 Готовность к пробою")
        lines.append(f"\u2022 цена вплотную подошла к крышке ${tt:.5g} и поджимается")
        lines.append(f"\u2022 следи за закрытием свечи ВЫШЕ ${tt:.5g} \u2014 это будет пробой")
        lines.append("\u2022 не входи заранее: часто бывает ложный прокол вниз")
        lines.append("\u2022 \U0001F514 Бот сам предупредит отдельным сообщением, когда пробой произойдёт \u2014 не нужно сидеть у графика")
    elif tri=="breakout":
        lines.append("\U0001F680 ПРОБОЙ вверх")
        lines.append(f"\u2022 цена закрылась выше крышки ${tt:.5g} \u2014 треугольник пробит")
        lines.append(f"\u2022 \u26A0\uFE0F бывают ЛОЖНЫЕ пробои (снятие стопов) \u2014 подтверждение: удержание выше ${tt:.5g} или ретест крышки сверху")

    if m.get("btc_weak") and m["cor"]>=0.3:
        lines.append("")
        lines.append(f"\U0001F7E1 BTC слабеет ({m['btc_weak']}) — при корреляции {m['cor']*100:.0f}% риск потянуть вниз")
    if m.get("watching"):
        zlo,zhi=m["watching"]; wk=m.get("watch_kind","зоне")
        lines.append("")
        lines.append(f"\u23F3 Взял на отслеживание \u2014 позову на ретесте к {wk} ${zlo:.5g}\u2013${zhi:.5g}")
        if tri=="breakout":
            lines.append("\u2022 вход не на проколе, а на ретесте крышки сверху \u2014 защита от ложного пробоя")

    _n,_w=track_record("triangle")
    if _n>0:
        lines += ["", f"\U0001F4C8 Трек-рекорд ТРЕУГОЛЬНИКОВ: измерено {_n}, в плюсе через 24ч {_w} ({_w/_n*100:.0f}%)"]
    lines += ["", "\u2501"*16,
        "\u26A0\uFE0F Подсветка, не приказ. Стоп на Bybit \u2014 обязателен."]
    return "\n".join(lines)

# ---------- ЛОГИРОВАНИЕ СИГНАЛОВ (для честной статистики, не для показа) ----------
_track_cache={"ts":0,"data":{}}
def track_record(sig_type):
    """Возвращает (n_measured, n_win) для сигналов типа старше 24ч. Кэш 10 мин."""
    import time as _t
    if _t.time()-_track_cache["ts"]<600 and sig_type in _track_cache["data"]:
        return _track_cache["data"][sig_type]
    n=win=0
    if os.path.exists(SIGNALS_FILE):
        now=dt.datetime.now()
        try:
            with open(SIGNALS_FILE) as f:
                for r in csv.DictReader(f):
                    if r.get("type")!=sig_type: continue
                    ts=dt.datetime.fromisoformat(r["ts"])
                    if (now-ts).total_seconds()/3600 < 24: continue
                    sym=r["coin"] if r["coin"].endswith("USDT") else r["coin"]+"USDT"
                    fwd=price_at_cached(sym, ts+dt.timedelta(hours=24))  # цена через 24ч
                    if fwd is None: continue
                    n+=1
                    if fwd/float(r["price"])-1>0: win+=1
        except Exception: pass
    _track_cache["data"][sig_type]=(n,win); _track_cache["ts"]=_t.time()
    return n,win

def reversal_setup(closes, highs, lows, vols, oic):
    """Даунтренд -> затухание падения (нет новых минимумов) -> всплеск объёма
    на зелёной свече -> рост OI (фильтр от шорт-сквиза)."""
    if len(closes)<70 or len(oic)<4: return False,{}
    e50_past=ema(closes[-70:-10],50)
    was_downtrend=closes[-10]<e50_past
    recent_low=min(lows[-REVERSAL_NO_LOW_BARS:])
    prior_low=min(lows[-REVERSAL_NO_LOW_BASE:-REVERSAL_NO_LOW_BARS])
    no_new_low=recent_low>=prior_low
    vol_avg=sum(vols[-10:-1])/9 if len(vols)>=10 else sum(vols[:-1])/max(1,len(vols)-1)
    thr=REVERSAL_VOL_MIN*night_mult()   # ночью/выходные порог выше
    # объём УСТОЙЧИВЫЙ: последняя И предыдущая свеча выше нормы (не однобарный фитиль)
    vol_spike=vol_avg>0 and vols[-1]>=vol_avg*thr and vols[-2]>=vol_avg*VOL_PREV_MIN
    green=closes[-1]>closes[-2]
    oi_chg=oic[-1]/oic[-3]-1 if oic[-3]>0 else 0
    oi_up=oi_chg>=REVERSAL_OI_MIN
    ok=was_downtrend and no_new_low and vol_spike and green and oi_up
    return ok, dict(recent_low=recent_low, prior_low=prior_low,
        vol_ratio=(vols[-1]/vol_avg if vol_avg>0 else 0), oi_chg=oi_chg)

def card_reversal(m, ex, details):
    sc=_score(m,ex); rsi_v=int(m.get("rsi",50))
    lines=[
        f"\U0001F53B\u2192\U0001F680 <b>{m['coin']} · РАЗВОРОТ</b> (объём после сползания)",
        f"\U0001F4B5 ${m['price']:.5g} (Bybit)",
        "",
        f"\U0001F4AA Сила сетапа: {sc}/10 {_bar(sc/10,5)}",
        f"\U0001F4C9 Объём \u00d7{details.get('vol_ratio',0):.1f} от нормы \u00b7 OI {details.get('oi_chg',0)*100:+.1f}% \u00b7 RSI {rsi_v}",
        f"\U0001F517 Корреляция с BTC {m['cor']*100:.0f}%",
        "",
        "\u26A0\uFE0F <b>Разворот после даунтренда \u2014 контр-тренд, риск шорт-сквиза выше обычного лонга.</b>",
        "\u2022 рост OI подтверждён \u2014 это НОВЫЕ деньги, а не закрытие шортов",
        "\u2022 <b>жди закрытия следующей свечи выше текущей</b> \u2014 только тогда вход",
        f"\u2022 стоп под минимумом разворота: ${details.get('recent_low', m['price']):.5g}",
    ]
    ch=chain_line(m["coin"],"reversal")
    if ch: lines += ["", ch]
    _n,_w=track_record("reversal")
    if _n>0:
        lines += ["", f"\U0001F4C8 Трек-рекорд РАЗВОРОТОВ: измерено {_n}, в плюсе через 24ч {_w} ({_w/_n*100:.0f}%)"]
    lines += ["", "\u2501"*16,
        "\u26A0\uFE0F Это ловля разворота (предсказание дна) \u2014 самый рискованный тип. "
        "Подсветка, не приказ. Стоп на Bybit обязателен, размер пробный."]
    return "\n".join(lines)

def card_early15(m, ex, mv, rvol, slope, three_green_ok, btc_hits):
    """Полная карточка раннего 15м-сигнала: все показания + какие фильтры пройдены."""
    p=m["price"]; coin=m["coin"]
    fund=ex.get("funding",0); lsr=m.get("ls_ratio")
    # оценка силы: сколько подтверждающих факторов сошлось
    strength=sum([rvol>=3, m.get("daily_rvol",1)>=2, (lsr or 0)>1.2,
                  m["cor"]<0.5, m.get("macd_h",0)>0, m.get("roc",0)>0])
    bars="\u25A0"*strength+"\u25A1"*(6-strength)
    L=[
        f"\U0001F440 <b>{coin} · РАННИЙ 15м-СИГНАЛ</b>",
        f"\U0001F4B5 ${p:.5g}  \u00b7  \u25B2 +{mv*100:.1f}% на 15м",
        f"\U0001F4AA Сила раннего сетапа: {strength}/6  {bars}",
        "",
        "\U0001F4CA <b>ПОКАЗАНИЯ:</b>",
        f"\U0001F525 Всплеск объёма (15м): RVOL \u00d7{rvol:.1f}  {'\u2705 сильный' if rvol>=3 else '\u2014 по динамике'}",
        f"\U0001F4C8 Объём сегодня: \u00d7{m.get('daily_rvol',1):.1f} от обычного",
        f"\u26A1 Ускорение объёма: \u00d7{slope:.1f} (свежие свечи vs прошлые)",
        f"\U0001F7E2 3 зелёные 15м подряд: {'\u2705 да' if three_green_ok else '\u2014'}",
        f"\U0001F321 RSI: {m.get('rsi',0):.0f}  (порог раннего \u2264{EARLY_RSI_MAX})",
        f"\U0001F517 Корреляция с BTC: {m['cor']*100:.0f}%  {'\u26A0\uFE0F ходит за BTC' if m['cor']>=0.5 else '\u2705 свой импульс'}",
        f"\U0001F4A7 Ликвидность: ${m['turn']/1e6:.0f}M/сутки  (порог \u2265${THIN_TURN//1_000_000}M)",
    ]
    if lsr: L.append(f"\u2696\uFE0F Лонг/Шорт: {lsr:.1f}  {'(перекос в лонг)' if lsr>1.2 else '(баланс/шорт)'}")
    L.append(f"\U0001F4B0 Funding: {fund*100:.3f}%  {'\u26A0\uFE0F повышен' if fund>0.01 else '\u2705 норма'}")
    # момент MACD/ROC (справка)
    L.append(f"\U0001F4C9 Момент: MACD {'бычий \u2713' if m.get('macd_h',0)>0 else 'медвежий \u2717'} \u00b7 ROC(12ч) {m.get('roc',0):+.1f}%")
    L += [
        "",
        "\U0001F6E1 <b>ПРОЙДЕННЫЕ ФИЛЬТРЫ (почему сигнал прошёл):</b>",
        f"\u2705 не падающий нож (просадка {m['dd']*100:.0f}% > {int(KNIFE_DD*100)}%)",
        f"\u2705 не в чопе (волатильность {m.get('atrr',1)*100:.0f}% нормы \u2265{int(ATR_MIN_RATIO*100)}%)",
        f"\u2705 BTC не валится ({btc_hits}/6 медвежьих признаков < {BTC_RISK_MIN_HITS})",
        f"\u2705 монета зрелая (\u2265{MIN_AGE_DAYS} дней) и ликвидная (\u2265${THIN_TURN//1_000_000}M)",
        "",
        "\u23F3 <b>ЧТО ДАЛЬШЕ:</b> взял на живое отслеживание (объём/лонгисты/funding). "
        "Если через 1-2ч придёт полный \U0001F7E2 ЛОНГ-сигнал \u2014 движение подтвердилось деньгами (OI+объём за 4ч).",
    ]
    ch=chain_line(m["coin"],"early15")
    if ch: L += ["", ch]
    _n,_w=track_record("early15")
    if _n>0:
        L += ["", f"\U0001F4C8 Трек-рекорд РАННИХ 15м: измерено {_n}, в плюсе через 24ч {_w} ({_w/_n*100:.0f}%)"]
    L += ["", "\u2501"*16,
        "\u26A0\uFE0F <b>Это РАДАР, не команда входа.</b> Ранний сигнал ловит начало движения "
        "ДО подтверждения деньгами \u2014 выше шанс ложного. Вход \u2014 на часовом подтверждении/ретесте. "
        "Размер пробный, стоп обязателен."]
    return "\n".join(L)

def card_early(m, zone_hi, zone_lo):
    """Ранний сигнал: пробой сжатия ДО подтверждения деньгами. Честно помечен."""
    p=m["price"]; stop=zone_lo
    risk=(p-stop)/p*100 if p>0 else 0
    lines=[
        f"\U0001F535 <b>{m['coin']} · РАННИЙ СИГНАЛ</b> (эксперим.)",
        f"\U0001F4B5 ${p:.5g} \u2014 только что пробил зону сжатия ${zone_lo:.5g}\u2013${zone_hi:.5g}",
        "",
        "\u26A0\uFE0F <b>Это НЕ подтверждённый лонг-сетап.</b> Деньги (OI/объём) ещё "
        "НЕ подтвердили движение. Это ставка на то, что сжатие разрешится вверх \u2014 "
        "а пробой вверх и вниз почти равновероятны.",
        "",
        f"\U0001F4CA RSI {m.get('rsi',0):.0f}  \u00b7  корр. с BTC {m['cor']*100:.0f}%  \u00b7  волатильн. {m.get('atrr',1)*100:.0f}% от нормы",
        "",
        "\U0001F4CD <b>Если тестируешь:</b>",
        "\u2022 <b>пробный объём</b>, не полный размер (сигнал недоказан)",
        f"\u2022 стоп чуть ниже зоны сжатия: <b>${stop:.5g}</b> (риск ~{risk:.1f}%)",
        "\u2022 если через 1-2ч придёт обычная \U0001F7E2 ЛОНГ-карточка по этой монете \u2014 "
        "это деньги подтвердили движение, можно усиливать",
    ]
    _n,_w=track_record("early")
    if _n>0:
        lines += ["", f"\U0001F4C8 Трек-рекорд РАННИХ: измерено {_n}, в плюсе через 24ч {_w} ({_w/_n*100:.0f}%)"]
    lines += ["", "\u2501"*16,
        "\u26A0\uFE0F Экспериментальный сигнал на проверке. Пойдёт ли вверх \u2014 НЕ гарантия. "
        "Стоп обязателен, размер пробный."]
    return "\n".join(lines)

SIGNAL_CHAIN_HOURS=12   # окно, в котором сигналы считаются одной цепочкой развития

def signal_chain(coin, exclude_type):
    """Ищет недавние сигналы ДРУГИХ типов по этой монете за SIGNAL_CHAIN_HOURS.
    Возвращает список (тип, часов_назад) — цепочку развития движения.
    Идея: reversal -> early15 -> long по одной монете = разворот перешёл в тренд,
    сильное подтверждение."""
    if not os.path.exists(SIGNALS_FILE): return []
    now=dt.datetime.now(); out=[]
    try:
        with open(SIGNALS_FILE) as f:
            for r in csv.DictReader(f):
                if r.get("coin")!=coin: continue
                if r.get("type")==exclude_type: continue
                ts=dt.datetime.fromisoformat(r["ts"])
                hrs=(now-ts).total_seconds()/3600
                if 0<=hrs<=SIGNAL_CHAIN_HOURS:
                    out.append((r["type"], hrs))
    except Exception: return []
    # последние по каждому типу
    seen={}
    for t,h in sorted(out, key=lambda x:x[1]):
        if t not in seen: seen[t]=h
    return [(t,h) for t,h in seen.items()]

def chain_line(coin, exclude_type):
    """Строка-подсказка о цепочке сигналов для карточки (или пусто)."""
    ch=signal_chain(coin, exclude_type)
    if not ch: return None
    names={"long":"🟢 лонг","triangle":"🔺 треугольник","early":"🔵 ранний",
           "early15":"👀 ранний-15м","reversal":"🔻→🚀 разворот"}
    parts=[f"{names.get(t,t)} ({h:.0f}ч назад)" for t,h in ch]
    return "\U0001F517 <b>Цепочка по монете:</b> " + " → ".join(parts) + " → <b>сейчас</b>"

def log_signal(coin, sig_type, price):
    """Каждый отправленный сигнал (long / triangle) фиксируется с ценой и
    временем — без этого невозможно посчитать реальный форвардный win rate
    и понять, работают ли пороги фильтров или это подгонка. См. /stats."""
    try:
        new=not os.path.exists(SIGNALS_FILE)
        with open(SIGNALS_FILE,"a",newline="") as f:
            w=csv.writer(f)
            if new: w.writerow(["ts","coin","type","price","btc_price"])
            btcp=""
            try:
                bp=bybit_price("BTC")
                if bp: btcp=f"{bp:.2f}"
            except Exception: pass
            w.writerow([dt.datetime.now().isoformat(timespec="seconds"), coin, sig_type, f"{price:.6g}", btcp])
    except Exception as e:
        print("log_signal:",e)

def btc_block_stats():
    """Форвардная проверка САМОГО рубильника: для каждой блокировки смотрим,
    что BTC сделал через 4ч/24ч после неё. Если после блокировок BTC в среднем
    падал — порог '2 из 6' адекватен (защита сработала). Если рос — порог ложный,
    надо поднимать до 3. Это проверяет, не глушим ли мы сигналы зря."""
    if not os.path.exists(BLOCKS_FILE):
        return "Блокировок ещё не было — рубильник BTC пока не срабатывал."
    rows=[]
    try:
        with open(BLOCKS_FILE) as f:
            for r in csv.DictReader(f): rows.append(r)
    except Exception: return "Не удалось прочитать журнал блокировок."
    if not rows: return "Блокировок ещё не было."
    now=dt.datetime.now()
    btc_now=None
    try: btc_now=bybit_price("BTC")
    except Exception: pass
    out=["\U0001F6D1 ПРОВЕРКА BTC-РУБИЛЬНИКА (была ли блокировка оправдана)\n",
         f"Всего блокировок: {len(rows)}"]
    for horizon_h,label in [(4,"4ч"),(24,"24ч")]:
        moves=[]
        for r in rows:
            ts=dt.datetime.fromisoformat(r["ts"])
            if (now-ts).total_seconds()/3600 < horizon_h: continue
            bp0=r.get("btc_price","")
            if bp0 and btc_now:
                try: moves.append(btc_now/float(bp0)-1)
                except Exception: pass
        if not moves: continue
        n=len(moves); avg=sum(moves)/n*100
        fell=sum(1 for x in moves if x<0)
        verdict=("\u2705 в среднем BTC падал — блокировки оправданы" if avg<0
                 else "\u26A0\uFE0F BTC в среднем РОС после блокировок — порог, возможно, слишком чувствительный")
        out.append(f"Через {label}: n={n}, BTC в среднем {avg:+.2f}%, падал в {fell}/{n} случаях\n   {verdict}")
    if len(out)==2:
        out.append("Пока нет блокировок старше 4ч для проверки — подожди.")
    out.append("\n<i>Так через месяц будет видно: порог '2 из 6' адекватен или его надо поднять до 3.</i>")
    return "\n".join(out)

def price_at(symbol, target_dt):
    """Цена (close) свечи, ближайшей к моменту target_dt — то есть цена ИМЕННО
    через N часов после сигнала, а не 'сейчас'. Берём часовые свечи Bybit по времени.
    None если данных нет."""
    try:
        start_ms=int(target_dt.timestamp()*1000)
        res=bget("/v5/market/kline", {"category":"linear","symbol":symbol,
                 "interval":"60","start":start_ms,"limit":2})
        lst=res.get("list") or []
        if not lst: return None
        # Bybit отдаёт новые->старые; берём свечу, чей старт ближе всего к target
        best=min(lst, key=lambda x: abs(int(x[0])-start_ms))
        return float(best[4])   # close
    except Exception:
        return None

_price_at_cache={}
def price_at_cached(symbol, target_dt):
    key=(symbol, int(target_dt.timestamp()//3600))
    if key in _price_at_cache: return _price_at_cache[key]
    p=price_at(symbol, target_dt); _price_at_cache[key]=p; return p

def compute_stats():
    """Форвардная статистика с БЕНЧМАРКОМ против BTC. Для каждого сигнала считаем
    его % через 4ч/24ч И сравниваем со следующим: а сколько дал бы за то же время
    просто BTC ('купил биток и ничего не делал'). Если сигналы НЕ обгоняют биток —
    это не edge, а иллюзия. Кэшируем цены, чтобы не дёргать API по кругу."""
    if not os.path.exists(SIGNALS_FILE):
        return "Журнал сигналов пуст \u2014 статистики пока нет."
    rows=[]
    with open(SIGNALS_FILE) as f:
        for r in csv.DictReader(f): rows.append(r)
    if not rows:
        return "Журнал сигналов пуст \u2014 статистики пока нет."
    now=dt.datetime.now()
    price_cache={}
    def cur_price(coin):
        if coin in price_cache: return price_cache[coin]
        p=None
        try: p=bybit_price(coin)
        except Exception: pass
        price_cache[coin]=p; return p
    btc_now=cur_price("BTC")
    out=["\U0001F4CA СТАТИСТИКА ПО СИГНАЛАМ (форвард + бенчмарк BTC)\n"]
    for horizon_h, label in [(4,"4ч"),(24,"24ч")]:
        for sig_type in ("long","triangle","early","early15","reversal"):
            sig_pcts=[]; btc_pcts=[]
            for r in rows:
                if r["type"]!=sig_type: continue
                ts=dt.datetime.fromisoformat(r["ts"])
                if (now-ts).total_seconds()/3600 < horizon_h: continue
                target=ts+dt.timedelta(hours=horizon_h)          # ИМЕННО через N часов
                sym=r["coin"] if r["coin"].endswith("USDT") else r["coin"]+"USDT"
                fwd=price_at_cached(sym, target)                 # цена через N часов
                if fwd is None: continue
                entry=float(r["price"])
                sig_pcts.append(fwd/entry-1)
                # бенчмарк: что дал бы BTC за ТОТ ЖЕ период (через N часов)
                bp0=r.get("btc_price","")
                if bp0:
                    btc_fwd=price_at_cached("BTCUSDT", target)
                    if btc_fwd:
                        try: btc_pcts.append(btc_fwd/float(bp0)-1)
                        except Exception: pass
            if not sig_pcts: continue
            n=len(sig_pcts); wins=sum(1 for x in sig_pcts if x>0)
            avg=sum(sig_pcts)/n*100
            line=f"{sig_type.upper()} @ {label}: n={n}, win {wins/n*100:.0f}%, средний {avg:+.2f}%"
            if btc_pcts:
                bavg=sum(btc_pcts)/len(btc_pcts)*100
                edge=avg-bavg
                verdict="\u2705 обгоняет BTC" if edge>0 else "\u274C ХУЖЕ, чем просто держать BTC"
                line+=f"\n   BTC за тот же период: {bavg:+.2f}% \u2192 твой edge: {edge:+.2f}% {verdict}"
            out.append(line)
    if len(out)==1:
        out.append("Пока нет сигналов старше 4ч \u2014 подожди накопления.")
    out.append("\n\u26A0\uFE0F Малая выборка (n<30-50) НЕ значима. Смотри на edge против BTC, "
        "а не на голый win rate: обгонять 'просто держать биток' \u2014 вот реальная планка.")
    return "\n".join(out)

# ---------- сопровождение позиции ----------
def position_status(coin):
    p=POSITIONS.get(coin)
    if not p: return None,None
    try:
        closes,_,_,_=klines(p["sym"],limit=80); time.sleep(0.15)
        oic=open_interest(p["sym"],limit=10); time.sleep(0.15)
    except Exception: return None,None
    if len(closes)<55 or len(oic)<6: return None,None
    price=closes[-1]; pnl=price/p["entry"]-1
    oi1=oic[-1]/oic[-2]-1 if oic[-2]>0 else 0
    oi4=oic[-1]/oic[-5]-1 if oic[-5]>0 else 0
    e50=ema(closes[-60:],50)
    reasons=[]
    if oi1<=-0.03: reasons.append(f"OI резко вниз ({oi1*100:+.0f}% за 1ч) — деньги выходят")
    if price<e50: reasons.append("цена ушла ниже EMA50")
    if reasons:
        msg=(f"\U0001F6A8 {coin}: похоже на РАЗВОРОТ — пора решать!\n"
            f"P&L: {pnl*100:+.2f}% (вход ${p['entry']:.5g} → ${price:.5g})\n"
            + "; ".join(reasons)+".\n"
            "Если на бирже стоит стоп — он сработает сам. Решение твоё.")
        return "reversal",msg
    msg=(f"\U0001F7E2 {coin}: держится\n"
        f"P&L: {pnl*100:+.2f}% (вход ${p['entry']:.5g} → ${price:.5g})\n"
        f"Деньги ещё заходят (OI 4ч {oi4*100:+.0f}%), цена выше EMA50. Моментум цел.")
    return "ok",msg

def pos_buttons(coin): return [[{"text":"\u274C Выйти / зафиксировать","callback_data":f"exit|{coin}"}]]

def close_trade(coin):
    p=POSITIONS.pop(coin,None)
    if not p: return None
    try:
        cc,_,_,_=klines(p["sym"],limit=2); price=cc[-1]
    except Exception: price=p["entry"]
    pnl=price/p["entry"]-1
    # кулдаун после стоп-лосса: запоминаем убыточную сделку по монете
    if pnl<0:
        RECENT_LOSSES[coin]=time.time()
    new=not os.path.exists(TRADES)
    with open(TRADES,"a") as f:
        if new: f.write("entry_ts,coin,entry_price,exit_ts,exit_price,pnl_pct\n")
        f.write(f"{p['ts']},{coin},{p['entry']:.6g},"
            f"{dt.datetime.now().isoformat(timespec='seconds')},{price:.6g},{pnl*100:.2f}\n")
    return pnl,p["entry"],price

def enrich(sym):
    """Доп.данные с Bybit: funding. Ликвидаций в публичном REST нет — поле
    не заводим, чтобы не создавать иллюзию несуществующей проверки."""
    out={}
    try:
        t=ticker_info(sym)
        if t:
            out["funding"]=t["funding"]
            out["spread"]=t.get("spread",0.0)
    except Exception: pass
    return out

# ---------- скан ----------
def night_mult():
    """Множитель порога объёма в тихие часы UTC (ночь 0-6 UTC + выходные) —
    тогда база объёма ниже, RVOL завышен, поэтому требуем выше порог."""
    import datetime as _dt
    u=_dt.datetime.now(_dt.timezone.utc)   # timezone-aware (utcnow() устарел в 3.12+)
    quiet = u.hour<6 or u.weekday()>=5   # ночь UTC или сб/вс
    return NIGHT_VOL_MULT if quiet else 1.0

def btc_short_risk():
    """Зеркальный шортовый набор по самому BTC. Возвращает (hits, reasons, price, cor_note).
    Если hits>=BTC_RISK_MIN_HITS — риск серьёзной коррекции подтверждён."""
    try:
        closes,highs,lows,vols=klines("BTCUSDT",limit=120); time.sleep(0.12)
        oic=open_interest("BTCUSDT",limit=30); time.sleep(0.12)
    except Exception:
        return 0,[],None
    if len(closes)<60 or len(oic)<6: return 0,[],(closes[-1] if closes else None)
    price=closes[-1]
    p1=closes[-1]/closes[-2]-1
    p4=closes[-1]/closes[-5]-1
    oi4=oic[-1]/oic[-5]-1 if oic[-5]>0 else 0
    e21=ema(closes[-60:],21); e50=ema(closes[-60:],50)
    vr=sum(vols[-4:]); vb=(sum(vols[-28:-4])/24*4) if len(vols)>=28 else vr
    vspike=vr/vb if vb>0 else 0
    r=rsi(closes[-40:],14)
    reasons=[]
    if p1<=BTC_DUMP_1H:      reasons.append(f"обвал за 1ч {p1*100:+.1f}%")
    if p4<=BTC_DROP_4H:      reasons.append(f"падение за 4ч {p4*100:+.1f}%")
    if oi4<=BTC_OI_DROP_4H:  reasons.append(f"отток OI {oi4*100:+.0f}% (деньги уходят)")
    if price<e50 and e21<e50: reasons.append("тренд вниз (ниже EMA50)")
    if vspike>=BTC_VOL_SPIKE and p4<0: reasons.append(f"растущий объём на падении (×{vspike:.1f})")
    if r<BTC_RSI_OVERSOLD:   reasons.append(f"RSI перепродан ({r:.0f})")
    return len(reasons),reasons,price

def run_scan(cid, announce=False):
    if announce: tg_send(cid,"\U0001F50D Ищу лонг-сетапы, подожди пару минут...")
    try: coins=universe()
    except Exception as e: tg_send(cid,f"Ошибка данных: {e}"); return
    try:
        btc,_,_,_=klines("BTCUSDT",limit=30); time.sleep(0.12)
        btc_dump=len(btc)>2 and (btc[-1]/btc[-2]-1)<BTC_DUMP_1H
        btc_p4=btc[-1]/btc[-5]-1 if len(btc)>=5 else 0.0
    except Exception: btc=[]; btc_dump=False; btc_p4=0.0

    # ЗЕРКАЛЬНЫЙ ШОРТОВЫЙ ФИЛЬТР BTC — рубильник лонгов
    global LAST_BTC_WARN
    btc_hits,btc_reasons,btc_price=btc_short_risk()
    if btc_hits>=BTC_RISK_MIN_HITS:
        msg=("\U0001F6D1 <b>Сигналы приостановлены: БИТКОИН по шортовым фильтрам "
             "готов к серьёзной коррекции</b>\n\n"
             + "\n".join("\u2022 "+r for r in btc_reasons)
             + (f"\n\nЦена BTC: <b>${btc_price:,.0f}</b>" if btc_price else "")
             + "\n\n<i>Это РЕАКТИВНАЯ защита (по факту, не прогноз): BTC уже показывает "
               "медвежьи признаки. Не предсказываю обвал — реагирую на то, что уже происходит. "
               "Пока BTC валится, лонги по альтам опасны, жду стабилизации.</i>")
        # ручной /scan — всегда; автоскан — не чаще раза в 30 мин
        if announce or time.time()-LAST_BTC_WARN>30*60:
            tg_send(cid,msg); LAST_BTC_WARN=time.time()
        try:
            new=not os.path.exists(BLOCKS_FILE)
            with open(BLOCKS_FILE,"a",newline="") as bf:
                wb=csv.writer(bf)
                if new: wb.writerow(["ts","btc_price","hits","reasons"])
                wb.writerow([dt.datetime.now().isoformat(timespec="seconds"),
                             f"{btc_price:.2f}" if btc_price else "", btc_hits, "; ".join(btc_reasons)])
        except Exception as e: print("blocks:",e)
        return
    # 1 признак — мягкое предупреждение в карточки коррелированных монет
    btc_weak = btc_reasons[0] if btc_hits==1 else None

    shown=0
    now=time.time()
    for coin,sym in coins:
        if shown>=MAX_ALERTS: break
        try:
            closes,highs,lows,vols=klines(sym,limit=200); time.sleep(0.15)
            oic=open_interest(sym,limit=50); time.sleep(0.15)
        except Exception:
            continue
        if len(closes)<MIN_BARS: continue
        # отсекаем свежие листинги (< полугода) — только для монет, прошедших предфильтр
        # проверяем возраст 1 раз и кэшируем в SYM_CACHE-подобном словаре
        if coin not in globals().setdefault("_age_ok",{}):
            globals()["_age_ok"][coin] = coin_age_days(sym)>=MIN_AGE_DAYS
            time.sleep(0.1)
        if not globals()["_age_ok"][coin]: continue
        # мультитаймфрейм-статус треугольника (справка): 15м / 1ч / 4ч
        tri_mtf={"1ч": detect_triangle(highs,lows,closes,closes[-1])[0]}
        try:
            c15,h15,l15,v15=klines_tf(sym,"15",limit=120); time.sleep(0.12)
            tri_mtf["15м"]=detect_triangle(h15,l15,c15,c15[-1])[0]
        except Exception: tri_mtf["15м"]=None; c15=h15=l15=v15=None
        try:
            c4,h4,l4,_=klines_tf(sym,"240",limit=120); time.sleep(0.12)
            tri_mtf["4ч"]=detect_triangle(h4,l4,c4,c4[-1])[0]
        except Exception: tri_mtf["4ч"]=None
        ti=ticker_info(sym)
        turn24=ti["turnover"] if ti else None
        m=core(coin,closes,highs,lows,vols,oic,btc,btc_p4,tri_mtf=tri_mtf,turn24=turn24)
        if m: m["btc_weak"]=btc_weak
        if not m: continue
        SYM_CACHE[coin]=sym

        # доп.данные нужны и ранним сигналам, и основному — считаем ЗДЕСЬ, до сигналов
        ex=enrich(sym)
        m["ls_ratio"]=long_short_ratio(sym); time.sleep(0.1)
        by=bybit_price(coin)
        if by: m["bybit"]=by

        # === СТАДИЯ 1: РАННЕЕ ОБНАРУЖЕНИЕ НА 15м (движение началось, час подтвердит позже) ===
        if EARLY15_ENABLED and c15 and v15:
            try:
                started, mv, rvol, e_slope, e_3green = detect_early_15m(c15,h15,l15,v15)
            except Exception:
                started, mv, rvol, e_slope, e_3green = False,0,0,0,False
            if (started and m.get("rsi",100)<=EARLY_RSI_MAX and m["dd"]>KNIFE_DD
                    and m.get("atrr",1.0)>=ATR_MIN_RATIO and btc_hits<BTC_RISK_MIN_HITS
                    and abs(ex.get("funding",0))<FUNDING_CUTOFF
                    and m["turn"]>=THIN_TURN):
                le15=LAST_EARLY15.get(coin,0)
                if now-le15>=EARLY15_COOLDOWN_H*3600:
                    LAST_EARLY15[coin]=now
                    # взять на ЖИВОЕ отслеживание развития (объём/лонгисты/funding)
                    v0=sum(v15[-4:])/4
                    EARLY_WATCH[coin]=dict(sym=sym, ts=now, v0=v0,
                        lsr0=m.get("ls_ratio"), fund0=ex.get("funding",0), price0=m["price"])
                    tg_send(cid, card_early15(m, ex, mv, rvol, e_slope, e_3green, btc_hits))
                    shown+=1
                    log_signal(coin, "early15", m["price"])

        # === РАЗВОРОТНЫЙ СИГНАЛ (контр-тренд, эксперим., отдельная статистика) ===
        if REVERSAL_ENABLED:
            try:
                rev_ok, rev_d = reversal_setup(closes,highs,lows,vols,oic)
            except Exception:
                rev_ok, rev_d = False,{}
            # фильтры безопасности: ликвидность, BTC не валится, funding норм, не в чопе
            if (rev_ok and m["turn"]>=THIN_TURN and btc_hits<BTC_RISK_MIN_HITS
                    and abs(ex.get("funding",0))<FUNDING_CUTOFF and m.get("atrr",1.0)>=ATR_MIN_RATIO):
                lr=LAST_REVERSAL.get(coin,0)
                if now-lr>=REVERSAL_COOLDOWN_H*3600:
                    LAST_REVERSAL[coin]=now
                    # НЕ шлём сразу: ждём закрытия следующей свечи выше триггера (защита от ложного спайка)
                    WATCH[coin]=dict(ts=now, sym=sym, kind="reversal_pending",
                        trigger_price=m["price"], stop_price=rev_d.get("recent_low", m["price"]),
                        details=rev_d, m_snapshot=dict(m), ex_snapshot=dict(ex))

        # === РАННИЙ СИГНАЛ (эксперим.): пробой сжатия ДО подтверждения деньгами ===
        if EARLY_ENABLED:
            try:
                broke, zhi, zlo = detect_compression(closes,highs,lows,vols)
            except Exception:
                broke, zhi, zlo = False,0,0
            # строгие доп. условия: RSI не перегрет, не падающий нож, не в чопе, BTC не валится
            # ВОРОТА "ПОЗДНО": не зовём, если движение уже выдохлось (как в long_ok)
            too_late_e = m.get("ext",0)>MAX_EXT_ENTRY or m["p4"]>MAX_MOVE_4H
            if (broke and m.get("rsi",100)<=EARLY_RSI_MAX and m["dd"]>KNIFE_DD
                    and m.get("atrr",1.0)>=ATR_MIN_RATIO and btc_hits<BTC_RISK_MIN_HITS
                    and abs(ex.get("funding",0))<FUNDING_CUTOFF
                    and m["turn"]>=THIN_TURN
                    and not too_late_e):
                le=LAST_EARLY.get(coin,0)
                if now-le>=EARLY_COOLDOWN_H*3600:
                    LAST_EARLY[coin]=now
                    bt=[[{"text":"\u2705 Я вошёл","callback_data":f"enter|{coin}|{m['price']:.6g}"}]]
                    tg_send(cid, card_early(m,zhi,zlo), buttons=bt); shown+=1
                    log_signal(coin, "early", m["price"])

        if not long_ok(m): continue
        last=LAST_ALERT.get(coin,0)
        # базовый кулдаун; если по монете недавно был стоп-лосс — кулдаун длиннее
        cd=COOLDOWN_H*3600
        loss_ts=RECENT_LOSSES.get(coin,0)
        if loss_ts and now-loss_ts < COOLDOWN_H*LOSS_COOLDOWN_MULT*3600:
            cd=COOLDOWN_H*LOSS_COOLDOWN_MULT*3600
        if now-last<cd: continue

        # FUNDING-CUTOFF: экстремальный funding = перегрев лонгами перед каскадом. Полный отказ.
        if abs(ex.get("funding",0))>=FUNDING_CUTOFF:
            continue
        # СПРЕД: очень широкий (>2x порога) = слишком тонкий стакан, блокируем (по нашей логике)
        if ex.get("spread",0) >= SPREAD_MAX*2:
            continue

        lv=m.get("lvl")
        if lv and abs(lv["dist"])<0.03 and lv["touches"]>=3:
            base=lv["price"]
            WATCH[m["coin"]]=dict(sym=sym, zone_hi=base*1.008, zone_lo=base*0.99,
                ts=time.time(), price0=m["price"], kind=f"сильному уровню ${base:.5g}")
            m["watching"]=(base*0.99, base*1.008)
            m["watch_kind"]=f"уровню ${base:.5g} ({lv['touches']} касаний)"
        elif m.get("tri")=="breakout" and m.get("tri_top",0)>0:
            top=m["tri_top"]
            WATCH[m["coin"]]=dict(sym=sym, zone_hi=top*1.004, zone_lo=top*0.985,
                ts=time.time(), price0=m["price"], kind="пробой треугольника")
            m["watching"]=(top*0.985, top*1.004)
            m["watch_kind"]="крышке треугольника"
        else:
            zone_hi=m.get("e21",m["price"]); zone_lo=m.get("consol_base",m["price"])
            if m.get("extended") and zone_hi>0:
                WATCH[m["coin"]]=dict(sym=sym, zone_hi=zone_hi, zone_lo=zone_lo,
                    ts=time.time(), price0=m["price"], kind="откат к зоне")
                m["watching"]=(zone_lo,zone_hi)
                m["watch_kind"]="зоне отката"

        btn=[[{"text":"\u2705 Я вошёл","callback_data":f"enter|{m['coin']}|{m['price']:.6g}"}]]

        # Сигнал 1: чистый лонг-сетап
        tg_send(cid, card_long(m,ex), buttons=btn); shown+=1
        log_signal(m["coin"], "long", m["price"])

        # Сигнал 2 (отдельный): лонг-сетап + треугольник (ready или breakout)
        tri_card = card_triangle(m,ex)
        if tri_card:
            tg_send(cid, tri_card, buttons=btn); shown+=1
            log_signal(m["coin"], "triangle", m["price"])

        # Если стадия "ready" — ставим монету на АКТИВНОЕ ожидание пробоя.
        # Это отдельный от антиспама механизм: даже если карточка лонг-сетапа
        # не придёт повторно 4 часа (COOLDOWN_H), пробой крышки треугольника
        # всё равно прилетит мгновенно отдельным уведомлением (проверка раз в 15с).
        if m.get("tri")=="ready" and m.get("tri_top",0)>0 and m["coin"] not in TRI_ALERT:
            TRI_ALERT[m["coin"]]=dict(sym=sym, top=m["tri_top"], ts=time.time())

        LAST_ALERT[coin]=now

    if shown==0 and announce:
        tg_send(cid,"Сейчас чистых сетапов не найдено. Бот продолжает сканировать автоматически \u2014 пришлю, как только появится.")

def check_early_watch(chat):
    """Живое отслеживание монет после 15м-всплеска: растёт ли объём, что с лонгистами
    и funding. Сигналит о РАЗВИТИИ движения (усиливается/выдыхается)."""
    if not chat or not EARLY_WATCH: return
    now=time.time()
    for coin in list(EARLY_WATCH):
        w=EARLY_WATCH[coin]
        if now-w["ts"]>EARLY_WATCH_HOURS*3600:
            del EARLY_WATCH[coin]; continue
        try:
            c15,h15,l15,v15=klines_tf(w["sym"],"15",limit=20); time.sleep(0.12)
        except Exception:
            continue
        if len(v15)<4: continue
        v_now=sum(v15[-4:])/4
        vol_growth=v_now/w["v0"] if w["v0"]>0 else 1
        price=c15[-1]; move=price/w["price0"]-1
        # развалилось (ушло в минус от старта) — снять
        if move<-0.03:
            del EARLY_WATCH[coin]; continue
        # сигналим только на ЗАМЕТНОМ усилении: объём вырос ещё в 1.5х от старта
        if vol_growth>=1.5:
            lsr=None; fund=None
            try: lsr=long_short_ratio(w["sym"])
            except Exception: pass
            try:
                ti=ticker_info(w["sym"]);  fund=ti["funding"] if ti else None
            except Exception: pass
            parts=[f"\U0001F4B9 объём растёт \u00d7{vol_growth:.1f} от старта", f"цена {move*100:+.1f}%"]
            if lsr: parts.append(f"Л/Ш {lsr:.1f}")
            if fund is not None: parts.append(f"funding {fund*100:.3f}%")
            tg_send(chat,
                f"\U0001F4C8 <b>{coin}: развитие движения</b>\n"
                + " \u00b7 ".join(parts)
                + "\n<i>15м-движение усиливается. Ждём часового подтверждения деньгами (полный ЛОНГ-сигнал) для входа.</i>")
            EARLY_WATCH[coin]["v0"]=v_now      # обновить базу, чтобы не спамить
            EARLY_WATCH[coin]["ts"]=now

def check_watchlist(chat):
    if not chat or not WATCH: return
    now=time.time()
    for coin in list(WATCH):
        w=WATCH[coin]
        # --- РАЗВОРОТ: ждём ПОДТВЕРЖДЕНИЯ (закрытие след. свечи выше триггера) ---
        if w.get("kind")=="reversal_pending":
            if now-w["ts"]>REVERSAL_COOLDOWN_H*3600:
                del WATCH[coin]; continue          # не подтвердилось за N часов — сброс
            try:
                res=bget("/v5/market/kline", {"category":"linear","symbol":w["sym"],"interval":"60","limit":3})
                k=res["list"]; time.sleep(0.15)
            except Exception:
                continue
            if len(k)<1: continue
            last_closed=k[1] if len(k)>1 else k[0]  # k[0]=текущая незакрытая, k[1]=последняя закрытая
            close_price=float(last_closed[4])
            if close_price<=w["stop_price"]:
                del WATCH[coin]; continue            # ушла ниже стопа — разворот не состоялся
            if close_price>w["trigger_price"]:       # ПОДТВЕРЖДЕНО закрытием выше
                mm=w["m_snapshot"]; mm["price"]=close_price
                tg_send(chat, card_reversal(mm, w["ex_snapshot"], w["details"]),
                    buttons=[[{"text":"\u2705 Я вошёл","callback_data":f"enter|{coin}|{close_price:.6g}"}]])
                log_signal(coin, "reversal", close_price)
                del WATCH[coin]
            continue                                 # reversal_pending обработан, дальше не идём
        # --- обычная retest-логика (треугольники/уровни) ---
        if now-w["ts"]>WATCH_HOURS*3600:
            del WATCH[coin]; continue
        try:
            res=bget("/v5/market/kline", {"category":"linear","symbol":w["sym"],"interval":"60","limit":3})
            k=res["list"]; time.sleep(0.15)
        except Exception:
            continue
        if len(k)<1: continue
        last=k[0]; o=float(last[1]); c=float(last[4]); lo=float(last[3])
        touched = lo <= w["zone_hi"]
        bounced = c >= o
        if c < w["zone_lo"]*0.97:
            del WATCH[coin]; continue
        if touched and (bounced or not RETEST_NEED_BOUNCE):
            by=bybit_price(coin)
            byline = f"\nBybit: ${by:.5g}" if by else ""
            kind=w.get("kind","зоне")
            tg_send(chat,
                f"\U0001F3AF {coin}: РЕТЕСТ ({kind})!\n"
                f"Цена вернулась к ${w['zone_lo']:.5g}\u2013${w['zone_hi']:.5g} и отбивается (зелёная свеча).\n"
                f"Сейчас: ${c:.5g}{byline}\n"
                f"Вот безопасная точка входа, которую ты ждал. Проверь глазами, стоп обязателен.",
                buttons=[[{"text":"\u2705 Я вошёл","callback_data":f"enter|{coin}|{c:.6g}"}]])
            del WATCH[coin]

# ---------- чат ----------
def ensure_dirs():
    """Создаёт директории для журналов, если их нет (нужно для volume /data)."""
    for path in (TRADES, SIGNALS_FILE, CHAT_FILE, BLOCKS_FILE):
        d=os.path.dirname(path)
        if d and not os.path.exists(d):
            try: os.makedirs(d, exist_ok=True)
            except Exception as e: print("mkdir",d,e)

def save_chat(c):
    try:
        with open(CHAT_FILE,"w") as f: f.write(str(c))
    except Exception as e: print("save_chat:",e)

def load_chat():
    try:
        with open(CHAT_FILE) as f: return f.read().strip()
    except: return None

def handle_callback(q):
    data=q.get("data",""); cid=str(((q.get("message") or {}).get("chat") or {}).get("id",""))
    tg_answer(q.get("id",""))
    if not cid: return
    parts=data.split("|")
    if parts[0]=="enter" and len(parts)>=3:
        coin=parts[1]; price=float(parts[2]); sym=SYM_CACHE.get(coin)
        if not sym: tg_send(cid,f"Не могу найти {coin} для ведения. Сделай /scan заново."); return
        POSITIONS[coin]=dict(entry=price,ts=dt.datetime.now().isoformat(timespec="seconds"),
            sym=sym,last_upd=0,last_check=0,last_state="ok")
        tg_send(cid,f"\u2705 Веду позицию {coin} от ${price:.5g}.\n"
            f"Проверяю каждые {CHECK_POS_MIN} мин. Молчу, пока всё ок — крикну ‼️ при развороте.\n\n"
            f"\u26A0\uFE0F Сразу выстави стоп-ордер на Bybit — это твоя мгновенная защита. "
            f"Бот предупредит, но от резкого пролива спасает только стоп на бирже.",
            buttons=pos_buttons(coin))
    elif parts[0]=="exit" and len(parts)>=2:
        coin=parts[1]; res=close_trade(coin)
        if not res: tg_send(cid,f"Позиции по {coin} нет."); return
        pnl,e,x=res
        emo="\U0001F7E2" if pnl>=0 else "\U0001F534"
        tg_send(cid,f"{emo} Сделка по {coin} закрыта.\n"
            f"Вход ${e:.5g} → выход ${x:.5g} = {pnl*100:+.2f}%\n"
            f"Записал в журнал. Команда /log — скачать всю историю сделок.")

def main():
    global TG_TOKEN
    TG_TOKEN=os.environ.get("TG_TOKEN","").strip() or input("Токен бота: ").strip()
    if len(TG_TOKEN)<20: print("Нет валидного TG_TOKEN."); return
    ensure_dirs()
    me=tg("getMe")
    if not me.get("ok"): print("Не подключиться — проверь TG_TOKEN."); return
    print(f"Бот @{me['result']['username']} запущен (server mode).")
    offset=None; last_scan=0; chat=load_chat()
    while True:
        try:
            for u in tg("getUpdates",offset=offset,timeout=30).get("result",[]):
                offset=u["update_id"]+1
                if "callback_query" in u: handle_callback(u["callback_query"]); continue
                msg=u.get("message") or {}; text=(msg.get("text") or "").lower()
                cid=str((msg.get("chat") or {}).get("id",""))
                if not cid: continue
                if text.startswith("/start"):
                    chat=cid; save_chat(cid)
                    tg_send(cid,"\u2705 Сканер на сервере, работает 24/7.\n"
                        "/scan — искать лонг-сетапы\n/pos — мои позиции\n/watch — кого отслеживаю\n"
                        "/log — журнал сделок\n/stats — статистика по сигналам\n/bybit — проверка доступа к Bybit\n\n"
                        "Подсвечу сетап → нажмёшь «Я вошёл» → буду вести позицию и комментировать. Решаешь ты.")
                elif text.startswith("/scan"): run_scan(cid, announce=True)
                elif text.startswith("/pos"):
                    if POSITIONS: tg_send(cid,"Открытые: "+", ".join(POSITIONS))
                    else: tg_send(cid,"Открытых позиций нет.")
                elif text.startswith("/btcstats"):
                    tg_send(cid, btc_block_stats())
                elif text.startswith("/stats"):
                    tg_send(cid, compute_stats())
                elif text.startswith("/bybit"):
                    try:
                        r=requests.get("https://api.bybit.com/v5/market/tickers",
                            params={"category":"linear","symbol":"BTCUSDT"}, timeout=10)
                        if r.status_code==200:
                            j=r.json()
                            p=(j.get("result") or {}).get("list",[{}])[0].get("lastPrice","?")
                            tg_send(cid, f"\u2705 Bybit ДОСТУПЕН с сервера!\n"
                                f"BTC цена с Bybit: ${p}\n"
                                f"Регион EU работает \u2014 можно переходить на Bybit-данные.")
                        else:
                            tg_send(cid, f"\u274C Bybit вернул код {r.status_code} (возможно, блок региона). "
                                f"Ответ: {r.text[:200]}")
                    except Exception as e:
                        tg_send(cid, f"\u274C Bybit НЕдоступен с сервера: {type(e).__name__}. "
                            f"Похоже, регион всё ещё блокируется.")
                elif text.startswith("/watch"):
                    if WATCH:
                        rows=[]
                        for c,w in WATCH.items():
                            if w.get("kind")=="reversal_pending":
                                rows.append(f"\u2022 {c}: \U0001F53B\u2192\U0001F680 жду ПОДТВЕРЖДЕНИЯ разворота (закрытие >${w['trigger_price']:.5g})")
                            else:
                                rows.append(f"\u2022 {c}: жду ретест ${w['zone_lo']:.5g}\u2013${w['zone_hi']:.5g} ({w.get('kind','зона')})")
                        tg_send(cid,"\u23F3 На отслеживании:\n"+"\n".join(rows))
                    else:
                        tg_send(cid,"Список ожидания пуст \u2014 никого не отслеживаю.")
                elif text.startswith("/log"):
                    if os.path.exists(TRADES) and os.path.getsize(TRADES)>0:
                        n=sum(1 for _ in open(TRADES))-1
                        tg_send_doc(cid,TRADES,f"Журнал сделок: {n}. Сохрани — на сервере файл сбрасывается при передеплое.")
                    else: tg_send(cid,"Журнал пуст — ещё не было закрытых сделок.")

            if chat and time.time()-last_scan>SCAN_EVERY_MIN*60:
                print(f'[scan] авто-скан {MAX_COINS} монет, chat={"есть" if chat else "НЕТ /start"}')
                run_scan(chat, announce=False); last_scan=time.time()

            if time.time()-globals().get('_last_ew',0) > EARLY_WATCH_CHECK_SEC:
                globals()['_last_ew']=time.time()
                try: check_early_watch(chat)
                except Exception as e: print('ewatch:',e)
            if time.time()-globals().get('_last_watch',0) > WATCH_CHECK_SEC:
                globals()['_last_watch']=time.time()
                try: check_watchlist(chat)
                except Exception as e: print('watch:',e)

            for coin in list(POSITIONS):
                p=POSITIONS[coin]; now=time.time()
                if now-p["last_check"]<CHECK_POS_MIN*60: continue
                p["last_check"]=now
                state,m=position_status(coin)
                if not state: continue
                if state=="reversal" and p.get("last_state")!="reversal":
                    tg_send(chat,m,buttons=pos_buttons(coin)); p["last_upd"]=now
                elif state=="ok" and now-p.get("last_upd",0)>CALM_UPDATE_MIN*60:
                    tg_send(chat,m,buttons=pos_buttons(coin)); p["last_upd"]=now
                p["last_state"]=state
            time.sleep(1)
        except Exception as e:
            print("loop:",e); time.sleep(10)

if __name__=="__main__":
    main()
