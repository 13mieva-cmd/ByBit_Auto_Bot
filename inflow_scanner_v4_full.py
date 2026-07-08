# -*- coding: utf-8 -*-
"""
СКАНЕР ВЛИВАНИЙ v4 — ЛОНГ + ТРЕУГОЛЬНИК + FOLLOW-UP + /stats2
=============================================================
Telegram-бот для Railway / VPS.
Бот НЕ торгует сам. Он:
1) ищет лонг-сетапы (OI↑ + объём↑ + тренд вверх);
2) отдельно шлёт карточку треугольника, если есть triangle ready/breakout;
3) ведёт позицию по кнопке «Я вошёл»;
4) логирует каждый сигнал в SIGNALS_FILE;
5) считает форвардную статистику по сигналам через /stats и /stats2.

Текущая версия добавляет:
- аккуратный полный файл без обрывов;
- /stats2 с expectancy, PF, median, edge vs BTC;
- более строгую и безопасную структуру кода;
- сохранение журналов на диск.
"""

import os
import csv
import json
import time
import math
import datetime as dt
from typing import List, Dict, Tuple, Optional

import numpy as np
import requests

BYBIT = "https://api.bybit.com"
QUOTE = "USDT"
MAX_COINS = 300
SCAN_EVERY_MIN = 5
MAX_ALERTS = 8
CHECK_POS_MIN = 2
CALM_UPDATE_MIN = 30

OI_4H_MIN = 0.05
VOL_SPIKE_MIN = 1.5
KNIFE_DD = -0.40
THIN_TURN = 5_000_000
BTC_DUMP_1H = -0.02
HI_CORR = 0.8
BTC_DROP_4H = -0.02
BTC_OI_DROP_4H = -0.05
BTC_VOL_SPIKE = 1.5
BTC_RSI_OVERSOLD = 30
BTC_RISK_MIN_HITS = 2
LAST_BTC_WARN = 0
PRICE_UP_4H_MIN = 0.005
RSI_MAX = 78
MIN_BARS = 200
COOLDOWN_H = 4
FUNDING_CUTOFF = 0.0005
ATR_MIN_RATIO = 0.6
EARLY_ENABLED = True
EARLY_COMPRESS_MAX = 0.7
EARLY_VOL_MIN = 1.2
EARLY_RSI_MAX = 68
EARLY_COOLDOWN_H = 4
LOSS_COOLDOWN_MULT = 3
WATCH_HOURS = 12
WATCH_CHECK_SEC = 15
RETEST_NEED_BOUNCE = True
TRI_ALERT_HOURS = 6

TRADES = os.environ.get("TRADES_FILE", "/tmp/scanner_trades.csv")
CHAT_FILE = os.environ.get("CHAT_FILE", "/tmp/scanner_chat.txt")
SIGNALS_FILE = os.environ.get("SIGNALS_FILE", "/tmp/scanner_signals.csv")
BLOCKS_FILE = os.environ.get("BLOCKS_FILE", "/data/scanner_blocks.csv")
TG_TOKEN = os.environ.get("TG_TOKEN", "").strip()

SYM_CACHE = {}
POSITIONS = {}
LAST_ALERT = {}
WATCH = {}
TRI_ALERT = {}
LAST_EARLY = {}
RECENT_LOSSES = {}
_track_cache = {"ts": 0, "data": {}}
_tickers_cache = {"ts": 0, "data": []}
LAST_BTC_WARN = 0

# ---------- Telegram ----------
def tg(method, **p):
    try:
        return requests.get(f"https://api.telegram.org/bot{TG_TOKEN}/{method}", params=p, timeout=40).json()
    except requests.exceptions.ReadTimeout:
        return {}
    except Exception as e:
        print("TG:", e)
        return {}


def kb(rows):
    return json.dumps({"inline_keyboard": rows}, ensure_ascii=False)


def tg_send(cid, text, buttons=None):
    p = {"chat_id": cid, "text": text, "parse_mode": "HTML"}
    if buttons:
        p["reply_markup"] = kb(buttons)
    tg("sendMessage", **p)


def tg_answer(qid, text=""):
    tg("answerCallbackQuery", callback_query_id=qid, text=text)


def tg_send_doc(cid, path, caption=""):
    try:
        with open(path, "rb") as f:
            requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendDocument",
                data={"chat_id": cid, "caption": caption},
                files={"document": f},
                timeout=60,
            )
    except Exception as e:
        print("doc:", e)


def ensure_dirs():
    for path in (TRADES, SIGNALS_FILE, CHAT_FILE, BLOCKS_FILE):
        d = os.path.dirname(path)
        if d and not os.path.exists(d):
            os.makedirs(d, exist_ok=True)


def save_chat(c):
    try:
        with open(CHAT_FILE, "w") as f:
            f.write(str(c))
    except Exception as e:
        print("save_chat:", e)


def load_chat():
    try:
        with open(CHAT_FILE) as f:
            return f.read().strip()
    except Exception:
        return None

# ---------- Bybit ----------
def bget(path, params):
    r = requests.get(f"{BYBIT}{path}", params=params, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    j = r.json()
    if j.get("retCode") != 0:
        raise RuntimeError(f"Bybit retCode {j.get('retCode')}: {j.get('retMsg')}")
    return j["result"]


def bybit_price(coin):
    try:
        r = requests.get(
            f"{BYBIT}/v5/market/tickers",
            params={"category": "linear", "symbol": coin + "USDT"},
            timeout=8,
        )
        if r.status_code != 200:
            return None
        j = r.json()
        lst = (j.get("result") or {}).get("list") or []
        return float(lst[0]["lastPrice"]) if lst else None
    except Exception:
        return None


def all_tickers():
    if time.time() - _tickers_cache["ts"] < 60 and _tickers_cache["data"]:
        return _tickers_cache["data"]
    res = bget("/v5/market/tickers", {"category": "linear"})
    _tickers_cache["data"] = res["list"]
    _tickers_cache["ts"] = time.time()
    return res["list"]


def universe():
    rows = [x for x in all_tickers() if x["symbol"].endswith("USDT")]
    rows.sort(key=lambda x: float(x.get("turnover24h", 0) or 0), reverse=True)
    seen, out = set(), []
    for x in rows:
        b = x["symbol"][:-4]
        if b and b not in seen:
            seen.add(b)
            out.append((b, x["symbol"]))
    return out[:MAX_COINS]


def klines(symbol, limit=200):
    res = bget("/v5/market/kline", {"category": "linear", "symbol": symbol, "interval": "60", "limit": limit})
    k = res["list"][::-1]
    closes = [float(x[4]) for x in k]
    highs = [float(x[2]) for x in k]
    lows = [float(x[3]) for x in k]
    vols = [float(x[5]) for x in k]
    return closes, highs, lows, vols


def open_interest(symbol, limit=50):
    res = bget("/v5/market/open-interest", {"category": "linear", "symbol": symbol, "intervalTime": "1h", "limit": limit})
    oi = res["list"][::-1]
    return [float(x["openInterest"]) for x in oi]


def long_short_ratio(symbol):
    try:
        res = bget("/v5/market/account-ratio", {"category": "linear", "symbol": symbol, "period": "1h", "limit": 1})
        lst = res.get("list") or []
        if not lst:
            return None
        b = float(lst[0].get("buyRatio", 0))
        s = float(lst[0].get("sellRatio", 0))
        if s <= 0:
            return None
        return b / s
    except Exception:
        return None


def ticker_info(symbol):
    for t in all_tickers():
        if t["symbol"] == symbol:
            return dict(
                price=float(t["lastPrice"]),
                funding=float(t.get("fundingRate", 0) or 0),
                turnover=float(t.get("turnover24h", 0) or 0),
            )
    return None

# ---------- Indicators ----------
def rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    d = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    g = [x if x > 0 else 0 for x in d]
    l = [-x if x < 0 else 0 for x in d]
    ag = sum(g[:period]) / period
    al = sum(l[:period]) / period
    for i in range(period, len(d)):
        ag = (ag * (period - 1) + g[i]) / period
        al = (al * (period - 1) + l[i]) / period
    if al == 0:
        return 100.0
    return 100 - 100 / (1 + ag / al)


def ema(v, span):
    a = 2 / (span + 1)
    e = v[0]
    for x in v[1:]:
        e = a * x + (1 - a) * e
    return e


def corr(a, b):
    n = min(len(a), len(b))
    if n < 10:
        return 0.0
    ra = np.diff(a[-n:])
    rb = np.diff(b[-n:])
    if ra.std() == 0 or rb.std() == 0:
        return 0.0
    return float(np.corrcoef(ra, rb)[0, 1])


def atr(highs, lows, closes, period=14):
    n = len(closes)
    if n <= period:
        return 0.0
    trs = []
    for i in range(1, n):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        trs.append(tr)
    if len(trs) < period:
        return 0.0
    vals = []
    cur = sum(trs[:period]) / period
    vals.append(cur)
    for x in trs[period:]:
        cur = (cur * (period - 1) + x) / period
        vals.append(cur)
    return vals[-1]


def atr_ratio(highs, lows, closes):
    a = atr(highs, lows, closes, 14)
    if len(closes) < 60:
        return 1.0
    a30 = []
    for i in range(30, len(closes)):
        a30.append(atr(highs[: i + 1], lows[: i + 1], closes[: i + 1], 14))
    avg = sum(a30[-30:]) / min(30, len(a30)) if a30 else a
    if avg <= 0:
        return 1.0
    return a / avg


def _bar(frac, n=5):
    frac = max(0.0, min(1.0, frac))
    filled = int(round(frac * n))
    return "■" * filled + "□" * (n - filled)


def _swing_points(vals, is_high, win=3):
    pts = []
    for i in range(win, len(vals) - win):
        seg = vals[i - win : i + win + 1]
        if is_high and vals[i] == max(seg):
            pts.append((i, vals[i]))
        if not is_high and vals[i] == min(seg):
            pts.append((i, vals[i]))
    return pts


def _fit_line(pts):
    if len(pts) < 2:
        return None
    xs = np.array([p[0] for p in pts], dtype=float)
    ys = np.array([p[1] for p in pts], dtype=float)
    if xs.std() == 0:
        return None
    slope, intercept = np.polyfit(xs, ys, 1)
    return float(slope), float(intercept)


def find_levels(highs, lows, closes, price, min_touches=3):
    if len(closes) < 40:
        return []
    tol = price * 0.010
    away = price * 0.015
    H = highs[-150:]
    L = lows[-150:]
    cand = []
    for i in range(2, len(H) - 2):
        if H[i] >= max(H[i - 2 : i + 3]):
            cand.append(H[i])
        if L[i] <= min(L[i - 2 : i + 3]):
            cand.append(L[i])
    levels = []
    used = [False] * len(cand)
    for i, base in enumerate(cand):
        if used[i]:
            continue
        cluster = [base]
        used[i] = True
        for j in range(i + 1, len(cand)):
            if not used[j] and abs(cand[j] - base) <= tol:
                cluster.append(cand[j])
                used[j] = True
        lvl = sum(cluster) / len(cluster)
        touches = 0
        state = "away"
        for c in closes[-150:]:
            if abs(c - lvl) <= tol and state == "away":
                touches += 1
                state = "near"
            elif abs(c - lvl) > away:
                state = "away"
        if touches >= min_touches:
            levels.append((lvl, touches))
    levels.sort(key=lambda x: -x[1])
    out = []
    for lv in levels:
        if all(abs(lv[0] - o[0]) > tol * 2 for o in out):
            out.append(lv)
    return out[:4]


def nearest_level(levels, price):
    if not levels:
        return None
    lv = min(levels, key=lambda x: abs(x[0] - price))
    return dict(price=lv[0], touches=lv[1], dist=(price - lv[0]) / price)


def liq_zones(price, funding=0.0):
    k = 0.005
    out = {}
    for L in (10, 25):
        out[f"long_{L}x"] = price * (1 - (1.0 / L) + k)
        out[f"short_{L}x"] = price * (1 + (1.0 / L) - k)
    if funding >= 0.0003:
        out["heavy"] = "long"
    elif funding <= -0.0003:
        out["heavy"] = "short"
    else:
        out["heavy"] = None
    return out


def detect_triangle(highs, lows, closes, price, win=45, swing_win=3):
    n = len(closes)
    if n < win + 5:
        return None, price, price, price
    hi_pts = _swing_points(highs[-win:], True, swing_win)
    lo_pts = _swing_points(lows[-win:], False, swing_win)
    if len(hi_pts) < 2 or len(lo_pts) < 2:
        return None, price, price, price
    r = _fit_line(hi_pts)
    s = _fit_line(lo_pts)
    if not r or not s:
        return None, price, price, price
    r_slope, r_int = r
    s_slope, s_int = s
    if not (r_slope < 0 and s_slope > 0):
        return None, price, price, price
    last_x = win - 1
    res_now = r_slope * last_x + r_int
    sup_now = s_slope * last_x + s_int
    if res_now <= sup_now:
        return None, price, price, price
    width_now = res_now - sup_now
    x0 = hi_pts[0][0]
    if x0 >= win - 5:
        return None, price, price, price
    width_0 = abs((r_slope * x0 + r_int) - (s_slope * x0 + s_int))
    contracting = width_0 > 0 and width_now < width_0 * 0.7
    if not contracting:
        return None, price, price, price
    if price > res_now:
        return "breakout", res_now, res_now, sup_now
    dist_to_res = (res_now - price) / price if price > 0 else 1
    if dist_to_res <= 0.02:
        return "ready", res_now, res_now, sup_now
    return "forming", res_now, res_now, sup_now


def early_breakout(closes, highs, lows, vols):
    if len(closes) < 40:
        return False, None, None
    zone_hi = max(highs[-15:-1])
    zone_lo = min(lows[-15:-1])
    median_range = np.median([h - l for h, l in zip(highs[-25:-1], lows[-25:-1])]) if len(highs) >= 25 else zone_hi - zone_lo
    cur_range = highs[-1] - lows[-1]
    compressed = median_range > 0 and cur_range < median_range * EARLY_COMPRESS_MAX
    broke_up = closes[-1] > zone_hi
    vb = sum(vols[-25:-1]) / max(1, len(vols[-25:-1])) if len(vols) >= 25 else sum(vols) / max(1, len(vols))
    vol_ok = vols[-1] >= vb * EARLY_VOL_MIN if vb > 0 else False
    return (compressed and broke_up and vol_ok), zone_hi, zone_lo


def core(coin, closes, highs, lows, vols, oic, btc, btc_p4=0.0, tri_mtf=None):
    price = closes[-1]
    p4 = closes[-1] / closes[-5] - 1 if len(closes) >= 5 else 0
    oi1 = oic[-1] / oic[-2] - 1 if len(oic) > 1 and oic[-2] > 0 else 0
    oi4 = oic[-1] / oic[-5] - 1 if len(oic) > 4 and oic[-5] > 0 else 0
    oi24 = oic[-1] / oic[-25] - 1 if len(oic) > 24 and oic[-25] > 0 else 0
    vr = sum(vols[-4:])
    vb = (sum(vols[-28:-4]) / 24 * 4) if len(vols) >= 28 else vr
    spike = vr / vb if vb > 0 else 0
    e21 = ema(closes[-60:], 21)
    e50 = ema(closes[-60:], 50)
    uptrend = price > e50 and e21 > e50
    ext = (price - e21) / e21 if e21 > 0 else 0
    consol_base = min(lows[-8:]) if len(lows) >= 8 else min(lows)
    old_high = max(highs[-72:-4]) if len(highs) > 76 else max(highs[:-4] or highs)
    extended = ext > 0.05
    levels = find_levels(highs, lows, closes, price, min_touches=3)
    lvl = nearest_level(levels, price)
    tri, tri_top, tri_res_now, tri_sup_now = detect_triangle(highs, lows, closes, price)
    flag = None
    flag_top = price
    if len(closes) >= 30:
        imp = closes[-15] / closes[-25] - 1
        pull = closes[-1] / closes[-15] - 1
        pull_range = (max(highs[-12:]) - min(lows[-12:])) / price
        if imp >= 0.05 and -0.06 <= pull <= 0.01 and pull_range < 0.06:
            flag_top = max(highs[-12:-1])
            flag = "breakout" if price > flag_top else "forming"
    hi7 = max(highs[-168:]) if len(highs) >= 168 else max(highs)
    dd = price / hi7 - 1
    turn = sum(vols[-24:]) * price
    cor = corr(btc, closes)
    r = rsi(closes[-40:], 14)
    btc_beta = cor >= HI_CORR and btc_p4 > 0 and abs(p4 - btc_p4) < 0.01
    tf = sum([oi1 > 0.01, oi4 >= OI_4H_MIN, oi24 > 0.10])
    brk = price > max(highs[-168:-1]) if len(highs) > 168 else False
    atrr = atr_ratio(highs, lows, closes)
    return dict(
        coin=coin,
        price=price,
        p4=p4,
        oi1=oi1,
        oi4=oi4,
        oi24=oi24,
        spike=spike,
        uptrend=uptrend,
        dd=dd,
        turn=turn,
        cor=cor,
        tf=tf,
        brk=brk,
        rsi=r,
        btc_beta=btc_beta,
        e21=e21,
        ext=ext,
        consol_base=consol_base,
        old_high=old_high,
        extended=extended,
        tri=tri,
        tri_top=tri_top,
        tri_res_now=tri_res_now,
        tri_sup_now=tri_sup_now,
        flag=flag,
        flag_top=flag_top,
        levels=levels,
        lvl=lvl,
        atrr=atrr,
        tri_mtf=tri_mtf,
    )


def long_ok(m):
    return (
        m["oi4"] >= OI_4H_MIN
        and m["spike"] >= VOL_SPIKE_MIN
        and m["uptrend"]
        and m["dd"] > KNIFE_DD
        and m["turn"] >= THIN_TURN
        and m["p4"] >= PRICE_UP_4H_MIN
        and m["rsi"] <= RSI_MAX
        and m.get("atrr", 1.0) >= ATR_MIN_RATIO
    )


def _score(m, ex):
    s = 0
    s += 2 if m["oi4"] >= 0.10 else (1 if m["oi4"] >= 0.05 else 0)
    s += 2 if m["spike"] >= 3 else (1 if m["spike"] >= 1.5 else 0)
    s += m["tf"]
    s += 1 if m["brk"] else 0
    s += 1 if 50 <= m.get("rsi", 50) <= 70 else 0
    s += 1 if not m.get("btc_beta") else 0
    s += 1 if m["turn"] >= 20_000_000 else 0
    if ex.get("funding", 0) > 0.01:
        s -= 1
    return max(0, min(10, s))

# ---------- Cards ----------
def track_record(sig_type):
    import time as _t
    if _t.time() - _track_cache["ts"] < 600 and sig_type in _track_cache["data"]:
        return _track_cache["data"][sig_type]
    n = win = 0
    if os.path.exists(SIGNALS_FILE):
        now = dt.datetime.now()
        try:
            with open(SIGNALS_FILE) as f:
                for r in csv.DictReader(f):
                    if r.get("type") != sig_type:
                        continue
                    ts = dt.datetime.fromisoformat(r["ts"])
                    if (now - ts).total_seconds() / 3600 < 24:
                        continue
                    cur = bybit_price(r["coin"])
                    if cur is None:
                        continue
                    n += 1
                    if cur / float(r["price"]) - 1 > 0:
                        win += 1
        except Exception:
            pass
    _track_cache["data"][sig_type] = (n, win)
    _track_cache["ts"] = _t.time()
    return n, win


def card_long(m, ex):
    cautions = []
    if m.get("btc_beta"):
        cautions.append("движение в основном за BTC — не собственный приток")
    elif m["cor"] >= HI_CORR:
        cautions.append(f"сильно ходит за биткоином (корреляция {m['cor']*100:.0f}%)")
    if ex.get("funding", 0) > 0.01:
        cautions.append(f"повышенный funding ({ex.get('funding',0)*100:.3f}%) — плечо копится")
    if m.get("extended"):
        cautions.append("вход на пике импульса — лучше ждать откат")
    sc = _score(m, ex)
    head = "🟢" if not cautions else "🟡"
    arrow = "▲" if m["p4"] >= 0 else "▼"
    rsi_v = int(m.get("rsi", 50))
    tf_txt = {3: "1ч+4ч+24ч ✅", 2: "2 интервала", 1: "1 интервал ⚠️"}.get(m["tf"], "")
    table = (
        f"💰 Приток OI {m['oi4']*100:+.0f}% {_bar(m['oi4']/0.20,5)}\n"
        f"📈 Объём ×{m['spike']:.1f} {_bar(m['spike']/5,5)}\n"
        f"🌡 RSI {rsi_v} {_bar(rsi_v/100,5)}\n"
        f"💧 Ликвидн. ${m['turn']/1e6:.0f}M {_bar(min(m['turn']/100e6,1),5)}\n"
        f"⚡ Волатильность {m.get('atrr',1.0)*100:.0f}% от нормы {_bar(min(m.get('atrr',1.0),1.5)/1.5,5)}\n"
        f"🔗 Корр. с BTC {m['cor']*100:.0f}% {_bar(abs(m['cor']),5)}"
    )
    by = m.get("bybit")
    if by:
        spread = (by - m["price"]) / m["price"] * 100
        rel = "вровень" if abs(spread) < 0.15 else (f"выше +{spread:.1f}%" if spread > 0 else f"ниже {spread:.1f}%")
        price_line = f"💵 ${m['price']:.5g} (Bybit) {arrow} {m['p4']*100:+.1f}% за 4ч, сверка: {rel}"
    else:
        price_line = f"💵 ${m['price']:.5g} (Bybit) {arrow} {m['p4']*100:+.1f}% за 4ч"
    lines = [
        f"{head} {m['coin']} · ЛОНГ-СЕТАП",
        price_line,
        "",
        f"💪 Сила сетапа: {sc}/10 {_bar(sc/10,5)}",
        "",
        table,
        f"📊 Подтверждение: {tf_txt}",
    ]
    lsr = m.get("ls_ratio")
    long_stops, short_stops = stop_map(m)
    stop_lines = ["", "🗺 Карта стопов (ориентир, не точная):"]
    if lsr:
        if lsr >= 1.5:
            perekos = f"перекос в ЛОНГ ×{lsr:.1f} — толпа в лонге, риск слива за их стопами"
        elif lsr <= 0.67:
            perekos = f"перекос в ШОРТ (Л/Ш {lsr:.2f}) — шортов много, их стопы = топливо вверх"
        else:
            perekos = f"баланс (Л/Ш {lsr:.2f})"
        stop_lines.append(f"⚖️ Толпа: {perekos}")
    if short_stops:
        stop_lines.append(f"🎯 Стопы шортистов: над ${short_stops:.5g} — топливо для рывка вверх")
    if long_stops:
        stop_lines.append(f"🛑 Стопы лонгистов: под ${long_stops:.5g} — риск слива за ними")
    lz = liq_zones(m["price"], ex.get("funding", 0.0))
    stop_lines.append(f"🔥 Ликвидации лонгов: ~${lz['long_25x']:.5g} (25x) / ~${lz['long_10x']:.5g} (10x)")
    stop_lines.append(f"🔥 Ликвидации шортов: ~${lz['short_25x']:.5g} (25x) / ~${lz['short_10x']:.5g} (10x)")
    if lz["heavy"] == "long":
        stop_lines.append("• funding+ : рынок перегружен лонгами — нижний кластер плотнее")
    elif lz["heavy"] == "short":
        stop_lines.append("• funding− : рынок перегружен шортами — верхний кластер плотнее")
    lines += stop_lines
    reasons = []
    reasons.append("деньги активно заходят" if m["oi4"] >= 0.10 else "деньги заходят")
    reasons.append("тренд вверх (>EMA50)")
    if m["brk"]:
        reasons.append("пробой 7д-максимума")
    if 50 <= rsi_v <= 70:
        reasons.append("RSI здоровый")
    lines.append("✅ " + ", ".join(reasons) + ".")
    if cautions:
        lines.append("")
        lines.append("🛡 Учти риски:")
        for c in cautions:
            lines.append("⚠️ " + c)
    else:
        lines.append("🛡 Риски: чисто ✅")
    e21 = m.get("e21", m["price"])
    base = m.get("consol_base", m["price"])
    oh = m.get("old_high", m["price"])
    ext = m.get("ext", 0)
    fl = m.get("flag")
    ft = m.get("flag_top", m["price"])
    if fl:
        lines.append("")
        if fl == "forming":
            lines.append("🚩 Флаг: откат после импульса")
            lines.append(f"• верх флага: ${ft:.5g}")
            lines.append(f"• цель — выход выше ${ft:.5g}")
        elif fl == "breakout":
            lines.append("🚩🚀 Флаг: пробой вверх")
            lines.append(f"• цена вышла выше ${ft:.5g} — импульс продолжается")
    lv = m.get("lvl")
    if lv:
        pos = "цена НА уровне" if abs(lv["dist"]) < 0.012 else ("цена НАД уровнем" if lv["dist"] > 0 else "цена ПОД уровнем")
        strength = "очень сильный" if lv["touches"] >= 6 else ("сильный" if lv["touches"] >= 4 else "заметный")
        lines.append("")
        lines.append(f"📏 Уровень ${lv['price']:.5g} — {strength}: касаний {lv['touches']}")
        lines.append(f"• {pos}")
        if abs(lv["dist"]) < 0.012:
            lines.append("• лучше входить только на ретесте с отбоем")
    lines.append("")
    lines.append("🛑 Вход не по рынку сейчас. Только на ретесте с отбоем.")
    if m.get("btc_weak") and m["cor"] >= 0.3:
        lines.append("")
        lines.append(f"🟡 BTC слабеет ({m['btc_weak']}) — при корреляции {m['cor']*100:.0f}% риск потянуть вниз")
    if m.get("watching"):
        zlo, zhi = m["watching"]
        wk = m.get("watch_kind", "зоне")
        lines.append("")
        lines.append(f"⏳ На отслеживании — позову на ретесте к {wk} ${zlo:.5g}–${zhi:.5g}")
    lines.append("📍 Где входить:")
    if m.get("extended"):
        lines.append(f"⚠️ цена на +{ext*100:.0f}% выше EMA21 — не гонись за свечой")
        lines.append(f"• зона отката: ${e21:.5g} (EMA21) – ${base:.5g}")
    hi_note = " — пробивается 🚀" if m["price"] > oh else " — цель"
    lines.append(f"• старый хай: ${oh:.5g}{hi_note}")
    _n, _w = track_record("long")
    if _n > 0:
        lines += ["", f"📈 Трек-рекорд ЛОНГ: измерено {_n}, в плюсе через 24ч {_w} ({_w/_n*100:.0f}%)"]
    lines += ["", "━━━━━━━━━━━━━━━━", "⚠️ Подсветка, не приказ. Стоп на Bybit обязателен."]
    return "\n".join(lines)


def card_triangle(m, ex):
    tri = m.get("tri")
    if tri not in ("ready", "breakout"):
        return None
    tt = m.get("tri_top", m["price"])
    sc = _score(m, ex)
    rsi_v = int(m.get("rsi", 50))
    lines = [
        f"🔺 {m['coin']} · СЕТАП ТРЕУГОЛЬНИК",
        f"💵 ${m['price']:.5g} (Bybit)",
        "",
        f"💪 Сила сетапа: {sc}/10 {_bar(sc/10,5)}",
        f"💰 Приток OI {m['oi4']*100:+.0f}% · Объём ×{m['spike']:.1f} · RSI {rsi_v}",
        f"🔗 Корреляция с BTC {m['cor']*100:.0f}% {_bar(abs(m['cor']),5)}",
    ]
    mtf = m.get("tri_mtf")
    if mtf:
        def _mk(v):
            return "✅" if v in ("ready", "breakout", "forming") else "—"
        n_active = sum(1 for tf in ("15м", "1ч", "4ч") if mtf.get(tf) in ("ready", "breakout", "forming"))
        lines.append(f"🕒 Треугольник по ТФ: 15м {_mk(mtf.get('15м'))} 1ч {_mk(mtf.get('1ч'))} 4ч {_mk(mtf.get('4ч'))}")
        if n_active >= 2:
            lines.append(f"• виден на {n_active} ТФ — структура подтверждена")
        else:
            lines.append("• виден только на одном ТФ — слабее подтверждён")
    lines.append("")
    if tri == "ready":
        lines.append("⚡ Готовность к пробою")
        lines.append(f"• цена вплотную подошла к крышке ${tt:.5g} и поджимается")
        lines.append(f"• следи за закрытием свечи выше ${tt:.5g}")
        lines.append("• не входи заранее: часто бывает ложный прокол вниз")
    elif tri == "breakout":
        lines.append("🚀 ПРОБОЙ вверх")
        lines.append(f"• цена закрылась выше крышки ${tt:.5g} — треугольник пробит")
        lines.append(f"• подтверждение: удержание выше ${tt:.5g} или ретест сверху")
        lines.append("• вход лучше на ретесте, а не на проколе")
    if m.get("btc_weak") and m["cor"] >= 0.3:
        lines.append("")
        lines.append(f"🟡 BTC слабеет ({m['btc_weak']}) — при корреляции {m['cor']*100:.0f}% риск потянуть вниз")
    if m.get("watching"):
        zlo, zhi = m["watching"]
        wk = m.get("watch_kind", "зоне")
        lines.append("")
        lines.append(f"⏳ На отслеживании — позову на ретесте к {wk} ${zlo:.5g}–${zhi:.5g}")
    _n, _w = track_record("triangle")
    if _n > 0:
        lines += ["", f"📈 Трек-рекорд ТРЕУГОЛЬНИКОВ: измерено {_n}, в плюсе через 24ч {_w} ({_w/_n*100:.0f}%)"]
    lines += ["", "━━━━━━━━━━━━━━━━", "⚠️ Подсветка, не приказ. Стоп на Bybit обязателен."]
    return "\n".join(lines)


def card_early(m, zone_hi, zone_lo):
    p = m["price"]
    stop = zone_lo
    risk = (p - stop) / p * 100 if p > 0 else 0
    lines = [
        f"🔵 {m['coin']} · РАННИЙ СИГНАЛ (эксперим.)",
        f"💵 ${p:.5g} — только что пробил зону сжатия ${zone_lo:.5g}–${zone_hi:.5g}",
        "",
        "⚠️ Это НЕ подтверждённый лонг-сетап. Деньги (OI/объём) ещё не подтвердили движение.",
        "",
        f"📊 RSI {m.get('rsi',0):.0f} · корр. с BTC {m['cor']*100:.0f}% · волатильность {m.get('atrr',1)*100:.0f}% от нормы",
        "",
        "📍 Если тестируешь:",
        "• пробный объём, не полный размер",
        f"• стоп чуть ниже зоны сжатия: ${stop:.5g} (риск ~{risk:.1f}%)",
        "• если через 1-2ч придёт обычная 🟢 ЛОНГ-карточка по этой монете — движение подтвердилось",
    ]
    _n, _w = track_record("early")
    if _n > 0:
        lines += ["", f"📈 Трек-рекорд РАННИХ: измерено {_n}, в плюсе через 24ч {_w} ({_w/_n*100:.0f}%)"]
    lines += ["", "━━━━━━━━━━━━━━━━", "⚠️ Экспериментальный сигнал. Пойдёт ли вверх — НЕ гарантия."]
    return "\n".join(lines)

# ---------- logging ----------
def log_signal(coin, sig_type, price):
    try:
        new = not os.path.exists(SIGNALS_FILE)
        with open(SIGNALS_FILE, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["ts", "coin", "type", "price", "btc_price"])
            btcp = ""
            try:
                bp = bybit_price("BTC")
                if bp:
                    btcp = f"{bp:.2f}"
            except Exception:
                pass
            w.writerow([dt.datetime.now().isoformat(timespec="seconds"), coin, sig_type, f"{price:.6g}", btcp])
    except Exception as e:
        print("log_signal:", e)


def btc_short_risk():
    try:
        closes, highs, lows, vols = klines("BTCUSDT", limit=120)
        time.sleep(0.12)
        oic = open_interest("BTCUSDT", limit=30)
        time.sleep(0.12)
    except Exception:
        return 0, [], None
    if len(closes) < 60 or len(oic) < 6:
        return 0, [], closes[-1] if closes else None
    price = closes[-1]
    p1 = closes[-1] / closes[-2] - 1
    p4 = closes[-1] / closes[-5] - 1
    oi4 = oic[-1] / oic[-5] - 1 if oic[-5] > 0 else 0
    vr = sum(vols[-4:])
    vb = (sum(vols[-28:-4]) / 24 * 4) if len(vols) >= 28 else vr
    vspike = vr / vb if vb > 0 else 0
    r = rsi(closes[-40:], 14)
    reasons = []
    if p1 <= BTC_DUMP_1H:
        reasons.append(f"обвал за 1ч {p1*100:+.1f}%")
    if p4 <= BTC_DROP_4H:
        reasons.append(f"падение за 4ч {p4*100:+.1f}%")
    if oi4 <= BTC_OI_DROP_4H:
        reasons.append(f"отток OI {oi4*100:+.0f}%")
    if vspike >= BTC_VOL_SPIKE and p4 < 0:
        reasons.append(f"растущий объём на падении ×{vspike:.1f}")
    if r <= BTC_RSI_OVERSOLD:
        reasons.append(f"RSI {r:.0f} перепродан")
    return len(reasons), reasons, price


def btc_block_stats():
    if not os.path.exists(BLOCKS_FILE):
        return "Блокировок ещё не было — рубильник BTC пока не срабатывал."
    rows = []
    try:
        with open(BLOCKS_FILE) as f:
            for r in csv.DictReader(f):
                rows.append(r)
    except Exception:
        return "Не удалось прочитать журнал блокировок."
    if not rows:
        return "Блокировок ещё не было."
    now = dt.datetime.now()
    btc_now = bybit_price("BTC")
    out = ["🛑 ПРОВЕРКА BTC-РУБИЛЬНИКА\n", f"Всего блокировок: {len(rows)}"]
    for horizon_h, label in [(4, "4ч"), (24, "24ч")]:
        moves = []
        for r in rows:
            ts = dt.datetime.fromisoformat(r["ts"])
            if (now - ts).total_seconds() / 3600 < horizon_h:
                continue
            bp0 = r.get("btc_price", "")
            if bp0 and btc_now:
                try:
                    moves.append(btc_now / float(bp0) - 1)
                except Exception:
                    pass
        if not moves:
            continue
        n = len(moves)
        avg = sum(moves) / n * 100
        fell = sum(1 for x in moves if x < 0)
        verdict = "✅ в среднем BTC падал — блокировки оправданы" if avg < 0 else "⚠️ BTC в среднем рос после блокировок"
        out.append(f"Через {label}: n={n}, BTC в среднем {avg:+.2f}%, падал в {fell}/{n} случаях\n{verdict}")
    if len(out) == 2:
        out.append("Пока нет блокировок старше 4ч для проверки — подожди.")
    return "\n".join(out)


def compute_stats():
    if not os.path.exists(SIGNALS_FILE):
        return "Журнал сигналов пуст — статистики пока нет."
    rows = []
    with open(SIGNALS_FILE) as f:
        for r in csv.DictReader(f):
            rows.append(r)
    if not rows:
        return "Журнал сигналов пуст — статистики пока нет."
    now = dt.datetime.now()
    price_cache = {}

    def cur_price(coin):
        if coin in price_cache:
            return price_cache[coin]
        p = bybit_price(coin)
        price_cache[coin] = p
        return p

    btc_now = cur_price("BTC")
    out = ["📊 СТАТИСТИКА ПО СИГНАЛАМ (форвард + бенчмарк BTC)\n"]
    for horizon_h, label in [(4, "4ч"), (24, "24ч")]:
        for sig_type in ("long", "triangle", "early"):
            sig_pcts = []
            btc_pcts = []
            for r in rows:
                if r["type"] != sig_type:
                    continue
                ts = dt.datetime.fromisoformat(r["ts"])
                if (now - ts).total_seconds() / 3600 < horizon_h:
                    continue
                cur = cur_price(r["coin"])
                if cur is None:
                    continue
                entry = float(r["price"])
                sig_pcts.append(cur / entry - 1)
                bp0 = r.get("btc_price", "")
                if bp0 and btc_now:
                    try:
                        btc_pcts.append(btc_now / float(bp0) - 1)
                    except Exception:
                        pass
            if not sig_pcts:
                continue
            n = len(sig_pcts)
            wins = sum(1 for x in sig_pcts if x > 0)
            avg = sum(sig_pcts) / n * 100
            med = float(np.median(sig_pcts)) * 100
            win_vals = [x for x in sig_pcts if x > 0]
            loss_vals = [x for x in sig_pcts if x <= 0]
            avg_win = (sum(win_vals) / len(win_vals) * 100) if win_vals else 0.0
            avg_loss = (sum(loss_vals) / len(loss_vals) * 100) if loss_vals else 0.0
            expectancy = (wins / n) * avg_win + (1 - wins / n) * avg_loss
            gross_profit = sum(x for x in sig_pcts if x > 0)
            gross_loss = abs(sum(x for x in sig_pcts if x < 0))
            pf = (gross_profit / gross_loss) if gross_loss > 0 else None
            best = max(sig_pcts) * 100
            worst = min(sig_pcts) * 100
            line = [f"{sig_type.upper()} @ {label}: n={n}, win {wins/n*100:.0f}%"]
            line.append(f"средний {avg:+.2f}%, медиана {med:+.2f}%")
            line.append(f"avg win {avg_win:+.2f}%, avg loss {avg_loss:+.2f}%")
            line.append(f"expectancy {expectancy:+.2f}%")
            line.append(f"best {best:+.2f}%, worst {worst:+.2f}%")
            if pf is not None:
                line.append(f"PF {pf:.2f}")
            if btc_pcts:
                bavg = sum(btc_pcts) / len(btc_pcts) * 100
                edge = avg - bavg
                verdict = "✅ edge>0" if edge > 0 else "❌ edge<=0"
                line.append(f"BTC {bavg:+.2f}%, edge {edge:+.2f}% {verdict}")
            out.append(" | ".join(line))
        out.append("")
    out.append("⚠️ Малая выборка (n<30-50) слабая. Смотри на edge против BTC, expectancy и PF.")
    return "\n".join(out)


def candle_close_after(symbol, ts_ms, hours):
    limit = max(2, int(hours) + 2)
    res = bget("/v5/market/kline", {"category": "linear", "symbol": symbol, "interval": "60", "limit": limit})
    k = res["list"][::-1]
    for x in k:
        if int(x[0]) >= ts_ms:
            return float(x[4])
    return float(k[-1][4]) if k else None


def compute_advanced_stats():
    if not os.path.exists(SIGNALS_FILE):
        return "Журнал сигналов пуст — расширенной статистики пока нет."
    rows = []
    with open(SIGNALS_FILE) as f:
        for r in csv.DictReader(f):
            rows.append(r)
    if not rows:
        return "Журнал сигналов пуст — расширенной статистики пока нет."

    def pct_after(coin, ts_iso, entry, hours):
        try:
            ts = dt.datetime.fromisoformat(ts_iso)
            fut = candle_close_after(coin + "USDT", int(ts.timestamp() * 1000), hours)
            if fut is None:
                return None
            return fut / float(entry) - 1
        except Exception:
            return None

    def fmt(x):
        return f"{x:+.2f}%"

    out = ["📊 РАСШИРЕННАЯ СТАТИСТИКА ПО СИГНАЛАМ\n"]
    for horizon_h, label in ((4, "4ч"), (24, "24ч")):
        out.append(f"=== Горизонт {label} ===")
        for sig_type in ("long", "triangle", "early"):
            sig = []
            btc = []
            for r in rows:
                if r.get("type") != sig_type:
                    continue
                try:
                    age_h = (dt.datetime.now() - dt.datetime.fromisoformat(r["ts"])).total_seconds() / 3600
                except Exception:
                    continue
                if age_h < horizon_h + 1:
                    continue
                sp = pct_after(r["coin"], r["ts"], r["price"], horizon_h)
                if sp is None:
                    continue
                sig.append(sp)
                bp0 = r.get("btc_price", "")
                if bp0:
                    bp = pct_after("BTC", r["ts"], bp0, horizon_h)
                    if bp is not None:
                        btc.append(bp)
            if not sig:
                continue
            n = len(sig)
            wins = sum(1 for x in sig if x > 0)
            wr = wins / n * 100
            avg = sum(sig) / n * 100
            med = float(np.median(sig)) * 100
            win_vals = [x for x in sig if x > 0]
            loss_vals = [x for x in sig if x <= 0]
            avg_win = (sum(win_vals) / len(win_vals) * 100) if win_vals else 0.0
            avg_loss = (sum(loss_vals) / len(loss_vals) * 100) if loss_vals else 0.0
            expectancy = (wr / 100) * avg_win + (1 - wr / 100) * avg_loss
            gross_profit = sum(x for x in sig if x > 0)
            gross_loss = abs(sum(x for x in sig if x < 0))
            pf = (gross_profit / gross_loss) if gross_loss > 0 else None
            best = max(sig) * 100
            worst = min(sig) * 100
            line = [f"{sig_type.upper()}: n={n}, win {wr:.0f}%"]
            line.append(f"средний {fmt(avg)}, медиана {fmt(med)}")
            line.append(f"avg win {fmt(avg_win)}, avg loss {fmt(avg_loss)}")
            line.append(f"expectancy {fmt(expectancy)}")
            line.append(f"best {fmt(best)}, worst {fmt(worst)}")
            if pf is not None:
                line.append(f"PF {pf:.2f}")
            if btc:
                bavg = sum(btc) / len(btc) * 100
                edge = avg - bavg
                verdict = "✅ edge>0" if edge > 0 else "❌ edge<=0"
                line.append(f"BTC {fmt(bavg)}, edge {fmt(edge)} {verdict}")
            out.append(" | ".join(line))
        out.append("")
    out.append("⚠️ Для live важнее expectancy, PF и edge против BTC, чем один win rate.")
    out.append("⚠️ Выборка меньше 30 сигналов на сегмент — слабая.")
    return "\n".join(out)


def stop_map(m):
    price = m["price"]
    levels = m.get("levels") or []
    below = [lv for lv in levels if lv[0] < price * 0.998]
    above = [lv for lv in levels if lv[0] > price * 1.002]
    long_stops = max(below, key=lambda x: x[1])[0] if below else m.get("consol_base")
    short_stops = min(above, key=lambda x: x[0])[0] if above else None
    return long_stops, short_stops


def average_true_range_gap(highs, lows, closes):
    a = atr(highs, lows, closes, 14)
    if len(closes) < 60:
        return 1.0
    vals = []
    for i in range(30, len(closes)):
        vals.append(atr(highs[:i+1], lows[:i+1], closes[:i+1], 14))
    avg = sum(vals[-30:]) / min(30, len(vals)) if vals else a
    return a / avg if avg > 0 else 1.0

# ---------- Position follow-up ----------
def pos_buttons(coin):
    return [[{"text": "❌ Выйти", "callback_data": f"exit|{coin}"}]]


def close_trade(coin):
    p = POSITIONS.pop(coin, None)
    if not p:
        return None
    try:
        cur = bybit_price(coin)
        if cur is None:
            return None
        pnl = cur / p["entry"] - 1
        new = not os.path.exists(TRADES)
        with open(TRADES, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["ts_open", "coin", "entry", "ts_close", "exit", "pnl"])
            w.writerow([p["ts"], coin, f"{p['entry']:.6g}", dt.datetime.now().isoformat(timespec="seconds"), f"{cur:.6g}", f"{pnl:.6f}"])
        if pnl < 0:
            RECENT_LOSSES[coin] = time.time()
        return pnl, p["entry"], cur
    except Exception:
        return None


def position_status(coin):
    p = POSITIONS.get(coin)
    if not p:
        return None, None
    try:
        closes, _, _, _ = klines(p["sym"], limit=80)
        time.sleep(0.15)
        oic = open_interest(p["sym"], limit=10)
        time.sleep(0.15)
    except Exception:
        return None, None
    if len(closes) < 55 or len(oic) < 6:
        return None, None
    price = closes[-1]
    pnl = price / p["entry"] - 1
    oi1 = oic[-1] / oic[-2] - 1 if oic[-2] > 0 else 0
    oi4 = oic[-1] / oic[-5] - 1 if oic[-5] > 0 else 0
    e50 = ema(closes[-60:], 50)
    reasons = []
    if oi1 <= -0.03:
        reasons.append(f"OI резко вниз ({oi1*100:+.0f}% за 1ч)")
    if price < e50:
        reasons.append("цена ниже EMA50")
    if oi4 <= -0.05:
        reasons.append(f"OI 4ч {oi4*100:+.0f}%")
    state = "ok" if not reasons else "warn"
    msg = f"{coin}: {price:.6g} | PnL {pnl*100:+.2f}% | {'; '.join(reasons) if reasons else 'держится'}"
    return msg, state

# ---------- Scan ----------
def run_scan(cid, announce=False):
    global LAST_BTC_WARN
    coins = universe()
    try:
        btc_closes, btc_highs, btc_lows, btc_vols = klines("BTCUSDT", limit=120)
        time.sleep(0.12)
        btc_oic = open_interest("BTCUSDT", limit=30)
        time.sleep(0.12)
    except Exception:
        btc_closes, btc_highs, btc_lows, btc_vols, btc_oic = [], [], [], [], []
    btc_hits, btc_reasons, btc_price = btc_short_risk()
    if btc_hits >= BTC_RISK_MIN_HITS:
        msg = (
            "🛑 Сигналы приостановлены: БИТКОИН по шортовым фильтрам готов к серьёзной коррекции\n\n"
            + "\n".join("• " + r for r in btc_reasons)
            + (f"\n\nЦена BTC: ${btc_price:,.0f}" if btc_price else "")
            + "\n\nЭто реактивная защита: BTC уже показывает медвежьи признаки."
        )
        if announce or time.time() - LAST_BTC_WARN > 30 * 60:
            tg_send(cid, msg)
            LAST_BTC_WARN = time.time()
        try:
            new = not os.path.exists(BLOCKS_FILE)
            with open(BLOCKS_FILE, "a", newline="") as bf:
                wb = csv.writer(bf)
                if new:
                    wb.writerow(["ts", "btc_price", "hits", "reasons"])
                wb.writerow([dt.datetime.now().isoformat(timespec="seconds"), f"{btc_price:.2f}" if btc_price else "", btc_hits, "; ".join(btc_reasons)])
        except Exception as e:
            print("blocks:", e)
        return
    btc_weak = btc_reasons[0] if btc_hits == 1 else None
    shown = 0
    now = time.time()
    for coin, sym in coins:
        if shown >= MAX_ALERTS:
            break
        try:
            closes, highs, lows, vols = klines(sym, limit=200)
            time.sleep(0.15)
            oic = open_interest(sym, limit=50)
            time.sleep(0.15)
        except Exception:
            continue
        if len(closes) < MIN_BARS or len(oic) < 30:
            continue
        m = core(coin, closes, highs, lows, vols, oic, btc_closes or closes, btc_p4=(btc_closes[-1] / btc_closes[-5] - 1 if len(btc_closes) >= 5 else 0))
        ex = ticker_info(sym) or {}
        by = bybit_price(coin)
        if by:
            m["bybit"] = by
        m["ls_ratio"] = long_short_ratio(sym)
        m["btc_weak"] = btc_weak
        if EARLY_ENABLED:
            early, zhi, zlo = early_breakout(closes, highs, lows, vols)
            if early and m.get("atrr", 1.0) >= ATR_MIN_RATIO and now - LAST_EARLY.get(coin, 0) > EARLY_COOLDOWN_H * 3600:
                LAST_EARLY[coin] = now
                buttons = [[{"text": "✅ Я вошёл", "callback_data": f"enter|{coin}|{m['price']:.6g}"}]]
                tg_send(cid, card_early(m, zhi, zlo), buttons=buttons)
                shown += 1
                log_signal(coin, "early", m["price"])
        if not long_ok(m):
            continue
        last = LAST_ALERT.get(coin, 0)
        cd = COOLDOWN_H * 3600
        loss_ts = RECENT_LOSSES.get(coin, 0)
        if loss_ts and now - loss_ts < COOLDOWN_H * LOSS_COOLDOWN_MULT * 3600:
            cd = COOLDOWN_H * LOSS_COOLDOWN_MULT * 3600
        if now - last < cd:
            continue
        lv = m.get("lvl")
        if lv and abs(lv["dist"]) < 0.03 and lv["touches"] >= 3:
            base = lv["price"]
            WATCH[m["coin"]] = dict(sym=sym, zone_hi=base * 1.008, zone_lo=base * 0.99, ts=time.time(), price0=m["price"], kind=f"уровню ${base:.5g}")
            m["watching"] = (base * 0.99, base * 1.008)
            m["watch_kind"] = f"уровню ${base:.5g} ({lv['touches']} касаний)"
        elif m.get("tri") == "breakout" and m.get("tri_top", 0) > 0:
            top = m["tri_top"]
            WATCH[m["coin"]] = dict(sym=sym, zone_hi=top * 1.004, zone_lo=top * 0.985, ts=time.time(), price0=m["price"], kind="пробой треугольника")
            m["watching"] = (top * 0.985, top * 1.004)
            m["watch_kind"] = "крышке треугольника"
        else:
            zone_hi = m.get("e21", m["price"])
            zone_lo = m.get("consol_base", m["price"])
            if m.get("extended"):
                WATCH[m["coin"]] = dict(sym=sym, zone_hi=zone_hi, zone_lo=zone_lo, ts=time.time(), price0=m["price"], kind="откат к зоне")
                m["watching"] = (zone_lo, zone_hi)
                m["watch_kind"] = "зоне отката"
        buttons = [[{"text": "✅ Я вошёл", "callback_data": f"enter|{m['coin']}|{m['price']:.6g}"}]]
        tg_send(cid, card_long(m, ex), buttons=buttons)
        shown += 1
        log_signal(m["coin"], "long", m["price"])
        tri_card = card_triangle(m, ex)
        if tri_card:
            tg_send(cid, tri_card, buttons=buttons)
            shown += 1
            log_signal(m["coin"], "triangle", m["price"])
            if m.get("tri") == "ready" and m.get("tri_top", 0) > 0 and m["coin"] not in TRI_ALERT:
                TRI_ALERT[m["coin"]] = dict(sym=sym, top=m["tri_top"], ts=time.time())
        LAST_ALERT[coin] = now
    if shown == 0 and announce:
        tg_send(cid, "Сейчас чистых сетапов не найдено. Бот продолжает сканировать автоматически.")


def check_watchlist(chat):
    if not chat or not WATCH:
        return
    now = time.time()
    for coin in list(WATCH):
        w = WATCH[coin]
        if now - w["ts"] > WATCH_HOURS * 3600:
            del WATCH[coin]
            continue
        try:
            res = bget("/v5/market/kline", {"category": "linear", "symbol": w["sym"], "interval": "60", "limit": 3})
            k = res["list"]
            time.sleep(0.15)
        except Exception:
            continue
        if len(k) < 1:
            continue
        last = k[0]
        o = float(last[1])
        c = float(last[4])
        lo = float(last[3])
        touched = lo <= w["zone_hi"]
        bounced = c >= o
        if c < w["zone_lo"] * 0.97:
            del WATCH[coin]
            continue
        if touched and (bounced or not RETEST_NEED_BOUNCE):
            by = bybit_price(coin)
            byline = f"\nBybit: ${by:.5g}" if by else ""
            kind = w.get("kind", "зоне")
            tg_send(
                chat,
                f"🎯 {coin}: РЕТЕСТ ({kind})!\n"
                f"Цена вернулась к ${w['zone_lo']:.5g}–${w['zone_hi']:.5g} и отбивается (зелёная свеча).\n"
                f"Сейчас: ${c:.5g}{byline}\n"
                f"Проверь глазами, стоп обязателен.",
                buttons=[[{"text": "✅ Я вошёл", "callback_data": f"enter|{coin}|{c:.6g}"}]],
            )
            del WATCH[coin]

# ---------- callbacks ----------
def handle_callback(q):
    data = q.get("data", "")
    cid = str(((q.get("message") or {}).get("chat") or {}).get("id", ""))
    tg_answer(q.get("id", ""))
    if not cid:
        return
    parts = data.split("|")
    if parts[0] == "enter" and len(parts) >= 3:
        coin = parts[1]
        price = float(parts[2])
        sym = SYM_CACHE.get(coin)
        if not sym:
            tg_send(cid, f"Не могу найти {coin} для ведения. Сделай /scan заново.")
            return
        POSITIONS[coin] = dict(entry=price, ts=dt.datetime.now().isoformat(timespec="seconds"), sym=sym, last_upd=0, last_check=0, last_state="ok")
        tg_send(
            cid,
            f"✅ Веду позицию {coin} от ${price:.5g}.\n"
            f"Проверяю каждые {CHECK_POS_MIN} мин.\n\n"
            f"⚠️ Сразу выстави стоп-ордер на Bybit — это твоя мгновенная защита.",
            buttons=pos_buttons(coin),
        )
    elif parts[0] == "exit" and len(parts) >= 2:
        coin = parts[1]
        res = close_trade(coin)
        if not res:
            tg_send(cid, f"Позиции по {coin} нет.")
            return
        pnl, e, x = res
        emo = "🟢" if pnl >= 0 else "🔴"
        tg_send(cid, f"{emo} Сделка по {coin} закрыта.\nВход ${e:.5g} → выход ${x:.5g} = {pnl*100:+.2f}%\nЗаписал в журнал.")

# ---------- trade journal ----------
def pos_buttons(coin):
    return [[{"text": "❌ Выйти", "callback_data": f"exit|{coin}"}]]


def close_trade(coin):
    p = POSITIONS.pop(coin, None)
    if not p:
        return None
    try:
        cur = bybit_price(coin)
        if cur is None:
            return None
        pnl = cur / p["entry"] - 1
        new = not os.path.exists(TRADES)
        with open(TRADES, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["ts_open", "coin", "entry", "ts_close", "exit", "pnl"])
            w.writerow([p["ts"], coin, f"{p['entry']:.6g}", dt.datetime.now().isoformat(timespec="seconds"), f"{cur:.6g}", f"{pnl:.6f}"])
        if pnl < 0:
            RECENT_LOSSES[coin] = time.time()
        return pnl, p["entry"], cur
    except Exception:
        return None


def position_status(coin):
    p = POSITIONS.get(coin)
    if not p:
        return None, None
    try:
        closes, _, _, _ = klines(p["sym"], limit=80)
        time.sleep(0.15)
        oic = open_interest(p["sym"], limit=10)
        time.sleep(0.15)
    except Exception:
        return None, None
    if len(closes) < 55 or len(oic) < 6:
        return None, None
    price = closes[-1]
    pnl = price / p["entry"] - 1
    oi1 = oic[-1] / oic[-2] - 1 if oic[-2] > 0 else 0
    oi4 = oic[-1] / oic[-5] - 1 if oic[-5] > 0 else 0
    e50 = ema(closes[-60:], 50)
    reasons = []
    if oi1 <= -0.03:
        reasons.append(f"OI резко вниз ({oi1*100:+.0f}% за 1ч)")
    if price < e50:
        reasons.append("цена ниже EMA50")
    if oi4 <= -0.05:
        reasons.append(f"OI 4ч {oi4*100:+.0f}%")
    state = "ok" if not reasons else "warn"
    msg = f"{coin}: {price:.6g} | PnL {pnl*100:+.2f}% | {'; '.join(reasons) if reasons else 'держится'}"
    return msg, state

# ---------- main ----------
def main():
    if len(TG_TOKEN) < 20:
        print("Нет валидного TG_TOKEN.")
        return
    ensure_dirs()
    me = tg("getMe")
    if not me.get("ok"):
        print("Не подключиться — проверь TG_TOKEN.")
        return
    print(f"Бот @{me['result']['username']} запущен (server mode).")
    offset = None
    last_scan = 0
    chat = load_chat()
    last_watch = 0
    while True:
        try:
            updates = tg("getUpdates", offset=offset, timeout=30).get("result", [])
            for u in updates:
                offset = u["update_id"] + 1
                if "callback_query" in u:
                    handle_callback(u["callback_query"])
                    continue
                msg = u.get("message") or {}
                text = (msg.get("text") or "").lower().strip()
                cid = str((msg.get("chat") or {}).get("id", ""))
                if not cid:
                    continue
                if text.startswith("/start"):
                    chat = cid
                    save_chat(cid)
                    tg_send(cid, "✅ Сканер на сервере, работает 24/7.\n/start — старт\n/scan — искать сетапы\n/pos — мои позиции\n/watch — кто в ожидании\n/log — журнал сделок\n/stats — статистика по сигналам\n/stats2 — расширенная статистика\n/bybit — проверка доступа к Bybit\n/btcstats — проверка рубильника BTC")
                elif text.startswith("/scan"):
                    run_scan(cid, announce=True)
                elif text.startswith("/pos"):
                    if POSITIONS:
                        rows = []
                        for c in list(POSITIONS):
                            ps, st = position_status(c)
                            if ps:
                                rows.append(ps)
                        tg_send(cid, "\n".join(rows) if rows else "Открытые позиции есть, но пока нечего обновить.")
                    else:
                        tg_send(cid, "Открытых позиций нет.")
                elif text.startswith("/btcstats"):
                    tg_send(cid, btc_block_stats())
                elif text.startswith("/stats2"):
                    tg_send(cid, compute_advanced_stats())
                elif text.startswith("/stats"):
                    tg_send(cid, compute_stats())
                elif text.startswith("/bybit"):
                    try:
                        r = requests.get(f"{BYBIT}/v5/market/tickers", params={"category": "linear", "symbol": "BTCUSDT"}, timeout=10)
                        if r.status_code == 200:
                            j = r.json()
                            p = (j.get("result") or {}).get("list", [{}])[0].get("lastPrice", "?")
                            tg_send(cid, f"✅ Bybit доступен. BTC цена: ${p}")
                        else:
                            tg_send(cid, f"❌ Bybit вернул код {r.status_code}")
                    except Exception as e:
                        tg_send(cid, f"❌ Bybit недоступен: {type(e).__name__}")
                elif text.startswith("/watch"):
                    if WATCH:
                        rows = [f"• {c}: жду ретест ${w['zone_lo']:.5g}–${w['zone_hi']:.5g} ({w.get('kind','зона')})" for c, w in WATCH.items()]
                        tg_send(cid, "⏳ На отслеживании:\n" + "\n".join(rows))
                    else:
                        tg_send(cid, "Список ожидания пуст.")
                elif text.startswith("/log"):
                    if os.path.exists(TRADES) and os.path.getsize(TRADES) > 0:
                        n = sum(1 for _ in open(TRADES, encoding="utf-8")) - 1
                        tg_send_doc(cid, TRADES, f"Журнал сделок: {n}")
                    else:
                        tg_send(cid, "Журнал пуст.")
            if chat and time.time() - last_scan > SCAN_EVERY_MIN * 60:
                run_scan(chat, announce=False)
                last_scan = time.time()
            if time.time() - last_watch > WATCH_CHECK_SEC:
                last_watch = time.time()
                try:
                    check_watchlist(chat)
                except Exception as e:
                    print("watch:", e)
            time.sleep(1)
        except Exception as e:
            print("loop:", e)
            time.sleep(10)


if __name__ == "__main__":
    main()
