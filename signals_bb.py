"""BB Squeeze signal logic (long + optional short)."""
from __future__ import annotations
from typing import Optional

from config import (
    BB_PERIOD, BB_MULT,
    BB_SQUEEZE_LOOKBACK, BB_SQUEEZE_PERCENTILE, BB_SQUEEZE_MAX_BW,
    BB_SQUEEZE_FRESH_BARS, BB_BREAKOUT_VOL_MIN,
    BB_PULLBACK_MAX_PCT, BB_PULLBACK_RSI_MAX, BB_PULLBACK_MIN_PCT, BB_REQUIRE_PULLBACK,
    BB_OI_24H_MIN, BB_OI_4H_MIN, BB_PARABOLIC_MAX_PCT,
    BB_REQUIRE_ABOVE_MID, BB_REQUIRE_EXPANSION, BB_REJECT_FALSE_BREAKOUT,
    BB_REQUIRE_KC_SQUEEZE, BB_KC_SQUEEZE_BARS, BB_REQUIRE_KC_BREAKOUT,
    BB_SQUEEZE_MIN_KC_BARS, BB_SQUEEZE_SL_BUFFER_PCT,
    BB_SQUEEZE_REQUIRE_BULL_CLOSE, BB_SQUEEZE_RSI_MOMENTUM_MIN,
    BB_SQUEEZE_TP_BW_MULT,
    AUTO_BB_TP_PCT, AUTO_BB_SL_PCT,
    USE_EMA_FILTER,
    ENABLE_BB_SQUEEZE_SHORT,
    REL_STRENGTH_VS_BTC_ENABLED, REL_STRENGTH_MIN_PCT, LONG_BIAS_STRICT_TREND,
    EMA_SLOPE_MIN_PCT, EMA_SLOPE_LOOKBACK, SHORT_REQUIRE_BEAR_TREND,
    ADX_FILTER_ENABLED, ADX_MIN_THRESHOLD, HTF_ALIGNMENT_ENABLED, ATR_SL_ENABLED, ATR_SL_MULT,
    BB_PULLBACK_REQUIRE_REJECTION, BB_BOUNCE_MIN_PCT,
    SQUEEZE_FUNDING_FILTER, SQUEEZE_FUNDING_MAX, SQUEEZE_FUNDING_MIN,
    ZONE_TP_MULT,
)
from indicators import calculate_rsi, calculate_ema_slope_pct, calculate_adx


def try_bb_squeeze(
    d: dict,
    closes_15m: list[float],
    opens_15m: list[float] | None = None,
    lows_15m: list[float] | None = None,
    highs_15m: list[float] | None = None,
) -> Optional[dict]:
    """
    BB_SQUEEZE medium preset:
    squeeze → expansion → upper break → optional pullback → long
    """
    if d.get("bb_upper") is None or d.get("bb_bandwidth") is None:
        return None
    if not closes_15m or len(closes_15m) < BB_PERIOD + 3:
        return None

    if REL_STRENGTH_VS_BTC_ENABLED:
        pc24 = d.get("price_change_24h")
        btc24 = d.get("btc_24h")
        if pc24 is not None and btc24 is not None:
            if (float(pc24) - float(btc24)) < REL_STRENGTH_MIN_PCT:
                return None

    oi24 = d.get("oi_change_24h")
    oi4 = d.get("oi_change_4h")
    if BB_OI_24H_MIN > 0:
        if oi24 is None or float(oi24) < BB_OI_24H_MIN:
            return None
    if BB_OI_4H_MIN > 0:
        if oi4 is None or float(oi4) < BB_OI_4H_MIN:
            return None

    if SQUEEZE_FUNDING_FILTER:
        fr = d.get("funding_rate")
        if fr is not None:
            if fr > SQUEEZE_FUNDING_MAX or fr < SQUEEZE_FUNDING_MIN:
                return None

    bw = d["bb_bandwidth"]
    hist = d.get("bb_history_bw") or []
    hist_kc = d.get("kc_squeeze_hist") or []

    max_run = 0
    cur_run = 0
    for v in hist_kc:
        if v:
            cur_run += 1
            max_run = max(max_run, cur_run)
        else:
            cur_run = 0
    if BB_REQUIRE_KC_SQUEEZE and max_run < BB_SQUEEZE_MIN_KC_BARS:
        return None

    fresh_n = max(2, min(BB_SQUEEZE_FRESH_BARS, len(hist) if hist else 1))
    recent = hist[:fresh_n] if hist else [bw]
    min_recent = min(recent)

    percentile_ok = False
    if hist and len(hist) >= 10:
        sorted_bw = sorted(hist)
        pidx = max(0, int(len(sorted_bw) * BB_SQUEEZE_PERCENTILE / 100) - 1)
        percentile_ok = min_recent <= sorted_bw[pidx]
    cap_ok = min_recent <= BB_SQUEEZE_MAX_BW
    if not (percentile_ok or cap_ok):
        return None

    if BB_REQUIRE_EXPANSION and len(hist) >= 3:
        if bw < min_recent * 1.02 and bw <= (hist[1] if len(hist) > 1 else bw):
            return None

    upper = d["bb_upper"]
    mid = d.get("bb_middle")

    broke = False
    breakout_high = d["price"]
    broke_idx = None
    look = min(3, len(closes_15m))
    for i in range(1, look + 1):
        c = closes_15m[-i]
        if c > upper:
            broke = True
            if c >= breakout_high:
                breakout_high = c
                broke_idx = i
    if not broke or broke_idx is None:
        return None

    if BB_SQUEEZE_REQUIRE_BULL_CLOSE and opens_15m and len(opens_15m) >= broke_idx:
        if closes_15m[-broke_idx] <= opens_15m[-broke_idx]:
            return None

    if mid is not None and closes_15m[-broke_idx] <= mid:
        return None

    end_bo = len(closes_15m) - broke_idx + 1
    rsi_at_bo = None
    if end_bo >= 15:
        rsi_at_bo = calculate_rsi(closes_15m[:end_bo], 14)
    if rsi_at_bo is not None and rsi_at_bo < BB_SQUEEZE_RSI_MOMENTUM_MIN:
        return None

    if BB_REQUIRE_KC_BREAKOUT:
        kc_up = d.get("kc_upper")
        if kc_up is None:
            return None
        if not any(closes_15m[-i] > kc_up for i in range(1, look + 1)):
            return None

    if BB_REJECT_FALSE_BREAKOUT and mid is not None:
        n = len(closes_15m)
        for i in range(max(0, n - 5), n):
            if closes_15m[i] > upper:
                for j in range(i + 1, n):
                    if closes_15m[j] < mid:
                        return None
                break

    vol15 = d.get("vol_spike_15m") or 0.0
    if vol15 < BB_BREAKOUT_VOL_MIN:
        return None

    if len(closes_15m) >= 3:
        local_low = min(closes_15m[-3], closes_15m[-2], closes_15m[-1])
        if local_low > 0:
            spike_pct = (closes_15m[-1] - local_low) / local_low * 100
            if spike_pct > BB_PARABOLIC_MAX_PCT:
                return None

    # Break-and-retest: не входим прямо на свече пробоя, ждём откат к пробитому уровню
    pullback_pct = (breakout_high - d["price"]) / breakout_high * 100 if breakout_high > 0 else 0
    if BB_REQUIRE_PULLBACK:
        if pullback_pct < max(0.05, BB_PULLBACK_MIN_PCT):
            return None
    elif pullback_pct < BB_PULLBACK_MIN_PCT:
        return None
    if pullback_pct > BB_PULLBACK_MAX_PCT:
        return None

    # Подтверждающая свеча отката: последняя закрылась выше предыдущей и выше своего открытия —
    # импульс уже развернулся вверх, не входим в момент падения к уровню, а после отбоя
    if BB_PULLBACK_REQUIRE_REJECTION and len(closes_15m) >= 2:
        last_close = closes_15m[-1]
        prev_close = closes_15m[-2]
        last_open = opens_15m[-1] if opens_15m and len(opens_15m) >= 1 else None
        bullish_close = last_open is None or last_close >= last_open
        if not (last_close > prev_close and bullish_close):
            return None

    if LONG_BIAS_STRICT_TREND:
        ema50 = d.get("ema50_1h")
        above_mid = mid is not None and d["price"] >= mid
        above_ema = ema50 is not None and d["price"] >= ema50
        ema_slope = d.get("ema50_1h_slope_pct")
        slope_ok = ema_slope is None or ema_slope >= EMA_SLOPE_MIN_PCT
        if not (above_mid and above_ema and slope_ok):
            return None
    elif BB_REQUIRE_ABOVE_MID and mid is not None and d["price"] < mid:
        return None
    elif USE_EMA_FILTER and d.get("ema50_1h") is not None and d["price"] < d["ema50_1h"]:
        return None

    if ADX_FILTER_ENABLED:
        adx = d.get("adx_1h")
        if adx is not None and adx < ADX_MIN_THRESHOLD:
            return None

    if HTF_ALIGNMENT_ENABLED:
        ema50_4h = d.get("ema50_4h")
        if ema50_4h is not None and d["price"] < ema50_4h:
            return None
        slope_4h = d.get("ema50_4h_slope_pct")
        if slope_4h is not None and slope_4h < 0:
            return None

    rsi_15 = d.get("rsi_15m")
    if rsi_15 is not None and rsi_15 > BB_PULLBACK_RSI_MAX:
        return None

    # mild momentum: last close not lower than 3 bars ago (skip if just fired same bar)
    if len(closes_15m) >= 3 and pullback_pct > 0.05:
        if closes_15m[-1] <= closes_15m[-3]:
            return None

    entry = float(d["price"])
    zone_lows = []
    if hist_kc and lows_15m:
        i = 0
        while i < len(hist_kc) and not hist_kc[i]:
            i += 1
        while i < len(hist_kc) and hist_kc[i]:
            if len(lows_15m) > i:
                zone_lows.append(lows_15m[-(i + 1)])
            i += 1
    if not zone_lows and lows_15m and len(lows_15m) >= broke_idx:
        zone_lows = [lows_15m[-broke_idx]]
    if not zone_lows:
        zone_lows = [closes_15m[-broke_idx]]
    sl_raw = min(zone_lows)
    if mid is not None:
        sl_raw = min(sl_raw, mid)
    sl_price = sl_raw * (1 - BB_SQUEEZE_SL_BUFFER_PCT / 100)

    if ATR_SL_ENABLED:
        atr = d.get("kc_atr")
        if atr is not None and atr > 0:
            atr_sl_price = entry - atr * ATR_SL_MULT
            sl_price = max(sl_price, atr_sl_price)

    max_sl = entry * (1 - AUTO_BB_SL_PCT / 100)
    if sl_price < max_sl:
        sl_price = max_sl
    if sl_price >= entry:
        sl_price = entry * (1 - 0.4 / 100)

    zone_highs = []
    if hist_kc and highs_15m:
        zi = 0
        while zi < len(hist_kc) and not hist_kc[zi]:
            zi += 1
        while zi < len(hist_kc) and hist_kc[zi]:
            if len(highs_15m) > zi:
                zone_highs.append(highs_15m[-(zi + 1)])
            zi += 1
    if zone_highs and zone_lows:
        z_hi = max(zone_highs)
        z_lo = min(zone_lows)
        zone_range_pct = (z_hi - z_lo) / entry * 100 if entry > 0 else 0.0
    else:
        zone_range_pct = float(bw)

    tp_pct = max(
        float(AUTO_BB_TP_PCT),
        float(bw) * float(BB_SQUEEZE_TP_BW_MULT),
        float(zone_range_pct) * float(ZONE_TP_MULT),
    )
    tp_price = entry * (1 + tp_pct / 100)
    sl_pct = (entry - sl_price) / entry * 100 if entry > 0 else AUTO_BB_SL_PCT

    stars = 1
    if (oi24 or 0) >= max(BB_OI_24H_MIN, 1.0) * 1.5 and vol15 >= max(BB_BREAKOUT_VOL_MIN, 1.0) * 1.3:
        stars = 2
    if stars == 2 and max_run >= BB_SQUEEZE_MIN_KC_BARS + 2:
        stars = 3
    if stars >= 2 and len(hist) >= 2 and bw >= min_recent * 1.15:
        stars = 3

    return {
        **d,
        "stars": stars,
        "signal_type": "BB_SQUEEZE",
        "bb_pullback_pct": round(pullback_pct, 2),
        "bb_bandwidth": round(bw, 2),
        "vol_spike_15m": round(vol15, 2),
        "squeeze_bars": max_run,
        "tp_price_abs": round(tp_price, 8),
        "sl_price_abs": round(sl_price, 8),
        "tp_pct": round(tp_pct, 3),
        "sl_pct": round(sl_pct, 3),
        "zone_range_pct": round(zone_range_pct, 3),
        "entry_note": (
            f"KC×{max_run}→break→entry | vol×{vol15:.2f} | "
            f"SL {sl_pct:.2f}% | TP {tp_pct:.2f}%"
        ),
    }


def try_bb_squeeze_short(
    d: dict,
    closes_15m: list[float],
    opens_15m: list[float] | None = None,
    highs_15m: list[float] | None = None,
) -> Optional[dict]:
    """SHORT mirror — optional."""
    if not ENABLE_BB_SQUEEZE_SHORT:
        return None
    if d.get("bb_lower") is None or d.get("bb_bandwidth") is None:
        return None
    if not closes_15m or len(closes_15m) < BB_PERIOD + 3:
        return None

    bw = d["bb_bandwidth"]
    hist = d.get("bb_history_bw") or []
    hist_kc = d.get("kc_squeeze_hist") or []

    max_run = 0
    cur_run = 0
    for v in hist_kc:
        if v:
            cur_run += 1
            max_run = max(max_run, cur_run)
        else:
            cur_run = 0
    if BB_REQUIRE_KC_SQUEEZE and max_run < BB_SQUEEZE_MIN_KC_BARS:
        return None

    fresh_n = max(2, min(BB_SQUEEZE_FRESH_BARS, len(hist) if hist else 1))
    recent = hist[:fresh_n] if hist else [bw]
    min_recent = min(recent)
    percentile_ok = False
    if hist and len(hist) >= 10:
        sorted_bw = sorted(hist)
        pidx = max(0, int(len(sorted_bw) * BB_SQUEEZE_PERCENTILE / 100) - 1)
        percentile_ok = min_recent <= sorted_bw[pidx]
    if not (percentile_ok or min_recent <= BB_SQUEEZE_MAX_BW):
        return None

    if BB_REQUIRE_EXPANSION and len(hist) >= 3:
        if bw < min_recent * 1.02 and bw <= (hist[1] if len(hist) > 1 else bw):
            return None

    lower = d["bb_lower"]
    mid = d.get("bb_middle")
    broke = False
    breakout_low = d["price"]
    broke_idx = None
    look = min(3, len(closes_15m))
    for i in range(1, look + 1):
        c = closes_15m[-i]
        if c < lower:
            broke = True
            if c <= breakout_low:
                breakout_low = c
                broke_idx = i
    if not broke or broke_idx is None:
        return None

    if BB_SQUEEZE_REQUIRE_BULL_CLOSE and opens_15m and len(opens_15m) >= broke_idx:
        if closes_15m[-broke_idx] >= opens_15m[-broke_idx]:
            return None

    vol15 = d.get("vol_spike_15m") or 0.0
    if vol15 < BB_BREAKOUT_VOL_MIN:
        return None

    price = d["price"]
    bounce = (price - breakout_low) / breakout_low * 100 if breakout_low > 0 else 0
    if bounce > BB_PULLBACK_MAX_PCT:
        return None
    if bounce < BB_BOUNCE_MIN_PCT:
        return None
    if mid is not None and price > mid:
        return None

    if BB_PULLBACK_REQUIRE_REJECTION and len(closes_15m) >= 2:
        last_close = closes_15m[-1]
        prev_close = closes_15m[-2]
        last_open = opens_15m[-1] if opens_15m and len(opens_15m) >= 1 else None
        bearish_close = last_open is None or last_close <= last_open
        if not (last_close < prev_close and bearish_close):
            return None

    if SHORT_REQUIRE_BEAR_TREND:
        ema50 = d.get("ema50_1h")
        below_mid = mid is not None and price <= mid
        below_ema = ema50 is not None and price <= ema50
        ema_slope = d.get("ema50_1h_slope_pct")
        slope_ok = ema_slope is None or ema_slope <= -EMA_SLOPE_MIN_PCT
        if not (below_mid and below_ema and slope_ok):
            return None

    if ADX_FILTER_ENABLED:
        adx = d.get("adx_1h")
        if adx is not None and adx < ADX_MIN_THRESHOLD:
            return None

    if HTF_ALIGNMENT_ENABLED:
        ema50_4h = d.get("ema50_4h")
        if ema50_4h is not None and price > ema50_4h:
            return None
        slope_4h = d.get("ema50_4h_slope_pct")
        if slope_4h is not None and slope_4h > 0:
            return None

    entry = float(price)
    zone_highs = []
    if hist_kc and highs_15m:
        i = 0
        while i < len(hist_kc) and not hist_kc[i]:
            i += 1
        while i < len(hist_kc) and hist_kc[i]:
            if len(highs_15m) > i:
                zone_highs.append(highs_15m[-(i + 1)])
            i += 1
    if not zone_highs and highs_15m and len(highs_15m) >= broke_idx:
        zone_highs = [highs_15m[-broke_idx]]
    if not zone_highs:
        zone_highs = [closes_15m[-broke_idx]]
    sl_raw = max(zone_highs)
    if mid is not None:
        sl_raw = max(sl_raw, mid)
    sl_price = sl_raw * (1 + BB_SQUEEZE_SL_BUFFER_PCT / 100)

    if ATR_SL_ENABLED:
        atr = d.get("kc_atr")
        if atr is not None and atr > 0:
            atr_sl_price = entry + atr * ATR_SL_MULT
            sl_price = min(sl_price, atr_sl_price)

    max_sl = entry * (1 + AUTO_BB_SL_PCT / 100)
    if sl_price > max_sl:
        sl_price = max_sl
    tp_pct = max(float(AUTO_BB_TP_PCT), float(bw) * float(BB_SQUEEZE_TP_BW_MULT))
    tp_price = entry * (1 - tp_pct / 100)
    sl_pct = (sl_price - entry) / entry * 100 if entry > 0 else AUTO_BB_SL_PCT

    return {
        **d,
        "stars": 1,
        "signal_type": "BB_SQUEEZE_SHORT",
        "side": "Sell",
        "bb_bandwidth": round(bw, 2),
        "vol_spike_15m": round(vol15, 2),
        "squeeze_bars": max_run,
        "tp_price_abs": round(tp_price, 8),
        "sl_price_abs": round(sl_price, 8),
        "tp_pct": round(tp_pct, 3),
        "sl_pct": round(sl_pct, 3),
        "entry_note": f"SHORT KC×{max_run} vol×{vol15:.2f}",
    }
