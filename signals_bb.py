"""BB Squeeze signal logic (long + optional short). Extracted from scanner for maintainability."""
from __future__ import annotations
from typing import Optional

from config import (
    BB_PERIOD, BB_MULT,
    BB_SQUEEZE_LOOKBACK, BB_SQUEEZE_PERCENTILE, BB_SQUEEZE_MAX_BW,
    BB_SQUEEZE_FRESH_BARS, BB_BREAKOUT_VOL_MIN,
    BB_PULLBACK_MAX_PCT, BB_PULLBACK_RSI_MAX,
    BB_OI_24H_MIN, BB_OI_4H_MIN, BB_PARABOLIC_MAX_PCT,
    BB_REQUIRE_ABOVE_MID, BB_REQUIRE_EXPANSION, BB_REJECT_FALSE_BREAKOUT,
    BB_REQUIRE_KC_SQUEEZE, BB_KC_SQUEEZE_BARS, BB_REQUIRE_KC_BREAKOUT,
    BB_SQUEEZE_MIN_KC_BARS, BB_SQUEEZE_SL_BUFFER_PCT,
    BB_SQUEEZE_REQUIRE_BULL_CLOSE, BB_SQUEEZE_RSI_MOMENTUM_MIN,
    BB_SQUEEZE_TP_BW_MULT,
    AUTO_BB_TP_PCT, AUTO_BB_SL_PCT,
    USE_EMA_FILTER,
    ENABLE_BB_SQUEEZE_SHORT,
)
from indicators import calculate_rsi


def try_bb_squeeze(
    d: dict,
    closes_15m: list[float],
    opens_15m: list[float] | None = None,
    lows_15m: list[float] | None = None,
) -> Optional[dict]:
    """
    BB_SQUEEZE ideal preset (Carter/TTM + pullback):
    1) ≥ MIN consecutive bars BB inside KC (energy build)
    2) BW squeeze (percentile OR cap) + expansion
    3) Close > upper, bullish candle, close > mid, RSI@breakout ≥ 50
    4) Not false breakout; pullback hold mid; vol ≥ 1.2x; OI
    5) SL = min(lows of squeeze zone) − buffer, capped
    6) Soft TP = max(fixed%, 1.5 × BW) → asymmetry + trail after TP1
    """
    if d.get("bb_upper") is None or d.get("bb_bandwidth") is None:
        return None
    if not closes_15m or len(closes_15m) < BB_PERIOD + 3:
        return None
    if USE_EMA_FILTER and d.get("ema50_1h") is not None and d["price"] < d["ema50_1h"]:
        return None

    oi24 = d.get("oi_change_24h")
    oi4 = d.get("oi_change_4h")
    if oi24 is None or oi24 < BB_OI_24H_MIN:
        return None
    if oi4 is None or oi4 < BB_OI_4H_MIN:
        return None

    bw = d["bb_bandwidth"]
    hist = d.get("bb_history_bw") or []
    hist_kc = d.get("kc_squeeze_hist") or []

    # --- 1) Min consecutive BB-inside-KC (Carter red dots) ---
    max_run = 0
    cur_run = 0
    for v in hist_kc:
        if v:
            cur_run += 1
            if cur_run > max_run:
                max_run = cur_run
        else:
            cur_run = 0
    if BB_REQUIRE_KC_SQUEEZE and max_run < BB_SQUEEZE_MIN_KC_BARS:
        return None

    # Also keep "fresh" any() check soft if min_run already passed
    if BB_REQUIRE_KC_SQUEEZE and not hist_kc:
        if not d.get("kc_squeeze_now"):
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

    # Bullish breakout candle
    if BB_SQUEEZE_REQUIRE_BULL_CLOSE and opens_15m and len(opens_15m) >= broke_idx:
        if closes_15m[-broke_idx] <= opens_15m[-broke_idx]:
            return None

    # Momentum proxy: breakout close > mid + RSI at breakout ≥ 50
    if mid is not None and closes_15m[-broke_idx] <= mid:
        return None
    rsi_at_bo = None
    end_bo = len(closes_15m) - broke_idx + 1
    if end_bo >= 15:
        from indicators import calculate_rsi
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

    pullback_pct = (breakout_high - d["price"]) / breakout_high * 100 if breakout_high > 0 else 0
    if pullback_pct < 0.15:
        return None
    if pullback_pct > BB_PULLBACK_MAX_PCT:
        return None

    if BB_REQUIRE_ABOVE_MID and mid is not None and d["price"] < mid:
        return None

    rsi_15 = d.get("rsi_15m")
    if rsi_15 is not None and rsi_15 > BB_PULLBACK_RSI_MAX:
        return None

    if len(closes_15m) < 3 or closes_15m[-1] <= closes_15m[-3]:
        return None

    # --- SL from entire squeeze zone (min low of consecutive KC-inside bars) ---
    entry = float(d["price"])
    zone_lows = []
    if hist_kc and lows_15m:
        i = 0
        # skip leading False (already fired / expansion bars)
        while i < len(hist_kc) and not hist_kc[i]:
            i += 1
        while i < len(hist_kc) and hist_kc[i]:
            # hist_kc[i] ↔ lows_15m[-(i+1)]
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
    max_sl = entry * (1 - AUTO_BB_SL_PCT / 100)
    if sl_price < max_sl:
        sl_price = max_sl
    if sl_price >= entry:
        sl_price = entry * (1 - 0.4 / 100)

    # Soft TP: max(fixed%, 1.5 × bandwidth at fire) — capture expansion
    tp_pct = max(float(AUTO_BB_TP_PCT), float(bw) * float(BB_SQUEEZE_TP_BW_MULT))
    tp_price = entry * (1 + tp_pct / 100)
    sl_pct = (entry - sl_price) / entry * 100 if entry > 0 else AUTO_BB_SL_PCT

    stars = 1
    if (oi24 or 0) >= BB_OI_24H_MIN * 1.5 and vol15 >= BB_BREAKOUT_VOL_MIN * 1.3:
        stars = 2
    if (
        stars == 2
        and (oi4 or 0) >= BB_OI_4H_MIN * 1.6
        and d.get("btc_1h", 0) >= -0.3
        and min_recent <= BB_SQUEEZE_MAX_BW * 0.75
    ):
        stars = 3
    if stars >= 2 and max_run >= BB_SQUEEZE_MIN_KC_BARS + 2:
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
        "entry_note": (
            f"KC×{max_run}→break→pullback | vol×{vol15:.2f} | "
            f"SL zone {sl_pct:.2f}% | TP soft {tp_pct:.2f}%"
        ),
    }



def try_bb_squeeze_short(
    d: dict,
    closes_15m: list[float],
    opens_15m: list[float] | None = None,
    highs_15m: list[float] | None = None,
) -> Optional[dict]:
    """Mirror of long squeeze for SHORT: squeeze → close below lower → bounce entry.
    Alert-only by default (enable auto via AUTO_TRADE_SIGNAL_TYPES=BB_SQUEEZE_SHORT).
    """
    if not ENABLE_BB_SQUEEZE_SHORT:
        return None
    if d.get("bb_lower") is None or d.get("bb_bandwidth") is None:
        return None
    if not closes_15m or len(closes_15m) < BB_PERIOD + 3:
        return None
    # Short prefers 24h not strongly up
    pc24 = d.get("price_change_24h")
    if pc24 is not None and pc24 > 8:
        return None

    oi24 = d.get("oi_change_24h")
    if oi24 is None or oi24 < BB_OI_24H_MIN * 0.5:
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
        # for short: bearish candle close < open
        if closes_15m[-broke_idx] >= opens_15m[-broke_idx]:
            return None

    if mid is not None and closes_15m[-broke_idx] >= mid:
        return None

    vol15 = d.get("vol_spike_15m") or 0.0
    if vol15 < BB_BREAKOUT_VOL_MIN:
        return None

    # bounce entry: price recovered a bit from breakout low
    price = d["price"]
    bounce = (price - breakout_low) / breakout_low * 100 if breakout_low > 0 else 0
    if bounce < 0.15 or bounce > BB_PULLBACK_MAX_PCT:
        return None
    if mid is not None and price > mid:
        return None

    entry = float(price)
    # SL above zone high
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
        "entry_note": f"SHORT squeeze KC×{max_run} vol×{vol15:.2f}",
    }
