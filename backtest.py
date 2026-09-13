"""
Backtest BB_SQUEEZE (parity with live scanner.try_bb_squeeze).

Entry mirrors live:
  - BW squeeze (percentile OR cap)
  - >= MIN consecutive bars BB inside Keltner
  - expansion, close > upper, bullish candle, close > mid
  - RSI at breakout >= 50 (approx)
  - pullback, hold mid, vol >= 1.2x
  - SL = min(lows of squeeze zone) - buffer, cap AUTO_BB_SL_PCT
  - Soft TP = max(AUTO_BB_TP_PCT, BW * mult)
  - BE @ +0.6%, partial concept via trail after TP1 trigger, structure exit

Usage:
  python backtest.py --symbol BTCUSDT --days 20
  python backtest.py --top 15 --days 14
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from config import (
    BYBIT_BASE_URL,
    BB_PERIOD, BB_MULT,
    BB_SQUEEZE_LOOKBACK, BB_SQUEEZE_PERCENTILE, BB_SQUEEZE_MAX_BW,
    BB_SQUEEZE_FRESH_BARS, BB_BREAKOUT_VOL_MIN,
    BB_PULLBACK_MAX_PCT, BB_PULLBACK_RSI_MAX,
    BB_PARABOLIC_MAX_PCT, BB_REQUIRE_ABOVE_MID,
    BB_REQUIRE_EXPANSION, BB_REJECT_FALSE_BREAKOUT,
    BB_REQUIRE_KC_SQUEEZE, BB_SQUEEZE_MIN_KC_BARS,
    BB_SQUEEZE_SL_BUFFER_PCT, BB_SQUEEZE_REQUIRE_BULL_CLOSE,
    BB_SQUEEZE_RSI_MOMENTUM_MIN, BB_SQUEEZE_TP_BW_MULT,
    KC_EMA_PERIOD, KC_ATR_PERIOD, KC_ATR_MULT,
    AUTO_BB_TP_PCT, AUTO_BB_SL_PCT,
    POSITION_SIZE_USD, MIN_VOLUME_USD_24H, BLACKLIST,
    EMA_PERIOD, USE_EMA_FILTER,
    AUTO_BE_ENABLED, AUTO_BE_TRIGGER_PCT, AUTO_BE_BUFFER_PCT,
    STRUCTURE_EXIT_ENABLED, STRUCTURE_EXIT_EMA_1H,
    AUTO_TRAIL_ENABLED, AUTO_TP1_TRIGGER_PCT_BB, AUTO_TRAIL_DISTANCE_PCT_BB,
)
from indicators import (
    calculate_bollinger, calculate_ema, calculate_rsi,
    calculate_keltner, bb_inside_keltner,
)

log = logging.getLogger("backtest")

FEE_PCT = 0.055  # one side
MAX_HOLD_BARS = 64  # ~16h on 15m — closer to 8-10 bar fire + extension


@dataclass
class Trade:
    symbol: str
    entry_ts: int
    entry_price: float
    tp: float
    sl: float
    exit_ts: int = 0
    exit_price: float = 0.0
    reason: str = ""
    pnl_pct: float = 0.0
    pnl_usd: float = 0.0
    bars_held: int = 0
    bw: float = 0.0
    squeeze_bars: int = 0
    vol_spike: float = 0.0


@dataclass
class BacktestResult:
    symbol: str
    days: int
    bars: int
    signals: int = 0
    trades: list = field(default_factory=list)

    @property
    def wins(self) -> int:
        return sum(1 for t in self.trades if t.pnl_usd > 0)

    @property
    def losses(self) -> int:
        return sum(1 for t in self.trades if t.pnl_usd <= 0)

    @property
    def winrate(self) -> float:
        n = len(self.trades)
        return (self.wins / n * 100) if n else 0.0

    @property
    def total_pnl_usd(self) -> float:
        return sum(t.pnl_usd for t in self.trades)

    @property
    def avg_pnl_pct(self) -> float:
        n = len(self.trades)
        return (sum(t.pnl_pct for t in self.trades) / n) if n else 0.0

    @property
    def profit_factor(self) -> float:
        gp = sum(t.pnl_usd for t in self.trades if t.pnl_usd > 0)
        gl = abs(sum(t.pnl_usd for t in self.trades if t.pnl_usd < 0))
        if gl < 1e-9:
            return float("inf") if gp > 0 else 0.0
        return gp / gl


async def fetch_klines(session, symbol: str, interval: str, limit: int = 1000):
    base = BYBIT_BASE_URL.rstrip("/")
    # public market works on same host for demo/main often
    url = f"{base}/v5/market/kline"
    params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=20)) as r:
            data = await r.json(content_type=None)
        if not isinstance(data, dict) or data.get("retCode") != 0:
            return []
        rows = data.get("result", {}).get("list", [])
        # Bybit: newest first → reverse to oldest first
        rows = list(reversed(rows))
        return rows
    except Exception as e:
        log.warning(f"klines {symbol}: {e}")
        return []


def _simulate_exit(
    highs, lows, closes,
    entry_i, tp, sl, entry_price,
    closes_1h_aligned=None,
):
    """SL first; BE; trail after TP1; optional structure; soft TP."""
    end = min(entry_i + MAX_HOLD_BARS, len(closes) - 1)
    be_active = False
    trail_active = False
    cur_sl = sl
    peak = entry_price
    for j in range(entry_i + 1, end + 1):
        lo, hi, cl = lows[j], highs[j], closes[j]
        if hi > peak:
            peak = hi
        gain = (cl - entry_price) / entry_price * 100 if entry_price > 0 else 0

        if AUTO_BE_ENABLED and not be_active and gain >= AUTO_BE_TRIGGER_PCT:
            be_active = True
            cur_sl = max(cur_sl, entry_price * (1 + AUTO_BE_BUFFER_PCT / 100))

        if AUTO_TRAIL_ENABLED and not trail_active and gain >= AUTO_TP1_TRIGGER_PCT_BB:
            trail_active = True
        if trail_active:
            trail_sl = peak * (1 - AUTO_TRAIL_DISTANCE_PCT_BB / 100)
            cur_sl = max(cur_sl, trail_sl)

        if STRUCTURE_EXIT_ENABLED and STRUCTURE_EXIT_EMA_1H and closes_1h_aligned is not None:
            if len(closes_1h_aligned) >= EMA_PERIOD:
                ema = calculate_ema(closes_1h_aligned, EMA_PERIOD)
                if ema is not None and cl < ema and gain < 0:
                    return j, cl, "STRUCTURE"

        hit_sl = lo <= cur_sl
        # Soft TP only if not trailing yet (once trail on, TP cancelled like live)
        hit_tp = (not trail_active) and hi >= tp
        if hit_sl and hit_tp:
            return j, cur_sl, "BE" if be_active else "SL"
        if hit_sl:
            if trail_active:
                return j, cur_sl, "TRAILING"
            if be_active and abs(cur_sl - entry_price) / max(entry_price, 1e-12) < 0.005:
                return j, cur_sl, "BE"
            return j, cur_sl, "SL"
        if hit_tp:
            return j, tp, "TP"
    return end, closes[end], "TIMEOUT"


def signal_at(i, opens, highs, lows, closes, volumes):
    """Return signal dict or None at bar i (index in oldest-first arrays)."""
    if i < BB_PERIOD + BB_SQUEEZE_LOOKBACK + 5:
        return None
    if i >= len(closes) - 1:
        return None

    # Need history up to i inclusive
    c = closes[: i + 1]
    h = highs[: i + 1]
    l = lows[: i + 1]
    o = opens[: i + 1]
    v = volumes[: i + 1]

    bb = calculate_bollinger(c, BB_PERIOD, BB_MULT)
    if not bb:
        return None
    kc = calculate_keltner(h, l, c, KC_EMA_PERIOD, KC_ATR_PERIOD, KC_ATR_MULT)
    if not kc:
        return None

    # Build bw hist + kc hist newest-first relative to i
    bb_history_bw = []
    kc_squeeze_hist = []
    atr_need = max(KC_EMA_PERIOD, KC_ATR_PERIOD) + 1
    for back in range(BB_SQUEEZE_LOOKBACK):
        end = len(c) - back
        if end < max(BB_PERIOD, atr_need):
            break
        b = calculate_bollinger(c[:end], BB_PERIOD, BB_MULT)
        k = calculate_keltner(h[:end], l[:end], c[:end], KC_EMA_PERIOD, KC_ATR_PERIOD, KC_ATR_MULT)
        if b:
            bb_history_bw.append(b["bandwidth"])
        if b and k:
            kc_squeeze_hist.append(bb_inside_keltner(b, k))

    max_run = 0
    cur = 0
    for flag in kc_squeeze_hist:
        if flag:
            cur += 1
            max_run = max(max_run, cur)
        else:
            cur = 0
    if BB_REQUIRE_KC_SQUEEZE and max_run < BB_SQUEEZE_MIN_KC_BARS:
        return None

    bw = bb["bandwidth"]
    hist = bb_history_bw
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

    upper = bb["upper"]
    mid = bb.get("middle")

    # breakout in last 1-3 bars ending at i
    broke = False
    breakout_high = c[-1]
    broke_idx = None  # 1 = current bar relative to end
    look = min(3, len(c))
    for bi in range(1, look + 1):
        if c[-bi] > upper:
            broke = True
            if c[-bi] >= breakout_high:
                breakout_high = c[-bi]
                broke_idx = bi
    if not broke or broke_idx is None:
        return None

    if BB_SQUEEZE_REQUIRE_BULL_CLOSE and o[-broke_idx] >= c[-broke_idx]:
        return None

    if mid is not None and c[-broke_idx] <= mid:
        return None

    end_bo = len(c) - broke_idx + 1
    if end_bo >= 15:
        rsi_bo = calculate_rsi(c[:end_bo], 14)
        if rsi_bo is not None and rsi_bo < BB_SQUEEZE_RSI_MOMENTUM_MIN:
            return None

    if BB_REJECT_FALSE_BREAKOUT and mid is not None:
        n = len(c)
        for ii in range(max(0, n - 5), n):
            if c[ii] > upper:
                for jj in range(ii + 1, n):
                    if c[jj] < mid:
                        return None
                break

    # volume spike on last bar
    if len(v) < 21:
        return None
    avg_v = sum(v[-21:-1]) / 20
    if avg_v <= 0:
        return None
    vol15 = v[-1] / avg_v
    if vol15 < BB_BREAKOUT_VOL_MIN:
        return None

    if len(c) >= 3:
        local_low = min(c[-3], c[-2], c[-1])
        if local_low > 0:
            spike_pct = (c[-1] - local_low) / local_low * 100
            if spike_pct > BB_PARABOLIC_MAX_PCT:
                return None

    price = c[-1]
    pullback_pct = (breakout_high - price) / breakout_high * 100 if breakout_high > 0 else 0
    if pullback_pct > BB_PULLBACK_MAX_PCT:
        return None

    if BB_REQUIRE_ABOVE_MID and mid is not None and price < mid:
        return None

    rsi_15 = calculate_rsi(c, 14)
    if rsi_15 is not None and rsi_15 > BB_PULLBACK_RSI_MAX:
        return None

    if len(c) < 3 or c[-1] <= c[-3]:
        return None

    # Zone SL
    zone_lows = []
    if kc_squeeze_hist:
        zi = 0
        while zi < len(kc_squeeze_hist) and not kc_squeeze_hist[zi]:
            zi += 1
        while zi < len(kc_squeeze_hist) and kc_squeeze_hist[zi]:
            if len(l) > zi:
                zone_lows.append(l[-(zi + 1)])
            zi += 1
    if not zone_lows:
        zone_lows = [l[-broke_idx]]
    sl_raw = min(zone_lows)
    if mid is not None:
        sl_raw = min(sl_raw, mid)
    sl_price = sl_raw * (1 - BB_SQUEEZE_SL_BUFFER_PCT / 100)
    max_sl = price * (1 - AUTO_BB_SL_PCT / 100)
    if sl_price < max_sl:
        sl_price = max_sl
    if sl_price >= price:
        sl_price = price * (1 - 0.4 / 100)

    tp_pct = max(float(AUTO_BB_TP_PCT), float(bw) * float(BB_SQUEEZE_TP_BW_MULT))
    tp_price = price * (1 + tp_pct / 100)

    return {
        "entry": price,
        "tp": tp_price,
        "sl": sl_price,
        "bw": bw,
        "squeeze_bars": max_run,
        "vol_spike": vol15,
        "tp_pct": tp_pct,
        "sl_pct": (price - sl_price) / price * 100,
    }


async def backtest_symbol(session, symbol: str, days: int) -> BacktestResult:
    limit = min(1000, days * 96 + 100)
    rows = await fetch_klines(session, symbol, "15", limit)
    if len(rows) < BB_PERIOD + 50:
        return BacktestResult(symbol=symbol, days=days, bars=len(rows))

    opens = [float(r[1]) for r in rows]
    highs = [float(r[2]) for r in rows]
    lows = [float(r[3]) for r in rows]
    closes = [float(r[4]) for r in rows]
    volumes = [float(r[5]) for r in rows]
    ts = [int(r[0]) for r in rows]

    # 1h for structure / EMA filter
    rows_1h = await fetch_klines(session, symbol, "60", min(500, days * 24 + 50))
    closes_1h = [float(r[4]) for r in rows_1h] if rows_1h else []
    ts_1h = [int(r[0]) for r in rows_1h] if rows_1h else []

    result = BacktestResult(symbol=symbol, days=days, bars=len(closes))
    cooldown_until = 0

    for i in range(BB_PERIOD + BB_SQUEEZE_LOOKBACK + 5, len(closes) - 2):
        if ts[i] < cooldown_until:
            continue

        # medium: no hard 24h uptrend requirement (was blocking all trades)

        if USE_EMA_FILTER and closes_1h:
            # last 1h bar at or before this 15m ts
            ema = calculate_ema(closes_1h, EMA_PERIOD)
            if ema is not None and closes[i] < ema:
                continue

        sig = signal_at(i, opens, highs, lows, closes, volumes)
        if not sig:
            continue

        result.signals += 1
        entry = sig["entry"]
        tp = sig["tp"]
        sl = sig["sl"]

        # align 1h closes available at this time for structure
        closes_1h_now = [closes_1h[j] for j, t in enumerate(ts_1h) if t <= ts[i]]

        exit_i, exit_px, reason = _simulate_exit(
            highs, lows, closes, i, tp, sl, entry, closes_1h_now
        )

        pnl_pct = (exit_px - entry) / entry * 100 - 2 * FEE_PCT
        pnl_usd = POSITION_SIZE_USD * pnl_pct / 100
        tr = Trade(
            symbol=symbol,
            entry_ts=ts[i],
            entry_price=entry,
            tp=tp,
            sl=sl,
            exit_ts=ts[exit_i],
            exit_price=exit_px,
            reason=reason,
            pnl_pct=pnl_pct,
            pnl_usd=pnl_usd,
            bars_held=exit_i - i,
            bw=sig["bw"],
            squeeze_bars=sig["squeeze_bars"],
            vol_spike=sig["vol_spike"],
        )
        result.trades.append(tr)
        # cooldown 48h in bars ~ 192
        cooldown_until = ts[exit_i] + 48 * 3600 * 1000

    return result


def format_result(r: BacktestResult) -> str:
    lines = [
        f"📊 <b>Backtest BB_SQUEEZE</b> — <code>{r.symbol}</code>",
        f"Период: {r.days}д | баров 15m: {r.bars}",
        f"Сигналов: <b>{r.signals}</b> | сделок: <b>{len(r.trades)}</b>",
        f"Правила: KC≥{BB_SQUEEZE_MIN_KC_BARS} | vol≥{BB_BREAKOUT_VOL_MIN}× | zone SL cap {AUTO_BB_SL_PCT}%",
        f"Soft TP max({AUTO_BB_TP_PCT}%, {BB_SQUEEZE_TP_BW_MULT}×BW) | BE+trail",
    ]
    if not r.trades:
        lines.append("\n<i>Сделок нет.</i>")
        return "\n".join(lines)

    reasons = {}
    for t in r.trades:
        reasons[t.reason] = reasons.get(t.reason, 0) + 1
    pf = r.profit_factor
    pf_s = "∞" if math.isinf(pf) else f"{pf:.2f}"
    lines += [
        f"Winrate: <b>{r.winrate:.1f}%</b> ({r.wins}W / {r.losses}L)",
        f"Reasons: {reasons}",
        f"Σ PnL: <b>${r.total_pnl_usd:+.2f}</b> (size ${POSITION_SIZE_USD})",
        f"Avg: {r.avg_pnl_pct:+.2f}% | PF: {pf_s}",
        "",
        "<b>Последние сделки:</b>",
    ]
    for t in r.trades[-10:]:
        dt = datetime.fromtimestamp(t.entry_ts / 1000, tz=timezone.utc).strftime("%m-%d %H:%M")
        emoji = "✅" if t.pnl_usd > 0 else "🛑"
        lines.append(
            f"{emoji} {dt} {t.reason} {t.pnl_pct:+.2f}% "
            f"(KC×{t.squeeze_bars} BW{t.bw:.1f} vol×{t.vol_spike:.1f})"
        )
    return "\n".join(lines)



async def top_symbols(session, top_n: int = 15) -> list:
    """Top linear USDT symbols by 24h turnover (for /backtest TOP)."""
    base = BYBIT_BASE_URL.rstrip("/")
    try:
        async with session.get(
            f"{base}/v5/market/tickers",
            params={"category": "linear"},
            timeout=aiohttp.ClientTimeout(total=20),
        ) as resp:
            data = await resp.json(content_type=None)
    except Exception as e:
        log.warning(f"top_symbols: {e}")
        return []
    tickers = data.get("result", {}).get("list", []) if isinstance(data, dict) else []
    scored = []
    for t in tickers:
        sym = t.get("symbol") or ""
        if not sym.endswith("USDT"):
            continue
        if sym.replace("USDT", "") in BLACKLIST:
            continue
        try:
            turn = float(t.get("turnover24h") or 0)
        except (TypeError, ValueError):
            turn = 0.0
        if turn < MIN_VOLUME_USD_24H:
            continue
        scored.append((turn, sym))
    scored.sort(reverse=True)
    return [s for _, s in scored[:top_n]]


def format_summary(results):
    all_tr = []
    for r in results:
        all_tr.extend(r.trades)
    lines = [
        f"📊 <b>Backtest BB_SQUEEZE — {len(results)} symbols</b>",
        f"Сделок: {len(all_tr)}",
    ]
    if not all_tr:
        lines.append("<i>Сделок нет.</i>")
        return "\n".join(lines)
    wins = sum(1 for t in all_tr if t.pnl_usd > 0)
    total = sum(t.pnl_usd for t in all_tr)
    wr = wins / len(all_tr) * 100
    gp = sum(t.pnl_usd for t in all_tr if t.pnl_usd > 0)
    gl = abs(sum(t.pnl_usd for t in all_tr if t.pnl_usd < 0))
    pf = (gp / gl) if gl > 1e-9 else (float("inf") if gp > 0 else 0.0)
    pf_s = "∞" if math.isinf(pf) else f"{pf:.2f}"
    lines += [
        f"Winrate: <b>{wr:.1f}%</b> ({wins}W / {len(all_tr) - wins}L)",
        f"Σ PnL: <b>${total:+.2f}</b> | PF: {pf_s}",
        "",
        "<b>По монетам:</b>",
    ]
    for r in sorted(results, key=lambda x: x.total_pnl_usd, reverse=True)[:15]:
        lines.append(
            f"{r.symbol}: {len(r.trades)}tr WR{r.winrate:.0f}% ${r.total_pnl_usd:+.2f}"
        )
    return "\n".join(lines)


async def main_async(args):
    logging.basicConfig(level=logging.INFO)
    async with aiohttp.ClientSession() as session:
        if args.symbol:
            r = await backtest_symbol(session, args.symbol.upper().replace("USDT", "") + "USDT"
                                      if not args.symbol.upper().endswith("USDT")
                                      else args.symbol.upper(), args.days)
            print(format_result(r).replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", "").replace("<i>", "").replace("</i>", ""))
        else:
            # top by turnover from tickers
            base = BYBIT_BASE_URL.rstrip("/")
            async with session.get(f"{base}/v5/market/tickers", params={"category": "linear"}) as resp:
                data = await resp.json(content_type=None)
            tickers = data.get("result", {}).get("list", []) if isinstance(data, dict) else []
            scored = []
            for t in tickers:
                sym = t.get("symbol", "")
                if not sym.endswith("USDT"):
                    continue
                base_sym = sym.replace("USDT", "")
                if base_sym in BLACKLIST:
                    continue
                try:
                    turn = float(t.get("turnover24h") or 0)
                except Exception:
                    turn = 0
                if turn < MIN_VOLUME_USD_24H:
                    continue
                scored.append((turn, sym))
            scored.sort(reverse=True)
            symbols = [s for _, s in scored[: args.top]]
            results = []
            for sym in symbols:
                r = await backtest_symbol(session, sym, args.days)
                results.append(r)
                log.info(f"{sym}: {len(r.trades)} trades, WR {r.winrate:.0f}%")
                await asyncio.sleep(0.15)
            print(format_summary(results).replace("<b>", "").replace("</b>", "").replace("<i>", "").replace("</i>", ""))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", type=str, default="")
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--days", type=int, default=14)
    args = ap.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
