"""
Backtest BB_LOWER v2 — mirrors scanner.try_bb_lower as closely as possible.

Entry (reclaim):
  1) 24h price trend >= BB_LOWER_TREND_24H_MIN
  2) prev 15m close < lower BB, current close >= lower BB (reclaim)
  3) not choppy (<= BB_LOWER_MAX_CHOP_PIERCES pierces in last 16 bars)
  4) volume on reclaim >= BB_LOWER_VOL_MIN x avg20
  5) optional: OI 24h >= BB_LOWER_OI_24H_MIN (if --oi and history available)

Exit:
  TP = mid BB (or fallback % if mid too close), SL = under min(lows, lower) - buffer
  (capped at AUTO_BB_LOWER_SL_PCT). Intrabar: SL before TP if both touched.

Usage:
  python backtest.py --symbol PRLUSDT --days 30
  python backtest.py --top 15 --days 14
  python backtest.py --top 10 --days 20 --no-oi

Telegram: /backtest [SYMBOL|TOP] [days]
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
    BB_LOWER_TREND_24H_MIN,
    BB_LOWER_RECLAIM,
    BB_LOWER_OI_24H_MIN,
    BB_LOWER_VOL_MIN,
    BB_LOWER_MAX_CHOP_PIERCES,
    BB_LOWER_SL_BUFFER_PCT,
    BB_LOWER_MIN_TP_PCT,
    BB_LOWER_FALLBACK_TP_PCT,
    AUTO_BB_LOWER_TP_PCT,
    AUTO_BB_LOWER_SL_PCT,
    POSITION_SIZE_USD,
    MIN_VOLUME_USD_24H,
    BLACKLIST,
    EMA_PERIOD,
    USE_EMA_FILTER,
)
from indicators import calculate_bollinger, calculate_ema

log = logging.getLogger("backtest")

FEE_PCT = 0.055
MAX_HOLD_BARS = 48


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
    trend24_pct: float = 0.0
    vol_spike: float = 0.0


@dataclass
class BacktestResult:
    symbol: str
    days: int
    bars: int
    signals: int = 0
    trades: list = field(default_factory=list)
    skipped_oi: int = 0
    use_oi: bool = False

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


async def _fetch_klines(session, symbol, interval, start_ms, end_ms):
    out = []
    cursor_end = end_ms
    base = BYBIT_BASE_URL.rstrip("/")
    while cursor_end > start_ms and len(out) < 20000:
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "end": cursor_end,
            "limit": 1000,
        }
        try:
            async with session.get(f"{base}/v5/market/kline", params=params, timeout=30) as r:
                data = await r.json(content_type=None)
        except Exception as e:
            log.warning(f"kline fetch {symbol}: {e}")
            break
        if not isinstance(data, dict) or data.get("retCode") != 0:
            break
        batch = data.get("result", {}).get("list", [])
        if not batch:
            break
        batch = list(reversed(batch))
        out = batch + out
        oldest = int(batch[0][0])
        if oldest <= start_ms:
            break
        cursor_end = oldest - 1
        await asyncio.sleep(0.04)

    seen = set()
    cleaned = []
    for k in out:
        ts = int(k[0])
        if ts < start_ms or ts > end_ms:
            continue
        if ts in seen:
            continue
        seen.add(ts)
        cleaned.append(k)
    cleaned.sort(key=lambda x: int(x[0]))
    return cleaned


async def _fetch_oi_4h(session, symbol, start_ms, end_ms):
    base = BYBIT_BASE_URL.rstrip("/")
    out = []
    cursor_end = end_ms
    for _ in range(30):
        params = {
            "category": "linear",
            "symbol": symbol,
            "intervalTime": "4h",
            "limit": 200,
            "endTime": cursor_end,
        }
        try:
            async with session.get(
                f"{base}/v5/market/open-interest", params=params, timeout=30
            ) as r:
                data = await r.json(content_type=None)
        except Exception as e:
            log.warning(f"OI fetch {symbol}: {e}")
            break
        if not isinstance(data, dict) or data.get("retCode") != 0:
            break
        batch = data.get("result", {}).get("list", [])
        if not batch:
            break
        for item in batch:
            try:
                ts = int(item.get("timestamp") or 0)
                oi = float(item.get("openInterest") or 0)
            except (TypeError, ValueError):
                continue
            if ts < start_ms:
                continue
            out.append((ts, oi))
        oldest = min(int(x.get("timestamp") or cursor_end) for x in batch)
        if oldest <= start_ms:
            break
        cursor_end = oldest - 1
        await asyncio.sleep(0.05)

    out.sort(key=lambda x: x[0])
    seen = set()
    deduped = []
    for ts, oi in out:
        if ts in seen:
            continue
        seen.add(ts)
        deduped.append((ts, oi))
    return deduped


def _oi_change_24h_at(oi_series, ts_ms):
    if not oi_series:
        return None
    end_i = None
    for i in range(len(oi_series) - 1, -1, -1):
        if oi_series[i][0] <= ts_ms:
            end_i = i
            break
    if end_i is None:
        return None
    target = ts_ms - 24 * 3600 * 1000
    start_i = None
    for i in range(end_i, -1, -1):
        if oi_series[i][0] <= target:
            start_i = i
            break
    if start_i is None:
        if end_i >= 6:
            start_i = end_i - 6
        else:
            return None
    oi0 = oi_series[start_i][1]
    oi1 = oi_series[end_i][1]
    if oi0 <= 0:
        return None
    return (oi1 - oi0) / oi0 * 100


def _simulate_exit(highs, lows, closes, entry_i, tp, sl):
    end = min(entry_i + MAX_HOLD_BARS, len(closes) - 1)
    for j in range(entry_i + 1, end + 1):
        lo, hi = lows[j], highs[j]
        hit_sl = lo <= sl
        hit_tp = hi >= tp
        if hit_sl and hit_tp:
            return j, sl, "SL"
        if hit_sl:
            return j, sl, "SL"
        if hit_tp:
            return j, tp, "TP"
    return end, closes[end], "TIMEOUT"


def _signal_at(
    i, closes, highs, lows, vols, bars_per_24h,
    closes_1h=None, oi24=None, require_oi=False,
):
    if i < BB_PERIOD + 4 or i < bars_per_24h:
        return None

    p0 = closes[i - bars_per_24h]
    p1 = closes[i]
    if p0 <= 0:
        return None
    trend24 = (p1 - p0) / p0 * 100
    if trend24 < BB_LOWER_TREND_24H_MIN:
        return None

    if USE_EMA_FILTER and closes_1h and len(closes_1h) >= EMA_PERIOD:
        ema = calculate_ema(closes_1h, EMA_PERIOD)
        if ema is not None and closes_1h[-1] < ema:
            return None

    bb_now = calculate_bollinger(closes[: i + 1], BB_PERIOD, BB_MULT)
    bb_prev = calculate_bollinger(closes[:i], BB_PERIOD, BB_MULT)
    if not bb_now or not bb_prev:
        return None

    close = closes[i]
    prev = closes[i - 1]
    lower_now = bb_now["lower"]
    lower_prev = bb_prev["lower"]
    mid = bb_now["middle"]
    upper = bb_now["upper"]

    if BB_LOWER_RECLAIM:
        if prev >= lower_prev:
            return None
        if close < lower_now:
            return None
    else:
        if close >= lower_now:
            return None

    pierces = 0
    look = min(16, i - BB_PERIOD)
    for k in range(look):
        end = i - k
        if end < BB_PERIOD:
            break
        b = calculate_bollinger(closes[: end + 1], BB_PERIOD, BB_MULT)
        if b and closes[end] < b["lower"]:
            pierces += 1
    if pierces > BB_LOWER_MAX_CHOP_PIERCES:
        return None

    vol_spike = 0.0
    if i >= 20 and len(vols) > i:
        avg_v = sum(vols[i - 20 : i]) / 20
        vol_spike = (vols[i] / avg_v) if avg_v > 0 else 0.0
        if vol_spike < BB_LOWER_VOL_MIN:
            return None

    if require_oi:
        if oi24 is None or oi24 < BB_LOWER_OI_24H_MIN:
            return None

    candle_low = min(lows[i - 1], lows[i], prev)
    sl_raw = min(candle_low, lower_prev, lower_now)
    sl_price = sl_raw * (1 - BB_LOWER_SL_BUFFER_PCT / 100)

    entry = close
    tp1_pct = (mid - entry) / entry * 100 if entry > 0 else 0.0
    if tp1_pct < BB_LOWER_MIN_TP_PCT:
        tp_price = entry * (1 + BB_LOWER_FALLBACK_TP_PCT / 100)
        if upper > entry:
            tp_price = min(tp_price, upper)
    else:
        tp_price = mid

    max_sl = entry * (1 - AUTO_BB_LOWER_SL_PCT / 100)
    if sl_price < max_sl:
        sl_price = max_sl
    if sl_price >= entry:
        sl_price = entry * (1 - 0.5 / 100)

    return {
        "price": entry,
        "tp": tp_price,
        "sl": sl_price,
        "trend24": trend24,
        "vol_spike": vol_spike,
    }


async def backtest_symbol(session, symbol, days=30, use_oi=False):
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - days * 86400 * 1000
    bars_per_24h = 96
    warm_ms = start_ms - (BB_PERIOD + bars_per_24h + 30) * 15 * 60 * 1000

    kl_15 = await _fetch_klines(session, symbol, "15", warm_ms, end_ms)
    kl_1h = await _fetch_klines(session, symbol, "60", warm_ms, end_ms)

    result = BacktestResult(symbol=symbol, days=days, bars=len(kl_15), use_oi=use_oi)
    if len(kl_15) < BB_PERIOD + bars_per_24h + 10:
        log.warning(f"{symbol}: not enough 15m bars ({len(kl_15)})")
        return result

    ts_15 = [int(k[0]) for k in kl_15]
    highs = [float(k[2]) for k in kl_15]
    lows = [float(k[3]) for k in kl_15]
    closes = [float(k[4]) for k in kl_15]
    vols = [float(k[5]) for k in kl_15]

    closes_1h_all = [float(k[4]) for k in kl_1h] if kl_1h else []
    ts_1h = [int(k[0]) for k in kl_1h] if kl_1h else []

    oi_series = []
    if use_oi:
        oi_series = await _fetch_oi_4h(session, symbol, warm_ms, end_ms)

    cooldown_until = -1
    start_i = 0
    while start_i < len(ts_15) and ts_15[start_i] < start_ms:
        start_i += 1

    i = max(start_i, BB_PERIOD + bars_per_24h + 5)
    while i < len(closes) - 2:
        if i <= cooldown_until:
            i += 1
            continue

        closes_1h_now = None
        if ts_1h:
            closes_1h_now = [c for t, c in zip(ts_1h, closes_1h_all) if t <= ts_15[i]]

        oi24 = _oi_change_24h_at(oi_series, ts_15[i]) if oi_series else None
        if use_oi and (oi24 is None or oi24 < BB_LOWER_OI_24H_MIN):
            result.skipped_oi += 1
            i += 1
            continue

        sig = _signal_at(
            i, closes, highs, lows, vols, bars_per_24h,
            closes_1h=closes_1h_now,
            oi24=oi24,
            require_oi=use_oi,
        )
        if not sig:
            i += 1
            continue

        result.signals += 1
        entry = sig["price"]
        tp = sig["tp"]
        sl = sig["sl"]
        exit_i, exit_px, reason = _simulate_exit(highs, lows, closes, i, tp, sl)

        pnl_pct = (exit_px - entry) / entry * 100 - 2 * FEE_PCT
        pnl_usd = POSITION_SIZE_USD * pnl_pct / 100

        result.trades.append(
            Trade(
                symbol=symbol,
                entry_ts=ts_15[i],
                entry_price=entry,
                tp=tp,
                sl=sl,
                exit_ts=ts_15[exit_i],
                exit_price=exit_px,
                reason=reason,
                pnl_pct=pnl_pct,
                pnl_usd=pnl_usd,
                bars_held=exit_i - i,
                trend24_pct=sig["trend24"],
                vol_spike=sig["vol_spike"],
            )
        )
        cooldown_until = exit_i + 6
        i = exit_i + 1

    return result


async def top_symbols(session, n=15):
    base = BYBIT_BASE_URL.rstrip("/")
    async with session.get(
        f"{base}/v5/market/tickers",
        params={"category": "linear"},
        timeout=30,
    ) as r:
        data = await r.json(content_type=None)
    rows = []
    for t in data.get("result", {}).get("list", []):
        sym = t.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        base_coin = sym.replace("USDT", "")
        if base_coin in BLACKLIST:
            continue
        try:
            turn = float(t.get("turnover24h") or 0)
            pc = float(t.get("price24hPcnt") or 0) * 100
        except (TypeError, ValueError):
            continue
        if turn < MIN_VOLUME_USD_24H:
            continue
        if pc < BB_LOWER_TREND_24H_MIN:
            continue
        rows.append((turn, sym))
    rows.sort(reverse=True)
    return [s for _, s in rows[:n]]


def format_result(r):
    mode = "reclaim + OI" if r.use_oi else "reclaim (no OI)"
    lines = [
        f"📊 <b>Backtest BB_LOWER v2</b> — <code>{r.symbol}</code>",
        f"Период: {r.days}д | баров 15m: {r.bars}",
        f"Режим: {mode}",
        f"Тренд 24ч ≥{BB_LOWER_TREND_24H_MIN:+.1f}% | reclaim → TP mid / SL structure",
        f"Сигналов: <b>{r.signals}</b> | сделок: <b>{len(r.trades)}</b>",
    ]
    if r.use_oi:
        lines.append(f"Пропущено по OI: {r.skipped_oi}")
    if not r.trades:
        lines.append("\n<i>Сделок нет — условия строгие или мало данных.</i>")
        return "\n".join(lines)

    tp_n = sum(1 for t in r.trades if t.reason == "TP")
    sl_n = sum(1 for t in r.trades if t.reason == "SL")
    to_n = sum(1 for t in r.trades if t.reason == "TIMEOUT")
    pf = r.profit_factor
    pf_s = "∞" if math.isinf(pf) else f"{pf:.2f}"

    lines += [
        f"Winrate: <b>{r.winrate:.1f}%</b> ({r.wins}W / {r.losses}L)",
        f"TP/SL/Timeout: {tp_n}/{sl_n}/{to_n}",
        f"Σ PnL: <b>${r.total_pnl_usd:+.2f}</b> (поз. ${POSITION_SIZE_USD})",
        f"Avg: {r.avg_pnl_pct:+.2f}% | PF: {pf_s}",
        f"Fallback TP +{BB_LOWER_FALLBACK_TP_PCT}% / SL cap −{AUTO_BB_LOWER_SL_PCT}% | fee {FEE_PCT}%×2",
        "",
        "<b>Последние сделки:</b>",
    ]
    for t in r.trades[-8:]:
        dt = datetime.fromtimestamp(t.entry_ts / 1000, tz=timezone.utc).strftime("%m-%d %H:%M")
        emoji = "✅" if t.pnl_usd > 0 else "🛑" if t.reason == "SL" else "⏱"
        lines.append(
            f"{emoji} {dt} {t.reason} {t.pnl_pct:+.2f}% "
            f"(тр24 {t.trend24_pct:+.1f}% | vol×{t.vol_spike:.1f})"
        )
    return "\n".join(lines)


def format_summary(results):
    all_tr = []
    for r in results:
        all_tr.extend(r.trades)
    lines = [
        f"📊 <b>Backtest BB_LOWER v2 — TOP{len(results)}</b>",
        f"Монет: {len(results)} | сделок: {len(all_tr)}",
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
    ranked = sorted(results, key=lambda x: x.total_pnl_usd, reverse=True)
    for r in ranked[:12]:
        if not r.trades:
            continue
        lines.append(
            f"• {r.symbol.replace('USDT', '')}: {len(r.trades)} сд. "
            f"WR {r.winrate:.0f}% PnL ${r.total_pnl_usd:+.2f}"
        )
    return "\n".join(lines)


async def run_backtest_cli(symbol, days, top, no_oi):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    use_oi = not no_oi
    async with aiohttp.ClientSession() as session:
        if top > 0:
            syms = await top_symbols(session, top)
            log.info(f"Top symbols: {syms}")
            results = []
            for s in syms:
                log.info(f"Backtesting {s}...")
                results.append(await backtest_symbol(session, s, days, use_oi=use_oi))
            text = format_summary(results)
            for tag in ("<b>", "</b>", "<code>", "</code>", "<i>", "</i>"):
                text = text.replace(tag, "")
            print(text)
        else:
            sym = symbol or "BTCUSDT"
            if not sym.endswith("USDT"):
                sym += "USDT"
            r = await backtest_symbol(session, sym, days, use_oi=use_oi)
            text = format_result(r)
            for tag in ("<b>", "</b>", "<code>", "</code>", "<i>", "</i>"):
                text = text.replace(tag, "")
            print(text)


def main():
    ap = argparse.ArgumentParser(description="BB_LOWER v2 backtest")
    ap.add_argument("--symbol", "-s", default=None)
    ap.add_argument("--days", "-d", type=int, default=30)
    ap.add_argument("--top", type=int, default=0)
    ap.add_argument("--no-oi", action="store_true")
    args = ap.parse_args()
    asyncio.run(run_backtest_cli(args.symbol, args.days, args.top, args.no_oi))


if __name__ == "__main__":
    main()
