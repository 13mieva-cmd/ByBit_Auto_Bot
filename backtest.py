"""
Backtest for BB_SQUEEZE (15m) and optional multi-symbol scan.

Usage:
  python backtest.py --symbol PRLUSDT --days 30
  python backtest.py --top 15 --days 14
  python backtest.py --symbol ARBUSDT --days 21 --no-oi

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
    BB_SQUEEZE_LOOKBACK, BB_SQUEEZE_PERCENTILE, BB_SQUEEZE_MAX_BW,
    BB_SQUEEZE_FRESH_BARS, BB_BREAKOUT_VOL_MIN,
    BB_PULLBACK_MAX_PCT, BB_PULLBACK_RSI_MAX, BB_OI_24H_MIN,
    BB_OI_4H_MIN, BB_PARABOLIC_MAX_PCT, BB_REQUIRE_ABOVE_MID,
    USE_EMA_FILTER, EMA_PERIOD,
    AUTO_BB_TP_PCT, AUTO_BB_SL_PCT,
    POSITION_SIZE_USD, MIN_VOLUME_USD_24H, BLACKLIST,
)
from indicators import calculate_rsi, calculate_ema, calculate_bollinger

log = logging.getLogger("backtest")

# Fees (Bybit taker ~0.055% each side) — optional drag on results
FEE_PCT = 0.055
# Max hold after entry (15m bars). 48 = 12h
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
    bw: float = 0.0
    pullback: float = 0.0
    vol_spike: float = 0.0


@dataclass
class BacktestResult:
    symbol: str
    days: int
    bars: int
    signals: int = 0
    trades: list[Trade] = field(default_factory=list)
    skipped_oi: int = 0
    skipped_ema: int = 0
    use_oi: bool = True

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


async def _fetch_klines(
    session: aiohttp.ClientSession,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
) -> list[list]:
    """Paginate Bybit klines (oldest → newest). Candle: [ts, o, h, l, c, vol, turnover]."""
    out: list[list] = []
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
        async with session.get(f"{base}/v5/market/kline", params=params, timeout=30) as r:
            data = await r.json(content_type=None)
        if not isinstance(data, dict) or data.get("retCode") != 0:
            break
        batch = data.get("result", {}).get("list", [])
        if not batch:
            break
        # API returns newest first
        batch = list(reversed(batch))
        out = batch + out
        oldest = int(batch[0][0])
        if oldest <= start_ms:
            break
        cursor_end = oldest - 1
        await asyncio.sleep(0.05)
    # Filter to window and dedupe by ts
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


async def _fetch_oi_1h(
    session: aiohttp.ClientSession,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> list[tuple[int, float]]:
    """Return list of (ts_ms, oi) oldest→newest. Best-effort; may be short history."""
    base = BYBIT_BASE_URL.rstrip("/")
    out: list[tuple[int, float]] = []
    cursor_end = end_ms
    for _ in range(30):
        params = {
            "category": "linear",
            "symbol": symbol,
            "intervalTime": "1h",
            "endTime": cursor_end,
            "limit": 200,
        }
        try:
            async with session.get(
                f"{base}/v5/market/open-interest", params=params, timeout=20
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
                ts = int(item.get("timestamp") or item.get("ts") or 0)
                oi = float(item["openInterest"])
                if start_ms <= ts <= end_ms:
                    out.append((ts, oi))
            except (KeyError, TypeError, ValueError):
                continue
        oldest = min(int(x.get("timestamp") or x.get("ts") or cursor_end) for x in batch)
        if oldest <= start_ms:
            break
        cursor_end = oldest - 1
        await asyncio.sleep(0.05)
    # dedupe
    by_ts = {ts: oi for ts, oi in out}
    return sorted(by_ts.items(), key=lambda x: x[0])



def _oi_change_hours_at(oi_series: list[tuple[int, float]], ts_ms: int, hours: int) -> Optional[float]:
    """OI change vs ~hours earlier at or before ts_ms."""
    if len(oi_series) < 2:
        return None
    cur = None
    for i in range(len(oi_series) - 1, -1, -1):
        if oi_series[i][0] <= ts_ms:
            cur = i
            break
    if cur is None:
        return None
    target = ts_ms - hours * 3600 * 1000
    prev = None
    for i in range(cur, -1, -1):
        if oi_series[i][0] <= target:
            prev = i
            break
    if prev is None:
        # approximate by bar count (1h series)
        step = max(1, hours)
        if cur >= step:
            prev = cur - step
        else:
            return None
    oi_now = oi_series[cur][1]
    oi_old = oi_series[prev][1]
    if oi_old <= 0:
        return None
    return (oi_now - oi_old) / oi_old * 100


def _oi_change_24h_at(oi_series: list[tuple[int, float]], ts_ms: int) -> Optional[float]:
    """Approx OI change vs ~24h earlier at or before ts_ms."""
    if len(oi_series) < 2:
        return None
    # find latest point <= ts
    cur = None
    for i in range(len(oi_series) - 1, -1, -1):
        if oi_series[i][0] <= ts_ms:
            cur = i
            break
    if cur is None:
        return None
    target = ts_ms - 24 * 3600 * 1000
    prev = None
    for i in range(cur, -1, -1):
        if oi_series[i][0] <= target:
            prev = i
            break
    if prev is None:
        # fall back to oldest available if within 36h
        if cur >= 20:
            prev = cur - 24 if cur >= 24 else 0
        else:
            return None
    oi_now = oi_series[cur][1]
    oi_old = oi_series[prev][1]
    if oi_old <= 0:
        return None
    return (oi_now - oi_old) / oi_old * 100


def _simulate_exit(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    entry_i: int,
    tp: float,
    sl: float,
) -> tuple[int, float, str]:
    """Intrabar: SL before TP if both touched (conservative)."""
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
    i: int,
    closes_15: list[float],
    vols_15: list[float],
    closes_1h: list[float],
    ts_15: list[int],
    oi_series: Optional[list[tuple[int, float]]],
    use_oi: bool,
) -> Optional[dict]:
    """Evaluate BB_SQUEEZE at bar i using only data available up to i (no look-ahead)."""
    need = BB_PERIOD + BB_SQUEEZE_LOOKBACK + 5
    if i < need or i < 3:
        return None

    window = closes_15[: i + 1]
    price = window[-1]

    # EMA50 on 1h — map 15m ts to latest 1h close
    if USE_EMA_FILTER:
        if len(closes_1h) < EMA_PERIOD:
            return None
        ema50 = calculate_ema(closes_1h, EMA_PERIOD)
        if ema50 is None or price < ema50:
            return None
    else:
        ema50 = None

    # OI filter: 24h + 4h (устойчивый приток)
    oi_chg = None
    oi_4h = None
    if use_oi and oi_series:
        oi_chg = _oi_change_hours_at(oi_series, ts_15[i], 24)
        oi_4h = _oi_change_hours_at(oi_series, ts_15[i], 4)
        if oi_chg is None or oi_chg < BB_OI_24H_MIN:
            return None
        if oi_4h is None or oi_4h < BB_OI_4H_MIN:
            return None
    elif use_oi:
        return None  # requested OI but no data

    bb = calculate_bollinger(window, BB_PERIOD, BB_MULT)
    if not bb:
        return None

    # bandwidth history (newest first, same as live bot)
    hist = []
    for k in range(BB_SQUEEZE_LOOKBACK):
        end = len(window) - k
        if end < BB_PERIOD:
            break
        b = calculate_bollinger(window[:end], BB_PERIOD, BB_MULT)
        if b:
            hist.append(b["bandwidth"])

    bw = bb["bandwidth"]
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

    if min_recent > 0 and bw > min_recent * 1.8 and bw > BB_SQUEEZE_MAX_BW * 1.5:
        return None

    # breakout
    broke = False
    breakout_high = price
    look = min(3, len(window))
    for off in range(1, look + 1):
        c = window[-off]
        if c > bb["upper"]:
            broke = True
            breakout_high = max(breakout_high, c)
    if not broke:
        return None

    # volume spike 15m
    if i < 20:
        return None
    avg_v = sum(vols_15[i - 20 : i]) / 20
    vol_spike = (vols_15[i] / avg_v) if avg_v > 0 else 0.0
    if vol_spike < BB_BREAKOUT_VOL_MIN:
        return None

    # Anti-parabolic spike over last ~30m
    if len(window) >= 3:
        local_low = min(window[-3], window[-2], window[-1])
        if local_low > 0:
            spike_pct = (window[-1] - local_low) / local_low * 100
            if spike_pct > BB_PARABOLIC_MAX_PCT:
                return None

    pullback_pct = (breakout_high - price) / breakout_high * 100 if breakout_high > 0 else 0
    if pullback_pct < 0.15 or pullback_pct > BB_PULLBACK_MAX_PCT:
        return None

    if BB_REQUIRE_ABOVE_MID and price < bb["middle"]:
        return None

    rsi_15 = calculate_rsi(window, 14)
    if rsi_15 is not None and rsi_15 > BB_PULLBACK_RSI_MAX:
        return None

    if window[-1] <= window[-3]:
        return None

    return {
        "price": price,
        "bw": bw,
        "pullback": pullback_pct,
        "vol_spike": vol_spike,
        "oi_chg": oi_chg,
        "ema50": ema50,
    }


async def backtest_symbol(
    session: aiohttp.ClientSession,
    symbol: str,
    days: int = 30,
    use_oi: bool = True,
) -> BacktestResult:
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - days * 86400 * 1000
    # warm-up for indicators
    warm_ms = start_ms - (BB_PERIOD + BB_SQUEEZE_LOOKBACK + 50) * 15 * 60 * 1000

    kl_15 = await _fetch_klines(session, symbol, "15", warm_ms, end_ms)
    kl_1h = await _fetch_klines(session, symbol, "60", warm_ms, end_ms)

    result = BacktestResult(symbol=symbol, days=days, bars=len(kl_15), use_oi=use_oi)
    if len(kl_15) < BB_PERIOD + BB_SQUEEZE_LOOKBACK + 10:
        log.warning(f"{symbol}: not enough 15m bars ({len(kl_15)})")
        return result

    ts_15 = [int(k[0]) for k in kl_15]
    opens = [float(k[1]) for k in kl_15]
    highs = [float(k[2]) for k in kl_15]
    lows = [float(k[3]) for k in kl_15]
    closes = [float(k[4]) for k in kl_15]
    vols = [float(k[5]) for k in kl_15]
    closes_1h_all = [float(k[4]) for k in kl_1h]
    ts_1h = [int(k[0]) for k in kl_1h]

    oi_series = None
    if use_oi:
        oi_series = await _fetch_oi_1h(session, symbol, warm_ms, end_ms)
        if len(oi_series) < 10:
            log.warning(f"{symbol}: OI history thin ({len(oi_series)}), OI filter may block all")

    # Prebuild 1h close series available at each 15m bar (no look-ahead)
    def closes_1h_at(ts: int) -> list[float]:
        out = []
        for t, c in zip(ts_1h, closes_1h_all):
            if t <= ts:
                out.append(c)
            else:
                break
        return out

    cooldown_until = -1
    start_i = 0
    while start_i < len(ts_15) and ts_15[start_i] < start_ms:
        start_i += 1

    i = max(start_i, BB_PERIOD + BB_SQUEEZE_LOOKBACK + 5)
    while i < len(closes) - 2:
        if i <= cooldown_until:
            i += 1
            continue

        c1h = closes_1h_at(ts_15[i])
        sig = _signal_at(i, closes, vols, c1h, ts_15, oi_series, use_oi)
        if not sig:
            i += 1
            continue

        result.signals += 1
        entry = sig["price"]
        tp = entry * (1 + AUTO_BB_TP_PCT / 100)
        sl = entry * (1 - AUTO_BB_SL_PCT / 100)
        exit_i, exit_px, reason = _simulate_exit(highs, lows, closes, i, tp, sl)

        # fees both sides
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
                bw=sig["bw"],
                pullback=sig["pullback"],
                vol_spike=sig["vol_spike"],
            )
        )
        # no overlapping trades; small cooldown 6 bars after exit
        cooldown_until = exit_i + 6
        i = exit_i + 1

    return result


async def top_symbols(session: aiohttp.ClientSession, n: int = 15) -> list[str]:
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
        except (TypeError, ValueError):
            continue
        if turn < MIN_VOLUME_USD_24H:
            continue
        rows.append((turn, sym))
    rows.sort(reverse=True)
    return [s for _, s in rows[:n]]


def format_result(r: BacktestResult) -> str:
    lines = [
        f"📊 <b>Backtest BB_SQUEEZE</b> — <code>{r.symbol}</code>",
        f"Период: {r.days}д | баров 15m: {r.bars}",
        f"OI-фильтр: {'вкл' if r.use_oi else 'выкл'}",
        f"Сигналов: <b>{r.signals}</b> | сделок: <b>{len(r.trades)}</b>",
    ]
    if not r.trades:
        lines.append("\n<i>Сделок нет — условия слишком строгие или мало данных.</i>")
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
        f"TP +{AUTO_BB_TP_PCT}% / SL −{AUTO_BB_SL_PCT}% | fee {FEE_PCT}%×2",
        "",
        "<b>Последние сделки:</b>",
    ]
    for t in r.trades[-8:]:
        dt = datetime.fromtimestamp(t.entry_ts / 1000, tz=timezone.utc).strftime("%m-%d %H:%M")
        emoji = "✅" if t.pnl_usd > 0 else "🛑" if t.reason == "SL" else "⏱"
        lines.append(
            f"{emoji} {dt} {t.reason} {t.pnl_pct:+.2f}% "
            f"(bw={t.bw:.2f} vol×{t.vol_spike:.1f})"
        )
    return "\n".join(lines)


def format_summary(results: list[BacktestResult]) -> str:
    all_tr: list[Trade] = []
    for r in results:
        all_tr.extend(r.trades)
    lines = [
        f"📊 <b>Backtest BB_SQUEEZE — TOP{len(results)}</b>",
        f"Монет с данными: {len(results)} | сделок: {len(all_tr)}",
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
            f"• {r.symbol.replace('USDT','')}: {len(r.trades)} сд. "
            f"WR {r.winrate:.0f}% PnL ${r.total_pnl_usd:+.2f}"
        )
    return "\n".join(lines)


async def run_backtest_cli(symbol: Optional[str], days: int, top: int, no_oi: bool):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    async with aiohttp.ClientSession() as session:
        if top > 0:
            syms = await top_symbols(session, top)
            log.info(f"Top symbols: {syms}")
            results = []
            for s in syms:
                log.info(f"Backtesting {s}...")
                results.append(await backtest_symbol(session, s, days, use_oi=not no_oi))
            print(format_summary(results).replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", "").replace("<i>", "").replace("</i>", ""))
        else:
            sym = symbol or "BTCUSDT"
            if not sym.endswith("USDT"):
                sym += "USDT"
            r = await backtest_symbol(session, sym, days, use_oi=not no_oi)
            print(format_result(r).replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", "").replace("<i>", "").replace("</i>", ""))


def main():
    ap = argparse.ArgumentParser(description="BB_SQUEEZE backtest")
    ap.add_argument("--symbol", "-s", default=None)
    ap.add_argument("--days", "-d", type=int, default=30)
    ap.add_argument("--top", type=int, default=0, help="Backtest top N by turnover")
    ap.add_argument("--no-oi", action="store_true", help="Disable OI filter")
    args = ap.parse_args()
    asyncio.run(run_backtest_cli(args.symbol, args.days, args.top, args.no_oi))


if __name__ == "__main__":
    main()
