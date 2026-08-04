"""
Backtest for BB_LOWER — "buy the dip": coin in a confirmed 24h uptrend,
enter long the moment price closes (candle body) below the lower Bollinger
Band. TP/SL: AUTO_BB_LOWER_TP_PCT / AUTO_BB_LOWER_SL_PCT. Optional multi-symbol scan.

Usage:
  python backtest.py --symbol PRLUSDT --days 30
  python backtest.py --top 15 --days 14

Note: --no-oi is accepted but is a no-op — OI confirmation isn't part of BB_LOWER.
This does NOT backtest BB_SQUEEZE (the separate squeeze/breakout signal) — that
would need its own signal-reconstruction function mirroring try_bb_squeeze().

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
    BB_PERIOD, BB_MULT, BB_LOWER_TREND_24H_MIN,
    AUTO_BB_LOWER_TP_PCT, AUTO_BB_LOWER_SL_PCT,
    POSITION_SIZE_USD, MIN_VOLUME_USD_24H, BLACKLIST,
)
from indicators import calculate_bollinger

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
    break_pct: float = 0.0
    trend24_pct: float = 0.0


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


def _signal_at(i: int, closes_15: list[float], bars_per_24h: int) -> Optional[dict]:
    """Evaluate BB_SQUEEZE (repurposed: buy-the-dip) at bar i, no look-ahead.
    Mirrors scanner.py's _bb_squeeze_eval() exactly — same gates, same order.

    1) Монета в восходящем тренде за 24ч (по прошлым 15m-барам, bars_per_24h назад).
    2) Цена закрытием СВЕЧИ пробивает нижнюю BB — именно момент пробоя.
    """
    if i < BB_PERIOD + 2 or i < bars_per_24h:
        return None

    window = closes_15[: i + 1]
    price = window[-1]

    price_24h_ago = closes_15[i - bars_per_24h]
    trend24 = ((price - price_24h_ago) / price_24h_ago * 100) if price_24h_ago > 0 else None
    if trend24 is None or trend24 < BB_LOWER_TREND_24H_MIN:
        return None

    bb = calculate_bollinger(window, BB_PERIOD, BB_MULT)
    if not bb:
        return None

    lower = bb["lower"]
    prev = window[-2]
    if not (price < lower and prev >= lower):
        return None

    break_pct = (lower - price) / lower * 100 if lower else 0

    return {
        "price": price,
        "break_pct": break_pct,
        "trend24": trend24,
    }


async def backtest_symbol(
    session: aiohttp.ClientSession,
    symbol: str,
    days: int = 30,
    use_oi: bool = True,
) -> BacktestResult:
    """use_oi is accepted for CLI/caller compatibility but no longer affects
    BB_SQUEEZE, which dropped its OI confirmation when the signal was
    repurposed into a simple dip-buy (see _signal_at)."""
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - days * 86400 * 1000

    bb_timeframe_min = 15  # scanner.py hardcodes "15" for BB_LOWER now (BB_TIMEFRAME config removed)
    bars_per_24h = max(1, (24 * 60) // bb_timeframe_min)
    # Warm-up: BB period + a full 24h of lookback for the trend filter + buffer
    warm_ms = start_ms - (BB_PERIOD + bars_per_24h + 20) * bb_timeframe_min * 60 * 1000

    kl_15 = await _fetch_klines(session, symbol, "15", warm_ms, end_ms)

    result = BacktestResult(symbol=symbol, days=days, bars=len(kl_15), use_oi=use_oi)
    if len(kl_15) < BB_PERIOD + bars_per_24h + 10:
        log.warning(f"{symbol}: not enough 15m bars ({len(kl_15)})")
        return result

    ts_15 = [int(k[0]) for k in kl_15]
    opens = [float(k[1]) for k in kl_15]
    highs = [float(k[2]) for k in kl_15]
    lows = [float(k[3]) for k in kl_15]
    closes = [float(k[4]) for k in kl_15]
    vols = [float(k[5]) for k in kl_15]

    cooldown_until = -1
    start_i = 0
    while start_i < len(ts_15) and ts_15[start_i] < start_ms:
        start_i += 1

    i = max(start_i, BB_PERIOD + bars_per_24h + 5)
    while i < len(closes) - 2:
        if i <= cooldown_until:
            i += 1
            continue

        sig = _signal_at(i, closes, bars_per_24h)
        if not sig:
            i += 1
            continue

        result.signals += 1
        entry = sig["price"]
        tp = entry * (1 + AUTO_BB_LOWER_TP_PCT / 100)
        sl = entry * (1 - AUTO_BB_LOWER_SL_PCT / 100)
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
                break_pct=sig["break_pct"],
                trend24_pct=sig["trend24"],
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
        f"📊 <b>Backtest BB_LOWER</b> — <code>{r.symbol}</code>",
        f"Период: {r.days}д | баров 15m: {r.bars}",
        f"Тренд 24ч ≥{BB_LOWER_TREND_24H_MIN:+.1f}% | пробой нижней BB телом",
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
        f"TP +{AUTO_BB_LOWER_TP_PCT}% / SL −{AUTO_BB_LOWER_SL_PCT}% | fee {FEE_PCT}%×2",
        "",
        "<b>Последние сделки:</b>",
    ]
    for t in r.trades[-8:]:
        dt = datetime.fromtimestamp(t.entry_ts / 1000, tz=timezone.utc).strftime("%m-%d %H:%M")
        emoji = "✅" if t.pnl_usd > 0 else "🛑" if t.reason == "SL" else "⏱"
        lines.append(
            f"{emoji} {dt} {t.reason} {t.pnl_pct:+.2f}% "
            f"(пробой −{t.break_pct:.2f}% | тренд24ч {t.trend24_pct:+.1f}%)"
        )
    return "\n".join(lines)


def format_summary(results: list[BacktestResult]) -> str:
    all_tr: list[Trade] = []
    for r in results:
        all_tr.extend(r.trades)
    lines = [
        f"📊 <b>Backtest BB_LOWER — TOP{len(results)}</b>",
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
