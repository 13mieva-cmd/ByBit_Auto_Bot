"""Auto-trading orchestration: signal -> entry, reconciliation, safety rails."""
import asyncio
import logging
import math
import time
from datetime import datetime, timezone
from typing import Optional

import aiohttp
from aiogram import Bot

from config import (
    BYBIT_BASE_URL,
    POSITION_SIZE_USD, AUTO_TP_PCT, AUTO_HARD_SL_PCT,
    AUTO_PULLBACK_TP_PCT, AUTO_PULLBACK_SL_PCT,
    AUTO_BB_TP_PCT, AUTO_BB_SL_PCT,
    AUTO_BB_LOWER_TP_PCT, AUTO_BB_LOWER_SL_PCT,
    MAX_AUTO_POSITIONS, DAILY_LOSS_LIMIT_USD, CONSECUTIVE_LOSS_BLOCK,
    AUTO_TRADE_SIGNAL_TYPES, RECONCILE_INTERVAL_SEC,
    AUTO_REQUIRE_24H_UPTREND, AUTO_MIN_24H_CHANGE_PCT,
    POST_TRADE_COOLDOWN_HOURS,
    BTC_FILTER_ENABLED, BTC_FILTER_15M_DROP_MAX,
    BTC_FILTER_15M_PUMP_MAX, BTC_FILTER_1H_VOLATILITY_MAX,
    TELEGRAM_CHAT_ID,
    LEVERAGE, DEPOSIT_USD,
    AUTO_TRAIL_ENABLED, AUTO_TP1_TRIGGER_PCT, AUTO_TRAIL_DISTANCE_PCT,
    AUTO_TP1_TRIGGER_PCT_PB, AUTO_TRAIL_DISTANCE_PCT_PB,
    AUTO_TP1_TRIGGER_PCT_BB, AUTO_TRAIL_DISTANCE_PCT_BB,
    AUTO_TP1_TRIGGER_PCT_BB_LOWER, AUTO_TRAIL_DISTANCE_PCT_BB_LOWER,
    AUTO_BE_ENABLED, AUTO_BE_TRIGGER_PCT, AUTO_BE_BUFFER_PCT,
    STRUCTURE_EXIT_ENABLED, STRUCTURE_EXIT_EMA_1H, STRUCTURE_EXIT_EMA_15M,
    EMA_PERIOD, BYBIT_BASE_URL as _BYBIT_BASE,
)
from indicators import calculate_ema
from trader import BybitTrader

log = logging.getLogger("auto")


BYBIT_PUBLIC = BYBIT_BASE_URL


async def check_btc_health() -> dict:
    """
    Returns dict with BTC stats and is_ok flag.
    Fetches 15m and 1h klines for BTC.
    """
    result = {
        "is_ok": True,
        "reason": "",
        "change_15m": 0.0,
        "volatility_1h": 0.0,
    }
    try:
        async with aiohttp.ClientSession() as session:
            # Last 15m candle change
            async with session.get(
                f"{BYBIT_PUBLIC}/v5/market/kline",
                params={"category": "linear", "symbol": "BTCUSDT", "interval": "15", "limit": 1},
                timeout=10
            ) as r:
                data = await r.json()
            kl = data.get("result", {}).get("list", [])
            if not kl:
                return result
            op_15m = float(kl[0][1])
            cl_15m = float(kl[0][4])
            change_15m = (cl_15m - op_15m) / op_15m * 100 if op_15m > 0 else 0
            result["change_15m"] = change_15m

            # Last 1h candles for volatility (std dev of closes over 4×15m candles)
            async with session.get(
                f"{BYBIT_PUBLIC}/v5/market/kline",
                params={"category": "linear", "symbol": "BTCUSDT", "interval": "15", "limit": 4},
                timeout=10
            ) as r:
                data1h = await r.json()
            kl1h = data1h.get("result", {}).get("list", [])
            if len(kl1h) >= 4:
                closes = [float(k[4]) for k in kl1h]
                mean = sum(closes) / len(closes)
                if mean > 0:
                    variance = sum((c - mean) ** 2 for c in closes) / len(closes)
                    std_dev = math.sqrt(variance)
                    vol_pct = (std_dev / mean) * 100
                    result["volatility_1h"] = vol_pct
    except Exception as e:
        log.warning(f"check_btc_health: {e}")
        return result

    # Apply thresholds
    if change_15m <= -BTC_FILTER_15M_DROP_MAX:
        result["is_ok"] = False
        result["reason"] = f"BTC падает быстро ({change_15m:+.2f}% за 15м)"
    elif change_15m >= BTC_FILTER_15M_PUMP_MAX:
        result["is_ok"] = False
        result["reason"] = f"BTC резко растёт ({change_15m:+.2f}% за 15м) — FOMO ралли"
    elif result["volatility_1h"] >= BTC_FILTER_1H_VOLATILITY_MAX:
        result["is_ok"] = False
        result["reason"] = f"BTC волатилен ({result['volatility_1h']:.2f}% std за 1ч)"

    return result


class AutoTrader:
    def __init__(self, bot: Bot, trader: BybitTrader, state_store):
        self.bot = bot
        self.trader = trader
        self.state = state_store
        self.allowed_types = {t.strip() for t in AUTO_TRADE_SIGNAL_TYPES.split(",")}
        self._signal_lock = asyncio.Lock()

    async def notify(self, text: str):
        try:
            await self.bot.send_message(TELEGRAM_CHAT_ID, text)
        except Exception as e:
            log.warning(f"notify: {e}")

    async def handle_signal(self, signal: dict):
        """Called by scanner when alert is generated. Decides auto-entry."""
        async with self._signal_lock:
            if not self.state.is_enabled():
                return
            sig_type = signal["signal_type"]
            if sig_type not in self.allowed_types:
                return
            # Per-signal-type toggle check
            if not self.state.get_signal_toggle(sig_type):
                log.info(f"Signal type {sig_type} disabled, skip {signal['symbol']}")
                return

            # Только монеты в 24h лонг-тренде
            if AUTO_REQUIRE_24H_UPTREND:
                pc24 = signal.get("price_change_24h")
                if pc24 is None:
                    log.info(f"{signal['symbol']}: no 24h change data, skip auto")
                    return
                if pc24 < AUTO_MIN_24H_CHANGE_PCT:
                    log.info(
                        f"{signal['symbol']}: 24h {pc24:+.2f}% < {AUTO_MIN_24H_CHANGE_PCT}% — not uptrend, skip"
                    )
                    return

            # BB_LOWER: сигнал валиден (reclaim / close-ok флаг из сканера)
            if sig_type == "BB_LOWER":
                if not signal.get("bb_lower_close_ok"):
                    log.info(f"{signal['symbol']}: BB_LOWER without close<lower flag, skip")
                    return

            # Post-trade cooldown check
            if self.state.is_in_post_trade_cooldown(signal['symbol']):
                log.info(f"{signal['symbol']} in post-trade cooldown, skip auto-entry")
                return

            # BTC market filter check
            if BTC_FILTER_ENABLED and self.state.is_btc_filter_enabled():
                btc_health = await check_btc_health()
                if not btc_health["is_ok"]:
                    log.info(f"BTC filter blocked {signal['symbol']}: {btc_health['reason']}")
                    base = signal["symbol"].replace("USDT", "")
                    await self.notify(
                        f"⛔ <b>{base}</b> — авто-вход пропущен\n\n"
                        f"Сигнал: {sig_type} {'⭐' * signal['stars']}\n"
                        f"Причина: <b>{btc_health['reason']}</b>\n\n"
                        f"<i>Алерт пришёл, но рынок BTC нестабилен — "
                        f"бот не вошёл для безопасности. Можешь зайти вручную, если уверен.</i>"
                    )
                    return

            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            self.state.maybe_reset_day(today)

            if self.state.is_blocked():
                log.info(f"Auto blocked ({self.state.blocked_reason}), skip {signal['symbol']}")
                return
            if len(self.state.active_positions) >= MAX_AUTO_POSITIONS:
                log.info(f"Max {MAX_AUTO_POSITIONS} positions, skip {signal['symbol']}")
                return
            symbol = signal["symbol"]
            if symbol in self.state.active_positions:
                return

            # Also check Bybit-side: is there really no position? (sync safety)
            bybit_positions = await self.trader.get_open_positions(symbol)
            if bybit_positions:
                log.info(f"Bybit already has position in {symbol}, skip")
                return

            # Select TP/SL based on signal type
            if sig_type == "PULLBACK":
                tp_pct = AUTO_PULLBACK_TP_PCT
                sl_pct = AUTO_PULLBACK_SL_PCT
            elif sig_type == "BB_SQUEEZE":
                tp_pct = AUTO_BB_TP_PCT
                sl_pct = AUTO_BB_SL_PCT
            elif sig_type == "BB_LOWER":
                tp_pct = float(signal.get("tp_pct") or AUTO_BB_LOWER_TP_PCT)
                sl_pct = float(signal.get("sl_pct") or AUTO_BB_LOWER_SL_PCT)
            else:
                tp_pct = AUTO_TP_PCT
                sl_pct = AUTO_HARD_SL_PCT

            base = symbol.replace("USDT", "")
            stars_str = "⭐" * signal["stars"]
            await self.notify(
                f"🤖 <b>AUTO-ENTRY</b> — {base}\n"
                f"Сигнал: {sig_type} {stars_str}\n"
                f"Размер: ${POSITION_SIZE_USD}\n"
                f"TP +{tp_pct}% / SL −{sl_pct}%\n"
                f"Открываю позицию..."
            )

            result = await self.trader.open_long_with_tpsl(
                symbol, POSITION_SIZE_USD, tp_pct, sl_pct, leverage=LEVERAGE,
            )
            if not result["ok"]:
                err = result.get("error", "unknown")
                code = result.get("code", "")
                await self.notify(
                    f"❌ <b>{base}</b>: ошибка входа\n"
                    f"<code>{err}</code> (код {code})"
                )
                return

            await asyncio.sleep(2)
            positions = await self.trader.get_open_positions(symbol)
            if not positions:
                await self.notify(
                    f"⚠️ <b>{base}</b>: ордер отправлен, "
                    f"но позиция не подтверждена. Проверь Bybit."
                )
                return
            pos = positions[0]

            # === Пересчитать TP/SL от РЕАЛЬНОЙ цены входа (avgPrice) ===
            # Ордер маркетный — реальная цена исполнения отличается от той,
            # что использовалась при расчёте TP/SL до входа. Переставляем на бирже.
            adjust_result = await self.trader.set_tpsl_from_fill(symbol, tp_pct, sl_pct)
            if sig_type == "BB_LOWER" and signal.get("tp_price_abs") and signal.get("sl_price_abs"):
                # Сдвинуть уровни BB на дельту fill vs signal price (проскальзывание)
                sig_px = float(signal.get("price") or 0) or float(pos["entry_price"])
                fill_px = float(pos["entry_price"])
                delta = fill_px - sig_px
                tp_abs = float(signal["tp_price_abs"]) + delta
                sl_abs = float(signal["sl_price_abs"]) + delta
                if sl_abs >= fill_px:
                    sl_abs = fill_px * (1 - float(signal.get("sl_pct") or AUTO_BB_LOWER_SL_PCT) / 100)
                if tp_abs <= fill_px:
                    tp_abs = fill_px * (1 + float(signal.get("tp_pct") or AUTO_BB_LOWER_TP_PCT) / 100)
                abs_res = await self.trader.set_tpsl_prices(symbol, tp_abs, sl_abs)
                if abs_res.get("ok"):
                    adjust_result = abs_res
                    log.info(f"{symbol}: BB_LOWER structure TP/SL applied (delta={delta:.6g})")
                else:
                    log.warning(f"{symbol}: structure TPSL failed {abs_res}")
            if adjust_result.get("ok"):
                result["tp_price"] = adjust_result["tp_price"]
                result["sl_price"] = adjust_result["sl_price"]
            else:
                log.warning(
                    f"Failed to adjust TP/SL for {symbol}: {adjust_result.get('error')}"
                )

            # === Проверка, что стоп РЕАЛЬНО стоит на бирже ===
            protected = await self.trader.verify_position_protected(symbol)
            if protected.get("has_stop") is False:
                await self.notify(
                    f"🚨 <b>{base}</b>: стоп не установлен на бирже! "
                    f"Закрываю позицию немедленно."
                )
                await self.trader.close_position_market(symbol)
                return

            self.state.add_position(
                symbol=symbol,
                entry_price=pos["entry_price"],
                qty=pos["size"],
                tp_price=result["tp_price"],
                sl_price=result["sl_price"],
                leverage=result["leverage"],
                signal_type=sig_type,
                stars=signal["stars"],
            )
            _t0 = self.state.active_positions.get(symbol)
            if _t0 is not None:
                _t0["trailing_active"] = False
                self.state._save()
            await self.notify(
                f"✅ <b>{base}</b> позиция открыта ({sig_type})\n\n"
                f"Вход: <code>${pos['entry_price']:.6g}</code>\n"
                f"Размер: ${POSITION_SIZE_USD} (qty {pos['size']})\n"
                f"Плечо: {result['leverage']:.0f}x\n"
                f"🎯 TP: <code>${result['tp_price']:.6g}</code> (+{tp_pct}%)\n"
                f"🛑 SL: <code>${result['sl_price']:.6g}</code> (−{sl_pct}%) "
                f"≈ −${POSITION_SIZE_USD * sl_pct / 100:.2f} "
                f"({POSITION_SIZE_USD * sl_pct / 100 / DEPOSIT_USD * 100:.1f}% депозита)\n\n"
                f"Активных позиций: {len(self.state.active_positions)}/{MAX_AUTO_POSITIONS}"
            )

    async def reconcile_loop(self):
        while True:
            try:
                await self.reconcile_once()
            except Exception as e:
                log.exception(f"reconcile: {e}")
            await asyncio.sleep(RECONCILE_INTERVAL_SEC)

    async def reconcile_once(self):
        if not self.state.active_positions:
            return
        bybit_positions = await self.trader.get_open_positions()
        bybit_map = {p["symbol"]: p for p in bybit_positions}
        for symbol in list(self.state.active_positions.keys()):
            if symbol in bybit_map:
                live = bybit_map[symbol]
                # 1) Слом структуры → market close
                if STRUCTURE_EXIT_ENABLED:
                    closed = await self.maybe_structure_exit(symbol, live)
                    if closed:
                        continue
                # 2) Ранний BE
                if AUTO_BE_ENABLED:
                    await self.maybe_move_to_be(symbol, live)
                # 3) Трейлинг после TP1
                if AUTO_TRAIL_ENABLED:
                    await self.maybe_activate_trailing(symbol, live)
                continue
            await self.handle_closed_position(symbol)


    async def _fetch_closes(self, symbol: str, interval: str, limit: int) -> list[float]:
        """Публичные klines Bybit → список close."""
        try:
            base = (BYBIT_PUBLIC or "https://api-demo.bybit.com").rstrip("/")
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{base}/v5/market/kline",
                    params={
                        "category": "linear",
                        "symbol": symbol,
                        "interval": interval,
                        "limit": limit,
                    },
                    timeout=aiohttp.ClientTimeout(total=12),
                ) as r:
                    data = await r.json(content_type=None)
            if not isinstance(data, dict) or data.get("retCode") != 0:
                return []
            rows = data.get("result", {}).get("list", [])
            # newest first → reverse
            closes = [float(k[4]) for k in reversed(rows)]
            return closes
        except Exception as e:
            log.warning(f"klines {symbol} {interval}: {e}")
            return []

    async def maybe_move_to_be(self, symbol: str, live_pos: dict) -> None:
        """После +AUTO_BE_TRIGGER_PCT% перенести SL на entry (+ буфер)."""
        tracked = self.state.active_positions.get(symbol)
        if not tracked or tracked.get("be_active") or tracked.get("trailing_active"):
            return
        entry = tracked.get("entry_price") or 0
        mark = live_pos.get("mark_price") or 0
        if entry <= 0 or mark <= 0:
            return
        gain_pct = (mark - entry) / entry * 100
        if gain_pct < AUTO_BE_TRIGGER_PCT:
            return
        sl_price = entry * (1 + AUTO_BE_BUFFER_PCT / 100)
        # Не ставить SL выше рынка
        if sl_price >= mark:
            sl_price = entry
        res = await self.trader.set_stop_loss(symbol, sl_price)
        base = symbol.replace("USDT", "")
        if res.get("ok"):
            tracked["be_active"] = True
            tracked["sl_price"] = res.get("sl_price", sl_price)
            self.state._save()
            await self.notify(
                f"🛡 <b>{base}</b>: +{gain_pct:.2f}% — SL в <b>безубыток</b>\n"
                f"Стоп ≈ <code>{tracked['sl_price']:.6g}</code> (entry +{AUTO_BE_BUFFER_PCT}%)\n"
                f"<i>Дальше риск по позиции ≈ 0, ждём TP / трейлинг / структуру.</i>"
            )
        else:
            log.warning(f"BE failed {symbol}: {res}")

    async def maybe_structure_exit(self, symbol: str, live_pos: dict) -> bool:
        """True если позицию закрыли по слому EMA50 (1h и/или 15m)."""
        tracked = self.state.active_positions.get(symbol)
        if not tracked:
            return False
        reasons = []
        if STRUCTURE_EXIT_EMA_1H:
            closes_1h = await self._fetch_closes(symbol, "60", EMA_PERIOD + 5)
            if len(closes_1h) >= EMA_PERIOD:
                ema = calculate_ema(closes_1h, EMA_PERIOD)
                last = closes_1h[-1]
                if ema is not None and last < ema:
                    reasons.append(f"1h close {last:.6g} < EMA50 {ema:.6g}")
        # 15m EMA50 exit: skip for BB_LOWER (entry near lower band often already < EMA50 15m)
        if STRUCTURE_EXIT_EMA_15M and tracked.get("signal_type") != "BB_LOWER":
            closes_15 = await self._fetch_closes(symbol, "15", EMA_PERIOD + 5)
            if len(closes_15) >= EMA_PERIOD:
                ema = calculate_ema(closes_15, EMA_PERIOD)
                last = closes_15[-1]
                if ema is not None and last < ema:
                    reasons.append(f"15m close {last:.6g} < EMA50 {ema:.6g}")
        if not reasons:
            return False
        base = symbol.replace("USDT", "")
        res = await self.trader.close_position_market(symbol)
        reason = "; ".join(reasons)
        if res.get("ok"):
            tracked["exit_reason"] = "STRUCTURE"
            tracked["exit_detail"] = reason
            self.state._save()
            await self.notify(
                f"📉 <b>{base}</b> — выход по <b>слому структуры</b>\n"
                f"{reason}\n"
                f"<i>Close ниже EMA50 — тренд развернулся, не ждём полный SL.</i>"
            )
            await self.handle_closed_position(symbol)
            return True
        log.warning(f"structure exit failed {symbol}: {res}")
        await self.notify(
            f"⚠️ <b>{base}</b>: слом структуры ({reason}), но close не прошёл: {res.get('error')}"
        )
        return False

    async def maybe_activate_trailing(self, symbol: str, live_pos: dict):
        """Когда цена прошла TP1-триггер — снять фиксированный TP и включить
        биржевой трейлинг-стоп. Один раз на позицию, дальше ведёт Bybit."""
        tracked = self.state.active_positions.get(symbol)
        if not tracked or tracked.get("trailing_active"):
            return
        entry = tracked["entry_price"]
        mark = live_pos.get("mark_price") or 0
        if entry <= 0 or mark <= 0:
            return
        gain_pct = (mark - entry) / entry * 100
        if tracked.get("signal_type") == "PULLBACK":
            trigger, trail_dist = AUTO_TP1_TRIGGER_PCT_PB, AUTO_TRAIL_DISTANCE_PCT_PB
        elif tracked.get("signal_type") == "BB_SQUEEZE":
            trigger, trail_dist = AUTO_TP1_TRIGGER_PCT_BB, AUTO_TRAIL_DISTANCE_PCT_BB
        elif tracked.get("signal_type") == "BB_LOWER":
            trigger, trail_dist = AUTO_TP1_TRIGGER_PCT_BB_LOWER, AUTO_TRAIL_DISTANCE_PCT_BB_LOWER
        else:
            trigger, trail_dist = AUTO_TP1_TRIGGER_PCT, AUTO_TRAIL_DISTANCE_PCT
        if gain_pct < trigger:
            return
        res = await self.trader.set_trailing_stop(symbol, trail_dist)
        base = symbol.replace("USDT", "")
        if res.get("ok"):
            tracked["trailing_active"] = True
            self.state._save()
            await self.notify(
                f"\U0001F513 <b>{base}</b>: +{gain_pct:.1f}% — TP1 пройден\n"
                f"Фиксированный TP снят, включён <b>трейлинг {trail_dist}%</b>.\n"
                f"<i>Стоп идёт за ценой вверх, вниз не двигается. Ведёт Bybit.</i>"
            )
        else:
            await self.notify(
                f"\u26A0\uFE0F <b>{base}</b>: трейлинг не включился "
                f"(<code>{res.get('error')}</code>). Обычные TP/SL остаются."
            )

    async def handle_closed_position(self, symbol: str):
        tracked = self.state.active_positions.get(symbol)
        if not tracked:
            return

        entry = float(tracked.get("entry_price") or 0)
        tp_price = float(tracked.get("tp_price") or 0)
        sl_price = float(tracked.get("sl_price") or 0)
        opened_at = float(tracked.get("opened_at") or 0)

        # Reason set by bot (structure / panic)
        close_reason = tracked.get("exit_reason") or None
        exit_detail = tracked.get("exit_detail") or ""

        # closed-pnl from exchange (retry once on empty)
        closed = await self.trader.get_closed_pnl(symbol, 10)
        if not closed:
            await asyncio.sleep(1.5)
            closed = await self.trader.get_closed_pnl(symbol, 10)

        pnl_usd = None
        exit_price = None
        order_type = ""
        best = None
        for cp in closed:
            try:
                updated_ts = int(cp.get("updatedTime", 0) or 0) / 1000
                if updated_ts and updated_ts < opened_at - 30:
                    continue
                best = cp
                break
            except (KeyError, ValueError, TypeError):
                continue
        if best is None and closed:
            best = closed[0]

        if best:
            try:
                pnl_usd = float(best.get("closedPnl") or 0)
            except (TypeError, ValueError):
                pnl_usd = None
            try:
                ep = float(best.get("avgExitPrice") or 0)
                exit_price = ep if ep > 0 else None
            except (TypeError, ValueError):
                exit_price = None
            order_type = str(best.get("orderType") or "")

        # Infer reason from price / flags if bot did not tag it
        if not close_reason and exit_price and entry > 0:
            gain_pct = (exit_price - entry) / entry * 100
            if tracked.get("be_active") and abs(gain_pct) <= 0.35:
                close_reason = "BE"
            elif tp_price > 0 and abs(exit_price - tp_price) / tp_price < 0.012:
                close_reason = "TP"
            elif sl_price > 0 and abs(exit_price - sl_price) / max(sl_price, 1e-12) < 0.015:
                close_reason = "SL"
            elif tracked.get("trailing_active"):
                close_reason = "TRAILING"
            elif gain_pct >= float(tracked.get("tp_pct") or AUTO_BB_TP_PCT) * 0.85:
                close_reason = "TP"
            elif gain_pct <= -0.5:
                close_reason = "SL"
            elif "Market" in order_type and abs(gain_pct) < 0.4:
                close_reason = "BE" if tracked.get("be_active") else "MANUAL"
            else:
                close_reason = "MANUAL"

        if not close_reason:
            if tracked.get("trailing_active"):
                close_reason = "TRAILING"
            elif tracked.get("be_active"):
                close_reason = "BE"
            elif pnl_usd is not None:
                close_reason = "TP" if pnl_usd > 0 else ("SL" if pnl_usd < 0 else "MANUAL")
            else:
                close_reason = "UNKNOWN"

        reason_ru = {
            "TP": "Take Profit",
            "SL": "Stop Loss",
            "BE": "Breakeven (BE)",
            "TRAILING": "Trailing stop",
            "STRUCTURE": "Structure break (EMA50)",
            "MANUAL": "Manual / market close",
            "PANIC": "Panic close",
            "UNKNOWN": "not determined",
        }.get(close_reason, close_reason)

        # Russian labels
        reason_ru_map = {
            "TP": "Take Profit",
            "SL": "Stop Loss",
            "BE": "Безубыток (BE)",
            "TRAILING": "Трейлинг-стоп",
            "STRUCTURE": "Слом структуры (EMA50)",
            "MANUAL": "Ручное / market close",
            "PANIC": "Panic close",
            "UNKNOWN": "не определена",
        }
        reason_ru = reason_ru_map.get(close_reason, close_reason)

        if pnl_usd is not None:
            self.state.add_pnl(pnl_usd)
            if pnl_usd < 0:
                self.state.incr_consecutive_loss()
            elif pnl_usd > 0:
                self.state.reset_consecutive_loss()

        self.state.remove_position(symbol)
        self.state.add_post_trade_cooldown(symbol, POST_TRADE_COOLDOWN_HOURS)

        base = symbol.replace("USDT", "")
        emoji = {
            "TP": "✅", "SL": "🛑", "BE": "🛡️", "TRAILING": "📈",
            "STRUCTURE": "📉", "MANUAL": "✋", "PANIC": "🚨", "UNKNOWN": "❓",
        }.get(close_reason, "❓")
        pnl_str = f"${pnl_usd:+.2f}" if pnl_usd is not None else "?"
        exit_str = f"<code>${exit_price:.6g}</code>" if exit_price else "?"
        detail = f"\n<i>{exit_detail}</i>" if exit_detail else ""

        msg = (
            f"{emoji} <b>{base}</b> закрыта\n\n"
            f"Причина: <b>{reason_ru}</b>{detail}\n"
            f"Выход: {exit_str}\n"
            f"P&L: <b>{pnl_str}</b>\n"
            f"Дневной P&L: <b>${self.state.daily_pnl:+.2f}</b>\n"
            f"Подряд убытков: {self.state.consecutive_losses}\n"
            f"Активных позиций: {len(self.state.active_positions)}/{MAX_AUTO_POSITIONS}\n\n"
            f"🔒 <b>{base}</b> заблокирован для авто-входа на {POST_TRADE_COOLDOWN_HOURS}ч.\n"
            f"<i>(Алерты в Telegram продолжат приходить, но бот не будет автоматически входить.)</i>"
        )

        if self.state.daily_pnl <= -DAILY_LOSS_LIMIT_USD:
            self.state.block_daily()
            msg += (
                f"\n\n🚫 <b>Daily loss limit</b> (−${DAILY_LOSS_LIMIT_USD}) достигнут.\n"
                f"Авто-торговля заблокирована до завтра (UTC)."
            )
        elif self.state.consecutive_losses >= CONSECUTIVE_LOSS_BLOCK:
            self.state.block_consecutive()
            msg += (
                f"\n\n🚫 <b>{CONSECUTIVE_LOSS_BLOCK} убытков подряд</b>.\n"
                f"Авто-торговля заблокирована. <code>/resume</code> чтобы разблокировать."
            )

        await self.notify(msg)

    async def panic_close_all(self) -> tuple[int, int]:
        """Close all auto positions by market. Returns (closed_ok, closed_fail)."""
        ok, fail = 0, 0
        for symbol in list(self.state.active_positions.keys()):
            try:
                await self.trader.cancel_all_orders(symbol)
                result = await self.trader.close_position_market(symbol)
                if result.get("ok"):
                    pos = self.state.active_positions.get(symbol)
                    if pos:
                        pos["exit_reason"] = "PANIC"
                    self.state.remove_position(symbol)
                    ok += 1
                else:
                    fail += 1
            except Exception as e:
                log.warning(f"panic {symbol}: {e}")
                fail += 1
        self.state.block_panic()
        return ok, fail
