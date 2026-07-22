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
    POSITION_SIZE_USD, AUTO_TP_PCT, AUTO_HARD_SL_PCT,
    AUTO_PULLBACK_TP_PCT, AUTO_PULLBACK_SL_PCT,
    MAX_AUTO_POSITIONS, DAILY_LOSS_LIMIT_USD, CONSECUTIVE_LOSS_BLOCK,
    AUTO_TRADE_SIGNAL_TYPES, RECONCILE_INTERVAL_SEC,
    POST_TRADE_COOLDOWN_HOURS,
    BTC_FILTER_ENABLED, BTC_FILTER_15M_DROP_MAX,
    BTC_FILTER_15M_PUMP_MAX, BTC_FILTER_1H_VOLATILITY_MAX,
    TELEGRAM_CHAT_ID,
    LEVERAGE, DEPOSIT_USD,
    AUTO_TRAIL_ENABLED, AUTO_TP1_TRIGGER_PCT, AUTO_TRAIL_DISTANCE_PCT,
    AUTO_TP1_TRIGGER_PCT_PB, AUTO_TRAIL_DISTANCE_PCT_PB,
)
from trader import BybitTrader

log = logging.getLogger("auto")


BYBIT_PUBLIC = "https://api-demo.bybit.com"


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
                if AUTO_TRAIL_ENABLED:
                    await self.maybe_activate_trailing(symbol, bybit_map[symbol])
                continue
            await self.handle_closed_position(symbol)

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

        # Lookup closed PnL
        closed = await self.trader.get_closed_pnl(symbol, 5)
        pnl_usd = None
        close_reason = "UNKNOWN"
        exit_price = None
        for cp in closed:
            try:
                updated_ts = int(cp.get("updatedTime", 0)) / 1000
                if updated_ts > tracked["opened_at"] - 5:
                    pnl_usd = float(cp.get("closedPnl", 0))
                    exit_price = float(cp.get("avgExitPrice", 0))
                    if exit_price > 0:
                        tp_dist = abs(exit_price - tracked["tp_price"]) / tracked["tp_price"]
                        sl_dist = abs(exit_price - tracked["sl_price"]) / tracked["sl_price"]
                        if tp_dist < 0.005:
                            close_reason = "TP"
                        elif sl_dist < 0.01:
                            close_reason = "SL"
                        else:
                            close_reason = "MANUAL"
                    break
            except (KeyError, ValueError, TypeError):
                continue

        if pnl_usd is not None:
            self.state.add_pnl(pnl_usd)
            if pnl_usd < 0:
                self.state.incr_consecutive_loss()
            elif pnl_usd > 0:
                self.state.reset_consecutive_loss()

        self.state.remove_position(symbol)

        # Set post-trade cooldown: don't auto-trade this symbol again for N hours
        self.state.add_post_trade_cooldown(symbol, POST_TRADE_COOLDOWN_HOURS)

        base = symbol.replace("USDT", "")
        emoji = {"TP": "✅", "SL": "🛑", "MANUAL": "✋", "UNKNOWN": "❓"}.get(close_reason, "❓")
        pnl_str = f"${pnl_usd:+.2f}" if pnl_usd is not None else "?"
        exit_str = f"<code>${exit_price:.6g}</code>" if exit_price else "?"

        msg = (
            f"{emoji} <b>{base}</b> закрыта\n\n"
            f"Причина: <b>{close_reason}</b>\n"
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
                    self.state.remove_position(symbol)
                    ok += 1
                else:
                    fail += 1
            except Exception as e:
                log.warning(f"panic {symbol}: {e}")
                fail += 1
        self.state.block_panic()
        return ok, fail
