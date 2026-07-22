"""Storage: positions, ignore list, stats."""
import json
import logging
import os
import time

log = logging.getLogger("storage")


class JsonStore:
    def __init__(self, path: str, default):
        self.path = path
        self.data = default
        self._load()

    def _load(self):
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r") as f:
                self.data = json.load(f)
        except Exception as e:
            log.error(f"Failed to load {self.path}: {e}")

    def _save(self):
        try:
            with open(self.path, "w") as f:
                json.dump(self.data, f, indent=2)
        except Exception as e:
            log.error(f"Failed to save {self.path}: {e}")


class PositionStore(JsonStore):
    def __init__(self, path: str):
        super().__init__(path, {})

    @staticmethod
    def _normalize(symbol: str) -> str:
        s = symbol.upper()
        return s if s.endswith("USDT") else s + "USDT"

    def add(self, symbol: str, entry_price: float, tp1: float, tp2: float, hard_sl: float, signal_type: str = "STANDARD") -> bool:
        symbol = self._normalize(symbol)
        if symbol in self.data:
            return False
        self.data[symbol] = {
            "symbol": symbol,
            "entry_price": entry_price,
            "tp1": tp1, "tp2": tp2,
            "hard_sl": hard_sl,
            "signal_type": signal_type,
            "opened_at": time.time(),
            "tp1_hit": False, "tp2_hit": False, "hard_sl_hit": False,
            "warned_timeout": False,
            "warned_oi_drop_count": 0,
            "smart_hold_active": False,
            "be_active": False,   # break-even SL active
            "be_price": None,
        }
        self._save()
        return True

    def remove(self, symbol: str) -> bool:
        symbol = self._normalize(symbol)
        if symbol in self.data:
            del self.data[symbol]
            self._save()
            return True
        return False

    def list_all(self) -> dict:
        return dict(self.data)

    def get(self, symbol: str):
        return self.data.get(self._normalize(symbol))

    def mark(self, symbol: str, field: str, value=True):
        s = self._normalize(symbol)
        if s in self.data:
            self.data[s][field] = value
            self._save()

    def activate_breakeven(self, symbol: str):
        s = self._normalize(symbol)
        if s in self.data:
            self.data[s]["be_active"] = True
            self.data[s]["be_price"] = self.data[s]["entry_price"]
            self._save()


class IgnoreStore(JsonStore):
    def __init__(self, path: str):
        super().__init__(path, {})

    def add(self, symbol: str, hours: int):
        symbol = symbol.upper()
        if not symbol.endswith("USDT"):
            symbol += "USDT"
        self.data[symbol] = time.time() + hours * 3600
        self._save()

    def remove(self, symbol: str) -> bool:
        symbol = symbol.upper()
        if not symbol.endswith("USDT"):
            symbol += "USDT"
        if symbol in self.data:
            del self.data[symbol]
            self._save()
            return True
        return False

    def is_ignored(self, symbol: str) -> bool:
        now = time.time()
        expired = [s for s, ts in self.data.items() if ts < now]
        if expired:
            for s in expired:
                del self.data[s]
            self._save()
        s = symbol.upper()
        if not s.endswith("USDT"):
            s += "USDT"
        return s in self.data

    def list_active(self) -> dict:
        now = time.time()
        return {s: ts for s, ts in self.data.items() if ts > now}


class StatsStore(JsonStore):
    def __init__(self, path: str):
        super().__init__(path, {
            "alerts_today": 0,
            "alerts_total": 0,
            "tp1_hits": 0,
            "tp2_hits": 0,
            "sl_hits": 0,
            "timeouts": 0,
            "last_reset_day": "",
            "alerts_by_star": {"1": 0, "2": 0, "3": 0},
            "alerts_by_type": {"STANDARD": 0, "SURGE": 0, "PULLBACK": 0},
        })

    def reset_daily_if_needed(self, today: str):
        if self.data.get("last_reset_day") != today:
            self.data["alerts_today"] = 0
            self.data["alerts_by_star"] = {"1": 0, "2": 0, "3": 0}
            self.data["last_reset_day"] = today
            self._save()

    def incr_alert(self, stars: int, signal_type: str = "STANDARD"):
        self.data["alerts_today"] += 1
        self.data["alerts_total"] += 1
        self.data["alerts_by_star"][str(stars)] = self.data["alerts_by_star"].get(str(stars), 0) + 1
        self.data["alerts_by_type"][signal_type] = self.data["alerts_by_type"].get(signal_type, 0) + 1
        self._save()

    def incr(self, field: str):
        self.data[field] = self.data.get(field, 0) + 1
        self._save()


class AutoStateStore(JsonStore):
    """Auto-trading state: enabled, blocks, daily PnL, consecutive losses, active positions."""
    def __init__(self, path: str):
        super().__init__(path, {
            "auto_enabled": False,
            "daily_pnl": 0.0,
            "daily_pnl_date": "",
            "consecutive_losses": 0,
            "blocked_until": 0,
            "blocked_reason": "",
            "active_positions": {},
            "signal_toggles": {
                "STANDARD": True,
                "SURGE": True,
                "PULLBACK": True,
            },
            "post_trade_cooldown": {},  # symbol -> expiration timestamp
            "btc_filter_enabled": True,
        })

    def is_enabled(self) -> bool:
        return self.data.get("auto_enabled", False)

    def set_enabled(self, on: bool):
        self.data["auto_enabled"] = on
        self._save()

    def is_blocked(self) -> bool:
        return time.time() < self.data.get("blocked_until", 0)

    @property
    def blocked_reason(self) -> str:
        return self.data.get("blocked_reason", "")

    def block_daily(self):
        self.data["blocked_until"] = time.time() + 24 * 3600
        self.data["blocked_reason"] = "daily_loss_limit"
        self._save()

    def block_consecutive(self):
        self.data["blocked_until"] = time.time() + 365 * 86400
        self.data["blocked_reason"] = "consecutive_losses"
        self._save()

    def block_panic(self):
        self.data["auto_enabled"] = False
        self.data["blocked_until"] = time.time() + 365 * 86400
        self.data["blocked_reason"] = "panic"
        self._save()

    def unblock(self):
        self.data["blocked_until"] = 0
        self.data["blocked_reason"] = ""
        self.data["consecutive_losses"] = 0
        self._save()

    def maybe_reset_day(self, today: str):
        if self.data.get("daily_pnl_date") != today:
            self.data["daily_pnl"] = 0.0
            self.data["daily_pnl_date"] = today
            self._save()

    def add_pnl(self, pnl: float):
        self.data["daily_pnl"] = self.data.get("daily_pnl", 0) + pnl
        self._save()

    @property
    def daily_pnl(self) -> float:
        return self.data.get("daily_pnl", 0.0)

    @property
    def consecutive_losses(self) -> int:
        return self.data.get("consecutive_losses", 0)

    def incr_consecutive_loss(self):
        self.data["consecutive_losses"] = self.data.get("consecutive_losses", 0) + 1
        self._save()

    def reset_consecutive_loss(self):
        self.data["consecutive_losses"] = 0
        self._save()

    @property
    def active_positions(self) -> dict:
        return self.data.get("active_positions", {})

    def add_position(self, symbol, entry_price, qty, tp_price, sl_price, leverage, signal_type, stars):
        positions = self.data.setdefault("active_positions", {})
        positions[symbol] = {
            "symbol": symbol,
            "entry_price": entry_price,
            "qty": qty,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "leverage": leverage,
            "signal_type": signal_type,
            "stars": stars,
            "opened_at": time.time(),
        }
        self._save()

    def remove_position(self, symbol):
        positions = self.data.get("active_positions", {})
        if symbol in positions:
            del positions[symbol]
            self._save()

    def get_signal_toggle(self, signal_type: str) -> bool:
        """Returns True if this signal type is enabled for auto-trade."""
        toggles = self.data.get("signal_toggles", {})
        return toggles.get(signal_type, True)

    def set_signal_toggle(self, signal_type: str, on: bool):
        toggles = self.data.setdefault("signal_toggles", {})
        toggles[signal_type] = on
        self._save()

    def all_signal_toggles(self) -> dict:
        return self.data.get("signal_toggles", {})

    def add_post_trade_cooldown(self, symbol: str, hours: int):
        """Block symbol from auto-trade for N hours after closed trade."""
        cooldowns = self.data.setdefault("post_trade_cooldown", {})
        cooldowns[symbol] = time.time() + hours * 3600
        self._save()

    def is_in_post_trade_cooldown(self, symbol: str) -> bool:
        """Returns True if symbol is currently blocked from auto-trade."""
        cooldowns = self.data.get("post_trade_cooldown", {})
        now = time.time()
        # Cleanup expired entries while we're here
        expired = [s for s, ts in cooldowns.items() if ts <= now]
        if expired:
            for s in expired:
                del cooldowns[s]
            self._save()
        return symbol in cooldowns

    def remove_post_trade_cooldown(self, symbol: str) -> bool:
        """Manually clear cooldown for a symbol."""
        cooldowns = self.data.get("post_trade_cooldown", {})
        if symbol in cooldowns:
            del cooldowns[symbol]
            self._save()
            return True
        return False

    def list_post_trade_cooldowns(self) -> dict:
        """Returns active cooldowns: symbol -> expiration_ts."""
        cooldowns = self.data.get("post_trade_cooldown", {})
        now = time.time()
        return {s: ts for s, ts in cooldowns.items() if ts > now}

    def is_btc_filter_enabled(self) -> bool:
        return self.data.get("btc_filter_enabled", True)

    def set_btc_filter(self, on: bool):
        self.data["btc_filter_enabled"] = on
        self._save()
