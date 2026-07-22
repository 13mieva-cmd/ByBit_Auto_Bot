"""Bybit V5 signed API client for trading."""
import asyncio
import hashlib
import hmac
import json
import logging
import math
import time
from typing import Optional

import aiohttp

log = logging.getLogger("trader")


class BybitTrader:
    def __init__(self, api_key: str, api_secret: str,
                 base_url: str = "https://api-demo.bybit.com", recv_window: int = 5000):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.recv_window = recv_window
        self._instruments_cache: dict = {}
        self._cache_time: float = 0

    def _sign(self, timestamp: str, payload: str) -> str:
        param = f"{timestamp}{self.api_key}{self.recv_window}{payload}"
        return hmac.new(self.api_secret.encode(), param.encode(), hashlib.sha256).hexdigest()

    async def _signed_request(self, method: str, path: str, params: dict = None) -> dict:
        timestamp = str(int(time.time() * 1000))
        params = params or {}

        try:
            async with aiohttp.ClientSession() as session:
                if method == "GET":
                    sorted_items = sorted(params.items())
                    query = "&".join(f"{k}={v}" for k, v in sorted_items)
                    sign = self._sign(timestamp, query)
                    url = f"{self.base_url}{path}"
                    if query:
                        url += f"?{query}"
                    headers = {
                        "X-BAPI-API-KEY": self.api_key,
                        "X-BAPI-SIGN": sign,
                        "X-BAPI-TIMESTAMP": timestamp,
                        "X-BAPI-RECV-WINDOW": str(self.recv_window),
                    }
                    async with session.get(url, headers=headers, timeout=15) as r:
                        return await r.json()
                else:
                    body = json.dumps(params, separators=(',', ':'))
                    sign = self._sign(timestamp, body)
                    headers = {
                        "X-BAPI-API-KEY": self.api_key,
                        "X-BAPI-SIGN": sign,
                        "X-BAPI-TIMESTAMP": timestamp,
                        "X-BAPI-RECV-WINDOW": str(self.recv_window),
                        "Content-Type": "application/json",
                    }
                    async with session.post(f"{self.base_url}{path}", data=body, headers=headers, timeout=15) as r:
                        return await r.json()
        except Exception as e:
            log.error(f"{method} {path}: {e}")
            return {"retCode": -1, "retMsg": str(e)}

    async def get_wallet_balance_usdt(self) -> Optional[float]:
        resp = await self._signed_request("GET", "/v5/account/wallet-balance", {"accountType": "UNIFIED"})
        if resp.get("retCode") != 0:
            log.warning(f"wallet-balance: {resp.get('retMsg')}")
            return None
        try:
            for coin in resp["result"]["list"][0]["coin"]:
                if coin["coin"] == "USDT":
                    return float(coin.get("walletBalance", 0))
        except (KeyError, IndexError, ValueError):
            pass
        return None

    async def get_instruments_cached(self, force: bool = False) -> dict:
        if self._instruments_cache and not force and (time.time() - self._cache_time) < 21600:
            return self._instruments_cache
        async with aiohttp.ClientSession() as session:
            cache = {}
            cursor = ""
            while True:
                params = {"category": "linear", "limit": 1000}
                if cursor:
                    params["cursor"] = cursor
                try:
                    async with session.get(f"{self.base_url}/v5/market/instruments-info",
                                           params=params, timeout=15) as r:
                        data = await r.json()
                except Exception as e:
                    log.error(f"instruments fetch: {e}")
                    break
                for inst in data.get("result", {}).get("list", []):
                    try:
                        sym = inst["symbol"]
                        if not sym.endswith("USDT"):
                            continue
                        cache[sym] = {
                            "qty_step": float(inst["lotSizeFilter"]["qtyStep"]),
                            "min_qty": float(inst["lotSizeFilter"]["minOrderQty"]),
                            "tick_size": float(inst["priceFilter"]["tickSize"]),
                            "max_leverage": float(inst["leverageFilter"]["maxLeverage"]),
                        }
                    except (KeyError, ValueError):
                        continue
                cursor = data.get("result", {}).get("nextPageCursor", "")
                if not cursor:
                    break
        self._instruments_cache = cache
        self._cache_time = time.time()
        log.info(f"Cached {len(cache)} instruments info")
        return cache

    async def get_open_positions(self, symbol: str = None) -> list:
        params = {"category": "linear", "settleCoin": "USDT"}
        if symbol:
            params["symbol"] = symbol
        resp = await self._signed_request("GET", "/v5/position/list", params)
        if resp.get("retCode") != 0:
            log.warning(f"position/list: {resp.get('retMsg')}")
            return []
        positions = []
        for p in resp.get("result", {}).get("list", []):
            try:
                size = float(p.get("size", 0))
                if size > 0:
                    positions.append({
                        "symbol": p["symbol"],
                        "side": p["side"],
                        "size": size,
                        "entry_price": float(p["avgPrice"]),
                        "mark_price": float(p.get("markPrice", 0)),
                        "unrealised_pnl": float(p.get("unrealisedPnl", 0)),
                        "leverage": float(p.get("leverage", 1)),
                        "position_idx": int(p.get("positionIdx", 0)),
                    })
            except (KeyError, ValueError):
                continue
        return positions

    async def ensure_one_way_mode(self, symbol: str) -> bool:
        """Переключить символ в One-Way режим (одна позиция на символ).
        Нужно, потому что весь код шлёт positionIdx=0 — это значение для One-Way.
        Если аккаунт в Hedge Mode, Bybit отвечает 10001 'position idx not match'.
        retCode 34036 = режим уже One-Way (это ОК)."""
        params = {"category": "linear", "symbol": symbol, "mode": 0}
        resp = await self._signed_request("POST", "/v5/position/switch-mode", params)
        code = resp.get("retCode")
        if code in (0, 34036):
            return True
        # 110025 = mode not modified / нельзя менять при открытой позиции — тоже не блокер
        if code == 110025:
            return True
        log.warning(f"switch-mode {symbol}: {resp.get('retMsg')} (код {code})")
        return False

    async def set_leverage(self, symbol: str, leverage: float) -> bool:
        params = {
            "category": "linear",
            "symbol": symbol,
            "buyLeverage": str(int(leverage)),
            "sellLeverage": str(int(leverage)),
        }
        resp = await self._signed_request("POST", "/v5/position/set-leverage", params)
        if resp.get("retCode") in (0, 110043):
            return True
        log.warning(f"set-leverage {symbol} {leverage}x: {resp.get('retMsg')}")
        return False

    @staticmethod
    def _round_qty_down(qty: float, step: float) -> float:
        if step <= 0:
            return qty
        return math.floor(qty / step) * step

    @staticmethod
    def _round_price(price: float, tick: float) -> float:
        if tick <= 0:
            return price
        return round(round(price / tick) * tick, 10)

    @staticmethod
    def _decimals(step: float) -> int:
        if step >= 1:
            return 0
        return max(0, -int(math.floor(math.log10(step))))

    @classmethod
    def _fmt(cls, val: float, step: float) -> str:
        d = cls._decimals(step)
        return f"{val:.{d}f}"

    async def get_last_price(self, symbol: str) -> Optional[float]:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(f"{self.base_url}/v5/market/tickers",
                                       params={"category": "linear", "symbol": symbol},
                                       timeout=10) as r:
                    data = await r.json()
                return float(data["result"]["list"][0]["lastPrice"])
            except Exception:
                return None

    async def open_long_with_tpsl(self, symbol: str, position_size_usd: float,
                                   tp_pct: float, sl_pct: float,
                                   leverage: float = None) -> dict:
        """Открыть лонг с TP/SL на уровне позиции.

        ИСПРАВЛЕНО (было опасно):
          1) плечо ставилось МАКСИМАЛЬНОЕ (info["max_leverage"], до 75-100x).
             При таком плече ликвидация наступает на 1-4% и стоп-лосс не успевал
             сработать вообще. Теперь плечо ФИКСИРОВАННОЕ и ограничено лимитом биржи.
          2) TP/SL считались от last_price ДО входа. Ордер маркетный, реальная цена
             исполнения другая -> цели уезжали. Теперь после входа читаем фактическую
             avgPrice позиции и ПЕРЕСТАВЛЯЕМ TP/SL от неё.
        """
        instruments = await self.get_instruments_cached()
        info = instruments.get(symbol)
        if not info:
            return {"ok": False, "error": f"no instrument info for {symbol}"}

        current_price = await self.get_last_price(symbol)
        if current_price is None or current_price <= 0:
            return {"ok": False, "error": "price fetch failed"}

        # Calculate qty
        qty_raw = position_size_usd / current_price
        qty = self._round_qty_down(qty_raw, info["qty_step"])
        if qty < info["min_qty"]:
            return {"ok": False, "error": f"qty {qty} below min {info['min_qty']}"}

        # TP/SL prices
        tp_price = self._round_price(current_price * (1 + tp_pct / 100), info["tick_size"])
        sl_price = self._round_price(current_price * (1 - sl_pct / 100), info["tick_size"])

        qty_str = self._fmt(qty, info["qty_step"])
        tp_str = self._fmt(tp_price, info["tick_size"])
        sl_str = self._fmt(sl_price, info["tick_size"])

        # РЕЖИМ ПОЗИЦИИ: переключаем в One-Way, иначе positionIdx=0 даёт ошибку 10001
        await self.ensure_one_way_mode(symbol)

        # ПЛЕЧО: фиксированное из конфига, но не выше лимита инструмента
        lev = leverage if leverage else 10.0
        lev = min(float(lev), float(info["max_leverage"]))
        await self.set_leverage(symbol, lev)

        # Place market entry with position-level TP/SL
        # Note: tpslMode=Full requires tpOrderType=Market (Bybit API requirement)
        order_params = {
            "category": "linear",
            "symbol": symbol,
            "side": "Buy",
            "orderType": "Market",
            "qty": qty_str,
            "takeProfit": tp_str,
            "stopLoss": sl_str,
            "tpslMode": "Full",
            "tpOrderType": "Market",
            "slOrderType": "Market",
            "tpTriggerBy": "LastPrice",
            "slTriggerBy": "LastPrice",
            "positionIdx": 0,
        }
        resp = await self._signed_request("POST", "/v5/order/create", order_params)
        # ЗАПАСНОЙ ВАРИАНТ: если аккаунт остался в Hedge Mode (переключить не вышло),
        # positionIdx=0 даёт 10001. Повторяем с positionIdx=1 (Long в hedge).
        if resp.get("retCode") == 10001 and "position idx" in str(resp.get("retMsg", "")).lower():
            log.warning(f"{symbol}: One-Way не сработал, пробую hedge positionIdx=1")
            order_params["positionIdx"] = 1
            resp = await self._signed_request("POST", "/v5/order/create", order_params)
        if resp.get("retCode") != 0:
            return {
                "ok": False,
                "error": resp.get("retMsg", "unknown"),
                "code": resp.get("retCode"),
            }

        return {
            "ok": True,
            "order_id": resp.get("result", {}).get("orderId"),
            "qty": qty,
            "entry_price_estimate": current_price,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "leverage": lev,
            "position_idx": order_params["positionIdx"],
        }

    async def set_tpsl_from_fill(self, symbol: str, tp_pct: float, sl_pct: float) -> dict:
        """Пересчитать TP/SL от ФАКТИЧЕСКОЙ цены входа (avgPrice) и переставить на бирже.
        Вызывать после подтверждения позиции — маркет-ордер исполняется не по last_price."""
        positions = await self.get_open_positions(symbol)
        if not positions:
            return {"ok": False, "error": "нет позиции"}
        entry = positions[0]["entry_price"]
        pidx = positions[0].get("position_idx", 0)
        if entry <= 0:
            return {"ok": False, "error": "нулевая цена входа"}
        instruments = await self.get_instruments_cached()
        info = instruments.get(symbol)
        if not info:
            return {"ok": False, "error": "нет данных инструмента"}
        tp_price = self._round_price(entry * (1 + tp_pct / 100), info["tick_size"])
        sl_price = self._round_price(entry * (1 - sl_pct / 100), info["tick_size"])
        params = {
            "category": "linear",
            "symbol": symbol,
            "takeProfit": self._fmt(tp_price, info["tick_size"]),
            "stopLoss": self._fmt(sl_price, info["tick_size"]),
            "tpslMode": "Full",
            "tpTriggerBy": "LastPrice",
            "slTriggerBy": "LastPrice",
            "positionIdx": pidx,
        }
        resp = await self._signed_request("POST", "/v5/position/trading-stop", params)
        if resp.get("retCode") not in (0, 34040):   # 34040 = not modified
            return {"ok": False, "error": resp.get("retMsg"), "code": resp.get("retCode")}
        return {"ok": True, "entry_price": entry, "tp_price": tp_price, "sl_price": sl_price}

    async def set_trailing_stop(self, symbol: str, trail_distance_pct: float) -> dict:
        """Включить БИРЖЕВОЙ трейлинг-стоп (Bybit ведёт сам, даже если бот офлайн).
        trailingStop задаётся АБСОЛЮТНОЙ дистанцией в цене — считаем от текущей.
        Фиксированный takeProfit снимаем (='0'), чтобы позиция шла за трейлингом.
        positionIdx берём из реальной позиции — работает в One-Way и Hedge."""
        positions = await self.get_open_positions(symbol)
        if not positions:
            return {"ok": False, "error": "нет позиции"}
        pidx = positions[0].get("position_idx", 0)
        instruments = await self.get_instruments_cached()
        info = instruments.get(symbol)
        if not info:
            return {"ok": False, "error": "нет данных инструмента"}
        price = await self.get_last_price(symbol)
        if price is None or price <= 0:
            return {"ok": False, "error": "нет цены"}
        trail_dist = self._round_price(price * trail_distance_pct / 100, info["tick_size"])
        if trail_dist <= 0:
            return {"ok": False, "error": "нулевая дистанция"}
        params = {
            "category": "linear",
            "symbol": symbol,
            "trailingStop": self._fmt(trail_dist, info["tick_size"]),
            "takeProfit": "0",
            "tpslMode": "Full",
            "positionIdx": pidx,
        }
        resp = await self._signed_request("POST", "/v5/position/trading-stop", params)
        if resp.get("retCode") not in (0, 34040):
            return {"ok": False, "error": resp.get("retMsg"), "code": resp.get("retCode")}
        return {"ok": True, "trail_distance": trail_dist, "ref_price": price}

    async def verify_position_protected(self, symbol: str) -> dict:
        """Проверить, что у позиции РЕАЛЬНО стоит стоп на бирже.
        Если бот падал/редеплоился, позиция могла остаться без защиты."""
        resp = await self._signed_request("GET", "/v5/position/list",
                                          {"category": "linear", "symbol": symbol})
        if resp.get("retCode") != 0:
            return {"ok": False, "error": resp.get("retMsg")}
        for p in resp.get("result", {}).get("list", []):
            if float(p.get("size", 0) or 0) > 0:
                sl = p.get("stopLoss") or ""
                return {"ok": True, "has_stop": sl not in ("", "0", 0),
                        "stop_loss": sl, "entry": float(p.get("avgPrice", 0) or 0)}
        return {"ok": True, "has_stop": None, "no_position": True}

    async def close_position_market(self, symbol: str) -> dict:
        positions = await self.get_open_positions(symbol)
        if not positions:
            return {"ok": True, "msg": "no position"}
        pos = positions[0]
        instruments = await self.get_instruments_cached()
        info = instruments.get(symbol)
        if not info:
            return {"ok": False, "error": "no instrument info"}
        qty_str = self._fmt(pos["size"], info["qty_step"])
        params = {
            "category": "linear",
            "symbol": symbol,
            "side": "Sell",
            "orderType": "Market",
            "qty": qty_str,
            "reduceOnly": True,
            "positionIdx": pos.get("position_idx", 0),
        }
        resp = await self._signed_request("POST", "/v5/order/create", params)
        if resp.get("retCode") != 0:
            return {"ok": False, "error": resp.get("retMsg")}
        return {"ok": True}

    async def cancel_all_orders(self, symbol: str) -> bool:
        params = {"category": "linear", "symbol": symbol}
        resp = await self._signed_request("POST", "/v5/order/cancel-all", params)
        return resp.get("retCode") == 0

    async def get_closed_pnl(self, symbol: str = None, limit: int = 20) -> list:
        params = {"category": "linear", "limit": str(limit)}
        if symbol:
            params["symbol"] = symbol
        resp = await self._signed_request("GET", "/v5/position/closed-pnl", params)
        if resp.get("retCode") != 0:
            return []
        return resp.get("result", {}).get("list", [])
