"""Indicators: RSI, EMA, Bollinger, Keltner, sparkline."""
import math
from typing import Optional


def calculate_rsi(closes: list[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    deltas = [closes[i + 1] - closes[i] for i in range(len(closes) - 1)]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calculate_ema(values: list[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = v * k + ema * (1 - k)
    return ema


def calculate_ema_slope_pct(values: list[float], period: int, lookback: int = 5) -> Optional[float]:
    """% изменение EMA(period) за последние lookback баров — прокси наклона тренда.
    >0 = растущий тренд, <0 = падающий, ~0 = флэт (без направленного тренда)."""
    if len(values) < period + lookback:
        return None
    ema_now = calculate_ema(values, period)
    ema_prev = calculate_ema(values[:-lookback], period)
    if ema_now is None or ema_prev is None or ema_prev == 0:
        return None
    return (ema_now - ema_prev) / ema_prev * 100


def calculate_adx(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> Optional[float]:
    """Wilder's ADX — сила тренда (не направление). ADX < 20 = слабый/боковой рынок,
    классический regime-фильтр перед trend-following входом."""
    n = len(closes)
    if n < period * 2 + 1 or len(highs) < n or len(lows) < n:
        return None

    plus_dm = []
    minus_dm = []
    trs = []
    for i in range(1, n):
        up_move = highs[i] - highs[i - 1]
        down_move = lows[i - 1] - lows[i]
        plus_dm.append(up_move if (up_move > down_move and up_move > 0) else 0.0)
        minus_dm.append(down_move if (down_move > up_move and down_move > 0) else 0.0)
        trs.append(_true_range(highs[i], lows[i], closes[i - 1]))

    if len(trs) < period * 2:
        return None

    def _wilder_smooth(vals: list[float], period: int) -> list[float]:
        smoothed = [sum(vals[:period])]
        for v in vals[period:]:
            smoothed.append(smoothed[-1] - smoothed[-1] / period + v)
        return smoothed

    atr_s = _wilder_smooth(trs, period)
    plus_s = _wilder_smooth(plus_dm, period)
    minus_s = _wilder_smooth(minus_dm, period)

    dx_values = []
    for a, p, m in zip(atr_s, plus_s, minus_s):
        if a == 0:
            continue
        plus_di = 100 * p / a
        minus_di = 100 * m / a
        denom = plus_di + minus_di
        dx = 100 * abs(plus_di - minus_di) / denom if denom > 0 else 0.0
        dx_values.append(dx)

    if len(dx_values) < period:
        return None
    return sum(dx_values[-period:]) / period


def calculate_bollinger(closes: list[float], period: int = 20, mult: float = 2.0) -> Optional[dict]:
    if len(closes) < period:
        return None
    window = closes[-period:]
    middle = sum(window) / period
    variance = sum((c - middle) ** 2 for c in window) / period
    std = math.sqrt(variance)
    upper = middle + mult * std
    lower = middle - mult * std
    bandwidth = (upper - lower) / middle * 100 if middle > 0 else 0.0
    return {"upper": upper, "middle": middle, "lower": lower, "bandwidth": bandwidth}


def _true_range(high: float, low: float, prev_close: float) -> float:
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def calculate_atr(highs, lows, closes, period: int = 20) -> Optional[float]:
    n = len(closes)
    if n < period + 1 or len(highs) < n or len(lows) < n:
        return None
    trs = [_true_range(highs[i], lows[i], closes[i - 1]) for i in range(1, n)]
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


def calculate_keltner(highs, lows, closes, ema_period=20, atr_period=20, atr_mult=1.5) -> Optional[dict]:
    if len(closes) < max(ema_period, atr_period) + 1:
        return None
    if len(highs) != len(closes) or len(lows) != len(closes):
        return None
    mid = calculate_ema(closes, ema_period)
    atr = calculate_atr(highs, lows, closes, atr_period)
    if mid is None or atr is None:
        return None
    return {"upper": mid + atr_mult * atr, "middle": mid, "lower": mid - atr_mult * atr, "atr": atr}


def bb_inside_keltner(bb: Optional[dict], kc: Optional[dict]) -> bool:
    if not bb or not kc:
        return False
    try:
        return float(bb["upper"]) <= float(kc["upper"]) and float(bb["lower"]) >= float(kc["lower"])
    except (KeyError, TypeError, ValueError):
        return False


def sparkline(values: list[float], width: int = 10) -> str:
    if not values or len(values) < 2:
        return "─" * width
    blocks = "▁▂▃▄▅▆▇█"
    sample = values[-width:] if len(values) >= width else values
    vmin, vmax = min(sample), max(sample)
    if vmax == vmin:
        return "─" * len(sample)
    return "".join(blocks[int((v - vmin) / (vmax - vmin) * (len(blocks) - 1))] for v in sample)


def progress_bar(value: float, low: float, high: float, width: int = 12) -> str:
    if high <= low:
        return "─" * width
    pct = max(0, min(1, (value - low) / (high - low)))
    filled = int(pct * width)
    return "█" * filled + "░" * (width - filled)
