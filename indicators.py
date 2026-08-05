"""Indicators: RSI, EMA, Bollinger, Keltner, ATR, sparkline."""
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


def calculate_bollinger(
    closes: list[float], period: int = 20, mult: float = 2.0
) -> Optional[dict]:
    """Bollinger Bands on the last candle: upper, middle, lower, bandwidth (%)."""
    if len(closes) < period:
        return None
    window = closes[-period:]
    middle = sum(window) / period
    variance = sum((c - middle) ** 2 for c in window) / period
    std = math.sqrt(variance)
    upper = middle + mult * std
    lower = middle - mult * std
    bandwidth = (upper - lower) / middle * 100 if middle > 0 else 0.0
    return {
        "upper": upper,
        "middle": middle,
        "lower": lower,
        "bandwidth": bandwidth,
    }


def calculate_atr(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    period: int = 10,
) -> Optional[float]:
    """Wilder ATR on the last bar."""
    n = len(closes)
    if n < period + 1 or len(highs) != n or len(lows) != n:
        return None
    trs = []
    for i in range(1, n):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)
    if len(trs) < period:
        return None
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def calculate_keltner(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    ema_period: int = 20,
    atr_period: int = 10,
    atr_mult: float = 1.5,
) -> Optional[dict]:
    """Keltner Channels: EMA ± ATR * mult."""
    if len(closes) < max(ema_period, atr_period) + 1:
        return None
    mid = calculate_ema(closes, ema_period)
    atr = calculate_atr(highs, lows, closes, atr_period)
    if mid is None or atr is None:
        return None
    return {
        "upper": mid + atr_mult * atr,
        "middle": mid,
        "lower": mid - atr_mult * atr,
        "atr": atr,
    }


def bb_inside_keltner(bb: dict, kc: dict) -> bool:
    """TTM-style squeeze: entire Bollinger band inside Keltner channel."""
    if not bb or not kc:
        return False
    return bb["upper"] <= kc["upper"] and bb["lower"] >= kc["lower"]


def sparkline(values: list[float], width: int = 10) -> str:
    """ASCII sparkline of N most recent values."""
    if not values or len(values) < 2:
        return "─" * width
    blocks = "▁▂▃▄▅▆▇█"
    sample = values[-width:] if len(values) >= width else values
    vmin, vmax = min(sample), max(sample)
    if vmax == vmin:
        return "─" * len(sample)
    result = ""
    for v in sample:
        idx = int((v - vmin) / (vmax - vmin) * (len(blocks) - 1))
        result += blocks[idx]
    return result


def progress_bar(value: float, low: float, high: float, width: int = 12) -> str:
    """Visual progress bar. Position of `value` between `low` and `high`."""
    if high <= low:
        return "─" * width
    pct = (value - low) / (high - low)
    pct = max(0, min(1, pct))
    filled = int(pct * width)
    return "█" * filled + "░" * (width - filled)
