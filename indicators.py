"""Indicators: RSI, EMA, sparkline."""
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
