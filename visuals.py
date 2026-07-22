"""Visual helpers: progress bars, sparklines."""


def progress_bar(current: float, low: float, high: float, width: int = 20) -> str:
    """Linear progress bar between low and high. Returns string like '████░░░░░░'."""
    if high <= low:
        return "░" * width
    pct = max(0, min(1, (current - low) / (high - low)))
    filled = int(pct * width)
    return "█" * filled + "░" * (width - filled)


def sparkline(values: list[float], width: int = 10) -> str:
    """Returns a unicode sparkline."""
    if not values or len(values) < 2:
        return ""
    blocks = "▁▂▃▄▅▆▇█"
    vmin, vmax = min(values), max(values)
    if vmax == vmin:
        return blocks[0] * min(len(values), width)
    # Sample down to width
    if len(values) > width:
        step = len(values) / width
        sampled = [values[int(i * step)] for i in range(width)]
    else:
        sampled = values
    out = ""
    for v in sampled:
        norm = (v - vmin) / (vmax - vmin)
        idx = int(norm * (len(blocks) - 1))
        out += blocks[idx]
    return out


def position_progress(entry: float, current: float, tp1: float, tp2: float, hard_sl: float, width: int = 22) -> str:
    """
    Visualize where current price is in the range hard_sl <-> tp2.
    Marks: |SL ---- entry ---- TP1 ---- TP2|
    """
    if tp2 <= hard_sl:
        return ""
    pct = (current - hard_sl) / (tp2 - hard_sl)
    pct = max(0, min(1, pct))
    pos = int(pct * (width - 1))

    bar = ""
    entry_pos = int((entry - hard_sl) / (tp2 - hard_sl) * (width - 1))
    tp1_pos = int((tp1 - hard_sl) / (tp2 - hard_sl) * (width - 1))

    for i in range(width):
        if i == pos:
            bar += "●"
        elif i == entry_pos:
            bar += "│"
        elif i == tp1_pos:
            bar += "┊"
        elif i == 0 or i == width - 1:
            bar += "│"
        else:
            bar += "─"
    return bar
