Bot entry point (per Procfile): scanner.py — this is the file that actually runs.

Live files: config.py, scanner.py, trader.py, auto_trade.py, storage.py, indicators.py, visuals.py
Offline tool: backtest.py — tests BB_LOWER only (python backtest.py --symbol XUSDT --days 30 | --top 15 --days 14)
Diagnostics: /bb_debug SYMBOL in Telegram — trace for BB_SQUEEZE on that symbol right now.

Signal types: STANDARD, SURGE, PULLBACK, BB_SQUEEZE, BB_LOWER — five now, each
independently toggleable via /sig_on /sig_off, each with its own TP/SL/trailing.

BB_SQUEEZE — restored to a real squeeze/breakout signal (separate from BB_LOWER,
not the same slot anymore):
  1) Squeeze = bandwidth in the bottom BB_SQUEEZE_PERCENTILE of its own lookback
     history OR below the absolute BB_SQUEEZE_MAX_BW cap (genuine OR — the old
     redundancy bug where the percentile branch never mattered is fixed: it's
     compared against its own threshold independently, not min()'d with the cap
     first).
  2) Optionally also confirmed by Keltner: BB must have been inside the Keltner
     Channel within the last BB_KC_SQUEEZE_BARS bars (BB_REQUIRE_KC_SQUEEZE).
  3) True breakout: close above upper band, optional confirmation that bandwidth
     is actually expanding (BB_REQUIRE_EXPANSION) and that price hasn't already
     closed back below mid since the breakout (BB_REJECT_FALSE_BREAKOUT).
  4) Small pullback (0.15–1.8%), volume, anti-parabolic guard, OI 24h+4h, EMA50,
     RSI ceiling, momentum — same spirit as before, thresholds loosened.

BB_LOWER — the "buy the dip" signal (unchanged logic from last round, still a
separate signal from BB_SQUEEZE): coin in a confirmed 24h uptrend + still above
EMA50(1h), price closes (body) below the lower Bollinger Band at the moment of
the break, RSI in a healthy dip zone (not overbought, not full capitulation),
break depth capped so it's a pullback and not a trend breakdown.

Fixes applied this round:
  - indicators.py: scanner.py called calculate_keltner() with a signature
    (separate EMA/ATR periods) and a return key ("middle") that didn't exist —
    would have crashed with TypeError/KeyError on the very first scan. Fixed to
    match. Also added bb_inside_keltner() — scanner.py imports it but it was
    never defined anywhere, which would have crashed on startup with ImportError
    before any code even ran.
  - backtest.py: imported BB_MIN_TREND_24H_PCT and BB_TIMEFRAME, both removed/
    renamed in the new config.py (would have crashed on import). Renamed to
    BB_LOWER_TREND_24H_MIN / AUTO_BB_LOWER_TP_PCT / AUTO_BB_LOWER_SL_PCT and
    hardcoded the 15m timeframe (scanner.py no longer makes it configurable).
    Also relabeled everything from "BB_SQUEEZE" to "BB_LOWER" in report headers —
    it tests the dip-buy signal, which is a different signal now than the
    restored BB_SQUEEZE. It does NOT backtest the new BB_SQUEEZE (squeeze/
    breakout) — that would need its own signal-reconstruction function mirroring
    try_bb_squeeze(); not built yet.
  - scanner.py: the /backtest command's confirmation message said "Backtest
    BB_SQUEEZE" — same mislabeling, fixed to say BB_LOWER since that's what it
    actually runs.

Verified after fixing (see audit): both try_bb_squeeze() and try_bb_lower() were
run through synthetic squeeze→breakout→pullback and uptrend→dip scenarios. Every
negative gate (EMA, OI, RSI, volume, missing Keltner squeeze, downtrend, BTC
weakness) correctly blocks; positive cases correctly fire with sensible star
tiers. Full config.py <-> scanner.py/auto_trade.py/backtest.py cross-import
check: 0 missing names.

Auto-trading additions from trader-2.py / auto_trade-2.py (kept, both verified
clean):
  - trader.py: set_stop_loss() — moves SL only, to an absolute price.
  - Early breakeven stop (AUTO_BE_ENABLED): after +0.6% moves SL to entry+buffer.
  - Structure exit (STRUCTURE_EXIT_ENABLED): market-closes if price closes below
    EMA50 on 1h and/or 15m, instead of waiting for the full stop-loss.

Known caveats (unchanged):
  - backtest.py only covers BB_LOWER, not BB_SQUEEZE, and only simulates a
    static TP/SL bracket — the live trailing stop and breakeven/structure-exit
    mechanics aren't modeled, so results are a conservative lower bound.
  - SCAN_INTERVAL_MIN=1 (was 10) is a 10x increase in scan frequency — watch
    Bybit rate limits if the pre-filtered coin count grows.

NOTE: inflow_scanner_v4_full.py, if still present anywhere as a separate stale
file in the repo, should be deleted — the real file is scanner.py per Procfile.
