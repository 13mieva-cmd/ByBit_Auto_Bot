Bot entry point (per Procfile): scanner.py — this is the file that actually runs.

Live files: config.py, scanner.py, trader.py, auto_trade.py, storage.py, indicators.py, visuals.py
Offline tool: backtest.py (python backtest.py --symbol XUSDT --days 30 | --top 15 --days 14)
Diagnostics: /bb_debug SYMBOL in Telegram — shows the exact pass/fail trace for BB_SQUEEZE
on that symbol right now (which gate blocked it, or that it fired).

Signal types: STANDARD, SURGE, PULLBACK, BB_SQUEEZE

BB_SQUEEZE — rewritten to fix "zero signals ever" (previous version's percentile-vs-
absolute-cap squeeze test and a 0.15–1.2% pullback timing window made the combined
AND-conditions almost impossible to satisfy on a 10-min scan cadence):
  - Squeeze definition replaced with the industry-standard John Carter / TTM Squeeze
    test: Bollinger Bands fully inside the Keltner Channel (BB_PERIOD/BB_MULT vs
    KC_ATR_PERIOD/KC_MULT). Self-adaptive per coin's own volatility — no more manual
    percentile/absolute-bandwidth tuning.
  - Added bandwidth-expansion confirmation (BB_BW_EXPANSION_MIN): current bandwidth
    must be meaningfully wider than the tightest point in the fresh-squeeze window —
    "bands are actually diverging," not just a one-bar poke outside.
  - OI confirmation changed from AND to OR: 24h strong OR 4h strong (previously both
    were required simultaneously, which rarely lined up).
  - Pullback-window gate (0.15–1.2%) removed entirely — it was the single biggest
    reason signals never fired; a narrow retracement window on 15m bars is almost
    never caught by a scan every 10 minutes. Pullback % is still computed and shown
    in the alert, it just no longer decides accept/reject.
  - BB_TIMEFRAME is now configurable (default "15") and shared by scanner.py and
    backtest.py, so testing on 1h is just an env var change.
  - BB_DEBUG_LOG=true logs the exact rejection reason for every coin at INFO level.

Mode: 4-signal auto-trading (STANDARD/SURGE/PULLBACK/BB_SQUEEZE — each toggleable
independently via /sig_on /sig_off), long-only trend filter, trailing stop enabled
after TP1 (per-signal-type trigger/distance, including BB_SQUEEZE).

Earlier fixes (still in place, see prior audit):
  - scanner.py: percentile-vs-cap squeeze bug fixed and later fully replaced (see above).
  - backtest.py: fixed a look-ahead bias where the EMA50(1h) filter could use a 1h
    candle's final close before that candle had actually finished forming.
  - backtest.py: fixed warm-up window sizing so the first hours of every backtest
    window aren't a guaranteed dead zone for EMA50.

Known caveat (not fixed, needs a decision): backtest.py simulates a static TP/SL
bracket only. The live bot's trailing stop (activates at AUTO_TP1_TRIGGER_PCT_BB,
trails at AUTO_TRAIL_DISTANCE_PCT_BB, removes the fixed TP) is NOT modeled in the
backtest, and backtest.py imposes a 12h (MAX_HOLD_BARS=48) timeout that doesn't exist
in live auto-trading. Backtest results are a lower bound / conservative estimate.

NOTE: inflow_scanner_v4_full.py (if still present in the repo) is a stale, un-patched
duplicate of an earlier version of scanner.py — it is not referenced by the Procfile and
is not imported by anything. Safe to delete; kept out of this delivery to avoid two
diverging copies of the same bot.
