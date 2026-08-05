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

=== Follow-up audit: full logic/math re-check ===

Bugs found and fixed this pass:
  - try_pullback() (scanner.py): the "EMA50 must be rising" guard computed a
    50-period EMA on a 10-bar slice (closes_1h[-15:-5]) — calculate_ema always
    returns None when given fewer than `period` values, so this check could
    never actually reject anything. It's been silently dead since before this
    conversation started. Fixed to use closes_1h[:-5] (enough history for a
    real EMA_PERIOD-length calculation), giving a legitimate "EMA50 as of 5
    bars ago" to compare against. Verified: a genuinely declining EMA50 now
    correctly blocks the signal; a genuinely rising one still passes.
  - storage.py: default alerts_by_type / signal_toggles dicts didn't list
    BB_LOWER (only mattered cosmetically — every read path already used
    .get(key, default), so nothing crashed, but a fresh deployment wouldn't
    show "BB_LOWER: 0" until the first one ever fired). Added for consistency.

Math finding fixed:
  - AUTO_BE_BUFFER_PCT was 0.05%, but round-trip taker fees (~0.055% x2 =
    0.11%) exceed that — the "breakeven" stop was actually locking in a small
    net loss (~$0.15 on a $250 position) whenever it triggered, not a true
    breakeven. Raised default to 0.15% (covers fees + a small real margin).

Verified clean (re-checked, no issues found):
  - Risk math across all 5 signal types (STANDARD/SURGE, PULLBACK, BB_SQUEEZE,
    BB_LOWER): R:R ratios, breakeven win rates, liquidation buffers (5-8.3x),
    daily-loss-limit buffer (5-8.3 stops) — all internally consistent.
  - BE-stop trigger (0.6%) fires before the trailing-stop trigger for every
    signal type, confirmed no ordering conflict.
  - periodic_scanner's loop is sequential (scan, then sleep) — cannot overlap
    even at SCAN_INTERVAL_MIN=1; a slow scan just pushes the next one later,
    doesn't stack. Not a bug.
  - try_standard, try_surge unchanged and logically sound.
  - Full cross-import check (config.py <-> scanner.py/auto_trade.py/
    backtest.py): 0 missing names. Full compile: all 8 files clean.

=== Follow-up: comparing a parallel-edited upload against this baseline ===

Good additions kept from that upload (all verified working):
  - auto_trade.py: AUTO_REQUIRE_24H_UPTREND / AUTO_MIN_24H_CHANGE_PCT — a
    second, independent 24h-uptrend check at the auto-trade execution layer
    (on top of BB_LOWER's own signal-level check) — genuine defense in depth.
  - scanner.py: try_bb_lower() gained a price-action quality filter — rejects
    if the 15m candle closed in the bottom 25% of its own high-low range
    ("still free-falling, no rejection shown yet"), and now correctly sets
    the returned signal's "price" to the actual 15m breakout close instead of
    inheriting the 1h reference price from base_data (base_data's "price" is
    closes_1h[-1] — a different value than the 15m close that triggered the
    signal).
  - scanner.py: added bb_lower_close_ok flag on the signal dict, checked by
    auto_trade.py before entering — another defense-in-depth pairing.
  - auto_trade.py: handle_closed_position() rewritten — much more reliable
    close-reason detection (widened the closed-PnL timestamp match window,
    added a retry on empty response, and — most importantly — now reads the
    reason the bot itself already knows when IT initiated the close
    (STRUCTURE/PANIC) instead of only ever trying to reverse-engineer it from
    price proximity to tp/sl). Before this, every BE-stop, trailing-stop, or
    structure-exit close would have been misreported as MANUAL/UNKNOWN in the
    closing notification — a real gap from when BE-stop/structure-exit were
    first added, now closed. Also added proper BE/TRAILING/STRUCTURE/PANIC
    labels and emojis instead of just TP/SL/MANUAL/UNKNOWN.

Regressions found and re-fixed (this upload was edited in parallel and lost
some earlier fixes):
  - try_pullback()'s "EMA50 must be rising" check had reverted to the original
    dead-code version (10-bar slice against period=50 -> calculate_ema always
    None -> check never fires). Re-applied the closes_1h[:-5] fix.
  - The /backtest command's confirmation message had reverted to saying
    "Backtest BB_SQUEEZE" again, despite backtest.py actually testing BB_LOWER
    (its own docstring and report headers correctly say BB_LOWER throughout —
    only the scanner.py confirmation message regressed). Re-fixed.
  - AUTO_BB_LOWER_TP_PCT had reverted to 2.0% while AUTO_BB_LOWER_SL_PCT
    stayed at the requested 10.0% — recreating the exact 0.2:1 R:R / 83%
    breakeven-win-rate problem we identified and fixed together last round.
    Re-applied TP=10.0% (clean 1:1 R:R / 50% breakeven WR). Also re-widened
    AUTO_TP1_TRIGGER_PCT_BB_LOWER / AUTO_TRAIL_DISTANCE_PCT_BB_LOWER to
    3.0%/2.0% (were back to the tight 1.0%/0.6% BB_SQUEEZE-inherited values,
    which would have locked in profit almost immediately on any winner,
    making the wider TP unreachable in practice).

Minor cleanup:
  - try_bb_lower() had a structurally-unreachable check ("close >= mid ->
    reject") a few lines after already confirming close < lower — since
    lower < mid always by definition, close < lower already guarantees
    close < mid. Harmless (never blocked anything that should've passed) but
    dead code; removed.

Verified after this pass: try_bb_lower fires correctly on an "absorption"
candle (rejection from the lows) and correctly blocks a genuine free-fall
candle (close at its own low) — the new price-action filter works as
intended. bb_lower_close_ok and the corrected "price" field both confirmed
present on the returned signal. EMA-rising fix re-verified (real value
instead of always-None). R:R confirmed back to 1:1. Full compile +
cross-import re-checked clean across all 8 files.

=== Follow-up: new active-coin pre-filter, same 3 regressions AGAIN ===

Good new addition (verified wired up correctly, not dangling config):
  - scan_once() pre-filter now also requires: |24h change| >= 
    MIN_ABS_CHANGE_24H_PCT (skip flat/dead coins), 24h change >= 
    ACTIVE_MIN_24H_UP_PCT when ACTIVE_REQUIRE_24H_UP is on, bid/ask spread
    <= MAX_SPREAD_PCT (skip illiquid), and caps the candidate list to the
    top MAX_SCAN_SYMBOLS (80) by turnover after filtering. MIN_VOLUME_USD_24H
    raised 3M -> 8M. This uses the ticker's price24hPcnt for a cheap
    pre-filter pass — separate from and complementary to the more precise
    price_change_24h that analyze_coin/try_bb_lower independently recompute
    from actual 1h candle closes. Checked this doesn't break the data flow:
    confirmed try_bb_lower still reads a correctly-populated
    price_change_24h, not the pre-filter's ticker_pc24 field (different
    variables, both correct, no key-rename regression).
  - This also helps the SCAN_INTERVAL_MIN=1 rate-limit concern noted earlier
    — scanning is now capped to the top 80 liquid/active symbols instead of
    the whole market.

Same 3 regressions reappeared a THIRD time in this upload (from the same
earlier, pre-fix baseline each time, it seems) — re-applied again:
  - try_pullback()'s EMA50-rising dead-code bug (closes_1h[-15:-5] against
    period=50 -> always None -> never fires). Fixed to closes_1h[:-5].
  - /backtest confirmation message said "BB_SQUEEZE" again; it runs BB_LOWER.
  - AUTO_BB_LOWER_TP_PCT back to 2.0% (with SL still at the requested 10.0%)
    -> 0.2:1 R:R / 83% breakeven win rate again. Re-widened to 10.0% (1:1).
    AUTO_TP1_TRIGGER_PCT_BB_LOWER / AUTO_TRAIL_DISTANCE_PCT_BB_LOWER had also
    reverted to inherit BB_SQUEEZE's tight 1.0%/0.6% — re-widened to 3.0%/2.0%.
  - (minor, also reappeared) the structurally-unreachable "close >= mid"
    check in try_bb_lower — removed again.

Heads up: if edits keep coming from an older baseline instead of the last
delivered file set, these same 3-4 things will likely keep reverting each
round. Worth starting future edits from whatever this chat delivers instead,
to stop this loop.

Verified after this round: full compile + cross-import clean, try_bb_lower
positive/negative regression re-confirmed (absorption candle fires, free-fall
and downtrend correctly block), R:R back to 1:1.

=== Follow-up: BB_LOWER-only mode + wider SL/TP (explicit request) ===

  - BB_LOWER_TREND_24H_MIN: 2.0% -> 5.0%. The old 2% threshold barely
    filtered anything — most actively-traded alts swing more than that in
    24h on an ordinary day regardless of any real trend. 5% is a much more
    meaningful bar.
  - AUTO_TRADE_SIGNAL_TYPES: "STANDARD,SURGE,PULLBACK,BB_SQUEEZE,BB_LOWER"
    -> "BB_LOWER" only. The other 4 signal types still scan and alert in
    Telegram as before (their ENABLE_* flags are untouched) but can no
    longer auto-trade — this is a hard gate in auto_trade.py's
    handle_signal(), independent of the /sig_on /sig_off runtime toggles.
  - storage.py's default signal_toggles now match: only BB_LOWER defaults
    to True, so /auto's status display doesn't misleadingly show the other
    4 as "enabled" when they can never actually trade.
  - AUTO_BB_LOWER_SL_PCT: 1.2% -> 10.0% (explicit request). IMPORTANT: at
    10x leverage this puts the stop right at the ~10% liquidation line —
    buffer is ~1.0x, meaning on a fast move or slippage the exchange could
    liquidate before the stop order fills. This was flagged explicitly
    before applying; proceeding was a deliberate choice.
  - AUTO_BB_LOWER_TP_PCT: 2.0% -> 10.0% (widened to match, per explicit
    request, R:R now 1:1 / 50% breakeven win rate — was 0.2:1 / 83% before
    this fix, which would have been an unwinnable setup).
  - AUTO_TP1_TRIGGER_PCT_BB_LOWER / AUTO_TRAIL_DISTANCE_PCT_BB_LOWER:
    1.0%/0.6% -> 3.0%/2.0%. These were still sized for the old tight 2% TP
    (inherited from BB_SQUEEZE) — left as-is, trailing would have activated
    at +1% and locked in as little as +0.4% on almost every winning trade,
    making the wider 10% TP essentially unreachable in practice. Scaled up
    to actually give the position room consistent with the new SL/TP scale.

Verified after this round: BB_LOWER still fires correctly above the new 5%
trend threshold, correctly blocks below it (tested at 3%, which used to pass
under the old 2% default). AUTO_TRADE_SIGNAL_TYPES correctly resolves to
{'BB_LOWER'} only. Full compile + cross-import re-checked clean.
