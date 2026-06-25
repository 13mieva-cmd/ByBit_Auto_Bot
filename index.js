const http = require('http');
const TelegramBot = require('node-telegram-bot-api');
const cfg = require('./config');
const { loadState, saveState } = require('./state');
const { setTradingStop, placeOrder, getPosition, cancelAll } = require('./bybit');

const state = loadState();
state.settings = state.settings || { autoTrading: true, liveTrading: false };
state.settings.autoTrading = state.settings.autoTrading !== false && cfg.AUTO_TRADING;
state.settings.liveTrading = state.settings.liveTrading || cfg.LIVE_TRADING;
state.activeTrades = state.activeTrades || [];
state.closedTrades = state.closedTrades || [];

const BASE = 'https://api.bybit.com';
function log(m) { console.log(`[${new Date().toISOString()}] ${m}`); }
function num(v) { const x = Number(v); return Number.isFinite(x) ? x : null; }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function roundQty(q) { return Math.max(Number(q.toFixed(3)), 0.001); }

const server = http.createServer((_, res) => { res.writeHead(200); res.end('OK'); });
const PORT = Number(process.env.PORT || 10000);
server.listen(PORT, '0.0.0.0', () => {
  log('http up');
  const selfUrl = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;
  setInterval(() => {
    fetch(selfUrl).catch(() => {});
    log('self-ping');
  }, 4 * 60 * 1000);
});

if (!cfg.BOT_TOKEN) {
  log('BOT_TOKEN missing');
  process.exit(1);
}

const bot = new TelegramBot(cfg.BOT_TOKEN, {
  polling: { interval: 1000, autoStart: true, params: { timeout: 10 } }
});
bot.on('polling_error', e => log(`telegram: ${e.message}`));

function mainKb() {
  return {
    reply_markup: {
      keyboard: [
        ['Статус', 'Помощь'],
        ['Мои сделки', 'Я вышел'],
        ['Подключить авто-торговлю', 'Отключить авто-торговлю'],
        ['Включить live trading', 'Выключить live trading'],
        ['Сброс']
      ],
      resize_keyboard: true
    }
  };
}

function statusText() {
  return [
    '🤖 Smart Money Bot v29 proper fixed',
    `Авто-торговля: ${state.settings.autoTrading ? 'ON' : 'OFF'}`,
    `Live trading: ${state.settings.liveTrading ? 'ON' : 'OFF'}`,
    `API key: ${cfg.BYBIT_API_KEY ? '✅ SET' : '❌ MISSING'}`,
    `Сделок: ${state.activeTrades.length}`,
    `Min score: ${cfg.MIN_SCORE} | Min R/R: ${cfg.MIN_RR}`,
    `Min ликвидность: $${cfg.MIN_TURNOVER_24H / 1e6}M`,
    `Интервал: ${cfg.ALERT_INTERVAL_MS / 60000} мин`
  ].join('\n');
}

async function bybitGet(path, retries = 2) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const r = await fetch(BASE + path, { signal: AbortSignal.timeout(6000) });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return r.json();
    } catch (e) {
      if (attempt === retries) throw e;
      await sleep(2000 * (attempt + 1));
    }
  }
}

async function getAllSymbols() {
  const j = await bybitGet('/v5/market/tickers?category=linear');
  return (j?.result?.list || []).filter(x => /USDT$/.test(x.symbol) && (num(x.turnover24h) || 0) >= cfg.MIN_TURNOVER_24H);
}
async function getTicker(symbol) {
  const j = await bybitGet(`/v5/market/tickers?category=linear&symbol=${symbol}`);
  return j?.result?.list?.[0] || null;
}
async function getOI(symbol, interval, limit) {
  const j = await bybitGet(`/v5/market/open-interest?category=linear&symbol=${symbol}&intervalTime=${interval}&limit=${limit}`);
  return j?.result?.list || [];
}
async function getKlines(symbol, interval, limit) {
  const j = await bybitGet(`/v5/market/kline?category=linear&symbol=${symbol}&interval=${interval}&limit=${limit}`);
  return j?.result?.list || [];
}
async function getFunding(symbol) {
  const j = await bybitGet(`/v5/market/funding/history?category=linear&symbol=${symbol}&limit=3`);
  return j?.result?.list || [];
}

function calcATR(klines, period = 14) {
  if (!klines || klines.length < period + 1) return null;
  const trs = [];
  for (let i = 0; i < period; i++) {
    const hi = num(klines[i][2]);
    const lo = num(klines[i][3]);
    const pc = num(klines[i + 1][4]);
    if (hi == null || lo == null || pc == null) return null;
    trs.push(Math.max(hi - lo, Math.abs(hi - pc), Math.abs(lo - pc)));
  }
  return trs.reduce((a, b) => a + b, 0) / trs.length;
}

function analyzeOI(oiList) {
  if (!oiList || oiList.length < 3) return { growing: false, changePct: 0, acceleration: 0 };
  const vals = oiList.map(x => num(x?.openInterest)).filter(v => v != null);
  if (vals.length < 3) return { growing: false, changePct: 0, acceleration: 0 };
  const latest = vals[0];
  const oldest = vals[vals.length - 1];
  const mid = vals[Math.floor(vals.length / 2)];
  const changePct = (latest - oldest) / oldest * 100;
  const firstHalfChg = (mid - oldest) / oldest * 100;
  const secondHalfChg = (latest - mid) / mid * 100;
  const acceleration = secondHalfChg - firstHalfChg;
  return { growing: changePct > 0.5, changePct, acceleration, latest, oldest };
}

function oiPriceDivergence(oiAnalysis, priceChangePct) {
  if (!oiAnalysis.growing) return { type: 'none', strength: 0 };
  const oiChg = Math.abs(oiAnalysis.changePct);
  const prChg = Math.abs(priceChangePct);
  if (oiChg >= 1.5 && prChg < 0.3) return { type: 'accumulation', strength: 3 };
  if (oiChg >= 0.8 && prChg < 0.5) return { type: 'accumulation', strength: 2 };
  if (oiChg >= 1.0 && prChg >= 0.5) return { type: 'confirmation', strength: 2 };
  if (oiChg >= 0.5 && prChg >= 0.3) return { type: 'confirmation', strength: 1 };
  return { type: 'weak', strength: 0 };
}

function tfTrend(klines) {
  if (!klines || klines.length < 8) return 'flat';
  const c = klines.slice(0, 8).map(k => num(k[4])).filter(v => v != null).reverse();
  if (c.length < 8) return 'flat';
  const k = 2 / 9;
  let ema = c[0];
  let emaPrev = c[0];
  for (let i = 1; i < c.length; i++) ema = c[i] * k + ema * (1 - k);
  for (let i = 1; i < c.length - 1; i++) emaPrev = c[i] * k + emaPrev * (1 - k);
  const slope = ema - emaPrev;
  const mid = (Math.max(...c) + Math.min(...c)) / 2;
  const lastClose = c[c.length - 1];
  const upStrict = c[c.length - 1] > c[c.length - 2] && c[c.length - 2] > c[c.length - 3];
  const downStrict = c[c.length - 1] < c[c.length - 2] && c[c.length - 2] < c[c.length - 3];
  if ((slope > 0 && lastClose > mid) || upStrict) return 'up';
  if ((slope < 0 && lastClose < mid) || downStrict) return 'down';
  return 'flat';
}

function dailyTrend(klines) {
  if (!klines || klines.length < 5) return 'flat';
  const c = klines.slice(0, 5).map(k => num(k[4])).filter(v => v != null).reverse();
  if (c.length < 5) return 'flat';
  const rising = c[4] > c[0] && c[4] > c[2];
  const falling = c[4] < c[0] && c[4] < c[2];
  return rising ? 'up' : falling ? 'down' : 'flat';
}

function rrOk(entry, sl, tp1) {
  const risk = Math.abs(entry - sl);
  const reward = Math.abs(tp1 - entry);
  if (risk <= 0) return false;
  return reward / risk >= cfg.MIN_RR;
}

function tradeCard(t) {
  return [
    `${t.symbol} | ${t.side || 'LONG'}`,
    `entry: ${t.entry}`,
    `sl: ${t.sl}`,
    `tp1: ${t.tp1}`,
    `tp2: ${t.tp2}`,
    `qty: ${t.qty || '-'}`
  ].join('\n');
}

async function sendSignal(sig, kind = 'SIGNAL') {
  return bot.sendMessage(cfg.CHAT_ID, [
    `${kind} ${sig.symbol}`,
    `score: ${sig.score}`,
    `entry: ${sig.entry}`,
    `sl: ${sig.sl}`,
    `tp1: ${sig.tp1}`,
    `tp2: ${sig.tp2}`,
    `trend: ${sig.tf5}/${sig.tf1h}/${sig.tf4h} daily:${sig.daily}`,
    `oi: ${Number(sig.oi.changePct || 0).toFixed(2)}% accel:${Number(sig.oi.acceleration || 0).toFixed(2)}`
  ].join('\n'), mainKb()).catch(e => log(`sendSignal: ${e.message}`));
}

async function openTrade(sig) {
  if (!state.settings.autoTrading) return false;
  if (!state.settings.liveTrading) return false;
  if (!cfg.BYBIT_API_KEY || !cfg.BYBIT_API_SECRET) return false;
  if (state.activeTrades.length >= cfg.MAX_OPEN_TRADES) return false;
  if (state.activeTrades.find(t => t.symbol === sig.symbol)) return false;
  try {
    const posRes = await getPosition(sig.symbol);
    const posList = posRes?.result?.list || [];
    if (posList.some(p => (num(p.size) || 0) > 0)) return false;
    const qty = roundQty((cfg.ORDER_USDT * cfg.LEVERAGE) / sig.entry);
    const r = await placeOrder({ symbol: sig.symbol, side: 'Buy', qty, orderType: 'Market' });
    await setTradingStop({
      symbol: sig.symbol,
      takeProfit: sig.tp2,
      stopLoss: sig.sl,
      trailingStop: sig.entry * (cfg.TRAILING_STOP_PCT / 100),
      activePrice: sig.tp1
    });
    const trade = {
      symbol: sig.symbol,
      side: 'LONG',
      entry: sig.entry,
      sl: sig.sl,
      tp1: sig.tp1,
      tp2: sig.tp2,
      qty,
      openedAt: Date.now(),
      orderId: r?.result?.orderId || null,
      score: sig.score,
      oi: sig.oi,
      tf5: sig.tf5,
      tf1h: sig.tf1h,
      tf4h: sig.tf4h,
      daily: sig.daily
    };
    state.activeTrades.push(trade);
    saveState(state);
    await sendSignal(trade, 'OPENED');
    return true;
  } catch (e) {
    log(`openTrade ${sig.symbol}: ${e.message}`);
    return false;
  }
}

async function closeTrade(t, reason = 'manual') {
  try {
    await cancelAll(t.symbol).catch(() => {});
    const posRes = await getPosition(t.symbol).catch(() => null);
    const pos = posRes?.result?.list?.[0] || null;
    const size = Math.abs(num(pos?.size) || num(t.qty) || 0);
    if (size > 0 && state.settings.liveTrading) {
      await placeOrder({ symbol: t.symbol, side: 'Sell', qty: roundQty(size), reduceOnly: true, orderType: 'Market' });
    }
  } catch (e) {
    log(`closeTrade ${t.symbol}: ${e.message}`);
  }
  state.activeTrades = state.activeTrades.filter(x => x.symbol !== t.symbol);
  state.closedTrades.unshift({ ...t, closedAt: Date.now(), reason });
  state.closedTrades = state.closedTrades.slice(0, 200);
  saveState(state);
  await sendSignal({ ...t }, `CLOSED ${reason}`);
}

let scanRunning = false;
let scanStartTime = 0;

async function evaluateSymbol(symbol) {
  const [k5, k1h, k4h, kd, oi1h, funding, ticker] = await Promise.all([
    getKlines(symbol, '5', 120),
    getKlines(symbol, '60', 120),
    getKlines(symbol, '240', 120),
    getKlines(symbol, 'D', 10),
    getOI(symbol, '60', 12),
    getFunding(symbol),
    getTicker(symbol)
  ]);

 
  const tf5 = tfTrend(k5);
const tf1h = tfTrend(k1h);
const tf4h = tfTrend(k4h);
const daily = dailyTrend(kd);

if (!(tf1h === 'up' && tf4h === 'up' && (tf5 === 'up' || tf5 === 'flat'))) return null;
if (daily === 'down') return null;

  const oi = analyzeOI(oi1h);
  if (!oi.growing || oi.acceleration < cfg.OI_ACCEL_THRESHOLD) return null;

  const price = num(ticker?.lastPrice);
  const price24h = num(ticker?.price24hPcnt) ? num(ticker.price24hPcnt) * 100 : 0;
  const div = oiPriceDivergence(oi, price24h);
  const atr = calcATR(k1h, 14);
  if (!price || !atr) return null;

  const entry = price;
  const sl = +(entry - atr * 1.5).toFixed(6);
  const tp1 = +(entry + (entry - sl) * 1.5).toFixed(6);
  const tp2 = +(entry + (entry - sl) * 3.5).toFixed(6);
  if (!rrOk(entry, sl, tp1)) return null;

  const fr = num(funding?.[0]?.fundingRate) || 0;
  const fundingOk = fr <= cfg.FUNDING_THRESHOLD;
  if (!fundingOk) return null;

  let score = 0;
  score += 40;
  score += Math.min(40, Math.max(0, oi.changePct * 10));
  score += Math.min(30, Math.max(0, oi.acceleration * 20));
  score += div.strength * 10;
  score += fundingOk ? 10 : 0;

  const sig = { symbol, entry, sl, tp1, tp2, score: Math.round(score), tf5, tf1h, tf4h, daily, oi };
  return sig.score >= cfg.MIN_SCORE ? sig : null;
}

async function scan() {
  if (scanRunning) return;
  scanRunning = true;
  scanStartTime = Date.now();
  log('scan start');
  try {
    const symbols = await getAllSymbols();
    for (const s of symbols.slice(0, 60)) {
      try {
        const sig = await evaluateSymbol(s.symbol);
        if (!sig) continue;
        const fp = `${sig.symbol}:${sig.entry}:${sig.score}`;
        if (!state.lastSignals.includes(fp)) {
          state.lastSignals.push(fp);
          state.lastSignals = state.lastSignals.slice(-200);
          saveState(state);
          await sendSignal(sig);
          await openTrade(sig);
        }
      } catch (e) {
        log(`symbol ${s.symbol}: ${e.message}`);
      }
    }
  } finally {
    scanRunning = false;
    log('scan done');
  }
}

async function updateTrades() {
  for (const t of [...state.activeTrades]) {
    try {
      const ticker = await getTicker(t.symbol);
      const last = num(ticker?.lastPrice);
      if (!last) continue;
      if (last <= t.sl) await closeTrade(t, 'SL');
      else if (last >= t.tp2) await closeTrade(t, 'TP2');
    } catch (e) {
      log(`updateTrades ${t.symbol}: ${e.message}`);
    }
  }
}

bot.onText(/\/start/, m => bot.sendMessage(m.chat.id, `🤖 Smart Money Bot v29 proper fixed запущен.\n${statusText()}\n\n/test — проверка\n/scan — сканировать сейчас`, mainKb()));
bot.onText(/\/status/, m => bot.sendMessage(m.chat.id, statusText(), mainKb()));
bot.onText(/\/test/, m => bot.sendMessage(m.chat.id, 'Бот на связи ✅', mainKb()));
bot.onText(/\/scan/, async m => {
  await bot.sendMessage(m.chat.id, 'Сканирую...', mainKb());
  await scan();
});
bot.onText(/\/trades/, m => bot.sendMessage(m.chat.id, state.activeTrades.length ? state.activeTrades.map(tradeCard).join('\n\n') : 'Сделок нет.', mainKb()));
bot.onText(/\/reset/, m => {
  state.lastSignals = [];
  state.activeTrades = [];
  saveState(state);
  bot.sendMessage(m.chat.id, 'Сброс.', mainKb());
});

bot.on('message', m => {
  if (!m.text || m.text.startsWith('/')) return;
  const t = m.text.trim().toLowerCase();
  if (t === 'статус') return bot.sendMessage(m.chat.id, statusText(), mainKb());
  if (t === 'помощь') return bot.sendMessage(m.chat.id, '/start /status /test /scan /reset /trades', mainKb());
  if (t === 'мои сделки') return bot.sendMessage(m.chat.id, state.activeTrades.length ? state.activeTrades.map(tradeCard).join('\n\n') : 'Сделок нет.', mainKb());
  if (t === 'я вышел' || t === 'пропустить') {
    state.activeTrades = [];
    saveState(state);
    return bot.sendMessage(m.chat.id, 'Остановлено.', mainKb());
  }
  if (t === 'сброс') {
    state.lastSignals = [];
    state.activeTrades = [];
    saveState(state);
    return bot.sendMessage(m.chat.id, 'Сброс.', mainKb());
  }
  if (t === 'подключить авто-торговлю') {
    state.settings.autoTrading = true;
    saveState(state);
    return bot.sendMessage(m.chat.id, 'Авто ON.', mainKb());
  }
  if (t === 'отключить авто-торговлю') {
    state.settings.autoTrading = false;
    saveState(state);
    return bot.sendMessage(m.chat.id, 'Авто OFF.', mainKb());
  }
  if (t === 'включить live trading') {
    state.settings.liveTrading = true;
    saveState(state);
    return bot.sendMessage(m.chat.id, 'Live ON.', mainKb());
  }
  if (t === 'выключить live trading') {
    state.settings.liveTrading = false;
    saveState(state);
    return bot.sendMessage(m.chat.id, 'Live OFF.', mainKb());
  }
});

if (cfg.CHAT_ID) {
  bot.sendMessage(cfg.CHAT_ID, `🤖 Smart Money Bot v29 proper fixed запущен.\n${statusText()}\n\n/test — проверка\n/scan — сканировать сейчас`, mainKb()).catch(e => log(`boot: ${e.message}`));
}

async function continuousScan() {
  while (true) {
    try {
      await scan();
    } catch (e) {
      const msg = e instanceof AggregateError ? `AggregateError: ${e.errors?.map(x => x.message).join(', ')}` : e.message;
      log(`continuousScan error: ${msg}`);
    }
    await sleep(30000);
  }
}

process.on('unhandledRejection', reason => {
  const msg = reason instanceof AggregateError ? `AggregateError: ${reason.errors?.map(x => x.message).join(', ')}` : String(reason?.message || reason);
  log(`unhandledRejection: ${msg}`);
});

continuousScan();
setInterval(updateTrades, 15000);
setInterval(() => saveState(state), 10000);
setInterval(() => log(`heartbeat | scanRunning:${scanRunning} | trades:${state.activeTrades.length}`), 120000);
setInterval(() => {
  if (scanRunning && Date.now() - scanStartTime > 300000) {
    log('WATCHDOG: scan stuck 5min, forcing reset');
    scanRunning = false;
  }
}, 60000);
