const CryptoJS = require('crypto-js');
const cfg = require('./config');
async function request(path, method='GET', params={}){
  const ts = Date.now().toString();
  const recv = '5000';
  const query = method === 'GET' && Object.keys(params).length ? '?' + new URLSearchParams(params).toString() : '';
  const body = method === 'GET' ? '' : JSON.stringify(params);
  const payload = ts + cfg.BYBIT_API_KEY + recv + (method === 'GET' ? query.replace(/^\?/, '') : body);
  const sign = CryptoJS.HmacSHA256(payload, cfg.BYBIT_API_SECRET).toString();
  const headers = {'X-BAPI-API-KEY': cfg.BYBIT_API_KEY,'X-BAPI-TIMESTAMP': ts,'X-BAPI-RECV-WINDOW': recv,'X-BAPI-SIGN': sign,'X-BAPI-SIGN-TYPE': '2','Content-Type': 'application/json'};
  const res = await fetch(cfg.BYBIT_BASE + path + query, {method, headers, body: method==='GET' ? undefined : body});
  return res.json();
}
async function publicGet(path){ const res = await fetch(cfg.BYBIT_BASE + path); return res.json(); }
async function getSymbols(){ const j = await publicGet('/v5/market/tickers?category=linear'); return (j?.result?.list || []).filter(x => /USDT$/.test(x.symbol)); }
async function getTicker(symbol){ const j = await publicGet(`/v5/market/tickers?category=linear&symbol=${symbol}`); return j?.result?.list?.[0] || null; }
async function getKlines(symbol, interval, limit=120){ const j = await publicGet(`/v5/market/kline?category=linear&symbol=${symbol}&interval=${interval}&limit=${limit}`); return (j?.result?.list || []).reverse(); }
async function getOI(symbol, interval='60', limit=20){ const j = await publicGet(`/v5/market/open-interest?category=linear&symbol=${symbol}&intervalTime=${interval}&limit=${limit}`); return j?.result?.list || []; }
async function getFunding(symbol){ const j = await publicGet(`/v5/market/funding/history?category=linear&symbol=${symbol}&limit=3`); return j?.result?.list || []; }
async function getPosition(symbol){ const j = await request('/v5/position/list', 'GET', {category:'linear', symbol}); return j?.result?.list?.[0] || null; }
async function placeOrder({symbol, side, qty, reduceOnly=false, orderType='Market', takeProfit, stopLoss}){ return request('/v5/order/create', 'POST', {category:'linear', symbol, side, orderType, qty: String(qty), timeInForce:'IOC', reduceOnly, takeProfit: takeProfit ? String(takeProfit) : undefined, stopLoss: stopLoss ? String(stopLoss) : undefined}); }
async function setTradingStop({symbol, takeProfit, stopLoss, trailingStop, activePrice, positionIdx=0}){ return request('/v5/position/trading-stop', 'POST', {category:'linear', symbol, tpslMode:'Full', takeProfit: takeProfit ? String(takeProfit) : undefined, stopLoss: stopLoss ? String(stopLoss) : undefined, trailingStop: trailingStop ? String(trailingStop) : undefined, activePrice: activePrice ? String(activePrice) : undefined, positionIdx}); }
async function cancelAll(symbol){ return request('/v5/order/cancel-all', 'POST', {category:'linear', symbol}); }
module.exports = { request, publicGet, getSymbols, getTicker, getKlines, getOI, getFunding, getPosition, placeOrder, setTradingStop, cancelAll };
