const CryptoJS = require('crypto-js');
const { BYBIT_API_KEY, BYBIT_API_SECRET, BYBIT_BASE } = require('./config');
function clean(obj){ return Object.fromEntries(Object.entries(obj).filter(([,v]) => v !== undefined && v !== null && v !== '')); }
function signGet(params, ts) {
  const sorted = Object.keys(params).sort().map(k => `${k}=${params[k]}`).join('&');
  return CryptoJS.HmacSHA256(`${ts}${BYBIT_API_KEY}5000${sorted}`, BYBIT_API_SECRET).toString();
}
function signPost(body, ts) {
  return CryptoJS.HmacSHA256(`${ts}${BYBIT_API_KEY}5000${JSON.stringify(body)}`, BYBIT_API_SECRET).toString();
}
async function request(path, method = 'GET', params = {}) {
  const ts = Date.now().toString();
  const payload = clean(params);
  const url = `${BYBIT_BASE}${path}${method === 'GET' && Object.keys(payload).length ? `?${new URLSearchParams(payload)}` : ''}`;
  const headers = {
    'X-BAPI-API-KEY': BYBIT_API_KEY,
    'X-BAPI-TIMESTAMP': ts,
    'X-BAPI-RECV-WINDOW': '5000',
    'X-BAPI-SIGN': method === 'GET' ? signGet(payload, ts) : signPost(payload, ts),
    'X-BAPI-SIGN-TYPE': '2',
    'Content-Type': 'application/json'
  };
  const res = await fetch(url, { method, headers, body: method === 'GET' ? undefined : JSON.stringify(payload) });
  return res.json();
}
async function setTradingStop({ symbol, takeProfit, stopLoss, trailingStop, activePrice, positionIdx = 0 }) {
  return request('/v5/position/trading-stop', 'POST', clean({
    category: 'linear', symbol, tpslMode: 'Full',
    takeProfit: takeProfit != null ? String(takeProfit) : undefined,
    stopLoss: stopLoss != null ? String(stopLoss) : undefined,
    trailingStop: trailingStop != null ? String(trailingStop) : undefined,
    activePrice: activePrice != null ? String(activePrice) : undefined,
    positionIdx
  }));
}
async function placeOrder({ symbol, side, qty, reduceOnly = false, orderType = 'Market' }) {
  return request('/v5/order/create', 'POST', clean({
    category: 'linear', symbol, side, orderType,
    qty: String(qty), timeInForce: 'IOC', reduceOnly
  }));
}
async function getPosition(symbol) {
  return request('/v5/position/list', 'GET', { category: 'linear', symbol });
}
async function cancelAll(symbol) {
  return request('/v5/order/cancel-all', 'POST', { category: 'linear', symbol });
}
module.exports = { setTradingStop, placeOrder, getPosition, cancelAll };
