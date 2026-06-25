const fs = require('fs'), path = require('path');
const FILE = path.join(__dirname, 'state.json');
function loadState() {
  try { return JSON.parse(fs.readFileSync(FILE, 'utf8')); }
  catch { return { lastSignals: [], settings: { autoTrading: true, liveTrading: false }, activeTrades: [], closedTrades: [] }; }
}
function saveState(s) { fs.writeFileSync(FILE, JSON.stringify(s, null, 2)); }
module.exports = { loadState, saveState };
