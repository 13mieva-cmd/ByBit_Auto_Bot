const fs = require('fs');
const path = require('path');
const FILE = path.join(__dirname, require('./config').STATE_FILE || 'state.json');
function loadState(){ try { return JSON.parse(fs.readFileSync(FILE,'utf8')); } catch { return { lastAlerts: {}, activeTrades: [], closedTrades: [], seenNews: [] }; } }
function saveState(s){ fs.writeFileSync(FILE, JSON.stringify(s, null, 2)); }
module.exports = { loadState, saveState, FILE };
