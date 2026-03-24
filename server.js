require('dotenv').config();

const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');
const db = require('./db');
const collector = require('./collector');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));

collector.setBroadcast((msg) => {
  const payload = JSON.stringify(msg);
  wss.clients.forEach(client => {
    if (client.readyState === 1) {
      client.send(payload);
    }
  });
});

wss.on('connection', (ws) => {
  const status = collector.getStatus();
  ws.send(JSON.stringify({ type: 'status', data: status }));
});

app.get('/api/status', (req, res) => {
  res.json(collector.getStatus());
});

app.get('/api/ticks', async (req, res) => {
  try {
    const { market, limit, before } = req.query;
    if (!market) return res.status(400).json({ error: 'market param required' });
    const ticks = await db.getTicks({ market, limit: parseInt(limit) || 3600, before });
    res.json(ticks);
  } catch (err) {
    console.error('/api/ticks error:', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/cycles', async (req, res) => {
  try {
    const { market, limit } = req.query;
    const cycles = await db.getCycles({ market, limit: parseInt(limit) || 200 });
    res.json(cycles);
  } catch (err) {
    console.error('/api/cycles error:', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/export/ticks.json', async (req, res) => {
  try {
    const ticks = await db.getAllTicks();
    res.setHeader('Content-Disposition', 'attachment; filename="polymarket_ticks.json"');
    res.json(ticks);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/export/cycles.json', async (req, res) => {
  try {
    const cycles = await db.getAllCycles();
    res.setHeader('Content-Disposition', 'attachment; filename="polymarket_cycles.json"');
    res.json(cycles);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/export/cycles.csv', async (req, res) => {
  try {
    const cycles = await db.getAllCycles();
    const headers = [
      'cycle_id', 'market', 'cycle_start', 'cycle_end', 'duration_min',
      'outcome', 'final_price_up', 'final_price_down', 'opening_price_up',
      'total_ticks', 'resolved_at'
    ];
    let csv = headers.join(',') + '\n';
    for (const c of cycles) {
      csv += headers.map(h => {
        const val = c[h];
        if (val == null) return '';
        if (typeof val === 'string' && val.includes(',')) return `"${val}"`;
        return val;
      }).join(',') + '\n';
    }
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="polymarket_cycles.csv"');
    res.send(csv);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

async function main() {
  try {
    await db.migrate();
    await db.purgeAll();
    console.log('[server] Database ready, stale data purged');
  } catch (err) {
    console.error('[server] DB migration error:', err.message);
    console.warn('[server] Continuing without DB — will retry on queries');
  }

  collector.start();

  server.listen(PORT, () => {
    console.log(`[server] Listening on http://0.0.0.0:${PORT}`);
  });
}

main();
