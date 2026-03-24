# Polymarket Trading Data Collector & Visualizer

Real-time tracker for Polymarket BTC/ETH binary candle markets (15-min and 1-hour cycles). Polls prices every second, stores ticks in PostgreSQL, streams to a browser dashboard via WebSocket.

## Markets Tracked

| Market | Cycle | Source |
|--------|-------|--------|
| BTC 15MIN | 15-minute candle up/down | `btc-updown-15m-{ts}` |
| BTC 1HOUR | Hourly candle up/down | `btc-up-or-down-hourly` series |
| ETH 15MIN | 15-minute candle up/down | `eth-updown-15m-{ts}` |
| ETH 1HOUR | Hourly candle up/down | `eth-up-or-down-hourly` series |

## Setup

```bash
# Install dependencies
npm install

# Copy env and configure DATABASE_URL
cp .env.example .env

# Run locally (needs PostgreSQL)
npm start
```

## Deploy on Railway

1. Create a new project on [Railway](https://railway.app)
2. Add a **PostgreSQL** plugin — this provides `DATABASE_URL` automatically
3. Deploy this repo (Railway auto-detects Node.js)
4. The app starts on the port Railway assigns via `$PORT`

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | Dashboard UI |
| `GET /api/status` | Current market state |
| `GET /api/ticks?market=BTC_15MIN&limit=3600` | Recent ticks |
| `GET /api/cycles` | Completed cycles |
| `GET /api/export/ticks.json` | Full ticks export |
| `GET /api/export/cycles.json` | Full cycles export |
| `GET /api/export/cycles.csv` | Cycles CSV for pandas |

## Data for Python

```python
import pandas as pd

df = pd.read_json('polymarket_ticks.json')
df['timestamp'] = pd.to_datetime(df['timestamp'])
```

## Architecture

- **server.js** — Express HTTP + WebSocket server
- **collector.js** — Polymarket API poller (1s interval), market discovery, cycle resolution
- **db.js** — PostgreSQL schema, migrations, query helpers
- **public/index.html** — Single-page dashboard (Chart.js realtime charts)
