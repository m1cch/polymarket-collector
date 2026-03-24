const db = require('./db');

const GAMMA = 'https://gamma-api.polymarket.com';
const CLOB = 'https://clob.polymarket.com';

const MARKETS_CONFIG = [
  { key: 'BTC_15MIN', coin: 'btc', type: '15m', durationMin: 15 },
  { key: 'BTC_1HOUR', coin: 'btc', type: '1h', durationMin: 60 },
  { key: 'ETH_15MIN', coin: 'eth', type: '15m', durationMin: 15 },
  { key: 'ETH_1HOUR', coin: 'eth', type: '1h', durationMin: 60 },
];

const marketState = new Map();
let broadcast = () => {};

function setBroadcast(fn) {
  broadcast = fn;
}

function get15mSlug(coin) {
  const ts = Math.floor(Date.now() / 1000 / 900) * 900;
  return `${coin}-updown-15m-${ts}`;
}

function get15mCycleId(coin) {
  return Math.floor(Date.now() / 1000 / 900);
}

function get1hSeriesSlug(coin) {
  const map = { btc: 'btc-up-or-down-hourly', eth: 'eth-up-or-down-hourly' };
  return map[coin];
}

function get1hSlug(coin) {
  const months = ['january','february','march','april','may','june','july','august','september','october','november','december'];
  const now = new Date();
  const et = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const month = months[et.getMonth()];
  const day = et.getDate();
  const year = et.getFullYear();
  let hour = et.getHours();
  const ampm = hour >= 12 ? 'pm' : 'am';
  hour = hour % 12 || 12;

  const coinName = coin === 'btc' ? 'bitcoin' : 'ethereum';
  return `${coinName}-up-or-down-${month}-${day}-${year}-${hour}${ampm}-et`;
}

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.json();
}

async function fetchPrices(tokenIds) {
  const payload = tokenIds.map(id => ({ token_id: id, side: 'BUY' }));
  const res = await fetch(`${CLOB}/prices`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(`CLOB prices HTTP ${res.status}`);
  return res.json();
}

async function refreshMarket(cfg) {
  const state = marketState.get(cfg.key) || {};
  try {
    let event;

    if (cfg.type === '15m') {
      const slug = get15mSlug(cfg.coin);
      const events = await fetchJSON(`${GAMMA}/events?slug=${slug}`);
      event = events && events[0];
    } else {
      const seriesSlug = get1hSeriesSlug(cfg.coin);
      let events = await fetchJSON(
        `${GAMMA}/events?series_slug=${seriesSlug}&active=true&closed=false&order=end_date&ascending=true`
      );
      if (!events || events.length === 0) {
        const slug = get1hSlug(cfg.coin);
        events = await fetchJSON(`${GAMMA}/events?slug=${slug}`);
      }
      event = events && events[0];
    }

    if (!event || !event.markets || event.markets.length === 0) {
      console.warn(`[collector] No active event found for ${cfg.key}`);
      state.active = false;
      marketState.set(cfg.key, state);
      return;
    }

    const mkt = event.markets[0];
    const tokenIds = JSON.parse(mkt.clobTokenIds);
    const endDate = new Date(mkt.endDate);
    const startDate = mkt.eventStartTime ? new Date(mkt.eventStartTime) : new Date(endDate.getTime() - cfg.durationMin * 60000);

    const cycleId = cfg.type === '15m'
      ? Math.floor(startDate.getTime() / 1000 / 900)
      : Math.floor(startDate.getTime() / 1000 / 3600);

    Object.assign(state, {
      key: cfg.key,
      active: true,
      eventSlug: event.slug,
      marketId: mkt.id || mkt.conditionId,
      upTokenId: tokenIds[0],
      downTokenId: tokenIds[1],
      cycleStart: startDate.toISOString(),
      cycleEnd: endDate.toISOString(),
      cycleId,
      durationMin: cfg.durationMin,
      acceptingOrders: mkt.acceptingOrders,
      closed: mkt.closed || false,
      priceUp: null,
      priceDown: null,
      resolving: false,
      openingPriceUp: state.openingPriceUp || null,
    });

    marketState.set(cfg.key, state);
    console.log(`[collector] Refreshed ${cfg.key}: cycle #${cycleId}, ends ${endDate.toISOString()}`);
  } catch (err) {
    console.error(`[collector] refreshMarket ${cfg.key} error:`, err.message);
    state.active = false;
    marketState.set(cfg.key, state);
  }
}

async function resolveCycle(cfg, state) {
  if (state.resolving) return;
  state.resolving = true;

  console.log(`[collector] Resolving cycle #${state.cycleId} for ${cfg.key}...`);

  let outcome = null;
  let attempts = 0;

  while (!outcome && attempts < 30) {
    attempts++;
    try {
      const slug = state.eventSlug;
      const events = await fetchJSON(`${GAMMA}/events?slug=${slug}`);
      if (events && events[0]) {
        const mkt = events[0].markets[0];
        if (mkt.closed) {
          const prices = JSON.parse(mkt.outcomePrices);
          const upFinal = parseFloat(prices[0]);
          outcome = upFinal >= 0.5 ? 'UP' : 'DOWN';
        }
      }
    } catch (e) {
      console.warn(`[collector] resolve poll error for ${cfg.key}:`, e.message);
    }
    if (!outcome) await sleep(5000);
  }

  if (!outcome) {
    outcome = (state.priceUp || 0) >= 0.5 ? 'UP' : 'DOWN';
    console.warn(`[collector] Fallback outcome for ${cfg.key}: ${outcome}`);
  }

  const tickCount = await db.getTickCount(cfg.key, state.cycleStart, state.cycleEnd);
  const firstTick = await db.getFirstTick(cfg.key, state.cycleStart);

  const cycle = {
    cycle_id: state.cycleId,
    market: cfg.key,
    cycle_start: state.cycleStart,
    cycle_end: state.cycleEnd,
    duration_min: cfg.durationMin,
    outcome,
    final_price_up: state.priceUp,
    final_price_down: state.priceDown,
    opening_price_up: firstTick ? firstTick.price_up : state.openingPriceUp,
    total_ticks: tickCount,
    resolved_at: new Date().toISOString(),
  };

  try {
    await db.insertCycle(cycle);
    broadcast({ type: 'cycle_close', data: cycle });
    console.log(`[collector] Cycle #${state.cycleId} for ${cfg.key}: ${outcome}`);
  } catch (err) {
    console.error(`[collector] insertCycle error for ${cfg.key}:`, err.message);
  }

  state.resolving = false;
  state.openingPriceUp = null;
  await refreshMarket(cfg);
}

async function pollPrices() {
  const allTokenIds = [];
  const tokenMap = {};

  for (const cfg of MARKETS_CONFIG) {
    const state = marketState.get(cfg.key);
    if (!state || !state.active || !state.upTokenId) continue;
    allTokenIds.push(state.upTokenId, state.downTokenId);
    tokenMap[state.upTokenId] = { key: cfg.key, side: 'up' };
    tokenMap[state.downTokenId] = { key: cfg.key, side: 'down' };
  }

  if (allTokenIds.length === 0) return;

  let pricesResult;
  try {
    pricesResult = await fetchPrices(allTokenIds);
  } catch (err) {
    console.error('[collector] fetchPrices error:', err.message);
    return;
  }

  const now = new Date();
  const unixMs = now.getTime();

  for (const cfg of MARKETS_CONFIG) {
    const state = marketState.get(cfg.key);
    if (!state || !state.active || !state.upTokenId) continue;

    const upPrice = pricesResult[state.upTokenId]?.BUY;
    const downPrice = pricesResult[state.downTokenId]?.BUY;

    if (upPrice == null && downPrice == null) continue;

    state.priceUp = parseFloat(upPrice) || 0;
    state.priceDown = parseFloat(downPrice) || 0;
    const spread = Math.abs(1 - state.priceUp - state.priceDown);

    if (state.openingPriceUp === null) {
      state.openingPriceUp = state.priceUp;
    }

    const endMs = new Date(state.cycleEnd).getTime();
    const secondsRemaining = Math.max(0, Math.floor((endMs - unixMs) / 1000));

    const tick = {
      timestamp: now.toISOString(),
      unix_ms: unixMs,
      market: cfg.key,
      cycle_id: state.cycleId,
      cycle_start: state.cycleStart,
      cycle_end: state.cycleEnd,
      price_up: state.priceUp,
      price_down: state.priceDown,
      spread: parseFloat(spread.toFixed(4)),
      seconds_remaining: secondsRemaining,
    };

    try {
      await db.insertTick(tick);
    } catch (err) {
      console.error(`[collector] insertTick error for ${cfg.key}:`, err.message);
    }

    broadcast({ type: 'tick', data: tick });

    if (secondsRemaining <= 0 && !state.resolving) {
      resolveCycle(cfg, state);
    }
  }
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function start() {
  console.log('[collector] Starting market collector...');

  await Promise.allSettled(MARKETS_CONFIG.map(cfg => refreshMarket(cfg)));
  console.log('[collector] Initial market refresh done');

  setInterval(async () => {
    try {
      await pollPrices();
    } catch (err) {
      console.error('[collector] pollPrices uncaught:', err.message);
    }
  }, 1000);

  setInterval(async () => {
    for (const cfg of MARKETS_CONFIG) {
      const state = marketState.get(cfg.key);
      if (!state || !state.active) {
        await refreshMarket(cfg);
      }
    }
  }, 30000);
}

function getStatus() {
  const result = {};
  for (const cfg of MARKETS_CONFIG) {
    const state = marketState.get(cfg.key);
    if (!state) {
      result[cfg.key] = { active: false };
      continue;
    }
    const endMs = new Date(state.cycleEnd).getTime();
    const secondsRemaining = Math.max(0, Math.floor((endMs - Date.now()) / 1000));
    result[cfg.key] = {
      active: state.active,
      cycleId: state.cycleId,
      cycleStart: state.cycleStart,
      cycleEnd: state.cycleEnd,
      priceUp: state.priceUp,
      priceDown: state.priceDown,
      secondsRemaining,
      durationMin: state.durationMin,
    };
  }
  return result;
}

module.exports = { start, setBroadcast, getStatus };
