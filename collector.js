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

function getETDate() {
  const utcNow = new Date();
  const fmt = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric', month: 'numeric', day: 'numeric',
    hour: 'numeric', hour12: false,
  });
  const parts = {};
  for (const p of fmt.formatToParts(utcNow)) parts[p.type] = p.value;
  return {
    year: parseInt(parts.year),
    month: parseInt(parts.month) - 1,
    day: parseInt(parts.day),
    hour: parseInt(parts.hour),
  };
}

function get1hSlug(coin) {
  const months = ['january','february','march','april','may','june','july','august','september','october','november','december'];
  const et = getETDate();
  const month = months[et.month];
  const day = et.day;
  const year = et.year;
  let hour = et.hour;
  const ampm = hour >= 12 ? 'pm' : 'am';
  hour = hour % 12 || 12;
  const coinName = coin === 'btc' ? 'bitcoin' : 'ethereum';
  return `${coinName}-up-or-down-${month}-${day}-${year}-${hour}${ampm}-et`;
}

function get1hSlugPrev(coin) {
  const months = ['january','february','march','april','may','june','july','august','september','october','november','december'];
  const utcNow = new Date(Date.now() - 3600000);
  const fmt = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric', month: 'numeric', day: 'numeric',
    hour: 'numeric', hour12: false,
  });
  const parts = {};
  for (const p of fmt.formatToParts(utcNow)) parts[p.type] = p.value;
  const et = { year: parseInt(parts.year), month: parseInt(parts.month) - 1, day: parseInt(parts.day), hour: parseInt(parts.hour) };
  const month = months[et.month];
  let hour = et.hour;
  const ampm = hour >= 12 ? 'pm' : 'am';
  hour = hour % 12 || 12;
  const coinName = coin === 'btc' ? 'bitcoin' : 'ethereum';
  return `${coinName}-up-or-down-${month}-${et.day}-${et.year}-${hour}${ampm}-et`;
}

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.json();
}

async function fetchMidpoint(tokenId) {
  const res = await fetch(`${CLOB}/midpoint?token_id=${tokenId}`);
  if (!res.ok) throw new Error(`CLOB midpoint HTTP ${res.status}`);
  const data = await res.json();
  return parseFloat(data.mid);
}

async function fetchAllMidpoints(pairs) {
  const results = {};
  const promises = pairs.map(async ({ key, upId, downId }) => {
    const [upMid, downMid] = await Promise.all([
      fetchMidpoint(upId),
      fetchMidpoint(downId),
    ]);
    results[key] = { up: upMid, down: downMid };
  });
  await Promise.all(promises);
  return results;
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
      const slug = get1hSlug(cfg.coin);
      let events = await fetchJSON(`${GAMMA}/events?slug=${slug}`);
      if (!events || events.length === 0) {
        const slugPrev = get1hSlugPrev(cfg.coin);
        events = await fetchJSON(`${GAMMA}/events?slug=${slugPrev}`);
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
  const pairs = [];
  for (const cfg of MARKETS_CONFIG) {
    const state = marketState.get(cfg.key);
    if (!state || !state.active || !state.upTokenId) continue;
    pairs.push({ key: cfg.key, upId: state.upTokenId, downId: state.downTokenId });
  }
  if (pairs.length === 0) return;

  let midpoints;
  try {
    midpoints = await fetchAllMidpoints(pairs);
  } catch (err) {
    console.error('[collector] fetchMidpoints error:', err.message);
    return;
  }

  const now = new Date();
  const unixMs = now.getTime();

  for (const cfg of MARKETS_CONFIG) {
    const state = marketState.get(cfg.key);
    if (!state || !state.active || !state.upTokenId) continue;

    const mp = midpoints[cfg.key];
    if (!mp || (isNaN(mp.up) && isNaN(mp.down))) continue;

    let priceUp = mp.up;
    let priceDown = mp.down;

    if (isNaN(priceUp) && !isNaN(priceDown)) priceUp = 1 - priceDown;
    if (isNaN(priceDown) && !isNaN(priceUp)) priceDown = 1 - priceUp;

    priceUp = Math.round(priceUp * 10000) / 10000;
    priceDown = Math.round(priceDown * 10000) / 10000;

    state.priceUp = priceUp;
    state.priceDown = priceDown;
    const spread = Math.round(Math.abs(1 - priceUp - priceDown) * 10000) / 10000;

    if (state.openingPriceUp === null) {
      state.openingPriceUp = priceUp;
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
      price_up: priceUp,
      price_down: priceDown,
      spread,
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
