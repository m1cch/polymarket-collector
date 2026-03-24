const db = require('./db');

const GAMMA = 'https://gamma-api.polymarket.com';

const MARKETS_CONFIG = [
  { key: 'BTC_15MIN', coin: 'btc', type: '15m', durationMin: 15 },
  { key: 'BTC_1HOUR', coin: 'btc', type: '1h', durationMin: 60 },
  { key: 'ETH_15MIN', coin: 'eth', type: '15m', durationMin: 15 },
  { key: 'ETH_1HOUR', coin: 'eth', type: '1h', durationMin: 60 },
];

const marketState = new Map();
let broadcast = () => {};

function setBroadcast(fn) { broadcast = fn; }

function get15mSlug(coin) {
  const ts = Math.floor(Date.now() / 1000 / 900) * 900;
  return `${coin}-updown-15m-${ts}`;
}

function getETDate(offsetMs = 0) {
  const utcNow = new Date(Date.now() + offsetMs);
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
    hour: parseInt(parts.hour === '24' ? '0' : parts.hour),
  };
}

function get1hSlug(coin, offsetMs = 0) {
  const months = ['january','february','march','april','may','june','july','august','september','october','november','december'];
  const et = getETDate(offsetMs);
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

async function refreshMarket(cfg) {
  const state = marketState.get(cfg.key) || {};
  try {
    let event;

    if (cfg.type === '15m') {
      const slug = get15mSlug(cfg.coin);
      const events = await fetchJSON(`${GAMMA}/events?slug=${slug}`);
      event = events?.[0];
    } else {
      const slug = get1hSlug(cfg.coin);
      let events = await fetchJSON(`${GAMMA}/events?slug=${slug}`);
      if (!events?.length) {
        events = await fetchJSON(`${GAMMA}/events?slug=${get1hSlug(cfg.coin, -3600000)}`);
      }
      event = events?.[0];
    }

    if (!event?.markets?.length) {
      console.warn(`[collector] No event for ${cfg.key}`);
      state.active = false;
      marketState.set(cfg.key, state);
      return;
    }

    const mkt = event.markets[0];
    const endDate = new Date(mkt.endDate);
    const startDate = mkt.eventStartTime
      ? new Date(mkt.eventStartTime)
      : new Date(endDate.getTime() - cfg.durationMin * 60000);

    const cycleId = cfg.type === '15m'
      ? Math.floor(startDate.getTime() / 1000 / 900)
      : Math.floor(startDate.getTime() / 1000 / 3600);

    const outcomePrices = JSON.parse(mkt.outcomePrices);

    Object.assign(state, {
      key: cfg.key,
      active: true,
      eventSlug: event.slug,
      gammaMarketId: mkt.id,
      conditionId: mkt.conditionId,
      cycleStart: startDate.toISOString(),
      cycleEnd: endDate.toISOString(),
      cycleId,
      durationMin: cfg.durationMin,
      closed: mkt.closed || false,
      priceUp: parseFloat(outcomePrices[0]),
      priceDown: parseFloat(outcomePrices[1]),
      resolving: false,
      openingPriceUp: state.openingPriceUp ?? null,
    });

    marketState.set(cfg.key, state);
    console.log(`[collector] Refreshed ${cfg.key}: cycle #${cycleId}, market=${mkt.id}, ends ${endDate.toISOString()}, UP=${outcomePrices[0]}`);
  } catch (err) {
    console.error(`[collector] refreshMarket ${cfg.key}:`, err.message);
    state.active = false;
    marketState.set(cfg.key, state);
  }
}

async function pollMarketPrice(cfg) {
  const state = marketState.get(cfg.key);
  if (!state?.active || !state.gammaMarketId) return;

  const data = await fetchJSON(`${GAMMA}/markets/${state.gammaMarketId}`);
  const outcomePrices = JSON.parse(data.outcomePrices);

  const priceUp = parseFloat(outcomePrices[0]);
  const priceDown = parseFloat(outcomePrices[1]);

  return { priceUp, priceDown, bestBid: data.bestBid, bestAsk: data.bestAsk, closed: data.closed };
}

async function pollAllPrices() {
  const now = new Date();
  const unixMs = now.getTime();

  const results = await Promise.allSettled(
    MARKETS_CONFIG.map(async cfg => {
      const state = marketState.get(cfg.key);
      if (!state?.active || !state.gammaMarketId) return null;

      let priceData;
      try {
        priceData = await pollMarketPrice(cfg);
      } catch (err) {
        return null;
      }
      if (!priceData) return null;

      state.priceUp = priceData.priceUp;
      state.priceDown = priceData.priceDown;

      const spread = priceData.bestAsk != null && priceData.bestBid != null
        ? Math.round((priceData.bestAsk - priceData.bestBid) * 10000) / 10000
        : 0;

      if (state.openingPriceUp === null) {
        state.openingPriceUp = priceData.priceUp;
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
        price_up: priceData.priceUp,
        price_down: priceData.priceDown,
        spread,
        seconds_remaining: secondsRemaining,
      };

      try { await db.insertTick(tick); } catch (err) {
        console.error(`[collector] insertTick ${cfg.key}:`, err.message);
      }

      broadcast({ type: 'tick', data: tick });

      if (secondsRemaining <= 0 && !state.resolving) {
        resolveCycle(cfg, state);
      }

      return tick;
    })
  );
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
      const data = await fetchJSON(`${GAMMA}/markets/${state.gammaMarketId}`);
      if (data.closed) {
        const prices = JSON.parse(data.outcomePrices);
        const upFinal = parseFloat(prices[0]);
        outcome = upFinal >= 0.5 ? 'UP' : 'DOWN';
        state.priceUp = upFinal;
        state.priceDown = parseFloat(prices[1]);
      }
    } catch (e) {
      console.warn(`[collector] resolve poll ${cfg.key}:`, e.message);
    }
    if (!outcome) await sleep(5000);
  }

  if (!outcome) {
    outcome = (state.priceUp || 0) >= 0.5 ? 'UP' : 'DOWN';
    console.warn(`[collector] Fallback outcome ${cfg.key}: ${outcome}`);
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
    console.log(`[collector] Cycle #${state.cycleId} ${cfg.key}: ${outcome}`);
  } catch (err) {
    console.error(`[collector] insertCycle ${cfg.key}:`, err.message);
  }

  state.resolving = false;
  state.openingPriceUp = null;
  await refreshMarket(cfg);
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function start() {
  console.log('[collector] Starting...');
  await Promise.allSettled(MARKETS_CONFIG.map(cfg => refreshMarket(cfg)));
  console.log('[collector] Initial refresh done');

  setInterval(async () => {
    try { await pollAllPrices(); } catch (err) {
      console.error('[collector] poll error:', err.message);
    }
  }, 1000);

  setInterval(async () => {
    for (const cfg of MARKETS_CONFIG) {
      const state = marketState.get(cfg.key);
      if (!state?.active) await refreshMarket(cfg);
    }
  }, 30000);
}

function getStatus() {
  const result = {};
  for (const cfg of MARKETS_CONFIG) {
    const state = marketState.get(cfg.key);
    if (!state) { result[cfg.key] = { active: false }; continue; }
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
