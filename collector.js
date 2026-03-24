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
const nextMarket = new Map();
let broadcast = () => {};

function setBroadcast(fn) { broadcast = fn; }

function get15mSlug(coin, offsetSec = 0) {
  const ts = Math.floor((Date.now() / 1000 + offsetSec) / 900) * 900;
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
  let hour = et.hour;
  const ampm = hour >= 12 ? 'pm' : 'am';
  hour = hour % 12 || 12;
  const coinName = coin === 'btc' ? 'bitcoin' : 'ethereum';
  return `${coinName}-up-or-down-${months[et.month]}-${et.day}-${et.year}-${hour}${ampm}-et`;
}

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.json();
}

function parseEvent(event, cfg) {
  if (!event?.markets?.length) return null;
  const mkt = event.markets[0];
  const tokenIds = JSON.parse(mkt.clobTokenIds);
  const endDate = new Date(mkt.endDate);
  const startDate = mkt.eventStartTime
    ? new Date(mkt.eventStartTime)
    : new Date(endDate.getTime() - cfg.durationMin * 60000);
  const cycleId = cfg.type === '15m'
    ? Math.floor(startDate.getTime() / 1000 / 900)
    : Math.floor(startDate.getTime() / 1000 / 3600);
  return {
    slug: event.slug,
    gammaMarketId: mkt.id,
    upTokenId: tokenIds[0],
    downTokenId: tokenIds[1],
    cycleStart: startDate.toISOString(),
    cycleEnd: endDate.toISOString(),
    cycleId,
    durationMin: cfg.durationMin,
  };
}

async function fetchEvent(cfg, offsetSec = 0) {
  if (cfg.type === '15m') {
    const events = await fetchJSON(`${GAMMA}/events?slug=${get15mSlug(cfg.coin, offsetSec)}`);
    return events?.[0];
  } else {
    const offsetMs = offsetSec * 1000;
    let events = await fetchJSON(`${GAMMA}/events?slug=${get1hSlug(cfg.coin, offsetMs)}`);
    if (!events?.length) events = await fetchJSON(`${GAMMA}/events?slug=${get1hSlug(cfg.coin, offsetMs - 3600000)}`);
    return events?.[0];
  }
}

async function refreshMarket(cfg) {
  const state = marketState.get(cfg.key) || {};
  try {
    const event = await fetchEvent(cfg);
    const parsed = parseEvent(event, cfg);
    if (!parsed) {
      state.active = false;
      marketState.set(cfg.key, state);
      return;
    }
    Object.assign(state, {
      key: cfg.key, active: true, ...parsed,
      closed: false, priceUp: null, priceDown: null,
      resolving: false, openingPriceUp: state.openingPriceUp ?? null,
    });
    marketState.set(cfg.key, state);
    console.log(`[collector] Refreshed ${cfg.key}: #${parsed.cycleId}, ends ${parsed.cycleEnd}`);
  } catch (err) {
    console.error(`[collector] refreshMarket ${cfg.key}:`, err.message);
    state.active = false;
    marketState.set(cfg.key, state);
  }
}

async function prefetchNext(cfg) {
  try {
    const offsetSec = cfg.type === '15m' ? 900 : 3600;
    const event = await fetchEvent(cfg, offsetSec);
    const parsed = parseEvent(event, cfg);
    if (parsed) {
      nextMarket.set(cfg.key, parsed);
    }
  } catch (e) {}
}

function switchToNext(cfg) {
  const next = nextMarket.get(cfg.key);
  if (!next) return false;
  const state = marketState.get(cfg.key) || {};
  Object.assign(state, {
    key: cfg.key, active: true, ...next,
    closed: false, priceUp: null, priceDown: null,
    resolving: false, openingPriceUp: null,
  });
  marketState.set(cfg.key, state);
  nextMarket.delete(cfg.key);
  console.log(`[collector] Switched ${cfg.key} → #${next.cycleId}, ends ${next.cycleEnd}`);
  return true;
}

async function fetchClobPrices(tokenPairs) {
  const payload = [];
  for (const { upId, downId } of tokenPairs) {
    payload.push({ token_id: upId, side: 'BUY' });
    payload.push({ token_id: downId, side: 'BUY' });
  }
  const res = await fetch(`${CLOB}/prices`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(`CLOB /prices HTTP ${res.status}`);
  return res.json();
}

async function pollAllPrices() {
  const pairs = [];
  for (const cfg of MARKETS_CONFIG) {
    const state = marketState.get(cfg.key);
    if (!state?.active || !state.upTokenId) continue;
    pairs.push({ key: cfg.key, upId: state.upTokenId, downId: state.downTokenId });
  }
  if (!pairs.length) return;

  let clobData;
  try {
    clobData = await fetchClobPrices(pairs);
  } catch (err) {
    console.error('[collector] CLOB error:', err.message);
    return;
  }

  const now = new Date();
  const unixMs = now.getTime();

  for (const cfg of MARKETS_CONFIG) {
    const state = marketState.get(cfg.key);
    if (!state?.active || !state.upTokenId) continue;

    const endMs = new Date(state.cycleEnd).getTime();
    const secondsRemaining = Math.max(0, Math.floor((endMs - unixMs) / 1000));

    if (secondsRemaining <= 0) {
      const oldState = { ...state };
      const switched = switchToNext(cfg);
      if (switched) {
        resolveCycleBackground(cfg, oldState);
        continue;
      } else if (!state.resolving) {
        resolveCycleAndRefresh(cfg, state);
        continue;
      }
    }

    if (secondsRemaining > 0 && secondsRemaining <= 60 && !nextMarket.has(cfg.key)) {
      prefetchNext(cfg);
    }

    const rawUp = clobData[state.upTokenId]?.BUY;
    const rawDown = clobData[state.downTokenId]?.BUY;
    if (rawUp == null && rawDown == null) continue;

    const priceUp = parseFloat(rawUp) || 0;
    const priceDown = parseFloat(rawDown) || 0;
    const spread = Math.round((1 - priceUp - priceDown) * 10000) / 10000;

    state.priceUp = priceUp;
    state.priceDown = priceDown;
    if (state.openingPriceUp === null) state.openingPriceUp = priceUp;

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

    try { await db.insertTick(tick); } catch (err) {
      console.error(`[collector] insertTick ${cfg.key}:`, err.message);
    }
    broadcast({ type: 'tick', data: tick });
  }
}

function resolveCycleBackground(cfg, oldState) {
  (async () => {
    console.log(`[collector] Background resolve #${oldState.cycleId} ${cfg.key}...`);
    let outcome = null;
    for (let i = 0; i < 30 && !outcome; i++) {
      try {
        const data = await fetchJSON(`${GAMMA}/markets/${oldState.gammaMarketId}`);
        if (data.closed) {
          const prices = JSON.parse(data.outcomePrices);
          outcome = parseFloat(prices[0]) >= 0.5 ? 'UP' : 'DOWN';
          oldState.priceUp = parseFloat(prices[0]);
          oldState.priceDown = parseFloat(prices[1]);
        }
      } catch (e) {}
      if (!outcome) await new Promise(r => setTimeout(r, 5000));
    }
    if (!outcome) outcome = (oldState.priceUp || 0) >= 0.5 ? 'UP' : 'DOWN';
    await saveCycle(cfg, oldState, outcome);
  })();
}

async function resolveCycleAndRefresh(cfg, state) {
  if (state.resolving) return;
  state.resolving = true;
  console.log(`[collector] Resolving #${state.cycleId} ${cfg.key}...`);

  let outcome = null;
  for (let i = 0; i < 30 && !outcome; i++) {
    try {
      const data = await fetchJSON(`${GAMMA}/markets/${state.gammaMarketId}`);
      if (data.closed) {
        const prices = JSON.parse(data.outcomePrices);
        outcome = parseFloat(prices[0]) >= 0.5 ? 'UP' : 'DOWN';
        state.priceUp = parseFloat(prices[0]);
        state.priceDown = parseFloat(prices[1]);
      }
    } catch (e) {}
    if (!outcome) await new Promise(r => setTimeout(r, 5000));
  }
  if (!outcome) outcome = (state.priceUp || 0) >= 0.5 ? 'UP' : 'DOWN';

  await saveCycle(cfg, state, outcome);
  state.resolving = false;
  state.openingPriceUp = null;
  await refreshMarket(cfg);
}

async function saveCycle(cfg, state, outcome) {
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
    console.log(`[collector] Cycle #${state.cycleId} ${cfg.key}: ${outcome} (${tickCount} ticks)`);
  } catch (err) {
    console.error(`[collector] insertCycle ${cfg.key}:`, err.message);
  }
}

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
  }, 15000);
}

function getStatus() {
  const result = {};
  for (const cfg of MARKETS_CONFIG) {
    const state = marketState.get(cfg.key);
    if (!state) { result[cfg.key] = { active: false }; continue; }
    const secondsRemaining = Math.max(0, Math.floor((new Date(state.cycleEnd).getTime() - Date.now()) / 1000));
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
