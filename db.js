const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  max: 10,
  idleTimeoutMillis: 30000,
});

async function migrate() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS ticks (
        id                BIGSERIAL PRIMARY KEY,
        timestamp         TIMESTAMPTZ NOT NULL,
        unix_ms           BIGINT NOT NULL,
        market            VARCHAR(20) NOT NULL,
        cycle_id          INT,
        cycle_start       TIMESTAMPTZ,
        cycle_end         TIMESTAMPTZ,
        price_up          NUMERIC(6,4),
        price_down        NUMERIC(6,4),
        spread            NUMERIC(6,4),
        seconds_remaining INT
      );

      CREATE INDEX IF NOT EXISTS idx_ticks_market_ts ON ticks(market, timestamp DESC);

      CREATE TABLE IF NOT EXISTS cycles (
        id               SERIAL PRIMARY KEY,
        cycle_id         INT,
        market           VARCHAR(20) NOT NULL,
        cycle_start      TIMESTAMPTZ,
        cycle_end        TIMESTAMPTZ,
        duration_min     INT,
        outcome          VARCHAR(10),
        final_price_up   NUMERIC(6,4),
        final_price_down NUMERIC(6,4),
        opening_price_up NUMERIC(6,4),
        total_ticks      INT,
        resolved_at      TIMESTAMPTZ
      );

      CREATE INDEX IF NOT EXISTS idx_cycles_market ON cycles(market, cycle_start DESC);
    `);
    console.log('[db] Schema migration complete');
  } finally {
    client.release();
  }
}

async function insertTick(tick) {
  const sql = `
    INSERT INTO ticks (timestamp, unix_ms, market, cycle_id, cycle_start, cycle_end, price_up, price_down, spread, seconds_remaining)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
  `;
  await pool.query(sql, [
    tick.timestamp, tick.unix_ms, tick.market, tick.cycle_id,
    tick.cycle_start, tick.cycle_end, tick.price_up, tick.price_down,
    tick.spread, tick.seconds_remaining,
  ]);
}

async function insertCycle(cycle) {
  const sql = `
    INSERT INTO cycles (cycle_id, market, cycle_start, cycle_end, duration_min, outcome, final_price_up, final_price_down, opening_price_up, total_ticks, resolved_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
  `;
  await pool.query(sql, [
    cycle.cycle_id, cycle.market, cycle.cycle_start, cycle.cycle_end,
    cycle.duration_min, cycle.outcome, cycle.final_price_up, cycle.final_price_down,
    cycle.opening_price_up, cycle.total_ticks, cycle.resolved_at,
  ]);
}

async function getTicks({ market, limit = 3600, before }) {
  let sql = 'SELECT * FROM ticks WHERE market = $1';
  const params = [market];
  if (before) {
    sql += ' AND timestamp < $2';
    params.push(before);
  }
  sql += ' ORDER BY timestamp DESC LIMIT $' + (params.length + 1);
  params.push(limit);
  const { rows } = await pool.query(sql, params);
  return rows.reverse();
}

async function getCycles({ market, limit = 200 } = {}) {
  let sql = 'SELECT * FROM cycles';
  const params = [];
  if (market) {
    sql += ' WHERE market = $1';
    params.push(market);
  }
  sql += ' ORDER BY cycle_start DESC LIMIT $' + (params.length + 1);
  params.push(limit);
  const { rows } = await pool.query(sql, params);
  return rows;
}

async function getAllTicks() {
  const { rows } = await pool.query('SELECT * FROM ticks ORDER BY timestamp ASC');
  return rows;
}

async function getAllCycles() {
  const { rows } = await pool.query('SELECT * FROM cycles ORDER BY cycle_start ASC');
  return rows;
}

async function getTickCount(market, cycleStart, cycleEnd) {
  const { rows } = await pool.query(
    'SELECT COUNT(*) as cnt FROM ticks WHERE market = $1 AND timestamp >= $2 AND timestamp <= $3',
    [market, cycleStart, cycleEnd]
  );
  return parseInt(rows[0].cnt, 10);
}

async function getFirstTick(market, cycleStart) {
  const { rows } = await pool.query(
    'SELECT price_up FROM ticks WHERE market = $1 AND cycle_start = $2 ORDER BY timestamp ASC LIMIT 1',
    [market, cycleStart]
  );
  return rows[0] || null;
}

async function purgeAll() {
  await pool.query('TRUNCATE ticks RESTART IDENTITY');
  await pool.query('TRUNCATE cycles RESTART IDENTITY');
  console.log('[db] All old data purged');
}

module.exports = { pool, migrate, insertTick, insertCycle, getTicks, getCycles, getAllTicks, getAllCycles, getTickCount, getFirstTick, purgeAll };
