-- Recipes for the PolyLedger database.
-- Run any of these with:  polyledger query "<sql>"
-- or open the file directly:  duckdb data/polyledger.duckdb

-- ---------------------------------------------------------------------------
-- Daily volume across the whole exchange
-- ---------------------------------------------------------------------------
SELECT
    date_trunc('day', block_time) AS day,
    count(*)                      AS fills,
    sum(usd_size)                 AS volume_usd,
    count(DISTINCT token_id)      AS active_tokens
FROM trades
WHERE block_time IS NOT NULL
GROUP BY 1
ORDER BY 1 DESC;

-- ---------------------------------------------------------------------------
-- Busiest markets by notional
-- ---------------------------------------------------------------------------
SELECT
    question,
    market_slug,
    sum(usd_size)          AS volume_usd,
    count(*)               AS fills,
    min(block_time)        AS first_trade,
    max(block_time)        AS last_trade
FROM trades
WHERE question IS NOT NULL
GROUP BY 1, 2
ORDER BY volume_usd DESC
LIMIT 25;

-- ---------------------------------------------------------------------------
-- Hourly OHLC for one outcome token
-- ---------------------------------------------------------------------------
SELECT
    date_trunc('hour', block_time)      AS bucket,
    first(price ORDER BY block_number)  AS open,
    max(price)                          AS high,
    min(price)                          AS low,
    last(price ORDER BY block_number)   AS close,
    sum(shares)                         AS volume_shares,
    sum(usd_size)                       AS volume_usd
FROM trades
WHERE token_id = '<TOKEN_ID>' AND price IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- ---------------------------------------------------------------------------
-- Volume-weighted average price per outcome
-- ---------------------------------------------------------------------------
SELECT
    question,
    outcome,
    sum(price * shares) / nullif(sum(shares), 0) AS vwap,
    sum(usd_size)                               AS volume_usd
FROM trades
WHERE price IS NOT NULL
GROUP BY 1, 2
ORDER BY volume_usd DESC
LIMIT 25;

-- ---------------------------------------------------------------------------
-- One address's activity, netted per outcome
--   Positive net_shares means the address ended up long that outcome.
-- ---------------------------------------------------------------------------
WITH sides AS (
    SELECT maker AS address,
           CASE WHEN maker_side = 'BUY' THEN shares ELSE -shares END AS signed_shares,
           CASE WHEN maker_side = 'BUY' THEN -usd_size ELSE usd_size END AS signed_usd,
           question, outcome, token_id
    FROM trades
    UNION ALL
    SELECT taker,
           CASE WHEN taker_side = 'BUY' THEN shares ELSE -shares END,
           CASE WHEN taker_side = 'BUY' THEN -usd_size ELSE usd_size END,
           question, outcome, token_id
    FROM trades
)
SELECT question, outcome,
       sum(signed_shares) AS net_shares,
       sum(signed_usd)    AS net_usd
FROM sides
WHERE address = lower('<ADDRESS>')
GROUP BY 1, 2
ORDER BY abs(sum(signed_usd)) DESC;

-- ---------------------------------------------------------------------------
-- Most active traders by notional touched
-- ---------------------------------------------------------------------------
SELECT address, count(*) AS fills, sum(usd_size) AS volume_usd
FROM (
    SELECT maker AS address, usd_size FROM trades
    UNION ALL
    SELECT taker, usd_size FROM trades
)
GROUP BY 1
ORDER BY volume_usd DESC
LIMIT 50;

-- ---------------------------------------------------------------------------
-- Data health: what still has no market metadata attached?
-- ---------------------------------------------------------------------------
SELECT token_id, count(*) AS fills, sum(usd_size) AS volume_usd
FROM trades
WHERE unmatched_market
GROUP BY 1
ORDER BY fills DESC
LIMIT 20;

-- ---------------------------------------------------------------------------
-- Split by contract, useful when indexing with --contracts all
-- ---------------------------------------------------------------------------
SELECT exchange_name, exchange_version,
       count(*) AS fills, sum(usd_size) AS volume_usd,
       min(block_number) AS first_block, max(block_number) AS last_block
FROM trades
GROUP BY 1, 2
ORDER BY fills DESC;
