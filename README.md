# PolyLedger

A resumable indexer for Polymarket market metadata and on-chain trade data, backed by DuckDB.

PolyLedger pulls every market from the Polymarket CLOB API, streams every `OrderFilled` event from Polygon via [Envio HyperSync](https://envio.dev), and writes both into a single DuckDB file you can query with SQL immediately.

---

## Table of contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [CLI reference](#cli-reference)
- [Data model](#data-model)
- [Indexed contracts](#indexed-contracts)
- [Configuration](#configuration)
- [Design notes](#design-notes)
- [Development](#development)
- [Limitations](#limitations)
- [License](#license)

## Features

- **Resumable by construction.** Rows and the block cursor are committed in a single transaction, so an interrupted run resumes exactly where it stopped.
- **Idempotent writes.** `order_fills` is keyed on `(transaction_hash, log_index)` and inserted with `ON CONFLICT DO NOTHING`. Re-running a range is a no-op.
- **DuckDB storage.** Columnar compression, real types, indexes, and SQL joins, all in one file on disk. No server.
- **Both contract generations.** Decodes the V2 `OrderFilled` layout and the older V1 one, with separate checkpoints per stream.
- **Schema validation.** Every API response passes through Pydantic models, so an upstream format change fails loudly instead of writing nulls for hours.
- **Resilient networking.** Exponential backoff with jitter, `Retry-After` support, and a shared token-bucket rate limiter across all REST sources.
- **Gap recovery.** Token ids missing from the CLOB listing are flagged and backfilled from the Gamma API rather than silently dropped.
- **Parquet export.** One command dumps every table for use with pandas, polars, or Spark.

## Requirements

- Python 3.11 or newer
- A HyperSync API token (required since November 2025; the free tier is sufficient). Register at [envio.dev](https://envio.dev).

The CLOB and Gamma APIs are public and need no credentials.

## Installation


```bat
git clone https://github.com/nahrek/polyledger
cd polyledger
python -m venv .venv
.venv\Scripts\activate.bat
pip install -e .
```

Then configure the HyperSync token. Copy `.env.example` to `.env`, open it in any
editor, and set `HYPERSYNC_BEARER_TOKEN`. PolyLedger reads `.env` from the working
directory on startup, so there is no export step. Real environment variables still
take precedence if you prefer to set them that way.

## Quick start

Run the full pipeline:

```bash
polyledger sync
```

The first backfill is slow. On the free HyperSync tier it takes anywhere from a few hours to a couple of days depending on how much history you want. It is safe to interrupt at any point. Every subsequent run is incremental and finishes in seconds.

To verify the setup on a small slice first:

```bash
polyledger markets                    # metadata only, a few minutes
polyledger chain --max-blocks 200000  # roughly five days of Polygon history
polyledger stats
```

`stats` reports what is actually in the database:

```
  markets          14231
  tokens           28455
  fills            1204773
  unmatched_fills  392
  first_block      75014820
  last_block       75214820
  first_time       2026-05-14 08:11:23
  last_time        2026-05-19 03:44:02
  checkpoints:
    order_filled:v2: block 75214820, 1204773 rows, updated 2026-09-02 15:11:23
```

Then query it:

```bash
polyledger query "SELECT question, sum(usd_size) AS volume FROM trades GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
```

## CLI reference

| Command | Description |
| --- | --- |
| `polyledger markets [--backfill]` | Sync market metadata from the CLOB. `--backfill` also resolves unmatched token ids via Gamma. |
| `polyledger chain [options]` | Index `OrderFilled` logs from Polygon. |
| `polyledger trades [--drop-unmatched]` | Materialise the joined `trades_mat` table. |
| `polyledger sync [options]` | Run markets → chain → backfill → trades in order. |
| `polyledger stats` | Show row counts, block range, and checkpoints. |
| `polyledger export [--out DIR]` | Write every table to Parquet. |
| `polyledger query "SELECT ..."` | Run one SQL statement and print the result. |

`chain` options:

| Flag | Description |
| --- | --- |
| `--contracts v2\|v1\|all` | Which exchange generation to index. Default `v2`. |
| `--from-block N` | Override the start block. Ignored if a checkpoint exists. |
| `--to-block N` | Stop at this block. |
| `--max-blocks N` | Index at most this many blocks, then exit. |

Global flags: `--db PATH` to use a different database file, `-v` for verbose logging.

The `polyledger` executable is installed by `pip install -e .`. Without installing, `python -m polyledger` and `python -m polyledger.cli` are equivalent. The entry point is `main()` in `polyledger/cli.py`.

## Data model

### `trades`

The main analytical view, joining chain fills to market metadata. Always live and always current. Run `polyledger trades` to materialise it as `trades_mat` if you plan to issue many queries against it.

| Column | Description |
| --- | --- |
| `block_time`, `block_number` | Block timestamp (UTC) and height |
| `question`, `market_slug`, `outcome` | Human-readable context |
| `token_id`, `condition_id` | Outcome and market identifiers |
| `maker`, `taker` | Counterparty addresses |
| `maker_side`, `taker_side` | `BUY` / `SELL`, both sides stated explicitly |
| `price` | Execution price, 0 to 1 |
| `shares` | Outcome share quantity |
| `usd_size` | Notional in dollars |
| `fee` | Fee from the event |
| `exchange_name`, `exchange_version` | Which contract emitted the fill |
| `unmatched_market` | `true` when no market metadata was found |

`price`, `shares`, and `usd_size` are not stored on chain. They are derived from `makerAmountFilled` and `takerAmountFilled` according to the maker's side: when the maker buys, their amount is collateral and the taker's is shares; when the maker sells, it is the other way round. Both are base units with 6 decimals, matching pUSD.

Many datasets collapse the two sides into a single ambiguous `side` column. PolyLedger records both, so you never have to guess whose perspective a row is written from.

### Other tables

| Table | Contents |
| --- | --- |
| `markets` | One row per market (`condition_id`), with question, slug, `active`/`closed`/`neg_risk` flags, tick size, and fees |
| `tokens` | One row per outcome (`token_id`). This is the join key against fills |
| `order_fills` | Raw decoded logs with no interpretation applied. Build your own join from here if you disagree with the price math |
| `checkpoints` | One row per indexing stream |

Ready-made queries for daily volume, OHLC candles, VWAP, per-address positions, and top traders are in [`examples/queries.sql`](examples/queries.sql).

The database is a plain DuckDB file:

```python
import duckdb
con = duckdb.connect("data/polyledger.duckdb", read_only=True)
df = con.sql("SELECT * FROM trades WHERE token_id = '1001'").df()
```

Or export it with `polyledger export` and read the Parquet files from anywhere.

## Indexed contracts

V2 contracts, live since the April 2026 migration, are indexed by default:

| Contract | Address |
| --- | --- |
| CTF Exchange V2 | `0xE111180000d2663C0091e4f400237545B87B996B` |
| NegRisk CTF Exchange V2 | `0xe2222d279d744050d28e00520010520000310F59` |
| NegRisk CTF Exchange V2 (b) | `0xe2222d002000ba0053cef3375333610f64600036` |

Use `--contracts v1` for the frozen V1 contracts (`0x4bFb41d5…`, `0xC5d563A3…`), or `--contracts all` for both generations. Each stream keeps its own checkpoint, so switching modes will not corrupt a cursor built by earlier runs.

The two generations emit different event layouts, and the decoder handles both:

```
V2: OrderFilled(bytes32 orderHash, address maker, address taker, uint8 side,
                uint256 tokenId, uint256 makerAmountFilled,
                uint256 takerAmountFilled, uint256 fee,
                bytes32 builder, bytes32 metadata)

V1: OrderFilled(bytes32 orderHash, address maker, address taker,
                uint256 makerAssetId, uint256 takerAssetId,
                uint256 makerAmountFilled, uint256 takerAmountFilled, uint256 fee)
```

V2 states the side explicitly. V1 does not, so it has to be inferred: asset id `0` is collateral, and whichever leg is zero determines the maker's direction. V1 token-for-token fills between complementary outcomes are skipped, since they have no collateral leg and therefore no meaningful price.

You do not need to specify a start block. With no checkpoint and no `POLYLEDGER_FROM_BLOCK`, PolyLedger locates the first block containing a matching log. HyperSync skips empty ranges server-side, so this costs a handful of requests rather than a full chain scan.

## Configuration

All settings are read from environment variables or from a `.env` file in the working directory, which is loaded automatically at startup. Environment variables take precedence.

| Variable | Default | Description |
| --- | --- | --- |
| `HYPERSYNC_BEARER_TOKEN` | - | Envio API token. Required |
| `POLYLEDGER_DB` | `data/polyledger.duckdb` | Database file path |
| `POLYLEDGER_EXPORT_DIR` | `data/parquet` | Parquet export directory |
| `POLYLEDGER_CONTRACTS` | `v2` | `v2`, `v1`, or `all` |
| `POLYLEDGER_FROM_BLOCK` | `0` | `0` means auto-detect |
| `POLYLEDGER_REORG_BUFFER` | `64` | Blocks to stay behind the chain head |
| `POLYLEDGER_FLUSH_EVERY` | `50000` | Fills buffered before a checkpointed flush |
| `POLYLEDGER_HTTP_RPS` | `8` | Request rate limit for CLOB and Gamma |
| `POLYLEDGER_HTTP_CONCURRENCY` | `8` | Parallel HTTP requests |
| `POLYLEDGER_HTTP_RETRIES` | `6` | Attempts per request |
| `POLYLEDGER_HTTP_TIMEOUT` | `30` | Request timeout in seconds |

`REORG_BUFFER` guards against chain reorganisations by stopping 64 blocks short of the head, so a fill from a block that later gets orphaned is never written.

## Design notes

**DuckDB rather than CSV.** Appending to a multi-gigabyte CSV degrades without bound, and any read means parsing every byte. DuckDB gives columnar compression, real types, indexes, and a proper SQL join instead of a `pandas.merge` that has to hold both sides in memory. It remains a single file with no server or daemon.

**Atomic checkpoints.** Rows and the new cursor value are written in one transaction. There is no state where data landed but the cursor did not, or the reverse.

**Idempotent inserts.** Duplicates within a single batch are collapsed before the write, and duplicates against existing rows are dropped by the primary key. A crash mid-batch cannot produce double rows on restart.

**Loud failures on schema drift.** Polymarket changed its response format in April 2026 and broke every consumer built on the old shape. Pydantic validation means the next change surfaces as an error on the first affected row.

**Visible gaps.** Some token ids from older fills are absent from the CLOB listing. Those rows are marked `unmatched_market = true`, counted in `stats`, and recovered from Gamma with `--backfill`. Nothing is discarded; the trade stays in the database, just without human-readable context.

## Development

```bash
pip install -e ".[dev]"
pytest
```

41 tests, none requiring network access. They cover decoding for both event versions against fixtures, insert idempotency and transaction rollback, price and side arithmetic, CLOB pagination including the stuck-cursor guard, Gamma batching and fallback, the rate limiter, and the backoff schedule. One test recomputes `topic0` from the textual event signatures and compares it against the hard-coded constants, so a typo in either cannot go unnoticed.

## Limitations

- **Polymarket only.** Kalshi does not settle on chain and needs a different collector. The staged architecture allows for one, but it is not written.
- **Filled trades only.** Order book state, quotes, and cancellations never reach the chain. Capturing those requires the CLOB WebSocket feed in real time.
- **Reorgs are avoided, not reconciled.** The indexer stays behind the head rather than rolling back written blocks. This is sufficient for historical analysis and not sufficient for live trading.
- **Fees come from the event's `fee` field.** Separate `FeeCharged` events are not indexed.

## License

MIT
