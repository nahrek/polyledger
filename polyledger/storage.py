"""DuckDB storage.

Why DuckDB instead of the CSV files the original pipeline used:

* appending to a multi-gigabyte CSV gets slower forever, and reading one means
  parsing every byte;
* joining fills to markets becomes a real indexed SQL join instead of a
  row-by-row pandas merge held entirely in RAM;
* a `PRIMARY KEY (transaction_hash, log_index)` plus `ON CONFLICT DO NOTHING`
  makes every write idempotent, so a crash mid-batch can't duplicate rows;
* the checkpoint is updated inside the same transaction as the rows it covers,
  so "written" and "recorded as written" can never disagree.

It is still just one file on disk. No server, no daemon.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Self

import duckdb

from .config import UNIT, Settings
from .decode import exchange_label
from .models import Market, OrderFill, Token

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS markets (
    condition_id      VARCHAR PRIMARY KEY,
    question_id       VARCHAR,
    question          VARCHAR,
    description       VARCHAR,
    market_slug       VARCHAR,
    end_date_iso      VARCHAR,
    game_start_time   VARCHAR,
    active            BOOLEAN,
    closed            BOOLEAN,
    archived          BOOLEAN,
    accepting_orders  BOOLEAN,
    neg_risk          BOOLEAN,
    enable_order_book BOOLEAN,
    minimum_tick_size DOUBLE,
    minimum_order_size DOUBLE,
    maker_base_fee    DOUBLE,
    taker_base_fee    DOUBLE,
    icon              VARCHAR,
    tags              VARCHAR[],
    source            VARCHAR,
    updated_at        TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tokens (
    token_id      VARCHAR PRIMARY KEY,
    condition_id  VARCHAR,
    outcome       VARCHAR,
    outcome_index INTEGER,
    price         DOUBLE,
    winner        BOOLEAN,
    updated_at    TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_fills (
    transaction_hash    VARCHAR,
    log_index           INTEGER,
    block_number        BIGINT,
    block_timestamp     BIGINT,
    exchange            VARCHAR,
    exchange_name       VARCHAR,
    exchange_version    VARCHAR,
    order_hash          VARCHAR,
    maker               VARCHAR,
    taker               VARCHAR,
    maker_side          TINYINT,
    token_id            VARCHAR,
    maker_amount_filled HUGEINT,
    taker_amount_filled HUGEINT,
    fee                 HUGEINT,
    PRIMARY KEY (transaction_hash, log_index)
);

CREATE TABLE IF NOT EXISTS checkpoints (
    stream     VARCHAR PRIMARY KEY,
    last_block BIGINT NOT NULL,
    rows_seen  BIGINT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT now()
);
"""

FILL_COLUMNS = (
    "transaction_hash", "log_index", "block_number", "block_timestamp",
    "exchange", "exchange_name", "exchange_version", "order_hash",
    "maker", "taker", "maker_side", "token_id",
    "maker_amount_filled", "taker_amount_filled", "fee",
)

MARKET_COLUMNS = (
    "condition_id", "question_id", "question", "description", "market_slug",
    "end_date_iso", "game_start_time", "active", "closed", "archived",
    "accepting_orders", "neg_risk", "enable_order_book", "minimum_tick_size",
    "minimum_order_size", "maker_base_fee", "taker_base_fee", "icon", "tags",
    "source",
)

TOKEN_COLUMNS = (
    "token_id", "condition_id", "outcome", "outcome_index", "price", "winner",
)

# The analytical view. Amounts are 6-decimal base units on chain; which leg is
# collateral and which is shares depends on the maker's side.
TRADES_SQL = f"""
SELECT
    f.transaction_hash,
    f.log_index,
    f.block_number,
    f.block_timestamp,
    CASE WHEN f.block_timestamp IS NULL THEN NULL
         ELSE make_timestamp(f.block_timestamp * 1000000) END AS block_time,
    f.exchange_name,
    f.exchange_version,
    f.order_hash,
    f.maker,
    f.taker,
    f.token_id,
    t.condition_id,
    t.outcome,
    t.outcome_index,
    m.question,
    m.market_slug,
    m.neg_risk,
    CASE WHEN f.maker_side = 0 THEN 'BUY' ELSE 'SELL' END   AS maker_side,
    CASE WHEN f.maker_side = 0 THEN 'SELL' ELSE 'BUY' END   AS taker_side,
    CASE WHEN f.maker_side = 0 THEN f.taker_amount_filled
         ELSE f.maker_amount_filled END / {UNIT}.0          AS shares,
    CASE WHEN f.maker_side = 0 THEN f.maker_amount_filled
         ELSE f.taker_amount_filled END / {UNIT}.0          AS usd_size,
    CASE
        WHEN (CASE WHEN f.maker_side = 0 THEN f.taker_amount_filled
                   ELSE f.maker_amount_filled END) = 0 THEN NULL
        ELSE (CASE WHEN f.maker_side = 0 THEN f.maker_amount_filled
                   ELSE f.taker_amount_filled END)::DOUBLE
             / (CASE WHEN f.maker_side = 0 THEN f.taker_amount_filled
                     ELSE f.maker_amount_filled END)::DOUBLE
    END                                                     AS price,
    f.fee / {UNIT}.0                                        AS fee,
    t.token_id IS NULL                                      AS unmatched_market
FROM order_fills f
LEFT JOIN tokens  t ON t.token_id = f.token_id
LEFT JOIN markets m ON m.condition_id = t.condition_id
"""


class Store:
    def __init__(self, settings: Settings | None = None, path: Path | None = None):
        self.settings = settings or Settings()
        self.path = Path(path or self.settings.db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.path))
        self.con.execute(SCHEMA)
        self.con.execute(f"CREATE OR REPLACE VIEW trades AS {TRADES_SQL}")

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- checkpoints -------------------------------------------------------

    def get_checkpoint(self, stream: str) -> int | None:
        row = self.con.execute(
            "SELECT last_block FROM checkpoints WHERE stream = ?", [stream]
        ).fetchone()
        return int(row[0]) if row else None

    def set_checkpoint(self, stream: str, last_block: int, rows_seen: int = 0) -> None:
        self.con.execute(
            """
            INSERT INTO checkpoints (stream, last_block, rows_seen, updated_at)
            VALUES (?, ?, ?, now())
            ON CONFLICT (stream) DO UPDATE SET
                last_block = excluded.last_block,
                rows_seen  = checkpoints.rows_seen + excluded.rows_seen,
                updated_at = now()
            """,
            [stream, int(last_block), int(rows_seen)],
        )

    # -- writes ------------------------------------------------------------

    def _upsert(
        self,
        table: str,
        columns: Sequence[str],
        rows: list[tuple],
        key: Sequence[str],
        *,
        replace: bool,
    ) -> int:
        """Load rows via a temp table, de-duplicate on `key`, then merge."""
        if not rows:
            return 0
        col_list = ", ".join(columns)
        placeholders = ", ".join("?" for _ in columns)
        tmp = f"tmp_{table}"
        self.con.execute(f"DROP TABLE IF EXISTS {tmp}")
        self.con.execute(f"CREATE TEMP TABLE {tmp} AS SELECT {col_list} FROM {table} LIMIT 0")
        self.con.executemany(f"INSERT INTO {tmp} VALUES ({placeholders})", rows)

        if replace:
            updates = ", ".join(
                f"{c} = excluded.{c}" for c in columns if c not in key
            )
            conflict = (
                f"ON CONFLICT ({', '.join(key)}) DO UPDATE SET "
                f"{updates}, updated_at = now()"
            )
        else:
            conflict = f"ON CONFLICT ({', '.join(key)}) DO NOTHING"

        before = self.con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        self.con.execute(
            f"""
            INSERT INTO {table} ({col_list})
            SELECT {col_list} FROM (
                SELECT DISTINCT ON ({', '.join(key)}) * FROM {tmp}
            )
            {conflict}
            """
        )
        after = self.con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        self.con.execute(f"DROP TABLE IF EXISTS {tmp}")
        return int(after - before)

    def upsert_markets(self, markets: Iterable[Market]) -> int:
        rows = [
            tuple(getattr(m, c) for c in MARKET_COLUMNS) for m in markets
        ]
        return self._upsert("markets", MARKET_COLUMNS, rows,
                            key=["condition_id"], replace=True)

    def upsert_tokens(self, tokens: Iterable[Token]) -> int:
        rows = [tuple(getattr(t, c) for c in TOKEN_COLUMNS) for t in tokens]
        return self._upsert("tokens", TOKEN_COLUMNS, rows,
                            key=["token_id"], replace=True)

    def insert_fills(self, fills: Iterable[OrderFill]) -> int:
        rows = []
        for f in fills:
            rows.append(
                (
                    f.transaction_hash, f.log_index, f.block_number,
                    f.block_timestamp, f.exchange, exchange_label(f.exchange),
                    f.exchange_version, f.order_hash, f.maker, f.taker,
                    f.maker_side, f.token_id, f.maker_amount_filled,
                    f.taker_amount_filled, f.fee,
                )
            )
        return self._upsert("order_fills", FILL_COLUMNS, rows,
                            key=["transaction_hash", "log_index"], replace=False)

    def commit_batch(
        self, stream: str, fills: list[OrderFill], next_block: int
    ) -> int:
        """Write a batch of fills and advance the cursor atomically."""
        self.con.execute("BEGIN TRANSACTION")
        try:
            inserted = self.insert_fills(fills)
            self.set_checkpoint(stream, next_block, rows_seen=inserted)
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        return inserted

    # -- reads -------------------------------------------------------------

    def unmatched_token_ids(self, limit: int | None = None) -> list[str]:
        """Token ids seen on chain that no known market accounts for."""
        sql = """
            SELECT DISTINCT f.token_id
            FROM order_fills f
            LEFT JOIN tokens t ON t.token_id = f.token_id
            WHERE t.token_id IS NULL
        """
        if limit:
            sql += f" LIMIT {int(limit)}"
        return [r[0] for r in self.con.execute(sql).fetchall()]

    def stats(self) -> dict[str, object]:
        def scalar(sql: str) -> object:
            row = self.con.execute(sql).fetchone()
            return row[0] if row else None

        return {
            "markets": scalar("SELECT count(*) FROM markets"),
            "tokens": scalar("SELECT count(*) FROM tokens"),
            "fills": scalar("SELECT count(*) FROM order_fills"),
            "unmatched_fills": scalar(
                "SELECT count(*) FROM order_fills f "
                "LEFT JOIN tokens t ON t.token_id = f.token_id "
                "WHERE t.token_id IS NULL"
            ),
            "first_block": scalar("SELECT min(block_number) FROM order_fills"),
            "last_block": scalar("SELECT max(block_number) FROM order_fills"),
            "first_time": scalar(
                "SELECT make_timestamp(min(block_timestamp) * 1000000) FROM order_fills"
            ),
            "last_time": scalar(
                "SELECT make_timestamp(max(block_timestamp) * 1000000) FROM order_fills"
            ),
            "checkpoints": self.con.execute(
                "SELECT stream, last_block, rows_seen, updated_at FROM checkpoints"
            ).fetchall(),
        }

    def export_parquet(self, out_dir: Path | None = None) -> list[Path]:
        """Dump the whole database to Parquet for sharing or archival."""
        target = Path(out_dir or self.settings.export_dir)
        target.mkdir(parents=True, exist_ok=True)
        written: list[Path] = []
        for name in ("markets", "tokens", "order_fills", "trades"):
            path = target / f"{name}.parquet"
            self.con.execute(
                f"COPY (SELECT * FROM {name}) TO '{path}' "
                "(FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            written.append(path)
        return written
