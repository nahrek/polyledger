"""Stage 3 — materialise the joined `trades` table.

The `trades` view is always live and always correct, so this stage is purely a
performance choice: if you plan to run many analytical queries, paying the join
once and storing the result beats recomputing it on every scan.
"""

from __future__ import annotations

import logging

from ..storage import TRADES_SQL, Store

log = logging.getLogger(__name__)


def build_trades(store: Store, *, drop_unmatched: bool = False) -> dict[str, int]:
    where = "WHERE t.token_id IS NOT NULL" if drop_unmatched else ""
    store.con.execute("DROP TABLE IF EXISTS trades_mat")
    store.con.execute(f"CREATE TABLE trades_mat AS {TRADES_SQL} {where}")
    store.con.execute(
        "CREATE INDEX IF NOT EXISTS idx_trades_mat_token "
        "ON trades_mat (token_id)"
    )
    store.con.execute(
        "CREATE INDEX IF NOT EXISTS idx_trades_mat_time "
        "ON trades_mat (block_timestamp)"
    )
    rows = store.con.execute("SELECT count(*) FROM trades_mat").fetchone()[0]
    unmatched = store.con.execute(
        "SELECT count(*) FROM trades_mat WHERE unmatched_market"
    ).fetchone()[0]
    log.info("trades_mat built: %s rows (%s without market metadata)", rows, unmatched)
    return {"rows": int(rows), "unmatched": int(unmatched)}
