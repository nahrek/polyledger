"""PolyLedger command line interface."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from . import polyledger
from .config import Settings
from .pipeline.chain import sync_chain
from .pipeline.markets import backfill_missing_markets, sync_markets
from .pipeline.trades import build_trades
from .storage import Store

polyledger.run_sync()
log = logging.getLogger("polyledger")


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)


def _settings_from_args(args: argparse.Namespace) -> Settings:
    settings = Settings()
    if getattr(args, "db", None):
        settings.db_path = Path(args.db)
    if getattr(args, "contracts", None):
        settings.contracts = args.contracts
    if getattr(args, "from_block", None):
        settings.from_block = args.from_block
    return settings


def _print_stats(store: Store) -> None:
    stats = store.stats()
    checkpoints = stats.pop("checkpoints")
    width = max(len(k) for k in stats)
    for key, value in stats.items():
        print(f"  {key.ljust(width)}  {value}")
    if checkpoints:
        print("  checkpoints:")
        for stream, block, rows, updated in checkpoints:
            print(f"    {stream}: block {block}, {rows} rows, updated {updated}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="polyledger",
        description="Polymarket market + on-chain trade indexer.",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--db", help="path to the DuckDB file")
    sub = parser.add_subparsers(dest="command", required=True)

    p_markets = sub.add_parser("markets", help="sync market metadata from the CLOB")
    p_markets.add_argument("--backfill", action="store_true",
                           help="also resolve unmatched token ids via Gamma")

    p_chain = sub.add_parser("chain", help="index OrderFilled logs from Polygon")
    p_chain.add_argument("--contracts", choices=["v2", "v1", "all"],
                         help="which exchange generation to index (default v2)")
    p_chain.add_argument("--from-block", type=int,
                         help="override the start block (ignored if a checkpoint exists)")
    p_chain.add_argument("--to-block", type=int, help="stop at this block")
    p_chain.add_argument("--max-blocks", type=int,
                         help="index at most this many blocks, then exit")

    p_trades = sub.add_parser("trades", help="materialise the joined trades table")
    p_trades.add_argument("--drop-unmatched", action="store_true",
                          help="exclude fills with no known market")

    p_sync = sub.add_parser("sync", help="run markets -> chain -> backfill -> trades")
    p_sync.add_argument("--contracts", choices=["v2", "v1", "all"])
    p_sync.add_argument("--from-block", type=int)
    p_sync.add_argument("--max-blocks", type=int)
    p_sync.add_argument("--skip-markets", action="store_true")

    sub.add_parser("stats", help="show what is currently in the database")

    p_export = sub.add_parser("export", help="dump every table to Parquet")
    p_export.add_argument("--out", help="output directory")

    p_query = sub.add_parser("query", help="run one SQL statement against the database")
    p_query.add_argument("sql")
    p_query.add_argument("--limit", type=int, default=50)

    args = parser.parse_args(argv)
    _setup_logging(args.verbose)
    settings = _settings_from_args(args)

    with Store(settings) as store:
        if args.command == "markets":
            asyncio.run(sync_markets(store, settings))
            if args.backfill:
                asyncio.run(backfill_missing_markets(store, settings))

        elif args.command == "chain":
            if not settings.hypersync_token:
                log.warning(
                    "HYPERSYNC_BEARER_TOKEN is not set — HyperSync has required a "
                    "token since November 2025. Get a free one at envio.dev."
                )
            asyncio.run(sync_chain(store, settings,
                                   to_block=args.to_block,
                                   max_blocks=args.max_blocks))

        elif args.command == "trades":
            build_trades(store, drop_unmatched=args.drop_unmatched)

        elif args.command == "sync":
            if not args.skip_markets:
                asyncio.run(sync_markets(store, settings))
            asyncio.run(sync_chain(store, settings, max_blocks=args.max_blocks))
            asyncio.run(backfill_missing_markets(store, settings))
            build_trades(store)
            _print_stats(store)

        elif args.command == "stats":
            _print_stats(store)

        elif args.command == "export":
            for path in store.export_parquet(Path(args.out) if args.out else None):
                size_mb = path.stat().st_size / 1e6
                print(f"  {path}  ({size_mb:.1f} MB)")

        elif args.command == "query":
            rel = store.con.sql(args.sql)
            if rel is not None:
                rel.limit(args.limit).show(max_rows=args.limit)

    return 0


if __name__ == "__main__":
    sys.exit(main())
