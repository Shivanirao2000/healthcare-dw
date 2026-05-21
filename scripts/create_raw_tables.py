"""
Create (or recreate) RAW schema tables in Snowflake.

Reads table definitions from ingestion/schema_definitions.py and executes them
in dependency order (RAW_PROVIDERS → RAW_PROCEDURES → RAW_CLAIMS).

Run this script once before the first ingestion run, and again whenever the
schema definition changes. Because all DDL uses CREATE TABLE IF NOT EXISTS, it
is safe to re-run against a populated database — existing tables and data are
left untouched unless you pass --drop-first.

Usage:
    # Create all tables (no-op if they already exist)
    python scripts/create_raw_tables.py

    # Preview the DDL without connecting to Snowflake
    python scripts/create_raw_tables.py --dry-run

    # Create only one table
    python scripts/create_raw_tables.py --table RAW_CLAIMS

    # Drop and recreate all tables (destructive — loses all data)
    python scripts/create_raw_tables.py --drop-first --force

    # Drop and recreate one table
    python scripts/create_raw_tables.py --table RAW_CLAIMS --drop-first --force
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv
from loguru import logger

# Allow running as a script from the project root without installing the package
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.cms_ingest import SnowflakeConfig
from ingestion.schema_definitions import ALL_TABLES, TableSchema, render

load_dotenv()


# ─────────────────────────────────────────────────────────────────────────────
# Argument parsing
# ─────────────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    valid_names = [t.name for t in ALL_TABLES]
    p = argparse.ArgumentParser(
        description="Create Snowflake RAW schema tables from ingestion/schema_definitions.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Known tables (in creation order): {', '.join(valid_names)}",
    )
    p.add_argument(
        "--table",
        metavar="TABLE_NAME",
        choices=valid_names,
        default=None,
        help="Create only this table instead of all tables",
    )
    p.add_argument(
        "--drop-first",
        action="store_true",
        dest="drop_first",
        help="DROP TABLE IF EXISTS before CREATE — destroys all existing data in the table",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Required alongside --drop-first to confirm the destructive operation",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="Print DDL statements to stdout without connecting to Snowflake",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        dest="log_level",
    )
    return p


# ─────────────────────────────────────────────────────────────────────────────
# DDL execution helpers
# ─────────────────────────────────────────────────────────────────────────────

def _print_ddl(tables: list[TableSchema], cfg: SnowflakeConfig) -> None:
    """Print all DDL to stdout — used by --dry-run."""
    separator = "-" * 78
    for table in tables:
        ddl = render(table.ddl, database=cfg.database, schema=cfg.schema)
        print(separator)
        print(f"-- {table.name}: {table.description}")
        print(separator)
        print(ddl + ";")
        print()


def _drop_table(
    cur: snowflake.connector.cursor.SnowflakeCursor,
    database: str,
    schema: str,
    table_name: str,
) -> None:
    qualified = f"{database}.{schema}.{table_name}"
    logger.warning(f"  dropping {qualified}")
    cur.execute(f"DROP TABLE IF EXISTS {qualified}")
    logger.info(f"  {qualified} dropped")


def _create_table(
    cur: snowflake.connector.cursor.SnowflakeCursor,
    table: TableSchema,
    database: str,
    schema: str,
) -> None:
    qualified = f"{database}.{schema}.{table.name}"
    ddl = render(table.ddl, database=database, schema=schema)

    logger.debug(f"  executing DDL:\n{ddl}")
    t0 = time.monotonic()
    cur.execute(ddl)

    # Snowflake returns the status string in the first row, e.g.
    # "Table RAW_PROVIDERS successfully created." or
    # "Table RAW_PROVIDERS already exists, statement succeeded."
    result = cur.fetchone()
    elapsed = time.monotonic() - t0
    status = result[0] if result else "no status returned"
    logger.info(f"  {qualified}: {status}  ({elapsed:.2f}s)")


def _verify_table(
    cur: snowflake.connector.cursor.SnowflakeCursor,
    database: str,
    schema: str,
    table_name: str,
) -> int:
    """Return the row count of a table; -1 if it does not exist."""
    try:
        cur.execute(
            f"SELECT COUNT(*) FROM {database}.{schema}.{table_name}"
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
    except snowflake.connector.errors.ProgrammingError:
        return -1


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)

    logger.remove()
    logger.add(sys.stderr, level=args.log_level, colorize=True)

    # Validate --drop-first safety gate
    if args.drop_first and not args.force and not args.dry_run:
        logger.error(
            "--drop-first is a destructive operation that deletes all table data. "
            "Pass --force to confirm, or --dry-run to preview the DDL first."
        )
        sys.exit(1)

    # Select which tables to process
    if args.table:
        tables = [t for t in ALL_TABLES if t.name == args.table]
    else:
        tables = list(ALL_TABLES)

    cfg = SnowflakeConfig.from_env()

    # ── Dry-run: print DDL and exit ──────────────────────────────────────────
    if args.dry_run:
        if args.drop_first:
            for table in tables:
                q = f"{cfg.database}.{cfg.schema}.{table.name}"
                print(f"DROP TABLE IF EXISTS {q};")
                print()
        _print_ddl(tables, cfg)
        logger.info("dry-run complete — no changes made to Snowflake")
        return

    # ── Live run ─────────────────────────────────────────────────────────────
    logger.info(
        f"connecting  account={cfg.account}  "
        f"db={cfg.database}.{cfg.schema}  "
        f"warehouse={cfg.warehouse}  role={cfg.role}"
    )
    conn = snowflake.connector.connect(
        account=cfg.account,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        schema=cfg.schema,
        warehouse=cfg.warehouse,
        role=cfg.role,
        login_timeout=60,
    )

    try:
        cur = conn.cursor()

        # When dropping, reverse order to respect logical dependencies:
        # RAW_CLAIMS before RAW_PROCEDURES before RAW_PROVIDERS
        if args.drop_first:
            for table in reversed(tables):
                _drop_table(cur, cfg.database, cfg.schema, table.name)

        for table in tables:
            logger.info(f"creating {table.name}  —  {table.description}")
            _create_table(cur, table, cfg.database, cfg.schema)

        # Post-creation summary
        logger.info("")
        logger.info("─" * 60)
        logger.info("summary")
        logger.info("─" * 60)
        for table in tables:
            rows = _verify_table(cur, cfg.database, cfg.schema, table.name)
            row_str = f"{rows:,}" if rows >= 0 else "table not found"
            logger.info(f"  {table.name:<22} {row_str:>12} rows")
        logger.info("─" * 60)

    finally:
        conn.close()
        logger.debug("Snowflake connection closed")


if __name__ == "__main__":
    main()
