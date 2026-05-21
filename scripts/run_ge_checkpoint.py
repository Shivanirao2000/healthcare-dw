"""
Run a Great Expectations checkpoint against Snowflake RAW tables and exit
with code 1 on any expectation failure so Airflow marks the task as failed.

Usage:
    # Run the default nightly checkpoint
    python scripts/run_ge_checkpoint.py

    # Run a specific checkpoint
    python scripts/run_ge_checkpoint.py --checkpoint daily_raw_checkpoint

    # Validate only a specific DATA_YEAR (adds a WHERE clause via the query override)
    python scripts/run_ge_checkpoint.py --year 2022

    # Print the full list of unexpected values for failing expectations
    python scripts/run_ge_checkpoint.py --result-format COMPLETE

Airflow integration:
    The BashOperator in airflow/dags/ingestion_dag.py runs:
        cd /opt/healthcare-dw && make ge-check
    which calls this script.  A non-zero exit code fails the Airflow task
    and triggers the retry policy and on-failure callbacks defined in the DAG.

Exit codes:
    0  — all expectations passed
    1  — one or more expectations failed, or the checkpoint raised an exception
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

import great_expectations as gx
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# Path to the great_expectations/ project root, resolved from this script's location.
GE_ROOT = Path(__file__).resolve().parents[1] / "great_expectations"
DEFAULT_CHECKPOINT = "daily_raw_checkpoint"


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Run a GE checkpoint and exit 1 on failure (Airflow-safe)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--checkpoint",
        default=DEFAULT_CHECKPOINT,
        help="Name of the checkpoint to run (must exist in great_expectations/checkpoints/)",
    )
    p.add_argument(
        "--year",
        type=int,
        default=None,
        help=(
            "Restrict validation to a single CMS data year by injecting a WHERE clause. "
            "Omit to validate the full RAW_CLAIMS table."
        ),
    )
    p.add_argument(
        "--result-format",
        default="SUMMARY",
        choices=["BASIC", "SUMMARY", "COMPLETE"],
        dest="result_format",
        help=(
            "BASIC: pass/fail only.  "
            "SUMMARY: includes a sample of unexpected values.  "
            "COMPLETE: full unexpected-value list (expensive on large tables)."
        ),
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        dest="log_level",
    )
    return p


# ─────────────────────────────────────────────────────────────────────────────
# Formatting helpers
# ─────────────────────────────────────────────────────────────────────────────

def _format_result_row(expectation_type: str, success: bool, result: dict) -> str:
    """Return a single-line summary of one expectation result."""
    icon   = "✓" if success else "✗"
    detail = ""
    if not success:
        observed = result.get("observed_value", "")
        unexpected_count = result.get("unexpected_count", "")
        unexpected_pct   = result.get("unexpected_percent", "")
        parts = []
        if observed is not None and observed != "":
            parts.append(f"observed={observed}")
        if unexpected_count:
            parts.append(f"unexpected={unexpected_count} ({unexpected_pct:.2f}%)")
        if parts:
            detail = "  →  " + ", ".join(str(p) for p in parts)
    return f"  {icon}  {expectation_type}{detail}"


def _print_summary(validation_results: list) -> None:
    """Print a structured pass/fail table for all expectations."""
    total_pass = total_fail = 0
    logger.info("─" * 70)
    logger.info("Expectation results")
    logger.info("─" * 70)
    for vr in validation_results:
        stats = vr.statistics
        logger.info(
            f"Suite: {vr.meta['expectation_suite_name']}  |  "
            f"Evaluated: {stats['evaluated_expectations']}  |  "
            f"Passed: {stats['successful_expectations']}  |  "
            f"Failed: {stats['unsuccessful_expectations']}"
        )
        for er in vr.results:
            line = _format_result_row(
                er.expectation_config.expectation_type,
                er.success,
                er.result,
            )
            if er.success:
                logger.info(line)
            else:
                logger.error(line)
                # Log unexpected index values so the Airflow log is self-diagnosing.
                unexpected_list = er.result.get("partial_unexpected_index_list") or \
                                  er.result.get("unexpected_index_list") or []
                for idx_row in unexpected_list[:5]:  # cap at 5 to keep logs readable
                    logger.error(f"       sample failing row: {idx_row}")
        total_pass += stats["successful_expectations"]
        total_fail += stats["unsuccessful_expectations"]
    logger.info("─" * 70)
    logger.info(f"Total:  {total_pass + total_fail} expectations  |  "
                f"{total_pass} passed  |  {total_fail} failed")
    logger.info("─" * 70)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)

    logger.remove()
    logger.add(sys.stderr, level=args.log_level, colorize=True,
               format="<level>{level: <8}</level> | {message}")

    # ── Validate environment before connecting ────────────────────────────────
    required_vars = [
        "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_DATABASE", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_ROLE",
    ]
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        logger.error("Ensure .env is loaded or variables are set in the shell.")
        sys.exit(1)

    # ── Load GE context ───────────────────────────────────────────────────────
    logger.info(f"Loading GE context from {GE_ROOT}")
    if not GE_ROOT.exists():
        logger.error(f"GE root directory not found: {GE_ROOT}")
        sys.exit(1)

    try:
        context = gx.get_context(context_root_dir=str(GE_ROOT))
    except Exception as exc:
        logger.error(f"Failed to load GE context: {exc}")
        sys.exit(1)

    # ── Build runtime parameters ──────────────────────────────────────────────
    # If --year was passed, inject a year-scoped WHERE clause so the checkpoint
    # validates only the specified CMS release rather than all ~50 M rows.
    # The checkpoint YAML references #{run_date} as a batch identifier; we
    # supply today's date so each run produces a distinct entry in the
    # validations store (enabling historical trend comparison in Data Docs).
    run_date = date.today().isoformat()

    batch_parameters: dict = {"run_date": run_date}
    if args.year is not None:
        logger.info(f"Scoping validation to DATA_YEAR = {args.year}")
        # Override the query in the checkpoint's batch_request at runtime.
        batch_parameters["query"] = (
            f"SELECT * FROM RAW.RAW_CLAIMS WHERE DATA_YEAR = {args.year}"
        )

    runtime_configuration = {
        "result_format": {
            "result_format": args.result_format,
            "unexpected_index_column_names": ["RNDRNG_NPI", "HCPCS_CD", "DATA_YEAR"],
        }
    }

    # ── Run checkpoint ────────────────────────────────────────────────────────
    logger.info(
        f"Running checkpoint '{args.checkpoint}'  "
        f"result_format={args.result_format}  "
        f"run_date={run_date}"
        + (f"  year={args.year}" if args.year else "")
    )

    try:
        result = context.run_checkpoint(
            checkpoint_name=args.checkpoint,
            batch_parameters=batch_parameters,
            runtime_configuration=runtime_configuration,
        )
    except Exception as exc:
        # Any exception from GE (connection failure, YAML parse error, missing
        # suite, etc.) must exit 1 so Airflow marks the task as failed rather
        # than silently succeeding.
        logger.error(f"Checkpoint raised an exception: {exc}")
        logger.exception(exc)
        sys.exit(1)

    # ── Print results ─────────────────────────────────────────────────────────
    _print_summary(result.list_validation_results())

    # ── Data Docs location ────────────────────────────────────────────────────
    docs_path = GE_ROOT / "uncommitted" / "data_docs" / "local_site" / "index.html"
    if docs_path.exists():
        logger.info(f"Data Docs updated → file://{docs_path}")

    # ── Exit code ─────────────────────────────────────────────────────────────
    if result.success:
        logger.info("Checkpoint PASSED — all expectations met")
        sys.exit(0)
    else:
        logger.error(
            "Checkpoint FAILED — one or more expectations were not met. "
            "Review the output above and the Data Docs site for details. "
            "Fix the data quality issue or update the expectation suite before "
            "proceeding with dbt model runs."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
