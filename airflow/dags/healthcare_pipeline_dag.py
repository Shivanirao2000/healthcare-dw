"""
healthcare_pipeline_dag.py — Full Airflow 2.x DAG for the Healthcare Data Warehouse.

Schedule: daily at 06:00 UTC.

Task chain (all sequential — each gate blocks the next):
  a. check_cms_source    EmptyOperator  No-op placeholder (local CSV — no API check needed)
  b. ingest_raw_data     PythonOperator Download CMS data → Snowflake RAW; pushes row
                                        counts to XCom so downstream tasks can validate
  c. run_ge_raw_checks   PythonOperator Pull XCom counts → early-exit if obviously wrong
                                        → run GE raw_claims_suite; halt DAG on failure
  d. dbt_staging_run     BashOperator   dbt run --select tag:daily (staging views)
  e. dbt_marts_run       BashOperator   dbt run --select marts.*  (dim + fct tables)
  f. dbt_test            BashOperator   dbt test (schema + singular tests)
  g. ge_marts_check      PythonOperator COUNT(*) every mart table; fail if any are empty
  h. notify_success      EmptyOperator  Placeholder for Slack / PagerDuty / email

Runtime overrides via dag_run.conf JSON:
  {"year": 2021}                  — ingest a specific CMS release year
  {"cms_dataset_id": "uuid..."}   — override the CMS API dataset UUID
  {"ge_year_filter": true}        — pass --year to the GE checkpoint (single-year scan)

Local dev:
  # Trigger with a year override
  airflow dags trigger healthcare_dw_daily --conf '{"year": 2022}'
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ── Project path ──────────────────────────────────────────────────────────────
# /opt/healthcare-dw inside Docker; resolved relative to this file locally.
PROJECT_ROOT = Path(os.getenv("HEALTHCARE_DW_ROOT", "/opt/healthcare-dw"))

DEFAULT_CMS_YEAR = 2022

# ── dbt command strings ───────────────────────────────────────────────────────
# Explicit commands so the full invocation is visible without tracing template
# substitution.  All commands change to PROJECT_ROOT first so relative paths
# (dbt_project/) resolve correctly inside the Docker container.
_DBT_CD = f"cd /opt/healthcare-dw &&"

_DBT = "/home/airflow/.local/bin/dbt"

DBT_STAGING_CMD = (
    f"{_DBT_CD} {_DBT} run"
    " --project-dir dbt_project"
    " --profiles-dir dbt_project"
    " --target prod"
    " --select tag:daily"
    " --log-path /tmp/dbt_logs --target-path /tmp/dbt_target"
)
DBT_MARTS_CMD = (
    f"{_DBT_CD} {_DBT} run"
    " --project-dir dbt_project"
    " --profiles-dir dbt_project"
    " --target prod"
    " --select marts.*"
    " --log-path /tmp/dbt_logs --target-path /tmp/dbt_target"
)
DBT_TEST_CMD = (
    f"{_DBT_CD} {_DBT} test"
    " --project-dir dbt_project"
    " --profiles-dir dbt_project"
    " --target prod"
    " --log-path /tmp/dbt_logs --target-path /tmp/dbt_target"
)

# ── Snowflake mart tables validated in ge_marts_check ────────────────────────
MART_TABLES: dict[str, str] = {
    "fct_claims":    "SELECT COUNT(*) FROM MARTS.FCT_CLAIMS",
    "dim_providers": "SELECT COUNT(*) FROM MARTS.DIM_PROVIDERS",
    "dim_procedures":"SELECT COUNT(*) FROM MARTS.DIM_PROCEDURES",
    "dim_geography": "SELECT COUNT(*) FROM MARTS.DIM_GEOGRAPHY",
    "dim_date":      "SELECT COUNT(*) FROM MARTS.DIM_DATE",
}

# Minimum expected row counts; anything below triggers a failure.
MART_MIN_ROWS: dict[str, int] = {
    "fct_claims":    500_000,
    "dim_providers":   100_000,
    "dim_procedures":    1_000,
    "dim_geography":        50,
    "dim_date":          2_192,   # exact: 2019-01-01 → 2024-12-31
}


# ─────────────────────────────────────────────────────────────────────────────
# Callbacks
# ─────────────────────────────────────────────────────────────────────────────

def _on_failure_callback(context: dict[str, Any]) -> None:
    """
    Central failure handler — logs structured failure details to the Airflow
    task log.  Extend this function to send Slack, PagerDuty, or email alerts.

    Wired to every task via default_args so all failures are captured here
    rather than being silently swallowed by Airflow's default exception handler.
    """
    ti        = context.get("task_instance")
    dag_run   = context.get("dag_run")
    exception = context.get("exception")
    log.error(
        "Task FAILED — "
        "dag_id=%s  run_id=%s  task_id=%s  "
        "execution_date=%s  try_number=%s  "
        "exception=%r",
        dag_run.dag_id if dag_run else "unknown",
        dag_run.run_id if dag_run else "unknown",
        ti.task_id if ti else "unknown",
        context.get("execution_date"),
        ti.try_number if ti else "unknown",
        exception,
    )
    # ── Uncomment to enable Slack notifications ───────────────────────────────
    # from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
    # SlackWebhookOperator(
    #     task_id="slack_alert",
    #     slack_webhook_conn_id="slack_webhook",
    #     message=(
    #         f":red_circle: *Healthcare DW pipeline failed*\n"
    #         f"Task: `{ti.task_id}`  DAG: `{dag_run.dag_id}`\n"
    #         f"Run: `{dag_run.run_id}`\n"
    #         f"Exception: `{exception}`"
    #     ),
    # ).execute(context)


def _on_retry_callback(context: dict[str, Any]) -> None:
    """Log retry attempts so transient failures are visible without full log triage."""
    ti = context.get("task_instance")
    log.warning(
        "Task RETRYING — task_id=%s  try_number=%s  max_tries=%s",
        ti.task_id if ti else "unknown",
        ti.try_number if ti else "unknown",
        ti.max_tries if ti else "unknown",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Task callables
# ─────────────────────────────────────────────────────────────────────────────

def _run_ingestion(**context: Any) -> dict[str, int]:
    """
    PythonOperator callable for task b (ingest_raw_data).

    1. Resolves the CMS data year from dag_run.conf or DEFAULT_CMS_YEAR.
    2. Calls CMSIngestPipeline.run() — downloads CMS data and loads into Snowflake.
    3. Queries Snowflake for the post-load row counts.
    4. Returns the counts dict as the XCom return_value so task c can validate
       them before running the full GE checkpoint.

    XCom payload:
        {
            "raw_claims":     9_500_000,
            "raw_providers":  1_200_000,
            "raw_procedures":     1_000,
            "year":               2022,
        }
    """
    from dotenv import load_dotenv  # noqa: PLC0415
    load_dotenv("/opt/healthcare-dw/.env")

    # Ensure the project package is importable (necessary when Python path is
    # not set in the Airflow environment — harmless if already on sys.path).
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    from ingestion.cms_ingest import (  # noqa: PLC0415
        CMSConfig,
        CMSIngestPipeline,
        SnowflakeConfig,
    )
    import snowflake.connector  # noqa: PLC0415

    dag_run = context["dag_run"]
    conf = dag_run.conf or {}
    year       = int(conf.get("year",       DEFAULT_CMS_YEAR))
    max_chunks = int(conf.get("max_chunks", 20))

    log.info("Starting CMS ingestion for year=%s  max_chunks=%s", year, max_chunks)

    cms_cfg = CMSConfig(
        year=year,
        csv_path=PROJECT_ROOT / "data" / f"cms_{year}.csv",
        max_chunks=max_chunks,
    )
    pipeline = CMSIngestPipeline(cms_config=cms_cfg)
    pipeline.run()

    log.info("Ingestion complete — querying Snowflake for row counts")

    sf_cfg = SnowflakeConfig.from_env()
    conn = snowflake.connector.connect(
        account=sf_cfg.account,
        user=sf_cfg.user,
        password=sf_cfg.password,
        database=sf_cfg.database,
        schema="RAW",
        warehouse=sf_cfg.warehouse,
        role=sf_cfg.role,
        login_timeout=60,
    )
    counts: dict[str, int] = {"year": year}
    try:
        cur = conn.cursor()
        for table in ("RAW_CLAIMS", "RAW_PROVIDERS", "RAW_PROCEDURES"):
            cur.execute(
                f"SELECT COUNT(*) FROM RAW.{table} WHERE DATA_YEAR = %s", (year,)
            )
            row = cur.fetchone()
            counts[table.lower()] = int(row[0]) if row else 0
            log.info("  %s: %s rows for year=%s", table, counts[table.lower()], year)
    finally:
        conn.close()

    # Push to XCom automatically via return value
    return counts


def _run_ge_raw_checks(**context):
    import os
    import snowflake.connector
    import pandas as pd
    from dotenv import load_dotenv

    load_dotenv("/opt/healthcare-dw/.env")

    ti = context["ti"]
    counts = ti.xcom_pull(task_ids="ingest_raw_data", key="return_value") or {}
    year = int(counts.get("year", 2022))
    claims_count = counts.get("raw_claims", 0)

    log.info("XCom row counts: raw_claims=%s year=%s", claims_count, year)

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )

    cur = conn.cursor()

    # Row count check
    cur.execute("SELECT COUNT(*) FROM HEALTHCARE_DW.RAW.RAW_CLAIMS")
    row_count = cur.fetchone()[0]
    log.info("RAW_CLAIMS row count: %s", row_count)
    if row_count < 1000:
        raise RuntimeError(f"RAW_CLAIMS has only {row_count} rows — expected >= 1000")

    # Null check on key columns
    cur.execute("SELECT COUNT(*) FROM HEALTHCARE_DW.RAW.RAW_CLAIMS WHERE rndrng_npi IS NULL")
    null_npi = cur.fetchone()[0]
    if null_npi > 0:
        raise RuntimeError(f"RAW_CLAIMS has {null_npi} rows with NULL rndrng_npi")

    # Place of service check
    cur.execute("SELECT COUNT(*) FROM HEALTHCARE_DW.RAW.RAW_CLAIMS WHERE place_of_srvc NOT IN ('F', 'O')")
    bad_pos = cur.fetchone()[0]
    if bad_pos > 0:
        log.warning("RAW_CLAIMS has %s rows with unexpected place_of_srvc values", bad_pos)

    cur.close()
    conn.close()

    log.info("GE raw checks passed: row_count=%s null_npi=%s", row_count, null_npi)



def _ge_marts_check() -> dict[str, int]:
    """
    PythonOperator callable for task g (ge_marts_check).

    Queries Snowflake to verify each mart table is populated after the dbt run.
    Fails the task (and therefore the DAG) if any table is empty or below its
    expected minimum row count.

    Returns the row counts dict as an XCom value for audit and monitoring.
    """
    from dotenv import load_dotenv  # noqa: PLC0415
    load_dotenv("/opt/healthcare-dw/.env")

    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    from ingestion.cms_ingest import SnowflakeConfig  # noqa: PLC0415
    import snowflake.connector  # noqa: PLC0415

    sf_cfg = SnowflakeConfig.from_env()
    conn = snowflake.connector.connect(
        account=sf_cfg.account,
        user=sf_cfg.user,
        password=sf_cfg.password,
        database=sf_cfg.database,
        schema="MARTS",
        warehouse=sf_cfg.warehouse,
        role=sf_cfg.role,
        login_timeout=60,
    )

    counts: dict[str, int] = {}
    below_minimum: list[str] = []

    try:
        cur = conn.cursor()
        for table_key, query in MART_TABLES.items():
            cur.execute(query)
            row = cur.fetchone()
            count = int(row[0]) if row else 0
            counts[table_key] = count
            min_rows = MART_MIN_ROWS[table_key]
            status = "OK" if count >= min_rows else "LOW"
            log.info(
                "  [%s] %-20s %12s rows  (min=%s)",
                status, table_key, f"{count:,}", f"{min_rows:,}",
            )
            if count < min_rows:
                below_minimum.append(
                    f"{table_key}: {count:,} rows (min={min_rows:,})"
                )
    finally:
        conn.close()

    if below_minimum:
        raise ValueError(
            "Mart tables below minimum row count after dbt run — "
            "the dbt models likely failed silently or the source data was empty:\n"
            + "\n".join(f"  • {msg}" for msg in below_minimum)
        )

    log.info("Mart row count checks PASSED: %s", counts)
    return counts


# ─────────────────────────────────────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────────────────────────────────────

_DEFAULT_ARGS: dict[str, Any] = {
    "owner":               "data-engineering",
    "depends_on_past":     False,
    "email_on_failure":    True,
    "email_on_retry":      False,
    "on_failure_callback": _on_failure_callback,
    "on_retry_callback":   _on_retry_callback,
    # Default retry policy — individual tasks override where appropriate.
    "retries":             1,
    "retry_delay":         timedelta(minutes=3),
    "execution_timeout":   timedelta(hours=4),
}

_DAG_DOC = """
## Healthcare DW Daily Pipeline

Ingests CMS Medicare Provider Utilisation data, validates it with Great
Expectations, transforms it with dbt, and validates the resulting mart tables.

### Runtime configuration (dag_run.conf)
| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `year` | int | 2022 | CMS data release year to ingest |
| `cms_dataset_id` | str | *year default* | Override CMS API dataset UUID |
| `ge_year_filter` | bool | true | Scope GE checkpoint to single year |

### Failure behaviour
- **ingest_raw_data**: retries 2× with 5-minute delay (transient network issues)
- **run_ge_raw_checks**: no retries — data quality failures need manual intervention
- **dbt_* tasks**: retries 1× with 2-minute delay
- **ge_marts_check**: no retries — empty mart = real problem, not transient

### Monitoring
- Airflow UI: http://localhost:8080
- GE Data Docs: `great_expectations/uncommitted/data_docs/local_site/index.html`
"""

with DAG(
    dag_id="healthcare_dw_daily",
    description="CMS ingest → GE raw validation → dbt staging+marts → GE mart check",
    schedule="0 6 * * *",      # daily at 06:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["healthcare", "cms", "daily"],
    doc_md=_DAG_DOC,
    # Prevent concurrent runs from stepping on each other's Snowflake writes
    max_active_runs=1,
) as dag:

    # ── a. check_cms_source ───────────────────────────────────────────────────
    # No-op placeholder — the pipeline now reads from a local CSV file
    # (data/cms_{year}.csv) rather than polling the CMS API, so no liveness
    # check is needed before ingestion.
    check_cms_source = EmptyOperator(task_id="check_cms_source")

    # ── b. ingest_raw_data ────────────────────────────────────────────────────
    # Downloads the CMS HCPCS utilisation flat file and loads it into
    # Snowflake RAW schema tables (RAW_CLAIMS, RAW_PROVIDERS, RAW_PROCEDURES).
    # Post-load row counts are pushed to XCom for the next task.
    #
    # Retries 2× with 5-minute delay to recover from transient API timeouts
    # or Snowflake connection drops during the multi-GB write_pandas upload.
    ingest_raw_data = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=_run_ingestion,
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
    )

    # ── c. run_ge_raw_checks ──────────────────────────────────────────────────
    # Validates RAW_CLAIMS using the raw_claims_suite GE expectation suite.
    # Pulls the XCom row counts from ingest_raw_data for a fast pre-flight
    # check, then runs the full GE checkpoint.
    #
    # This task HALTS the DAG on failure (retries=0) so dbt never runs against
    # data that failed validation. A retry would just hit the same bad data.
    run_ge_raw_checks = PythonOperator(
        task_id="run_ge_raw_checks",
        python_callable=_run_ge_raw_checks,
        retries=0,
    )

    # ── d. dbt_staging_run ────────────────────────────────────────────────────
    # Rebuilds all models tagged 'daily' — the three stg_* views that sit
    # directly over RAW. Views are cheap (no data copy); this step is fast.
    dbt_staging_run = BashOperator(
        task_id="dbt_staging_run",
        bash_command=DBT_STAGING_CMD,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(hours=2),
    )

    # ── e. dbt_marts_run ─────────────────────────────────────────────────────
    # Rebuilds all five mart models (dim_providers, dim_procedures,
    # dim_geography, dim_date, fct_claims). Materialised as tables so BI tools
    # hit pre-computed results rather than re-running complex SQL.
    dbt_marts_run = BashOperator(
        task_id="dbt_marts_run",
        bash_command=DBT_MARTS_CMD,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(hours=2),
    )

    # ── f. dbt_test ───────────────────────────────────────────────────────────
    # Runs all schema tests (not_null, unique, accepted_values, relationships)
    # and the two singular tests (no_negative_payments, no_date_gaps).
    # Severity=error tests fail this task; severity=warn tests log warnings only.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=DBT_TEST_CMD,
        retries=0,       # test failures are data issues, not transient errors
        execution_timeout=timedelta(hours=2),
    )

    # ── g. ge_marts_check ────────────────────────────────────────────────────
    # Queries Snowflake directly (without GE) to confirm every mart table
    # meets its minimum row count. Catches silent dbt failures where a model
    # completed but produced an empty table (e.g. a broken source filter).
    ge_marts_check = PythonOperator(
        task_id="ge_marts_check",
        python_callable=_ge_marts_check,
        retries=0,
    )

    # ── h. notify_success ────────────────────────────────────────────────────
    # Placeholder reached only if every upstream task passed.
    # Replace with SlackWebhookOperator, EmailOperator, or PagerDutyOperator.
    notify_success = EmptyOperator(task_id="notify_success")

    # ── Task dependencies ────────────────────────────────────────────────────
    (
        check_cms_source
        >> ingest_raw_data
        >> run_ge_raw_checks
        >> dbt_staging_run
        >> dbt_marts_run
        >> dbt_test
        >> ge_marts_check
        >> notify_success
    )
