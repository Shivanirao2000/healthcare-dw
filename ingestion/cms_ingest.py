"""
CMS Medicare Provider Utilization and Payment Data ingestion pipeline.

Reads the CMS HCPCS flat-file CSV from a local path and loads three normalised
tables into Snowflake RAW schema:

    RAW.RAW_PROVIDERS   — one row per rendering provider NPI
    RAW.RAW_PROCEDURES  — one row per unique HCPCS procedure code
    RAW.RAW_CLAIMS      — one row per NPI × HCPCS_Cd × Place_Of_Srvc × year

Chunk-wise loading strategy
────────────────────────────
The CSV is read with pd.read_csv(chunksize=50_000) so no more than one chunk
lives in memory at a time.  Each chunk is mapped to three table shapes and
written to Snowflake immediately via write_pandas():

  RAW_CLAIMS      — every chunk appended in order.  The first chunk truncates
                    any data left from a prior run (overwrite=True); subsequent
                    chunks append (overwrite=False).

  RAW_PROVIDERS   — one NPI may appear in many CSV rows (once per HCPCS code
                    billed).  A seen_npis set tracks every NPI written so far;
                    only rows with unseen NPIs are sent to Snowflake, keeping
                    the table at one row per NPI globally across all chunks.
                    First write truncates, subsequent writes append.

  RAW_PROCEDURES  — same cross-chunk deduplication via seen_hcpcs set.

Place the downloaded CMS file at data/cms_{year}.csv before running.  The
data/ directory is git-ignored; the file is never committed.

Run standalone:
    python ingestion/cms_ingest.py [--year 2022]
    python ingestion/cms_ingest.py --csv-path /path/to/custom.csv --year 2022

Via the pipeline registry:
    python -m ingestion.run --source cms
"""

from __future__ import annotations

import argparse
import os
import itertools
import pathlib
import sys
from dataclasses import dataclass, field
from datetime import date
from typing import Iterator

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from loguru import logger
from snowflake.connector.pandas_tools import write_pandas

from ingestion.base import BasePipeline

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# File and chunking constants
# ─────────────────────────────────────────────────────────────────────────────

# Root directory for local CMS CSV files.  Files are expected at:
#   data/cms_{year}.csv   (e.g. data/cms_2022.csv)
# Add data/ to .gitignore — files are hundreds of MB and must not be committed.
DEFAULT_CSV_DIR: pathlib.Path = pathlib.Path("data")

DEFAULT_YEAR: int = 2022

# Rows handed to each write_pandas() call.
# 50 000 rows ≈ 10–15 MB in memory; safe for machines with ≥ 8 GB RAM.
# Raise to 100_000–200_000 if ingestion is bottlenecked by Snowflake round-trips.
DEFAULT_CHUNK_ROWS: int = 50_000

# ─────────────────────────────────────────────────────────────────────────────
# CMS HCPCS flat-file column schema
# Source: CMS Medicare Physician & Other Practitioners data dictionary
# https://data.cms.gov/resources/medicare-physician-other-practitioners-data-dictionary
# ─────────────────────────────────────────────────────────────────────────────

# Provider demographics — grain: one row per NPI
PROVIDER_COLS: list[str] = [
    "Rndrng_NPI",
    "Rndrng_Prvdr_Last_Org_Name",
    "Rndrng_Prvdr_First_Name",
    "Rndrng_Prvdr_MI",
    "Rndrng_Prvdr_Crdntls",
    "Rndrng_Prvdr_Gndr",
    "Rndrng_Prvdr_Ent_Cd",         # 'I' = individual, 'O' = organisation
    "Rndrng_Prvdr_St1",
    "Rndrng_Prvdr_St2",
    "Rndrng_Prvdr_City",
    "Rndrng_Prvdr_State_Abrvtn",
    "Rndrng_Prvdr_State_FIPS",
    "Rndrng_Prvdr_Zip5",
    "Rndrng_Prvdr_RUCA",           # rural-urban commuting area code
    "Rndrng_Prvdr_RUCA_Desc",
    "Rndrng_Prvdr_Cntry",
    "Rndrng_Prvdr_Type",           # specialty type
    "Rndrng_Prvdr_Mdcr_Prtcptg_Ind",
]

# Procedure reference data — grain: one row per HCPCS code
PROCEDURE_COLS: list[str] = [
    "HCPCS_Cd",
    "HCPCS_Desc",
    "HCPCS_Drug_Ind",              # 'Y' = drug/biologic claim
]

# Claims utilisation and payment metrics
# Grain: NPI × HCPCS_Cd × Place_Of_Srvc (+ data year from LOAD_DATE)
CLAIMS_COLS: list[str] = [
    "Rndrng_NPI",
    "HCPCS_Cd",
    "Place_Of_Srvc",               # 'F' = facility, 'O' = office
    "Tot_Benes",                   # distinct Medicare beneficiaries
    "Tot_Srvcs",                   # total services billed
    "Tot_Bene_Day_Srvcs",          # beneficiary-day services
    "Avg_Sbmtd_Chrg",              # average submitted (billed) charge
    "Avg_Mdcr_Alowd_Amt",          # average Medicare allowed amount
    "Avg_Mdcr_Pymt_Amt",           # average Medicare payment (after adjustments)
    "Avg_Mdcr_Stdzd_Amt",          # geographically standardised payment
]

# Columns read as strings from the CSV that must be cast to numeric for Snowflake.
# All other columns stay as object (string) — safer than letting pandas infer types,
# which can silently truncate large integers or misinterpret zip codes.
NUMERIC_COLS: frozenset[str] = frozenset({
    "Tot_Benes",
    "Tot_Srvcs",
    "Tot_Bene_Day_Srvcs",
    "Avg_Sbmtd_Chrg",
    "Avg_Mdcr_Alowd_Amt",
    "Avg_Mdcr_Pymt_Amt",
    "Avg_Mdcr_Stdzd_Amt",
    "Rndrng_Prvdr_RUCA",
})

# ─────────────────────────────────────────────────────────────────────────────
# Configuration dataclasses
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    password: str
    database: str
    schema: str
    warehouse: str
    role: str

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        """Read Snowflake credentials from environment / .env file."""
        missing = [
            v for v in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")
            if not os.getenv(v)
        ]
        if missing:
            raise EnvironmentError(f"Missing required env vars: {', '.join(missing)}")
        return cls(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            database=os.getenv("SNOWFLAKE_DATABASE", "HEALTHCARE_DW"),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "HEALTHCARE_WH"),
            role=os.getenv("SNOWFLAKE_ROLE", "TRANSFORMER"),
        )


@dataclass(frozen=True)
class CMSConfig:
    year: int
    csv_path: pathlib.Path
    chunk_rows: int = field(default=DEFAULT_CHUNK_ROWS)
    # Maximum number of chunks to process. None = read the full file.
    # Set to a small integer (e.g. 20) to load only the first N × chunk_rows
    # rows — useful for smoke-testing the pipeline without a full 9.5 M-row load.
    max_chunks: int | None = field(default=None)

    @classmethod
    def for_year(
        cls,
        year: int = DEFAULT_YEAR,
        chunk_rows: int = DEFAULT_CHUNK_ROWS,
    ) -> "CMSConfig":
        """Construct config for a standard year using the default file location."""
        return cls(
            year=year,
            csv_path=DEFAULT_CSV_DIR / f"cms_{year}.csv",
            chunk_rows=chunk_rows,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Local CSV reader
# ─────────────────────────────────────────────────────────────────────────────

class CMSLocalFileClient:
    """Reads a local CMS HCPCS flat-file CSV in fixed-size row chunks.

    Validates the file exists at construction time so the pipeline fails
    immediately with a clear message rather than deep inside pd.read_csv().

    Each call to chunks() opens the file once and yields successive DataFrames
    of config.chunk_rows rows.  Column names are stripped of whitespace on every
    chunk because CMS files occasionally include trailing spaces in headers
    (e.g. "Rndrng_NPI " instead of "Rndrng_NPI"), which would cause every
    column-name lookup to silently miss.

    dtype=str: read every column as a string.  _coerce_numerics() applies
    precise numeric casts afterward.  This prevents pandas from:
      • converting NPI "0123456789" → int 123456789 (drops leading zero)
      • converting ZIP5 "02101"     → int 2101
      • coercing dollar amounts stored as "1,234.56" into NaN

    encoding='utf-8-sig': strips the UTF-8 BOM that CMS occasionally prepends,
    which would otherwise make the first header appear as '\\ufeffRndrng_NPI'.

    keep_default_na=False + na_values=[""]: only blank cells become NaN.
    Without this, pandas would treat the string "NA" as NaN — problematic
    because CMS uses "NA" in credential and description fields.
    """

    def __init__(self, config: CMSConfig) -> None:
        if not config.csv_path.exists():
            raise FileNotFoundError(
                f"CMS CSV file not found: {config.csv_path}\n"
                f"Download the {config.year} release from:\n"
                f"  https://data.cms.gov/provider-summary-by-type-of-service/"
                f"medicare-physician-other-practitioners/"
                f"medicare-physician-other-practitioners-by-provider-and-service\n"
                f"and save it to {config.csv_path}"
            )
        self._config = config

    def chunks(self) -> Iterator[pd.DataFrame]:
        """Yield successive DataFrames of chunk_rows rows each."""
        path = self._config.csv_path
        size_mb = path.stat().st_size / 1e6
        logger.info(
            f"  reading {path}  ({size_mb:.0f} MB)"
            f"  chunk_rows={self._config.chunk_rows:,}"
        )

        reader = pd.read_csv(
            path,
            dtype=str,
            low_memory=False,
            encoding="utf-8-sig",
            na_values=[""],
            keep_default_na=False,
            chunksize=self._config.chunk_rows,
        )

        for chunk in reader:
            chunk.columns = [c.strip() for c in chunk.columns]
            yield chunk


# ─────────────────────────────────────────────────────────────────────────────
# DataFrame transformation helpers
# ─────────────────────────────────────────────────────────────────────────────

def _coerce_numerics(df: pd.DataFrame) -> pd.DataFrame:
    """Cast known numeric columns; leave all others as string to avoid silent loss."""
    for col in NUMERIC_COLS & set(df.columns):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _add_metadata(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Stamp load metadata columns on every row."""
    df = df.copy()
    df["LOAD_DATE"] = date.today().isoformat()     # date of this pipeline run
    df["DATA_YEAR"] = year                          # CMS release year
    return df


def _select_cols(df: pd.DataFrame, cols: list[str], *, extra: list[str]) -> pd.DataFrame:
    """Return only the requested columns (+ extras), skipping any not present."""
    available = [c for c in cols + extra if c in df.columns]
    missing = set(cols) - set(df.columns)
    if missing:
        logger.warning(
            f"  columns absent from CSV file (schema drift between CMS releases?): "
            f"{sorted(missing)}"
        )
    return df[available].copy()


def _to_snowflake_names(df: pd.DataFrame) -> pd.DataFrame:
    """Uppercase all column names — Snowflake treats unquoted identifiers as upper."""
    df.columns = [c.upper() for c in df.columns]
    return df


def build_providers(raw: pd.DataFrame, year: int) -> pd.DataFrame:
    """Deduplicate to one row per NPI within this chunk and add metadata."""
    df = _select_cols(raw, PROVIDER_COLS, extra=["LOAD_DATE", "DATA_YEAR"])
    df = _add_metadata(df, year)
    df = df.drop_duplicates(subset=["Rndrng_NPI"])
    return _to_snowflake_names(df)


def build_procedures(raw: pd.DataFrame, year: int) -> pd.DataFrame:
    """Deduplicate to one row per HCPCS code within this chunk and add metadata."""
    df = _select_cols(raw, PROCEDURE_COLS, extra=["LOAD_DATE", "DATA_YEAR"])
    df = _add_metadata(df, year)
    df = df.drop_duplicates(subset=["HCPCS_Cd"])
    return _to_snowflake_names(df)


def build_claims(raw: pd.DataFrame, year: int) -> pd.DataFrame:
    """Return the full utilisation grain (NPI × HCPCS × place) with metadata."""
    df = _select_cols(raw, CLAIMS_COLS, extra=["LOAD_DATE", "DATA_YEAR"])
    df = _add_metadata(df, year)
    return _to_snowflake_names(df)


# ─────────────────────────────────────────────────────────────────────────────
# Snowflake loader
# ─────────────────────────────────────────────────────────────────────────────

class SnowflakeLoader:
    """Context manager that wraps a Snowflake connection and exposes load_table().

    The overwrite parameter controls truncation behaviour:

      overwrite=True  — TRUNCATE TABLE then INSERT (first chunk of each table).
        Clears any rows left over from a previous pipeline run.
        Logs the BEFORE and AFTER row counts via SELECT COUNT(*).

      overwrite=False — INSERT only, appending to existing rows (all subsequent
        chunks).  Skips the COUNT(*) queries to avoid one Snowflake round-trip
        per chunk across ~190 chunks.

    auto_create_table=True means the first run bootstraps the schema from the
    DataFrame column names and dtypes, so no separate CREATE TABLE step is
    required after schema changes — run create_raw_tables.py first for
    production-quality DDL with explicit types.
    """

    def __init__(self, config: SnowflakeConfig) -> None:
        self._config = config
        self._conn: snowflake.connector.SnowflakeConnection | None = None

    def __enter__(self) -> "SnowflakeLoader":
        logger.info(
            f"connecting to Snowflake  "
            f"account={self._config.account}  "
            f"db={self._config.database}.{self._config.schema}"
        )
        self._conn = snowflake.connector.connect(
            account=self._config.account,
            user=self._config.user,
            password=self._config.password,
            database=self._config.database,
            schema=self._config.schema,
            warehouse=self._config.warehouse,
            role=self._config.role,
            # Increase network timeouts for large write_pandas uploads
            network_timeout=300,
            login_timeout=60,
        )
        return self

    def __exit__(self, *_: object) -> None:
        if self._conn:
            self._conn.close()
            logger.debug("Snowflake connection closed")

    def _row_count(self, table_name: str) -> int:
        """Return current row count; -1 if the table does not yet exist."""
        assert self._conn is not None
        try:
            cur = self._conn.cursor()
            cur.execute(
                f"SELECT COUNT(*) FROM {self._config.database}.{self._config.schema}.{table_name}"
            )
            result = cur.fetchone()
            return int(result[0]) if result else 0
        except snowflake.connector.errors.ProgrammingError:
            return -1  # table does not exist yet

    def load_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        *,
        overwrite: bool = True,
        chunk_size: int = 10_000,
    ) -> None:
        """Write *df* to *table_name*.

        overwrite=True  — truncates the table before inserting; logs BEFORE and
                          AFTER row counts.
        overwrite=False — appends without truncation; skips COUNT(*) round-trips.
        """
        assert self._conn is not None

        # BEFORE count only on the truncating write — avoids 190 COUNT(*) queries
        # for the append-mode chunks that follow.
        if overwrite:
            pre = self._row_count(table_name)
            if pre == -1:
                logger.info(f"  [{table_name}] does not exist — will be created")
            else:
                logger.info(f"  [{table_name}] rows BEFORE load : {pre:>12,}")

        inbound = len(df)
        logger.info(f"  [{table_name}] rows inbound      : {inbound:>12,}")

        success, n_chunks, n_rows, copy_results = write_pandas(
            conn=self._conn,
            df=df,
            table_name=table_name,
            database=self._config.database,
            schema=self._config.schema,
            overwrite=overwrite,         # True = TRUNCATE then INSERT; False = INSERT
            auto_create_table=True,      # bootstrap DDL from DataFrame dtypes on first run
            quote_identifiers=False,     # column names become UPPER in Snowflake
            chunk_size=chunk_size,
            compression="gzip",
            parallel=4,
        )

        if not success:
            # write_pandas returns success=False on partial failures; surface detail
            for r in copy_results:
                logger.error(f"  COPY result: {r}")
            raise RuntimeError(f"write_pandas reported failure for table {table_name}")

        if overwrite:
            # AFTER count only on the truncating write for the same reason as BEFORE.
            post = self._row_count(table_name)
            logger.info(
                f"  [{table_name}] rows AFTER  load : {post:>12,}"
                f"  ({n_chunks} chunk(s) uploaded)"
            )
            if post != inbound:
                logger.warning(
                    f"  [{table_name}] row count mismatch: "
                    f"sent {inbound:,}, Snowflake has {post:,} — "
                    f"check for duplicate-key collisions or COPY errors"
                )
        else:
            logger.info(
                f"  [{table_name}] rows written       : {inbound:>12,}"
                f"  ({n_chunks} chunk(s) uploaded)"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline class (integrates with BasePipeline registry)
# ─────────────────────────────────────────────────────────────────────────────

class CMSIngestPipeline(BasePipeline):
    """Chunk-wise extract → load pipeline for CMS HCPCS utilisation data.

    Overrides BasePipeline.run() to interleave reading and writing: each 50 000-row
    CSV chunk is mapped to all three table shapes and written to Snowflake before
    the next chunk is read, keeping peak memory usage below ~150 MB regardless of
    the full file size.

    extract() and load() satisfy the BasePipeline ABC but are never called by
    this class's run() — all logic lives in _run_chunked().
    """

    name = "cms"

    def __init__(
        self,
        cms_config: CMSConfig | None = None,
        sf_config: SnowflakeConfig | None = None,
    ) -> None:
        self._cms_cfg = cms_config or CMSConfig.for_year(DEFAULT_YEAR)
        self._sf_cfg = sf_config or SnowflakeConfig.from_env()

    # ── Public interface ──────────────────────────────────────────────────────

    def run(self) -> None:
        """Override BasePipeline.run() — reads CSV and writes Snowflake per chunk."""
        limit_str = (
            f"max_chunks={self._cms_cfg.max_chunks}"
            if self._cms_cfg.max_chunks is not None
            else "max_chunks=unlimited"
        )
        logger.info(
            f"[{self.name}] starting  "
            f"year={self._cms_cfg.year}  "
            f"path={self._cms_cfg.csv_path}  "
            f"chunk_rows={self._cms_cfg.chunk_rows:,}  "
            f"{limit_str}"
        )
        self._run_chunked()
        logger.info(f"[{self.name}] complete")

    # ── Core chunked ETL ──────────────────────────────────────────────────────

    def _run_chunked(self) -> None:
        """Read CSV in chunks and write each chunk to Snowflake immediately.

        Claims loading
        ──────────────
        Every row in every chunk goes to RAW_CLAIMS.  The first chunk truncates
        (overwrite=True); subsequent chunks append (overwrite=False).

        Providers and procedures cross-chunk deduplication
        ──────────────────────────────────────────────────
        The same NPI appears once per HCPCS code billed, so a single provider
        is spread across many chunks.  build_providers() deduplicates within a
        chunk; seen_npis / seen_hcpcs sets track keys already written to
        Snowflake so only genuinely new rows are inserted on each chunk.

        Because seen_npis grows to ~1.2 M 10-character strings (~60 MB) it fits
        comfortably in memory.  seen_hcpcs grows to ~5 000 entries — trivial.

        The `initialized` flags track which tables have received their first
        (truncating) write.  They are separate from chunk numbering because chunk 1
        always has new claims, but a pathological file could theoretically have
        chunk 1 with only duplicate NPIs (in which case the truncating write
        happens on whichever chunk first has new providers).
        """
        client = CMSLocalFileClient(self._cms_cfg)
        year = self._cms_cfg.year

        # Per-table first-write tracking — True once a table has been truncated.
        claims_initialized     = False
        providers_initialized  = False
        procedures_initialized = False

        # Cross-chunk deduplication keys (uppercase, matching _to_snowflake_names output)
        seen_npis:  set[str] = set()
        seen_hcpcs: set[str] = set()

        # Running totals for the final summary log
        total_claims     = 0
        total_providers  = 0
        total_procedures = 0
        chunk_num        = 0

        # Optionally cap the number of chunks so the pipeline can be run against
        # a subset of the file (e.g. max_chunks=20 → first 1 M rows for smoke tests).
        chunks_iter = (
            itertools.islice(client.chunks(), self._cms_cfg.max_chunks)
            if self._cms_cfg.max_chunks is not None
            else client.chunks()
        )

        with SnowflakeLoader(self._sf_cfg) as loader:
            for chunk_num, raw_chunk in enumerate(chunks_iter, start=1):
                logger.info(
                    f"  chunk {chunk_num:>4}: {len(raw_chunk):>9,} raw rows read from CSV"
                )

                raw_chunk = _coerce_numerics(raw_chunk)

                # ── RAW_CLAIMS (full grain — no dedup) ──────────────────────
                claims = build_claims(raw_chunk, year)
                loader.load_table(
                    claims,
                    "RAW_CLAIMS",
                    overwrite=not claims_initialized,
                )
                claims_initialized = True
                total_claims += len(claims)

                # ── RAW_PROVIDERS (cross-chunk dedup via seen_npis) ─────────
                # After _to_snowflake_names(), the NPI column is "RNDRNG_NPI".
                providers = build_providers(raw_chunk, year)
                new_providers = providers[~providers["RNDRNG_NPI"].isin(seen_npis)]
                if not new_providers.empty:
                    loader.load_table(
                        new_providers,
                        "RAW_PROVIDERS",
                        overwrite=not providers_initialized,
                    )
                    seen_npis.update(new_providers["RNDRNG_NPI"].tolist())
                    providers_initialized = True
                    total_providers += len(new_providers)
                else:
                    logger.debug(f"  chunk {chunk_num:>4}: no new providers")

                # ── RAW_PROCEDURES (cross-chunk dedup via seen_hcpcs) ───────
                # After _to_snowflake_names(), the HCPCS code column is "HCPCS_CD".
                procedures = build_procedures(raw_chunk, year)
                new_procedures = procedures[~procedures["HCPCS_CD"].isin(seen_hcpcs)]
                if not new_procedures.empty:
                    loader.load_table(
                        new_procedures,
                        "RAW_PROCEDURES",
                        overwrite=not procedures_initialized,
                    )
                    seen_hcpcs.update(new_procedures["HCPCS_CD"].tolist())
                    procedures_initialized = True
                    total_procedures += len(new_procedures)
                else:
                    logger.debug(f"  chunk {chunk_num:>4}: no new procedures")

        if chunk_num == 0:
            raise RuntimeError(
                f"CSV file produced zero chunks: {self._cms_cfg.csv_path}. "
                f"The file may be empty or have only a header row."
            )

        logger.info(
            f"[{self.name}] load complete —"
            f"  chunks={chunk_num:,}"
            f"  claims={total_claims:,}"
            f"  providers={total_providers:,}"
            f"  procedures={total_procedures:,}"
        )

    # ── BasePipeline ABC stubs ────────────────────────────────────────────────
    # These satisfy the abstract interface but are intentionally never called;
    # all logic lives in _run_chunked(), invoked directly by run().

    def extract(self) -> None:
        raise NotImplementedError(
            "CMSIngestPipeline overrides run() and does not use extract(). "
            "Call pipeline.run() instead."
        )

    def load(self) -> None:
        raise NotImplementedError(
            "CMSIngestPipeline overrides run() and does not use load(). "
            "Call pipeline.run() instead."
        )


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Ingest CMS Medicare Provider Utilisation data into Snowflake RAW schema",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--year",
        type=int,
        default=DEFAULT_YEAR,
        help=(
            "CMS data release year to ingest. "
            "Resolves the CSV path as data/cms_{year}.csv unless --csv-path is given."
        ),
    )
    p.add_argument(
        "--csv-path",
        default=None,
        dest="csv_path",
        help=(
            "Override the CSV file path. "
            "Use when the file is not in the default data/cms_{year}.csv location."
        ),
    )
    p.add_argument(
        "--chunk-rows",
        type=int,
        default=DEFAULT_CHUNK_ROWS,
        dest="chunk_rows",
        help="Rows per pd.read_csv chunk and write_pandas() call (tune for available RAM)",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        dest="log_level",
    )
    return p


def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)

    logger.remove()
    logger.add(sys.stderr, level=args.log_level, colorize=True)

    if args.csv_path:
        cms_cfg = CMSConfig(
            year=args.year,
            csv_path=pathlib.Path(args.csv_path),
            chunk_rows=args.chunk_rows,
        )
    else:
        cms_cfg = CMSConfig.for_year(
            year=args.year,
            chunk_rows=args.chunk_rows,
        )

    pipeline = CMSIngestPipeline(cms_config=cms_cfg)
    pipeline.run()


if __name__ == "__main__":
    main()
