"""
Snowflake DDL definitions for all RAW schema tables populated by cms_ingest.py.

Column names are the exact uppercase versions produced by _to_snowflake_names()
in cms_ingest.py (e.g. "Rndrng_NPI" → "RNDRNG_NPI"). Data types are chosen
to match the CMS data dictionary precisely — do not widen VARCHAR lengths or
relax NOT NULL constraints without updating both the ingestion transform and
any downstream dbt staging models that depend on these shapes.

Types reference:
  VARCHAR(n)    — fixed-max string; sized to CMS data dictionary maximums
  NUMBER(p,s)   — Snowflake FIXED-precision decimal; no floating-point error
  DATE          — calendar date, no time component
  BOOLEAN       — not used here; CMS 'Y'/'N' flags are kept as VARCHAR(1)
                  so NULL vs 'N' remain distinguishable downstream

Primary key constraints are informational only in Snowflake (not enforced),
but they document the intended grain and drive dbt's uniqueness tests.

Usage:
    from ingestion.schema_definitions import ALL_TABLES, render
    sql = render(ALL_TABLES[0].ddl, database="HEALTHCARE_DW", schema="RAW")
"""

from __future__ import annotations

from typing import NamedTuple


class TableSchema(NamedTuple):
    """Pairs a table name with its CREATE TABLE DDL template."""
    name: str         # bare table name, no schema prefix
    ddl: str          # DDL string; use render() to substitute {database}/{schema}
    description: str  # one-line description of the table grain


def render(ddl: str, *, database: str, schema: str) -> str:
    """Substitute {database} and {schema} placeholders in a DDL template."""
    return ddl.format(database=database, schema=schema)


# ─────────────────────────────────────────────────────────────────────────────
# RAW_PROVIDERS
# Grain: one row per rendering provider NPI per CMS data year.
# Source columns: PROVIDER_COLS in cms_ingest.py, then uppercased.
# Deduplicated in build_providers() — the source file has one row per
# NPI × HCPCS pair, so the same NPI appears thousands of times in the raw
# download. Only the first occurrence per NPI is kept here.
# ─────────────────────────────────────────────────────────────────────────────

DDL_RAW_PROVIDERS = """\
CREATE TABLE IF NOT EXISTS {database}.{schema}.RAW_PROVIDERS (

    -- ── Identity ────────────────────────────────────────────────────────────
    RNDRNG_NPI                      VARCHAR(10)     NOT NULL
        COMMENT 'National Provider Identifier — 10-digit CMS-assigned unique ID; \
stored as string because it is an opaque key, not a quantity',

    -- ── Name ────────────────────────────────────────────────────────────────
    -- For individuals: last name. For organisations: full legal name.
    RNDRNG_PRVDR_LAST_ORG_NAME      VARCHAR(70)
        COMMENT 'Last name (individual) or organisation legal name',
    RNDRNG_PRVDR_FIRST_NAME         VARCHAR(20)
        COMMENT 'First name; blank for organisation providers',
    RNDRNG_PRVDR_MI                 VARCHAR(1)
        COMMENT 'Middle initial; blank for organisation providers',
    RNDRNG_PRVDR_CRDNTLS            VARCHAR(20)
        COMMENT 'Provider credentials as reported (e.g. MD, DO, NP)',

    -- ── Demographics ────────────────────────────────────────────────────────
    RNDRNG_PRVDR_GNDR               VARCHAR(1)
        COMMENT 'Gender: M = Male, F = Female, blank = not reported or organisation',
    RNDRNG_PRVDR_ENT_CD             VARCHAR(1)
        COMMENT 'Entity code: I = Individual provider, O = Organisation',

    -- ── Address ─────────────────────────────────────────────────────────────
    -- CMS address fields reflect the provider''s primary practice location.
    RNDRNG_PRVDR_ST1                VARCHAR(55)
        COMMENT 'Street address line 1',
    RNDRNG_PRVDR_ST2                VARCHAR(55)
        COMMENT 'Street address line 2 (suite, floor, etc.)',
    RNDRNG_PRVDR_CITY               VARCHAR(40)
        COMMENT 'City',
    RNDRNG_PRVDR_STATE_ABRVTN       VARCHAR(2)
        COMMENT 'US state or territory abbreviation (e.g. CA, NY, PR)',
    RNDRNG_PRVDR_STATE_FIPS         VARCHAR(2)
        COMMENT 'Two-digit FIPS state code; stored as string to preserve leading zeros (e.g. 06 = CA)',
    -- ZIP stored as string: leading zeros are meaningful (e.g. 02101 = Boston)
    RNDRNG_PRVDR_ZIP5               VARCHAR(5)
        COMMENT '5-digit ZIP code; string to preserve leading zeros',
    -- RUCA = Rural-Urban Commuting Area code (1.0 urban core … 10.6 rural)
    -- Nullable: blank for overseas providers and US territories.
    RNDRNG_PRVDR_RUCA               NUMBER(4,1)
        COMMENT 'RUCA code — measures rural/urban classification of the practice location; \
NULL for providers outside the continental US',
    RNDRNG_PRVDR_RUCA_DESC          VARCHAR(100)
        COMMENT 'Human-readable RUCA description corresponding to RNDRNG_PRVDR_RUCA',
    RNDRNG_PRVDR_CNTRY              VARCHAR(40)
        COMMENT 'Country; most rows are blank (US) or a 2-letter ISO code for foreign providers',

    -- ── Practice ────────────────────────────────────────────────────────────
    RNDRNG_PRVDR_TYPE               VARCHAR(55)
        COMMENT 'Provider specialty type as self-reported to Medicare (e.g. Internal Medicine)',
    RNDRNG_PRVDR_MDCR_PRTCPTG_IND  VARCHAR(1)
        COMMENT 'Medicare participation indicator: Y = accepts Medicare assignment, N = does not',

    -- ── Pipeline metadata ───────────────────────────────────────────────────
    LOAD_DATE                       DATE            NOT NULL
        COMMENT 'Calendar date on which this pipeline run loaded the row into Snowflake',
    DATA_YEAR                       NUMBER(4,0)     NOT NULL
        COMMENT 'CMS data release year this row belongs to (e.g. 2022)',

    CONSTRAINT pk_raw_providers PRIMARY KEY (RNDRNG_NPI, DATA_YEAR)
)
COMMENT = 'One row per rendering provider NPI per CMS data year. \
Source: CMS Medicare Physician & Other Practitioners by Provider and Service. \
Grain is deduplicated from the raw download which repeats each NPI once per HCPCS code billed.'
DATA_RETENTION_TIME_IN_DAYS = 7\
"""

# ─────────────────────────────────────────────────────────────────────────────
# RAW_PROCEDURES
# Grain: one row per HCPCS procedure code per CMS data year.
# Source columns: PROCEDURE_COLS in cms_ingest.py, then uppercased.
# Deduplicated in build_procedures() — same HCPCS code appears once per
# provider in the source, so deduplicated here to a reference table.
# ─────────────────────────────────────────────────────────────────────────────

DDL_RAW_PROCEDURES = """\
CREATE TABLE IF NOT EXISTS {database}.{schema}.RAW_PROCEDURES (

    -- ── Identity ────────────────────────────────────────────────────────────
    -- HCPCS (Healthcare Common Procedure Coding System) codes are always
    -- exactly 5 characters: one letter followed by 4 digits for Level II
    -- (e.g. A0425), or 5 digits for CPT codes embedded in Level I (e.g. 99213).
    HCPCS_CD                        VARCHAR(5)      NOT NULL
        COMMENT 'HCPCS procedure code — 5-character alphanumeric identifier',

    -- ── Description ─────────────────────────────────────────────────────────
    HCPCS_DESC                      VARCHAR(256)
        COMMENT 'Short descriptor for the HCPCS code as published by CMS',

    -- ── Classification ──────────────────────────────────────────────────────
    -- Drug indicator separates drug/biologic claims from procedure claims.
    -- Relevant downstream for cost benchmarking and formulary analysis.
    HCPCS_DRUG_IND                  VARCHAR(1)
        COMMENT 'Drug/biologic indicator: Y = claim is for a drug or biologic, N = procedure',

    -- ── Pipeline metadata ───────────────────────────────────────────────────
    LOAD_DATE                       DATE            NOT NULL
        COMMENT 'Calendar date on which this pipeline run loaded the row into Snowflake',
    DATA_YEAR                       NUMBER(4,0)     NOT NULL
        COMMENT 'CMS data release year this row belongs to (e.g. 2022)',

    CONSTRAINT pk_raw_procedures PRIMARY KEY (HCPCS_CD, DATA_YEAR)
)
COMMENT = 'One row per HCPCS procedure code per CMS data year. \
Source: CMS Medicare Physician & Other Practitioners by Provider and Service. \
Grain is deduplicated from the raw download which repeats each HCPCS code once per provider who billed it.'
DATA_RETENTION_TIME_IN_DAYS = 7\
"""

# ─────────────────────────────────────────────────────────────────────────────
# RAW_CLAIMS
# Grain: one row per NPI × HCPCS_CD × PLACE_OF_SRVC per CMS data year.
# Source columns: CLAIMS_COLS in cms_ingest.py, then uppercased.
# This is the fact table — all payment and utilisation metrics live here.
# No deduplication applied; this is the native grain of the CMS dataset.
#
# CMS suppression rule: rows where TOT_BENES < 11 are removed by CMS before
# publication to protect beneficiary privacy. NULL values in count/amount
# columns indicate CMS applied additional suppression (e.g. for small cells
# in drug claims). Downstream models must handle NULLs accordingly.
# ─────────────────────────────────────────────────────────────────────────────

DDL_RAW_CLAIMS = """\
CREATE TABLE IF NOT EXISTS {database}.{schema}.RAW_CLAIMS (

    -- ── Composite key ────────────────────────────────────────────────────────
    RNDRNG_NPI                      VARCHAR(10)     NOT NULL
        COMMENT 'Rendering provider NPI — foreign key to RAW_PROVIDERS.RNDRNG_NPI',
    HCPCS_CD                        VARCHAR(5)      NOT NULL
        COMMENT 'HCPCS procedure code — foreign key to RAW_PROCEDURES.HCPCS_CD',
    -- Place of service splits the same HCPCS into facility vs. non-facility
    -- reimbursement tracks. A single provider-HCPCS pair can have up to two
    -- rows here (one F, one O), each with different allowed amounts.
    PLACE_OF_SRVC                   VARCHAR(1)
        COMMENT 'Place of service: F = Facility (hospital/ASC), O = Office/non-facility',

    -- ── Volume ──────────────────────────────────────────────────────────────
    -- TOT_BENES: distinct beneficiaries served; CMS suppresses rows < 11.
    TOT_BENES                       NUMBER(10,0)
        COMMENT 'Distinct Medicare beneficiaries who received this service; \
CMS removes rows where this value would be fewer than 11',
    -- TOT_SRVCS: aggregate units of service, which CMS rounds to 2 decimal
    -- places to further protect small counts. Can be fractional.
    TOT_SRVCS                       NUMBER(12,2)
        COMMENT 'Total units of service billed (rounded); may be fractional due to CMS rounding',
    TOT_BENE_DAY_SRVCS              NUMBER(12,2)
        COMMENT 'Total beneficiary-day services (distinct beneficiary × service-date combinations)',

    -- ── Payment ─────────────────────────────────────────────────────────────
    -- All amounts are averages across beneficiaries for this NPI × HCPCS × POS.
    -- NULL indicates CMS applied additional privacy suppression beyond the
    -- 11-beneficiary threshold (common for high-cost drug claims).
    AVG_SBMTD_CHRG                  NUMBER(12,2)
        COMMENT 'Average charge amount submitted by the provider to Medicare',
    AVG_MDCR_ALOWD_AMT              NUMBER(12,2)
        COMMENT 'Average Medicare allowed amount (fee schedule or negotiated rate)',
    AVG_MDCR_PYMT_AMT               NUMBER(12,2)
        COMMENT 'Average Medicare payment to provider after cost-sharing deductions',
    -- Standardised amount removes geographic payment adjustments (GPCI) so
    -- you can compare utilisation across markets on a level playing field.
    AVG_MDCR_STDZD_AMT              NUMBER(12,2)
        COMMENT 'Average Medicare standardised payment — geographic adjustments removed; \
use this column for cross-market cost comparisons, not AVG_MDCR_PYMT_AMT',

    -- ── Pipeline metadata ───────────────────────────────────────────────────
    LOAD_DATE                       DATE            NOT NULL
        COMMENT 'Calendar date on which this pipeline run loaded the row into Snowflake',
    DATA_YEAR                       NUMBER(4,0)     NOT NULL
        COMMENT 'CMS data release year this row belongs to (e.g. 2022)',

    CONSTRAINT pk_raw_claims
        PRIMARY KEY (RNDRNG_NPI, HCPCS_CD, PLACE_OF_SRVC, DATA_YEAR)
)
COMMENT = 'One row per rendering provider × HCPCS code × place of service per CMS data year. \
Source: CMS Medicare Physician & Other Practitioners by Provider and Service. \
This is the primary fact table; join to RAW_PROVIDERS on RNDRNG_NPI and to \
RAW_PROCEDURES on HCPCS_CD. Rows with fewer than 11 distinct beneficiaries are \
pre-suppressed by CMS before publication.'
DATA_RETENTION_TIME_IN_DAYS = 7\
"""


# ─────────────────────────────────────────────────────────────────────────────
# Ordered table registry
# ─────────────────────────────────────────────────────────────────────────────

# Execution order matters: RAW_CLAIMS logically references the two reference
# tables above, so dimension tables are created first. Snowflake does not
# enforce FK constraints but maintaining this order documents the dependency
# and keeps --drop-first teardowns safe (drop in reverse, create in forward).

ALL_TABLES: tuple[TableSchema, ...] = (
    TableSchema(
        name="RAW_PROVIDERS",
        ddl=DDL_RAW_PROVIDERS,
        description="One row per NPI per data year — provider demographics and practice location",
    ),
    TableSchema(
        name="RAW_PROCEDURES",
        ddl=DDL_RAW_PROCEDURES,
        description="One row per HCPCS code per data year — procedure code reference",
    ),
    TableSchema(
        name="RAW_CLAIMS",
        ddl=DDL_RAW_CLAIMS,
        description="One row per NPI × HCPCS × place-of-service per data year — utilisation and payment metrics",
    ),
)
