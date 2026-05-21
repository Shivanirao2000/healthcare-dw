-- =============================================================================
-- Healthcare Data Warehouse — Snowflake Setup
-- =============================================================================
-- Run order: execute as SYSADMIN (objects) then SECURITYADMIN (roles/grants).
-- Snowflake separates object ownership from privilege management; mixing the
-- two in a single session with a single role leads to privilege gaps.
-- =============================================================================


-------------------------------------------------------------------------------
-- 0. Session context
-- USE ROLE switches the active role for every statement that follows.
-- In Snowflake, a session has one active role at a time; all DDL is owned by
-- that role. Start with SYSADMIN for infrastructure objects.
-------------------------------------------------------------------------------
USE ROLE SYSADMIN;


-- =============================================================================
-- 1. Virtual Warehouse
-- =============================================================================
-- A "warehouse" in Snowflake is a compute cluster, not a storage concept.
-- Compute and storage are billed independently. The warehouse only consumes
-- credits while actively running queries — AUTO_SUSPEND and AUTO_RESUME let
-- you pay only for what you use.
--
-- SIZE = X-SMALL → 1 server node, suitable for dev/ETL workloads up to ~10 GB.
--   Scale up to SMALL/MEDIUM for large dbt runs or concurrent BI traffic.
-- AUTO_SUSPEND = 60 → warehouse pauses after 60 s of inactivity.
--   Lower values save cost; higher values reduce cold-start latency for
--   dashboards. 60 s is a good default for batch pipelines.
-- AUTO_RESUME = TRUE → warehouse restarts automatically on the next query;
--   no manual intervention or scheduling required.
-- INITIALLY_SUSPENDED → don't spin up credits the moment this script runs.
-- =============================================================================

CREATE WAREHOUSE IF NOT EXISTS HEALTHCARE_WH
    WAREHOUSE_SIZE   = 'X-SMALL'
    AUTO_SUSPEND     = 60
    AUTO_RESUME      = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT          = 'Primary compute for ingestion, dbt transforms, and BI queries';


-- =============================================================================
-- 2. Database and schemas
-- =============================================================================
-- Snowflake databases are logical namespaces; storage is always in Snowflake's
-- managed object store (S3/Azure/GCS under the hood). Creating a database is
-- near-instant and costs nothing until data lands in it.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS HEALTHCARE_DW
    COMMENT = 'Production healthcare data warehouse';

USE DATABASE HEALTHCARE_DW;

-- ── RAW ───────────────────────────────────────────────────────────────────────
-- Landing zone for data exactly as it arrives from source systems.
-- Immutable by convention — no transforms applied here. This preserves an
-- audit trail and allows full replay if downstream logic changes.
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DW.RAW
    COMMENT = 'Immutable landing zone — source data as-delivered, no transforms';

-- ── STAGING ───────────────────────────────────────────────────────────────────
-- dbt staging layer: type-cast, rename, and light-clean RAW tables.
-- Materialised as views or transient tables (no Fail-safe cost).
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DW.STAGING
    COMMENT = 'dbt staging models — cleaned and typed, sourced from RAW';

-- ── MARTS ─────────────────────────────────────────────────────────────────────
-- Conformed dimensional / wide tables consumed by BI tools and analysts.
-- Typically owned by TRANSFORMER, read-only for REPORTER.
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DW.MARTS
    COMMENT = 'dbt mart models — business-ready tables for BI and analytics';


-- =============================================================================
-- 3. Time Travel — RAW schema
-- =============================================================================
-- Time Travel lets you query, clone, or restore any table AS OF a past
-- timestamp without maintaining your own snapshot infrastructure.
-- Snowflake stores micro-partition versions automatically.
--
-- DATA_RETENTION_TIME_IN_DAYS = 7:
--   7 days on RAW is intentional — source files may arrive late or be
--   corrected, and 7 days gives the engineering team a week to detect and
--   re-ingest without needing to re-pull from the source API.
--   After the retention window, data moves to Fail-safe (an additional 7-day
--   Snowflake-managed recovery layer, non-queryable by users).
--
-- Standard (non-Enterprise) accounts cap at 1 day; Enterprise supports 90.
-- Applying it at the schema level sets the default for every table created
-- inside it — individual tables can override with their own ALTER TABLE.
-- =============================================================================

ALTER SCHEMA HEALTHCARE_DW.RAW
    SET DATA_RETENTION_TIME_IN_DAYS = 7;

-- Example: query RAW 3 days ago to compare a re-delivered file
--   SELECT * FROM HEALTHCARE_DW.RAW.CLAIMS AT (OFFSET => -60*60*24*3);


-- =============================================================================
-- 4. Roles and privilege grants
-- =============================================================================
-- Snowflake's RBAC model: roles are granted to users (or other roles).
-- SECURITYADMIN owns role and grant management; never use ACCOUNTADMIN for
-- day-to-day work.
-- =============================================================================

USE ROLE SECURITYADMIN;

-- ── TRANSFORMER ───────────────────────────────────────────────────────────────
-- Intended for: dbt service account, Airflow task runner.
-- Needs: read RAW, full write on STAGING + MARTS.
CREATE ROLE IF NOT EXISTS TRANSFORMER
    COMMENT = 'dbt / Airflow service account role — transforms RAW into STAGING and MARTS';

-- Warehouse usage (required to execute any query)
GRANT USAGE ON WAREHOUSE HEALTHCARE_WH TO ROLE TRANSFORMER;

-- Database + schema traversal
GRANT USAGE ON DATABASE HEALTHCARE_DW               TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA   HEALTHCARE_DW.RAW           TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA   HEALTHCARE_DW.STAGING       TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA   HEALTHCARE_DW.MARTS         TO ROLE TRANSFORMER;

-- RAW: read-only (SELECT + REFERENCES for dbt source tests)
GRANT SELECT, REFERENCES ON ALL TABLES IN SCHEMA HEALTHCARE_DW.RAW TO ROLE TRANSFORMER;
-- Future-grant ensures new RAW tables are automatically accessible
GRANT SELECT, REFERENCES ON FUTURE TABLES IN SCHEMA HEALTHCARE_DW.RAW TO ROLE TRANSFORMER;

-- STAGING + MARTS: full DDL (dbt creates/replaces models on each run)
GRANT ALL PRIVILEGES ON SCHEMA HEALTHCARE_DW.STAGING TO ROLE TRANSFORMER;
GRANT ALL PRIVILEGES ON SCHEMA HEALTHCARE_DW.MARTS   TO ROLE TRANSFORMER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA HEALTHCARE_DW.STAGING TO ROLE TRANSFORMER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA HEALTHCARE_DW.MARTS   TO ROLE TRANSFORMER;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA HEALTHCARE_DW.STAGING TO ROLE TRANSFORMER;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA HEALTHCARE_DW.MARTS   TO ROLE TRANSFORMER;

-- Streams and Tasks (section 5) must also be owned by TRANSFORMER
GRANT CREATE STREAM ON SCHEMA HEALTHCARE_DW.RAW TO ROLE TRANSFORMER;
GRANT CREATE TASK   ON SCHEMA HEALTHCARE_DW.RAW TO ROLE TRANSFORMER;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE TRANSFORMER;

-- ── REPORTER ──────────────────────────────────────────────────────────────────
-- Intended for: Power BI service principal, Looker, ad-hoc analyst users.
-- Needs: read MARTS only. No access to raw PHI in RAW or intermediate STAGING.
-- Principle of least privilege — analysts never need to query raw claims rows.
CREATE ROLE IF NOT EXISTS REPORTER
    COMMENT = 'BI tools and analyst read-only access to MARTS';

GRANT USAGE ON WAREHOUSE HEALTHCARE_WH            TO ROLE REPORTER;
GRANT USAGE ON DATABASE HEALTHCARE_DW             TO ROLE REPORTER;
GRANT USAGE ON SCHEMA   HEALTHCARE_DW.MARTS       TO ROLE REPORTER;
GRANT SELECT ON ALL TABLES IN SCHEMA HEALTHCARE_DW.MARTS   TO ROLE REPORTER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA HEALTHCARE_DW.MARTS TO ROLE REPORTER;
-- Views in MARTS (e.g. row-level security wrappers) are also readable
GRANT SELECT ON ALL VIEWS IN SCHEMA HEALTHCARE_DW.MARTS    TO ROLE REPORTER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA HEALTHCARE_DW.MARTS TO ROLE REPORTER;

-- ── Role hierarchy ────────────────────────────────────────────────────────────
-- SYSADMIN inherits both operational roles so a human admin can act as either
-- without switching to a lower role. Never grant SYSADMIN to service accounts.
USE ROLE SECURITYADMIN;
GRANT ROLE TRANSFORMER TO ROLE SYSADMIN;
GRANT ROLE REPORTER    TO ROLE SYSADMIN;


-- =============================================================================
-- 5. Stream on RAW.CLAIMS + hourly Task
-- =============================================================================
-- Switch back to SYSADMIN (owns the database) and then act as TRANSFORMER
-- so the stream/task are owned by the correct role.
-- =============================================================================

USE ROLE SYSADMIN;

-- The target table must exist before the stream can be created.
-- This is the minimal DDL for the raw claims landing table; expand columns
-- to match your actual source schema.
CREATE TABLE IF NOT EXISTS HEALTHCARE_DW.RAW.CLAIMS (
    claim_id          VARCHAR        NOT NULL,
    member_id         VARCHAR        NOT NULL,
    provider_npi      VARCHAR,
    service_date      DATE,
    icd10_primary     VARCHAR,
    billed_amount     NUMBER(12, 2),
    allowed_amount    NUMBER(12, 2),
    paid_amount       NUMBER(12, 2),
    claim_status      VARCHAR,
    source_file_name  VARCHAR,
    loaded_at         TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw claims rows as delivered by payer — append-only, never updated in place';

-- ── Stream ────────────────────────────────────────────────────────────────────
-- A Stream is a CDC (change-data-capture) object. It tracks the offset of the
-- last time the stream was consumed and surfaces only the *new* micro-partitions
-- since that offset as METADATA$ACTION / METADATA$ISUPDATE / METADATA$ROW_ID.
--
-- APPEND_ONLY = TRUE:
--   Claims are always INSERT-only from the source; we don't expect UPDATEs or
--   DELETEs on raw data. APPEND_ONLY streams are cheaper — Snowflake only
--   tracks inserts, not full row versions, so storage overhead is minimal.
--   Use a standard stream (omit APPEND_ONLY) if your source can send updates.
--
-- The stream advances its offset only when a consuming DML statement
-- (INSERT INTO … SELECT … FROM <stream>) commits inside a transaction.
-- A failed or rolled-back transaction does NOT advance the offset.
-- =============================================================================

USE ROLE TRANSFORMER;
USE WAREHOUSE HEALTHCARE_WH;

CREATE STREAM IF NOT EXISTS HEALTHCARE_DW.RAW.CLAIMS_STREAM
    ON TABLE HEALTHCARE_DW.RAW.CLAIMS
    APPEND_ONLY = TRUE
    COMMENT     = 'CDC stream — surfaces new claim rows since last Task run';

-- ── Task ──────────────────────────────────────────────────────────────────────
-- A Task is a scheduled SQL statement (or stored-procedure call) managed by
-- Snowflake's internal scheduler. No external orchestrator needed for simple
-- micro-batch patterns.
--
-- SCHEDULE = '60 MINUTE':
--   Fires every hour. Adjust to '5 MINUTE' for near-real-time or to a CRON
--   expression for business-hours-only processing:
--   SCHEDULE = 'USING CRON 0 6-20 * * MON-FRI America/New_York'
--
-- WHEN SYSTEM$STREAM_HAS_DATA('...CLAIMS_STREAM'):
--   The WHEN clause is a pre-flight guard. If the stream is empty (no new
--   claims arrived since the last run), the task skips execution entirely and
--   charges zero credits. Without this guard, the warehouse would spin up
--   every hour even for empty runs — a common cost leak.
--
-- USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE:
--   Alternative to WAREHOUSE = ... — Snowflake spins up a serverless compute
--   cluster sized to the workload. Useful when task runtime is unpredictable.
--   We use an explicit warehouse here for cost predictability.
--
-- Tasks start in SUSPENDED state; the final RESUME statement activates them.
-- =============================================================================

CREATE TASK IF NOT EXISTS HEALTHCARE_DW.RAW.PROCESS_CLAIMS_TASK
    WAREHOUSE = HEALTHCARE_WH
    SCHEDULE  = '60 MINUTE'
    WHEN      SYSTEM$STREAM_HAS_DATA('HEALTHCARE_DW.RAW.CLAIMS_STREAM')
    COMMENT   = 'Hourly micro-batch: merge new raw claims into STAGING.CLAIMS'
AS
    -- Merge new stream rows into the staging table.
    -- METADATA$ACTION filters pure inserts from the stream (always 'INSERT'
    -- for an APPEND_ONLY stream, but explicit for clarity and safety).
    MERGE INTO HEALTHCARE_DW.STAGING.CLAIMS AS tgt
    USING (
        SELECT
            claim_id,
            member_id,
            provider_npi,
            service_date,
            icd10_primary,
            billed_amount,
            allowed_amount,
            paid_amount,
            claim_status,
            source_file_name,
            loaded_at,
            METADATA$ACTION      AS _stream_action,
            METADATA$ROW_ID      AS _stream_row_id
        FROM HEALTHCARE_DW.RAW.CLAIMS_STREAM
        WHERE METADATA$ACTION = 'INSERT'
    ) AS src
        ON tgt.claim_id = src.claim_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.claim_status   = src.claim_status,
            tgt.paid_amount    = src.paid_amount,
            tgt.loaded_at      = src.loaded_at
    WHEN NOT MATCHED THEN
        INSERT (
            claim_id, member_id, provider_npi, service_date,
            icd10_primary, billed_amount, allowed_amount, paid_amount,
            claim_status, source_file_name, loaded_at
        )
        VALUES (
            src.claim_id, src.member_id, src.provider_npi, src.service_date,
            src.icd10_primary, src.billed_amount, src.allowed_amount, src.paid_amount,
            src.claim_status, src.source_file_name, src.loaded_at
        );

-- Activate the task. Tasks are created SUSPENDED by default — an intentional
-- safety gate so you can inspect and test before enabling live scheduling.
ALTER TASK HEALTHCARE_DW.RAW.PROCESS_CLAIMS_TASK RESUME;


-- =============================================================================
-- 6. Zero-copy clone — DEV environment
-- =============================================================================
-- IMPORTANT: Do NOT run this block automatically.  Execute it manually:
--   • Once per developer who needs an isolated sandbox
--   • After a prod data refresh (re-clone to sync)
--   • Never on every deployment — it is idempotent but not free if repeated
--     rapidly (micro-partition metadata is re-written each time)
--
-- Zero-copy clone creates a new database that shares the *same underlying
-- micro-partitions* as the source at the moment of cloning. No data is
-- physically copied — only metadata pointers are duplicated. The clone is
-- immediately queryable and starts at zero additional storage cost.
--
-- Storage diverges only as the clone is modified (copy-on-write): developers
-- can INSERT, UPDATE, and DELETE freely without touching production data.
-- Cloning a multi-TB warehouse typically takes < 5 seconds.
--
-- Use cases:
--   1. Developer sandboxes — each engineer gets their own DEV_<name>_DW
--   2. Blue/green dbt runs — clone prod, run dbt, validate, then swap
--   3. QA / staging environments that need production-realistic data volumes
--   4. Incident forensics — clone prod before a hotfix to preserve the state
--
-- To run: uncomment the block below, fill in <DEVELOPER_NAME>, and execute as
-- SYSADMIN.  Then grant the developer's role USAGE on the new database.
-- =============================================================================

/*
USE ROLE SYSADMIN;

CREATE DATABASE DEV_HEALTHCARE_DW
    CLONE HEALTHCARE_DW
    COMMENT = 'Zero-copy dev clone of HEALTHCARE_DW — safe to modify freely';

-- Give the developer full ownership of their clone
GRANT OWNERSHIP ON DATABASE DEV_HEALTHCARE_DW TO ROLE TRANSFORMER;

-- Optional: grant the developer's personal role usage on the clone
-- GRANT USAGE ON DATABASE DEV_HEALTHCARE_DW TO ROLE <DEVELOPER_ROLE>;
-- GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE DEV_HEALTHCARE_DW TO ROLE <DEVELOPER_ROLE>;
-- GRANT ALL PRIVILEGES ON ALL TABLES  IN DATABASE DEV_HEALTHCARE_DW TO ROLE <DEVELOPER_ROLE>;
*/


-- =============================================================================
-- 7. Verification queries
-- =============================================================================
-- Run these after setup to confirm everything provisioned correctly.
-- =============================================================================

-- Show all objects created in this script
SHOW WAREHOUSES  LIKE 'HEALTHCARE_WH';
SHOW DATABASES   LIKE 'HEALTHCARE_DW';
SHOW SCHEMAS     IN DATABASE HEALTHCARE_DW;
SHOW ROLES       LIKE 'TRANSFORMER';
SHOW ROLES       LIKE 'REPORTER';
SHOW STREAMS     IN SCHEMA HEALTHCARE_DW.RAW;
SHOW TASKS       IN SCHEMA HEALTHCARE_DW.RAW;

-- Confirm Time Travel setting on RAW schema
SELECT catalog_name, schema_name, retention_time
FROM   HEALTHCARE_DW.INFORMATION_SCHEMA.SCHEMATA
WHERE  schema_name = 'RAW';

-- Confirm stream offset and staleness (stale = stream not consumed within retention window)
SELECT system$stream_get_table_timestamp('HEALTHCARE_DW.RAW.CLAIMS_STREAM') AS stream_offset_ts,
       system$stream_has_data('HEALTHCARE_DW.RAW.CLAIMS_STREAM')            AS has_unprocessed_rows;
