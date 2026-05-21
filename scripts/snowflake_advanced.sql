-- =============================================================================
-- Snowflake Advanced Features — Healthcare DW
-- =============================================================================
-- Every section in this file is a standalone runnable block.  Execute them
-- individually in a Snowflake worksheet; you do not need to run the whole file.
--
-- Prerequisites: snowflake_setup.sql has already been executed, so the
-- following objects exist:
--   HEALTHCARE_DW (database)
--   HEALTHCARE_DW.RAW, .STAGING, .MARTS (schemas)
--   HEALTHCARE_DW.RAW.RAW_CLAIMS (table, 7-day Time Travel)
--   HEALTHCARE_WH (X-SMALL virtual warehouse)
--   TRANSFORMER, REPORTER (roles)
--
-- Interview context: these four features come up constantly in Snowflake
-- data-engineer interviews.  For each one, the comments answer:
--   WHAT   — what the feature is at the SQL level
--   WHY    — the architectural problem it solves
--   HOW    — the internal mechanism (micro-partitions, offsets, etc.)
--   GOTCHA — the edge case or cost implication interviewers probe for
-- =============================================================================


-- =============================================================================
-- SECTION 1 — TIME TRAVEL
-- =============================================================================
-- WHAT: Time Travel lets you query, clone, or restore any table as it existed
--       at a past point in time — without any backup infrastructure.
--
-- WHY:  In traditional data warehouses you need expensive snapshot jobs to
--       answer "what did this table look like yesterday?".  In Snowflake every
--       DML operation is recorded implicitly via immutable micro-partitions.
--
-- HOW:  Snowflake never overwrites micro-partitions in place.  When a row is
--       updated or deleted, the old partitions are marked inactive but retained
--       until the retention window expires.  AT / BEFORE clauses tell the query
--       engine which partition generation to read.
--
-- GOTCHA: Retention has a storage cost — each retained partition version is
--         billed like live data.  A 7-day window on a table that changes
--         frequently can cost 2–8x its base storage.  Set retention per table,
--         not globally, to control this.  Also: Fail-safe (7 more days after
--         retention) is Snowflake-managed; you cannot query it, only ask
--         Snowflake support to restore.
-- =============================================================================

USE ROLE    SYSADMIN;
USE WAREHOUSE HEALTHCARE_WH;
USE DATABASE  HEALTHCARE_DW;


-- ── 1a. Current state — baseline for comparison ───────────────────────────────
SELECT
    DATA_YEAR,
    COUNT(*)                         AS row_count,
    MAX(LOAD_DATE)                   AS latest_load_date,
    ROUND(SUM(AVG_MDCR_PYMT_AMT
              * TOT_SRVCS) / 1e6, 2) AS total_medicare_payment_millions
FROM RAW.RAW_CLAIMS
GROUP BY DATA_YEAR
ORDER BY DATA_YEAR;


-- ── 1b. AT (OFFSET => -86400): query as of exactly 24 hours ago ───────────────
--
-- OFFSET is in seconds from the current wall-clock time.
-- -86400 = -1 day.  Snowflake resolves this to the precise micro-partition
-- state that was current at (NOW() - 86400 seconds).
--
-- Why use OFFSET instead of a timestamp?
-- OFFSET is timezone-agnostic — no daylight-saving ambiguity.
-- TIMESTAMP is useful when you know the exact moment (e.g. before a bad merge).
-- STATEMENT is useful when you know the Snowflake query ID of the bad DML.
--
-- Interview question: "How would you compare yesterday's payment totals
-- against today's to detect a data drift after a reload?"
SELECT
    DATA_YEAR,
    COUNT(*)                         AS row_count_yesterday,
    ROUND(SUM(AVG_MDCR_PYMT_AMT
              * TOT_SRVCS) / 1e6, 2) AS total_payment_yesterday_millions
FROM RAW.RAW_CLAIMS
    AT (OFFSET => -86400)            -- 24 hours ago in seconds
GROUP BY DATA_YEAR
ORDER BY DATA_YEAR;


-- ── 1c. AT (TIMESTAMP => ...): query at an exact calendar moment ──────────────
--
-- Use case: an analyst reports a dashboard showed wrong numbers at 2 AM last
-- night.  You know the bad dbt run started at 01:58 UTC.  Query what the table
-- looked like at 01:57 — before the run touched it.
SELECT COUNT(*), MAX(LOAD_DATE)
FROM RAW.RAW_CLAIMS
    AT (TIMESTAMP => '2024-01-15 01:57:00'::TIMESTAMP_NTZ);


-- ── 1d. AT (STATEMENT => ...): restore to just before a specific query ran ────
--
-- Every Snowflake query has a unique QUERY_ID.  If you ran a bad DELETE,
-- AT(STATEMENT => '<bad_query_id>') gives you the state immediately before
-- that statement committed — without needing to know the exact timestamp.
--
-- Find recent query IDs:
SELECT QUERY_ID, QUERY_TEXT, START_TIME, ROWS_PRODUCED
FROM TABLE(
    INFORMATION_SCHEMA.QUERY_HISTORY(
        DATE_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
        RESULT_LIMIT     => 20
    )
)
WHERE QUERY_TEXT ILIKE '%RAW_CLAIMS%'
ORDER BY START_TIME DESC;

-- Then use the ID from the result above:
-- SELECT * FROM RAW.RAW_CLAIMS AT (STATEMENT => '<query_id_from_above>');


-- ── 1e. Side-by-side diff: detect rows added by last night's ingest ───────────
--
-- A common audit pattern: MINUS finds rows present today but not yesterday.
-- INTERSECT would find rows unchanged across both snapshots.
SELECT RNDRNG_NPI, HCPCS_CD, DATA_YEAR, LOAD_DATE
FROM   RAW.RAW_CLAIMS                       -- current state
MINUS
SELECT RNDRNG_NPI, HCPCS_CD, DATA_YEAR, LOAD_DATE
FROM   RAW.RAW_CLAIMS AT (OFFSET => -86400) -- yesterday's state
LIMIT 100;


-- ── 1f. Verify the retention window set by snowflake_setup.sql ────────────────
-- snowflake_setup.sql ran: ALTER SCHEMA RAW SET DATA_RETENTION_TIME_IN_DAYS = 7
-- That schema-level setting propagates to every table created inside RAW.
-- Individual tables can override it: ALTER TABLE RAW_CLAIMS SET DATA_RETENTION_TIME_IN_DAYS = 1
SELECT
    CATALOG_NAME                AS database_name,
    SCHEMA_NAME,
    RETENTION_TIME              AS time_travel_days,
    -- Fail-safe adds 7 more days after retention (Enterprise accounts only).
    -- Those 7 days are Snowflake-managed; you cannot query them directly.
    RETENTION_TIME + 7          AS total_recovery_window_days
FROM INFORMATION_SCHEMA.SCHEMATA
WHERE SCHEMA_NAME = 'RAW';

-- Check the specific retention on RAW_CLAIMS (can differ from schema default):
SHOW TABLES LIKE 'RAW_CLAIMS' IN SCHEMA RAW;
-- Look for the RETENTION_TIME column in the output.


-- ── 1g. UNDROP: recover a dropped table ──────────────────────────────────────
--
-- WHAT: UNDROP TABLE restores the most recently dropped version of a table,
--       including all its data, within the Time Travel retention window.
--
-- WHY:  Accidental DROP TABLE is one of the most common data incidents.
--       In traditional warehouses this requires a full backup restore (hours).
--       In Snowflake it is a single SQL statement (seconds).
--
-- HOW:  DROP TABLE marks the table as deleted and releases its name, but the
--       underlying micro-partitions stay in the retention zone.  UNDROP
--       re-attaches those partitions to a restored table definition.
--
-- GOTCHA: UNDROP only works within the retention window.  After 7 days (for
--         our RAW schema) the micro-partitions are purged and you need
--         Snowflake support to attempt a Fail-safe recovery.  Also: if a new
--         table with the same name was created after the drop, UNDROP restores
--         to a name like RAW_CLAIMS_<timestamp> — always check SHOW TABLES HISTORY.
--
-- ── Step-by-step demo (safe to run — the UNDROP restores everything) ──────────

-- Step 1: Record current row count so we can verify after restore.
SELECT COUNT(*) AS rows_before_drop FROM RAW.RAW_CLAIMS;

-- Step 2: Simulate an accidental drop.
DROP TABLE RAW.RAW_CLAIMS;

-- Step 3: Confirm the table is gone from the live catalog.
SHOW TABLES LIKE 'RAW_CLAIMS' IN SCHEMA RAW;
-- The table should NOT appear here.

-- Step 4: But it IS visible in the history catalog with DROPPED_ON populated.
SHOW TABLES HISTORY LIKE 'RAW_CLAIMS' IN SCHEMA RAW;
-- Look for DROPPED_ON timestamp — this confirms the object is in Time Travel.

-- Step 5: Restore the table in one statement.
UNDROP TABLE RAW.RAW_CLAIMS;

-- Step 6: Verify full data recovery — count must match Step 1.
SELECT COUNT(*) AS rows_after_undrop FROM RAW.RAW_CLAIMS;

-- Step 7: Confirm retention setting survived the restore.
SHOW TABLES LIKE 'RAW_CLAIMS' IN SCHEMA RAW;
-- RETENTION_TIME should still be 7.


-- =============================================================================
-- SECTION 2 — ZERO-COPY CLONE
-- =============================================================================
-- WHAT: CREATE ... CLONE creates a full, independent copy of a database,
--       schema, or table that initially shares zero additional storage.
--
-- WHY:  Traditional environments create dev/QA environments by copying data
--       (slow, expensive).  A zero-copy clone gives you production-scale data
--       instantly at no initial storage cost, so developers can run dbt locally
--       against real data volumes without a separate ETL process to populate QA.
--
-- HOW:  Snowflake's storage layer is immutable micro-partitions managed
--       like a linked list.  A clone is a new metadata entry that points to the
--       same micro-partition files as the source.  Both the original and the
--       clone read those shared files.  When either modifies data, Snowflake
--       writes NEW partitions (copy-on-write) — the shared originals remain
--       unchanged.  Storage cost only diverges proportionally to modifications.
--
-- GOTCHA: A clone is NOT free forever.  Every INSERT, UPDATE, DELETE, or MERGE
--         against the clone writes new micro-partitions billed only to the clone.
--         A developer who runs a full dbt refresh on their clone adds storage
--         equal to the modified tables.  Monitor with:
--         SELECT * FROM INFORMATION_SCHEMA.DATABASE_STORAGE_USAGE_HISTORY;
-- =============================================================================

USE ROLE SYSADMIN;


-- ── 2a. Clone the entire production database for a developer sandbox ──────────
--
-- This completes in < 5 seconds regardless of the database size.
-- The clone is identical to prod at this instant — all schemas, tables,
-- views, and stages are included.  Streams and Tasks are NOT cloned.
CREATE DATABASE IF NOT EXISTS DEV_HEALTHCARE_DW
    CLONE HEALTHCARE_DW
    COMMENT = 'Developer sandbox — zero-copy clone of HEALTHCARE_DW at clone time';

-- Grant the developer full control over their sandbox:
USE ROLE SECURITYADMIN;
GRANT OWNERSHIP ON DATABASE DEV_HEALTHCARE_DW TO ROLE TRANSFORMER;
-- GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE DEV_HEALTHCARE_DW TO ROLE <dev_role>;
-- GRANT ALL PRIVILEGES ON ALL TABLES  IN DATABASE DEV_HEALTHCARE_DW TO ROLE <dev_role>;
USE ROLE SYSADMIN;


-- ── 2b. Clone a single table — cheaper for targeted dev work ─────────────────
--
-- Cloning at the table level is faster to grant and scoped to one object.
-- Useful when a developer only needs to experiment with one dataset.
CREATE TABLE IF NOT EXISTS RAW.RAW_CLAIMS_BACKUP
    CLONE RAW.RAW_CLAIMS
    COMMENT = 'Pre-migration backup — cloned before schema change on 2024-01-15';

-- Initial storage cost of this clone: $0 (shares all micro-partitions with prod).
-- Cost diverges only if RAW_CLAIMS_BACKUP or RAW_CLAIMS is subsequently modified.


-- ── 2c. Clone + Time Travel: "as of yesterday" clone ─────────────────────────
--
-- Combine CLONE with AT to create a clone of the table as it was at a past
-- point in time.  Use case: a bug was introduced in yesterday's CMS load.
-- Clone the table as it was before the bad load so analysts can compare.
--
-- Interview talking point: "You can combine two Snowflake features — cloning
-- gives you an independent copy, Time Travel lets you pick the source point in
-- time — without any ETL."
CREATE TABLE IF NOT EXISTS RAW.RAW_CLAIMS_BEFORE_BAD_LOAD
    CLONE RAW.RAW_CLAIMS
    AT    (OFFSET => -86400)   -- as of 24 hours ago
    COMMENT = 'Pre-bad-load snapshot for forensic comparison';

-- Query the clone alongside prod to measure what changed:
SELECT
    'prod'    AS source, DATA_YEAR, COUNT(*) AS rows
FROM RAW.RAW_CLAIMS
GROUP BY DATA_YEAR
UNION ALL
SELECT
    'before_bad_load', DATA_YEAR, COUNT(*)
FROM RAW.RAW_CLAIMS_BEFORE_BAD_LOAD
GROUP BY DATA_YEAR
ORDER BY DATA_YEAR, source;


-- ── 2d. Understand storage divergence ─────────────────────────────────────────
--
-- After modifying data in either the original or clone, monitor divergence:
SELECT
    TABLE_CATALOG,
    TABLE_SCHEMA,
    TABLE_NAME,
    -- ACTIVE_BYTES: live data in micro-partitions currently referenced by the table
    ROUND(ACTIVE_BYTES      / 1024 / 1024 / 1024, 3) AS active_gb,
    -- TIME_TRAVEL_BYTES: bytes in the retention zone (not yet purged)
    ROUND(TIME_TRAVEL_BYTES / 1024 / 1024 / 1024, 3) AS time_travel_gb,
    -- FAILSAFE_BYTES: bytes in Fail-safe (non-queryable, Snowflake-managed)
    ROUND(FAILSAFE_BYTES    / 1024 / 1024 / 1024, 3) AS failsafe_gb
FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE TABLE_SCHEMA = 'RAW'
  AND TABLE_NAME LIKE 'RAW_CLAIMS%'
ORDER BY TABLE_NAME;


-- ── 2e. Cleanup — drop clones when the work is done ──────────────────────────
-- After the investigation is complete, drop the point-in-time clones.
-- The shared micro-partitions are NOT deleted — they still exist in prod.
-- Only the metadata pointers for the clone are removed.
-- DROP TABLE IF EXISTS RAW.RAW_CLAIMS_BEFORE_BAD_LOAD;
-- DROP TABLE IF EXISTS RAW.RAW_CLAIMS_BACKUP;
-- DROP DATABASE IF EXISTS DEV_HEALTHCARE_DW;


-- =============================================================================
-- SECTION 3 — STREAMS AND TASKS
-- =============================================================================
-- WHAT: A Stream is a CDC (change-data-capture) log on a table — it records
--       which rows were inserted, updated, or deleted since the last time the
--       stream was consumed.  A Task is a Snowflake-native scheduler that runs
--       SQL (or a stored procedure) on a fixed schedule or after a parent task.
--
-- WHY:  Together they enable micro-batch pipelines entirely within Snowflake —
--       no Airflow, no Kafka, no external scheduler needed for simple feeds.
--       The stream guarantees exactly-once delivery: if a consuming transaction
--       fails, the stream offset does not advance, so no rows are lost or
--       double-processed.
--
-- HOW:  Snowflake assigns each micro-partition an internal version number.
--       A stream stores the version offset it last consumed.  When you query
--       the stream, Snowflake returns only the partitions written AFTER that
--       offset, surfaced as rows with three extra metadata columns:
--         METADATA$ACTION   — 'INSERT' or 'DELETE'
--         METADATA$ISUPDATE — TRUE when the row is the new version of an UPDATE
--                             (UPDATE = DELETE of old + INSERT of new)
--         METADATA$ROW_ID   — stable unique ID across versions of the same row
--
--       The offset advances ONLY when a transaction that reads the stream
--       commits successfully.  A rolled-back transaction leaves the offset
--       unchanged — automatic exactly-once semantics without any application
--       logic.
--
-- GOTCHA: Streams have a staleness window equal to the underlying table's
--         Time Travel retention (7 days for our RAW schema).  If no task
--         consumes the stream for 7 days, the stream becomes STALE — it loses
--         its offset and must be recreated.  Monitor IS_STALE in SHOW STREAMS.
--         For APPEND_ONLY streams, staleness rules are the same but storage
--         overhead is lower because Snowflake only needs to track insert offsets.
-- =============================================================================

USE ROLE TRANSFORMER;
USE WAREHOUSE HEALTHCARE_WH;


-- ── 3a. Standard stream — captures INSERT, UPDATE, DELETE ─────────────────────
--
-- Use a standard (non-append-only) stream when the source table can receive
-- updates or deletes — for example, if CMS sends corrected claim records.
--
-- Interview question: "When would you use a standard stream vs APPEND_ONLY?"
-- Answer: APPEND_ONLY is cheaper (tracks only insert-partition changes, not
-- full row versions) but misses updates and deletes.  Use APPEND_ONLY only when
-- you are certain the source is truly insert-only (e.g. an event log or an
-- immutable audit table).
CREATE OR REPLACE STREAM HEALTHCARE_DW.RAW.RAW_CLAIMS_STANDARD_STREAM
    ON TABLE HEALTHCARE_DW.RAW.RAW_CLAIMS
    -- APPEND_ONLY = FALSE is the default; stated explicitly here for clarity.
    -- Standard streams track all DML: INSERT (ACTION='INSERT', ISUPDATE=FALSE)
    --                                 UPDATE (ACTION='DELETE' ISUPDATE=TRUE +
    --                                         ACTION='INSERT' ISUPDATE=TRUE)
    --                                 DELETE (ACTION='DELETE', ISUPDATE=FALSE)
    COMMENT = 'Standard CDC stream — captures INSERTs, UPDATEs, and DELETEs on RAW_CLAIMS';


-- ── 3b. APPEND_ONLY stream — cheaper for insert-only sources ─────────────────
--
-- Our CMS ingestion is truncate-and-reload today, but if we switched to an
-- incremental append model (new rows appended each day), APPEND_ONLY would be
-- the right choice.  It only tracks new micro-partitions — no row-version
-- overhead — so it is faster to query and cheaper in Fail-safe storage.
CREATE OR REPLACE STREAM HEALTHCARE_DW.RAW.RAW_CLAIMS_APPEND_STREAM
    ON TABLE HEALTHCARE_DW.RAW.RAW_CLAIMS
    APPEND_ONLY = TRUE
    COMMENT = 'Insert-only CDC stream — use when RAW_CLAIMS switches to incremental append loads';


-- ── 3c. Inspect what the stream surfaces ─────────────────────────────────────
--
-- Before writing the Task, understand the stream's schema.  The three
-- METADATA$ columns are appended automatically; never add them to the target.
SELECT
    RNDRNG_NPI,
    HCPCS_CD,
    DATA_YEAR,
    LOAD_DATE,
    -- Stream metadata columns
    METADATA$ACTION     AS stream_action,    -- 'INSERT' or 'DELETE'
    METADATA$ISUPDATE   AS is_update_pair,   -- TRUE for UPDATE row pairs
    METADATA$ROW_ID     AS row_id            -- stable across row versions
FROM HEALTHCARE_DW.RAW.RAW_CLAIMS_STANDARD_STREAM
LIMIT 50;


-- ── 3d. Target table for the Task MERGE ──────────────────────────────────────
--
-- The Task merges stream rows into STAGING.STG_CLAIMS_INCREMENTAL, a persistent
-- staging table used for near-real-time feeds.  This is separate from the dbt
-- stg_claims VIEW (which reads directly from RAW_CLAIMS for batch analytics).
--
-- Use case: a downstream BI dashboard refreshes every 15 minutes and cannot
-- wait for the nightly dbt run.  The Task keeps STG_CLAIMS_INCREMENTAL current
-- in near-real-time; the dashboard reads from it.
CREATE TABLE IF NOT EXISTS HEALTHCARE_DW.STAGING.STG_CLAIMS_INCREMENTAL (
    -- Natural composite key — same grain as RAW_CLAIMS
    PROVIDER_NPI           VARCHAR(10)    NOT NULL,
    HCPCS_CODE             VARCHAR(5)     NOT NULL,
    PLACE_OF_SERVICE       VARCHAR(1),
    DATA_YEAR              NUMBER(4,0)    NOT NULL,
    -- Renamed + typed payment columns (mirrors stg_claims.sql)
    BENEFICIARY_COUNT      NUMBER(10,0),
    TOTAL_SERVICES         NUMBER(12,2),
    BENEFICIARY_DAY_SVCS   NUMBER(12,2),
    AVG_SUBMITTED_CHARGE   NUMBER(12,2),
    AVG_MEDICARE_ALLOWED   NUMBER(12,2),
    AVG_MEDICARE_PAYMENT   NUMBER(12,2),
    AVG_MEDICARE_STDZ      NUMBER(12,2),
    LOAD_DATE              DATE,
    -- Stream processing metadata — useful for debugging
    STREAM_PROCESSED_AT    TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_stg_claims_incremental
        PRIMARY KEY (PROVIDER_NPI, HCPCS_CODE, PLACE_OF_SERVICE, DATA_YEAR)
);


-- ── 3e. Task: MERGE from stream into staging table ───────────────────────────
--
-- The Task fires hourly.  The WHEN clause checks SYSTEM$STREAM_HAS_DATA()
-- before spinning up the warehouse.  If the stream is empty (no new CMS rows
-- since the last run), the task exits immediately at zero compute cost.
--
-- WHEN clause internals:
--   SYSTEM$STREAM_HAS_DATA() is a metadata call — it reads the stream's stored
--   offset and compares it to the table's current partition version.  No
--   warehouse compute is consumed.  The warehouse only starts if this returns TRUE.
--
-- This is the most important cost-control pattern for Tasks.  Without it,
-- the warehouse spins up every hour regardless of whether there is work to do.
CREATE OR REPLACE TASK HEALTHCARE_DW.RAW.SYNC_RAW_CLAIMS_TO_STAGING
    WAREHOUSE = HEALTHCARE_WH
    -- Hourly schedule.  For business-hours-only processing use CRON syntax:
    -- SCHEDULE = 'USING CRON 0/15 6-20 * * MON-FRI America/New_York'
    SCHEDULE  = '60 MINUTE'
    -- WHEN guard: skip (and charge $0) if no new rows in the stream.
    WHEN      SYSTEM$STREAM_HAS_DATA('HEALTHCARE_DW.RAW.RAW_CLAIMS_STANDARD_STREAM')
    COMMENT   = 'Hourly MERGE from RAW_CLAIMS stream into STAGING.STG_CLAIMS_INCREMENTAL'
AS
    -- ── The MERGE statement ────────────────────────────────────────────────────
    -- MERGE handles three scenarios from the stream in one atomic statement:
    --   1. New claim (INSERT action):     → insert into staging
    --   2. Updated claim (UPDATE action): → update the staging row
    --   3. Deleted claim (DELETE action): → delete from staging (rare for CMS)
    --
    -- Why MERGE over separate INSERT/UPDATE/DELETE statements?
    -- MERGE is a single transaction — if it fails, no partial rows land in
    -- staging, and the stream offset does NOT advance (exactly-once guarantee).
    -- Three separate statements could leave staging in a half-updated state.
    MERGE INTO HEALTHCARE_DW.STAGING.STG_CLAIMS_INCREMENTAL AS tgt

    -- Source: the stream, shaped into the staging column names.
    -- Filter only INSERT rows here; a separate WHEN MATCHED + ISUPDATE
    -- handles the update case below.
    USING (
        SELECT
            RNDRNG_NPI         AS provider_npi,
            HCPCS_CD           AS hcpcs_code,
            PLACE_OF_SRVC      AS place_of_service,
            DATA_YEAR,
            TOT_BENES          AS beneficiary_count,
            TOT_SRVCS          AS total_services,
            TOT_BENE_DAY_SRVCS AS beneficiary_day_svcs,
            AVG_SBMTD_CHRG     AS avg_submitted_charge,
            AVG_MDCR_ALOWD_AMT AS avg_medicare_allowed,
            AVG_MDCR_PYMT_AMT  AS avg_medicare_payment,
            AVG_MDCR_STDZD_AMT AS avg_medicare_stdz,
            LOAD_DATE,
            METADATA$ACTION    AS _action,
            METADATA$ISUPDATE  AS _is_update
        FROM HEALTHCARE_DW.RAW.RAW_CLAIMS_STANDARD_STREAM
        -- Exclude DELETE-side rows of UPDATE pairs from the source CTE.
        -- ISUPDATE=TRUE + ACTION='DELETE' is the "before" half of an UPDATE;
        -- ISUPDATE=TRUE + ACTION='INSERT' is the "after" half.
        -- Including both would attempt to match on the same key twice.
        WHERE NOT (METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = TRUE)
    ) AS src
        ON  tgt.PROVIDER_NPI     = src.provider_npi
        AND tgt.HCPCS_CODE       = src.hcpcs_code
        AND tgt.PLACE_OF_SERVICE = src.place_of_service
        AND tgt.DATA_YEAR        = src.data_year

    -- Matched + UPDATE action: refresh all payment fields.
    WHEN MATCHED AND src._action = 'INSERT' AND src._is_update = TRUE THEN
        UPDATE SET
            tgt.BENEFICIARY_COUNT    = src.beneficiary_count,
            tgt.TOTAL_SERVICES       = src.total_services,
            tgt.BENEFICIARY_DAY_SVCS = src.beneficiary_day_svcs,
            tgt.AVG_SUBMITTED_CHARGE = src.avg_submitted_charge,
            tgt.AVG_MEDICARE_ALLOWED = src.avg_medicare_allowed,
            tgt.AVG_MEDICARE_PAYMENT = src.avg_medicare_payment,
            tgt.AVG_MEDICARE_STDZ    = src.avg_medicare_stdz,
            tgt.LOAD_DATE            = src.LOAD_DATE,
            tgt.STREAM_PROCESSED_AT  = CURRENT_TIMESTAMP()

    -- Matched + DELETE action: remove the row from staging.
    WHEN MATCHED AND src._action = 'DELETE' THEN DELETE

    -- Not matched + INSERT action: add the new claim row to staging.
    WHEN NOT MATCHED AND src._action = 'INSERT' THEN
        INSERT (
            PROVIDER_NPI, HCPCS_CODE, PLACE_OF_SERVICE, DATA_YEAR,
            BENEFICIARY_COUNT, TOTAL_SERVICES, BENEFICIARY_DAY_SVCS,
            AVG_SUBMITTED_CHARGE, AVG_MEDICARE_ALLOWED,
            AVG_MEDICARE_PAYMENT, AVG_MEDICARE_STDZ,
            LOAD_DATE, STREAM_PROCESSED_AT
        )
        VALUES (
            src.provider_npi, src.hcpcs_code, src.place_of_service, src.data_year,
            src.beneficiary_count, src.total_services, src.beneficiary_day_svcs,
            src.avg_submitted_charge, src.avg_medicare_allowed,
            src.avg_medicare_payment, src.avg_medicare_stdz,
            src.LOAD_DATE, CURRENT_TIMESTAMP()
        );


-- ── 3f. Activate the Task ─────────────────────────────────────────────────────
--
-- Tasks are always created in SUSPENDED state.  This is an intentional safety
-- gate — you can inspect the DDL and test the MERGE manually before enabling
-- the live schedule.  Activate only after you have verified the MERGE logic.
ALTER TASK HEALTHCARE_DW.RAW.SYNC_RAW_CLAIMS_TO_STAGING RESUME;


-- ── 3g. Monitor streams — the most important operational queries ──────────────
--
-- SHOW STREAMS: the first thing to run when troubleshooting a pipeline that
-- appears to have stale data or is running unexpectedly.
SHOW STREAMS IN SCHEMA HEALTHCARE_DW.RAW;
-- Key columns to look at:
--   NAME              — stream identifier
--   SOURCE_TYPE       — Table, External Table, etc.
--   BASE_TABLES       — which table(s) the stream watches
--   STALE             — TRUE if the stream offset has fallen outside the
--                       retention window.  A stale stream must be dropped and
--                       recreated; it cannot be reset.
--   STALE_AFTER       — when the stream will become stale if not consumed
--   INVALID_REASON    — if the underlying table was dropped, renamed, etc.

-- Check staleness programmatically (useful in monitoring dashboards):
SELECT
    STREAM_NAME,
    STALE,
    STALE_AFTER,
    -- How many hours until the stream becomes stale?
    ROUND(DATEDIFF('hour', CURRENT_TIMESTAMP(), STALE_AFTER), 1) AS hours_until_stale,
    INVALID_REASON
FROM INFORMATION_SCHEMA.STREAMS
WHERE STREAM_SCHEMA = 'RAW'
  AND STREAM_CATALOG = 'HEALTHCARE_DW'
ORDER BY STALE_AFTER;

-- Does the stream currently have unprocessed rows?
-- (Returns TRUE/FALSE without reading the actual stream data)
SELECT
    SYSTEM$STREAM_HAS_DATA('HEALTHCARE_DW.RAW.RAW_CLAIMS_STANDARD_STREAM')
        AS has_unprocessed_rows,
    SYSTEM$STREAM_GET_TABLE_TIMESTAMP(
        'HEALTHCARE_DW.RAW.RAW_CLAIMS_STANDARD_STREAM'
    )   AS stream_offset_timestamp;

-- Monitor Task run history — did the WHEN clause skip any runs?
SHOW TASKS IN SCHEMA HEALTHCARE_DW.RAW;

SELECT
    NAME,
    STATE,               -- 'started', 'succeeded', 'failed', 'skipped'
    QUERY_START_TIME,
    COMPLETED_TIME,
    RETURN_VALUE,        -- rows affected by the MERGE
    ERROR_MESSAGE
FROM TABLE(
    INFORMATION_SCHEMA.TASK_HISTORY(
        SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP()),
        TASK_NAME => 'SYNC_RAW_CLAIMS_TO_STAGING'
    )
)
ORDER BY QUERY_START_TIME DESC
LIMIT 50;


-- ── 3h. Execute the Task immediately for testing (bypasses the schedule) ──────
EXECUTE TASK HEALTHCARE_DW.RAW.SYNC_RAW_CLAIMS_TO_STAGING;

-- After execution, verify the stream was consumed (offset advanced):
SELECT
    SYSTEM$STREAM_HAS_DATA('HEALTHCARE_DW.RAW.RAW_CLAIMS_STANDARD_STREAM')
        AS has_data_remaining;  -- Should be FALSE if MERGE succeeded


-- ── 3i. Suspend the Task when no longer needed ────────────────────────────────
-- (uncomment when you want to disable the schedule)
-- ALTER TASK HEALTHCARE_DW.RAW.SYNC_RAW_CLAIMS_TO_STAGING SUSPEND;


-- =============================================================================
-- SECTION 4 — VIRTUAL WAREHOUSE SIZING
-- =============================================================================
-- WHAT: A virtual warehouse in Snowflake is a named compute cluster (1–512
--       server nodes) that executes queries.  It is completely independent of
--       storage.  You can resize, suspend, or add more warehouses at any time
--       without moving data.
--
-- WHY:  Different workloads have wildly different compute needs:
--         Ingestion (write-heavy): X-SMALL is usually sufficient — bottleneck
--           is network I/O to the CMS API, not CPU.
--         dbt full runs (complex joins + aggregations across 10M rows): MEDIUM
--           or LARGE cuts runtime from 45 minutes to 8 minutes.
--         Concurrent BI queries (20 analysts running Power BI): X-SMALL gets
--           queue contention; SMALL or multi-cluster eliminates wait times.
--
-- HOW:  When you ALTER WAREHOUSE … SET WAREHOUSE_SIZE = 'MEDIUM', Snowflake
--       provisions new server nodes (2x more than X-SMALL) and drains the old
--       ones after in-flight queries complete.  The resize is near-instant
--       (< 30 seconds) because Snowflake provisions from a pool.
--       Credit consumption is DOUBLED at MEDIUM vs X-SMALL:
--         X-SMALL = 1 credit/hour  |  X-SMALL → SMALL = 2x  → MEDIUM = 4x
--         LARGE = 8x  |  X-LARGE = 16x  |  2X-LARGE = 32x  |  3X-LARGE = 64x
--
-- GOTCHA: Auto-Suspend does NOT happen during an in-progress query.  If you
--         scale up to LARGE for a dbt run but forget to scale back down, the
--         warehouse will auto-suspend after the inactivity window — but you
--         still pay LARGE credits for that window.  Always pair a scale-up with
--         an immediate scale-down after the workload completes.
-- =============================================================================

USE ROLE SYSADMIN;


-- ── 4a. Inspect current state before scaling ──────────────────────────────────
SHOW WAREHOUSES LIKE 'HEALTHCARE_WH';
-- Key columns: NAME, SIZE, RUNNING (active queries), QUEUED (waiting queries),
-- AUTO_SUSPEND, AUTO_RESUME, STATE (started/suspended)

-- More detail from INFORMATION_SCHEMA:
SELECT
    WAREHOUSE_NAME,
    -- Credit spend in the last 24 hours
    SUM(CREDITS_USED)                                  AS credits_24h,
    ROUND(SUM(CREDITS_USED) * 2.0, 2)                 AS estimated_cost_usd_24h,
    SUM(EXECUTION_TIME_MS) / 1000 / 60                AS active_minutes_24h,
    COUNT(DISTINCT QUERY_ID)                           AS query_count_24h,
    -- Queued time indicates the warehouse is under-sized for the load
    SUM(QUEUED_OVERLOAD_TIME_MS) / 1000 / 60           AS minutes_queries_waited_24h
FROM TABLE(
    INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
        DATE_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP())
    )
)
GROUP BY WAREHOUSE_NAME;


-- ── 4b. Scale UP before a heavy dbt run ───────────────────────────────────────
--
-- dbt full refresh on RAW_CLAIMS (10M rows) + fct_claims aggregation runs
-- ~40 min on X-SMALL, ~8 min on MEDIUM.  The 32-minute saving at $0.04/credit
-- (hypothetical) costs an extra (4-1) × (8/60) × 0.04 ≈ $0.16 more.
-- For a team of 10 waiting on CI, that trade-off almost always makes sense.
--
-- Best practice: scale up immediately before the workload, scale down
-- immediately after.  Never leave a large warehouse running idle.

ALTER WAREHOUSE HEALTHCARE_WH
    SET WAREHOUSE_SIZE = 'MEDIUM';

-- Verify the resize took effect:
SHOW WAREHOUSES LIKE 'HEALTHCARE_WH';

-- ── NOW RUN THE dbt WORKLOAD ──────────────────────────────────────────────────
-- (In Airflow, the dbt_staging_run + dbt_marts_run BashOperator tasks run here.
--  In a Snowflake worksheet, you would invoke dbt from your terminal instead.)
--
--   dbt run --select tag:daily --profiles-dir dbt_project --target prod
--   dbt run --select marts.*  --profiles-dir dbt_project --target prod
--   dbt test --profiles-dir dbt_project --target prod
--
-- After those complete, immediately scale back down:


-- ── 4c. Scale DOWN immediately after the dbt run completes ───────────────────
--
-- AUTO_SUSPEND = 60 means the warehouse auto-suspends after 60 seconds of
-- idle time and stops billing.  But we still pay MEDIUM credits during those
-- 60 seconds of idle.  Explicitly resizing back to X-SMALL before the idle
-- window saves a small amount but also expresses intent: this warehouse is
-- normally X-SMALL and only temporarily upsized.
ALTER WAREHOUSE HEALTHCARE_WH
    SET WAREHOUSE_SIZE = 'X-SMALL';

SHOW WAREHOUSES LIKE 'HEALTHCARE_WH';
-- Confirm SIZE = X-SMALL before moving on.


-- ── 4d. Automate the scale-up and scale-down with a Task graph ───────────────
--
-- Rather than relying on a human to resize, you can wrap the pattern in
-- Snowflake Tasks so the scale-up and scale-down are automatic.
-- This is the self-contained "serverless ETL" pattern used by teams
-- who want Snowflake-native orchestration.

CREATE OR REPLACE TASK HEALTHCARE_DW.RAW.SCALE_UP_BEFORE_DBT
    WAREHOUSE = HEALTHCARE_WH    -- a small warehouse to run the ALTER itself
    SCHEDULE  = 'USING CRON 0 6 * * * UTC'   -- fire at 06:00 UTC daily
    COMMENT   = 'Scale HEALTHCARE_WH to MEDIUM before the nightly dbt run'
AS
    ALTER WAREHOUSE HEALTHCARE_WH SET WAREHOUSE_SIZE = 'MEDIUM';

-- The scale-down task is a CHILD of the scale-up task.
-- Snowflake's task DAG: SCALE_UP → dbt (Airflow) → SCALE_DOWN
-- In practice, Airflow manages dbt; we create both tasks for completeness.
CREATE OR REPLACE TASK HEALTHCARE_DW.RAW.SCALE_DOWN_AFTER_DBT
    WAREHOUSE = HEALTHCARE_WH
    -- No SCHEDULE — this task fires when its parent succeeds.
    AFTER     HEALTHCARE_DW.RAW.SCALE_UP_BEFORE_DBT
    COMMENT   = 'Scale HEALTHCARE_WH back to X-SMALL after the nightly dbt run'
AS
    ALTER WAREHOUSE HEALTHCARE_WH SET WAREHOUSE_SIZE = 'X-SMALL';

-- Activate both tasks (children must be resumed before parents):
ALTER TASK HEALTHCARE_DW.RAW.SCALE_DOWN_AFTER_DBT RESUME;
ALTER TASK HEALTHCARE_DW.RAW.SCALE_UP_BEFORE_DBT  RESUME;


-- ── 4e. Multi-cluster warehouse — for concurrent BI query load ────────────────
--
-- REQUIRES: Snowflake Enterprise Edition or higher.
-- DOES NOT APPLY to Standard Edition accounts.
--
-- A multi-cluster warehouse automatically adds parallel clusters (each a full
-- copy of the warehouse size) when query concurrency causes queue buildup.
-- This is different from scaling UP (more CPU per query) — it is scaling OUT
-- (more concurrent queries run in parallel across clusters).
--
-- When to use multi-cluster vs. a single larger warehouse:
--
--   Single larger warehouse (MEDIUM, LARGE):
--     → A single long-running dbt query that needs more CPU / memory.
--     → The query itself is the bottleneck, not queue waiting.
--
--   Multi-cluster warehouse:
--     → 20 Power BI analysts all hitting dashboards at 9 AM simultaneously.
--     → Queries are individually fast (2–5s) but there are too many to queue.
--     → Each cluster handles a subset of the concurrent load.
--
-- SCALING_POLICY choices:
--   STANDARD  — adds a cluster as soon as any query queues.  Prioritises
--               latency.  Best for dashboards and interactive tools.
--   ECONOMY   — waits until the queue reaches a threshold before adding a
--               cluster.  Prioritises cost.  Best for batch workloads where
--               5–10s queue wait is acceptable.
--
-- Commented out — requires Enterprise Edition.  Show this in an interview
-- to demonstrate you understand the feature even if not on Enterprise.

/*
CREATE WAREHOUSE IF NOT EXISTS HEALTHCARE_BI_WH
    WAREHOUSE_SIZE    = 'SMALL'        -- each cluster is SMALL (2x X-SMALL credits)
    MIN_CLUSTER_COUNT = 1              -- always at least 1 cluster active
    MAX_CLUSTER_COUNT = 4              -- scale out to 4 clusters under load
    SCALING_POLICY    = 'STANDARD'     -- add a cluster as soon as any query queues
    AUTO_SUSPEND      = 120            -- longer suspend for BI (analysts expect fast response)
    AUTO_RESUME       = TRUE
    COMMENT           = 'Multi-cluster warehouse for concurrent Power BI / Looker traffic';

-- Credit cost: at MAX_CLUSTER_COUNT=4, this warehouse costs 4 × SMALL = 8 credits/hour
-- when all clusters are active.  Only 1 cluster bills during normal off-peak hours.
*/


-- ── 4f. Query history to identify sizing decisions ───────────────────────────
--
-- Before sizing a warehouse, look at actual query runtimes and queue times
-- to make a data-driven decision rather than guessing.
SELECT
    WAREHOUSE_SIZE,
    COUNT(*)                                  AS query_count,
    -- Average execution time in seconds
    ROUND(AVG(EXECUTION_TIME) / 1000, 1)      AS avg_exec_seconds,
    ROUND(MAX(EXECUTION_TIME) / 1000, 1)      AS max_exec_seconds,
    -- Queue time: non-zero means the warehouse was saturated
    ROUND(AVG(QUEUED_OVERLOAD_TIME) / 1000, 1) AS avg_queue_seconds,
    -- Compilation time: high values suggest complex queries, not compute bottleneck
    ROUND(AVG(COMPILATION_TIME) / 1000, 1)    AS avg_compile_seconds,
    -- Bytes scanned per query — helps right-size partition pruning
    ROUND(AVG(BYTES_SCANNED) / 1024 / 1024 / 1024, 2) AS avg_gb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE
    WAREHOUSE_NAME = 'HEALTHCARE_WH'
    AND START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    AND EXECUTION_STATUS = 'SUCCESS'
GROUP BY WAREHOUSE_SIZE
ORDER BY WAREHOUSE_SIZE;

-- Identify the slowest individual queries (good candidates for optimization
-- before reaching for a bigger warehouse):
SELECT
    QUERY_ID,
    QUERY_TYPE,
    WAREHOUSE_SIZE,
    LEFT(QUERY_TEXT, 80)                         AS query_preview,
    ROUND(EXECUTION_TIME     / 1000 / 60, 2)     AS exec_minutes,
    ROUND(QUEUED_OVERLOAD_TIME / 1000, 1)         AS queued_seconds,
    ROUND(BYTES_SCANNED / 1024 / 1024 / 1024, 2) AS gb_scanned,
    ROWS_PRODUCED
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE
    WAREHOUSE_NAME = 'HEALTHCARE_WH'
    AND START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    AND EXECUTION_STATUS = 'SUCCESS'
ORDER BY EXECUTION_TIME DESC
LIMIT 20;


-- =============================================================================
-- INTERVIEW CHEAT-SHEET — talking points for each feature
-- =============================================================================
--
-- TIME TRAVEL
--   "Snowflake stores micro-partitions immutably.  When data changes, new
--    partitions are written and old ones are retained for the retention window.
--    AT and BEFORE are syntactic sugar over partition version selection.  The
--    key distinction I always mention: Time Travel is user-queryable for up to
--    90 days (Enterprise); Fail-safe is an additional 7-day Snowflake-managed
--    recovery layer that you cannot query — it requires a support ticket.
--    UNDROP works within the retention window; after that, only Fail-safe."
--
-- ZERO-COPY CLONE
--   "A clone is a metadata operation — a new set of pointers to the same
--    micro-partition files.  It takes seconds, not hours, regardless of table
--    size.  Storage cost diverges only as copy-on-write modifies partitions.
--    Two power features combine here: CLONE + AT gives you a historical clone —
--    'give me a copy of production as it was before last night's bad load'.
--    One gotcha: Streams and dynamic table configurations do NOT clone — you
--    need to recreate them on the clone."
--
-- STREAMS + TASKS
--   "A stream is an offset tracker on a table's micro-partition changelog.
--    Reading the stream only advances the offset if the consuming DML commits
--    successfully — that's the exactly-once guarantee without application logic.
--    The WHEN SYSTEM$STREAM_HAS_DATA() guard is the most important cost-control
--    pattern: the warehouse never spins up if the stream is empty.
--    APPEND_ONLY streams cost less because Snowflake only tracks insert-partition
--    changes, not full row versions.  I use standard streams when CMS sends
--    corrected records, APPEND_ONLY when the source is truly insert-only."
--
-- WAREHOUSE SIZING
--   "Scaling UP (X-SMALL → MEDIUM) gives a single query more CPU and memory —
--    the right tool for a slow complex dbt run.  Scaling OUT (multi-cluster)
--    gives more parallel capacity — the right tool for concurrent BI queries
--    from many users hitting Power BI at 9 AM.  For batch workloads I always
--    pair a scale-up with an immediate scale-down and rely on AUTO_SUSPEND as
--    a safety net, not the primary cost-control mechanism.  The QUERY_HISTORY
--    view tells me whether the bottleneck is compute (high exec time, low queue)
--    or concurrency (low exec time, high queue) before I reach for a bigger
--    warehouse."
-- =============================================================================
