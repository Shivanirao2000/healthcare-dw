{{
  config(
    severity  = 'error',
    tags      = ['daily', 'data_quality']
  )
}}

{#
  Singular test: assert that no row in fct_claims carries a negative payment
  or charge amount.

  Why this matters
  ────────────────
  CMS payment amounts represent dollars paid to or charged by providers; they
  are physically constrained to be ≥ 0 in the CMS data dictionary.  A negative
  value means one of three things went wrong:

    1. A sign error was introduced in the fct_claims aggregation step (the
       most likely cause — e.g. a subtraction where a multiplication was
       intended when computing total_* from avg_* × total_services).

    2. The CMS source file changed format in a new release year and a column
       was misaligned during ingestion.

    3. A Snowflake write_pandas type-coercion issue silently negated a value.

  NULL values ARE permitted.  CMS applies small-cell suppression (< 11
  beneficiaries) and leaves payment columns NULL rather than zero; those rows
  are not flagged here.  Only strictly-negative values constitute a test
  failure.

  Columns tested
  ──────────────
  avg_submitted_charge        — volume-weighted avg charge per service
  avg_medicare_payment        — volume-weighted avg Medicare payment per service
  total_submitted_charges     — estimated total charges (avg × services)
  total_medicare_allowed      — estimated total Medicare-allowed amount
  total_medicare_payment      — estimated total Medicare payment
  total_medicare_standardized — estimated geographically-standardised payment

  How dbt evaluates singular tests
  ─────────────────────────────────
  dbt runs this SQL and counts the rows returned.  Zero rows → pass.
  Any rows returned → fail (each row is a failing record for debugging).
  The SELECT therefore returns the *failing* rows, not the passing ones.
#}

select
    claim_key,
    provider_npi,
    hcpcs_code,
    data_year,

    -- Include every payment column so the failure message is self-contained.
    avg_submitted_charge,
    avg_medicare_payment,
    total_submitted_charges,
    total_medicare_allowed,
    total_medicare_payment,
    total_medicare_standardized,

    -- Indicate which specific column(s) triggered the failure.
    case
        when avg_submitted_charge        < 0 then 'avg_submitted_charge'
        when avg_medicare_payment        < 0 then 'avg_medicare_payment'
        when total_submitted_charges     < 0 then 'total_submitted_charges'
        when total_medicare_allowed      < 0 then 'total_medicare_allowed'
        when total_medicare_payment      < 0 then 'total_medicare_payment'
        when total_medicare_standardized < 0 then 'total_medicare_standardized'
    end                                         as failing_column

from {{ ref('fct_claims') }}

where
    -- Each condition uses OR so that a row is returned if ANY column is negative.
    -- NULL-safe: comparisons with NULL evaluate to UNKNOWN (neither true nor false),
    -- so NULL values are never returned as failures.
       avg_submitted_charge        < 0
    or avg_medicare_payment        < 0
    or total_submitted_charges     < 0
    or total_medicare_allowed      < 0
    or total_medicare_payment      < 0
    or total_medicare_standardized < 0
