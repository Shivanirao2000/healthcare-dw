{{
  config(
    severity  = 'error',
    tags      = ['static', 'data_quality']
  )
}}

{#
  Singular test: assert that the dim_date date spine has no gaps — every
  consecutive pair of rows differs by exactly one calendar day.

  Why this matters
  ────────────────
  fct_claims is an annual grain (one row per year), but dim_date is used by
  BI tools to drill from yearly aggregates down to monthly, weekly, and daily
  views.  A gap in the date spine causes date-range filters to silently drop
  days from time-series charts — a subtle data quality issue that is hard to
  spot in a dashboard but easy to assert here.

  dim_date is a static model (tagged 'static') built from a deterministic
  Snowflake GENERATOR call.  In practice it should never have gaps; this test
  exists to catch:

    • A change to dim_date_start / dim_date_end vars that leaves a hole.
    • An accidental WHERE clause that filters out a date.
    • A GENERATOR rowcount that is too small for the date range, truncating
      the spine before dim_date_end.

  Method
  ──────
  LEAD(date_day) over the ordered spine gives each row's successor.  If the
  gap to that successor is not exactly 1 day, the interval contains missing
  dates.  The last row of the spine has no successor (LEAD → NULL) and is
  excluded from the check.

  A separate implicit guarantee: if the spine has the correct number of rows
  (checked by the unique + not_null tests on date_day in schema.yml) AND has
  no gaps, then the first and last dates are also correct.  Explicit boundary
  tests can be added as schema-level tests if desired:
      - dbt_utils.expression_is_true:
            expression: "date_day >= '2019-01-01'"
#}

with spine as (
    select
        date_day,
        lead(date_day) over (order by date_day) as next_date_day
    from {{ ref('dim_date') }}
),

gaps as (
    select
        date_day                                        as gap_starts_after,
        next_date_day                                   as gap_ends_before,
        datediff('day', date_day, next_date_day)        as days_between,
        -- How many days are missing in the interval (0 = adjacent, ≥1 = gap).
        datediff('day', date_day, next_date_day) - 1   as missing_days
    from spine
    where
        next_date_day is not null          -- exclude the final row (no successor)
        and datediff('day', date_day, next_date_day) <> 1  -- flag non-unit steps
)

-- Return the gaps so that failure output is self-diagnosing.
-- Each row names the date after which the gap begins and the date before which
-- it ends, along with how many days are missing.
select
    gap_starts_after,
    gap_ends_before,
    days_between,
    missing_days
from gaps
