{{
  config(materialized='table')
}}

-- Generate a complete date spine from dim_date_start to dim_date_end.
-- Driven by project vars so the range extends without code changes when new
-- CMS release years are ingested.
--
-- Snowflake GENERATOR + SEQ4() produces a monotonically-increasing integer
-- sequence (0, 1, 2, …) in a single scan; DATEADD converts it to dates.
-- The WHERE guard ensures we never exceed dim_date_end even if rowcount
-- overshoots (e.g. when adding a leap-year buffer).
--
-- CMS / HHS fiscal year: October 1 → September 30.
-- FY2024 = 2023-10-01 → 2024-09-30  →  fiscal_year = calendar_year + 1
--          when month >= 10.

with date_spine as (
    select
        dateadd(
            day,
            seq4(),
            '{{ var("dim_date_start") }}'::date
        ) as date_day
    from table(generator(rowcount => 2192))  -- 2019-01-01 through 2024-12-31 = 2192 days
    where date_day <= '{{ var("dim_date_end") }}'::date
),

final as (
    select
        -- ── Primary key ───────────────────────────────────────────────────────
        date_day,

        -- ── Year / quarter / month ────────────────────────────────────────────
        year(date_day)                                      as year,
        quarter(date_day)                                   as quarter_of_year,
        'Q' || quarter(date_day)::varchar                   as quarter_label,
        month(date_day)                                     as month_of_year,
        to_char(date_day, 'MMMM')                           as month_name,
        monthname(date_day)                                 as month_name_short,
        to_char(date_day, 'YYYY-MM')                        as year_month,

        -- ── Week / day ────────────────────────────────────────────────────────
        weekofyear(date_day)                                as week_of_year,
        dayofmonth(date_day)                                as day_of_month,
        dayofyear(date_day)                                 as day_of_year,
        -- ISO: 1 = Monday … 7 = Sunday
        dayofweekiso(date_day)                              as day_of_week_iso,
        dayname(date_day)                                   as day_name,
        iff(dayofweekiso(date_day) in (6, 7), true, false)  as is_weekend,

        -- ── CMS / HHS fiscal calendar ─────────────────────────────────────────
        -- FY starts October 1; month >= 10 means we are in the *next* fiscal year.
        case
            when month(date_day) >= 10 then year(date_day) + 1
            else                            year(date_day)
        end                                                 as fiscal_year,
        case
            when month(date_day) in (10, 11, 12) then 1  -- Oct–Dec = FQ1
            when month(date_day) in (1,   2,  3) then 2  -- Jan–Mar = FQ2
            when month(date_day) in (4,   5,  6) then 3  -- Apr–Jun = FQ3
            when month(date_day) in (7,   8,  9) then 4  -- Jul–Sep = FQ4
        end                                                 as fiscal_quarter

    from date_spine
)

select * from final
