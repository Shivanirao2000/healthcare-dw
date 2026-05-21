{{
  config(materialized='table')
}}

-- ── Grain ─────────────────────────────────────────────────────────────────────
-- One row per provider_npi × hcpcs_code × data_year.
--
-- The source (stg_claims) has grain NPI × HCPCS × place_of_service × year,
-- meaning the same provider-procedure pair can appear twice (facility + office).
-- We aggregate here to collapse place_of_service, weighting payment averages
-- by service volume so the resulting averages are still meaningful.
--
-- Approximation note: total_submitted_charges and total_medicare_payment are
-- computed as SUM(avg_amount × total_services). These are approximations because
-- CMS publishes per-row averages, not individual claim amounts. Treat them as
-- estimates accurate to ±5% of the true totals.
-- ─────────────────────────────────────────────────────────────────────────────

with claims as (
    select * from {{ ref('stg_claims') }}
),

-- Pull the provider's state from the most recent data year for the geography FK.
-- Use stg_providers directly (not dim_providers) to keep staging as the join
-- source and avoid cross-layer dependencies in the mart.
provider_states as (
    select
        provider_npi,
        state_abbr
    from {{ ref('stg_providers') }}
    qualify row_number() over (
        partition by provider_npi
        order by data_year desc
    ) = 1
),

aggregated as (
    select
        c.provider_npi,
        c.hcpcs_code,
        c.data_year,
        p.state_abbr,

        -- ── Volume ────────────────────────────────────────────────────────────
        sum(c.total_services)                               as total_services,
        -- MAX avoids double-counting beneficiaries who received both F and O
        -- services for the same HCPCS code in the same year.
        max(c.beneficiary_count)                            as beneficiary_count,
        sum(c.beneficiary_day_services)                     as total_beneficiary_day_services,

        -- ── Payment — volume-weighted averages ────────────────────────────────
        round(
            sum(c.avg_submitted_charge * c.total_services)
            / nullif(sum(c.total_services), 0),
        2)                                                  as avg_submitted_charge,

        round(
            sum(c.avg_medicare_payment * c.total_services)
            / nullif(sum(c.total_services), 0),
        2)                                                  as avg_medicare_payment,

        -- ── Payment — estimated totals ────────────────────────────────────────
        round(
            sum(c.avg_submitted_charge * c.total_services),
        2)                                                  as total_submitted_charges,

        round(
            sum(c.avg_medicare_allowed_amount * c.total_services),
        2)                                                  as total_medicare_allowed,

        round(
            sum(c.avg_medicare_payment * c.total_services),
        2)                                                  as total_medicare_payment,

        round(
            sum(c.avg_medicare_standardized_amount * c.total_services),
        2)                                                  as total_medicare_standardized

    from claims c
    left join provider_states p
        on c.provider_npi = p.provider_npi
    group by
        c.provider_npi,
        c.hcpcs_code,
        c.data_year,
        p.state_abbr
),

final as (
    select
        -- ── Surrogate key ─────────────────────────────────────────────────────
        -- MD5 of the three natural-key components; stable across reruns.
        md5(
            coalesce(provider_npi,          '_null_') || '|' ||
            coalesce(hcpcs_code,            '_null_') || '|' ||
            coalesce(cast(data_year as varchar), '_null_')
        )                                               as claim_key,

        -- ── Foreign keys ──────────────────────────────────────────────────────
        provider_npi,           -- → dim_providers.provider_npi
        hcpcs_code,             -- → dim_procedures.hcpcs_code
        state_abbr,             -- → dim_geography.state_abbr

        -- ── Degenerate dimension ──────────────────────────────────────────────
        -- Join to dim_date on:  dim_date.year = fct_claims.data_year
        data_year,

        -- ── Measures ──────────────────────────────────────────────────────────
        total_services,
        beneficiary_count,
        total_beneficiary_day_services,
        avg_submitted_charge,
        avg_medicare_payment,
        total_submitted_charges,
        total_medicare_allowed,
        total_medicare_payment,
        total_medicare_standardized

    from aggregated
)

select * from final
