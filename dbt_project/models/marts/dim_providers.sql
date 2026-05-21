{{
  config(materialized='table')
}}

with providers as (
    select * from {{ ref('stg_providers') }}
),

-- SCD Type 1: keep only the most recent data year for each NPI.
-- Provider attributes (name, specialty, location) evolve annually; BI tools
-- need a single current row per provider, not a history. Historical analysis
-- can filter fct_claims by data_year and re-join if needed.
latest as (
    select *
    from providers
    qualify row_number() over (
        partition by provider_npi
        order by data_year desc
    ) = 1
),

final as (
    select
        -- ── Primary key ───────────────────────────────────────────────────────
        provider_npi,

        -- ── Derived display name ──────────────────────────────────────────────
        -- Individuals: "Smith, John A" format.
        -- Organisations: the legal name as supplied.
        case entity_code
            when 'I' then trim(
                coalesce(last_org_name, '') || ', ' ||
                coalesce(first_name, '')   || ' ' ||
                coalesce(middle_initial, '')
            )
            else coalesce(last_org_name, '')
        end                                     as provider_name,

        -- ── Raw name parts ────────────────────────────────────────────────────
        last_org_name,
        first_name,
        middle_initial,
        credentials,

        -- ── Demographics ──────────────────────────────────────────────────────
        null::varchar as gender,
        entity_code,
        case entity_code
            when 'I' then 'Individual'
            when 'O' then 'Organization'
            else 'Unknown'
        end                                     as entity_type,
        medicare_participating_ind,

        -- ── Practice ──────────────────────────────────────────────────────────
        specialty,

        -- ── Location ──────────────────────────────────────────────────────────
        address_line_1,
        address_line_2,
        city,
        state_abbr,
        state_fips,
        zip5,
        country,

        -- ── Urban/rural ───────────────────────────────────────────────────────
        -- RUCA codes: 1–3 = Metropolitan, 4–6 = Micropolitan,
        --             7–9 = Small Town, 10.x = Rural.
        -- NULL RUCA (overseas/territory providers) → 'Unknown'.
        ruca_code,
        ruca_description,
        case
            when ruca_code between 1 and 3   then 'Metropolitan'
            when ruca_code between 4 and 6   then 'Micropolitan'
            when ruca_code between 7 and 9   then 'Small Town'
            when ruca_code >= 10             then 'Rural'
            else                                  'Unknown'
        end                                     as urban_rural_classification,

        -- ── Audit ─────────────────────────────────────────────────────────────
        data_year                               as source_data_year,
        _loaded_at

    from latest
)

select * from final
