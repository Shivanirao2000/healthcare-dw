{{
  config(materialized='table')
}}

with provider_states as (
    select
        state_abbr,
        ruca_code
    from {{ ref('stg_providers') }}
    where state_abbr is not null
      and state_abbr != ''
),

-- Aggregate RUCA codes to state level to derive a state-level urban/rural
-- classification. Mean RUCA < 4 = predominantly metropolitan, etc.
state_ruca as (
    select
        state_abbr,
        avg(ruca_code)  as avg_ruca_code,
        count(*)        as provider_count
    from provider_states
    group by state_abbr
),

enriched as (
    select
        state_abbr,
        avg_ruca_code,
        provider_count,

        -- ── State names (US Census Bureau spellings) ──────────────────────────
        case state_abbr
            when 'AL' then 'Alabama'               when 'AK' then 'Alaska'
            when 'AZ' then 'Arizona'               when 'AR' then 'Arkansas'
            when 'CA' then 'California'            when 'CO' then 'Colorado'
            when 'CT' then 'Connecticut'           when 'DE' then 'Delaware'
            when 'DC' then 'District of Columbia'  when 'FL' then 'Florida'
            when 'GA' then 'Georgia'               when 'HI' then 'Hawaii'
            when 'ID' then 'Idaho'                 when 'IL' then 'Illinois'
            when 'IN' then 'Indiana'               when 'IA' then 'Iowa'
            when 'KS' then 'Kansas'                when 'KY' then 'Kentucky'
            when 'LA' then 'Louisiana'             when 'ME' then 'Maine'
            when 'MD' then 'Maryland'              when 'MA' then 'Massachusetts'
            when 'MI' then 'Michigan'              when 'MN' then 'Minnesota'
            when 'MS' then 'Mississippi'           when 'MO' then 'Missouri'
            when 'MT' then 'Montana'               when 'NE' then 'Nebraska'
            when 'NV' then 'Nevada'                when 'NH' then 'New Hampshire'
            when 'NJ' then 'New Jersey'            when 'NM' then 'New Mexico'
            when 'NY' then 'New York'              when 'NC' then 'North Carolina'
            when 'ND' then 'North Dakota'          when 'OH' then 'Ohio'
            when 'OK' then 'Oklahoma'              when 'OR' then 'Oregon'
            when 'PA' then 'Pennsylvania'          when 'RI' then 'Rhode Island'
            when 'SC' then 'South Carolina'        when 'SD' then 'South Dakota'
            when 'TN' then 'Tennessee'             when 'TX' then 'Texas'
            when 'UT' then 'Utah'                  when 'VT' then 'Vermont'
            when 'VA' then 'Virginia'              when 'WA' then 'Washington'
            when 'WV' then 'West Virginia'         when 'WI' then 'Wisconsin'
            when 'WY' then 'Wyoming'
            when 'PR' then 'Puerto Rico'           when 'VI' then 'U.S. Virgin Islands'
            when 'GU' then 'Guam'                  when 'MP' then 'Northern Mariana Islands'
            when 'AS' then 'American Samoa'
            else state_abbr
        end                                         as state_name,

        -- ── US Census Bureau regions ──────────────────────────────────────────
        case state_abbr
            when 'CT' then 'Northeast'  when 'ME' then 'Northeast'
            when 'MA' then 'Northeast'  when 'NH' then 'Northeast'
            when 'RI' then 'Northeast'  when 'VT' then 'Northeast'
            when 'NJ' then 'Northeast'  when 'NY' then 'Northeast'
            when 'PA' then 'Northeast'
            when 'IN' then 'Midwest'    when 'IL' then 'Midwest'
            when 'MI' then 'Midwest'    when 'OH' then 'Midwest'
            when 'WI' then 'Midwest'    when 'IA' then 'Midwest'
            when 'KS' then 'Midwest'    when 'MN' then 'Midwest'
            when 'MO' then 'Midwest'    when 'NE' then 'Midwest'
            when 'ND' then 'Midwest'    when 'SD' then 'Midwest'
            when 'DE' then 'South'      when 'DC' then 'South'
            when 'FL' then 'South'      when 'GA' then 'South'
            when 'MD' then 'South'      when 'NC' then 'South'
            when 'SC' then 'South'      when 'VA' then 'South'
            when 'WV' then 'South'      when 'AL' then 'South'
            when 'KY' then 'South'      when 'MS' then 'South'
            when 'TN' then 'South'      when 'AR' then 'South'
            when 'LA' then 'South'      when 'OK' then 'South'
            when 'TX' then 'South'
            when 'AZ' then 'West'       when 'CO' then 'West'
            when 'ID' then 'West'       when 'NM' then 'West'
            when 'MT' then 'West'       when 'UT' then 'West'
            when 'NV' then 'West'       when 'WY' then 'West'
            when 'AK' then 'West'       when 'CA' then 'West'
            when 'HI' then 'West'       when 'OR' then 'West'
            when 'WA' then 'West'
            else 'Territory / Other'
        end                                         as census_region,

        -- ── US Census Bureau divisions ────────────────────────────────────────
        case state_abbr
            when 'CT' then 'New England'         when 'ME' then 'New England'
            when 'MA' then 'New England'         when 'NH' then 'New England'
            when 'RI' then 'New England'         when 'VT' then 'New England'
            when 'NJ' then 'Middle Atlantic'     when 'NY' then 'Middle Atlantic'
            when 'PA' then 'Middle Atlantic'
            when 'IN' then 'East North Central'  when 'IL' then 'East North Central'
            when 'MI' then 'East North Central'  when 'OH' then 'East North Central'
            when 'WI' then 'East North Central'
            when 'IA' then 'West North Central'  when 'KS' then 'West North Central'
            when 'MN' then 'West North Central'  when 'MO' then 'West North Central'
            when 'NE' then 'West North Central'  when 'ND' then 'West North Central'
            when 'SD' then 'West North Central'
            when 'DE' then 'South Atlantic'      when 'DC' then 'South Atlantic'
            when 'FL' then 'South Atlantic'      when 'GA' then 'South Atlantic'
            when 'MD' then 'South Atlantic'      when 'NC' then 'South Atlantic'
            when 'SC' then 'South Atlantic'      when 'VA' then 'South Atlantic'
            when 'WV' then 'South Atlantic'
            when 'AL' then 'East South Central'  when 'KY' then 'East South Central'
            when 'MS' then 'East South Central'  when 'TN' then 'East South Central'
            when 'AR' then 'West South Central'  when 'LA' then 'West South Central'
            when 'OK' then 'West South Central'  when 'TX' then 'West South Central'
            when 'AZ' then 'Mountain'            when 'CO' then 'Mountain'
            when 'ID' then 'Mountain'            when 'NM' then 'Mountain'
            when 'MT' then 'Mountain'            when 'UT' then 'Mountain'
            when 'NV' then 'Mountain'            when 'WY' then 'Mountain'
            when 'AK' then 'Pacific'             when 'CA' then 'Pacific'
            when 'HI' then 'Pacific'             when 'OR' then 'Pacific'
            when 'WA' then 'Pacific'
            else 'Territory / Other'
        end                                         as census_division,

        -- ── Urban/rural classification ────────────────────────────────────────
        -- Derived from the state-average RUCA code across all providers.
        -- Individual provider RUCA is in dim_providers.urban_rural_classification.
        case
            when avg_ruca_code <  4 then 'Metropolitan'
            when avg_ruca_code <  7 then 'Micropolitan'
            when avg_ruca_code < 10 then 'Small Town'
            else                         'Rural'
        end                                         as urban_rural_classification

    from state_ruca
)

select * from enriched
