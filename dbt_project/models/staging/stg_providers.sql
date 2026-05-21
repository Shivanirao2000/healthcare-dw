with source as (
    select * from {{ source('raw', 'raw_providers') }}
),
renamed as (
    select
        rndrng_npi                          as provider_npi,
        rndrng_prvdr_last_org_name          as last_org_name,
        rndrng_prvdr_first_name             as first_name,
        rndrng_prvdr_mi                     as middle_initial,
        rndrng_prvdr_crdntls                as credentials,
        rndrng_prvdr_ent_cd                 as entity_code,
        rndrng_prvdr_st1                    as address_line_1,
        rndrng_prvdr_st2                    as address_line_2,
        rndrng_prvdr_city                   as city,
        rndrng_prvdr_state_abrvtn           as state_abbr,
        rndrng_prvdr_state_fips             as state_fips,
        rndrng_prvdr_zip5                   as zip5,
        rndrng_prvdr_ruca                   as ruca_code,
        rndrng_prvdr_ruca_desc              as ruca_description,
        rndrng_prvdr_cntry                  as country,
        rndrng_prvdr_type                   as specialty,
        rndrng_prvdr_mdcr_prtcptg_ind       as medicare_participating_ind,
        data_year,
        load_date                           as _source_load_date,
        current_timestamp()::timestamp_ntz  as _loaded_at
    from source
)
select * from renamed
