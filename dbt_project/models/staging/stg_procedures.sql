with source as (
    select * from {{ source('raw', 'raw_procedures') }}
),
renamed as (
    select
        hcpcs_cd                            as hcpcs_code,
        hcpcs_desc                          as hcpcs_description,
        hcpcs_drug_ind                      as drug_indicator,
        data_year,
        load_date                           as _source_load_date,
        current_timestamp()::timestamp_ntz  as _loaded_at
    from source
)
select * from renamed
