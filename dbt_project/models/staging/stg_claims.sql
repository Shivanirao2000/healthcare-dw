with source as (
    select * from {{ source('raw', 'raw_claims') }}
),
renamed as (
    select
        rndrng_npi                          as provider_npi,
        hcpcs_cd                            as hcpcs_code,
        place_of_srvc                       as place_of_service,
        tot_benes                           as beneficiary_count,
        tot_srvcs                           as total_services,
        tot_bene_day_srvcs                  as beneficiary_day_services,
        avg_sbmtd_chrg                      as avg_submitted_charge,
        avg_mdcr_alowd_amt                  as avg_medicare_allowed_amount,
        avg_mdcr_pymt_amt                   as avg_medicare_payment,
        avg_mdcr_stdzd_amt                  as avg_medicare_standardized_amount,
        data_year,
        load_date                           as _source_load_date,
        current_timestamp()::timestamp_ntz  as _loaded_at
    from source
)
select * from renamed
