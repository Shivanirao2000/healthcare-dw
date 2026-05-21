{{
  config(materialized='table')
}}

with procedures as (
    select * from {{ ref('stg_procedures') }}
),

-- Keep the most recent year's descriptor for each HCPCS code.
-- CMS occasionally revises descriptions between release years.
latest as (
    select *
    from procedures
    qualify row_number() over (
        partition by hcpcs_code
        order by data_year desc
    ) = 1
),

final as (
    select
        -- ── Primary key ───────────────────────────────────────────────────────
        hcpcs_code,

        -- ── Description ───────────────────────────────────────────────────────
        hcpcs_description,
        drug_indicator,

        -- ── HCPCS level ───────────────────────────────────────────────────────
        -- Level I  (CPT): first character is a digit (00000–99999)
        -- Level II (HCPCS): first character is a letter (A–V)
        case
            when regexp_like(hcpcs_code, '^[0-9].*') then 'Level I (CPT)'
            else                                           'Level II (HCPCS)'
        end                                         as hcpcs_level,

        -- ── Category ──────────────────────────────────────────────────────────
        -- Level II categories are determined by the first letter.
        -- Level I (CPT) categories are determined by numeric range per AMA convention.
        case
            -- Level II letter-coded categories
            when left(hcpcs_code, 1) = 'A' then 'Transportation & Medical Supplies'
            when left(hcpcs_code, 1) = 'B' then 'Enteral & Parenteral Therapy'
            when left(hcpcs_code, 1) = 'C' then 'Hospital Outpatient PPS'
            when left(hcpcs_code, 1) = 'D' then 'Dental Procedures'
            when left(hcpcs_code, 1) = 'E' then 'Durable Medical Equipment'
            when left(hcpcs_code, 1) = 'G' then 'Professional Services (Temporary)'
            when left(hcpcs_code, 1) = 'H' then 'Behavioral Health & Substance Use'
            when left(hcpcs_code, 1) = 'J' then 'Drugs & Biologics (Injectable)'
            when left(hcpcs_code, 1) = 'K' then 'DME (SADMERC Temporary)'
            when left(hcpcs_code, 1) = 'L' then 'Orthotics & Prosthetics'
            when left(hcpcs_code, 1) = 'M' then 'Medical Services'
            when left(hcpcs_code, 1) = 'P' then 'Pathology & Laboratory'
            when left(hcpcs_code, 1) = 'Q' then 'Temporary Codes'
            when left(hcpcs_code, 1) = 'R' then 'Diagnostic Radiology'
            when left(hcpcs_code, 1) = 'S' then 'Temporary National Codes'
            when left(hcpcs_code, 1) = 'T' then 'Medicaid State Codes'
            when left(hcpcs_code, 1) = 'V' then 'Vision, Hearing & Speech'
            -- Level I CPT numeric ranges (first 2 digits)
            when left(hcpcs_code, 2) between '00' and '09' then 'Anesthesia'
            when left(hcpcs_code, 2) between '10' and '69' then 'Surgery'
            when left(hcpcs_code, 2) between '70' and '79' then 'Radiology'
            when left(hcpcs_code, 2) between '80' and '89' then 'Pathology & Lab (CPT)'
            when left(hcpcs_code, 2) between '90' and '99' then 'Evaluation, Management & Other'
            else                                                 'Other'
        end                                         as hcpcs_category,

        -- ── Audit ─────────────────────────────────────────────────────────────
        data_year                                   as source_data_year,
        _loaded_at

    from latest
)

select * from final
