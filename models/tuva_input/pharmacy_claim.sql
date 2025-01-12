{% set exists_pharmacy_header_current = check_table_exists('raw', 'pharmacy_header_current') %}
{% set exists_pharmacy_detail_current = check_table_exists('raw', 'pharmacy_detail_current') %}
{% set exists_pharmacy_header_historical = check_table_exists('raw', 'pharmacy_header_historical') %}
{% set exists_pharmacy_detail_historical = check_table_exists('raw', 'pharmacy_detail_historical') %}

with
{% if exists_pharmacy_header_current and exists_pharmacy_detail_current %}
pharmacy_current as (
    select distinct
        cast(headerc.bill_id as varchar) as claim_id,
        cast(detailc.line_number as integer) as claim_line_number,
        case
            when headerc.patient_account_number is null or trim(headerc.patient_account_number) = ''
            then lpad(
                cast(
                    (
                        hash(
                            concat_ws(
                                '||',
                                coalesce(headerc.employee_mailing_city, ''),
                                coalesce(headerc.employee_mailing_state_code, ''),
                                coalesce(headerc.employee_mailing_postal_code, ''),
                                coalesce(headerc.employee_mailing_country, ''),
                                coalesce(cast(headerc.employee_date_of_birth as varchar), ''),
                                coalesce(headerc.employee_gender_code, '')
                            ),
                            'xxhash64'
                        ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
            )
            else patient_account_number
        end as person_id,
        cast(null as varchar) as member_id,
        cast(null as varchar) as payer,
        cast(null as varchar) as plan,
        cast(headerc.billing_provider_national as varchar) as prescribing_provider_npi,
        cast(coalesce(detailc.rendering_line_provider, headerc.rendering_bill_provider_4) as varchar) as dispensing_provider_npi,
        cast(detailc.prescription_line_date as date) as dispensing_date,
        cast(detailc.ndc_billed_code as varchar) as ndc_code,
        cast(detailc.drugs_supplies_quantity as integer) as quantity,
        cast(detailc.drugs_supplies_number_of as integer) as days_supply,
        cast(null as integer) as refills,
        cast(headerc.date_insurer_paid_bill as date) as paid_date,
        cast(detailc.total_amount_paid_per_line as float) as paid_amount,
        cast(null as float) as allowed_amount,
        cast(null as float) as charge_amount,
        cast(null as float) as coinsurance_amount,
        cast(headerc.total_amount_paid_per_bill as float) as copayment_amount,
        cast(null as float) as deductible_amount,
        cast(null as integer) as in_network_flag,
        'Texas Department of Insurance, Division of Workers Compensation (DWC)' as data_source,
        'pharmacy_current' as file_name,
        now() as ingest_datetime
    from {{ source('raw', 'pharmacy_header_current') }} headerc
    join {{ source('raw', 'pharmacy_detail_current') }} detailc ON headerc.bill_id = detailc.bill_id
),
{% endif %}

{% if exists_pharmacy_header_historical and exists_pharmacy_detail_historical %}
pharm_historical as (
    select distinct
        cast(headerh.bill_id as varchar) as claim_id,
        cast(detailh.line_number as integer) as claim_line_number,
        case
            when headerh.patient_account_number is null or trim(headerh.patient_account_number) = ''
            then lpad(
                cast(
                    (
                        hash(
                            concat_ws(
                                '||',
                                coalesce(headerh.employee_mailing_city, ''),
                                coalesce(headerh.employee_mailing_state_code, ''),
                                coalesce(headerh.employee_mailing_postal_code, ''),
                                coalesce(headerh.employee_mailing_country, ''),
                                coalesce(cast(headerh.employee_date_of_birth as varchar), ''),
                                coalesce(headerh.employee_gender_code, '')
                            ),
                            'xxhash64'
                        ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
            )
            else patient_account_number
        end as person_id,
        cast(null as varchar) as member_id,
        cast(null as varchar) as payer,
        cast(null as varchar) as plan,
        cast(null as varchar) as prescribing_provider_npi,
        cast(headerh.rendering_bill_provider_4 as varchar) as dispensing_provider_npi,
        cast(detailh.prescription_line_date as date) as dispensing_date,
        cast(detailh.ndc_billed_code as varchar) as ndc_code,
        cast(detailh.drugs_supplies_quantity as integer) as quantity,
        cast(detailh.drugs_supplies_number_of as integer) as days_supply,
        cast(null as integer) as refills,
        cast(headerh.date_insurer_paid_bill as date) as paid_date,
        cast(detailh.total_amount_paid_per_line as float) as paid_amount,
        cast(null as float) as allowed_amount,
        cast(null as float) as charge_amount,
        cast(null as float) as coinsurance_amount,
        cast(headerh.total_amount_paid_per_bill as float) as copayment_amount,
        cast(null as float) as deductible_amount,
        cast(null as integer) as in_network_flag,
        'Texas Department of Insurance, Division of Workers Compensation (DWC)' as data_source,
        'pharmacy_historical' as file_name,
        now() as ingest_datetime
    from {{ source('raw', 'pharmacy_header_historical') }} headerh
    join {{ source('raw', 'pharmacy_detail_historical') }} detailh ON headerh.bill_id = detailh.bill_id
)
{% endif %}

select *
from
{% if (exists_pharmacy_header_current and exists_pharmacy_detail_current) or (exists_pharmacy_header_historical and exists_pharmacy_detail_historical) %}
    (
    {% if exists_pharmacy_header_current and exists_pharmacy_detail_current and exists_pharmacy_header_historical and exists_pharmacy_detail_historical %}
        select * from pharmacy_current
        union
        select * from pharm_historical
    {% elif exists_pharmacy_header_current and exists_pharmacy_detail_current %}
        select * from pharmacy_current
    {% elif exists_pharmacy_header_historical and exists_pharmacy_detail_historical %}
        select * from pharm_historical
    {% endif %}
    )
{% endif %}