{% set exists_institutional_header_current = check_table_exists('raw', 'institutional_header_current') %}
{% set exists_institutional_header_historical = check_table_exists('raw', 'institutional_header_historical') %}
{% set exists_professional_header_current = check_table_exists('raw', 'professional_header_current') %}
{% set exists_professional_header_historical = check_table_exists('raw', 'professional_header_historical') %}
{% set exists_pharmacy_header_current = check_table_exists('raw', 'pharmacy_header_current') %}
{% set exists_pharmacy_header_historical = check_table_exists('raw', 'pharmacy_header_historical') %}

with
{% if exists_institutional_header_current %}
inst_current as (
    select distinct
        case
            when patient_account_number is null or trim(patient_account_number) = ''
            then lpad(
                cast(
                    (
                        hash(
                            concat_ws(
                                '||',
                                coalesce(employee_mailing_city, ''),
                                coalesce(employee_mailing_state_code, ''),
                                coalesce(employee_mailing_postal_code, ''),
                                coalesce(employee_mailing_country, ''),
                                coalesce(cast(employee_date_of_birth as varchar), ''),
                                coalesce(employee_gender_code, '')
                            ),
                            'xxhash64'
                        ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
            )
            else patient_account_number
        end as patient_id,
        cast(null as varchar) as member_id,
        cast(null as varchar) as subscriber_id,
        case
            when employee_gender_code = 'M' then 'male'
            when employee_gender_code = 'F' then 'female'
            else 'unknown'
        end as gender,
        cast(null as varchar) as race,
        cast(employee_date_of_birth as date) as birth_date,
        cast(null as date) as death_date,
        cast(false as boolean) as death_flag,
        cast(null as date) as enrollment_start_date,
        cast(null as date) as enrollment_end_date,
        cast(null as varchar) as payer,
        cast(null as varchar) as payer_type,
        cast(null as varchar) as plan,
        cast(null as varchar) as original_reason_entitlement_code,
        cast(null as varchar) as dual_status_code,
        cast(null as varchar) as medicare_status_code,
        cast(null as varchar) as first_name,
        cast(null as varchar) as last_name,
        cast(null as varchar) as social_security_number,
        cast(null as varchar) as subscriber_relation,
        cast(null as varchar) as address,
        cast(employee_mailing_city as varchar) as city,
        cast(employee_mailing_state_code as varchar) as state,
        cast(employee_mailing_postal_code as varchar) as zip_code,
        cast(null as varchar) as phone,
        'Texas Department of Insurance, Division of Workers Compensation (DWC)' as data_source,
        'institutional_header_current' as file_name,
        now() as ingest_datetime
    from {{ source('raw', 'institutional_header_current') }}
),
{% endif %}

{% if exists_institutional_header_historical %}
inst_historical as (
    select distinct
        case
            when patient_account_number is null or trim(patient_account_number) = ''
            then lpad(
                cast(
                    (
                        hash(
                            concat_ws(
                                '||',
                                coalesce(employee_mailing_city, ''),
                                coalesce(employee_mailing_state_code, ''),
                                coalesce(employee_mailing_postal_code, ''),
                                coalesce(employee_mailing_country, ''),
                                coalesce(cast(employee_date_of_birth as varchar), ''),
                                coalesce(employee_gender_code, '')
                            ),
                            'xxhash64'
                        ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
            )
            else patient_account_number
        end as patient_id,
        cast(null as varchar) as member_id,
        cast(null as varchar) as subscriber_id,
        case
            when employee_gender_code = 'M' then 'male'
            when employee_gender_code = 'F' then 'female'
            else 'unknown'
        end as gender,
        cast(null as varchar) as race,
        cast(employee_date_of_birth as date) as birth_date,
        cast(null as date) as death_date,
        cast(false as boolean) as death_flag,
        cast(null as date) as enrollment_start_date,
        cast(null as date) as enrollment_end_date,
        cast(null as varchar) as payer,
        cast(null as varchar) as payer_type,
        cast(null as varchar) as plan,
        cast(null as varchar) as original_reason_entitlement_code,
        cast(null as varchar) as dual_status_code,
        cast(null as varchar) as medicare_status_code,
        cast(null as varchar) as first_name,
        cast(null as varchar) as last_name,
        cast(null as varchar) as social_security_number,
        cast(null as varchar) as subscriber_relation,
        cast(null as varchar) as address,
        cast(employee_mailing_city as varchar) as city,
        cast(employee_mailing_state_code as varchar) as state,
        cast(employee_mailing_postal_code as varchar) as zip_code,
        cast(null as varchar) as phone,
        'Texas Department of Insurance, Division of Workers Compensation (DWC)' as data_source,
        'institutional_header_historical' as file_name,
        now() as ingest_datetime
    from {{ source('raw', 'institutional_header_historical') }}
),
{% endif %}

{% if exists_professional_header_current %}
prof_current as (
    select distinct
        case
            when patient_account_number is null or trim(patient_account_number) = ''
            then lpad(
                cast(
                    (
                        hash(
                            concat_ws(
                                '||',
                                coalesce(employee_mailing_city, ''),
                                coalesce(employee_mailing_state_code, ''),
                                coalesce(employee_mailing_postal_code, ''),
                                coalesce(employee_mailing_country, ''),
                                coalesce(cast(employee_date_of_birth as varchar), ''),
                                coalesce(employee_gender_code, '')
                            ),
                            'xxhash64'
                        ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
            )
            else patient_account_number
        end as patient_id,
        cast(null as varchar) as member_id,
        cast(null as varchar) as subscriber_id,
        case
            when employee_gender_code = 'M' then 'male'
            when employee_gender_code = 'F' then 'female'
            else 'unknown'
        end as gender,
        cast(null as varchar) as race,
        cast(employee_date_of_birth as date) as birth_date,
        cast(null as date) as death_date,
        cast(false as boolean) as death_flag,
        cast(null as date) as enrollment_start_date,
        cast(null as date) as enrollment_end_date,
        cast(null as varchar) as payer,
        cast(null as varchar) as payer_type,
        cast(null as varchar) as plan,
        cast(null as varchar) as original_reason_entitlement_code,
        cast(null as varchar) as dual_status_code,
        cast(null as varchar) as medicare_status_code,
        cast(null as varchar) as first_name,
        cast(null as varchar) as last_name,
        cast(null as varchar) as social_security_number,
        cast(null as varchar) as subscriber_relation,
        cast(null as varchar) as address,
        cast(employee_mailing_city as varchar) as city,
        cast(employee_mailing_state_code as varchar) as state,
        cast(billing_provider_postal_code as varchar) as zip_code,
        cast(null as varchar) as phone,
        'Texas Department of Insurance, Division of Workers Compensation (DWC)' as data_source,
        'professional_header_current' as file_name,
        now() as ingest_datetime
    from {{ source('raw', 'professional_header_current') }}
),
{% endif %}

{% if exists_professional_header_historical %}
prof_historical as (
    select distinct
        case
            when patient_account_number is null or trim(patient_account_number) = ''
            then lpad(
                cast(
                    (
                        hash(
                            concat_ws(
                                '||',
                                coalesce(employee_mailing_city, ''),
                                coalesce(employee_mailing_state_code, ''),
                                coalesce(employee_mailing_postal_code, ''),
                                coalesce(employee_mailing_country, ''),
                                coalesce(cast(employee_date_of_birth as varchar), ''),
                                coalesce(employee_gender_code, '')
                            ),
                            'xxhash64'
                        ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
            )
            else patient_account_number
        end as patient_id,
        cast(null as varchar) as member_id,
        cast(null as varchar) as subscriber_id,
        case
            when employee_gender_code = 'M' then 'male'
            when employee_gender_code = 'F' then 'female'
            else 'unknown'
        end as gender,
        cast(null as varchar) as race,
        cast(employee_date_of_birth as date) as birth_date,
        cast(null as date) as death_date,
        cast(false as boolean) as death_flag,
        cast(null as date) as enrollment_start_date,
        cast(null as date) as enrollment_end_date,
        cast(null as varchar) as payer,
        cast(null as varchar) as payer_type,
        cast(null as varchar) as plan,
        cast(null as varchar) as original_reason_entitlement_code,
        cast(null as varchar) as dual_status_code,
        cast(null as varchar) as medicare_status_code,
        cast(null as varchar) as first_name,
        cast(null as varchar) as last_name,
        cast(null as varchar) as social_security_number,
        cast(null as varchar) as subscriber_relation,
        cast(null as varchar) as address,
        cast(employee_mailing_city as varchar) as city,
        cast(employee_mailing_state_code as varchar) as state,
        cast(billing_provider_postal_code as varchar) as zip_code,
        cast(null as varchar) as phone,
        'Texas Department of Insurance, Division of Workers Compensation (DWC)' as data_source,
        'professional_header_historical' as file_name,
        now() as ingest_datetime
    from {{ source('raw', 'professional_header_historical') }}
),
{% endif %}

{% if exists_pharmacy_header_current %}
pharm_current as (
    select distinct
        case
            when patient_account_number is null or trim(patient_account_number) = ''
            then lpad(
                cast(
                    (
                        hash(
                            concat_ws(
                                '||',
                                coalesce(employee_mailing_city, ''),
                                coalesce(employee_mailing_state_code, ''),
                                coalesce(employee_mailing_postal_code, ''),
                                coalesce(employee_mailing_country, ''),
                                coalesce(cast(employee_date_of_birth as varchar), ''),
                                coalesce(employee_gender_code, '')
                            ),
                            'xxhash64'
                        ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
            )
            else patient_account_number
        end as patient_id,
        cast(null as varchar) as member_id,
        cast(null as varchar) as subscriber_id,
        case
            when employee_gender_code = 'M' then 'male'
            when employee_gender_code = 'F' then 'female'
            else 'unknown'
        end as gender,
        cast(null as varchar) as race,
        cast(employee_date_of_birth as date) as birth_date,
        cast(null as date) as death_date,
        cast(false as boolean) as death_flag,
        cast(null as date) as enrollment_start_date,
        cast(null as date) as enrollment_end_date,
        cast(null as varchar) as payer,
        cast(null as varchar) as payer_type,
        cast(null as varchar) as plan,
        cast(null as varchar) as original_reason_entitlement_code,
        cast(null as varchar) as dual_status_code,
        cast(null as varchar) as medicare_status_code,
        cast(null as varchar) as first_name,
        cast(null as varchar) as last_name,
        cast(null as varchar) as social_security_number,
        cast(null as varchar) as subscriber_relation,
        cast(null as varchar) as address,
        cast(employee_mailing_city as varchar) as city,
        cast(employee_mailing_state_code as varchar) as state,
        cast(billing_provider_postal_code as varchar) as zip_code,
        cast(null as varchar) as phone,
        'Texas Department of Insurance, Division of Workers Compensation (DWC)' as data_source,
        'pharmacy_header_current' as file_name,
        now() as ingest_datetime
    from {{ source('raw', 'pharmacy_header_current') }}
),
{% endif %}

{% if exists_pharmacy_header_historical %}
pharm_historical as (
    select distinct
        case
            when patient_account_number is null or trim(patient_account_number) = ''
            then lpad(
                cast(
                    (
                        hash(
                            concat_ws(
                                '||',
                                coalesce(employee_mailing_city, ''),
                                coalesce(employee_mailing_state_code, ''),
                                coalesce(employee_mailing_postal_code, ''),
                                coalesce(employee_mailing_country, ''),
                                coalesce(cast(employee_date_of_birth as varchar), ''),
                                coalesce(employee_gender_code, '')
                            ),
                            'xxhash64'
                        ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
            )
            else patient_account_number
        end as patient_id,
        cast(null as varchar) as member_id,
        cast(null as varchar) as subscriber_id,
        case
            when employee_gender_code = 'M' then 'male'
            when employee_gender_code = 'F' then 'female'
            else 'unknown'
        end as gender,
        cast(null as varchar) as race,
        cast(employee_date_of_birth as date) as birth_date,
        cast(null as date) as death_date,
        cast(false as boolean) as death_flag,
        cast(null as date) as enrollment_start_date,
        cast(null as date) as enrollment_end_date,
        cast(null as varchar) as payer,
        cast(null as varchar) as payer_type,
        cast(null as varchar) as plan,
        cast(null as varchar) as original_reason_entitlement_code,
        cast(null as varchar) as dual_status_code,
        cast(null as varchar) as medicare_status_code,
        cast(null as varchar) as first_name,
        cast(null as varchar) as last_name,
        cast(null as varchar) as social_security_number,
        cast(null as varchar) as subscriber_relation,
        cast(null as varchar) as address,
        cast(employee_mailing_city as varchar) as city,
        cast(employee_mailing_state_code as varchar) as state,
        cast(billing_provider_postal_code as varchar) as zip_code,
        cast(null as varchar) as phone,
        'Texas Department of Insurance, Division of Workers Compensation (DWC)' as data_source,
        'pharmacy_header_historical' as file_name,
        now() as ingest_datetime
    from {{ source('raw', 'pharmacy_header_historical') }}
)
{% endif %}

select *
from
{% if exists_institutional_header_current or exists_institutional_header_historical %}
    (
    {% if exists_institutional_header_current and exists_institutional_header_historical %}
        select * from inst_current
        union
        select * from inst_historical
    {% elif exists_institutional_header_current %}
        select * from inst_current
    {% elif exists_institutional_header_historical %}
        select * from inst_historical
    {% endif %}
    )
{% endif %}

{% if (exists_institutional_header_current or exists_institutional_header_historical) 
      and (exists_professional_header_current or exists_professional_header_historical) %}
    union
{% endif %}

{% if exists_professional_header_current or exists_professional_header_historical %}
    (
    {% if exists_professional_header_current and exists_professional_header_historical %}
        select * from prof_current
        union
        select * from prof_historical
    {% elif exists_professional_header_current %}
        select * from prof_current
    {% elif exists_professional_header_historical %}
        select * from prof_historical
    {% endif %}
    )
{% endif %}

{% if 
      ( (exists_institutional_header_current or exists_institutional_header_historical)
        or (exists_professional_header_current or exists_professional_header_historical) )
      and (exists_pharmacy_header_current or exists_pharmacy_header_historical) %}
    union
{% endif %}

{% if exists_pharmacy_header_current or exists_pharmacy_header_historical %}
    (
    {% if exists_pharmacy_header_current and exists_pharmacy_header_historical %}
        select * from pharm_current
        union
        select * from pharm_historical
    {% elif exists_pharmacy_header_current %}
        select * from pharm_current
    {% elif exists_pharmacy_header_historical %}
        select * from pharm_historical
    {% endif %}
    )
{% endif %}