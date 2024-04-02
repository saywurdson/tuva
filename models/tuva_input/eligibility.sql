-- models/eligibility.sql

SELECT DISTINCT
    CAST(MD5(inst.employee_date_of_birth || inst.employee_gender_code) AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CAST(inst.employee_gender_code AS VARCHAR) AS gender,
    CAST(NULL AS VARCHAR) AS race,
    CAST(inst.employee_date_of_birth AS DATE) AS birth_date,
    CAST(NULL AS DATE) AS death_date,
    CAST(FALSE AS BOOLEAN) AS death_flag,
    CAST(NULL AS DATE) AS enrollment_start_date,
    CAST(NULL AS DATE) AS enrollment_end_date,
    CAST(NULL AS VARCHAR) AS payer,
    'self-insured' AS payer_type,
    CAST(NULL AS VARCHAR) AS plan,
    CAST(NULL AS VARCHAR) AS original_reason_entitlement_code,
    'NA' AS dual_status_code,
    CAST(NULL AS VARCHAR) AS medicare_status_code,
    CAST(NULL AS VARCHAR) AS first_name,
    CAST(NULL AS VARCHAR) AS last_name,
    CAST(NULL AS VARCHAR) AS address,
    CAST(inst.employee_mailing_city AS VARCHAR) AS city,
    CAST(inst.employee_mailing_state_code AS VARCHAR) AS state,
    CAST(inst.billing_provider_postal_code AS VARCHAR) AS zip_code,
    CAST(NULL AS VARCHAR) AS phone,
    'Texas Department of Insurance, Division of Workers Compensation (DWC)' AS data_source
FROM {{ source('tx', 'institutional_header') }} inst

UNION

SELECT DISTINCT
    CAST(MD5(prof.employee_date_of_birth || prof.employee_gender_code) AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CAST(prof.employee_gender_code AS VARCHAR) AS gender,
    CAST(NULL AS VARCHAR) AS race,
    CAST(prof.employee_date_of_birth AS DATE) AS birth_date,
    CAST(NULL AS DATE) AS death_date,
    CAST(FALSE AS BOOLEAN) AS death_flag,
    CAST(NULL AS DATE) AS enrollment_start_date,
    CAST(NULL AS DATE) AS enrollment_end_date,
    CAST(NULL AS VARCHAR) AS payer,
    'self-insured' AS payer_type,
    CAST(NULL AS VARCHAR) AS plan,
    CAST(NULL AS VARCHAR) AS original_reason_entitlement_code,
    'NA' AS dual_status_code,
    CAST(NULL AS VARCHAR) AS medicare_status_code,
    CAST(NULL AS VARCHAR) AS first_name,
    CAST(NULL AS VARCHAR) AS last_name,
    CAST(NULL AS VARCHAR) AS address,
    CAST(prof.employee_mailing_city AS VARCHAR) AS city,
    CAST(prof.employee_mailing_state_code AS VARCHAR) AS state,
    CAST(prof.billing_provider_postal_code AS VARCHAR) AS zip_code,
    CAST(NULL AS VARCHAR) AS phone,
    'Texas Department of Insurance, Division of Workers Compensation (DWC)' AS data_source
FROM {{ source('tx', 'professional_header') }} prof

UNION

SELECT DISTINCT
    CAST(MD5(pharm.employee_date_of_birth || pharm.employee_gender_code) AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CAST(pharm.employee_gender_code AS VARCHAR) AS gender,
    CAST(NULL AS VARCHAR) AS race,
    CAST(pharm.employee_date_of_birth AS DATE) AS birth_date,
    CAST(NULL AS DATE) AS death_date,
    CAST(FALSE AS BOOLEAN) AS death_flag,
    CAST(NULL AS DATE) AS enrollment_start_date,
    CAST(NULL AS DATE) AS enrollment_end_date,
    CAST(NULL AS VARCHAR) AS payer,
    'self-insured' AS payer_type,
    CAST(NULL AS VARCHAR) AS plan,
    CAST(NULL AS VARCHAR) AS original_reason_entitlement_code,
    'NA' AS dual_status_code,
    CAST(NULL AS VARCHAR) AS medicare_status_code,
    CAST(NULL AS VARCHAR) AS first_name,
    CAST(NULL AS VARCHAR) AS last_name,
    CAST(NULL AS VARCHAR) AS address,
    CAST(pharm.employee_mailing_city AS VARCHAR) AS city,
    CAST(pharm.employee_mailing_state_code AS VARCHAR) AS state,
    CAST(pharm.billing_provider_postal_code AS VARCHAR) AS zip_code,
    CAST(NULL AS VARCHAR) AS phone,
    'Texas Department of Insurance, Division of Workers Compensation (DWC)' AS data_source
FROM {{ source('tx', 'pharmacy_header') }} pharm