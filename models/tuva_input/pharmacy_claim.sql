-- models/pharmacy_claim.sql

SELECT DISTINCT
    CAST(header.bill_id AS VARCHAR) AS claim_id,
    CAST(detail.line_number AS INTEGER) AS claim_line_number,
    CAST(MD5(header.employee_date_of_birth || header.employee_gender_code) AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CAST(NULL AS VARCHAR) AS payer,
    CAST(NULL AS VARCHAR) AS plan,
    CAST(header.billing_provider_national AS VARCHAR) AS prescribing_provider_npi,
    CAST(COALESCE(detail.rendering_line_provider ,header.rendering_bill_provider_4) AS VARCHAR) AS dispensing_provider_npi,
    CAST(detail.prescription_line_date AS DATE) AS dispensing_date,
    CAST(detail.ndc_billed_code AS VARCHAR) AS ndc_code,
    CAST(detail.drugs_supplies_quantity AS INTEGER) AS quantity,
    CAST(detail.drugs_supplies_number_of AS INTEGER) AS days_supply,
    CAST(NULL AS INTEGER) AS refills,
    CAST(header.date_insurer_paid_bill AS DATE) AS paid_date,
    CAST(detail.total_amount_paid_per_line AS FLOAT) AS paid_amount,
    CAST(NULL AS FLOAT) AS allowed_amount,
    CAST(NULL AS FLOAT) AS coinsurance_amount,
    CAST(header.total_amount_paid_per_bill AS FLOAT) AS copayment_amount,
    CAST(NULL AS FLOAT) AS deductible_amount,
    'Texas Department of Insurance, Division of Workers Compensation (DWC)' AS data_source
FROM {{ source('tx', 'pharmacy_header') }} header
LEFT JOIN {{ source('tx', 'pharmacy_detail') }} detail ON header.bill_id = detail.bill_id