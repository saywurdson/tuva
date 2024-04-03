-- models/pharmacy_claim.sql

SELECT DISTINCT
    CAST(headerc.bill_id AS VARCHAR) AS claim_id,
    CAST(detailc.line_number AS INTEGER) AS claim_line_number,
    CAST(MD5(headerc.employee_date_of_birth || headerc.employee_gender_code) AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CAST(NULL AS VARCHAR) AS payer,
    CAST(NULL AS VARCHAR) AS plan,
    CAST(headerc.billing_provider_national AS VARCHAR) AS prescribing_provider_npi,
    CAST(COALESCE(detailc.rendering_line_provider, headerc.rendering_bill_provider_4) AS VARCHAR) AS dispensing_provider_npi,
    CAST(detailc.prescription_line_date AS DATE) AS dispensing_date,
    CAST(detailc.ndc_billed_code AS VARCHAR) AS ndc_code,
    CAST(detailc.drugs_supplies_quantity AS INTEGER) AS quantity,
    CAST(detailc.drugs_supplies_number_of AS INTEGER) AS days_supply,
    CAST(NULL AS INTEGER) AS refills,
    CAST(headerc.date_insurer_paid_bill AS DATE) AS paid_date,
    CAST(detailc.total_amount_paid_per_line AS FLOAT) AS paid_amount,
    CAST(NULL AS FLOAT) AS allowed_amount,
    CAST(NULL AS FLOAT) AS coinsurance_amount,
    CAST(headerc.total_amount_paid_per_bill AS FLOAT) AS copayment_amount,
    CAST(NULL AS FLOAT) AS deductible_amount,
    'Texas Department of Insurance, Division of Workers Compensation (DWC)' AS data_source
FROM {{ source('tx', 'pharmacy_header_current') }} headerc
LEFT JOIN {{ source('tx', 'pharmacy_detail_current') }} detailc ON headerc.bill_id = detailc.bill_id

UNION

SELECT DISTINCT
    CAST(headerh.bill_id AS VARCHAR) AS claim_id,
    CAST(detailh.line_number AS INTEGER) AS claim_line_number,
    CAST(MD5(headerh.employee_date_of_birth || headerh.employee_gender_code) AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CAST(NULL AS VARCHAR) AS payer,
    CAST(NULL AS VARCHAR) AS plan,
    CAST(NULL AS VARCHAR) AS prescribing_provider_npi,
    CAST(headerh.rendering_bill_provider_4 AS VARCHAR) AS dispensing_provider_npi,
    CAST(detailh.prescription_line_date AS DATE) AS dispensing_date,
    CAST(detailh.ndc_billed_code AS VARCHAR) AS ndc_code,
    CAST(detailh.drugs_supplies_quantity AS INTEGER) AS quantity,
    CAST(detailh.drugs_supplies_number_of AS INTEGER) AS days_supply,
    CAST(NULL AS INTEGER) AS refills,
    CAST(headerh.date_insurer_paid_bill AS DATE) AS paid_date,
    CAST(detailh.total_amount_paid_per_line AS FLOAT) AS paid_amount,
    CAST(NULL AS FLOAT) AS allowed_amount,
    CAST(NULL AS FLOAT) AS coinsurance_amount,
    CAST(headerh.total_amount_paid_per_bill AS FLOAT) AS copayment_amount,
    CAST(NULL AS FLOAT) AS deductible_amount,
    'Texas Department of Insurance, Division of Workers Compensation (DWC)' AS data_source
FROM {{ source('tx', 'pharmacy_header_historical') }} headerh
LEFT JOIN {{ source('tx', 'pharmacy_detail_historical') }} detailh ON headerh.bill_id = detailh.bill_id