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

UNION

SELECT DISTINCT
    CAST(pde.PDE_ID AS VARCHAR) AS claim_id,
    1 AS claim_line_number,
    CAST(pde.BENE_ID AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CAST(NULL AS VARCHAR) AS payer,
    CAST(NULL AS VARCHAR) AS plan,
    CAST(pde.PRSCRBR_ID AS VARCHAR) AS prescribing_provider_npi,
    CAST(NULL AS VARCHAR) AS dispensing_provider_npi,
    CAST(
        SUBSTRING(pde.SRVC_DT, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(pde.SRVC_DT, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(pde.SRVC_DT, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(pde.SRVC_DT, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(pde.SRVC_DT, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(pde.SRVC_DT, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(pde.SRVC_DT, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(pde.SRVC_DT, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(pde.SRVC_DT, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(pde.SRVC_DT, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(pde.SRVC_DT, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(pde.SRVC_DT, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(pde.SRVC_DT, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(pde.SRVC_DT, 1, 2)
        AS DATE
    ) AS dispensing_date,
    CAST(pde.PROD_SRVC_ID AS VARCHAR) AS ndc_code,
    CAST(pde.QTY_DSPNSD_NUM AS INTEGER) AS quantity,
    CAST(pde.DAYS_SUPLY_NUM AS INTEGER) AS days_supply,
    CAST(NULL AS INTEGER) AS refills,
    CAST(
        SUBSTRING(pde.PD_DT, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(pde.PD_DT, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(pde.PD_DT, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(pde.PD_DT, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(pde.PD_DT, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(pde.PD_DT, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(pde.PD_DT, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(pde.PD_DT, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(pde.PD_DT, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(pde.PD_DT, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(pde.PD_DT, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(pde.PD_DT, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(pde.PD_DT, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(pde.PD_DT, 1, 2)
        AS DATE
    ) AS paid_date,
    CAST(pde.CVRD_D_PLAN_PD_AMT AS FLOAT) AS paid_amount,
    CAST(pde.TOT_RX_CST_AMT AS FLOAT) AS allowed_amount,
    CAST(pde.CVRD_D_PLAN_PD_AMT AS FLOAT) AS coinsurance_amount,
    CAST(pde.NCVRD_PLAN_PD_AMT AS FLOAT) AS copayment_amount,
    CAST(NULL AS FLOAT) AS deductible_amount,
    'CMS Synthetic Medicare Enrollment, Fee-for-Service Claims, and Prescription Drug Event Data' AS data_source
FROM {{ source('ffs', 'pde') }} pde