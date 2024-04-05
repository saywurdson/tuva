-- models/medical_claim.sql

SELECT DISTINCT
    CAST(ihc.bill_id AS VARCHAR) AS claim_id,
    CAST(idc.line_number AS INTEGER) AS claim_line_number,
    'institutional' AS claim_type,
    CAST(MD5(ihc.employee_date_of_birth || ihc.employee_gender_code) AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CAST(NULL AS VARCHAR) AS payer,
    CAST(NULL AS VARCHAR) AS plan,
    CAST(ihc.reporting_period_start_date AS DATE) AS claim_start_date,
    CAST(ihc.reporting_period_end_date AS DATE) AS claim_end_date,
    CAST(idc.service_line_from_date AS DATE) AS claim_line_start_date,
    CAST(idc.service_line_to_date AS DATE) AS claim_line_end_date,
    CAST(COALESCE(ihc.admission_date, ihc.service_bill_from_date) AS DATE) AS admission_date,
    CAST(COALESCE(ihc.discharge_date, ihc.service_bill_to_date) AS DATE) AS discharge_date,
    '1' AS admit_source_code,
    CAST(ihc.admission_type_code AS VARCHAR) AS admit_type_code,
    CAST(NULL AS VARCHAR) AS discharge_disposition_code,
    CAST(ihc.facility_code AS VARCHAR) AS place_of_service_code,
    CAST(NULL AS VARCHAR) AS bill_type_code,
    CAST(ihc.diagnosis_related_group_code AS VARCHAR) AS ms_drg_code,
    CAST(NULL AS VARCHAR) AS apr_drg_code,
    CAST(idc.revenue_billed_code AS VARCHAR) AS revenue_center_code,
    CAST(NULL AS INTEGER) AS service_unit_quantity,
    CAST(idc.hcpcs_line_procedure_billed AS VARCHAR) AS hcpcs_code,
    CAST(idc.first_hcpcs_modifier_billed AS VARCHAR) AS hcpcs_modifier_1,
    CAST(idc.second_hcpcs_modifier_billed AS VARCHAR) AS hcpcs_modifier_2,
    CAST(NULL AS VARCHAR) AS hcpcs_modifier_3,
    CAST(NULL AS VARCHAR) AS hcpcs_modifier_4,
    CAST(NULL AS VARCHAR) AS hcpcs_modifier_5,
    CAST(NULL AS VARCHAR) AS rendering_npi,
    CAST(ihc.referring_provider_national AS VARCHAR) AS billing_npi,
    CAST(ihc.facility_national_provider AS VARCHAR) AS facility_npi,
    CAST(ihc.date_insurer_paid_bill AS DATE) AS paid_date,
    CAST(idc.total_amount_paid_per_line AS FLOAT) AS paid_amount,
    CAST(NULL AS FLOAT) AS allowed_amount,
    CAST(idc.total_charge_per_line AS FLOAT) AS charge_amount,
    CAST(NULL AS FLOAT) AS coinsurance_amount,
    CAST(NULL AS FLOAT) AS copayment_amount,
    CAST(NULL AS FLOAT) AS deductible_amount,
    CAST(ihc.total_charge_per_bill AS FLOAT) AS total_cost_amount,
    'icd-10-cm' AS diagnosis_code_type,
    CAST(REPLACE(ihc.admitting_diagnosis_code, '.', '') AS VARCHAR) AS diagnosis_code_1,
    CAST(REPLACE(ihc.first_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_2,
    CAST(REPLACE(ihc.second_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_3,
    CAST(REPLACE(ihc.third_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_4,
    CAST(REPLACE(ihc.fourth_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_5,
    CAST(REPLACE(ihc.fifth_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_6,
    CAST(NULL AS VARCHAR) AS diagnosis_code_7,
    CAST(NULL AS VARCHAR) AS diagnosis_code_8,
    CAST(NULL AS VARCHAR) AS diagnosis_code_9,
    CAST(NULL AS VARCHAR) AS diagnosis_code_10,
    CAST(NULL AS VARCHAR) AS diagnosis_code_11,
    CAST(NULL AS VARCHAR) AS diagnosis_code_12,
    CAST(NULL AS VARCHAR) AS diagnosis_code_13,
    CAST(NULL AS VARCHAR) AS diagnosis_code_14,
    CAST(NULL AS VARCHAR) AS diagnosis_code_15,
    CAST(NULL AS VARCHAR) AS diagnosis_code_16,
    CAST(NULL AS VARCHAR) AS diagnosis_code_17,
    CAST(NULL AS VARCHAR) AS diagnosis_code_18,
    CAST(NULL AS VARCHAR) AS diagnosis_code_19,
    CAST(NULL AS VARCHAR) AS diagnosis_code_20,
    CAST(NULL AS VARCHAR) AS diagnosis_code_21,
    CAST(NULL AS VARCHAR) AS diagnosis_code_22,
    CAST(NULL AS VARCHAR) AS diagnosis_code_23,
    CAST(NULL AS VARCHAR) AS diagnosis_code_24,
    CAST(NULL AS VARCHAR) AS diagnosis_code_25,
    'Y' AS diagnosis_poa_1,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_2,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_3,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_4,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_5,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_6,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_7,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_8,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_9,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_10,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_11,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_12,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_13,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_14,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_15,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_16,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_17,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_18,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_19,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_20,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_21,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_22,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_23,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_24,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_25,
    'icd-10-cm' AS procedure_code_type,
    CAST(COALESCE(ihc.icd_9cm_or_icd_10cm_principal, ihc.first_icd_9cm_or_icd_10cm_1) AS VARCHAR) AS procedure_code_1,
    CAST(ihc.second_icd_9cm_or_icd_10cm_1 AS VARCHAR) AS procedure_code_2,
    CAST(ihc.third_icd_9cm_or_icd_10cm_1 AS VARCHAR) AS procedure_code_3,
    CAST(ihc.fourth_icd_9cm_or_icd_10cm_1 AS VARCHAR) AS procedure_code_4,
    CAST(NULL AS VARCHAR) AS procedure_code_5,
    CAST(NULL AS VARCHAR) AS procedure_code_6,
    CAST(NULL AS VARCHAR) AS procedure_code_7,
    CAST(NULL AS VARCHAR) AS procedure_code_8,
    CAST(NULL AS VARCHAR) AS procedure_code_9,
    CAST(NULL AS VARCHAR) AS procedure_code_10,
    CAST(NULL AS VARCHAR) AS procedure_code_11,
    CAST(NULL AS VARCHAR) AS procedure_code_12,
    CAST(NULL AS VARCHAR) AS procedure_code_13,
    CAST(NULL AS VARCHAR) AS procedure_code_14,
    CAST(NULL AS VARCHAR) AS procedure_code_15,
    CAST(NULL AS VARCHAR) AS procedure_code_16,
    CAST(NULL AS VARCHAR) AS procedure_code_17,
    CAST(NULL AS VARCHAR) AS procedure_code_18,
    CAST(NULL AS VARCHAR) AS procedure_code_19,
    CAST(NULL AS VARCHAR) AS procedure_code_20,
    CAST(NULL AS VARCHAR) AS procedure_code_21,
    CAST(NULL AS VARCHAR) AS procedure_code_22,
    CAST(NULL AS VARCHAR) AS procedure_code_23,
    CAST(NULL AS VARCHAR) AS procedure_code_24,
    CAST(NULL AS VARCHAR) AS procedure_code_25,
    CAST(COALESCE(ihc.principal_procedure_date, ihc.first_procedure_date) AS DATE) AS procedure_date_1,
    CAST(ihc.second_procedure_date AS DATE) AS procedure_date_2,
    CAST(ihc.third_procedure_date AS DATE) AS procedure_date_3,
    CAST(ihc.fourth_procedure_date AS DATE) AS procedure_date_4,
    CAST(NULL AS DATE) AS procedure_date_5,
    CAST(NULL AS DATE) AS procedure_date_6,
    CAST(NULL AS DATE) AS procedure_date_7,
    CAST(NULL AS DATE) AS procedure_date_8,
    CAST(NULL AS DATE) AS procedure_date_9,
    CAST(NULL AS DATE) AS procedure_date_10,
    CAST(NULL AS DATE) AS procedure_date_11,
    CAST(NULL AS DATE) AS procedure_date_12,
    CAST(NULL AS DATE) AS procedure_date_13,
    CAST(NULL AS DATE) AS procedure_date_14,
    CAST(NULL AS DATE) AS procedure_date_15,
    CAST(NULL AS DATE) AS procedure_date_16,
    CAST(NULL AS DATE) AS procedure_date_17,
    CAST(NULL AS DATE) AS procedure_date_18,
    CAST(NULL AS DATE) AS procedure_date_19,
    CAST(NULL AS DATE) AS procedure_date_20,
    CAST(NULL AS DATE) AS procedure_date_21,
    CAST(NULL AS DATE) AS procedure_date_22,
    CAST(NULL AS DATE) AS procedure_date_23,
    CAST(NULL AS DATE) AS procedure_date_24,
    CAST(NULL AS DATE) AS procedure_date_25,
    'Texas Department of Insurance, Division of Workers Compensation (DWC)' AS data_source
FROM {{ source('tx', 'institutional_header_current') }} ihc
LEFT JOIN {{ source('tx', 'institutional_detail_current') }} idc ON ihc.bill_id = idc.bill_id

UNION

SELECT DISTINCT
    CAST(ihh.bill_id AS VARCHAR) AS claim_id,
    CAST(idh.line_number AS INTEGER) AS claim_line_number,
    'institutional' AS claim_type,
    CAST(MD5(ihh.employee_date_of_birth || ihh.employee_gender_code) AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CAST(NULL AS VARCHAR) AS payer,
    CAST(NULL AS VARCHAR) AS plan,
    CAST(ihh.reporting_period_start_date AS DATE) AS claim_start_date,
    CAST(ihh.reporting_period_end_date AS DATE) AS claim_end_date,
    CAST(idh.service_line_from_date AS DATE) AS claim_line_start_date,
    CAST(idh.service_line_to_date AS DATE) AS claim_line_end_date,
    CAST(COALESCE(ihh.admission_date, ihh.service_bill_from_date) AS DATE) AS admission_date,
    CAST(COALESCE(ihh.discharge_date, ihh.service_bill_to_date) AS DATE) AS discharge_date,
    '1' AS admit_source_code,
    CAST(ihh.admission_type_code AS VARCHAR) AS admit_type_code,
    CAST(NULL AS VARCHAR) AS discharge_disposition_code,
    CAST(ihh.facility_code AS VARCHAR) AS place_of_service_code,
    CAST(NULL AS VARCHAR) AS bill_type_code,
    CAST(ihh.diagnosis_related_group_code AS VARCHAR) AS ms_drg_code,
    CAST(NULL AS VARCHAR) AS apr_drg_code,
    CAST(idh.revenue_billed_code AS VARCHAR) AS revenue_center_code,
    CAST(NULL AS INTEGER) AS service_unit_quantity,
    CAST(idh.hcpcs_line_procedure_billed AS VARCHAR) AS hcpcs_code,
    CAST(idh.first_hcpcs_modifier_billed AS VARCHAR) AS hcpcs_modifier_1,
    CAST(idh.second_hcpcs_modifier_billed AS VARCHAR) AS hcpcs_modifier_2,
    CAST(NULL AS VARCHAR) AS hcpcs_modifier_3,
    CAST(NULL AS VARCHAR) AS hcpcs_modifier_4,
    CAST(NULL AS VARCHAR) AS hcpcs_modifier_5,
    CAST(NULL AS VARCHAR) AS rendering_npi,
    CAST(ihh.referring_provider_national AS VARCHAR) AS billing_npi,
    CAST(ihh.facility_national_provider AS VARCHAR) AS facility_npi,
    CAST(ihh.date_insurer_paid_bill AS DATE) AS paid_date,
    CAST(idh.total_amount_paid_per_line AS FLOAT) AS paid_amount,
    CAST(NULL AS FLOAT) AS allowed_amount,
    CAST(idh.total_charge_per_line AS FLOAT) AS charge_amount,
    CAST(NULL AS FLOAT) AS coinsurance_amount,
    CAST(NULL AS FLOAT) AS copayment_amount,
    CAST(NULL AS FLOAT) AS deductible_amount,
    CAST(ihh.total_charge_per_bill AS FLOAT) AS total_cost_amount,
    'icd-10-cm' AS diagnosis_code_type,
    CAST(REPLACE(ihh.admitting_diagnosis_code, '.', '') AS VARCHAR) AS diagnosis_code_1,
    CAST(REPLACE(ihh.first_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_2,
    CAST(REPLACE(ihh.second_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_3,
    CAST(REPLACE(ihh.third_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_4,
    CAST(REPLACE(ihh.fourth_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_5,
    CAST(REPLACE(ihh.fifth_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_6,
    CAST(NULL AS VARCHAR) AS diagnosis_code_7,
    CAST(NULL AS VARCHAR) AS diagnosis_code_8,
    CAST(NULL AS VARCHAR) AS diagnosis_code_9,
    CAST(NULL AS VARCHAR) AS diagnosis_code_10,
    CAST(NULL AS VARCHAR) AS diagnosis_code_11,
    CAST(NULL AS VARCHAR) AS diagnosis_code_12,
    CAST(NULL AS VARCHAR) AS diagnosis_code_13,
    CAST(NULL AS VARCHAR) AS diagnosis_code_14,
    CAST(NULL AS VARCHAR) AS diagnosis_code_15,
    CAST(NULL AS VARCHAR) AS diagnosis_code_16,
    CAST(NULL AS VARCHAR) AS diagnosis_code_17,
    CAST(NULL AS VARCHAR) AS diagnosis_code_18,
    CAST(NULL AS VARCHAR) AS diagnosis_code_19,
    CAST(NULL AS VARCHAR) AS diagnosis_code_20,
    CAST(NULL AS VARCHAR) AS diagnosis_code_21,
    CAST(NULL AS VARCHAR) AS diagnosis_code_22,
    CAST(NULL AS VARCHAR) AS diagnosis_code_23,
    CAST(NULL AS VARCHAR) AS diagnosis_code_24,
    CAST(NULL AS VARCHAR) AS diagnosis_code_25,
    'Y' AS diagnosis_poa_1,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_2,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_3,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_4,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_5,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_6,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_7,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_8,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_9,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_10,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_11,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_12,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_13,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_14,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_15,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_16,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_17,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_18,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_19,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_20,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_21,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_22,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_23,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_24,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_25,
    'icd-10-cm' AS procedure_code_type,
    CAST(COALESCE(ihh.icd_9cm_or_icd_10cm_principal, ihh.first_icd_9cm_or_icd_10cm_1) AS VARCHAR) AS procedure_code_1,
    CAST(ihh.second_icd_9cm_or_icd_10cm_1 AS VARCHAR) AS procedure_code_2,
    CAST(ihh.third_icd_9cm_or_icd_10cm_1 AS VARCHAR) AS procedure_code_3,
    CAST(ihh.fourth_icd_9cm_or_icd_10cm_1 AS VARCHAR) AS procedure_code_4,
    CAST(NULL AS VARCHAR) AS procedure_code_5,
    CAST(NULL AS VARCHAR) AS procedure_code_6,
    CAST(NULL AS VARCHAR) AS procedure_code_7,
    CAST(NULL AS VARCHAR) AS procedure_code_8,
    CAST(NULL AS VARCHAR) AS procedure_code_9,
    CAST(NULL AS VARCHAR) AS procedure_code_10,
    CAST(NULL AS VARCHAR) AS procedure_code_11,
    CAST(NULL AS VARCHAR) AS procedure_code_12,
    CAST(NULL AS VARCHAR) AS procedure_code_13,
    CAST(NULL AS VARCHAR) AS procedure_code_14,
    CAST(NULL AS VARCHAR) AS procedure_code_15,
    CAST(NULL AS VARCHAR) AS procedure_code_16,
    CAST(NULL AS VARCHAR) AS procedure_code_17,
    CAST(NULL AS VARCHAR) AS procedure_code_18,
    CAST(NULL AS VARCHAR) AS procedure_code_19,
    CAST(NULL AS VARCHAR) AS procedure_code_20,
    CAST(NULL AS VARCHAR) AS procedure_code_21,
    CAST(NULL AS VARCHAR) AS procedure_code_22,
    CAST(NULL AS VARCHAR) AS procedure_code_23,
    CAST(NULL AS VARCHAR) AS procedure_code_24,
    CAST(NULL AS VARCHAR) AS procedure_code_25,
    CAST(COALESCE(ihh.principal_procedure_date, ihh.first_procedure_date) AS DATE) AS procedure_date_1,
    CAST(ihh.second_procedure_date AS DATE) AS procedure_date_2,
    CAST(ihh.third_procedure_date AS DATE) AS procedure_date_3,
    CAST(ihh.fourth_procedure_date AS DATE) AS procedure_date_4,
    CAST(NULL AS DATE) AS procedure_date_5,
    CAST(NULL AS DATE) AS procedure_date_6,
    CAST(NULL AS DATE) AS procedure_date_7,
    CAST(NULL AS DATE) AS procedure_date_8,
    CAST(NULL AS DATE) AS procedure_date_9,
    CAST(NULL AS DATE) AS procedure_date_10,
    CAST(NULL AS DATE) AS procedure_date_11,
    CAST(NULL AS DATE) AS procedure_date_12,
    CAST(NULL AS DATE) AS procedure_date_13,
    CAST(NULL AS DATE) AS procedure_date_14,
    CAST(NULL AS DATE) AS procedure_date_15,
    CAST(NULL AS DATE) AS procedure_date_16,
    CAST(NULL AS DATE) AS procedure_date_17,
    CAST(NULL AS DATE) AS procedure_date_18,
    CAST(NULL AS DATE) AS procedure_date_19,
    CAST(NULL AS DATE) AS procedure_date_20,
    CAST(NULL AS DATE) AS procedure_date_21,
    CAST(NULL AS DATE) AS procedure_date_22,
    CAST(NULL AS DATE) AS procedure_date_23,
    CAST(NULL AS DATE) AS procedure_date_24,
    CAST(NULL AS DATE) AS procedure_date_25,
    'Texas Department of Insurance, Division of Workers Compensation (DWC)' AS data_source
FROM {{ source('tx', 'institutional_header_historical') }} ihh
LEFT JOIN {{ source('tx', 'institutional_detail_historical') }} idh ON ihh.bill_id = idh.bill_id

UNION

SELECT DISTINCT
    CAST(ph.bill_id AS VARCHAR) AS claim_id,
    CAST(pd.line_number AS INTEGER) AS claim_line_number,
    'professional' AS claim_type,
    CAST(MD5(ph.employee_date_of_birth || ph.employee_gender_code) AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CAST(NULL AS VARCHAR) AS payer,
    CAST(NULL AS VARCHAR) AS plan,
    CAST(ph.reporting_period_start_date AS DATE) AS claim_start_date,
    CAST(ph.reporting_period_end_date AS DATE) AS claim_end_date,
    CAST(pd.service_line_from_date AS DATE) AS claim_line_start_date,
    CAST(pd.service_line_to_date AS DATE) AS claim_line_end_date,
    CAST(NULL AS DATE) AS admission_date,
    CAST(NULL AS DATE) AS discharge_date,
    CAST(NULL AS VARCHAR) AS admit_source_code,
    CAST(NULL AS VARCHAR) AS admit_type_code,
    CAST(NULL AS VARCHAR) AS discharge_disposition_code,
    CAST(pd.place_of_service_line_code AS VARCHAR) AS place_of_service_code,
    CAST(NULL AS VARCHAR) AS bill_type_code,
    CAST(NULL AS VARCHAR) AS ms_drg_code,
    CAST(NULL AS VARCHAR) AS apr_drg_code,
    CAST(NULL AS VARCHAR) AS revenue_center_code,
    CAST(NULL AS INTEGER) AS service_unit_quantity,
    CAST(pd.hcpcs_line_procedure_billed AS VARCHAR) AS hcpcs_code,
    CAST(pd.first_hcpcs_modifier_billed AS VARCHAR) AS hcpcs_modifier_1,
    CAST(pd.second_hcpcs_modifier_billed AS VARCHAR) AS hcpcs_modifier_2,
    CAST(NULL AS VARCHAR) AS hcpcs_modifier_3,
    CAST(NULL AS VARCHAR) AS hcpcs_modifier_4,
    CAST(NULL AS VARCHAR) AS hcpcs_modifier_5,
    CAST(pd.rendering_line_provider AS VARCHAR) AS rendering_npi,
    CAST(ph.billing_provider_national AS VARCHAR) AS billing_npi,
    CAST(ph.facility_national_provider AS VARCHAR) AS facility_npi,
    CAST(ph.date_insurer_paid_bill AS DATE) AS paid_date,
    CAST(ph.total_amount_paid_per_bill AS FLOAT) AS paid_amount,
    CAST(NULL AS FLOAT) AS allowed_amount,
    CAST(pd.total_charge_per_line AS FLOAT) AS charge_amount,
    CAST(NULL AS FLOAT) AS coinsurance_amount,
    CAST(NULL AS FLOAT) AS copayment_amount,
    CAST(NULL AS FLOAT) AS deductible_amount,
    CAST(ph.total_charge_per_bill AS FLOAT) AS total_cost_amount,
    'icd-10-cm' AS diagnosis_code_type,
    CAST(REPLACE(ph.first_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_1,
    CAST(REPLACE(ph.second_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_2,
    CAST(REPLACE(ph.third_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_3,
    CAST(REPLACE(ph.fourth_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_4,
    CAST(REPLACE(ph.fifth_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_5,
    CAST(NULL AS VARCHAR) AS diagnosis_code_6,
    CAST(NULL AS VARCHAR) AS diagnosis_code_7,
    CAST(NULL AS VARCHAR) AS diagnosis_code_8,
    CAST(NULL AS VARCHAR) AS diagnosis_code_9,
    CAST(NULL AS VARCHAR) AS diagnosis_code_10,
    CAST(NULL AS VARCHAR) AS diagnosis_code_11,
    CAST(NULL AS VARCHAR) AS diagnosis_code_12,
    CAST(NULL AS VARCHAR) AS diagnosis_code_13,
    CAST(NULL AS VARCHAR) AS diagnosis_code_14,
    CAST(NULL AS VARCHAR) AS diagnosis_code_15,
    CAST(NULL AS VARCHAR) AS diagnosis_code_16,
    CAST(NULL AS VARCHAR) AS diagnosis_code_17,
    CAST(NULL AS VARCHAR) AS diagnosis_code_18,
    CAST(NULL AS VARCHAR) AS diagnosis_code_19,
    CAST(NULL AS VARCHAR) AS diagnosis_code_20,
    CAST(NULL AS VARCHAR) AS diagnosis_code_21,
    CAST(NULL AS VARCHAR) AS diagnosis_code_22,
    CAST(NULL AS VARCHAR) AS diagnosis_code_23,
    CAST(NULL AS VARCHAR) AS diagnosis_code_24,
    CAST(NULL AS VARCHAR) AS diagnosis_code_25,
    'Y' AS diagnosis_poa_1,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_2,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_3,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_4,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_5,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_6,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_7,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_8,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_9,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_10,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_11,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_12,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_13,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_14,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_15,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_16,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_17,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_18,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_19,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_20,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_21,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_22,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_23,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_24,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_25,
    'icd-10-cm' AS procedure_code_type,
    CAST(NULL AS VARCHAR) AS procedure_code_1,
    CAST(NULL AS VARCHAR) AS procedure_code_2,
    CAST(NULL AS VARCHAR) AS procedure_code_3,
    CAST(NULL AS VARCHAR) AS procedure_code_4,
    CAST(NULL AS VARCHAR) AS procedure_code_5,
    CAST(NULL AS VARCHAR) AS procedure_code_6,
    CAST(NULL AS VARCHAR) AS procedure_code_7,
    CAST(NULL AS VARCHAR) AS procedure_code_8,
    CAST(NULL AS VARCHAR) AS procedure_code_9,
    CAST(NULL AS VARCHAR) AS procedure_code_10,
    CAST(NULL AS VARCHAR) AS procedure_code_11,
    CAST(NULL AS VARCHAR) AS procedure_code_12,
    CAST(NULL AS VARCHAR) AS procedure_code_13,
    CAST(NULL AS VARCHAR) AS procedure_code_14,
    CAST(NULL AS VARCHAR) AS procedure_code_15,
    CAST(NULL AS VARCHAR) AS procedure_code_16,
    CAST(NULL AS VARCHAR) AS procedure_code_17,
    CAST(NULL AS VARCHAR) AS procedure_code_18,
    CAST(NULL AS VARCHAR) AS procedure_code_19,
    CAST(NULL AS VARCHAR) AS procedure_code_20,
    CAST(NULL AS VARCHAR) AS procedure_code_21,
    CAST(NULL AS VARCHAR) AS procedure_code_22,
    CAST(NULL AS VARCHAR) AS procedure_code_23,
    CAST(NULL AS VARCHAR) AS procedure_code_24,
    CAST(NULL AS VARCHAR) AS procedure_code_25,
    CAST(NULL AS DATE) AS procedure_date_1,
    CAST(NULL AS DATE) AS procedure_date_2,
    CAST(NULL AS DATE) AS procedure_date_3,
    CAST(NULL AS DATE) AS procedure_date_4,
    CAST(NULL AS DATE) AS procedure_date_5,
    CAST(NULL AS DATE) AS procedure_date_6,
    CAST(NULL AS DATE) AS procedure_date_7,
    CAST(NULL AS DATE) AS procedure_date_8,
    CAST(NULL AS DATE) AS procedure_date_9,
    CAST(NULL AS DATE) AS procedure_date_10,
    CAST(NULL AS DATE) AS procedure_date_11,
    CAST(NULL AS DATE) AS procedure_date_12,
    CAST(NULL AS DATE) AS procedure_date_13,
    CAST(NULL AS DATE) AS procedure_date_14,
    CAST(NULL AS DATE) AS procedure_date_15,
    CAST(NULL AS DATE) AS procedure_date_16,
    CAST(NULL AS DATE) AS procedure_date_17,
    CAST(NULL AS DATE) AS procedure_date_18,
    CAST(NULL AS DATE) AS procedure_date_19,
    CAST(NULL AS DATE) AS procedure_date_20,
    CAST(NULL AS DATE) AS procedure_date_21,
    CAST(NULL AS DATE) AS procedure_date_22,
    CAST(NULL AS DATE) AS procedure_date_23,
    CAST(NULL AS DATE) AS procedure_date_24,
    CAST(NULL AS DATE) AS procedure_date_25,
    'Texas Department of Insurance, Division of Workers Compensation (DWC)' AS data_source
FROM {{ source('tx', 'professional_header_current') }} ph
LEFT JOIN {{ source('tx', 'professional_detail_current') }} pd ON ph.bill_id = pd.bill_id

UNION

SELECT DISTINCT
    CAST(phh.bill_id AS VARCHAR) AS claim_id,
    CAST(pdh.line_number AS INTEGER) AS claim_line_number,
    'professional' AS claim_type,
    CAST(MD5(phh.employee_date_of_birth || phh.employee_gender_code) AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CAST(NULL AS VARCHAR) AS payer,
    CAST(NULL AS VARCHAR) AS plan,
    CAST(phh.reporting_period_start_date AS DATE) AS claim_start_date,
    CAST(phh.reporting_period_end_date AS DATE) AS claim_end_date,
    CAST(pdh.service_line_from_date AS DATE) AS claim_line_start_date,
    CAST(pdh.service_line_to_date AS DATE) AS claim_line_end_date,
    CAST(NULL AS DATE) AS admission_date,
    CAST(NULL AS DATE) AS discharge_date,
    CAST(NULL AS VARCHAR) AS admit_source_code,
    CAST(NULL AS VARCHAR) AS admit_type_code,
    CAST(NULL AS VARCHAR) AS discharge_disposition_code,
    CAST(pdh.place_of_service_line_code AS VARCHAR) AS place_of_service_code,
    CAST(NULL AS VARCHAR) AS bill_type_code,
    CAST(NULL AS VARCHAR) AS ms_drg_code,
    CAST(NULL AS VARCHAR) AS apr_drg_code,
    CAST(NULL AS VARCHAR) AS revenue_center_code,
    CAST(NULL AS INTEGER) AS service_unit_quantity,
    CAST(pdh.hcpcs_line_procedure_billed AS VARCHAR) AS hcpcs_code,
    CAST(pdh.first_hcpcs_modifier_billed AS VARCHAR) AS hcpcs_modifier_1,
    CAST(pdh.second_hcpcs_modifier_billed AS VARCHAR) AS hcpcs_modifier_2,
    CAST(NULL AS VARCHAR) AS hcpcs_modifier_3,
    CAST(NULL AS VARCHAR) AS hcpcs_modifier_4,
    CAST(NULL AS VARCHAR) AS hcpcs_modifier_5,
    CAST(pdh.rendering_line_provider AS VARCHAR) AS rendering_npi,
    CAST(phh.billing_provider_national AS VARCHAR) AS billing_npi,
    CAST(phh.facility_national_provider AS VARCHAR) AS facility_npi,
    CAST(phh.date_insurer_paid_bill AS DATE) AS paid_date,
    CAST(phh.total_amount_paid_per_bill AS FLOAT) AS paid_amount,
    CAST(NULL AS FLOAT) AS allowed_amount,
    CAST(pdh.total_charge_per_line AS FLOAT) AS charge_amount,
    CAST(NULL AS FLOAT) AS coinsurance_amount,
    CAST(NULL AS FLOAT) AS copayment_amount,
    CAST(NULL AS FLOAT) AS deductible_amount,
    CAST(phh.total_charge_per_bill AS FLOAT) AS total_cost_amount,
    'icd-10-cm' AS diagnosis_code_type,
    CAST(REPLACE(phh.first_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_1,
    CAST(REPLACE(phh.second_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_2,
    CAST(REPLACE(phh.third_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_3,
    CAST(REPLACE(phh.fourth_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_4,
    CAST(REPLACE(phh.fifth_icd_9cm_or_icd_10cm, '.', '') AS VARCHAR) AS diagnosis_code_5,
    CAST(NULL AS VARCHAR) AS diagnosis_code_6,
    CAST(NULL AS VARCHAR) AS diagnosis_code_7,
    CAST(NULL AS VARCHAR) AS diagnosis_code_8,
    CAST(NULL AS VARCHAR) AS diagnosis_code_9,
    CAST(NULL AS VARCHAR) AS diagnosis_code_10,
    CAST(NULL AS VARCHAR) AS diagnosis_code_11,
    CAST(NULL AS VARCHAR) AS diagnosis_code_12,
    CAST(NULL AS VARCHAR) AS diagnosis_code_13,
    CAST(NULL AS VARCHAR) AS diagnosis_code_14,
    CAST(NULL AS VARCHAR) AS diagnosis_code_15,
    CAST(NULL AS VARCHAR) AS diagnosis_code_16,
    CAST(NULL AS VARCHAR) AS diagnosis_code_17,
    CAST(NULL AS VARCHAR) AS diagnosis_code_18,
    CAST(NULL AS VARCHAR) AS diagnosis_code_19,
    CAST(NULL AS VARCHAR) AS diagnosis_code_20,
    CAST(NULL AS VARCHAR) AS diagnosis_code_21,
    CAST(NULL AS VARCHAR) AS diagnosis_code_22,
    CAST(NULL AS VARCHAR) AS diagnosis_code_23,
    CAST(NULL AS VARCHAR) AS diagnosis_code_24,
    CAST(NULL AS VARCHAR) AS diagnosis_code_25,
    'Y' AS diagnosis_poa_1,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_2,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_3,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_4,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_5,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_6,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_7,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_8,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_9,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_10,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_11,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_12,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_13,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_14,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_15,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_16,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_17,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_18,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_19,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_20,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_21,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_22,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_23,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_24,
    CAST(NULL AS VARCHAR) AS diagnosis_poa_25,
    'icd-10-cm' AS procedure_code_type,
    CAST(NULL AS VARCHAR) AS procedure_code_1,
    CAST(NULL AS VARCHAR) AS procedure_code_2,
    CAST(NULL AS VARCHAR) AS procedure_code_3,
    CAST(NULL AS VARCHAR) AS procedure_code_4,
    CAST(NULL AS VARCHAR) AS procedure_code_5,
    CAST(NULL AS VARCHAR) AS procedure_code_6,
    CAST(NULL AS VARCHAR) AS procedure_code_7,
    CAST(NULL AS VARCHAR) AS procedure_code_8,
    CAST(NULL AS VARCHAR) AS procedure_code_9,
    CAST(NULL AS VARCHAR) AS procedure_code_10,
    CAST(NULL AS VARCHAR) AS procedure_code_11,
    CAST(NULL AS VARCHAR) AS procedure_code_12,
    CAST(NULL AS VARCHAR) AS procedure_code_13,
    CAST(NULL AS VARCHAR) AS procedure_code_14,
    CAST(NULL AS VARCHAR) AS procedure_code_15,
    CAST(NULL AS VARCHAR) AS procedure_code_16,
    CAST(NULL AS VARCHAR) AS procedure_code_17,
    CAST(NULL AS VARCHAR) AS procedure_code_18,
    CAST(NULL AS VARCHAR) AS procedure_code_19,
    CAST(NULL AS VARCHAR) AS procedure_code_20,
    CAST(NULL AS VARCHAR) AS procedure_code_21,
    CAST(NULL AS VARCHAR) AS procedure_code_22,
    CAST(NULL AS VARCHAR) AS procedure_code_23,
    CAST(NULL AS VARCHAR) AS procedure_code_24,
    CAST(NULL AS VARCHAR) AS procedure_code_25,
    CAST(NULL AS DATE) AS procedure_date_1,
    CAST(NULL AS DATE) AS procedure_date_2,
    CAST(NULL AS DATE) AS procedure_date_3,
    CAST(NULL AS DATE) AS procedure_date_4,
    CAST(NULL AS DATE) AS procedure_date_5,
    CAST(NULL AS DATE) AS procedure_date_6,
    CAST(NULL AS DATE) AS procedure_date_7,
    CAST(NULL AS DATE) AS procedure_date_8,
    CAST(NULL AS DATE) AS procedure_date_9,
    CAST(NULL AS DATE) AS procedure_date_10,
    CAST(NULL AS DATE) AS procedure_date_11,
    CAST(NULL AS DATE) AS procedure_date_12,
    CAST(NULL AS DATE) AS procedure_date_13,
    CAST(NULL AS DATE) AS procedure_date_14,
    CAST(NULL AS DATE) AS procedure_date_15,
    CAST(NULL AS DATE) AS procedure_date_16,
    CAST(NULL AS DATE) AS procedure_date_17,
    CAST(NULL AS DATE) AS procedure_date_18,
    CAST(NULL AS DATE) AS procedure_date_19,
    CAST(NULL AS DATE) AS procedure_date_20,
    CAST(NULL AS DATE) AS procedure_date_21,
    CAST(NULL AS DATE) AS procedure_date_22,
    CAST(NULL AS DATE) AS procedure_date_23,
    CAST(NULL AS DATE) AS procedure_date_24,
    CAST(NULL AS DATE) AS procedure_date_25,
    'Texas Department of Insurance, Division of Workers Compensation (DWC)' AS data_source
FROM {{ source('tx', 'professional_header_historical') }} phh
LEFT JOIN {{ source('tx', 'professional_detail_historical') }} pdh ON phh.bill_id = pdh.bill_id

UNION

SELECT DISTINCT
    CAST(carrier.CLM_ID AS VARCHAR) AS claim_id,
    CAST(carrier.LINE_NUM AS INTEGER) AS claim_line_number,
    'professional' AS claim_type,
    CAST(carrier.BENE_ID AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CAST(NULL AS VARCHAR) AS payer,
    CAST(NULL AS VARCHAR) AS plan,
    CAST(
        SUBSTRING(carrier.CLM_FROM_DT, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(carrier.CLM_FROM_DT, 1, 2)
        AS DATE
    ) AS claim_start_date,
    CAST(
        SUBSTRING(carrier.CLM_THRU_DT, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(carrier.CLM_THRU_DT, 1, 2)
        AS DATE
    ) AS claim_end_date,
    CAST(
        SUBSTRING(carrier.CLM_FROM_DT, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(carrier.CLM_FROM_DT, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(carrier.CLM_FROM_DT, 1, 2)
        AS DATE
    ) AS claim_line_start_date,
    CAST(
        SUBSTRING(carrier.CLM_THRU_DT, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(carrier.CLM_THRU_DT, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(carrier.CLM_THRU_DT, 1, 2)
        AS DATE
    ) AS claim_line_end_date,
    CAST(
        SUBSTRING(inpatient.CLM_ADMSN_DT, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.CLM_ADMSN_DT, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.CLM_ADMSN_DT, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.CLM_ADMSN_DT, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.CLM_ADMSN_DT, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.CLM_ADMSN_DT, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.CLM_ADMSN_DT, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.CLM_ADMSN_DT, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.CLM_ADMSN_DT, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.CLM_ADMSN_DT, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.CLM_ADMSN_DT, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.CLM_ADMSN_DT, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.CLM_ADMSN_DT, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.CLM_ADMSN_DT, 1, 2)
        AS DATE
    ) AS admission_date,
    CAST(
        SUBSTRING(inpatient.NCH_BENE_DSCHRG_DT, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.NCH_BENE_DSCHRG_DT, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.NCH_BENE_DSCHRG_DT, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.NCH_BENE_DSCHRG_DT, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.NCH_BENE_DSCHRG_DT, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.NCH_BENE_DSCHRG_DT, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.NCH_BENE_DSCHRG_DT, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.NCH_BENE_DSCHRG_DT, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.NCH_BENE_DSCHRG_DT, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.NCH_BENE_DSCHRG_DT, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.NCH_BENE_DSCHRG_DT, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.NCH_BENE_DSCHRG_DT, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.NCH_BENE_DSCHRG_DT, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.NCH_BENE_DSCHRG_DT, 1, 2)
        AS DATE
    ) AS discharge_date,
    CAST(inpatient.CLM_SRC_IP_ADMSN_CD AS VARCHAR) AS admit_source_code,
    CAST(inpatient.CLM_IP_ADMSN_TYPE_CD AS VARCHAR) AS admit_type_code,
    CAST(inpatient.PTNT_DSCHRG_STUS_CD AS VARCHAR) AS discharge_disposition_code,
    CAST(carrier.LINE_PLACE_OF_SRVC_CD AS VARCHAR) AS place_of_service_code,
    CAST(NULL AS VARCHAR) AS bill_type_code,
    CAST(inpatient.CLM_DRG_CD AS VARCHAR) AS ms_drg_code,
    CAST(NULL AS VARCHAR) AS apr_drg_code,
    CAST(inpatient.REV_CNTR AS VARCHAR) AS revenue_center_code,
    CAST(carrier.LINE_SRVC_CNT AS INTEGER) AS service_unit_quantity,
    CAST(carrier.HCPCS_CD AS VARCHAR) AS hcpcs_code,
    CAST(carrier.HCPCS_1ST_MDFR_CD AS VARCHAR) AS hcpcs_modifier_1,
    CAST(carrier.HCPCS_2ND_MDFR_CD AS VARCHAR) AS hcpcs_modifier_2,
    CAST(dme.HCPCS_3RD_MDFR_CD AS VARCHAR) AS hcpcs_modifier_3,
    CAST(dme.HCPCS_4TH_MDFR_CD AS VARCHAR) AS hcpcs_modifier_4,
    CAST(NULL AS VARCHAR) AS hcpcs_modifier_5,
    CAST(carrier.PRF_PHYSN_NPI AS VARCHAR) AS rendering_npi,
    CAST(carrier.RFR_PHYSN_NPI AS VARCHAR) AS billing_npi,
    CAST(carrier.ORG_NPI_NUM AS VARCHAR) AS facility_npi,
    CAST(NULL AS DATE) AS paid_date,
    CAST(carrier.LINE_BENE_PMT_AMT AS FLOAT) AS paid_amount,
    CAST(carrier.LINE_ALOWD_CHRG_AMT AS FLOAT) AS allowed_amount,
    CAST(carrier.LINE_SBMTD_CHRG_AMT AS FLOAT) AS charge_amount,
    CAST(carrier.LINE_COINSRNC_AMT AS FLOAT) AS coinsurance_amount,
    CAST(carrier.LINE_PRVDR_PMT_AMT AS FLOAT) AS copayment_amount,
    CAST(carrier.LINE_BENE_PTB_DDCTBL_AMT AS FLOAT) AS deductible_amount,
    CAST(carrier.CLM_PMT_AMT AS FLOAT) AS total_cost_amount,
    'icd-10-cm' AS diagnosis_code_type,
    CAST(carrier.ICD_DGNS_CD1 AS VARCHAR) AS diagnosis_code_1,
    CAST(carrier.ICD_DGNS_CD2 AS VARCHAR) AS diagnosis_code_2,
    CAST(carrier.ICD_DGNS_CD3 AS VARCHAR) AS diagnosis_code_3,
    CAST(carrier.ICD_DGNS_CD4 AS VARCHAR) AS diagnosis_code_4,
    CAST(carrier.ICD_DGNS_CD5 AS VARCHAR) AS diagnosis_code_5,
    CAST(carrier.ICD_DGNS_CD6 AS VARCHAR) AS diagnosis_code_6,
    CAST(carrier.ICD_DGNS_CD7 AS VARCHAR) AS diagnosis_code_7,
    CAST(carrier.ICD_DGNS_CD8 AS VARCHAR) AS diagnosis_code_8,
    CAST(carrier.ICD_DGNS_CD9 AS VARCHAR) AS diagnosis_code_9,
    CAST(carrier.ICD_DGNS_CD10 AS VARCHAR) AS diagnosis_code_10,
    CAST(carrier.ICD_DGNS_CD11 AS VARCHAR) AS diagnosis_code_11,
    CAST(carrier.ICD_DGNS_CD12 AS VARCHAR) AS diagnosis_code_12,
    CAST(inpatient.ICD_DGNS_CD13 AS VARCHAR) AS diagnosis_code_13,
    CAST(inpatient.ICD_DGNS_CD14 AS VARCHAR) AS diagnosis_code_14,
    CAST(inpatient.ICD_DGNS_CD15 AS VARCHAR) AS diagnosis_code_15,
    CAST(inpatient.ICD_DGNS_CD16 AS VARCHAR) AS diagnosis_code_16,
    CAST(inpatient.ICD_DGNS_CD17 AS VARCHAR) AS diagnosis_code_17,
    CAST(inpatient.ICD_DGNS_CD18 AS VARCHAR) AS diagnosis_code_18,
    CAST(inpatient.ICD_DGNS_CD19 AS VARCHAR) AS diagnosis_code_19,
    CAST(inpatient.ICD_DGNS_CD20 AS VARCHAR) AS diagnosis_code_20,
    CAST(inpatient.ICD_DGNS_CD21 AS VARCHAR) AS diagnosis_code_21,
    CAST(inpatient.ICD_DGNS_CD22 AS VARCHAR) AS diagnosis_code_22,
    CAST(inpatient.ICD_DGNS_CD23 AS VARCHAR) AS diagnosis_code_23,
    CAST(inpatient.ICD_DGNS_CD24 AS VARCHAR) AS diagnosis_code_24,
    CAST(inpatient.ICD_DGNS_CD25 AS VARCHAR) AS diagnosis_code_25,
    CAST(inpatient.CLM_POA_IND_SW1 AS VARCHAR) AS diagnosis_poa_1,
    CAST(inpatient.CLM_POA_IND_SW2 AS VARCHAR) AS diagnosis_poa_2,
    CAST(inpatient.CLM_POA_IND_SW3 AS VARCHAR) AS diagnosis_poa_3,
    CAST(inpatient.CLM_POA_IND_SW4 AS VARCHAR) AS diagnosis_poa_4,
    CAST(inpatient.CLM_POA_IND_SW5 AS VARCHAR) AS diagnosis_poa_5,
    CAST(inpatient.CLM_POA_IND_SW6 AS VARCHAR) AS diagnosis_poa_6,
    CAST(inpatient.CLM_POA_IND_SW7 AS VARCHAR) AS diagnosis_poa_7,
    CAST(inpatient.CLM_POA_IND_SW8 AS VARCHAR) AS diagnosis_poa_8,
    CAST(inpatient.CLM_POA_IND_SW9 AS VARCHAR) AS diagnosis_poa_9,
    CAST(inpatient.CLM_POA_IND_SW10 AS VARCHAR) AS diagnosis_poa_10,
    CAST(inpatient.CLM_POA_IND_SW11 AS VARCHAR) AS diagnosis_poa_11,
    CAST(inpatient.CLM_POA_IND_SW12 AS VARCHAR) AS diagnosis_poa_12,
    CAST(inpatient.CLM_POA_IND_SW13 AS VARCHAR) AS diagnosis_poa_13,
    CAST(inpatient.CLM_POA_IND_SW14 AS VARCHAR) AS diagnosis_poa_14,
    CAST(inpatient.CLM_POA_IND_SW15 AS VARCHAR) AS diagnosis_poa_15,
    CAST(inpatient.CLM_POA_IND_SW16 AS VARCHAR) AS diagnosis_poa_16,
    CAST(inpatient.CLM_POA_IND_SW17 AS VARCHAR) AS diagnosis_poa_17,
    CAST(inpatient.CLM_POA_IND_SW18 AS VARCHAR) AS diagnosis_poa_18,
    CAST(inpatient.CLM_POA_IND_SW19 AS VARCHAR) AS diagnosis_poa_19,
    CAST(inpatient.CLM_POA_IND_SW20 AS VARCHAR) AS diagnosis_poa_20,
    CAST(inpatient.CLM_POA_IND_SW21 AS VARCHAR) AS diagnosis_poa_21,
    CAST(inpatient.CLM_POA_IND_SW22 AS VARCHAR) AS diagnosis_poa_22,
    CAST(inpatient.CLM_POA_IND_SW23 AS VARCHAR) AS diagnosis_poa_23,
    CAST(inpatient.CLM_POA_IND_SW24 AS VARCHAR) AS diagnosis_poa_24,
    CAST(inpatient.CLM_POA_IND_SW25 AS VARCHAR) AS diagnosis_poa_25,
    'icd-10-cm' AS procedure_code_type,
    CAST(inpatient.ICD_PRCDR_CD1 AS VARCHAR) AS procedure_code_1,
    CAST(inpatient.ICD_PRCDR_CD2 AS VARCHAR) AS procedure_code_2,
    CAST(inpatient.ICD_PRCDR_CD3 AS VARCHAR) AS procedure_code_3,
    CAST(inpatient.ICD_PRCDR_CD4 AS VARCHAR) AS procedure_code_4,
    CAST(inpatient.ICD_PRCDR_CD5 AS VARCHAR) AS procedure_code_5,
    CAST(inpatient.ICD_PRCDR_CD6 AS VARCHAR) AS procedure_code_6,
    CAST(inpatient.ICD_PRCDR_CD7 AS VARCHAR) AS procedure_code_7,
    CAST(inpatient.ICD_PRCDR_CD8 AS VARCHAR) AS procedure_code_8,
    CAST(inpatient.ICD_PRCDR_CD9 AS VARCHAR) AS procedure_code_9,
    CAST(inpatient.ICD_PRCDR_CD10 AS VARCHAR) AS procedure_code_10,
    CAST(inpatient.ICD_PRCDR_CD11 AS VARCHAR) AS procedure_code_11,
    CAST(inpatient.ICD_PRCDR_CD12 AS VARCHAR) AS procedure_code_12,
    CAST(inpatient.ICD_PRCDR_CD13 AS VARCHAR) AS procedure_code_13,
    CAST(inpatient.ICD_PRCDR_CD14 AS VARCHAR) AS procedure_code_14,
    CAST(inpatient.ICD_PRCDR_CD15 AS VARCHAR) AS procedure_code_15,
    CAST(inpatient.ICD_PRCDR_CD16 AS VARCHAR) AS procedure_code_16,
    CAST(inpatient.ICD_PRCDR_CD17 AS VARCHAR) AS procedure_code_17,
    CAST(inpatient.ICD_PRCDR_CD18 AS VARCHAR) AS procedure_code_18,
    CAST(inpatient.ICD_PRCDR_CD19 AS VARCHAR) AS procedure_code_19,
    CAST(inpatient.ICD_PRCDR_CD20 AS VARCHAR) AS procedure_code_20,
    CAST(inpatient.ICD_PRCDR_CD21 AS VARCHAR) AS procedure_code_21,
    CAST(inpatient.ICD_PRCDR_CD22 AS VARCHAR) AS procedure_code_22,
    CAST(inpatient.ICD_PRCDR_CD23 AS VARCHAR) AS procedure_code_23,
    CAST(inpatient.ICD_PRCDR_CD24 AS VARCHAR) AS procedure_code_24,
    CAST(inpatient.ICD_PRCDR_CD25 AS VARCHAR) AS procedure_code_25,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT1, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT1, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT1, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT1, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT1, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT1, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT1, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT1, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT1, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT1, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT1, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT1, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT1, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT1, 1, 2)
        AS DATE
    ) AS procedure_date_1,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT2, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT2, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT2, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT2, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT2, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT2, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT2, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT2, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT2, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT2, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT2, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT2, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT2, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT2, 1, 2)
        AS DATE
    ) AS procedure_date_2,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT3, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT3, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT3, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT3, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT3, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT3, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT3, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT3, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT3, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT3, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT3, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT3, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT3, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT3, 1, 2)
        AS DATE
    ) AS procedure_date_3,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT4, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT4, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT4, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT4, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT4, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT4, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT4, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT4, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT4, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT4, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT4, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT4, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT4, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT4, 1, 2)
        AS DATE
    ) AS procedure_date_4,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT5, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT5, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT5, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT5, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT5, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT5, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT5, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT5, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT5, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT5, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT5, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT5, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT5, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT5, 1, 2)
        AS DATE
    ) AS procedure_date_5,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT6, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT6, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT6, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT6, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT6, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT6, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT6, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT6, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT6, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT6, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT6, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT6, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT6, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT6, 1, 2)
        AS DATE
    ) AS procedure_date_6,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT7, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT7, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT7, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT7, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT7, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT7, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT7, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT7, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT7, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT7, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT7, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT7, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT7, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT7, 1, 2)
        AS DATE
    ) AS procedure_date_7,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT8, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT8, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT8, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT8, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT8, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT8, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT8, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT8, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT8, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT8, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT8, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT8, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT8, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT8, 1, 2)
        AS DATE
    ) AS procedure_date_8,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT9, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT9, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT9, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT9, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT9, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT9, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT9, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT9, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT9, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT9, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT9, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT9, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT9, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT9, 1, 2)
        AS DATE
    ) AS procedure_date_9,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT10, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT10, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT10, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT10, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT10, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT10, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT10, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT10, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT10, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT10, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT10, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT10, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT10, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT10, 1, 2)
        AS DATE
    ) AS procedure_date_10,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT11, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT11, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT11, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT11, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT11, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT11, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT11, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT11, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT11, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT11, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT11, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT11, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT11, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT11, 1, 2)
        AS DATE
    ) AS procedure_date_11,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT12, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT12, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT12, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT12, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT12, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT12, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT12, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT12, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT12, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT12, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT12, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT12, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT12, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT12, 1, 2)
        AS DATE
    ) AS procedure_date_12,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT13, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT13, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT13, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT13, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT13, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT13, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT13, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT13, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT13, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT13, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT13, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT13, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT13, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT13, 1, 2)
        AS DATE
    ) AS procedure_date_13,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT14, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT14, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT14, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT14, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT14, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT14, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT14, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT14, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT14, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT14, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT14, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT14, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT14, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT14, 1, 2)
        AS DATE
    ) AS procedure_date_14,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT15, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT15, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT15, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT15, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT15, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT15, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT15, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT15, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT15, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT15, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT15, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT15, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT15, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT15, 1, 2)
        AS DATE
    ) AS procedure_date_15,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT16, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT16, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT16, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT16, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT16, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT16, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT16, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT16, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT16, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT16, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT16, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT16, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT16, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT16, 1, 2)
        AS DATE
    ) AS procedure_date_16,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT17, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT17, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT17, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT17, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT17, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT17, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT17, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT17, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT17, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT17, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT17, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT17, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT17, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT17, 1, 2)
        AS DATE
    ) AS procedure_date_17,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT18, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT18, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT18, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT18, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT18, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT18, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT18, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT18, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT18, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT18, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT18, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT18, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT18, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT18, 1, 2)
        AS DATE
    ) AS procedure_date_18,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT19, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT19, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT19, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT19, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT19, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT19, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT19, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT19, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT19, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT19, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT19, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT19, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT19, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT19, 1, 2)
        AS DATE
    ) AS procedure_date_19,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT20, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT20, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT20, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT20, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT20, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT20, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT20, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT20, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT20, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT20, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT20, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT20, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT20, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT20, 1, 2)
        AS DATE
    ) AS procedure_date_20,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT21, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT21, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT21, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT21, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT21, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT21, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT21, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT21, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT21, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT21, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT21, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT21, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT21, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT21, 1, 2)
        AS DATE
    ) AS procedure_date_21,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT22, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT22, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT22, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT22, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT22, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT22, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT22, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT22, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT22, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT22, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT22, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT22, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT22, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT22, 1, 2)
        AS DATE
    ) AS procedure_date_22,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT23, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT23, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT23, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT23, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT23, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT23, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT23, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT23, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT23, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT23, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT23, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT23, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT23, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT23, 1, 2)
        AS DATE
    ) AS procedure_date_23,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT24, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT24, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT24, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT24, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT24, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT24, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT24, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT24, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT24, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT24, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT24, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT24, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT24, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT24, 1, 2)
        AS DATE
    ) AS procedure_date_24,
    CAST(
        SUBSTRING(inpatient.PRCDR_DT25, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(inpatient.PRCDR_DT25, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(inpatient.PRCDR_DT25, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(inpatient.PRCDR_DT25, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(inpatient.PRCDR_DT25, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(inpatient.PRCDR_DT25, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(inpatient.PRCDR_DT25, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(inpatient.PRCDR_DT25, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(inpatient.PRCDR_DT25, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(inpatient.PRCDR_DT25, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(inpatient.PRCDR_DT25, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(inpatient.PRCDR_DT25, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(inpatient.PRCDR_DT25, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(inpatient.PRCDR_DT25, 1, 2)
        AS DATE
    ) AS procedure_date_25,
    'CMS Synthetic Medicare Enrollment, Fee-for-Service Claims, and Prescription Drug Event Data' AS data_source
FROM {{ source('ffs', 'carrier') }} carrier
JOIN {{ source('ffs', 'inpatient') }} inpatient ON carrier.CLM_ID = inpatient.CLM_ID
JOIN {{ source('ffs', 'dme') }} dme ON carrier.CLM_ID = dme.CLM_ID