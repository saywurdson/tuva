-- models/eligibility.sql

SELECT DISTINCT
    CAST(MD5(instc.employee_date_of_birth || instc.employee_gender_code) AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CASE
        WHEN instc.employee_gender_code = 'M' THEN 'male'
        WHEN instc.employee_gender_code = 'F' THEN 'female'
        ELSE 'unknown'
    END AS gender,
    CAST(NULL AS VARCHAR) AS race,
    CAST(instc.employee_date_of_birth AS DATE) AS birth_date,
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
    CAST(instc.employee_mailing_city AS VARCHAR) AS city,
    CAST(instc.employee_mailing_state_code AS VARCHAR) AS state,
    CAST(instc.billing_provider_postal_code AS VARCHAR) AS zip_code,
    CAST(NULL AS VARCHAR) AS phone,
    'Texas Department of Insurance, Division of Workers Compensation (DWC)' AS data_source
FROM {{ source('tx', 'institutional_header_current') }} instc

UNION

SELECT DISTINCT
    CAST(MD5(insth.employee_date_of_birth || insth.employee_gender_code) AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CASE
        WHEN insth.employee_gender_code = 'M' THEN 'male'
        WHEN insth.employee_gender_code = 'F' THEN 'female'
        ELSE 'unknown'
    END AS gender,
    CAST(NULL AS VARCHAR) AS race,
    CAST(insth.employee_date_of_birth AS DATE) AS birth_date,
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
    CAST(insth.employee_mailing_city AS VARCHAR) AS city,
    CAST(insth.employee_mailing_state_code AS VARCHAR) AS state,
    CAST(insth.billing_provider_postal_code AS VARCHAR) AS zip_code,
    CAST(NULL AS VARCHAR) AS phone,
    'Texas Department of Insurance, Division of Workers Compensation (DWC)' AS data_source
FROM {{ source('tx', 'institutional_header_historical') }} insth

UNION

SELECT DISTINCT
    CAST(MD5(profc.employee_date_of_birth || profc.employee_gender_code) AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CASE
        WHEN profc.employee_gender_code = 'M' THEN 'male'
        WHEN profc.employee_gender_code = 'F' THEN 'female'
        ELSE 'unknown'
    END AS gender,
    CAST(NULL AS VARCHAR) AS race,
    CAST(profc.employee_date_of_birth AS DATE) AS birth_date,
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
    CAST(profc.employee_mailing_city AS VARCHAR) AS city,
    CAST(profc.employee_mailing_state_code AS VARCHAR) AS state,
    CAST(profc.billing_provider_postal_code AS VARCHAR) AS zip_code,
    CAST(NULL AS VARCHAR) AS phone,
    'Texas Department of Insurance, Division of Workers Compensation (DWC)' AS data_source
FROM {{ source('tx', 'professional_header_current') }} profc

UNION

SELECT DISTINCT
    CAST(MD5(profh.employee_date_of_birth || profh.employee_gender_code) AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CASE
        WHEN profh.employee_gender_code = 'M' THEN 'male'
        WHEN profh.employee_gender_code = 'F' THEN 'female'
        ELSE 'unknown'
    END AS gender,
    CAST(NULL AS VARCHAR) AS race,
    CAST(profh.employee_date_of_birth AS DATE) AS birth_date,
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
    CAST(profh.employee_mailing_city AS VARCHAR) AS city,
    CAST(profh.employee_mailing_state_code AS VARCHAR) AS state,
    CAST(profh.billing_provider_postal_code AS VARCHAR) AS zip_code,
    CAST(NULL AS VARCHAR) AS phone,
    'Texas Department of Insurance, Division of Workers Compensation (DWC)' AS data_source
FROM {{ source('tx', 'professional_header_historical') }} profh

UNION

SELECT DISTINCT
    CAST(MD5(phc.employee_date_of_birth || phc.employee_gender_code) AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CASE
        WHEN phc.employee_gender_code = 'M' THEN 'male'
        WHEN phc.employee_gender_code = 'F' THEN 'female'
        ELSE 'unknown'
    END AS gender,
    CAST(NULL AS VARCHAR) AS race,
    CAST(phc.employee_date_of_birth AS DATE) AS birth_date,
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
    CAST(phc.employee_mailing_city AS VARCHAR) AS city,
    CAST(phc.employee_mailing_state_code AS VARCHAR) AS state,
    CAST(phc.billing_provider_postal_code AS VARCHAR) AS zip_code,
    CAST(NULL AS VARCHAR) AS phone,
    'Texas Department of Insurance, Division of Workers Compensation (DWC)' AS data_source
FROM {{ source('tx', 'pharmacy_header_current') }} phc

UNION

SELECT DISTINCT
    CAST(MD5(phh.employee_date_of_birth || phh.employee_gender_code) AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CASE
        WHEN phh.employee_gender_code = 'M' THEN 'male'
        WHEN phh.employee_gender_code = 'F' THEN 'female'
        ELSE 'unknown'
    END AS gender,
    CAST(NULL AS VARCHAR) AS race,
    CAST(phh.employee_date_of_birth AS DATE) AS birth_date,
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
    CAST(phh.employee_mailing_city AS VARCHAR) AS city,
    CAST(phh.employee_mailing_state_code AS VARCHAR) AS state,
    CAST(phh.billing_provider_postal_code AS VARCHAR) AS zip_code,
    CAST(NULL AS VARCHAR) AS phone,
    'Texas Department of Insurance, Division of Workers Compensation (DWC)' AS data_source
FROM {{ source('tx', 'pharmacy_header_historical') }} phh

UNION

SELECT DISTINCT
    CAST(ffs.BENE_ID AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CASE
        WHEN ffs.SEX_IDENT_CD = '1' THEN 'male'
        WHEN ffs.SEX_IDENT_CD = '2' THEN 'female'
        ELSE 'unknown'
    END AS gender,
    CASE
        WHEN ffs.BENE_RACE_CD = '1' THEN 'white'
        WHEN ffs.BENE_RACE_CD = '2' THEN 'black or african american'
        WHEN ffs.BENE_RACE_CD = '3' THEN 'other race'
        WHEN ffs.BENE_RACE_CD = '4' THEN 'asian'
        WHEN ffs.BENE_RACE_CD = '5' THEN 'hispanic'
        WHEN ffs.BENE_RACE_CD = '6' THEN 'american indian or alaska native'
        ELSE 'unknown'
    END AS race,
    CAST(
        SUBSTRING(ffs.BENE_BIRTH_DT, 8, 4) || '-' ||
        CASE 
            WHEN SUBSTRING(ffs.BENE_BIRTH_DT, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(ffs.BENE_BIRTH_DT, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(ffs.BENE_BIRTH_DT, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(ffs.BENE_BIRTH_DT, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(ffs.BENE_BIRTH_DT, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(ffs.BENE_BIRTH_DT, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(ffs.BENE_BIRTH_DT, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(ffs.BENE_BIRTH_DT, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(ffs.BENE_BIRTH_DT, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(ffs.BENE_BIRTH_DT, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(ffs.BENE_BIRTH_DT, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(ffs.BENE_BIRTH_DT, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(ffs.BENE_BIRTH_DT, 1, 2)
        AS DATE
    ) AS birth_date,
    CAST(
        SUBSTRING(ffs.BENE_DEATH_DT, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(ffs.BENE_DEATH_DT, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(ffs.BENE_DEATH_DT, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(ffs.BENE_DEATH_DT, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(ffs.BENE_DEATH_DT, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(ffs.BENE_DEATH_DT, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(ffs.BENE_DEATH_DT, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(ffs.BENE_DEATH_DT, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(ffs.BENE_DEATH_DT, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(ffs.BENE_DEATH_DT, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(ffs.BENE_DEATH_DT, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(ffs.BENE_DEATH_DT, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(ffs.BENE_DEATH_DT, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(ffs.BENE_DEATH_DT, 1, 2)
        AS DATE
    ) AS death_date,
    CASE 
        WHEN ffs.BENE_DEATH_DT IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS death_flag,
    CAST(
        SUBSTRING(ffs.COVSTART, 8, 4) || '-' ||
        CASE
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(ffs.COVSTART, 1, 2)
        AS DATE
    ) AS enrollment_start_date,
    CAST(
        SUBSTRING(ffs.COVSTART, 8, 4) || '-' || 
        CASE 
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Jan' THEN '01'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Feb' THEN '02'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Mar' THEN '03'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Apr' THEN '04'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'May' THEN '05'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Jun' THEN '06'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Jul' THEN '07'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Aug' THEN '08'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Sep' THEN '09'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Oct' THEN '10'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Nov' THEN '11'
            WHEN SUBSTRING(ffs.COVSTART, 4, 3) = 'Dec' THEN '12'
            ELSE '00'
        END || '-' || 
        SUBSTRING(ffs.COVSTART, 1, 2)
    AS DATE) + 
    (CAST(ffs.BENE_HI_CVRAGE_TOT_MONS AS INT) * INTERVAL '1' MONTH) - INTERVAL '1' DAY AS enrollment_end_date,
    CAST(NULL AS VARCHAR) AS payer,
    'medicare' AS payer_type,
    CAST(NULL AS VARCHAR) AS plan,
    CAST(ffs.ENTLMT_RSN_ORIG AS VARCHAR) AS original_reason_entitlement_code,
    CAST(ffs.DUAL_STUS_CD_12 AS VARCHAR) AS dual_status_code,
    CAST(ffs.MDCR_STATUS_CODE_12 AS VARCHAR) AS medicare_status_code,
    CAST(NULL AS VARCHAR) AS first_name,
    CAST(NULL AS VARCHAR) AS last_name,
    CAST(NULL AS VARCHAR) AS address,
    CAST(NULL AS VARCHAR) AS city,
    CASE ffs.STATE_CODE
        WHEN '01' THEN 'Alabama'
        WHEN '02' THEN 'Alaska'
        WHEN '03' THEN 'Arizona'
        WHEN '04' THEN 'Arkansas'
        WHEN '05' THEN 'California'
        WHEN '06' THEN 'Colorado'
        WHEN '07' THEN 'Connecticut'
        WHEN '08' THEN 'Delaware'
        WHEN '09' THEN 'District of Columbia'
        WHEN '10' THEN 'Florida'
        WHEN '11' THEN 'Georgia'
        WHEN '12' THEN 'Hawaii'
        WHEN '13' THEN 'Idaho'
        WHEN '14' THEN 'Illinois'
        WHEN '15' THEN 'Indiana'
        WHEN '16' THEN 'Iowa'
        WHEN '17' THEN 'Kansas'
        WHEN '18' THEN 'Kentucky'
        WHEN '19' THEN 'Louisiana'
        WHEN '20' THEN 'Maine'
        WHEN '21' THEN 'Maryland'
        WHEN '22' THEN 'Massachusetts'
        WHEN '23' THEN 'Michigan'
        WHEN '24' THEN 'Minnesota'
        WHEN '25' THEN 'Mississippi'
        WHEN '26' THEN 'Missouri'
        WHEN '27' THEN 'Montana'
        WHEN '28' THEN 'Nebraska'
        WHEN '29' THEN 'Nevada'
        WHEN '30' THEN 'New Hampshire'
        WHEN '31' THEN 'New Jersey'
        WHEN '32' THEN 'New Mexico'
        WHEN '33' THEN 'New York'
        WHEN '34' THEN 'North Carolina'
        WHEN '35' THEN 'North Dakota'
        WHEN '36' THEN 'Ohio'
        WHEN '37' THEN 'Oklahoma'
        WHEN '38' THEN 'Oregon'
        WHEN '39' THEN 'Pennsylvania'
        WHEN '40' THEN 'Puerto Rico'
        WHEN '41' THEN 'Rhode Island'
        WHEN '42' THEN 'South Carolina'
        WHEN '43' THEN 'South Dakota'
        WHEN '44' THEN 'Tennessee'
        WHEN '45' THEN 'Texas'
        WHEN '46' THEN 'Utah'
        WHEN '47' THEN 'Vermont'
        WHEN '48' THEN 'Virgin Islands'
        WHEN '49' THEN 'Virginia'
        WHEN '50' THEN 'Washington'
        WHEN '51' THEN 'West Virginia'
        WHEN '52' THEN 'Wisconsin'
        WHEN '53' THEN 'Wyoming'
        WHEN '54' THEN 'Africa'
        WHEN '55' THEN 'Asia'
        WHEN '56' THEN 'Canada and Islands'
        WHEN '57' THEN 'Central America and West Indies'
        WHEN '58' THEN 'Europe'
        WHEN '59' THEN 'Mexico'
        WHEN '60' THEN 'Oceania'
        WHEN '61' THEN 'Philippines'
        WHEN '62' THEN 'South America'
        WHEN '63' THEN 'U.S. Possessions'
        WHEN '64' THEN 'American Samoa'
        ELSE 'Unknown'
    END AS state,
    CAST(ffs.ZIP_CD AS VARCHAR) AS zip_code,
    CAST(NULL AS VARCHAR) AS phone,
    'CMS Synthetic Medicare Enrollment, Fee-for-Service Claims, and Prescription Drug Event Data' AS data_source
FROM {{ source('ffs', 'beneficiary') }} ffs

UNION

SELECT DISTINCT
    CAST(desynpuf.DESYNPUF_ID AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS member_id,
    CASE
        WHEN desynpuf.BENE_SEX_IDENT_CD = '1' THEN 'male'
        WHEN desynpuf.BENE_SEX_IDENT_CD = '2' THEN 'female'
        ELSE 'unknown'
    END AS gender,
    CASE
        WHEN desynpuf.BENE_RACE_CD = '1' THEN 'white'
        WHEN desynpuf.BENE_RACE_CD = '2' THEN 'black or african american'
        WHEN desynpuf.BENE_RACE_CD = '3' THEN 'other race'
        WHEN desynpuf.BENE_RACE_CD = '4' THEN 'asian'
        WHEN desynpuf.BENE_RACE_CD = '5' THEN 'hispanic'
        WHEN desynpuf.BENE_RACE_CD = '6' THEN 'american indian or alaska native'
        ELSE 'unknown'
    END AS race,
    CAST(SUBSTR(desynpuf.BENE_BIRTH_DT, 1, 4) || '-' || SUBSTR(desynpuf.BENE_BIRTH_DT, 5, 2) || '-' || SUBSTR(desynpuf.BENE_BIRTH_DT, 7, 2) AS DATE) AS birth_date,
    CAST(SUBSTR(desynpuf.BENE_DEATH_DT, 1, 4) || '-' || SUBSTR(desynpuf.BENE_DEATH_DT, 5, 2) || '-' || SUBSTR(desynpuf.BENE_DEATH_DT, 7, 2) AS DATE) AS death_date,
    CASE 
        WHEN desynpuf.BENE_DEATH_DT IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS death_flag,
    CAST(desynpuf.YEAR || '-01-01' AS DATE) AS enrollment_start_date,
    (CAST(
        desynpuf.YEAR || '-01-01' AS DATE
    ) + CAST(desynpuf.BENE_HI_CVRAGE_TOT_MONS AS INT) * INTERVAL '1' MONTH) - INTERVAL '1' DAY AS enrollment_end_date,
    CAST(NULL AS VARCHAR) AS payer,
    'medicare' AS payer_type,
    CAST(NULL AS VARCHAR) AS plan,
    CAST(NULL AS VARCHAR) AS original_reason_entitlement_code,
    CAST(NULL AS VARCHAR) AS dual_status_code,
    CASE desynpuf.BENE_ESRD_IND
        WHEN 'Y' THEN '31'
        WHEN '0' THEN '10'
        ELSE NULL
    END AS medicare_status_code,
    CAST(NULL AS VARCHAR) AS first_name,
    CAST(NULL AS VARCHAR) AS last_name,
    CAST(NULL AS VARCHAR) AS address,
    CAST(NULL AS VARCHAR) AS city,
    CASE desynpuf.SP_STATE_CODE
        WHEN '01' THEN 'Alabama'
        WHEN '02' THEN 'Alaska'
        WHEN '03' THEN 'Arizona'
        WHEN '04' THEN 'Arkansas'
        WHEN '05' THEN 'California'
        WHEN '06' THEN 'Colorado'
        WHEN '07' THEN 'Connecticut'
        WHEN '08' THEN 'Delaware'
        WHEN '09' THEN 'District of Columbia'
        WHEN '10' THEN 'Florida'
        WHEN '11' THEN 'Georgia'
        WHEN '12' THEN 'Hawaii'
        WHEN '13' THEN 'Idaho'
        WHEN '14' THEN 'Illinois'
        WHEN '15' THEN 'Indiana'
        WHEN '16' THEN 'Iowa'
        WHEN '17' THEN 'Kansas'
        WHEN '18' THEN 'Kentucky'
        WHEN '19' THEN 'Louisiana'
        WHEN '20' THEN 'Maine'
        WHEN '21' THEN 'Maryland'
        WHEN '22' THEN 'Massachusetts'
        WHEN '23' THEN 'Michigan'
        WHEN '24' THEN 'Minnesota'
        WHEN '25' THEN 'Mississippi'
        WHEN '26' THEN 'Missouri'
        WHEN '27' THEN 'Montana'
        WHEN '28' THEN 'Nebraska'
        WHEN '29' THEN 'Nevada'
        WHEN '30' THEN 'New Hampshire'
        WHEN '31' THEN 'New Jersey'
        WHEN '32' THEN 'New Mexico'
        WHEN '33' THEN 'New York'
        WHEN '34' THEN 'North Carolina'
        WHEN '35' THEN 'North Dakota'
        WHEN '36' THEN 'Ohio'
        WHEN '37' THEN 'Oklahoma'
        WHEN '38' THEN 'Oregon'
        WHEN '39' THEN 'Pennsylvania'
        WHEN '40' THEN 'Puerto Rico'
        WHEN '41' THEN 'Rhode Island'
        WHEN '42' THEN 'South Carolina'
        WHEN '43' THEN 'South Dakota'
        WHEN '44' THEN 'Tennessee'
        WHEN '45' THEN 'Texas'
        WHEN '46' THEN 'Utah'
        WHEN '47' THEN 'Vermont'
        WHEN '48' THEN 'Virgin Islands'
        WHEN '49' THEN 'Virginia'
        WHEN '50' THEN 'Washington'
        WHEN '51' THEN 'West Virginia'
        WHEN '52' THEN 'Wisconsin'
        WHEN '53' THEN 'Wyoming'
        WHEN '54' THEN 'Africa'
        WHEN '55' THEN 'Asia'
        WHEN '56' THEN 'Canada and Islands'
        WHEN '57' THEN 'Central America and West Indies'
        WHEN '58' THEN 'Europe'
        WHEN '59' THEN 'Mexico'
        WHEN '60' THEN 'Oceania'
        WHEN '61' THEN 'Philippines'
        WHEN '62' THEN 'South America'
        WHEN '63' THEN 'U.S. Possessions'
        WHEN '64' THEN 'American Samoa'
        ELSE 'Unknown'
    END AS state,
    CAST(NULL AS VARCHAR) AS zip_code,
    CAST(NULL AS VARCHAR) AS phone,
    'CMS 2008-2010 Data Entrepreneurs Synthetic Public Use File (DE-SynPUF)' AS data_source
FROM {{ source('desynpuf', 'beneficiary') }} desynpuf