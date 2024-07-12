{{ config(
    enabled = var('claims_enabled', False)
) }}

WITH BASE as (
SELECT * 
FROM {{ ref('data_quality__stg_institutional_inpatient') }}
)
, tuva_last_run as(

    select cast(substring('{{ var('tuva_last_run') }}',1,10) as date) as tuva_last_run

)
,UNIQUE_FIELD as (
    SELECT DISTINCT CLAIM_ID
        ,cast(Procedure_Date_2 as {{ dbt.type_string() }})  as Field
        ,DATA_SOURCE
    FROM BASE

),
CLAIM_GRAIN as (
    SELECT CLAIM_ID
        ,DATA_SOURCE
        ,count(*) as FREQUENCY
    from UNIQUE_FIELD 
    GROUP BY CLAIM_ID
        ,DATA_SOURCE
),
CLAIM_AGG as (
SELECT
    CLAIM_ID,
    DATA_SOURCE,
    {{ dbt.listagg(measure="coalesce(Field, 'null')", delimiter_text="', '", order_by_clause="order by Field desc") }} AS FIELD_AGGREGATED
FROM
    UNIQUE_FIELD
GROUP BY
    CLAIM_ID,
    DATA_SOURCE
	)
SELECT DISTINCT -- to bring to claim_ID grain 
    M.Data_SOURCE
    ,coalesce(cast(M.CLAIM_START_DATE as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) AS SOURCE_DATE
    ,'MEDICAL_CLAIM' AS TABLE_NAME
    ,'Claim ID' AS DRILL_DOWN_KEY
    ,coalesce(M.CLAIM_ID, 'NULL') AS DRILL_DOWN_VALUE
    ,'institutional' AS CLAIM_TYPE
    ,'PROCEDURE_DATE_2' AS FIELD_NAME
    ,CASE 
        WHEN CG.FREQUENCY > 1 THEN 'multiple'      
        WHEN M.Procedure_Date_2 > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) THEN 'invalid'
        WHEN M.Procedure_Date_2 < {{ dbt.dateadd(datepart="year", interval=-10, from_date_or_timestamp="cte.tuva_last_run") }} THEN 'invalid'
        WHEN NOT (M.Procedure_Date_2 BETWEEN M.CLAIM_START_DATE AND M.CLAIM_END_DATE) THEN 'invalid'
        WHEN M.Procedure_Date_2 IS NULL THEN 'null'
        ELSE 'valid'
    END AS BUCKET_NAME
    ,CASE 
        WHEN CG.FREQUENCY > 1 THEN 'multiple'      
        WHEN M.Procedure_Date_2 > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) THEN 'future'
        WHEN M.Procedure_Date_2 < {{ dbt.dateadd(datepart="year", interval=-10, from_date_or_timestamp="cte.tuva_last_run") }} THEN 'too old'
        WHEN NOT (M.Procedure_Date_2 BETWEEN M.CLAIM_START_DATE AND M.CLAIM_END_DATE) THEN 'procedure date not between claim start and end dates'
        WHEN M.Procedure_Date_2 IS NULL THEN null
        ELSE 'valid'
    END AS INVALID_REASON
    ,CAST({{ substring('AGG.FIELD_AGGREGATED', 1, 255) }} as {{ dbt.type_string() }}) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM BASE M
LEFT JOIN CLAIM_GRAIN CG ON M.CLAIM_ID = CG.CLAIM_ID AND M.Data_Source = CG.Data_Source
LEFT JOIN CLAIM_AGG AGG ON M.CLAIM_ID = AGG.CLAIM_ID AND M.Data_Source = AGG.Data_Source
cross join tuva_last_run cte