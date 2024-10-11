{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT -- to bring to claim_ID grain
      m.data_source
    , coalesce(cast(m.paid_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    , 'PHARMACY_CLAIM' AS table_name
    , 'Claim ID | Claim Line Number' AS drill_down_key
    , {{ dbt.concat(["coalesce(cast(m.claim_id as " ~ dbt.type_string() ~ "), 'null')",
                    "'|'",
                    "coalesce(cast(m.claim_line_number as " ~ dbt.type_string() ~ "), 'null')"]) }} as drill_down_value
    , 'PHARMACY' AS claim_type
    , 'PRESCRIBING_PROVIDER_NPI' AS field_name
    , case when term.npi is not null          then        'valid'
          when m.prescribing_provider_npi is not null    then 'invalid'
                                             else 'null' end as bucket_name
    , case
        when m.prescribing_provider_npi is not null
            and term.npi is null
            then 'Prescribing Provider NPI does not join to Terminology Provider table'
        else null
    end as invalid_reason
    , cast(m.prescribing_provider_npi as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('pharmacy_claim')}} m
left join {{ ref('terminology__provider')}} as term on m.prescribing_provider_npi = term.npi