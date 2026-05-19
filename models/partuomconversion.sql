{{ config(
    materialized='incremental',
    unique_key=['UNITOFMEASUREVALUE'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['transform','sap','PARTUOMCONVERSION','daily']
) }}
 
WITH s_marm AS (
    SELECT *
    FROM {{ source('transformation', 'MARM') }}
),
 
transformed AS (
   SELECT
 
        s_marm.ACTIVE_FLAG AS ACTIVE_FLAG,
        s_marm.CREATED_BY AS CREATED_BY,
        s_marm.CREATED_DATE AS CREATED_DATE,
        s_marm.UMREZ AS FACTOR,
        s_marm.UMREN AS FACTOR1,
        s_marm.INSERTEDAT AS INSERTEDAT,
        s_marm.MODIFIED_BY AS MODIFIED_BY,
        s_marm.MODIFIED_DATE AS MODIFIED_DATE,
        s_marm.MATNR AS PARTUOMCONVERSIONMATNR,
        s_marm.WERKS AS PARTUOMCONVERSIONWERKS,
        s_marm.MEINH AS UNITOFMEASUREVALUE
 
    FROM s_marm
 
)
 
, final AS (
 
    SELECT *
    FROM transformed
 
)
 
SELECT *
FROM final
 
{% if is_incremental() %}
WHERE INSERTEDAT >= COALESCE(
    (SELECT MAX(INSERTEDAT) FROM {{ this }}),
    TO_TIMESTAMP('1900-01-01')
)
{% endif %}