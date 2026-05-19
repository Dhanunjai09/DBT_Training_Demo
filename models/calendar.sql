{{ config(
    materialized="incremental",
    unique_key=["CALENDARDATECALENDAR"],
    incremental_strategy="merge",
    on_schema_change="sync_all_columns",
    tags=["transform","sap","CALENDARDATE","daily"]
) }}
WITH
  s_tfacs AS (
    SELECT * FROM {{ source('transformation', 'TFACS') }}
  )
, transformed AS (
    SELECT
        s_tfacs.ACTIVE_FLAG AS ACTIVE_FLAG,
        s_tfacs.IDENT AS CALENDARDATECALENDAR,
        s_tfacs.MON12 AS CALENDARDATEVALUE,
        s_tfacs.MON01 AS CALENDARDATEVALUE1,
        s_tfacs.CREATED_BY AS CREATED_BY,
        s_tfacs.CREATED_DATE AS CREATED_DATE,
        s_tfacs.INSERTEDAT AS INSERTEDAT,
        s_tfacs.MODIFIED_BY AS MODIFIED_BY,
        s_tfacs.MODIFIED_DATE AS MODIFIED_DATE
    FROM s_tfacs
)
, final AS (
  SELECT *
  FROM transformed
)
SELECT *
FROM final
{% if is_incremental() %}
WHERE INSERTEDAT >= COALESCE(
    (
        SELECT MAX(INSERTEDAT)
        FROM {{ this }}
    ),
    TO_TIMESTAMP('1900-01-01')
)
{% endif %}