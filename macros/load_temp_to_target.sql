{# =============================================================================
   FILE: macros/load_temp_to_target.sql
   PURPOSE: Stage 3 — Load clean rows from _TEMP into TARGET tables.

   PRE-CONDITION (CHANGED FROM v2):
     TARGET tables MUST already exist in Snowflake before running.
     This macro NO LONGER auto-creates TARGET tables with CREATE TABLE IF NOT EXISTS.
     Reason: _TEMP may have fewer columns than TARGET; source DDL is authoritative.

   REFRESH LOGIC:
     DELTA  → MERGE to mark old rows ACTIVE_FLAG='N', then INSERT new rows
     FULL   → SET ACTIVE_FLAG='N' on all, then INSERT all rows from _TEMP
     Refresh type controlled per table in REFRESCO_CONTROL_TABLE (IS_ACTIVE='Y').

   MACROS:
     - get_refresh_type(stg_entity)                            ← reads REFRESCO_CONTROL_TABLE
     - apply_delta_merge(stg_entity, trans_entity, user)       ← MERGE using STTM PK cols
     - apply_full_refresh(tgt_fqtn)                            ← SET ACTIVE_FLAG='N'
     - insert_latest_rows(src_fqtn, tgt_fqtn)                  ← INSERT from _TEMP
     - truncate_temp(temp_fqtn)                                ← cleanup after load
     - load_temp_to_target(stg_entity, trans_entity)           ← single table
     - load_temp_to_target_multi(pipeline_tables)              ← multi table wrapper
   ============================================================================= #}

{% macro _get_pipeline_user_value(user_name=none) %}
    {% set u = user_name if (user_name is not none and user_name != '')
               else var('pipeline_user') %}
    {{ return((u | string) | replace("'", "''")) }}
{% endmacro %}


{% macro get_refresh_type(stg_entity) %}
    {# Reads from REFRESCO_CONTROL_TABLE. TABLE_NAME = stg_entity ~ '_TEMP'. #}
    {% set refresh_type = '' %}
    {% if execute %}
        {% set CTRL_TBL = var('target_database') ~ '.' ~
                          var('trans_schema') ~ '.' ~
                          var('control_table') %}
        {% set source_table = stg_entity ~ var('temp_suffix') %}
        {% set res = run_query(
            "SELECT REFRESH_TYPE FROM " ~ CTRL_TBL ~
            " WHERE IS_ACTIVE = 'Y'" ~
            " AND UPPER(TABLE_NAME) = UPPER('" ~ source_table ~ "')"
        ) %}
        {% if res is not none and (res.rows | length) > 0 %}
            {% set refresh_type = (res.columns[0].values() | list)[0] %}
        {% endif %}
    {% endif %}
    {{ return(refresh_type) }}
{% endmacro %}


{% macro apply_delta_merge(stg_entity, trans_entity, user) %}
    {% if execute %}
        {% set u         = _get_pipeline_user_value(user) %}
        {% set DB_SCHEMA = var('target_database') ~ '.' ~ var('trans_schema') %}
        {% set STTM_TBL  = DB_SCHEMA ~ '.' ~ var('sttm_table') %}

        {% set gen_sql %}
            SELECT
                'MERGE INTO {{ DB_SCHEMA }}.' || STG_ENTITY || ' AS TGT ' ||
                'USING {{ DB_SCHEMA }}.' || STG_ENTITY || '{{ var("temp_suffix") }} AS SRC ON '
                || LISTAGG('SRC.'||STG_ATRRIBUTE||'=TGT.'||STG_ATRRIBUTE,' AND ')
                || ' AND TGT.{{ var("col_active_flag") }}=''Y'' '
                || ' WHEN MATCHED THEN UPDATE SET '
                || '{{ var("col_active_flag") }}=''N'','
                || '{{ var("col_modified_date") }}=CURRENT_DATE(),'
                || '{{ var("col_modified_by") }}=''{{ u }}'''
                AS QRY
            FROM {{ STTM_TBL }}
            WHERE UPPER(STG_ENTITY)   = UPPER('{{ stg_entity }}')
              AND UPPER(TRANS_ENTITY) = UPPER('{{ trans_entity }}')
              AND STG_PRIMARYKEY      = TRUE
              AND ACTIVE_FLAG         = TRUE
            GROUP BY STG_ENTITY
        {% endset %}

        {% set q = run_query(gen_sql) %}
        {% if q is not none and (q.rows | length) > 0 %}
            {% do run_query((q.columns[0].values() | list)[0]) %}
        {% endif %}
    {% endif %}
{% endmacro %}


{% macro apply_full_refresh(tgt_fqtn) %}
    {% if execute %}
        {% do run_query(
            "UPDATE " ~ tgt_fqtn ~
            " SET " ~ var('col_active_flag') ~ "='N'"
        ) %}
    {% endif %}
{% endmacro %}


{% macro insert_latest_rows(src_fqtn, tgt_fqtn) %}
    {% if execute %}
        {# Get columns from _TEMP via INFORMATION_SCHEMA — no adapter cache.
           All column names double-quoted to handle special chars. #}
        {% set parts   = src_fqtn.split('.') %}
        {% set col_names = run_query(
            "SELECT COLUMN_NAME FROM " ~ parts[0] ~ ".INFORMATION_SCHEMA.COLUMNS" ~
            " WHERE TABLE_SCHEMA='" ~ parts[1] ~ "'" ~
            " AND TABLE_NAME='" ~ parts[2] ~ "'" ~
            " ORDER BY ORDINAL_POSITION"
        ).columns[0].values() | list %}

        {% set quoted_insert = [] %}
        {% set select_cols   = [] %}
        {% for c in col_names %}
            {% set cq = '"' ~ c ~ '"' %}
            {% do quoted_insert.append(cq) %}
            {% if c | upper == var('col_active_flag') | upper %}
                {% do select_cols.append("NVL(" ~ cq ~ ",'Y') AS " ~ cq) %}
            {% else %}
                {% do select_cols.append(cq) %}
            {% endif %}
        {% endfor %}

        {% do run_query(
            "INSERT INTO " ~ tgt_fqtn ~
            " (" ~ quoted_insert | join(', ') ~ ")" ~
            " SELECT " ~ select_cols | join(', ') ~
            " FROM " ~ src_fqtn
        ) %}
    {% endif %}
{% endmacro %}


{% macro truncate_temp(temp_fqtn) %}
    {% if execute %}
        {% do run_query("TRUNCATE TABLE " ~ temp_fqtn) %}
    {% endif %}
{% endmacro %}


{% macro load_temp_to_target(stg_entity, trans_entity) %}
    {% if not execute %}{{ return('') }}{% endif %}

    {% set DB_SCHEMA    = var('target_database') ~ '.' ~ var('trans_schema') %}
    {% set TGT_DB       = var('target_database') %}
    {% set TGT_SCHEMA   = var('trans_schema') %}
    {% set src_fqtn     = DB_SCHEMA ~ '.' ~ stg_entity ~ var('temp_suffix') %}
    {% set tgt_fqtn     = DB_SCHEMA ~ '.' ~ stg_entity %}
    {% set refresh_type = get_refresh_type(stg_entity) %}
    {% set eff_user     = _get_pipeline_user_value() %}

    {# Validate TARGET exists — fail fast with clear error if not pre-created #}
    {% set tgt_exists = run_query(
        "SELECT COUNT(*) FROM " ~ TGT_DB ~ ".INFORMATION_SCHEMA.TABLES" ~
        " WHERE TABLE_SCHEMA='" ~ TGT_SCHEMA ~ "'" ~
        " AND TABLE_NAME='" ~ stg_entity ~ "'"
    ).columns[0].values()[0] %}

    {% if tgt_exists == 0 %}
        {{ exceptions.raise_compiler_error(
            "[" ~ stg_entity ~ "] TARGET table does not exist: " ~ tgt_fqtn ~
            ". Pre-create it in Snowflake before running the pipeline."
        ) }}
    {% endif %}

    {% set audit_id = audit_log_insert(
        trans_entity = trans_entity,
        job_name     = 'LOAD_TEMP_TO_TARGET_' ~ stg_entity,
        job_status   = 'RUNNING',
        comments     = 'RefreshType=' ~ refresh_type ~
                       ' | ' ~ src_fqtn ~ ' → ' ~ tgt_fqtn
    ) %}

    {# Apply refresh strategy — controlled by client via REFRESCO_CONTROL_TABLE #}
    {% if refresh_type == 'DELTA' %}
        {{ apply_delta_merge(stg_entity, trans_entity, eff_user) }}
    {% else %}
        {{ apply_full_refresh(tgt_fqtn) }}
    {% endif %}

    {{ insert_latest_rows(src_fqtn, tgt_fqtn) }}
    {{ truncate_temp(src_fqtn) }}

    {% do audit_log_update(
        trans_entity = trans_entity,
        audit_id     = audit_id,
        status       = 'SUCCESS',
        comments     = 'Done. RefreshType=' ~ refresh_type ~
                       ' | ' ~ src_fqtn ~ ' → ' ~ tgt_fqtn
    ) %}

    {{ log('[' ~ stg_entity ~ '] {"status":"SUCCESS","refresh_type":"' ~
          refresh_type ~ '","audit_id":' ~ audit_id ~ '}', info=True) }}
{% endmacro %}


{% macro load_temp_to_target_multi(pipeline_tables) %}
    {% if not execute %}{{ return('') }}{% endif %}

    {% set tbl_names = pipeline_tables | map(attribute='stg_entity') | join(',') %}

    {% set audit_id = audit_log_insert(
        trans_entity = pipeline_tables[0].trans_entity,
        job_name     = 'BATCH_LOAD_TEMP_TO_TARGET',
        job_status   = 'RUNNING',
        comments     = 'Tables=' ~ tbl_names
    ) %}

    {% for t in pipeline_tables %}
        {{ load_temp_to_target(
            stg_entity   = t.stg_entity,
            trans_entity = t.trans_entity
        ) }}
    {% endfor %}

    {% do audit_log_update(
        trans_entity = pipeline_tables[0].trans_entity,
        audit_id     = audit_id,
        status       = 'SUCCESS',
        comments     = 'Done. Tables=' ~ tbl_names
    ) %}
{% endmacro %}
