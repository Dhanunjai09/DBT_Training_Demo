{# =============================================================================
   FILE: macros/load_temp_to_target.sql
   PURPOSE: Stage 3 — Load clean rows from _TEMP into TARGET tables.
            Refresh type (DELTA/FULL) read from STTM STG_REFRESH_TYPE.
            REFRESCO_CONTROL_TABLE no longer needed — STTM drives refresh type.
            TARGET auto-created if not exists.
   MACROS:
     - get_refresh_type(stg_entity, trans_entity)          ← reads from STTM
     - apply_delta_merge(stg_entity, trans_entity, user)   ← MERGE on PK cols
     - apply_full_refresh(tgt_fqtn)                        ← SET ACTIVE_FLAG='N'
     - insert_latest_rows(src_fqtn, tgt_fqtn)              ← INSERT from _TEMP
     - truncate_temp(temp_fqtn)                            ← cleanup after load
     - load_temp_to_target(stg_entity, trans_entity)       ← single table
     - load_temp_to_target_multi(pipeline_tables)          ← multi table wrapper
   ============================================================================= #}

{% macro _refresco_user_value(user_name=none) %}
    {% set u = user_name if (user_name is not none and user_name != '') else var('pipeline_user') %}
    {{ return((u | string) | replace("'", "''")) }}
{% endmacro %}


{% macro get_refresh_type(stg_entity, trans_entity) %}
    {% set refresh_type = 'FULL' %}
    {% if execute %}
        {% set STTM_TBL = var('audit_db') ~ '.' ~ var('trans_schema') ~ '.' ~ var('sttm_table') %}
        {% set res = run_query(
            "SELECT DISTINCT STG_REFRESH_TYPE FROM " ~ STTM_TBL ~
            " WHERE UPPER(STG_ENTITY)=UPPER('" ~ stg_entity ~ "')" ~
            " AND UPPER(TRANS_ENTITY)=UPPER('" ~ trans_entity ~ "')" ~
            " AND STG_PRIMARYKEY=TRUE AND ACTIVE_FLAG=TRUE"
        ) %}
        {% if res is not none and (res.rows | length) > 0 %}
            {% set refresh_type = (res.columns[0].values() | list)[0] %}
        {% endif %}
    {% endif %}
    {{ return(refresh_type) }}
{% endmacro %}


{% macro apply_delta_merge(stg_entity, trans_entity, user) %}
    {% if execute %}
        {% set u = _refresco_user_value(user) %}
        {% set DB_SCHEMA = var('audit_db') ~ '.' ~ var('trans_schema') %}
        {% set STTM_TBL  = DB_SCHEMA ~ '.' ~ var('sttm_table') %}

        {% set gen_sql %}
            SELECT
                'MERGE INTO {{ DB_SCHEMA }}.' || STG_ENTITY || ' AS TGT ' ||
                'USING {{ DB_SCHEMA }}.' || STG_ENTITY || '{{ var("temp_suffix") }} AS SRC ON '
                || LISTAGG('SRC.'||STG_ATRRIBUTE||'=TGT.'||STG_ATRRIBUTE,' AND ')
                || ' AND TGT.{{ var("col_active_flag") }}=''Y'' '
                || ' WHEN MATCHED THEN UPDATE SET {{ var("col_active_flag") }}=''N'','
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
        {% do run_query("UPDATE " ~ tgt_fqtn ~ " SET " ~ var('col_active_flag') ~ "='N'") %}
    {% endif %}
{% endmacro %}


{% macro insert_latest_rows(src_fqtn, tgt_fqtn) %}
    {% if execute %}
        {% set col_names   = _get_columns_from_info_schema(src_fqtn) %}
        {% set insert_cols = col_names | join(', ') %}

        {% set select_cols = [] %}
        {% for c in col_names %}
            {% if c | upper == var('col_active_flag') %}
                {% do select_cols.append("NVL(" ~ c ~ ",'Y') AS " ~ c) %}
            {% else %}
                {% do select_cols.append(c) %}
            {% endif %}
        {% endfor %}

        {% do run_query(
            "INSERT INTO " ~ tgt_fqtn ~ " (" ~ insert_cols ~ ")" ~
            " SELECT " ~ select_cols | join(', ') ~ " FROM " ~ src_fqtn
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

    {% set DB_SCHEMA    = var('audit_db') ~ '.' ~ var('trans_schema') %}
    {% set src_fqtn     = DB_SCHEMA ~ '.' ~ stg_entity ~ var('temp_suffix') %}
    {% set tgt_fqtn     = DB_SCHEMA ~ '.' ~ stg_entity %}
    {% set refresh_type = get_refresh_type(stg_entity, trans_entity) %}
    {% set eff_user     = _refresco_user_value() %}

    {% set audit_id = audit_log_insert(
        job_name='LOAD_TEMP_TO_TARGET_' ~ stg_entity, job_id=invocation_id,
        job_status='RUNNING',
        comments='RefreshType=' ~ refresh_type ~ ' | ' ~ src_fqtn ~ ' → ' ~ tgt_fqtn
    ) %}

    {# Ensure target exists before MERGE or UPDATE #}
    {% do run_query(
        "CREATE TABLE IF NOT EXISTS " ~ tgt_fqtn ~
        " AS SELECT * FROM " ~ src_fqtn ~ " WHERE 1=0"
    ) %}

    {% if refresh_type == 'DELTA' %}
        {{ apply_delta_merge(stg_entity, trans_entity, eff_user) }}
    {% else %}
        {{ apply_full_refresh(tgt_fqtn) }}
    {% endif %}

    {{ insert_latest_rows(src_fqtn, tgt_fqtn) }}
    {{ truncate_temp(src_fqtn) }}

    {% do audit_log_update(audit_id=audit_id, status='SUCCESS',
        comments='Done. RefreshType=' ~ refresh_type ~ ' | ' ~ src_fqtn ~ ' → ' ~ tgt_fqtn) %}

    {{ log('[' ~ stg_entity ~ '] {"status":"SUCCESS","refresh_type":"' ~ refresh_type ~ '","audit_id":' ~ audit_id ~ '}', info=True) }}
{% endmacro %}


{% macro load_temp_to_target_multi(pipeline_tables) %}
    {% if not execute %}{{ return('') }}{% endif %}

    {% set audit_id = audit_log_insert(
        job_name='BATCH_LOAD_TEMP_TO_TARGET', job_id=invocation_id,
        job_status='RUNNING', comments='Tables=' ~ (pipeline_tables | map(attribute='stg_entity') | join(','))
    ) %}

    {% for t in pipeline_tables %}
        {{ load_temp_to_target(stg_entity=t.stg_entity, trans_entity=t.trans_entity) }}
    {% endfor %}

    {% do audit_log_update(audit_id=audit_id, status='SUCCESS',
        comments='Done. Tables=' ~ (pipeline_tables | map(attribute='stg_entity') | join(','))) %}
{% endmacro %}
