{# =============================================================================
   FILE: macros/identify_errors.sql
   PURPOSE: Stage 2 — Identify NULL PK rows in _TEMP, move to _ERROR table.
            PK columns read from STTM_UPDATED (STG_PRIMARYKEY=TRUE).
            _ERROR auto-created if not exists.
            Error rows deleted from _TEMP so Stage 3 only loads clean rows.
   MACROS:
     - identify_errors(stg_entity, trans_entity)   ← single table
     - identify_errors_multi(pipeline_tables)       ← multi table wrapper
   ============================================================================= #}

{% macro _get_columns_from_info_schema(fqtn) %}
    {% set parts = fqtn.split('.') %}
    {% set col_res = run_query(
        "SELECT COLUMN_NAME FROM " ~ parts[0] ~ ".INFORMATION_SCHEMA.COLUMNS" ~
        " WHERE TABLE_SCHEMA='" ~ parts[1] ~ "' AND TABLE_NAME='" ~ parts[2] ~ "'" ~
        " ORDER BY ORDINAL_POSITION"
    ) %}
    {% if col_res is none or (col_res.rows | length) == 0 %}
        {{ exceptions.raise_compiler_error("Cannot get columns for: " ~ fqtn) }}
    {% endif %}
    {{ return(col_res.columns[0].values() | list) }}
{% endmacro %}


{% macro identify_errors(stg_entity, trans_entity) %}
    {% if not execute %}{{ return('') }}{% endif %}

    {% set DB_SCHEMA  = var('audit_db') ~ '.' ~ var('trans_schema') %}
    {% set STTM_TBL   = DB_SCHEMA ~ '.' ~ var('sttm_table') %}
    {% set temp_fqtn  = DB_SCHEMA ~ '.' ~ stg_entity ~ var('temp_suffix') %}
    {% set error_fqtn = DB_SCHEMA ~ '.' ~ stg_entity ~ var('error_suffix') %}

    {% set audit_id = audit_log_insert(
        job_name='IDENTIFY_ERRORS_' ~ stg_entity, job_id=invocation_id,
        job_status='RUNNING', comments='Checking NULL PKs in ' ~ temp_fqtn
    ) %}

    {# STEP 1: Get PK columns from STTM #}
    {% set pk_res  = run_query(
        "SELECT STG_ATRRIBUTE FROM " ~ STTM_TBL ~
        " WHERE UPPER(STG_ENTITY)=UPPER('" ~ stg_entity ~ "')" ~
        " AND UPPER(TRANS_ENTITY)=UPPER('" ~ trans_entity ~ "')" ~
        " AND STG_PRIMARYKEY=TRUE ORDER BY STG_ATRRIBUTE"
    ) %}
    {% set pk_cols = pk_res.columns[0].values() | map('upper') | list
        if (pk_res is not none and (pk_res.rows | length) > 0) else [] %}

    {% if (pk_cols | length) == 0 %}
        {% do audit_log_update(audit_id=audit_id, status='SUCCESS',
            comments='No PK cols in STTM for ' ~ stg_entity ~ ' — skipped.') %}
        {{ log('[' ~ stg_entity ~ '] WARNING: No PK columns — error step skipped.', info=True) }}
        {{ return('') }}
    {% endif %}

    {# STEP 2: Auto-create _ERROR from _TEMP structure + error columns #}
    {% do run_query(
        "CREATE TABLE IF NOT EXISTS " ~ error_fqtn ~
        " AS SELECT *, CAST(NULL AS VARCHAR) AS " ~ var('col_error_reason') ~
        ", CAST(NULL AS TIMESTAMP) AS " ~ var('col_error_captured_at') ~
        " FROM " ~ temp_fqtn ~ " WHERE 1=0"
    ) %}

    {# STEP 3: Build NULL check condition #}
    {% set null_checks   = pk_cols | map('string') | map('upper') | list %}
    {% set null_condition = (null_checks | map('string') | list | join(' IS NULL OR ')) ~ ' IS NULL' %}

    {% set reason_parts = [] %}
    {% for pk in pk_cols %}
        {% do reason_parts.append("IFF(" ~ pk ~ " IS NULL,'" ~ pk ~ " IS NULL',NULL)") %}
    {% endfor %}
    {% set error_reason_expr = "TRIM(CONCAT_WS(', '," ~ reason_parts | join(',') ~ "))" %}

    {# STEP 4: Count and move error rows #}
    {% set error_cnt = run_query(
        "SELECT COUNT(*) FROM " ~ temp_fqtn ~ " WHERE " ~ null_condition
    ).columns[0].values()[0] %}

    {{ log('[' ~ stg_entity ~ '] Error rows (null PK): ' ~ error_cnt, info=True) }}

    {% if error_cnt > 0 %}
        {% set temp_cols     = _get_columns_from_info_schema(temp_fqtn) %}
        {% set insert_cols   = temp_cols | join(', ') ~ ',' ~ var('col_error_reason') ~ ',' ~ var('col_error_captured_at') %}
        {% set select_cols   = temp_cols | join(', ') ~ ',' ~ error_reason_expr ~ ' AS ' ~ var('col_error_reason') ~ ',CURRENT_TIMESTAMP() AS ' ~ var('col_error_captured_at') %}

        {% do run_query("INSERT INTO " ~ error_fqtn ~ " (" ~ insert_cols ~ ") SELECT " ~ select_cols ~ " FROM " ~ temp_fqtn ~ " WHERE " ~ null_condition) %}
        {% do run_query("DELETE FROM " ~ temp_fqtn ~ " WHERE " ~ null_condition) %}
    {% endif %}

    {% set clean_cnt = run_query("SELECT COUNT(*) FROM " ~ temp_fqtn).columns[0].values()[0] %}

    {% do audit_log_update(audit_id=audit_id, status='SUCCESS',
        comments='error_cnt=' ~ error_cnt ~ '|clean_cnt=' ~ clean_cnt ~ '|pks=' ~ pk_cols | join(',')) %}

    {{ log('[' ~ stg_entity ~ '] {"status":"SUCCESS","error_cnt":' ~ error_cnt ~ ',"clean_cnt":' ~ clean_cnt ~ ',"audit_id":' ~ audit_id ~ '}', info=True) }}
{% endmacro %}


{% macro identify_errors_multi(pipeline_tables) %}
    {% if not execute %}{{ return('') }}{% endif %}

    {% set audit_id = audit_log_insert(
        job_name='BATCH_IDENTIFY_ERRORS', job_id=invocation_id,
        job_status='RUNNING', comments='Tables=' ~ (pipeline_tables | map(attribute='stg_entity') | join(','))
    ) %}

    {% for t in pipeline_tables %}
        {{ identify_errors(stg_entity=t.stg_entity, trans_entity=t.trans_entity) }}
    {% endfor %}

    {% do audit_log_update(audit_id=audit_id, status='SUCCESS',
        comments='Done. Tables=' ~ (pipeline_tables | map(attribute='stg_entity') | join(','))) %}
{% endmacro %}
