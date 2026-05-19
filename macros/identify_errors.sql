{# =============================================================================
   FILE: macros/identify_errors.sql
   PURPOSE: Stage 2 — Identify NULL PK rows in _TEMP, move to _ERROR table.
            PK columns read from STTM_UPDATED (STG_PRIMARYKEY=TRUE).
            _ERROR table has SAME structure as TARGET — no extra columns.
            Error rows deleted from _TEMP so Stage 3 loads only clean rows.
   MACROS:
     - identify_errors(stg_entity, trans_entity)   <- single table
     - identify_errors_multi(pipeline_tables)       <- multi table wrapper
   ============================================================================= #}

{% macro identify_errors(stg_entity, trans_entity) %}
    {% if not execute %}{{ return('') }}{% endif %}

    {% set DB_SCHEMA  = var('target_database') ~ '.' ~ var('trans_schema') %}
    {% set STTM_TBL   = DB_SCHEMA ~ '.' ~ var('sttm_table') %}
    {% set temp_fqtn  = DB_SCHEMA ~ '.' ~ stg_entity ~ var('temp_suffix') %}
    {% set error_fqtn = DB_SCHEMA ~ '.' ~ stg_entity ~ var('error_suffix') %}

    {% set audit_id = audit_log_insert(
        trans_entity = trans_entity,
        job_name     = 'IDENTIFY_ERRORS_' ~ stg_entity,
        job_status   = 'RUNNING',
        comments     = 'Checking NULL PKs in ' ~ temp_fqtn
    ) %}

    {# STEP 1: Get PK columns from STTM (STG_PRIMARYKEY=TRUE) #}
    {% set pk_res = run_query(
        "SELECT STG_ATRRIBUTE FROM " ~ STTM_TBL ~
        " WHERE UPPER(STG_ENTITY) = UPPER('" ~ stg_entity ~ "')" ~
        " AND STG_PRIMARYKEY = TRUE" ~
        " AND ACTIVE_FLAG = TRUE" ~
        " ORDER BY STG_ATRRIBUTE"
    ) %}

    {% set pk_cols = pk_res.columns[0].values() | map('upper') | list
        if (pk_res is not none and (pk_res.rows | length) > 0) else [] %}

    {% if (pk_cols | length) == 0 %}
        {% do audit_log_update(
            trans_entity = trans_entity,
            audit_id     = audit_id,
            status       = 'SUCCESS',
            comments     = 'No PK columns in STTM for ' ~ stg_entity ~ ' — error step skipped.'
        ) %}
        {{ log('[' ~ stg_entity ~ '] WARNING: No PK columns — error identification skipped.', info=True) }}
        {{ return('') }}
    {% endif %}

    {# STEP 2: Build NULL check condition #}
    {% set null_checks = [] %}
    {% for pk in pk_cols %}
        {% do null_checks.append(pk ~ ' IS NULL') %}
    {% endfor %}
    {% set null_condition = null_checks | join(' OR ') %}

    {# STEP 3: Count error rows #}
    {% set error_cnt = run_query(
        "SELECT COUNT(*) FROM " ~ temp_fqtn ~ " WHERE " ~ null_condition
    ).columns[0].values()[0] %}

    {{ log('[' ~ stg_entity ~ '] Error rows (null PK): ' ~ error_cnt, info=True) }}

    {% if error_cnt > 0 %}

        {# Get _TEMP columns via INFORMATION_SCHEMA #}
        {% set temp_parts = temp_fqtn.split('.') %}
        {% set temp_cols  = run_query(
            "SELECT COLUMN_NAME FROM " ~ temp_parts[0] ~ ".INFORMATION_SCHEMA.COLUMNS" ~
            " WHERE TABLE_SCHEMA='" ~ temp_parts[1] ~ "'" ~
            " AND TABLE_NAME='" ~ temp_parts[2] ~ "'" ~
            " ORDER BY ORDINAL_POSITION"
        ).columns[0].values() | list %}

        {# INSERT error rows into _ERROR — same columns, no extra error cols #}
        {% do run_query(
            "INSERT INTO " ~ error_fqtn ~
            " (" ~ temp_cols | join(', ') ~ ")" ~
            " SELECT " ~ temp_cols | join(', ') ~
            " FROM " ~ temp_fqtn ~
            " WHERE " ~ null_condition
        ) %}

        {# DELETE error rows from _TEMP — Stage 3 gets only clean rows #}
        {% do run_query(
            "DELETE FROM " ~ temp_fqtn ~ " WHERE " ~ null_condition
        ) %}

    {% endif %}

    {% set clean_cnt = run_query(
        "SELECT COUNT(*) FROM " ~ temp_fqtn
    ).columns[0].values()[0] %}

    {% do audit_log_update(
        trans_entity = trans_entity,
        audit_id     = audit_id,
        status       = 'SUCCESS',
        comments     = 'error_cnt=' ~ error_cnt ~ ' | clean_cnt=' ~ clean_cnt ~
                       ' | pks=' ~ pk_cols | join(',')
    ) %}

    {{ log('[' ~ stg_entity ~ '] {"status":"SUCCESS","error_cnt":' ~ error_cnt ~
          ',"clean_cnt":' ~ clean_cnt ~ ',"audit_id":' ~ audit_id ~ '}', info=True) }}
{% endmacro %}


{% macro identify_errors_multi(pipeline_tables) %}
    {% if not execute %}{{ return('') }}{% endif %}

    {% set tbl_names = pipeline_tables | map(attribute='stg_entity') | join(',') %}

    {% set audit_id = audit_log_insert(
        trans_entity = pipeline_tables[0].trans_entity,
        job_name     = 'BATCH_IDENTIFY_ERRORS',
        job_status   = 'RUNNING',
        comments     = 'Tables=' ~ tbl_names
    ) %}

    {% for t in pipeline_tables %}
        {{ identify_errors(
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
