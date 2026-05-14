{# ============================================================
  REFRESCO MACROS + AUDIT LOGS + ERROR IDENTIFICATION
  All values resolved via var() — no hardcoding anywhere.
  var() values set in dbt_project.yml via env_var('DBT_*')
  configured in dbt Cloud Environment Variables UI.
  FIX: all adapter.get_relation / adapter.get_columns_in_relation
       replaced with INFORMATION_SCHEMA queries to avoid stale
       adapter cache within a single dbt run.
============================================================ #}

{# -------------------------
   USER HELPER
-------------------------- #}
{% macro _refresco_user_value(user_name_from_args=none) %}
    {% set u = user_name_from_args %}
    {% if u is none or u == '' %}
        {% set u = var('pipeline_user') %}
    {% endif %}
    {{ return((u | string) | replace("'", "''")) }}
{% endmacro %}

{# -------------------------
   AUDIT HELPERS
-------------------------- #}
{% macro _refresco_audit_now_str() %}
    {{ return("to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS')") }}
{% endmacro %}

{% macro _refresco_audit_nextval() %}
    {% if not execute %}
        {{ return(none) }}
    {% endif %}

    {% set seq_sql %}
        SELECT {{ var('audit_db') }}.{{ var('trans_schema') }}.{{ var('audit_seq') }}.nextval AS AUDIT_ID
    {% endset %}

    {% set res = run_query(seq_sql) %}
    {{ return(res.columns[0].values()[0]) }}
{% endmacro %}

{# -------------------------
   HELPER: get columns from INFORMATION_SCHEMA
   Use this everywhere instead of adapter.get_columns_in_relation
   to avoid stale adapter cache within a single dbt run
-------------------------- #}
{% macro _get_columns_from_info_schema(fqtn) %}
    {% set parts  = fqtn.split('.') %}
    {% set db     = parts[0] %}
    {% set schema = parts[1] %}
    {% set tbl    = parts[2] %}

    {% set col_res = run_query(
        "SELECT COLUMN_NAME FROM " ~ db ~ ".INFORMATION_SCHEMA.COLUMNS" ~
        " WHERE TABLE_SCHEMA = '" ~ schema ~ "'" ~
        " AND TABLE_NAME = '" ~ tbl ~ "'" ~
        " ORDER BY ORDINAL_POSITION"
    ) %}

    {% if col_res is none or (col_res.rows | length) == 0 %}
        {{ exceptions.raise_compiler_error(
            "Could not retrieve columns for: " ~ fqtn ~
            ". Table may not exist or permissions missing."
        ) }}
    {% endif %}

    {{ return(col_res.columns[0].values() | list) }}
{% endmacro %}

{# -------------------------
   AUDIT MACROS
-------------------------- #}

{% macro refresco_audit_log_insert(
        job_name,
        job_id,
        job_status,
        comments,
        task_start_time=none,
        job_start_time=none
    ) %}

    {% if not execute %}
        {{ return(none) }}
    {% endif %}

    {% set AUDIT_TABLE_FQ =
        var('audit_db') ~ '.' ~
        var('trans_schema') ~ '.' ~
        var('audit_table')
    %}

    {% set audit_id = _refresco_audit_nextval() %}

    {% set c    = (comments   if comments   is not none else '') | replace("'", "''") %}
    {% set j_id = (job_id     if job_id     is not none else '') | replace("'", "''") %}
    {% set j_nm = (job_name   if job_name   is not none else '') | replace("'", "''") %}
    {% set st   = (job_status if job_status is not none else '') | replace("'", "''") %}

    {% if task_start_time is none %}
        {% set task_start_expr = _refresco_audit_now_str() %}
    {% else %}
        {% set task_start_expr = "'" ~ (task_start_time | replace("'", "''")) ~ "'" %}
    {% endif %}

    {% if job_start_time is none %}
        {% set job_start_expr = _refresco_audit_now_str() %}
    {% else %}
        {% set job_start_expr = "'" ~ (job_start_time | replace("'", "''")) ~ "'" %}
    {% endif %}

    {% set ins_sql %}
        INSERT INTO {{ AUDIT_TABLE_FQ }}
        (AUDIT_ID, JOB_NAME, TASK_START_TIME, TASK_END_TIME, JOB_START_TIME, JOB_END_TIME, JOB_ID, JOB_STATUS, COMMENTS)
        VALUES
        ({{ audit_id }},
         '{{ j_nm }}',
         {{ task_start_expr }},
         NULL,
         {{ job_start_expr }},
         NULL,
         '{{ j_id }}',
         '{{ st }}',
         '{{ c }}')
    {% endset %}

    {% do run_query(ins_sql) %}
    {{ return(audit_id) }}
{% endmacro %}

{% macro refresco_audit_log_update(
        audit_id,
        status,
        comments,
        task_end_time=none,
        job_end_time=none
    ) %}

    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {% set AUDIT_TABLE_FQ =
        var('audit_db') ~ '.' ~
        var('trans_schema') ~ '.' ~
        var('audit_table')
    %}

    {% set c  = (comments if comments is not none else '') | replace("'", "''") %}
    {% set st = (status   if status   is not none else '') | replace("'", "''") %}

    {% if task_end_time is none %}
        {% set task_end_expr = _refresco_audit_now_str() %}
    {% else %}
        {% set task_end_expr = "'" ~ (task_end_time | replace("'", "''")) ~ "'" %}
    {% endif %}

    {% if job_end_time is none %}
        {% set job_end_expr = _refresco_audit_now_str() %}
    {% else %}
        {% set job_end_expr = "'" ~ (job_end_time | replace("'", "''")) ~ "'" %}
    {% endif %}

    {% set upd_sql %}
        UPDATE {{ AUDIT_TABLE_FQ }}
        SET JOB_STATUS    = '{{ st }}',
            TASK_END_TIME = {{ task_end_expr }},
            JOB_END_TIME  = {{ job_end_expr }},
            COMMENTS      = '{{ c }}'
        WHERE AUDIT_ID   = {{ audit_id }}
          AND JOB_STATUS = 'RUNNING'
    {% endset %}

    {% do run_query(upd_sql) %}
    {{ return('') }}
{% endmacro %}

{# ============================================================
  REFRESCO MACROS
============================================================ #}

{% macro refresco_refresh_types(source_table_name) %}
    {% set refresh_type = '' %}
    {% if execute %}
        {% set CONTROL_TBL =
            var('audit_db') ~ '.' ~
            var('trans_schema') ~ '.' ~
            var('control_table')
        %}
        {% set sql %}
            SELECT REFRESH_TYPE
            FROM {{ CONTROL_TBL }}
            WHERE IS_ACTIVE = 'Y'
              AND UPPER(TABLE_NAME) = UPPER('{{ source_table_name }}')
        {% endset %}
        {% set res = run_query(sql) %}
        {% if res is not none and (res.rows | length) > 0 %}
            {% set refresh_type = (res.columns[0].values() | list)[0] %}
        {% endif %}
    {% endif %}
    {{ return(refresh_type) }}
{% endmacro %}

{% macro refresco_deltas(stg_entity_name, trans_entity_name, user_name) %}
    {% if execute %}

        {% set u = _refresco_user_value(user_name) %}

        {% set DB_SCHEMA =
            var('audit_db') ~ '.' ~
            var('trans_schema')
        %}
        {% set STTM_TBL =
            DB_SCHEMA ~ '.' ~
            var('sttm_table')
        %}

        {% set gen_sql %}
            SELECT
                'MERGE INTO {{ DB_SCHEMA }}.' || STG_ENTITY || ' AS TGT ' ||
                'USING {{ DB_SCHEMA }}.' || STG_ENTITY || '{{ var("temp_suffix") }} AS SRC ON '
                || LISTAGG('SRC.' || STG_ATRRIBUTE || '=TGT.' || STG_ATRRIBUTE, ' AND ')
                || ' AND TGT.{{ var("col_active_flag") }}=''Y'' '
                || ' WHEN MATCHED THEN UPDATE SET {{ var("col_active_flag") }}=''N'','
                || '{{ var("col_modified_date") }}=CURRENT_DATE(),'
                || '{{ var("col_modified_by") }}=''{{ u }}'''
                AS QRY
            FROM {{ STTM_TBL }}
            WHERE UPPER(STG_ENTITY)   = UPPER('{{ stg_entity_name }}')
              AND UPPER(TRANS_ENTITY) = UPPER('{{ trans_entity_name }}')
              AND SRC_PRIMARYKEY      = TRUE
            GROUP BY STG_ENTITY
        {% endset %}

        {% set q = run_query(gen_sql) %}
        {% if q is not none and (q.rows | length) > 0 %}
            {% set merge_sql = (q.columns[0].values() | list)[0] %}
            {% do run_query(merge_sql) %}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro refresco_full_refresh(table_fqtn) %}
    {% if execute %}
        {% do run_query("UPDATE " ~ table_fqtn ~ " SET " ~ var('col_active_flag') ~ "='N'") %}
    {% endif %}
{% endmacro %}

{% macro refresco_latest_rows(src_fqtn, tgt_fqtn) %}
    {% if execute %}

        {# Target table is guaranteed to exist by this point —
           CREATE TABLE IF NOT EXISTS was already called in insert_raw_target_tables
           before refresco_deltas / refresco_full_refresh ran.
           Get columns from src_fqtn (_TEMP) — always exists, same structure. #}
        {% set col_names   = _get_columns_from_info_schema(src_fqtn) %}
        {% set insert_cols = col_names | join(', ') %}

        {% set select_cols = [] %}
        {% for c in col_names %}
            {% if c | upper == var('col_active_flag') %}
                {% do select_cols.append("NVL(" ~ c ~ ", 'Y') AS " ~ c) %}
            {% else %}
                {% do select_cols.append(c) %}
            {% endif %}
        {% endfor %}

        {% do run_query(
            "INSERT INTO " ~ tgt_fqtn ~ " (" ~ insert_cols ~ ")" ~
            " SELECT " ~ select_cols | join(', ') ~
            " FROM " ~ src_fqtn
        ) %}

    {% endif %}
{% endmacro %}

{% macro refresco_delete_tables(table_fqtn) %}
    {% if execute %}
        {% do run_query("TRUNCATE TABLE " ~ table_fqtn) %}
    {% endif %}
{% endmacro %}

{# ============================================================
  MAIN LOADER
============================================================ #}

{% macro insert_raw_target_tables(
    table_name,
    trans_tablename,
    user_name
) %}

    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {% set DB_SCHEMA =
        var('audit_db') ~ '.' ~
        var('trans_schema')
    %}

    {% set source_table     = table_name ~ var('temp_suffix') %}
    {% set refresh_type     = refresco_refresh_types(source_table) %}
    {% set effective_job_id = invocation_id %}
    {% set eff_user         = _refresco_user_value(user_name) %}

    {% set audit_id = refresco_audit_log_insert(
        job_name   = 'LOADING_DATA TO TARGET TABLE_' ~ table_name,
        job_id     = effective_job_id,
        job_status = 'RUNNING',
        comments   = 'Started load. RefreshType=' ~ refresh_type
                     ~ '. Source=' ~ source_table ~ ', Target=' ~ table_name
    ) %}

    {% set src_tbl = DB_SCHEMA ~ '.' ~ source_table %}
    {% set tgt_tbl = DB_SCHEMA ~ '.' ~ table_name %}

    {# Ensure target table exists BEFORE delta merge or full refresh
       refresco_deltas runs MERGE INTO target — target must exist first #}
    {% do run_query(
        "CREATE TABLE IF NOT EXISTS " ~ tgt_tbl ~
        " AS SELECT * FROM " ~ src_tbl ~ " WHERE 1=0"
    ) %}

    {% if refresh_type == 'DELTA' %}
        {{ refresco_deltas(table_name, trans_tablename, eff_user) }}
    {% else %}
        {{ refresco_full_refresh(tgt_tbl) }}
    {% endif %}

    {{ refresco_latest_rows(src_tbl, tgt_tbl) }}
    {{ refresco_delete_tables(src_tbl) }}

    {% do refresco_audit_log_update(
        audit_id = audit_id,
        status   = 'SUCCESS',
        comments = 'Completed load. RefreshType=' ~ refresh_type
                   ~ '. Source=' ~ src_tbl ~ ', Target=' ~ tgt_tbl
    ) %}

{% endmacro %}

{# ============================================================
  MULTI TABLE WRAPPER
============================================================ #}

{% macro load_multiple_raw_target_tables(
    table_list,
    trans_tablename,
    user_name
) %}

    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {% set effective_job_id = invocation_id %}
    {% set eff_user         = _refresco_user_value(user_name) %}

    {% set batch_audit_id = refresco_audit_log_insert(
        job_name   = 'load_multiple_raw_target_tables',
        job_id     = effective_job_id,
        job_status = 'RUNNING',
        comments   = 'Started batch load. Tables=' ~ (table_list | join(','))
                     ~ ', TransEntity=' ~ trans_tablename
    ) %}

    {% for table_name in table_list %}
        {{ insert_raw_target_tables(
            table_name      = table_name,
            trans_tablename = trans_tablename,
            user_name       = eff_user
        ) }}
    {% endfor %}

    {% do refresco_audit_log_update(
        audit_id = batch_audit_id,
        status   = 'SUCCESS',
        comments = 'Completed batch load. Tables=' ~ (table_list | join(','))
                   ~ ', TransEntity=' ~ trans_tablename
    ) %}

{% endmacro %}

{# ============================================================
  ERROR IDENTIFICATION MACROS
  Uses STTM_UPDATED SRC_PRIMARYKEY=TRUE cols to detect NULL PK
  rows in _TEMP and routes them to _ERROR before Stage 3.
============================================================ #}

{% macro identify_and_load_errors(table_name, trans_tablename) %}

    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {% set DB_SCHEMA  =
        var('audit_db') ~ '.' ~
        var('trans_schema')
    %}
    {% set STTM_TBL   = DB_SCHEMA ~ '.' ~ var('sttm_table') %}
    {% set temp_fqtn  = DB_SCHEMA ~ '.' ~ table_name ~ var('temp_suffix') %}
    {% set error_fqtn = DB_SCHEMA ~ '.' ~ table_name ~ var('error_suffix') %}

    {% set audit_id = refresco_audit_log_insert(
        job_name   = 'IDENTIFY_ERRORS_' ~ table_name,
        job_id     = invocation_id,
        job_status = 'RUNNING',
        comments   = 'Identifying NULL PK rows in ' ~ temp_fqtn
    ) %}

    {# STEP 1: Get PK columns from STTM_UPDATED #}
    {% set pk_sql %}
        SELECT STG_ATRRIBUTE
        FROM   {{ STTM_TBL }}
        WHERE  UPPER(STG_ENTITY)   = UPPER('{{ table_name }}')
          AND  UPPER(TRANS_ENTITY) = UPPER('{{ trans_tablename }}')
          AND  SRC_PRIMARYKEY      = TRUE
        ORDER BY STG_ATRRIBUTE
    {% endset %}

    {% set pk_res  = run_query(pk_sql) %}
    {% set pk_cols = pk_res.columns[0].values() | map('upper') | list
        if (pk_res is not none and (pk_res.rows | length) > 0) else [] %}

    {% if (pk_cols | length) == 0 %}
        {% do refresco_audit_log_update(
            audit_id = audit_id,
            status   = 'SUCCESS',
            comments = 'No PK columns in STTM_UPDATED for ' ~ table_name ~ ' — error step skipped.'
        ) %}
        {{ log('[' ~ table_name ~ '] WARNING: No PK columns found — error identification skipped.', info=True) }}
        {{ return('') }}
    {% endif %}

    {# STEP 2: Auto-create _ERROR table if it does not exist
       Structure = _TEMP columns + ERROR_REASON + ERROR_CAPTURED_AT #}
    {% do run_query(
        "CREATE TABLE IF NOT EXISTS " ~ error_fqtn ~
        " AS SELECT *" ~
        ", CAST(NULL AS VARCHAR) AS " ~ var('col_error_reason') ~
        ", CAST(NULL AS TIMESTAMP) AS " ~ var('col_error_captured_at') ~
        " FROM " ~ temp_fqtn ~ " WHERE 1=0"
    ) %}

    {{ log('[' ~ table_name ~ '] _ERROR table ready: ' ~ error_fqtn, info=True) }}

    {# STEP 3: Build NULL check condition on PK columns #}
    {% set null_checks = [] %}
    {% for pk in pk_cols %}
        {% do null_checks.append(pk ~ ' IS NULL') %}
    {% endfor %}
    {% set null_condition = null_checks | join(' OR ') %}

    {% set reason_parts = [] %}
    {% for pk in pk_cols %}
        {% do reason_parts.append(
            "IFF(" ~ pk ~ " IS NULL, '" ~ pk ~ " IS NULL', NULL)"
        ) %}
    {% endfor %}
    {% set error_reason_expr =
        "TRIM(CONCAT_WS(', ', " ~ reason_parts | join(', ') ~ "))"
    %}

    {# STEP 4: Count error rows #}
    {% set error_cnt = run_query(
        "SELECT COUNT(*) FROM " ~ temp_fqtn ~ " WHERE " ~ null_condition
    ).columns[0].values()[0] %}

    {{ log('[' ~ table_name ~ '] Error rows (null PK): ' ~ error_cnt, info=True) }}

    {% if error_cnt > 0 %}

        {# Get _TEMP columns via INFORMATION_SCHEMA — no adapter cache #}
        {% set temp_col_names = _get_columns_from_info_schema(temp_fqtn) %}

        {% set insert_col_list = temp_col_names | join(', ')
            ~ ', ' ~ var('col_error_reason')
            ~ ', ' ~ var('col_error_captured_at') %}

        {% set select_col_list = temp_col_names | join(', ')
            ~ ', ' ~ error_reason_expr ~ ' AS ' ~ var('col_error_reason')
            ~ ', CURRENT_TIMESTAMP() AS ' ~ var('col_error_captured_at') %}

        {# INSERT error rows into _ERROR #}
        {% do run_query(
            "INSERT INTO " ~ error_fqtn ~
            " (" ~ insert_col_list ~ ")" ~
            " SELECT " ~ select_col_list ~
            " FROM " ~ temp_fqtn ~
            " WHERE " ~ null_condition
        ) %}

        {# DELETE error rows from _TEMP so Stage 3 only loads clean rows #}
        {% do run_query(
            "DELETE FROM " ~ temp_fqtn ~ " WHERE " ~ null_condition
        ) %}

    {% endif %}

    {% set clean_cnt = run_query(
        "SELECT COUNT(*) FROM " ~ temp_fqtn
    ).columns[0].values()[0] %}

    {% do refresco_audit_log_update(
        audit_id = audit_id,
        status   = 'SUCCESS',
        comments = 'Error rows moved=' ~ error_cnt
                   ~ ' | Clean rows remaining=' ~ clean_cnt
                   ~ ' | PKs checked=' ~ pk_cols | join(',')
    ) %}

    {{ log('[' ~ table_name ~ '] {"status":"SUCCESS","error_cnt":' ~ error_cnt
          ~ ',"clean_cnt":' ~ clean_cnt ~ ',"audit_id":' ~ audit_id ~ '}', info=True) }}

{% endmacro %}


{% macro identify_and_load_errors_multi(table_list, trans_tablename) %}

    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {% set batch_audit_id = refresco_audit_log_insert(
        job_name   = 'BATCH_IDENTIFY_ERRORS',
        job_id     = invocation_id,
        job_status = 'RUNNING',
        comments   = 'Error check for: ' ~ (table_list | join(', '))
    ) %}

    {% for table_name in table_list %}
        {{ identify_and_load_errors(
            table_name      = table_name,
            trans_tablename = trans_tablename
        ) }}
    {% endfor %}

    {% do refresco_audit_log_update(
        audit_id = batch_audit_id,
        status   = 'SUCCESS',
        comments = 'Error check complete for: ' ~ (table_list | join(', '))
    ) %}

{% endmacro %}
