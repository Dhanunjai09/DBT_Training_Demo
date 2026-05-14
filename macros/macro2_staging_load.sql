{# ============================================================
  AUDIT LOG MACROS (INSERT + UPDATE) + RAW->TARGET LOAD MACROS
  All values resolved via var() — no hardcoding anywhere.
  var() values are set in dbt_project.yml via env_var('DBT_*')
  which are configured in dbt Cloud Environment Variables UI.
============================================================ #}

{% macro _pipeline_user_sql() %}
    {% set u = var('pipeline_user') %}
    {{ return("'" ~ (u | replace("'", "''")) ~ "'") }}
{% endmacro %}

{% macro _audit_now_str() %}
    {{ return("to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS')") }}
{% endmacro %}

{% macro _audit_nextval() %}
    {% if not execute %}
        {{ return(none) }}
    {% endif %}

    {% set seq_sql %}
        SELECT {{ var('audit_db') }}.{{ var('trans_schema') }}.{{ var('audit_seq') }}.nextval AS AUDIT_ID
    {% endset %}

    {% set res = run_query(seq_sql) %}
    {{ return(res.columns[0].values()[0]) }}
{% endmacro %}

{% macro audit_log_insert(
        table_name,
        job_id,
        job_name,
        job_status,
        comments,
        task_start_time=none,
        job_start_time=none
    ) %}
    {% if not execute %}
        {{ return(none) }}
    {% endif %}

    {% set AUDIT_TABLE_NAME =
        var('audit_db') ~ '.' ~
        var('trans_schema') ~ '.' ~
        var('audit_table')
    %}

    {% set audit_id = _audit_nextval() %}

    {% set c    = (comments   if comments   is not none else '') | replace("'", "''") %}
    {% set j_id = (job_id     if job_id     is not none else '') | replace("'", "''") %}
    {% set j_nm = (job_name   if job_name   is not none else '') | replace("'", "''") %}
    {% set st   = (job_status if job_status is not none else '') | replace("'", "''") %}

    {% if task_start_time is none %}
        {% set task_start_expr = _audit_now_str() %}
    {% else %}
        {% set task_start_expr = "'" ~ (task_start_time | replace("'", "''")) ~ "'" %}
    {% endif %}

    {% if job_start_time is none %}
        {% set job_start_expr = _audit_now_str() %}
    {% else %}
        {% set job_start_expr = "'" ~ (job_start_time | replace("'", "''")) ~ "'" %}
    {% endif %}

    {% set ins_sql %}
        INSERT INTO {{ AUDIT_TABLE_NAME }}
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

{% macro audit_log_update(
        table_name,
        audit_id,
        status,
        comments,
        task_end_time=none,
        job_end_time=none
    ) %}
    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {% set AUDIT_TABLE_NAME =
        var('audit_db') ~ '.' ~
        var('trans_schema') ~ '.' ~
        var('audit_table')
    %}

    {% set c  = (comments if comments is not none else '') | replace("'", "''") %}
    {% set st = (status   if status   is not none else '') | replace("'", "''") %}

    {% if task_end_time is none %}
        {% set task_end_expr = _audit_now_str() %}
    {% else %}
        {% set task_end_expr = "'" ~ (task_end_time | replace("'", "''")) ~ "'" %}
    {% endif %}

    {% if job_end_time is none %}
        {% set job_end_expr = _audit_now_str() %}
    {% else %}
        {% set job_end_expr = "'" ~ (job_end_time | replace("'", "''")) ~ "'" %}
    {% endif %}

    {% set upd_sql %}
        UPDATE {{ AUDIT_TABLE_NAME }}
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
  RAW -> TEMP LOAD MACRO
============================================================ #}

{% macro insert_staging_data_raw_target(table_name, SRC_TABLE, TGT_TABLE) %}

    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {% set job_id         = invocation_id %}
    {% set audit_user_sql = _pipeline_user_sql() %}

    {% set ROW_COUNT_TBL =
        var('audit_db') ~ '.' ~
        var('trans_schema') ~ '.' ~
        var('row_count_table')
    %}

    {% set audit_id = audit_log_insert(
        table_name = table_name,
        job_id     = job_id,
        job_name   = 'LOADING_DATA FROM STAGING TABLE TO TEMP TABLE_' ~ table_name,
        job_status = 'RUNNING',
        comments   = 'Started load from ' ~ SRC_TABLE ~ ' to ' ~ TGT_TABLE
    ) %}

    {% set src_parts  = SRC_TABLE.split('.') %}
    {% set src_db     = src_parts[0] %}
    {% set src_schema = src_parts[1] %}
    {% set src_table  = src_parts[2] %}

    {% set tgt_parts  = TGT_TABLE.split('.') %}
    {% set tgt_db     = tgt_parts[0] %}
    {% set tgt_schema = tgt_parts[1] %}
    {% set tgt_table  = tgt_parts[2] %}

    {# Watermark must come from the FINAL TARGET table, not _TEMP.
       _TEMP is truncated after every run so MAX(INSERTEDAT) would
       always return NULL causing all rows to reload every run.
       Final target table = TGT_TABLE with temp_suffix removed.     #}
    {% set final_target = tgt_db ~ '.' ~ tgt_schema ~ '.' ~
        tgt_table | replace(var('temp_suffix'), '')
    %}

    {% set tgt_max_res = run_query(
        "SELECT MAX(" ~ var('incremental_col') ~ ") FROM " ~ final_target
    ) %}
    {% set tgt_max_val = tgt_max_res.columns[0].values()[0] %}

    {% if tgt_max_val is none %}
        {% set cutoff = "TO_TIMESTAMP('" ~ var('incremental_default_date') ~ "')" %}
    {% else %}
        {% set cutoff = "TO_TIMESTAMP('" ~ tgt_max_val ~ "')" %}
    {% endif %}

    {{ log("Cutoff " ~ var('incremental_col') ~ " = " ~ cutoff, info=True) }}

    {% set src_cnt = run_query(
        "SELECT COUNT(*) FROM " ~ SRC_TABLE ~
        " WHERE " ~ var('incremental_col') ~ " > " ~ cutoff
    ).columns[0].values()[0] %}

    {% if src_cnt == 0 %}

        {% do run_query("BEGIN") %}
        {% do run_query(
            "DELETE FROM " ~ ROW_COUNT_TBL ~
            " WHERE TABLE_NAME = '" ~ table_name ~ "'"
        ) %}
        {% do run_query(
            "INSERT INTO " ~ ROW_COUNT_TBL ~
            " (TABLE_NAME, SRC_TABLE_COUNT, TGT_TABLE_COUNT, STATUS) VALUES (" ~
            "'" ~ table_name ~ "', 0, 0, 'SUCCESS')"
        ) %}
        {% do run_query("COMMIT") %}

        {% do audit_log_update(
            table_name = table_name,
            audit_id   = audit_id,
            status     = 'SUCCESS',
            comments   = 'No new rows. Cutoff=' ~ cutoff
        ) %}

        {{ log('{"status":"SUCCESS","src_cnt":0,"inserted":0,"audit_id":' ~ audit_id ~ '}', info=True) }}

    {% else %}

        {% set tgt_relation = adapter.get_relation(
            database=tgt_db, schema=tgt_schema, identifier=tgt_table
        ) %}

        {% if tgt_relation is none %}
            {% do audit_log_update(
                table_name = table_name,
                audit_id   = audit_id,
                status     = 'FAILURE',
                comments   = 'Target relation not found: ' ~ TGT_TABLE
            ) %}
            {% do exceptions.raise_compiler_error("Target relation not found: " ~ TGT_TABLE) %}
        {% endif %}

        {% set tgt_cols = adapter.get_columns_in_relation(tgt_relation) %}

        {% set src_cols = run_query(
            "SELECT COLUMN_NAME FROM " ~ src_db ~ ".INFORMATION_SCHEMA.COLUMNS " ~
            "WHERE TABLE_SCHEMA = '" ~ src_schema ~ "' " ~
            "AND TABLE_NAME = '" ~ src_table ~ "'"
        ).columns[0].values() | map('upper') | list %}

        {% set select_exprs = [] %}
        {% for col in tgt_cols %}
            {% set col_u = col.name.upper() %}
            {% set col_q = adapter.quote(col.name) %}
            {% if   col_u == var('col_created_by')   %}{% do select_exprs.append(audit_user_sql ~ " AS " ~ col_q) %}
            {% elif col_u == var('col_modified_by')  %}{% do select_exprs.append(audit_user_sql ~ " AS " ~ col_q) %}
            {% elif col_u == var('col_created_date') %}{% do select_exprs.append("CURRENT_DATE() AS " ~ col_q) %}
            {% elif col_u == var('col_modified_date')%}{% do select_exprs.append("CURRENT_DATE() AS " ~ col_q) %}
            {% elif col_u in src_cols               %}{% do select_exprs.append(col_q) %}
            {% else                                 %}{% do select_exprs.append("NULL AS " ~ col_q) %}
            {% endif %}
        {% endfor %}

        {% set tgt_col_list  = tgt_cols | map(attribute='name') | map('string') | join(', ') %}
        {% set sel_expr_list = select_exprs | join(', ') %}

        {% do run_query(
            "INSERT INTO " ~ TGT_TABLE ~ " (" ~ tgt_col_list ~ ") " ~
            "SELECT " ~ sel_expr_list ~
            " FROM " ~ SRC_TABLE ~
            " WHERE " ~ var('incremental_col') ~ " > " ~ cutoff
        ) %}

        {% do run_query("BEGIN") %}
        {% do run_query(
            "DELETE FROM " ~ ROW_COUNT_TBL ~
            " WHERE TABLE_NAME = '" ~ table_name ~ "'"
        ) %}
        {% do run_query(
            "INSERT INTO " ~ ROW_COUNT_TBL ~
            " (TABLE_NAME, SRC_TABLE_COUNT, TGT_TABLE_COUNT, STATUS) VALUES (" ~
            "'" ~ table_name ~ "', " ~ src_cnt ~ ", " ~ src_cnt ~ ", 'SUCCESS')"
        ) %}
        {% do run_query("COMMIT") %}

        {% do audit_log_update(
            table_name = table_name,
            audit_id   = audit_id,
            status     = 'SUCCESS',
            comments   = 'Inserted ' ~ src_cnt ~ ' rows. Cutoff=' ~ cutoff
        ) %}

        {{ log('{"status":"SUCCESS","src_cnt":' ~ src_cnt ~ ',"inserted":' ~ src_cnt ~ ',"audit_id":' ~ audit_id ~ '}', info=True) }}

    {% endif %}

{% endmacro %}

{# ============================================================
  MULTI-TABLE WRAPPER
============================================================ #}

{% macro load_staging_from_raw_multi_tables(tables) %}
    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {% for t in tables %}
        {{ insert_staging_data_raw_target(t.table_name, t.SRC_TABLE, t.TGT_TABLE) }}
    {% endfor %}
{% endmacro %}
