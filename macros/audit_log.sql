{# =============================================================================
   FILE: macros/audit_log.sql
   PURPOSE: Per-entity audit log macros.
            Each TRANS_ENTITY gets its own audit table: <TRANS_ENTITY>_AUDITLOG
            Auto-creates audit table if it does not exist (audit table only —
            NOT source/target/temp/error tables; those are pre-created in Snowflake).
            Uses BATCH_AUDIT_SEQ for AUDIT_ID.
   MACROS:
     - _get_audit_table(trans_entity)      ← builds FQ audit table name
     - _ensure_audit_table(trans_entity)   ← creates audit table if not exists
     - _audit_nextval()                    ← gets next sequence value
     - _audit_now_str()                    ← current timestamp as varchar
     - audit_log_insert(...)               ← opens audit row (RUNNING)
     - audit_log_update(...)               ← closes audit row (SUCCESS/FAILURE)
   ============================================================================= #}

{% macro _get_audit_table(trans_entity) %}
    {{ return(
        var('target_database') ~ '.' ~
        var('trans_schema') ~ '.' ~
        trans_entity ~ var('audit_log_suffix')
    ) }}
{% endmacro %}


{% macro _ensure_audit_table(trans_entity) %}
    {% if not execute %}{{ return('') }}{% endif %}

    {% set audit_tbl = _get_audit_table(trans_entity) %}
    {% set db     = var('target_database') %}
    {% set schema = var('trans_schema') %}
    {% set tbl    = trans_entity ~ var('audit_log_suffix') %}

    {% set exists = run_query(
        "SELECT COUNT(*) FROM " ~ db ~ ".INFORMATION_SCHEMA.TABLES" ~
        " WHERE TABLE_SCHEMA = '" ~ schema ~ "'" ~
        " AND TABLE_NAME = '" ~ tbl ~ "'"
    ).columns[0].values()[0] %}

    {% if exists == 0 %}
        {% do run_query(
            "CREATE TABLE IF NOT EXISTS " ~ audit_tbl ~ " (" ~
            " AUDIT_ID NUMBER(38,0) DEFAULT " ~
                var('target_database') ~ "." ~ var('trans_schema') ~ "." ~ var('audit_seq') ~ ".NEXTVAL," ~
            " JOB_NAME VARCHAR(255)," ~
            " TASK_START_TIME VARCHAR(50)," ~
            " TASK_END_TIME VARCHAR(50)," ~
            " JOB_START_TIME VARCHAR(50)," ~
            " JOB_END_TIME VARCHAR(50)," ~
            " JOB_ID VARCHAR(255)," ~
            " JOB_STATUS VARCHAR(200)," ~
            " COMMENTS VARCHAR(16777216)" ~
            ")"
        ) %}
        {{ log('[AUDIT] Created audit table: ' ~ audit_tbl, info=True) }}
    {% endif %}
{% endmacro %}


{% macro _audit_nextval() %}
    {% if not execute %}{{ return(none) }}{% endif %}
    {% set res = run_query(
        "SELECT " ~
        var('target_database') ~ "." ~
        var('trans_schema') ~ "." ~
        var('audit_seq') ~ ".nextval AS AUDIT_ID"
    ) %}
    {{ return(res.columns[0].values()[0]) }}
{% endmacro %}


{% macro _audit_now_str() %}
    {{ return("to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS')") }}
{% endmacro %}


{% macro audit_log_insert(
        trans_entity,
        job_name,
        job_status,
        comments,
        task_start_time=none,
        job_start_time=none
    ) %}
    {% if not execute %}{{ return(none) }}{% endif %}

    {# Auto-create audit table if not exists — only the audit table, not data tables #}
    {{ _ensure_audit_table(trans_entity) }}

    {% set AUDIT_TBL = _get_audit_table(trans_entity) %}
    {% set audit_id  = _audit_nextval() %}
    {% set job_id    = invocation_id %}

    {% set c   = (comments   if comments   is not none else '') | replace("'", "''") %}
    {% set jnm = (job_name   if job_name   is not none else '') | replace("'", "''") %}
    {% set st  = (job_status if job_status is not none else '') | replace("'", "''") %}
    {% set jid = (job_id     if job_id     is not none else '') | replace("'", "''") %}

    {% set ts = "'" ~ (task_start_time | replace("'","''")) ~ "'"
        if task_start_time is not none else _audit_now_str() %}
    {% set js = "'" ~ (job_start_time  | replace("'","''")) ~ "'"
        if job_start_time  is not none else _audit_now_str() %}

    {% do run_query(
        "INSERT INTO " ~ AUDIT_TBL ~
        " (AUDIT_ID, JOB_NAME, TASK_START_TIME, TASK_END_TIME," ~
        "  JOB_START_TIME, JOB_END_TIME, JOB_ID, JOB_STATUS, COMMENTS)" ~
        " VALUES (" ~
        audit_id ~ ",'" ~ jnm ~ "'," ~ ts ~ ",NULL," ~
        js ~ ",NULL,'" ~ jid ~ "','" ~ st ~ "','" ~ c ~ "')"
    ) %}
    {{ return(audit_id) }}
{% endmacro %}


{% macro audit_log_update(
        trans_entity,
        audit_id,
        status,
        comments,
        task_end_time=none,
        job_end_time=none
    ) %}
    {% if not execute %}{{ return('') }}{% endif %}

    {% set AUDIT_TBL = _get_audit_table(trans_entity) %}
    {% set c  = (comments if comments is not none else '') | replace("'", "''") %}
    {% set st = (status   if status   is not none else '') | replace("'", "''") %}

    {% set te = "'" ~ (task_end_time | replace("'","''")) ~ "'"
        if task_end_time is not none else _audit_now_str() %}
    {% set je = "'" ~ (job_end_time  | replace("'","''")) ~ "'"
        if job_end_time  is not none else _audit_now_str() %}

    {% do run_query(
        "UPDATE " ~ AUDIT_TBL ~
        " SET JOB_STATUS='" ~ st ~ "'," ~
        " TASK_END_TIME=" ~ te ~ "," ~
        " JOB_END_TIME=" ~ je ~ "," ~
        " COMMENTS='" ~ c ~ "'" ~
        " WHERE AUDIT_ID=" ~ audit_id ~
        " AND JOB_STATUS='RUNNING'"
    ) %}
    {{ return('') }}
{% endmacro %}
