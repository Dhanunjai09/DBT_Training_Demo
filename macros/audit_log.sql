{# =============================================================================
   FILE: macros/audit_log.sql
   PURPOSE: Standalone audit log macros used by all pipeline stages.
            Single source of truth — no duplication across files.
   MACROS:
     - audit_log_insert(job_name, job_id, job_status, comments)
     - audit_log_update(audit_id, status, comments)
   ============================================================================= #}

{% macro _audit_nextval() %}
    {% if not execute %}{{ return(none) }}{% endif %}
    {% set res = run_query(
        "SELECT " ~ var('audit_db') ~ "." ~ var('trans_schema') ~ "." ~ var('audit_seq') ~ ".nextval AS AUDIT_ID"
    ) %}
    {{ return(res.columns[0].values()[0]) }}
{% endmacro %}

{% macro _audit_now_str() %}
    {{ return("to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS')") }}
{% endmacro %}

{% macro audit_log_insert(job_name, job_id, job_status, comments, task_start_time=none, job_start_time=none) %}
    {% if not execute %}{{ return(none) }}{% endif %}

    {% set AUDIT_TBL = var('audit_db') ~ '.' ~ var('trans_schema') ~ '.' ~ var('audit_table') %}
    {% set audit_id  = _audit_nextval() %}

    {% set c   = (comments   if comments   is not none else '') | replace("'", "''") %}
    {% set jid = (job_id     if job_id     is not none else '') | replace("'", "''") %}
    {% set jnm = (job_name   if job_name   is not none else '') | replace("'", "''") %}
    {% set st  = (job_status if job_status is not none else '') | replace("'", "''") %}

    {% set ts = "'" ~ (task_start_time | replace("'","''")) ~ "'" if task_start_time is not none else _audit_now_str() %}
    {% set js = "'" ~ (job_start_time  | replace("'","''")) ~ "'" if job_start_time  is not none else _audit_now_str() %}

    {% do run_query(
        "INSERT INTO " ~ AUDIT_TBL ~
        " (AUDIT_ID,JOB_NAME,TASK_START_TIME,TASK_END_TIME,JOB_START_TIME,JOB_END_TIME,JOB_ID,JOB_STATUS,COMMENTS) VALUES (" ~
        audit_id ~ ",'" ~ jnm ~ "'," ~ ts ~ ",NULL," ~ js ~ ",NULL,'" ~ jid ~ "','" ~ st ~ "','" ~ c ~ "')"
    ) %}
    {{ return(audit_id) }}
{% endmacro %}

{% macro audit_log_update(audit_id, status, comments, task_end_time=none, job_end_time=none) %}
    {% if not execute %}{{ return('') }}{% endif %}

    {% set AUDIT_TBL = var('audit_db') ~ '.' ~ var('trans_schema') ~ '.' ~ var('audit_table') %}
    {% set c  = (comments if comments is not none else '') | replace("'", "''") %}
    {% set st = (status   if status   is not none else '') | replace("'", "''") %}
    {% set te = "'" ~ (task_end_time | replace("'","''")) ~ "'" if task_end_time is not none else _audit_now_str() %}
    {% set je = "'" ~ (job_end_time  | replace("'","''")) ~ "'" if job_end_time  is not none else _audit_now_str() %}

    {% do run_query(
        "UPDATE " ~ AUDIT_TBL ~
        " SET JOB_STATUS='" ~ st ~ "', TASK_END_TIME=" ~ te ~ ", JOB_END_TIME=" ~ je ~ ", COMMENTS='" ~ c ~ "'" ~
        " WHERE AUDIT_ID=" ~ audit_id ~ " AND JOB_STATUS='RUNNING'"
    ) %}
    {{ return('') }}
{% endmacro %}
