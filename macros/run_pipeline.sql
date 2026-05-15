{# =============================================================================
   FILE: macros/run_pipeline.sql
   PURPOSE: Master pipeline orchestrator — single entry point.
            Table metadata resolved dynamically from STTM_UPDATED.
            Stage 1 uses same incremental logic for all tables.
            Stage 3 uses STG_REFRESH_TYPE from STTM for MERGE vs UPDATE only.

   USAGE:
     dbt run-operation run_pipeline --args '{"table_name": "ALL"}'
     dbt run-operation run_pipeline --args '{"table_name": "CUSTOMERS"}'
   ============================================================================= #}

{% macro _resolve_pipeline_tables(table_name) %}
    {% if not execute %}{{ return([]) }}{% endif %}

    {% set STTM_TBL = var('audit_db') ~ '.' ~ var('trans_schema') ~ '.' ~ var('sttm_table') %}

    {% set filter = "ACTIVE_FLAG = TRUE" if table_name | upper == 'ALL'
        else "ACTIVE_FLAG = TRUE AND UPPER(STG_ENTITY) = UPPER('" ~ table_name ~ "')" %}

    {% set res = run_query(
        "SELECT DISTINCT SRC_ENTITY, STG_ENTITY, TRANS_ENTITY" ~
        " FROM " ~ STTM_TBL ~
        " WHERE " ~ filter ~
        " ORDER BY STG_ENTITY"
    ) %}

    {% if res is none or (res.rows | length) == 0 %}
        {{ exceptions.raise_compiler_error(
            "No active tables found in STTM for table_name='" ~ table_name ~ "'."
        ) }}
    {% endif %}

    {% set pipeline_tables = [] %}
    {% for row in res.rows %}
        {% do pipeline_tables.append({
            "src_entity":   row[0],
            "stg_entity":   row[1],
            "trans_entity": row[2]
        }) %}
    {% endfor %}

    {{ return(pipeline_tables) }}
{% endmacro %}


{% macro run_pipeline(table_name='ALL') %}
    {% if not execute %}{{ return('') }}{% endif %}

    {{ log("=== run_pipeline START | table_name=" ~ table_name ~ " | env=" ~ target.name ~ " ===", info=True) }}

    {% set pipeline_tables = _resolve_pipeline_tables(table_name) %}

    {{ log("Tables: " ~ (pipeline_tables | map(attribute='stg_entity') | join(', ')), info=True) }}

    {% set batch_audit_id = audit_log_insert(
        job_name   = 'RUN_PIPELINE_' ~ table_name,
        job_id     = invocation_id,
        job_status = 'RUNNING',
        comments   = 'Tables=' ~ (pipeline_tables | map(attribute='stg_entity') | join(','))
    ) %}

    {# Stage 1: EXTRACT_SAP → _TEMP
       Same incremental logic for all tables.
       First run loads all rows, subsequent runs load only new rows. #}
    {{ log("--- Stage 1: load_raw_to_temp_multi ---", info=True) }}
    {{ load_raw_to_temp_multi(pipeline_tables) }}

    {# Stage 2: _TEMP → _ERROR
       Identifies NULL PK rows and removes them from _TEMP. #}
    {{ log("--- Stage 2: identify_errors_multi ---", info=True) }}
    {{ identify_errors_multi(pipeline_tables) }}

    {# Stage 3: _TEMP (clean rows only) → TARGET
       STG_REFRESH_TYPE from STTM drives MERGE (DELTA) or UPDATE (FULL). #}
    {{ log("--- Stage 3: load_temp_to_target_multi ---", info=True) }}
    {{ load_temp_to_target_multi(pipeline_tables) }}

    {% do audit_log_update(audit_id=batch_audit_id, status='SUCCESS',
        comments='Pipeline complete. Tables=' ~ (pipeline_tables | map(attribute='stg_entity') | join(','))) %}

    {{ log("=== run_pipeline END | table_name=" ~ table_name ~ " ===", info=True) }}
    {{ return('SUCCESS') }}
{% endmacro %}
