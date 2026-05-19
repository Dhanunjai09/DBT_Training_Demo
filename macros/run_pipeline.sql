{# =============================================================================
   FILE: macros/run_pipeline.sql
   PURPOSE: Master pipeline orchestrator — single entry point.
            Resolves all table metadata dynamically from STTM_UPDATED.
            Creates tables if not exist before loading.
            Per-entity audit tables auto-created if not exist.

   FULL FLOW:
     STTM_UPDATED (ACTIVE_FLAG=TRUE)
       → Step 0: ensure_pipeline_tables_batch  (create TARGET/_TEMP/_ERROR if missing)
       → Stage 1: load_raw_to_temp_multi       (EXTRACT_SAP → _TEMP, incremental)
       → Stage 2: identify_errors_multi        (_TEMP → _ERROR, NULL PK rows)
       → Stage 3: load_temp_to_target_multi    (_TEMP clean → TARGET, DELTA/FULL)

   USAGE:
     dbt run-operation run_pipeline --args '{"table_name": "ALL"}'
     dbt run-operation run_pipeline --args '{"table_name": "CUSTOMERS"}'
   ============================================================================= #}

{% macro _resolve_pipeline_tables(table_name) %}
    {% if not execute %}{{ return([]) }}{% endif %}

    {% set STTM_TBL = var('target_database') ~ '.' ~
                      var('trans_schema') ~ '.' ~
                      var('sttm_table') %}

    {% set filter = "ACTIVE_FLAG = TRUE"
        if table_name | upper == 'ALL'
        else "ACTIVE_FLAG = TRUE AND UPPER(STG_ENTITY) = UPPER('" ~ table_name ~ "')" %}

    {% set res = run_query(
        "SELECT DISTINCT SRC_ENTITY, STG_ENTITY, TRANS_ENTITY" ~
        " FROM " ~ STTM_TBL ~
        " WHERE " ~ filter ~
        " ORDER BY STG_ENTITY"
    ) %}

    {% if res is none or (res.rows | length) == 0 %}
        {{ exceptions.raise_compiler_error(
            "No active tables found in STTM for table_name='" ~ table_name ~ "'. " ~
            "Check STTM_UPDATED ACTIVE_FLAG and STG_ENTITY values."
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

    {{ log("=== run_pipeline START | table_name=" ~ table_name ~
           " | env=" ~ target.name ~
           " | db=" ~ var('target_database') ~ " ===", info=True) }}

    {# Resolve table list from STTM — one query drives everything #}
    {% set pipeline_tables = _resolve_pipeline_tables(table_name) %}

    {{ log("Tables: " ~ (pipeline_tables | map(attribute='stg_entity') | join(', ')), info=True) }}

    {# Batch-level audit — uses first table's trans_entity #}
    {% set batch_audit_id = audit_log_insert(
        trans_entity = pipeline_tables[0].trans_entity,
        job_name     = 'RUN_PIPELINE_' ~ table_name,
        job_status   = 'RUNNING',
        comments     = 'db=' ~ var('target_database') ~
                       ' | Tables=' ~ (pipeline_tables | map(attribute='stg_entity') | join(','))
    ) %}

    {# Step 0: Ensure TARGET, _TEMP, _ERROR tables exist — create from STTM if missing #}
    {{ log("--- Step 0: ensure_pipeline_tables_batch ---", info=True) }}
    {{ ensure_pipeline_tables_batch(pipeline_tables) }}

    {# Stage 1: EXTRACT_SAP → _TEMP (incremental, same logic all tables) #}
    {{ log("--- Stage 1: load_raw_to_temp_multi ---", info=True) }}
    {{ load_raw_to_temp_multi(pipeline_tables) }}

    {# Stage 2: _TEMP → _ERROR (NULL PK rows removed from _TEMP) #}
    {{ log("--- Stage 2: identify_errors_multi ---", info=True) }}
    {{ identify_errors_multi(pipeline_tables) }}

    {# Stage 3: _TEMP (clean rows only) → TARGET (DELTA/FULL from control table) #}
    {{ log("--- Stage 3: load_temp_to_target_multi ---", info=True) }}
    {{ load_temp_to_target_multi(pipeline_tables) }}

    {% do audit_log_update(
        trans_entity = pipeline_tables[0].trans_entity,
        audit_id     = batch_audit_id,
        status       = 'SUCCESS',
        comments     = 'Pipeline complete. Tables=' ~
                       (pipeline_tables | map(attribute='stg_entity') | join(','))
    ) %}

    {{ log("=== run_pipeline END | table_name=" ~ table_name ~ " ===", info=True) }}
    {{ return('SUCCESS') }}
{% endmacro %}
