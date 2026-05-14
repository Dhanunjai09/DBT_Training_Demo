{# ============================================================
  MASTER WRAPPER MACRO
  Calls:
    1) load_staging_from_raw_multi_tables   (RAW → TEMP)
    2) identify_and_load_errors_multi       (TEMP → ERROR)
    3) load_multiple_raw_target_tables      (TEMP clean → TARGET)
  All values resolved via env_var() in dbt_project.yml — no hardcoding
============================================================ #}

{% macro run_stage_and_refresco_pipeline(
    tables,
    table_list,
    trans_tablename
) %}

    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {% set user_name = var('pipeline_user') %}

    {# STAGE 1: RAW → TEMP #}
    {{ load_staging_from_raw_multi_tables(tables) }}

    {# STAGE 2: TEMP → ERROR (identify null PK rows, remove from TEMP) #}
    {{ identify_and_load_errors_multi(
        table_list      = table_list,
        trans_tablename = trans_tablename
    ) }}

    {# STAGE 3: TEMP (clean rows only) → TARGET #}
    {{ load_multiple_raw_target_tables(
        table_list      = table_list,
        trans_tablename = trans_tablename,
        user_name       = user_name
    ) }}

    {{ return('SUCCESS') }}

{% endmacro %}
