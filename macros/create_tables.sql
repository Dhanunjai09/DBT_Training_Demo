{# =============================================================================
   FILE: macros/create_tables.sql
   PURPOSE: Auto-create TARGET, _TEMP, and _ERROR tables from STTM_UPDATED
            STG_ columns if they do not already exist.
            Audit columns (CREATED_BY, CREATED_DATE, MODIFIED_BY,
            MODIFIED_DATE, ACTIVE_FLAG, INSERTEDAT) added automatically
            by the macro — they are not in STTM.
            All three table types have identical column structure.
   MACROS:
     - _build_create_table_sql(table_fqtn, stg_entity)
            ← builds CREATE TABLE SQL from STTM columns + audit columns
     - _ensure_table_exists(table_fqtn, stg_entity)
            ← creates table only if not exists
     - ensure_pipeline_tables(stg_entity)
            ← ensures TARGET, _TEMP, _ERROR all exist for one entity
     - ensure_pipeline_tables_batch(pipeline_tables)
            ← multi-table wrapper
   ============================================================================= #}

{% macro _build_create_table_sql(table_fqtn, stg_entity) %}
    {% if not execute %}{{ return('') }}{% endif %}

    {% set DB_SCHEMA = var('target_database') ~ '.' ~ var('trans_schema') %}
    {% set STTM_TBL  = DB_SCHEMA ~ '.' ~ var('sttm_table') %}

    {# Get columns from STTM for this STG_ENTITY #}
    {% set col_res = run_query(
        "SELECT STG_ATRRIBUTE, STG_DATATYPE, STG_LENGTH" ~
        " FROM " ~ STTM_TBL ~
        " WHERE UPPER(STG_ENTITY) = UPPER('" ~ stg_entity ~ "')" ~
        " AND ACTIVE_FLAG = TRUE" ~
        " ORDER BY STG_ATRRIBUTE"
    ) %}

    {% if col_res is none or (col_res.rows | length) == 0 %}
        {{ exceptions.raise_compiler_error(
            "No columns found in STTM for STG_ENTITY='" ~ stg_entity ~ "'. " ~
            "Check STTM_UPDATED data and ACTIVE_FLAG."
        ) }}
    {% endif %}

    {# Build column definitions from STTM #}
    {% set col_defs = [] %}
    {% for row in col_res.rows %}
        {% set col_name = row[0] %}
        {% set col_type = row[1] if row[1] is not none else 'VARCHAR' %}
        {% set col_len  = row[2] %}

        {% if col_len is not none and col_len > 0
              and col_type | upper in ('VARCHAR', 'CHAR', 'NUMBER', 'DECIMAL', 'NUMERIC') %}
            {% do col_defs.append(col_name ~ ' ' ~ col_type ~ '(' ~ col_len ~ ')') %}
        {% else %}
            {% do col_defs.append(col_name ~ ' ' ~ col_type) %}
        {% endif %}
    {% endfor %}

    {# Add standard audit columns — always appended by macro #}
    {% do col_defs.append(var('col_created_by')   ~ ' VARCHAR(10)') %}
    {% do col_defs.append(var('col_created_date') ~ ' DATE') %}
    {% do col_defs.append(var('col_modified_by')  ~ ' VARCHAR(10)') %}
    {% do col_defs.append(var('col_modified_date') ~ ' DATE') %}
    {% do col_defs.append(var('col_active_flag')  ~ ' VARCHAR(2)') %}
    {% do col_defs.append(var('incremental_col')  ~ ' DATE') %}

    {{ return(
        "CREATE TABLE IF NOT EXISTS " ~ table_fqtn ~
        " (" ~ col_defs | join(', ') ~ ")"
    ) }}
{% endmacro %}


{% macro _ensure_table_exists(table_fqtn, stg_entity) %}
    {% if not execute %}{{ return('') }}{% endif %}

    {% set parts  = table_fqtn.split('.') %}
    {% set db     = parts[0] %}
    {% set schema = parts[1] %}
    {% set tbl    = parts[2] %}

    {% set exists = run_query(
        "SELECT COUNT(*) FROM " ~ db ~ ".INFORMATION_SCHEMA.TABLES" ~
        " WHERE TABLE_SCHEMA = '" ~ schema ~ "'" ~
        " AND TABLE_NAME = '" ~ tbl ~ "'"
    ).columns[0].values()[0] %}

    {% if exists == 0 %}
        {% set create_sql = _build_create_table_sql(table_fqtn, stg_entity) %}
        {% do run_query(create_sql) %}
        {{ log('[CREATE_TABLE] Created: ' ~ table_fqtn, info=True) }}
    {% else %}
        {{ log('[CREATE_TABLE] Already exists: ' ~ table_fqtn, info=True) }}
    {% endif %}
{% endmacro %}


{% macro ensure_pipeline_tables(stg_entity) %}
    {% if not execute %}{{ return('') }}{% endif %}

    {% set DB_SCHEMA = var('target_database') ~ '.' ~ var('trans_schema') %}

    {% set target_fqtn = DB_SCHEMA ~ '.' ~ stg_entity %}
    {% set temp_fqtn   = DB_SCHEMA ~ '.' ~ stg_entity ~ var('temp_suffix') %}
    {% set error_fqtn  = DB_SCHEMA ~ '.' ~ stg_entity ~ var('error_suffix') %}

    {# Create TARGET table #}
    {{ _ensure_table_exists(target_fqtn, stg_entity) }}

    {# Create _TEMP table — same structure as TARGET #}
    {{ _ensure_table_exists(temp_fqtn, stg_entity) }}

    {# Create _ERROR table — same structure as TARGET #}
    {{ _ensure_table_exists(error_fqtn, stg_entity) }}
{% endmacro %}


{% macro ensure_pipeline_tables_batch(pipeline_tables) %}
    {% if not execute %}{{ return('') }}{% endif %}

    {% for t in pipeline_tables %}
        {{ ensure_pipeline_tables(stg_entity=t.stg_entity) }}
    {% endfor %}
{% endmacro %}
