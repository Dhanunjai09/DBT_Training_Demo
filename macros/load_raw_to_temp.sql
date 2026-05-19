{# =============================================================================
   FILE: macros/load_raw_to_temp.sql
   PURPOSE: Stage 1 — Incremental load from EXTRACT_SAP source into _TEMP tables.

   PRE-CONDITION (CHANGED FROM v2):
     _TEMP tables MUST already exist in Snowflake before running.
     This macro NO LONGER auto-creates tables. Tables are pre-created by the
     DBA/Snowflake admin, because:
       - Source columns may be a subset of target columns
       - CREATE TABLE privileges may not be available in all environments

   WATERMARK LOGIC (same for all tables):
     First run  → TARGET missing or empty  → cutoff = 1900-01-01 → all rows
     Next runs  → cutoff = MAX(INSERTEDAT) from TARGET (not _TEMP) → new rows only
     Watermark always read from final TARGET (not _TEMP).

   COLUMN MAPPING:
     Reads _TEMP columns from INFORMATION_SCHEMA (not adapter cache).
     All column names double-quoted to handle special chars (hyphens, spaces).
     Audit columns (CREATED_BY, MODIFIED_BY, etc.) filled from vars.
     Source columns not in _TEMP are silently skipped.
     _TEMP columns not in source get NULL.

   MACROS:
     - load_raw_to_temp(stg_entity, src_entity, trans_entity)  ← single table
     - load_raw_to_temp_multi(pipeline_tables)                 ← multi table
   ============================================================================= #}

{% macro _pipeline_user_sql() %}
    {% set u = var('pipeline_user') %}
    {{ return("'" ~ (u | replace("'", "''")) ~ "'") }}
{% endmacro %}


{% macro load_raw_to_temp(stg_entity, src_entity, trans_entity) %}
    {% if not execute %}{{ return('') }}{% endif %}

    {% set SRC_DB        = var('target_database') %}
    {% set SRC_SCHEMA    = var('src_schema') %}
    {% set TGT_DB        = var('target_database') %}
    {% set TGT_SCHEMA    = var('trans_schema') %}

    {% set SRC_TABLE     = SRC_DB ~ '.' ~ SRC_SCHEMA ~ '.' ~ src_entity %}
    {% set TGT_TABLE     = TGT_DB ~ '.' ~ TGT_SCHEMA ~ '.' ~ stg_entity ~ var('temp_suffix') %}
    {% set FINAL_TGT     = TGT_DB ~ '.' ~ TGT_SCHEMA ~ '.' ~ stg_entity %}
    {% set ROW_COUNT_TBL = TGT_DB ~ '.' ~ TGT_SCHEMA ~ '.' ~ var('row_count_table') %}
    {% set audit_user_sql = _pipeline_user_sql() %}

    {# Validate _TEMP exists — fail fast with clear error if not pre-created #}
    {% set temp_exists = run_query(
        "SELECT COUNT(*) FROM " ~ TGT_DB ~ ".INFORMATION_SCHEMA.TABLES" ~
        " WHERE TABLE_SCHEMA='" ~ TGT_SCHEMA ~ "'" ~
        " AND TABLE_NAME='" ~ stg_entity ~ var('temp_suffix') ~ "'"
    ).columns[0].values()[0] %}

    {% if temp_exists == 0 %}
        {{ exceptions.raise_compiler_error(
            "[" ~ stg_entity ~ "] _TEMP table does not exist: " ~ TGT_TABLE ~
            ". Pre-create it in Snowflake before running the pipeline."
        ) }}
    {% endif %}

    {% set audit_id = audit_log_insert(
        trans_entity = trans_entity,
        job_name     = 'LOAD_RAW_TO_TEMP_' ~ stg_entity,
        job_status   = 'RUNNING',
        comments     = 'Loading ' ~ SRC_TABLE ~ ' → ' ~ TGT_TABLE
    ) %}

    {# STEP 1: Watermark — same logic for all tables
       First run : TARGET missing or empty → cutoff = 1900-01-01 → all rows
       Next runs : cutoff = MAX(INSERTEDAT) from TARGET → new rows only        #}
    {% set tgt_exists = run_query(
        "SELECT COUNT(*) FROM " ~ TGT_DB ~ ".INFORMATION_SCHEMA.TABLES" ~
        " WHERE TABLE_SCHEMA='" ~ TGT_SCHEMA ~ "'" ~
        " AND TABLE_NAME='" ~ stg_entity ~ "'"
    ).columns[0].values()[0] %}

    {% if tgt_exists == 0 %}
        {% set cutoff = "TO_TIMESTAMP('" ~ var('incremental_default_date') ~ "')" %}
    {% else %}
        {% set max_val = run_query(
            "SELECT MAX(" ~ var('incremental_col') ~ ") FROM " ~ FINAL_TGT
        ).columns[0].values()[0] %}
        {# DATEADD(day,1,...) avoids DATE vs TIMESTAMP_NTZ re-load of same-day rows #}
        {% set cutoff = "TO_TIMESTAMP('" ~ var('incremental_default_date') ~ "')"
            if max_val is none
            else "DATEADD(day, 1, TO_DATE('" ~ max_val ~ "'))" %}
    {% endif %}

    {{ log('[' ~ stg_entity ~ '] Cutoff ' ~ var('incremental_col') ~ ' = ' ~ cutoff, info=True) }}

    {# STEP 2: Count new rows in source #}
    {% set src_cnt = run_query(
        "SELECT COUNT(*) FROM " ~ SRC_TABLE ~
        " WHERE " ~ var('incremental_col') ~ " > " ~ cutoff
    ).columns[0].values()[0] %}

    {% if src_cnt == 0 %}

        {% do run_query("BEGIN") %}
        {% do run_query(
            "DELETE FROM " ~ ROW_COUNT_TBL ~
            " WHERE TABLE_NAME='" ~ stg_entity ~ "'"
        ) %}
        {% do run_query(
            "INSERT INTO " ~ ROW_COUNT_TBL ~
            " (TABLE_NAME, SRC_TABLE_COUNT, TGT_TABLE_COUNT, STATUS)" ~
            " VALUES ('" ~ stg_entity ~ "', 0, 0, 'SUCCESS')"
        ) %}
        {% do run_query("COMMIT") %}

        {% do audit_log_update(
            trans_entity = trans_entity,
            audit_id     = audit_id,
            status       = 'SUCCESS',
            comments     = 'No new rows. Cutoff=' ~ cutoff
        ) %}

        {{ log('[' ~ stg_entity ~ '] {"status":"SUCCESS","src_cnt":0,"inserted":0,"audit_id":' ~ audit_id ~ '}', info=True) }}

    {% else %}

        {# STEP 3: Get _TEMP columns via INFORMATION_SCHEMA (no adapter cache)
           This respects the actual _TEMP DDL created in Snowflake.
           _TEMP may have fewer columns than source — that is intentional. #}
        {% set tgt_cols = run_query(
            "SELECT COLUMN_NAME FROM " ~ TGT_DB ~ ".INFORMATION_SCHEMA.COLUMNS" ~
            " WHERE TABLE_SCHEMA='" ~ TGT_SCHEMA ~ "'" ~
            " AND TABLE_NAME='" ~ stg_entity ~ var('temp_suffix') ~ "'" ~
            " ORDER BY ORDINAL_POSITION"
        ).columns[0].values() | list %}

        {% set src_cols = run_query(
            "SELECT COLUMN_NAME FROM " ~ SRC_DB ~ ".INFORMATION_SCHEMA.COLUMNS" ~
            " WHERE TABLE_SCHEMA='" ~ SRC_SCHEMA ~ "'" ~
            " AND TABLE_NAME='" ~ src_entity ~ "'"
        ).columns[0].values() | map('upper') | list %}

        {# STEP 4: Build SELECT — map source cols, set audit cols.
           All column names double-quoted to handle special chars (hyphens, spaces). #}
        {% set select_exprs = [] %}
        {% for col in tgt_cols %}
            {% set col_u = col | upper %}
            {% set col_q = '"' ~ col ~ '"' %}
            {% if   col_u == var('col_created_by')    | upper %}{% do select_exprs.append(audit_user_sql ~ " AS " ~ col_q) %}
            {% elif col_u == var('col_modified_by')   | upper %}{% do select_exprs.append(audit_user_sql ~ " AS " ~ col_q) %}
            {% elif col_u == var('col_created_date')  | upper %}{% do select_exprs.append("CURRENT_DATE() AS " ~ col_q) %}
            {% elif col_u == var('col_modified_date') | upper %}{% do select_exprs.append("CURRENT_DATE() AS " ~ col_q) %}
            {% elif col_u in src_cols                         %}{% do select_exprs.append(col_q) %}
            {% else                                           %}{% do select_exprs.append("NULL AS " ~ col_q) %}
            {% endif %}
        {% endfor %}

        {# Build quoted INSERT column list #}
        {% set tgt_col_quoted = [] %}
        {% for c in tgt_cols %}
            {% do tgt_col_quoted.append('"' ~ c ~ '"') %}
        {% endfor %}

        {# STEP 5: Insert incremental rows into _TEMP #}
        {% do run_query(
            "INSERT INTO " ~ TGT_TABLE ~
            " (" ~ tgt_col_quoted | join(', ') ~ ")" ~
            " SELECT " ~ select_exprs | join(', ') ~
            " FROM " ~ SRC_TABLE ~
            " WHERE " ~ var('incremental_col') ~ " > " ~ cutoff
        ) %}

        {# STEP 6: Update row count table #}
        {% do run_query("BEGIN") %}
        {% do run_query(
            "DELETE FROM " ~ ROW_COUNT_TBL ~
            " WHERE TABLE_NAME='" ~ stg_entity ~ "'"
        ) %}
        {% do run_query(
            "INSERT INTO " ~ ROW_COUNT_TBL ~
            " (TABLE_NAME, SRC_TABLE_COUNT, TGT_TABLE_COUNT, STATUS)" ~
            " VALUES ('" ~ stg_entity ~ "', " ~ src_cnt ~ ", " ~ src_cnt ~ ", 'SUCCESS')"
        ) %}
        {% do run_query("COMMIT") %}

        {% do audit_log_update(
            trans_entity = trans_entity,
            audit_id     = audit_id,
            status       = 'SUCCESS',
            comments     = 'Inserted ' ~ src_cnt ~ ' rows. Cutoff=' ~ cutoff
        ) %}

        {{ log('[' ~ stg_entity ~ '] {"status":"SUCCESS","src_cnt":' ~ src_cnt ~ ',"inserted":' ~ src_cnt ~ ',"audit_id":' ~ audit_id ~ '}', info=True) }}

    {% endif %}
{% endmacro %}


{% macro load_raw_to_temp_multi(pipeline_tables) %}
    {% if not execute %}{{ return('') }}{% endif %}

    {% set tbl_names = pipeline_tables | map(attribute='stg_entity') | join(',') %}

    {% set audit_id = audit_log_insert(
        trans_entity = pipeline_tables[0].trans_entity,
        job_name     = 'BATCH_LOAD_RAW_TO_TEMP',
        job_status   = 'RUNNING',
        comments     = 'Tables=' ~ tbl_names
    ) %}

    {% for t in pipeline_tables %}
        {{ load_raw_to_temp(
            stg_entity   = t.stg_entity,
            src_entity   = t.src_entity,
            trans_entity = t.trans_entity
        ) }}
    {% endfor %}

    {% do audit_log_update(
        trans_entity = pipeline_tables[0].trans_entity,
        audit_id     = audit_id,
        status       = 'SUCCESS',
        comments     = 'Batch complete. Tables=' ~ tbl_names
    ) %}
{% endmacro %}
