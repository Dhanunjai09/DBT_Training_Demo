{# =============================================================================
   FILE: macros/load_raw_to_temp.sql
   PURPOSE: Stage 1 — Load data from EXTRACT_SAP into _TEMP tables.
            Same logic for ALL tables:
              First run  → full load (target empty → cutoff=1900-01-01 → all rows)
              Next runs  → incremental only (cutoff=MAX(INSERTEDAT) from target)
            _TEMP auto-created from source structure — no DDL needed.
   MACROS:
     - load_raw_to_temp(stg_entity, src_entity)  ← single table
     - load_raw_to_temp_multi(pipeline_tables)   ← multi table wrapper
   ============================================================================= #}

{% macro _pipeline_user_sql() %}
    {% set u = var('pipeline_user') %}
    {{ return("'" ~ (u | replace("'", "''")) ~ "'") }}
{% endmacro %}


{% macro load_raw_to_temp(stg_entity, src_entity) %}
    {% if not execute %}{{ return('') }}{% endif %}

    {% set SRC_DB        = var('audit_db') %}
    {% set SRC_SCHEMA    = var('src_schema') %}
    {% set TGT_DB        = var('audit_db') %}
    {% set TGT_SCHEMA    = var('trans_schema') %}
    {% set SRC_TABLE     = SRC_DB ~ '.' ~ SRC_SCHEMA ~ '.' ~ src_entity %}
    {% set TGT_TABLE     = TGT_DB ~ '.' ~ TGT_SCHEMA ~ '.' ~ stg_entity ~ var('temp_suffix') %}
    {% set FINAL_TGT     = TGT_DB ~ '.' ~ TGT_SCHEMA ~ '.' ~ stg_entity %}
    {% set ROW_COUNT_TBL = TGT_DB ~ '.' ~ TGT_SCHEMA ~ '.' ~ var('row_count_table') %}
    {% set audit_user_sql = _pipeline_user_sql() %}

    {% set audit_id = audit_log_insert(
        job_name   = 'LOAD_RAW_TO_TEMP_' ~ stg_entity,
        job_id     = invocation_id,
        job_status = 'RUNNING',
        comments   = 'Loading ' ~ SRC_TABLE ~ ' → ' ~ TGT_TABLE
    ) %}

    {# STEP 1: Auto-create _TEMP from source structure — no DDL needed #}
    {% do run_query(
        "CREATE TABLE IF NOT EXISTS " ~ TGT_TABLE ~
        " AS SELECT * FROM " ~ SRC_TABLE ~ " WHERE 1=0"
    ) %}
    {{ log('[' ~ stg_entity ~ '] _TEMP ready: ' ~ TGT_TABLE, info=True) }}

    {# STEP 2: Watermark — same logic for all tables
       First run : target does not exist yet → cutoff = 1900-01-01 → loads all rows
       Next runs : reads MAX(INSERTEDAT) from final TARGET → loads only new rows
       NOTE: watermark from TARGET not _TEMP (_TEMP truncated after every run) #}
    {% set tgt_exists = run_query(
        "SELECT COUNT(*) FROM " ~ TGT_DB ~ ".INFORMATION_SCHEMA.TABLES" ~
        " WHERE TABLE_SCHEMA='" ~ TGT_SCHEMA ~ "' AND TABLE_NAME='" ~ stg_entity ~ "'"
    ).columns[0].values()[0] %}

    {% if tgt_exists == 0 %}
        {% set cutoff = "TO_TIMESTAMP('" ~ var('incremental_default_date') ~ "')" %}
    {% else %}
        {% set max_val = run_query(
            "SELECT MAX(" ~ var('incremental_col') ~ ") FROM " ~ FINAL_TGT
        ).columns[0].values()[0] %}
        {% set cutoff = "TO_TIMESTAMP('" ~ var('incremental_default_date') ~ "')"
            if max_val is none
            else "TO_TIMESTAMP('" ~ max_val ~ "')" %}
    {% endif %}

    {{ log('[' ~ stg_entity ~ '] Cutoff ' ~ var('incremental_col') ~ ' = ' ~ cutoff, info=True) }}

    {# STEP 3: Count new rows in source #}
    {% set src_cnt = run_query(
        "SELECT COUNT(*) FROM " ~ SRC_TABLE ~
        " WHERE " ~ var('incremental_col') ~ " > " ~ cutoff
    ).columns[0].values()[0] %}

    {% if src_cnt == 0 %}

        {% do run_query("BEGIN") %}
        {% do run_query("DELETE FROM " ~ ROW_COUNT_TBL ~ " WHERE TABLE_NAME='" ~ stg_entity ~ "'") %}
        {% do run_query("INSERT INTO " ~ ROW_COUNT_TBL ~ " (TABLE_NAME,SRC_TABLE_COUNT,TGT_TABLE_COUNT,STATUS) VALUES ('" ~ stg_entity ~ "',0,0,'SUCCESS')") %}
        {% do run_query("COMMIT") %}
        {% do audit_log_update(audit_id=audit_id, status='SUCCESS',
            comments='No new rows. Cutoff=' ~ cutoff) %}
        {{ log('[' ~ stg_entity ~ '] {"status":"SUCCESS","src_cnt":0,"inserted":0,"audit_id":' ~ audit_id ~ '}', info=True) }}

    {% else %}

        {# STEP 4: Get _TEMP columns via INFORMATION_SCHEMA — no adapter cache #}
        {% set tgt_cols = run_query(
            "SELECT COLUMN_NAME FROM " ~ TGT_DB ~ ".INFORMATION_SCHEMA.COLUMNS" ~
            " WHERE TABLE_SCHEMA='" ~ TGT_SCHEMA ~ "' AND TABLE_NAME='" ~ stg_entity ~ var('temp_suffix') ~ "'" ~
            " ORDER BY ORDINAL_POSITION"
        ).columns[0].values() | list %}

        {% set src_cols = run_query(
            "SELECT COLUMN_NAME FROM " ~ SRC_DB ~ ".INFORMATION_SCHEMA.COLUMNS" ~
            " WHERE TABLE_SCHEMA='" ~ SRC_SCHEMA ~ "' AND TABLE_NAME='" ~ src_entity ~ "'"
        ).columns[0].values() | map('upper') | list %}

        {% set select_exprs = [] %}
        {% for col in tgt_cols %}
            {% set col_u = col | upper %}
            {% if   col_u == var('col_created_by')    %}{% do select_exprs.append(audit_user_sql ~ " AS " ~ col) %}
            {% elif col_u == var('col_modified_by')   %}{% do select_exprs.append(audit_user_sql ~ " AS " ~ col) %}
            {% elif col_u == var('col_created_date')  %}{% do select_exprs.append("CURRENT_DATE() AS " ~ col) %}
            {% elif col_u == var('col_modified_date') %}{% do select_exprs.append("CURRENT_DATE() AS " ~ col) %}
            {% elif col_u in src_cols                 %}{% do select_exprs.append(col) %}
            {% else                                   %}{% do select_exprs.append("NULL AS " ~ col) %}
            {% endif %}
        {% endfor %}

        {% do run_query(
            "INSERT INTO " ~ TGT_TABLE ~ " (" ~ tgt_cols | join(', ') ~ ")" ~
            " SELECT " ~ select_exprs | join(', ') ~
            " FROM " ~ SRC_TABLE ~
            " WHERE " ~ var('incremental_col') ~ " > " ~ cutoff
        ) %}

        {% do run_query("BEGIN") %}
        {% do run_query("DELETE FROM " ~ ROW_COUNT_TBL ~ " WHERE TABLE_NAME='" ~ stg_entity ~ "'") %}
        {% do run_query("INSERT INTO " ~ ROW_COUNT_TBL ~ " (TABLE_NAME,SRC_TABLE_COUNT,TGT_TABLE_COUNT,STATUS) VALUES ('" ~ stg_entity ~ "'," ~ src_cnt ~ "," ~ src_cnt ~ ",'SUCCESS')") %}
        {% do run_query("COMMIT") %}

        {% do audit_log_update(audit_id=audit_id, status='SUCCESS',
            comments='Inserted ' ~ src_cnt ~ ' rows. Cutoff=' ~ cutoff) %}
        {{ log('[' ~ stg_entity ~ '] {"status":"SUCCESS","src_cnt":' ~ src_cnt ~ ',"inserted":' ~ src_cnt ~ ',"audit_id":' ~ audit_id ~ '}', info=True) }}

    {% endif %}
{% endmacro %}


{% macro load_raw_to_temp_multi(pipeline_tables) %}
    {% if not execute %}{{ return('') }}{% endif %}

    {% set audit_id = audit_log_insert(
        job_name='BATCH_LOAD_RAW_TO_TEMP', job_id=invocation_id,
        job_status='RUNNING',
        comments='Tables=' ~ (pipeline_tables | map(attribute='stg_entity') | join(','))
    ) %}

    {% for t in pipeline_tables %}
        {{ load_raw_to_temp(
            stg_entity = t.stg_entity,
            src_entity = t.src_entity
        ) }}
    {% endfor %}

    {% do audit_log_update(audit_id=audit_id, status='SUCCESS',
        comments='Batch complete. Tables=' ~ (pipeline_tables | map(attribute='stg_entity') | join(','))) %}
{% endmacro %}
