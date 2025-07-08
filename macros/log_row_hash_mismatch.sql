{% macro log_row_hash_mismatch(source_row, target_hash, log_table, business_key) %}
  insert into {{ log_table }} (table_name, key_column, key_value, source_hash, target_hash, log_ts)
  values (
    '{{ source_row["__table_name"] }}',
    '{{ business_key }}',
    '{{ source_row[business_key] }}',
    '{{ source_row["hash_val"] }}',
    '{{ target_hash }}',
    current_timestamp()
  )
{% endmacro %}