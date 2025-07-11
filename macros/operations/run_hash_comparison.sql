-- macros/operations/run_hash_comparison.sql

{% macro run_hash_comparison() %}
  {{ compare_table_hashes_with_logging([
      ('cust1', 'customer_source')
  ], exclude_columns=['last_updated']) }}
{% endmacro %}
