-- macros/operations/run_hash_comparison.sql

{% macro run_hash_comparison_2() %}
  {{ compare_table_hashes_with_logging_2([
      ('customers', 'customer_source2')
  ], exclude_columns=['last_updated']) }}
{% endmacro %}
