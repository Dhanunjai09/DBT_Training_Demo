{% macro run_hash_comparison_3() %}
  {{ compare_table_hashes_with_logging_3([
      ('claims_raw_staging', 'claims_raw_main'),
      ('policies_raw_staging', 'policies_raw_main')
    ],
    exclude_columns=['last_updated'],
    aggregation_columns={
      'claims_raw_staging': 'claim_amount',
      'claims_raw_main': 'claim_amount',  
      'policies_raw_staging': 'premium_amount',
      'policies_raw_main': 'premium_amount',  
    }
  ) }}
{% endmacro %}
