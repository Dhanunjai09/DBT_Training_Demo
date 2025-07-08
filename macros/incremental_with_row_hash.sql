{% macro incremental_with_hash(source_model, business_key, exclude_columns=[], log_changes=False) %}
  {% set source = ref(source_model) %}
  {% set target = this %}
  {% set hash_expr = TABLE_HASH_V(source_model, exclude_columns=exclude_columns) | replace('as hash_val', '') %}

  WITH source_data AS (
    SELECT
      *,
      {{ hash_expr }} AS hash_val,
      '{{ source_model }}' AS __table_name
    FROM {{ source }}
  ),
  target_data AS (
    SELECT {{ business_key }}, hash_val AS existing_hash
    FROM {{ target }}
  )

  SELECT s.*
  FROM source_data s
  LEFT JOIN target_data t
    ON s.{{ business_key }} = t.{{ business_key }}
  WHERE s.hash_val != t.existing_hash OR t.{{ business_key }} IS NULL
{% endmacro %}