{% macro TABLE_HASH_V(pair, log_table=None, exclude_columns=[]) %}
{% if log_table is none %}
--  {% set log_table = target.database ~ '.' ~ target.schema ~ '.hash_mismatch_log' %}
{% endif %}

 {% set model_1 = pair %}
 
{% if execute %}
 {% set columns_query %}
    select LISTAGG(column_name,'||')
    WITHIN GROUP (ORDER BY ordinal_position) AS column_list
    from information_schema.columns
    where table_name ilike '{{ model_1 }}'
    order by ordinal_position
  {% endset %}

  {% set results = run_query(columns_query) %}
  {% set all_columns = results.columns[0].values() %}
  {% set columns = all_columns | reject("in", exclude_columns) | list %}
  {% set col1 = columns|join('||')%}
  {{ log("Joined string: " ~ col1, info=True) }}


  {% set hash_1_query %}
    md5(
      {{ col1 }})
     as hash_val
  {% endset %}

  {{ return(hash_1_query) }} 
 {% endif %} 
{% endmacro %}
