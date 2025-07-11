{% macro compare_table_hashes_with_logging_3(pairs, log_table=None, exclude_columns=[], aggregation_columns={}) %}
{% if log_table is none %}
  {% set log_table = target.database ~ '.' ~ target.schema ~ '.hash_mismatch_log' %}
{% endif %}

{% set mismatches = [] %}
{% set matches = [] %}

{% for pair in pairs %}
  {% set model_1 = pair[0] %}
  {% set model_2 = pair[1] %}

  {% set columns_query %}
    select column_name
    from information_schema.columns
    where table_name ilike '{{ model_1 }}'
    order by ordinal_position
  {% endset %}

  {% set results = run_query(columns_query) %}
  {% set all_columns = results.columns[0].values() %}
  {% set columns = all_columns | reject("in", exclude_columns) | list %}

  {% set hash_1_query %}
    select md5(cast(sum(abs(hash({{ columns | join(', ') }}))) as string)) as hash
    from {{ ref(model_1) }}
  {% endset %}

  {% set hash_2_query %}
    select md5(cast(sum(abs(hash({{ columns | join(', ') }}))) as string)) as hash
    from {{ ref(model_2) }}
  {% endset %}

  {% set hash_1 = run_query(hash_1_query).columns[0].values()[0] %}
  {% set hash_2 = run_query(hash_2_query).columns[0].values()[0] %}

  {# Aggregate values if specified #}
  {% set agg_col_1 = aggregation_columns.get(model_1) %}
  {% set agg_col_2 = aggregation_columns.get(model_2) %}

  {% if agg_col_1 %}
    {% set agg_query_1 = "select sum(" ~ agg_col_1 ~ ") as agg from " ~ ref(model_1) %}
    {% set agg_1 = run_query(agg_query_1).columns[0].values()[0] %}
  {% else %}
    {% set agg_1 = none %}
  {% endif %}

  {% if agg_col_2 %}
    {% set agg_query_2 = "select sum(" ~ agg_col_2 ~ ") as agg from " ~ ref(model_2) %}
    {% set agg_2 = run_query(agg_query_2).columns[0].values()[0] %}
  {% else %}
    {% set agg_2 = none %}
  {% endif %}

  {% if hash_1 != hash_2 %}
    {% set message = "❌ Logged mismatch: " ~ model_1 ~ " ≠ " ~ model_2 %}
    {% do mismatches.append({
      'model_1': model_1,
      'model_2': model_2,
      'hash_1': hash_1,
      'hash_2': hash_2,
      'agg_1': agg_1,
      'agg_2': agg_2,
      'log_message': message
    }) %}
  {% else %}
    {% set message = "✅ Hash match: " ~ model_1 ~ " = " ~ model_2 %}
    {% do matches.append({
      'model_1': model_1,
      'model_2': model_2,
      'hash_1': hash_1,
      'hash_2': hash_2,
      'agg_1': agg_1,
      'agg_2': agg_2,
      'log_message': message
    }) %}
  {% endif %}
{% endfor %}

{% for record in mismatches + matches %}
  {% set insert_sql %}
    insert into {{ log_table }}
    (model_1, model_2, hash_1, hash_2, compared_at, run_started_at, invocation_id, log_message{% if record.agg_1 is not none %}, agg_1{% endif %}{% if record.agg_2 is not none %}, agg_2{% endif %})
    values (
      '{{ record.model_1 }}',
      '{{ record.model_2 }}',
      '{{ record.hash_1 }}',
      '{{ record.hash_2 }}',
      current_timestamp,
      '{{ run_started_at }}',
      '{{ invocation_id }}',
      '{{ record.log_message }}'
      {% if record.agg_1 is not none %}, {{ record.agg_1 }}{% endif %}
      {% if record.agg_2 is not none %}, {{ record.agg_2 }}{% endif %}
    )
  {% endset %}
  {% do run_query(insert_sql) %}
  {{ log(record.log_message, info=True) }}
{% endfor %}
{% endmacro %}
