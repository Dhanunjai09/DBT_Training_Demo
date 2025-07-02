{% macro compare_table_hashes_with_logging_2(pairs, log_table=None, exclude_columns=[]) %}
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
    select md5(
      cast(sum(abs(hash({{ columns | join(', ') }}))) as string)
    ) as hash from {{ ref(model_1) }}
  {% endset %}

  {% set hash_2_query %}
    select
    hash_id,
    table_name
from {{ ref(model_2) }}
where updated_at >= current_date - interval '1 day'
  {% endset %}

  {% set hash_1 = run_query(hash_1_query).columns[0].values()[0] %}
  {% set hash_2 = run_query(hash_2_query).columns[0].values()[0] %}

  {% if hash_1 != hash_2 %}
    {% set message = "❌ Logged mismatch: " ~ model_1 ~ " ≠ " ~ model_2 %}
    {% do mismatches.append({
      'model_1': model_1,
      'model_2': model_2,
      'hash_1': hash_1,
      'hash_2': hash_2,
      'log_message': message
    }) %}
  {% else %}
    {% set message = "✅ Hash match: " ~ model_1 ~ " = " ~ model_2 %}
    {% do matches.append({
      'model_1': model_1,
      'model_2': model_2,
      'hash_1': hash_1,
      'hash_2': hash_2,
      'log_message': message
    }) %}
  {% endif %}
{% endfor %}

{% for mismatch in mismatches %}
  {% set insert_sql %}
    insert into {{ log_table }}
    (model_1, model_2, hash_1, hash_2, compared_at, run_started_at, invocation_id, log_message)
    values (
      '{{ mismatch.model_1 }}',
      '{{ mismatch.model_2 }}',
      '{{ mismatch.hash_1 }}',
      '{{ mismatch.hash_2 }}',
      current_timestamp,
      '{{ run_started_at }}',
      '{{ invocation_id }}',
      '{{ mismatch.log_message }}'
    )
  {% endset %}
  {% do run_query(insert_sql) %}
  {{ log(mismatch.log_message, info=True) }}
{% endfor %}

{% for match in matches %}
  {% set insert_sql %}
    insert into {{ log_table }}
    (model_1, model_2, hash_1, hash_2, compared_at, run_started_at, invocation_id, log_message)
    values (
      '{{ match.model_1 }}',
      '{{ match.model_2 }}',
      '{{ match.hash_1 }}',
      '{{ match.hash_2 }}',
      current_timestamp,
      '{{ run_started_at }}',
      '{{ invocation_id }}',
      '{{ match.log_message }}'
    )
  {% endset %}
  {% do run_query(insert_sql) %}
  {{ log(match.log_message, info=True) }}
{% endfor %}
{% endmacro %}