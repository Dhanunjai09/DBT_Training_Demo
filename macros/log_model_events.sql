-- {% macro log_model_event(event, error_message=None) %}
--   {% set model_name = this.name %}
--   {% set run_by = env_var('DBT_USER', env_var('USER', 'unknown_user')) %}

--   insert into mm_db_test.mm_test_sf.dbt_model_logs (
--     model_name,
--     event,
--     run_at,
--     run_by,
--     error_message
--   ) values (
--     '{{ model_name }}',
--     '{{ event }}',
--     current_timestamp(),
--     '{{ run_by }}',
--     {% if error_message %}'{{ error_message }}'{% else %}null{% endif %}
--   );
-- {% endmacro %}


{% macro log_model_events(event, error_message='') %}
  {% set model_name = model.name if execute else 'unknown' %}
  {% set run_by = target.user if execute else 'unknown_user' %}
  {% set run_at = modules.datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") if execute else 'unknown_time' %}

  {% if execute %}
    {% set sql %}
        INSERT INTO {{ target.database }}.{{ target.schema }}.dbt_model_logs (
            model_name,
            event,
            run_at,
            run_by,
            error_message
        )
        VALUES (
            '{{ model_name }}',
            '{{ event }}',
            '{{ run_at }}',
            '{{ run_by }}',
            '{{ error_message | replace("'", "''") }}'
        )
     {% endset %}

    {% do run_query(sql) %}
  {% endif %} 
{% endmacro %}
