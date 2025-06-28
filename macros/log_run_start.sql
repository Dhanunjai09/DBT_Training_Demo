-- macros/log_run_start.sql

{% macro log_run_start() %}
    {% set run_id = invocation_id ~ '_run_start' %}
    {% set time_now = run_started_at %}
    {% set run_by = target.user %}

    {% set query %}
        INSERT INTO {{ target.database }}.{{ target.schema }}.event_log (
            id, model_name, status, execution_time, run_time, error_type, error_message, invocation_id, event_type, run_by
        ) VALUES (
            '{{ run_id }}', 'dbt_run', 'started', '{{ time_now }}', 0, NULL, NULL, '{{ invocation_id }}', 'run_start', '{{ run_by }}'
        )
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}
