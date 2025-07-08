-- macros/log_event.sql

{% macro log_event(model_name, status, run_time=0, error_type='', error_message='', event_type='model') %}
    {% set event_id = invocation_id ~ '_' ~ model_name %}
    {% set execution_time = run_started_at %}
    {% set run_by = target.user %}
    
    {% set query %}
        INSERT INTO {{ target.database }}.{{ target.schema }}.event_log (
            id, model_name, status, execution_time, run_time, error_type, error_message, invocation_id, event_type, run_by
        ) VALUES (
            '{{ event_id }}', '{{ model_name }}', '{{ status }}', 
            '{{ execution_time }}', {{ run_time }},
            {% if error_type %}'{{ error_type }}'{% else %}NULL{% endif %},
            {% if error_message %}'{{ error_message | replace("'", "") }}'{% else %}NULL{% endif %},
            '{{ invocation_id }}', '{{ event_type }}', '{{ run_by }}'
        )
    {% endset %}
    
    {% do run_query(query) %}
{% endmacro %}