-- macros/log_run_results.sql

{% macro log_run_results() %}
    {% if execute %}
        {% set run_by = target.user %}
        {% for result in results %}
            {% set model_name = result.node.name %}
            {% set status = result.status %}
            {% set run_time = result.execution_time %}
            {% set error_type = 'RuntimeError' if status != 'success' else '' %}
            {% set error_message = result.message if result.message else '' %}

            {{ log_event(model_name, status, run_time, error_type, error_message, 'model') }}
        {% endfor %}
    {% endif %}
{% endmacro %}
