{% macro dbt_formatted_query(p_query) %}
--Macro to format SQL query from config tables using environment variables
--with jinja code in a database executable format

--To use regex search
{% set re = modules.re %}
{% set query = p_query %}
{% set final_qry = namespace(qry='') %}

--Split the query to list to get the variable name from environment variable
--Get the query from config table; replace "{{" and "}}" with "}~;|{" as new delimiter; convert string to list

{% set v_delimiter = "}~;|{" %}
{% set qry = (query|replace("{{", v_delimiter)|replace("}}", v_delimiter)).split(v_delimiter) %}
{% for str in qry %}

    --If the split part contains env_var, get the variable name from it
    {% if 'env_var' in str|lower %}

        {% set pattern = "(?<=env_var[(]')(.*)(?='[)]')" %}

        --Replacing any unwanted space and new lines
        {% set str = str|replace(" ","")|replace("\n","") %}
        {% set val = re.search(pattern,str,re.IGNORECASE) %}
        {% if val is none %}
            {{ exceptions.raise_compiler_error ("Issue encountered while parsing the string") }} 
        {% else %}
            --Get the environment variable value
            {% set env_val = env_var (val.group(0)) %}
            --Construct final query by concatenating split parts
            {% set final_qry.qry = final_qry.qry + env_val %}
        {% endif %}
    {% else %}

        --Construct final query by concatenating split parts
        {% set final_qry.qry = final_qry.qry + str %}
    {% endif %}
{% endfor %}

{{ return(final_qry.qry) }}
    
{% endmacro %}