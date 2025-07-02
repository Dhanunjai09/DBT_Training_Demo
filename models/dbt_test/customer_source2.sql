{{ 
  config( 
    materialized='incremental', 
    unique_key='customer_id',
    schema='silver',
    merge_exclude_columns=['customer_id']
  ) 
}}

{% set hash1 = TABLE_HASH_V('customers', exclude_columns=['created_at', 'updated_at']) %}
{% set cleaned_name = hash1 | replace('as hash_val', '') %}
{{ log("Updated hash expression: " ~ cleaned_name, info=True) }}


SELECT * FROM {{ ref('customers') }}
{% if is_incremental() %}
WHERE NOT EXISTS (SELECT hash_val FROM {{ this }} WHERE customer_id = customers.customer_id and hash_val={{ cleaned_name }})
{% endif %}


