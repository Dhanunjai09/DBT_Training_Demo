{{ 
  config( 
    materialized='incremental', 
    unique_key='customer_id'  
  ) 
}}

SELECT *, {{ TABLE_HASH_V('customers_seed', exclude_columns=['created_at', 'updated_at']) }} 
FROM {{ ref('customers_seed') }}