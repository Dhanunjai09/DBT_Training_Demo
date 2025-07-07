{{ 
  config( 
    materialized='incremental', 
    unique_key='customer_id'  
  ) 
}}

SELECT *, {{ incremental_hash('customers_seed', exclude_columns=['created_at', 'updated_at']) }} 
FROM {{ ref('customers_seed') }}