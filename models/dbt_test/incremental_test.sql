{{
  config(
    materialized='incremental',
    unique_key='customer_id',  
    schema='silver',
    merge_exclude_columns=['customer_id', 'hash_val']  
  )
}}

{{ incremental_with_row_hash(
    source_model='customers_seed',              
    business_key='customer_id',                 
    exclude_columns=['created_at', 'updated_at'], 
    log_changes=True                            
) }}