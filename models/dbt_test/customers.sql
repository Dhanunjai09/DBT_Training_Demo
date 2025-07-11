{{ 
  config( 
    materialized='incremental', 
    unique_key='customer_id',
    incremental_strategy='merge'
  ) 
}}



WITH source_data AS (
  SELECT 
    *,
    {{ incremental_hash('customers_seed', exclude_columns=['created_at', 'updated_at']) }} AS hash_val
  FROM {{ ref('customers_seed') }}
),

existing_data AS (
  SELECT 
    customer_id,
    hash_val AS existing_hash
  FROM {{ this }}
)

SELECT 
  s.*,
  CASE 
    WHEN e.customer_id IS NOT NULL AND s.hash_val != e.existing_hash THEN 'N'
    ELSE 'Y'
  END AS active_flag
FROM source_data s
LEFT JOIN existing_data e
  ON s.customer_id = e.customer_id
WHERE 
  e.customer_id IS NULL           -- Insert new records
  OR s.hash_val != e.existing_hash -- Update if hash has changed