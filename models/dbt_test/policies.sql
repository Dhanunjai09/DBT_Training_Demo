
SELECT * FROM {{ ref('policies_seed') }}
order by Policy_Id
limit 10