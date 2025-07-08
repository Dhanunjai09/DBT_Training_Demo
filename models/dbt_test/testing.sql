select 
  md5(
    cast(
      sum(abs(hash(Customer_Id, First_Name, Last_Name, Country, Phone_1)))::string
      as string
    )
  ) as table_hash
from {{ ref('customers') }}
order by customer_id
