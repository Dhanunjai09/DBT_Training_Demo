
WITH customers AS (
    SELECT 
        Customer_Id,
        First_Name,
        Last_Name,
        Country,
        Phone_1
    FROM {{ ref('customers') }}
),

policies AS (
    SELECT 
        Policy_Id,
        Customer_Id,
        Policy_Type,
        Premium_Amount,
        Start_Date
    FROM {{ ref('policies') }}
),

claims AS (
    SELECT 
        Claim_Id,
        Policy_Id,
        Claim_Amount,
        Claim_Date,
        Status
    FROM {{ ref('claims') }}
)

SELECT
    cl.Claim_Id,
    cl.Claim_Amount,
    cl.Claim_Date,
    cl.Status,

    po.Policy_Id,
    po.Policy_Type,
    po.Premium_Amount,
    po.Start_Date,

    cu.Customer_Id,
    cu.First_Name,
    cu.Last_Name,
    cu.Country,
    cu.Phone_1

FROM claims cl
JOIN policies po ON cl.Policy_Id = po.Policy_Id
JOIN customers cu ON po.Customer_Id = cu.Customer_Id