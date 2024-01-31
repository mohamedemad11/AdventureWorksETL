with source as (

    select *
    from {{source('adventureworks','SalesPerson')}}


),

source_2 as (


    select *
    from {{source('adventureworks','Person')}}
),

source_3 as  (


    select SP.BusinessEntityID as SalesPersonID , SP.SalesQuota  , SP.Bonus , SP.CommissionPct , SP.SalesYTD , SP.SalesLastYear , 
P.Title , P.FirstName , P.MiddleName , P.LastName 
    from source SP
    left join source_2 P
    on SP.BusinessEntityID = P.BusinessEntityID
)

SELECT *
FROM source_3 