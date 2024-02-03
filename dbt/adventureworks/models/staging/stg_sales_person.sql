with source as (

    select *
    from {{source('adventureworks','SalesPerson')}}


),

source_2 as (


    select *
    from {{source('adventureworks','Person')}}
),
source_3 as (


    select *
    from {{source('adventureworks','Store')}}

),

source_4 as  (


    select SP.BusinessEntityID as SalesPersonID , SP.SalesQuota  , SP.Bonus , SP.CommissionPct , SP.SalesYTD , SP.SalesLastYear , 
P.Title , P.FirstName , P.MiddleName , P.LastName ,S.name as store_name 
    from source SP
    join source_2 P
    on SP.BusinessEntityID = P.BusinessEntityID
    join source_3 S 
    on S.salespersonid = SP.BusinessEntityID
)

SELECT * , current_timestamp() as ingestion_timestamp
FROM source_4