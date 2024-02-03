with source as (
    
    select *
    from {{source('adventureworks','SalesTerritory')}}

) ,

source_2 as (

    select *
    from {{source('adventureworks','SalesTerritoryHistory')}}

),

source_3 as (

    select ST.TerritoryID , ST.Name , ST.CountryRegionCode ,  ST.SalesYTD , ST.CostYTD , ST.CostLastYear , STH.StartDate , STH.EndDate
    , row_number() over(partition by ST.TerritoryID)  as row_n , current_timestamp() as ingestion_timestamp 
    from source ST 
    left join source_2 STH
    on ST.TerritoryID = STH.TerritoryID


)


select * 
except (row_n) ,
from source_3
where row_n = 1 