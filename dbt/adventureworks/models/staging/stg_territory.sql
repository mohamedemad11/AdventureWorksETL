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
    from source ST
    left join source_2 STH
    on ST.TerritoryID = STH.TerritoryID


)


select *
from source_3