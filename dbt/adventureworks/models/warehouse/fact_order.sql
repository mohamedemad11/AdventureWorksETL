with source as (


    select {{ dbt_utils.generate_surrogate_key(['CustomerID']) }} as customer_key 
    , {{ dbt_utils.generate_surrogate_key(['ProductID']) }} as product_key 
    , {{ dbt_utils.generate_surrogate_key(['SalesPersonID']) }} as salesperson_key 
    , {{ dbt_utils.generate_surrogate_key(['shipmethodid']) }} as shipmethod_key 
    , {{ dbt_utils.generate_surrogate_key(['TerritoryID']) }} as territory_key 
    , {{ dbt_utils.generate_surrogate_key(['OrderDate']) }} as order_date_key 
    , {{ dbt_utils.generate_surrogate_key(['ShipDate']) }} as ship_date_key 
    ,
    *
    from {{ref ('stg_order')}}
)


select *
from source 