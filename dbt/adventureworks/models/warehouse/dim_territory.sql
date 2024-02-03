with source as (



    select {{ dbt_utils.generate_surrogate_key(['stg_territory.TerritoryID']) }} as territory_key 
    , *
    from {{ref ('stg_territory')}}
)


select *
from source 