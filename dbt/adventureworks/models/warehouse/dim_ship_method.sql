with source as (



    select {{ dbt_utils.generate_surrogate_key(['stg_ship_method.shipmethodid']) }} as shipmethod_key 
    , *
    from {{  ref ('stg_ship_method')}}
)

select * from source 

