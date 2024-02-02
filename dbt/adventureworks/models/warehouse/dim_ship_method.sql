with source as (



    select *
    from {{  ref ('stg_ship_method')}}
)

select * from source 

