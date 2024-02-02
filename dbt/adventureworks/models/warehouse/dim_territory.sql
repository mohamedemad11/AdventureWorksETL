with source as (



    select *
    from {{ref ('stg_territory')}}
)


select *
from source 