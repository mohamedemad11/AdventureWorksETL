with source as (


    select *
    from {{ref ('stg_order')}}
)


select *
from source 