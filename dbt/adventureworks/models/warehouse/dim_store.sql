with source as (


    select *
    from {{ref ('stg_store')}}
)

select *
from source 

