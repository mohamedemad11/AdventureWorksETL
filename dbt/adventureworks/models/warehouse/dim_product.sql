with source as (




    select *
    from {{ref('stg_product')}}
)

select *
from source 