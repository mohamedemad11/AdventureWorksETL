with source as (


    select {{ dbt_utils.generate_surrogate_key(['stg_store.StoreID']) }} as store_key 
    , * 
    from {{ref ('stg_store')}}
)

select *
from source 

