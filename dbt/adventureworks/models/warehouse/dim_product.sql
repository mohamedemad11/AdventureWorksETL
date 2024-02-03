with source as (




    select {{ dbt_utils.generate_surrogate_key(['stg_product.ProductID']) }} as product_key 
    , *
    from {{ref('stg_product')}}
)

select *
from source 