with source as (



    select {{ dbt_utils.generate_surrogate_key(['stg_customer.CustomerID']) }} as customer_key 
    , *
    from {{ref('stg_customer')}}
)

select * from source 