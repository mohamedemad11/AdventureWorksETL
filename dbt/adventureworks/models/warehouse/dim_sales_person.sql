with source as (



    select {{ dbt_utils.generate_surrogate_key(['stg_sales_person.SalesPersonID']) }} as salesperson_key 
    , *
    from {{ref ('stg_sales_person')}}
)

select * from source 