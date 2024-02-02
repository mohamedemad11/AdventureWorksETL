with source as (



    select *
    from {{ref ('stg_sales_person')}}
)

select * from source 