with source as (



    select *
    from {{ref('stg_customer')}}
)

select * from source 