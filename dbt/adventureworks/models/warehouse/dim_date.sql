with source as (


   

    select {{ dbt_utils.generate_surrogate_key(['stg_date.full_date']) }} as date_key 
    , *
    from {{ref('stg_date')}}
)

select * from source 