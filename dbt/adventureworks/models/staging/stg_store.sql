with source as (



    select BusinessEntityID as StoreID , Name 
    from {{source('adventureworks','Store')}}
)


select *
from source 