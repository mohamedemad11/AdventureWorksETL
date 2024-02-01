with source as (



    select BusinessEntityID as StoreID , Name 
    from {{source('adventureworks','Store')}}
)


select * , current_timestamp() as ingestion_timestamp
from source 