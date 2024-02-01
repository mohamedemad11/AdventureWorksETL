with source as (

    select shipmethodid , name , shipbase , shiprate
    from {{source('adventureworks','ShipMethod')}}


)

select * , current_timestamp() as ingestion_timestamp
from source 