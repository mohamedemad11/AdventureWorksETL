with source as (

    select shipmethodid , name , shipbase , shiprate
    from {{source('adventureworks','ShipMethod')}}


)

select *
from source 