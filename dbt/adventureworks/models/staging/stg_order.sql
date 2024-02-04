with source as (


select *
from {{source('adventureworks','SalesOrderHeader')}}


)
,
source_2 as (

select *
from {{source('adventureworks','SalesOrderDetail')}}


)
,
source_3 as (

select H.SalesOrderID , OrderDate , ShipDate  , CustomerID , SalesPersonID , TerritoryID , ShipMethodID , SubTotal , TaxAmt , Freight , TotalDue , SalesOrderDetailID ,
ProductID , OrderQty , UnitPrice ,  row_number () over (partition by SalesOrderDetailID) as row_n , current_timestamp() as ingestion_timestamp 

from source H
left join source_2 D
on H.SalesOrderID = D.SalesOrderID


)


SELECT * 
except (row_n) , 
from source_3
where row_n = 1 