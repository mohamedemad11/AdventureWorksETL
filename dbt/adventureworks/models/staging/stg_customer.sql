with source as (


select * from {{ source('adventureworks','Customer')}}


)

, source_2 as (


select * from {{ source('adventureworks','Person')}}


)
, 
source_3 as (


    select CustomerID , Title , FirstName , MiddleName , LastName , source.ModifiedDate AS CreationDate 
    from source
    join source_2 
    on source.PersonID = source_2.BusinessEntityID
)

select *
from source_3