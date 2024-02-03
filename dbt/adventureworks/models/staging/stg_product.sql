
with source as (

    select *
    from {{source ('adventureworks','Product')}}
),

source_2 as (
    select *
    from {{source('adventureworks','ProductInventory')}}
),


source_3 as (

    select *
    from {{source('adventureworks','ProductSubcategory')}}


),
source_4 as (

    select *
    from {{source('adventureworks','ProductCategory')}}
),


source_5 as (

    select *
    from {{source('adventureworks','ProductModel')}}
),

source_6 as (

select p.ProductID , p.Name as product_name  , p.Color , 
p.StandardCost , p.ListPrice , p.Size , p.SizeUnitMeasureCode , p.WeightUnitMeasureCode , p.weight ,
p.DaysToManufacture , Quantity , ps.name as product_sub_category 
, pc.name as product_category , pm.name as product_model , row_number () over (partition by p.ProductID) as row_n , current_timestamp() as ingestion_timestamp 

from source p
left join source_2 pn
on p.ProductID = pn.ProductID
left join source_3 ps
on p.ProductSubcategoryID = ps.ProductSubcategoryID 
left join source_4 pc 
on ps.ProductCategoryID  = pc.ProductCategoryID 
left join source_5 pm 
on p.ProductModelID  = pm.ProductModelID

)


select * 
except (row_n) ,
from source_6 
where row_n = 1 










