with date_table as (
select * from unnest(generate_date_array('1990-01-01','2050-01-01',interval 1 day)) as date_day)

select format_date('%F',date_day) as id , date_day as full_date ,
extract(year from date_day) as year ,
extract(month from date_day) as month ,
extract(week from date_day ) as week ,
extract(day from date_day) as day ,
extract(year from date_day ) as fiscal_year,
format_date('%Q' ,date_day) as fiscal_quarter ,
format_date('%B', date_day) as Month_Name  ,
format_date('%w', date_day)    as WeekDay ,
format_date('%A', date_day)  as DayName ,
CASE 
WHEN format_date('%A',date_day)  in ('Saturday','Sunday') THEN 1 ELSE  0  
END AS Is_week_day 
from date_table 
order by id 