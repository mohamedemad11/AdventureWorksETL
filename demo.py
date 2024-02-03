import time 
import pandas as pd 
import pypyodbc  as odbc 
from google.cloud import bigquery
from sqlserver import SQLServer


SERVER_NAME = r'LAPTOP-UAHKN802'
DATABASE_NAME= 'AdventureWorks2014'
sql_server_instance=SQLServer(SERVER_NAME,DATABASE_NAME)
sql_server_instance.connect_to_sql_server()


table_schema_name_query= """


select concat(TABLE_SCHEMA,'.', TABLE_NAME)
from INFORMATION_SCHEMA.TABLES
where table_name in 
('Product' ,'SalesPerson','SalesTerritory','Person','SalesPerson','Customer','ProductInventory',
'Store','SalesOrderDetail','ShipMethod','ProductSubcategory','ProductCategory','ProductModel','SalesTerritoryHistory')
"""
columns_n ,table_names = sql_server_instance.query(table_schema_name_query)
df2 = pd.DataFrame(table_names , columns = columns_n)


table_names = [row[0] for index , row in df2.iterrows()]
print(table_names)

for table in table_names :
    sql_statement = f'SELECT * FROM {table}'
    columns,records = sql_server_instance.query(sql_statement)
    df= pd.DataFrame(records, columns = columns)
    print(df.shape)

    client= bigquery.Client()
    job_config= bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition='WRITE_TRUNCATE'
        
    )
    table_splitted=table.split('.')[1]
    target_table_id= f'adventureworks-412710.dl_adventureworks.{table_splitted}'

    job = client.load_table_from_dataframe(df, target_table_id,job_config=job_config)
    while job.state != 'DONE' :
        time.sleep(2)
        job.reload()
    print(job.result())
    table = client.get_table(target_table_id)
    print("Loaded {} rows and {} columns to '{}'".format(
        table.num_rows , len(table.schema),target_table_id)
    )



########################################################################################################################################


table_schema_name_query_for_date_only= """


select concat(TABLE_SCHEMA,'.', TABLE_NAME)
from INFORMATION_SCHEMA.TABLES
where table_name in 
('SalesOrderHeader')
"""
columns_n_date ,table_names_date = sql_server_instance.query(table_schema_name_query_for_date_only)
df3 = pd.DataFrame(table_names_date , columns = columns_n_date)


table_names_date_only = [row[0] for index , row in df3.iterrows()]
print(table_names_date_only)

for table in table_names_date_only :
    sql_statement = f'SELECT * FROM {table}'
    columns,records = sql_server_instance.query(sql_statement)
    df= pd.DataFrame(records, columns = columns)
    print(df.shape)

    client= bigquery.Client()
    job_config= bigquery.LoadJobConfig(
        autodetect=True ,
        write_disposition='WRITE_TRUNCATE',
        schema =[bigquery.SchemaField("orderdate",bigquery.enums.SqlTypeNames.DATETIME),
                 bigquery.SchemaField("duedate",bigquery.enums.SqlTypeNames.DATETIME),
                 bigquery.SchemaField("shipdate",bigquery.enums.SqlTypeNames.DATETIME),
                 bigquery.SchemaField("comment",bigquery.enums.SqlTypeNames.STRING)]
        
    )
    table_splitted=table.split('.')[1]
    target_table_id= f'adventureworks-412710.dl_adventureworks.{table_splitted}'

    job = client.load_table_from_dataframe(df, target_table_id,job_config=job_config)
    while job.state != 'DONE' :
        time.sleep(2)
        job.reload()
    print(job.result())
    table = client.get_table(target_table_id)
    print("Loaded {} rows and {} columns to '{}'".format(
        table.num_rows , len(table.schema),target_table_id)
    )