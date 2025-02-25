import snowflake.snowpark.functions as F

def model(dbt,session):
    
    dbt.config(materialized = 'table')
    
    customer_df = dbt.source('qwt_raw','RAW_CUSTOMERS') 

    return customer_df