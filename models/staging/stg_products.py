import snowflake.snowpark.functions as F

def model(dbt,session):
    
    dbt.config(materialized = 'table')
    
    products_df = dbt.source('qwt_raw','RAW_PRODUCTS') 

    return products_df