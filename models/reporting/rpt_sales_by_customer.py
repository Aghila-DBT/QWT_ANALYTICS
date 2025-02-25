import snowflake.snowpark.functions as F
import pandas as pd
import holidays

def is_holiday(holiday_date):
    french_holidays = holidays.France()
    is_holiday = (holiday_date in french_holidays)
    return is_holiday

def avgordervalue(orders, value):
    return value/orders
 
def model(dbt, session):
    
    dbt.config(materialized = 'table',schema = 'reporting_dev', packages = ['holidays'], pre_hook = 'use warehouse python_dbt_wh;')
    orders_df = dbt.ref('f_orders')
    
    Orders_aggregate_df = ( 
                            orders_df
                            .group_by('customerid')
                            .agg(
                                        F.min(F.col('orderdate')).alias('first_order_date'),
                                        F.max(F.col('orderdate')).alias('recent_order_Date'),
                                        F.count(F.col('orderid')).alias('total_orders'),
                                        F.countDistinct(F.col('productid')).alias('total_products'),
                                        F.sum(F.col('quantity')).alias('total_quantity'),
                                        F.sum(F.col('linesalesamount')).alias('total_sales'),
                                        F.avg(F.col('margin')).alias('avg_margin'),
                                )
                        )
    customers_df = dbt.ref('dim_customers')
    customer_order_df = (
                           customers_df
                            .join(Orders_aggregate_df, Orders_aggregate_df.customerid == customers_df.customerid,'left') 
                            .select
                            ( 
                                customers_df.companyname, 
                                customers_df.contactname,
                                Orders_aggregate_df.first_order_date,
                                Orders_aggregate_df.recent_order_Date,
                                Orders_aggregate_df.total_orders,
                                Orders_aggregate_df.total_products,
                                Orders_aggregate_df.total_quantity,
                                Orders_aggregate_df.total_sales,
                                Orders_aggregate_df.avg_margin,
                            )
        )
    
    customer_order_final_df = customer_order_df.withColumn('AverageorderAmount',avgordervalue(customer_order_df["total_quantity"],customer_order_df["total_sales"]))

    final_df = customer_order_final_df.filter(F.col('FIRST_ORDER_DATE').isNotNull())

    final_order_df = final_df.to_pandas()

    final_order_df["is_first_order_on_holiday"] = final_order_df["FIRST_ORDER_DATE"].apply(is_holiday)
    
    return final_order_df