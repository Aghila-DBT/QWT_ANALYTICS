{{config(materialized = 'table', schema = env_var('DBT_TRANSFORMSCHEMA', 'TRANSFORMING_DEV'))}}
{% set v_min_orderdate = get_order_date_min() %}
{% set v_max_orderdate = get_order_date_max() %}

{{dbt_date.get_date_dimension(v_min_orderdate, v_max_orderdate)}}