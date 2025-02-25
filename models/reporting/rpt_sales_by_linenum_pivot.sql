{{config(materialized = 'view',schema ='reporting_dev')}}
{% set v_linenumbers = get_line_numbers() %}
select 
orderid, 
{% for linenumber in v_linenumbers -%}
sum(case when LineNo = {{linenumber}} THEN linesalesamount end) as line{{linenumber}}_amount,
{% endfor %}
sum(linesalesamount) as total_sales_amount
from 
{{ref('f_orders')}}
group by orderid