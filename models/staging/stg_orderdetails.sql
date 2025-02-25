{{config(materialized = 'incremental', unique_key =['orderid','lineno'])}}

select
od.*,o.orderdate 
from 
{{source('qwt_raw','RAW_ORDERDETAILS')}}
as od
inner join
{{source('qwt_raw','RAW_ORDERS')}} as o 
on od.orderid = o.orderid

{% if is_incremental() %}
where o.orderdate > (select max(orderdate) from {{this}})
{% endif %}

