{{config(materialized = 'view', schema = 'reporting_dev')}}
 
select 
e.officecity,c.companyname ,c.contactname, count(o.orderID)   as total_oders, 
Sum(o.quantity) as total_quantity, 
sum(o.linesalesamount) as total_sales,
Avg(o.margin) as avg_margin
from
{{ref("f_orders")}} as o
inner join
{{ref('dim_customers')}} as c on o.customerid = C.customerid
inner join
{{ref('dim_employees')}} as e on e.employee_id = o.employeeid

where e.officecity = '{{var('v_city','Paris')}}'
group by e.officecity,c.companyname ,c.contactname
 