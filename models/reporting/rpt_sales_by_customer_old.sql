{{config(materialized = 'view', schema = 'reporting_dev')}}

select 
c.companyname as customername, 
c.contactname,
min(f.orderdate) as first_order_date,
max(f.orderdate) as recent_order_Date,
count(distinct f.orderid) as total_orders,
count(distinct p.productid) as total_products,
sum(f.quantity) as total_quanity,
sum(f.linesalesamount) as total_sales,
avg(f.margin) as avg_margin

from
{{ref("f_orders")}} as f 
inner join 
{{ref("dim_customers")}} as c on f.customerid = c.customerid
inner join 
{{ref("dim_products")}} as p on f.productid = p.productid
group by customername, contactname order by total_sales desc

