{{config(materialized = 'table', schema = env_var('DBT_TRANSFORMSCHEMA', 'TRANSFORMING_DEV'))}}
 
select 
p.productid,
p.productname,
c.categoryname,
s.CompanyName as suppliercompany,
s.ContactName as suppliercontact,
s.city as suppliercity,
s.country as suppliercountry,
p.quantityperunit,
p.unitcost,
p.unitprice,
p.unitsinstock,
p.unitsonorder,
to_decimal(p.unitprice - p.unitcost,9,2) as profit,
IFF(p.unitsonorder > p.unitsinstock,'Not Available','Available') as productavailability

from 
{{ref('stg_products')}} as p 
left join 
{{ref('trf_suppliers')}} as s 
on p.SupplierID = s.SupplierID
left join
{{ref('lkp_categories')}} c 
on c.categoryid = p.categoryid