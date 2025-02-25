{{config(materialized = 'table', schema = env_var('DBT_TRANSFORMSCHEMA', 'TRANSFORMING_DEV'))}}


select 

c.customerid,
c.companyname,
c.contactname,
c.city,
c.country,
d.divisionid,
c.address,
c.fax,
c.phone,
c.postalcode,
IFF(c.stateprovince = '', 'NA', c.stateprovince) as stateprovincename 



from
{{ref("stg_customers")}} as c left join {{ref("lkp_divisions")}} as d
on c.divisionid = d.DivisionID