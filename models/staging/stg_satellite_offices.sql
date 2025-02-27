{{config(materialized = 'table')}}

select 
ho.officehashkey,
current_timestamp as loaddate,
ho.recordsource,
ro.officeaddress,
ro.OFFICEPOSTALCODE ,
ro.OFFICECITY,
ro.OFFICESTATEPROVINCE,
ro.OFFICEPHONE ,
ro.OFFICEFAX ,
ro.OFFICECOUNTRY 
from 
{{ref('stg_hub_Offices')}} as ho 
inner join {{source('qwt_raw','RAW_VAULT_OFFICES')}} ro on
ro.officeid = ho.officeid