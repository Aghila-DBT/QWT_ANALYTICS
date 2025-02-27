{{config(materialized = 'table')}}

select 
md5(officeid) as officehashkey,
current_timestamp as loaddate,
sourcefile as recordsource,
officeid
from 
{{source('qwt_raw','RAW_VAULT_OFFICES')}}