{{config(materialized = 'table')}}

select 
OrderID ,
LineNo ,
ShipperID ,
CustomerID ,
ProductID ,
EmployeeID	,
CAST(SPLIT_PART(shipmentdate, ' ', 1) as DATE) as shipmentdate,
Status  
from 
{{source('qwt_raw','RAW_SHIPMENTS')}}