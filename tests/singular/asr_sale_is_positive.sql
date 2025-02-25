select orderid, sum(linesalesamount) as sales
from 
{{ref('f_orders')}}
group by orderid
having not (sales >=0)