select departamento_id, fecha, count(*) as filas
from {{ ref('stg_clima') }}
group by 1, 2
having count(*) > 1
