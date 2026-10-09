select departamento_id, count(*) as vigentes
from {{ ref('snap_riesgo_departamento') }}
where dbt_valid_to is null
group by 1
having count(*) > 1
