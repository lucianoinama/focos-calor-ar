select distinct on (f.foco_id)
    f.*,
    d.departamento_id
from {{ ref('stg_focos') }} f
join {{ ref('stg_departamentos') }} d
    on ST_Contains(d.geom, f.geom_punto)
order by f.foco_id, d.departamento_id
