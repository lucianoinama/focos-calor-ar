select
    g.foco_id,
    g.fecha_local,
    g.hora_utc,
    g.ts_utc,
    g.departamento_id,
    g.satelite,
    g.producto,
    g.latitud,
    g.longitud,
    g.frp,
    g.confianza,
    g.dia_noche,
    g.brillo_ti4,
    coalesce(s.categoria, 'sin_dato') as riesgo_vigente
from {{ ref('int_focos_geo') }} g
left join {{ ref('snap_riesgo_departamento') }} s
    on s.departamento_id = g.departamento_id
    and g.fecha_local >= s.dbt_valid_from::date
    and (s.dbt_valid_to is null or g.fecha_local < s.dbt_valid_to::date)
where g.fecha_local >= '{{ var("fecha_inicio") }}'::date
