with focos as (
    select
        departamento_id,
        fecha_local as fecha,
        count(*) as cantidad_focos,
        sum(frp) as frp_total
    from {{ ref('fct_focos') }}
    group by 1, 2
)

select
    c.departamento_id,
    c.fecha,
    c.temp_max_c,
    c.humedad_min_pct,
    c.precip_mm,
    c.viento_max_kmh,
    c.dias_sin_lluvia,
    c.cumple_30_30_30,
    coalesce(f.cantidad_focos, 0) as cantidad_focos,
    coalesce(f.frp_total, 0) as frp_total
from {{ ref('int_clima_metricas') }} c
left join focos f using (departamento_id, fecha)
where c.fecha >= '{{ var("fecha_inicio") }}'::date
