with base as (
    select
        *,
        coalesce(precip_mm < 1.0, false) as es_seco,
        coalesce(temp_max_c > 30 and humedad_min_pct < 30 and viento_max_kmh > 30, false) as cumple_30_30_30
    from {{ ref('stg_clima') }}
),

grupos as (
    select
        *,
        sum(case when es_seco then 0 else 1 end) over (
            partition by departamento_id order by fecha rows unbounded preceding
        ) as grupo_lluvia
    from base
)

select
    departamento_id,
    fecha,
    temp_max_c,
    humedad_min_pct,
    precip_mm,
    viento_max_kmh,
    cumple_30_30_30,
    case
        when es_seco then sum(case when es_seco then 1 else 0 end) over (
            partition by departamento_id, grupo_lluvia order by fecha rows unbounded preceding
        )
        else 0
    end as dias_sin_lluvia
from grupos
