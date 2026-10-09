with clima as (
    select
        departamento_id,
        date_trunc('month', fecha)::date as mes,
        count(*) filter (where cumple_30_30_30) as dias_30_30_30_mes,
        max(dias_sin_lluvia) as max_dias_sin_lluvia
    from {{ ref('int_clima_metricas') }}
    group by 1, 2
),

focos as (
    select
        departamento_id,
        date_trunc('month', fecha_local)::date as mes,
        count(*) as focos_mes
    from {{ ref('int_focos_geo') }}
    group by 1, 2
)

select
    c.departamento_id,
    c.mes,
    coalesce(f.focos_mes, 0) as focos_mes,
    c.dias_30_30_30_mes,
    c.max_dias_sin_lluvia,
    case
        when coalesce(f.focos_mes, 0) >= 50 or c.dias_30_30_30_mes >= 5 then 'alto'
        when coalesce(f.focos_mes, 0) >= 10 or c.dias_30_30_30_mes >= 1 then 'medio'
        else 'bajo'
    end as categoria
from clima c
left join focos f using (departamento_id, mes)
