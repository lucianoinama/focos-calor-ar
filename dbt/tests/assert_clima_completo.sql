with rango as (
    select min(fecha) as inicio, max(fecha) as fin
    from {{ ref('fct_clima_diario') }}
),

esperado as (
    select d.departamento_id, dia::date as fecha
    from {{ ref('dim_departamento') }} d
    cross join rango r
    cross join generate_series(r.inicio, r.fin, interval '1 day') as dia
)

select e.*
from esperado e
left join {{ ref('fct_clima_diario') }} c using (departamento_id, fecha)
where c.departamento_id is null
