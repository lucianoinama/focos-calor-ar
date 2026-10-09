with dias as (
    select generate_series('{{ var("fecha_inicio") }}'::date, current_date, interval '1 day')::date as fecha
)

select
    fecha,
    extract(year from fecha)::int as anio,
    extract(month from fecha)::int as mes,
    (array['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto',
           'septiembre', 'octubre', 'noviembre', 'diciembre'])[extract(month from fecha)::int] as nombre_mes,
    extract(isodow from fecha)::int as dia_semana,
    extract(isodow from fecha) in (6, 7) as es_fin_de_semana,
    case
        when extract(month from fecha) in (12, 1, 2) then 'verano'
        when extract(month from fecha) in (3, 4, 5) then 'otono'
        when extract(month from fecha) in (6, 7, 8) then 'invierno'
        else 'primavera'
    end as estacion
from dias
