select
    departamento_id,
    categoria,
    (mes + interval '1 month')::timestamp as vigente_desde
from {{ ref('int_riesgo_mensual') }}
where mes = {% if var('mes_corte') %}'{{ var("mes_corte") }}'::date
            {% else %}(date_trunc('month', current_date) - interval '1 month')::date{% endif %}
