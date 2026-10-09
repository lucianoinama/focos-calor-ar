select * from {{ ref('fct_clima_diario') }}
where humedad_min_pct < 0 or humedad_min_pct > 100
