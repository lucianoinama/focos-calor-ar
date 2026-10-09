select * from {{ ref('fct_focos') }}
where frp < 0
