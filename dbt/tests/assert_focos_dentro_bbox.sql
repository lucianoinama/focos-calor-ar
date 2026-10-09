select * from {{ ref('fct_focos') }}
where latitud not between -56 and -21
   or longitud not between -74 and -53
