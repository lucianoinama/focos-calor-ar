select
    *,
    ST_SetSRID(ST_MakePoint(longitud, latitud), 4326) as geom_punto
from {{ source('raw', 'focos') }}
