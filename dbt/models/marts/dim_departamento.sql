select
    departamento_id,
    departamento_nombre,
    provincia_id,
    provincia_nombre,
    superficie_km2,
    punto_lat,
    punto_lon
from {{ ref('stg_departamentos') }}
