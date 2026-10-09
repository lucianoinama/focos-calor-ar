"""Departamentos de Argentina: descarga de Georef, carga en PostGIS y puntos internos."""
import csv
import json

import shapely
from shapely.geometry import shape

from focos.config import LANDING_DIR, REFERENCE_DIR, conectar_dw, http_get, log

URL_PRINCIPAL = "https://apis.datos.gob.ar/georef/api/v2.0/departamentos.geojson"
URL_ALTERNATIVA = "https://infra.datos.gob.ar/georef/departamentos.geojson"
ARCHIVO_GEOJSON = LANDING_DIR / "georef" / "departamentos.geojson"
ARCHIVO_PUNTOS = REFERENCE_DIR / "departamentos_puntos.csv"
MINIMO_DEPARTAMENTOS = 500

SQL_CREAR = """
create extension if not exists postgis;
create schema if not exists raw;
drop table if exists raw.departamentos;
create table raw.departamentos (
    departamento_id text primary key,
    departamento_nombre text not null,
    provincia_id text not null,
    provincia_nombre text not null,
    geom geometry(MultiPolygon, 4326) not null,
    superficie_km2 double precision,
    punto_lat double precision not null,
    punto_lon double precision not null
);
"""

SQL_INSERTAR = """
insert into raw.departamentos
    (departamento_id, departamento_nombre, provincia_id, provincia_nombre, geom, punto_lat, punto_lon)
values (
    %s, %s, %s, %s,
    ST_Multi(ST_CollectionExtract(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)), 3)),
    %s, %s
)
"""

SQL_FINAL = """
update raw.departamentos set superficie_km2 = ST_Area(geom::geography) / 1e6;
create index departamentos_geom_gix on raw.departamentos using gist (geom);
"""


def _descargar() -> dict:
    """Descarga el GeoJSON (con URL alternativa) y lo guarda crudo en landing."""
    try:
        respuesta = http_get(URL_PRINCIPAL)
    except RuntimeError as exc:
        log.warning("Fallo la URL principal (%s); uso la alternativa", exc)
        respuesta = http_get(URL_ALTERNATIVA)
    ARCHIVO_GEOJSON.parent.mkdir(parents=True, exist_ok=True)
    ARCHIVO_GEOJSON.write_bytes(respuesta.content)
    return json.loads(respuesta.content)


def _provincia(propiedades: dict) -> tuple[str, str]:
    """Devuelve (provincia_id, provincia_nombre) en cualquiera de los dos formatos de Georef."""
    provincia = propiedades.get("provincia")
    if isinstance(provincia, dict) and "id" in provincia and "nombre" in provincia:
        return str(provincia["id"]), provincia["nombre"]
    if "provincia_id" in propiedades and "provincia_nombre" in propiedades:
        return str(propiedades["provincia_id"]), propiedades["provincia_nombre"]
    raise RuntimeError(f"No encuentro la provincia. Claves recibidas: {sorted(propiedades)}")


def cargar_departamentos() -> None:
    """Comando `referencias`: GeoJSON -> raw.departamentos + CSV de puntos."""
    datos = _descargar()
    filas, sin_geometria = [], 0
    for feature in datos["features"]:
        propiedades = feature.get("properties") or {}
        geometria = feature.get("geometry")
        if not geometria:
            sin_geometria += 1
            continue
        if "id" not in propiedades or "nombre" not in propiedades:
            raise RuntimeError(f"Faltan id o nombre. Claves recibidas: {sorted(propiedades)}")
        provincia_id, provincia_nombre = _provincia(propiedades)
        punto = shapely.make_valid(shape(geometria)).representative_point()
        filas.append((
            str(propiedades["id"]), propiedades["nombre"], provincia_id, provincia_nombre,
            json.dumps(geometria), round(punto.y, 6), round(punto.x, 6),
        ))
    log.info("Departamentos leidos: %s (sin geometria: %s)", len(filas), sin_geometria)
    if len(filas) < MINIMO_DEPARTAMENTOS:
        raise RuntimeError(f"Solo {len(filas)} departamentos; se esperaban al menos {MINIMO_DEPARTAMENTOS}")

    conexion = conectar_dw()
    try:
        with conexion, conexion.cursor() as cur:
            cur.execute(SQL_CREAR)
            cur.executemany(SQL_INSERTAR, filas)
            cur.execute(SQL_FINAL)
    finally:
        conexion.close()

    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    with ARCHIVO_PUNTOS.open("w", newline="", encoding="utf-8") as archivo:
        escritor = csv.writer(archivo, lineterminator="\n")
        escritor.writerow(["departamento_id", "punto_lat", "punto_lon"])
        for fila in sorted(filas):
            escritor.writerow([fila[0], f"{fila[5]:.6f}", f"{fila[6]:.6f}"])
    log.info("Cargados %s departamentos en raw.departamentos y %s", len(filas), ARCHIVO_PUNTOS)
