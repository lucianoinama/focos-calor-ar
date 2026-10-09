"""Configuracion compartida: variables de entorno, rutas, HTTP y Postgres."""
import logging
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg2
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("focos")

PROYECTO_DIR = Path(os.environ.get("PROYECTO_DIR", "/opt/airflow/proyecto"))
DATA_DIR = PROYECTO_DIR / "data"
LANDING_DIR = DATA_DIR / "landing"
REFERENCE_DIR = DATA_DIR / "reference"

ZONA_AR = ZoneInfo("America/Argentina/Buenos_Aires")
ESPERAS_REINTENTO = (10, 30, 90)  # segundos entre reintentos


def variable(nombre: str) -> str:
    """Lee una variable de entorno obligatoria."""
    valor = os.environ.get(nombre)
    if not valor:
        raise RuntimeError(f"Falta la variable {nombre} en .env")
    return valor


def hoy_ar() -> date:
    """Fecha de hoy en Argentina (D)."""
    return datetime.now(ZONA_AR).date()


def hoy_utc() -> date:
    """Fecha de hoy en UTC."""
    return datetime.now(timezone.utc).date()


def ocultar_secretos(texto: str) -> str:
    """Reemplaza la MAP_KEY de FIRMS por *** antes de loguear."""
    clave = os.environ.get("FIRMS_MAP_KEY")
    return texto.replace(clave, "***") if clave else texto


def http_get(url: str, params: dict | None = None, timeout: int = 120) -> requests.Response:
    """GET con reintentos ante 429, 5xx y timeouts. Falla de inmediato ante otros 4xx."""
    error = None
    for intento in range(len(ESPERAS_REINTENTO) + 1):
        try:
            respuesta = requests.get(url, params=params, timeout=timeout)
        except (requests.Timeout, requests.ConnectionError) as exc:
            error = repr(exc)
        else:
            if respuesta.status_code == 200:
                return respuesta
            if respuesta.status_code == 429 or respuesta.status_code >= 500:
                error = f"HTTP {respuesta.status_code}"
            else:
                raise RuntimeError(
                    f"HTTP {respuesta.status_code} en {ocultar_secretos(respuesta.url)}: "
                    f"{ocultar_secretos(respuesta.text[:500])}"
                )
        if intento < len(ESPERAS_REINTENTO):
            espera = ESPERAS_REINTENTO[intento]
            log.warning("Reintento en %s s (%s): %s", espera, error, ocultar_secretos(url))
            time.sleep(espera)
    raise RuntimeError(f"Fallo tras los reintentos ({error}): {ocultar_secretos(url)}")


def conectar_dw():
    """Conexion a la base focos del data warehouse."""
    return psycopg2.connect(
        host=variable("DW_HOST"),
        port=variable("DW_PORT"),
        dbname=variable("DW_DB"),
        user=variable("DW_USER"),
        password=variable("DW_PASSWORD"),
    )


def tabla_existe(conexion, esquema: str, tabla: str) -> bool:
    """True si la tabla existe en el data warehouse."""
    with conexion.cursor() as cur:
        cur.execute("select to_regclass(%s) is not null", (f"{esquema}.{tabla}",))
        return cur.fetchone()[0]

# --- Etapa 3: capas Delta y conexion JDBC ---
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
CONTROL_BRONZE_DIR = BRONZE_DIR / "_control"
CONTROL_SILVER_DIR = SILVER_DIR / "_control"


def opciones_jdbc() -> dict:
    """Opciones de Spark JDBC para escribir en la base focos."""
    return {
        "url": f"jdbc:postgresql://{variable('DW_HOST')}:{variable('DW_PORT')}/{variable('DW_DB')}",
        "user": variable("DW_USER"),
        "password": variable("DW_PASSWORD"),
        "driver": "org.postgresql.Driver",
    }
