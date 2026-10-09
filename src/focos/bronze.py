"""Landing -> Bronze (Delta): todo como texto + metadatos de origen. Solo agrega filas."""
import csv
import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from focos.config import BRONZE_DIR, CONTROL_BRONZE_DIR, LANDING_DIR, PROYECTO_DIR, log
from focos.spark_sesion import crear_sesion

COLUMNAS_FIRMS = [
    "latitude", "longitude", "bright_ti4", "scan", "track", "acq_date", "acq_time",
    "satellite", "instrument", "confidence", "version", "bright_ti5", "frp", "daynight",
]
# carpeta de landing -> (satelite, producto)
SOURCES = {
    "VIIRS_SNPP_NRT": ("SNPP", "NRT"),
    "VIIRS_NOAA20_NRT": ("NOAA20", "NRT"),
    "VIIRS_NOAA21_NRT": ("NOAA21", "NRT"),
    "VIIRS_SNPP_SP": ("SNPP", "SP"),
    "VIIRS_NOAA20_SP": ("NOAA20", "SP"),
}
VARIABLES_CLIMA = ["temperature_2m_max", "relative_humidity_2m_min", "precipitation_sum", "wind_speed_10m_max"]
CAMPOS_CONTROL = [
    "archivo", "source", "satelite", "producto", "cobertura_inicio", "cobertura_fin",
    "filas", "tamano_bytes", "mtime", "lote_ingesta",
]
PATRON_FIRMS = re.compile(r"^(?:diario_)?(\d{4}-\d{2}-\d{2})_([1-5])d\.csv$")
ESQUEMA_CLIMA = StructType(
    [StructField(nombre, StringType()) for nombre in ["departamento_id", "fecha", *VARIABLES_CLIMA, "archivo_origen"]]
)


def _lote() -> datetime:
    """Hora UTC de inicio de la corrida, sin zona ni microsegundos."""
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def _relativa(ruta: Path) -> str:
    return ruta.relative_to(PROYECTO_DIR).as_posix()


def _firma(ruta: Path) -> tuple[str, str]:
    """(tamano en bytes, fecha de modificacion en ns) para detectar cambios."""
    estado = ruta.stat()
    return str(estado.st_size), str(estado.st_mtime_ns)


def _leer_control(nombre: str) -> dict[str, tuple[str, str]]:
    archivo = CONTROL_BRONZE_DIR / nombre
    if not archivo.exists():
        return {}
    with archivo.open(encoding="utf-8", newline="") as entrada:
        return {fila["archivo"]: (fila["tamano_bytes"], fila["mtime"]) for fila in csv.DictReader(entrada)}


def _agregar_control(nombre: str, filas: list[dict]) -> None:
    CONTROL_BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    archivo = CONTROL_BRONZE_DIR / nombre
    nuevo = not archivo.exists()
    with archivo.open("a", encoding="utf-8", newline="") as salida:
        escritor = csv.DictWriter(salida, fieldnames=CAMPOS_CONTROL, lineterminator="\n")
        if nuevo:
            escritor.writeheader()
        escritor.writerows(filas)


def _pendientes(archivos: list[Path], control: dict) -> list[Path]:
    return [ruta for ruta in archivos if control.get(_relativa(ruta)) != _firma(ruta)]


def bronze_focos() -> None:
    """Comando `bronze_focos`: CSV de FIRMS -> data/bronze/focos."""
    archivos = sorted(r for r in (LANDING_DIR / "firms").glob("*/*.csv") if r.parent.name in SOURCES)
    pendientes = _pendientes(archivos, _leer_control("focos_procesados.csv"))
    if not pendientes:
        log.info("Bronze focos: sin archivos nuevos")
        return
    lote = _lote()
    spark = crear_sesion("bronze_focos")
    try:
        for ruta in pendientes:
            coincidencia = PATRON_FIRMS.match(ruta.name)
            if not coincidencia:
                raise RuntimeError(f"Nombre de archivo inesperado: {ruta.name}")
            inicio = date.fromisoformat(coincidencia.group(1))
            fin = inicio + timedelta(days=int(coincidencia.group(2)) - 1)
            satelite, producto = SOURCES[ruta.parent.name]
            tamano, mtime = _firma(ruta)
            leido = spark.read.option("header", True).csv(str(ruta))
            columnas = [
                F.col(c).cast("string").alias(c) if c in leido.columns else F.lit(None).cast("string").alias(c)
                for c in COLUMNAS_FIRMS
            ]
            datos = leido.select(*columnas).withColumns({
                "source": F.lit(ruta.parent.name),
                "satelite": F.lit(satelite),
                "producto": F.lit(producto),
                "archivo_origen": F.lit(_relativa(ruta)),
                "lote_ingesta": F.lit(lote),
            })
            filas = datos.count()
            datos.write.format("delta").mode("append").save(str(BRONZE_DIR / "focos"))
            _agregar_control("focos_procesados.csv", [{
                "archivo": _relativa(ruta), "source": ruta.parent.name, "satelite": satelite,
                "producto": producto, "cobertura_inicio": inicio.isoformat(), "cobertura_fin": fin.isoformat(),
                "filas": filas, "tamano_bytes": tamano, "mtime": mtime, "lote_ingesta": lote.isoformat(),
            }])
            log.info("Bronze focos: %s filas de %s", filas, ruta.name)
    finally:
        spark.stop()


def bronze_clima() -> None:
    """Comando `bronze_clima`: JSON de Open-Meteo -> data/bronze/clima (una fila por dia)."""
    base = LANDING_DIR / "openmeteo"
    archivos = sorted([*base.glob("diario/*/*.json"), *base.glob("backfill/*.json")])
    pendientes = _pendientes(archivos, _leer_control("clima_procesados.csv"))
    if not pendientes:
        log.info("Bronze clima: sin archivos nuevos")
        return
    lote = _lote()
    filas, control = [], []
    for ruta in pendientes:
        diario = json.loads(ruta.read_text(encoding="utf-8"))["daily"]
        fechas = diario["time"]
        for indice, fecha in enumerate(fechas):
            valores = [None if diario[v][indice] is None else str(diario[v][indice]) for v in VARIABLES_CLIMA]
            filas.append((ruta.stem, fecha, *valores, _relativa(ruta)))
        tamano, mtime = _firma(ruta)
        control.append({
            "archivo": _relativa(ruta), "source": "open-meteo", "satelite": "", "producto": "",
            "cobertura_inicio": min(fechas) if fechas else "", "cobertura_fin": max(fechas) if fechas else "",
            "filas": len(fechas), "tamano_bytes": tamano, "mtime": mtime, "lote_ingesta": lote.isoformat(),
        })
    spark = crear_sesion("bronze_clima")
    try:
        datos = spark.createDataFrame(filas, ESQUEMA_CLIMA).withColumn("lote_ingesta", F.lit(lote))
        datos.write.format("delta").mode("append").save(str(BRONZE_DIR / "clima"))
    finally:
        spark.stop()
    _agregar_control("clima_procesados.csv", control)
    log.info("Bronze clima: %s filas de %s archivos", len(filas), len(pendientes))
