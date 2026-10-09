"""Mantenimiento: compactar tablas Delta y reiniciar el snapshot de riesgo."""
from pathlib import Path

from delta.tables import DeltaTable

from focos.config import BRONZE_DIR, SILVER_DIR, conectar_dw, log
from focos.spark_sesion import crear_sesion

TABLAS_DELTA = [
    BRONZE_DIR / "focos",
    BRONZE_DIR / "clima",
    SILVER_DIR / "focos",
    SILVER_DIR / "clima_diario",
]
RETENCION_HORAS = 168  # 7 dias, el minimo seguro por defecto de Delta


def _contar_parquet(ruta: Path) -> int:
    return sum(1 for _ in ruta.rglob("*.parquet"))


def _tamano_mb(ruta: Path) -> float:
    return sum(f.stat().st_size for f in ruta.rglob("*") if f.is_file()) / 1_048_576


def compactar_delta() -> None:
    """Comando `compactar_delta`: OPTIMIZE + VACUUM de las 4 tablas Delta."""
    spark = crear_sesion("compactar_delta")
    try:
        for ruta in TABLAS_DELTA:
            if not DeltaTable.isDeltaTable(spark, str(ruta)):
                log.warning("No existe %s; se omite", ruta)
                continue
            antes = (_contar_parquet(ruta), _tamano_mb(ruta))
            tabla = DeltaTable.forPath(spark, str(ruta))
            tabla.optimize().executeCompaction()
            tabla.vacuum(RETENCION_HORAS)
            despues = (_contar_parquet(ruta), _tamano_mb(ruta))
            log.info(
                "%s: %s archivos (%.1f MB) -> %s archivos (%.1f MB)",
                ruta.relative_to(ruta.parents[1]), antes[0], antes[1], despues[0], despues[1],
            )
    finally:
        spark.stop()


def borrar_snapshot_riesgo() -> None:
    """Comando `borrar_snapshot_riesgo`: borra el snapshot para reconstruirlo desde enero."""
    conexion = conectar_dw()
    try:
        with conexion, conexion.cursor() as cur:
            cur.execute("drop table if exists snapshots.snap_riesgo_departamento")
    finally:
        conexion.close()
    log.info("Snapshot de riesgo borrado; se reconstruye mes por mes")
