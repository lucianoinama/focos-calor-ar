"""Silver (Delta) -> schema raw de Postgres, por JDBC."""
from delta.tables import DeltaTable

from focos.config import SILVER_DIR, conectar_dw, log, opciones_jdbc
from focos.spark_sesion import crear_sesion

# (tabla Silver, tabla destino)
TABLAS = [
    (SILVER_DIR / "focos", "raw.focos"),
    (SILVER_DIR / "clima_diario", "raw.clima_diario"),
]
# (tabla, sentencia) para crear indices despues de exportar
INDICES = [
    ("raw.focos", "create index if not exists focos_fecha_local_idx on raw.focos (fecha_local)"),
    ("raw.focos", "create index if not exists focos_satelite_fecha_idx on raw.focos (satelite, fecha_utc)"),
    ("raw.clima_diario", "create index if not exists clima_depto_fecha_idx on raw.clima_diario (departamento_id, fecha)"),
]


def exportar_postgres() -> None:
    """Comando `exportar_postgres`: copia completa de Silver a raw."""
    exportadas = []
    spark = crear_sesion("exportar_postgres")
    try:
        for ruta, tabla in TABLAS:
            if not DeltaTable.isDeltaTable(spark, str(ruta)):
                log.warning("No existe %s; se omite", ruta)
                continue
            datos = spark.read.format("delta").load(str(ruta))
            filas = datos.count()
            (datos.write.format("jdbc")
                .options(**opciones_jdbc(), dbtable=tabla, truncate="true", batchsize="5000")
                .mode("overwrite").save())
            exportadas.append(tabla)
            log.info("Exportadas %s filas a %s", filas, tabla)
    finally:
        spark.stop()

    conexion = conectar_dw()
    try:
        with conexion, conexion.cursor() as cur:
            for tabla, sentencia in INDICES:
                if tabla in exportadas:
                    cur.execute(sentencia)
    finally:
        conexion.close()
