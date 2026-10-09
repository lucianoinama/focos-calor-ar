"""Bronze -> Silver (Delta): tipado, deduplicado y consolidado con MERGE."""
import csv
from datetime import date, datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType, TimestampType

from focos.config import BRONZE_DIR, CONTROL_BRONZE_DIR, CONTROL_SILVER_DIR, SILVER_DIR, log
from focos.spark_sesion import crear_sesion

ESQUEMA_FOCOS = StructType([
    StructField("foco_id", StringType()),
    StructField("satelite", StringType()),
    StructField("producto", StringType()),
    StructField("fuente", StringType()),
    StructField("latitud", DoubleType()),
    StructField("longitud", DoubleType()),
    StructField("fecha_utc", DateType()),
    StructField("hora_utc", StringType()),
    StructField("ts_utc", TimestampType()),
    StructField("fecha_local", DateType()),
    StructField("frp", DoubleType()),
    StructField("brillo_ti4", DoubleType()),
    StructField("brillo_ti5", DoubleType()),
    StructField("scan", DoubleType()),
    StructField("track", DoubleType()),
    StructField("confianza", StringType()),
    StructField("dia_noche", StringType()),
    StructField("version", StringType()),
    StructField("actualizado_en", TimestampType()),
])
COLUMNAS_FOCOS = [campo.name for campo in ESQUEMA_FOCOS.fields]

ESQUEMA_CLIMA = StructType([
    StructField("departamento_id", StringType()),
    StructField("fecha", DateType()),
    StructField("temp_max_c", DoubleType()),
    StructField("humedad_min_pct", DoubleType()),
    StructField("precip_mm", DoubleType()),
    StructField("viento_max_kmh", DoubleType()),
    StructField("actualizado_en", TimestampType()),
])
COLUMNAS_CLIMA = [campo.name for campo in ESQUEMA_CLIMA.fields]


def _leer_marca(nombre: str) -> datetime | None:
    archivo = CONTROL_SILVER_DIR / nombre
    if not archivo.exists():
        return None
    return datetime.fromisoformat(archivo.read_text(encoding="utf-8").strip())


def _guardar_marca(nombre: str, valor: datetime) -> None:
    CONTROL_SILVER_DIR.mkdir(parents=True, exist_ok=True)
    (CONTROL_SILVER_DIR / nombre).write_text(valor.isoformat(), encoding="utf-8")


def _lote_nuevo(spark, ruta_bronze: str, marca: datetime | None):
    """Filas de Bronze posteriores a la marca y el maximo lote_ingesta, o (None, None)."""
    if not DeltaTable.isDeltaTable(spark, ruta_bronze):
        return None, None
    bronze = spark.read.format("delta").load(ruta_bronze)
    if marca is not None:
        bronze = bronze.filter(F.col("lote_ingesta") > F.lit(marca))
    maximo = bronze.agg(F.max("lote_ingesta")).first()[0]
    return (bronze, maximo) if maximo is not None else (None, None)


def _crear_si_no_existe(spark, ruta: str, esquema: StructType, particion: str | None = None) -> None:
    if DeltaTable.isDeltaTable(spark, ruta):
        return
    escritor = spark.createDataFrame([], esquema).write.format("delta")
    if particion:
        escritor = escritor.partitionBy(particion)
    escritor.save(ruta)


def _ultimo_por_clave(datos: DataFrame, claves: list[str]) -> DataFrame:
    """Una fila por clave: la del lote_ingesta mas reciente."""
    ventana = Window.partitionBy(*claves).orderBy(F.col("lote_ingesta").desc())
    return datos.withColumn("_orden", F.row_number().over(ventana)).filter("_orden = 1").drop("_orden")


def _tipar_focos(bronze: DataFrame) -> DataFrame:
    """Valida dominios, tipa y deduplica el lote de focos."""
    invalidos = (
        bronze.filter(
            F.col("confidence").isNull()
            | ~F.lower(F.col("confidence")).isin("l", "n", "h")
            | F.col("daynight").isNull()
            | ~F.upper(F.col("daynight")).isin("D", "N")
        )
        .select("confidence", "daynight").distinct().limit(20).collect()
    )
    if invalidos:
        raise RuntimeError(f"Valores inesperados de confidence/daynight: {[tuple(f) for f in invalidos]}")
    hora = F.lpad(F.col("acq_time"), 4, "0")
    ts = F.to_timestamp(F.concat_ws(" ", F.col("acq_date"), hora), "yyyy-MM-dd HHmm")
    latitud = F.col("latitude").cast("double")
    longitud = F.col("longitude").cast("double")
    confianza = F.lower(F.col("confidence"))
    tipado = bronze.select(
        F.sha2(F.concat_ws("|", F.col("satelite"), F.col("acq_date"), hora,
                           F.format_string("%.4f", latitud), F.format_string("%.4f", longitud)), 256).alias("foco_id"),
        F.col("satelite"),
        F.col("producto"),
        F.col("source").alias("fuente"),
        latitud.alias("latitud"),
        longitud.alias("longitud"),
        F.to_date(F.col("acq_date")).alias("fecha_utc"),
        hora.alias("hora_utc"),
        ts.alias("ts_utc"),
        F.to_date(ts - F.expr("INTERVAL 3 HOURS")).alias("fecha_local"),
        F.col("frp").cast("double").alias("frp"),
        F.col("bright_ti4").cast("double").alias("brillo_ti4"),
        F.col("bright_ti5").cast("double").alias("brillo_ti5"),
        F.col("scan").cast("double").alias("scan"),
        F.col("track").cast("double").alias("track"),
        F.when(confianza == "l", "baja").when(confianza == "n", "nominal").otherwise("alta").alias("confianza"),
        F.when(F.upper(F.col("daynight")) == "D", "dia").otherwise("noche").alias("dia_noche"),
        F.col("version"),
        F.current_timestamp().alias("actualizado_en"),
        F.col("lote_ingesta"),
    )
    return _ultimo_por_clave(tipado, ["foco_id"]).select(*COLUMNAS_FOCOS)


def _intervalos_sp(marca: datetime | None) -> list[tuple[str, date, date]]:
    """Coberturas de los archivos SP nuevos, unidas en intervalos continuos por satelite."""
    archivo = CONTROL_BRONZE_DIR / "focos_procesados.csv"
    if not archivo.exists():
        return []
    rangos: dict[str, list[tuple[date, date]]] = {}
    with archivo.open(encoding="utf-8", newline="") as entrada:
        for fila in csv.DictReader(entrada):
            if fila["producto"] != "SP":
                continue
            if marca is not None and datetime.fromisoformat(fila["lote_ingesta"]) <= marca:
                continue
            rangos.setdefault(fila["satelite"], []).append(
                (date.fromisoformat(fila["cobertura_inicio"]), date.fromisoformat(fila["cobertura_fin"]))
            )
    intervalos = []
    for satelite, lista in rangos.items():
        lista.sort()
        inicio, fin = lista[0]
        for siguiente_inicio, siguiente_fin in lista[1:]:
            if siguiente_inicio <= fin + timedelta(days=1):
                fin = max(fin, siguiente_fin)
            else:
                intervalos.append((satelite, inicio, fin))
                inicio, fin = siguiente_inicio, siguiente_fin
        intervalos.append((satelite, inicio, fin))
    return intervalos


def silver_focos() -> None:
    """Comando `silver_focos`: Bronze focos -> Silver focos."""
    ruta_bronze, ruta_silver = str(BRONZE_DIR / "focos"), str(SILVER_DIR / "focos")
    marca = _leer_marca("marca_focos.txt")
    spark = crear_sesion("silver_focos")
    try:
        bronze, maximo = _lote_nuevo(spark, ruta_bronze, marca)
        if bronze is None:
            log.info("Silver focos: sin lotes nuevos")
            return
        lote = _tipar_focos(bronze).cache()
        _crear_si_no_existe(spark, ruta_silver, ESQUEMA_FOCOS, "fecha_utc")

        # SP: reemplaza por completo los dias y satelites que cubre (etapa 6)
        sp = lote.filter(F.col("producto") == "SP")
        for satelite, inicio, fin in _intervalos_sp(marca):
            condicion = f"satelite = '{satelite}' AND fecha_utc >= '{inicio}' AND fecha_utc <= '{fin}'"
            (sp.filter(condicion).write.format("delta").mode("overwrite")
               .option("replaceWhere", condicion).save(ruta_silver))
            log.info("Silver focos: SP reemplazo %s de %s a %s", satelite, inicio, fin)

        # NRT: MERGE, salvo dias que ya tienen SP
        dias_sp = (spark.read.format("delta").load(ruta_silver)
                   .filter(F.col("producto") == "SP").select("satelite", "fecha_utc").distinct())
        nrt = (lote.filter(F.col("producto") == "NRT")
               .join(dias_sp, ["satelite", "fecha_utc"], "left_anti").select(*COLUMNAS_FOCOS))
        (DeltaTable.forPath(spark, ruta_silver).alias("t")
            .merge(nrt.alias("s"), "t.foco_id = s.foco_id AND t.producto = 'NRT'")
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

        total = spark.read.format("delta").load(ruta_silver).count()
        log.info("Silver focos: lote con %s focos unicos; tabla con %s filas", lote.count(), total)
        _guardar_marca("marca_focos.txt", maximo)
    finally:
        spark.stop()


def silver_clima() -> None:
    """Comando `silver_clima`: Bronze clima -> Silver clima_diario."""
    ruta_bronze, ruta_silver = str(BRONZE_DIR / "clima"), str(SILVER_DIR / "clima_diario")
    marca = _leer_marca("marca_clima.txt")
    spark = crear_sesion("silver_clima")
    try:
        bronze, maximo = _lote_nuevo(spark, ruta_bronze, marca)
        if bronze is None:
            log.info("Silver clima: sin lotes nuevos")
            return
        tipado = bronze.select(
            F.col("departamento_id"),
            F.to_date(F.col("fecha")).alias("fecha"),
            F.col("temperature_2m_max").cast("double").alias("temp_max_c"),
            F.col("relative_humidity_2m_min").cast("double").alias("humedad_min_pct"),
            F.col("precipitation_sum").cast("double").alias("precip_mm"),
            F.col("wind_speed_10m_max").cast("double").alias("viento_max_kmh"),
            F.current_timestamp().alias("actualizado_en"),
            F.col("lote_ingesta"),
        )
        lote = _ultimo_por_clave(tipado, ["departamento_id", "fecha"]).select(*COLUMNAS_CLIMA)
        _crear_si_no_existe(spark, ruta_silver, ESQUEMA_CLIMA)
        (DeltaTable.forPath(spark, ruta_silver).alias("t")
            .merge(lote.alias("s"), "t.departamento_id = s.departamento_id AND t.fecha = s.fecha")
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        total = spark.read.format("delta").load(ruta_silver).count()
        log.info("Silver clima: tabla con %s filas", total)
        _guardar_marca("marca_clima.txt", maximo)
    finally:
        spark.stop()
