"""Crea la SparkSession local con Delta Lake y el driver JDBC de Postgres."""
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

def crear_sesion(nombre_app: str) -> SparkSession:
    """Devuelve una SparkSession local configurada para Delta y Postgres."""
    builder = (
        SparkSession.builder.appName(nombre_app)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", os.environ["SPARK_DRIVER_MEMORY"])
        .config("spark.jars.ivy", "/opt/airflow/ivy2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
    )
    return configure_spark_with_delta_pip(
        builder, extra_packages=["org.postgresql:postgresql:42.7.4"]
    ).getOrCreate()
