"""
FASE 10 — Ejemplo 1: Un Job "de producción" para spark-submit
===============================================================
Diferencias con los scripts de las fases anteriores:

  ✗ NO hay .master("local[*]") en el código
      → el master lo decide quien despliega: spark-submit --master ...
        El mismo archivo corre en local, Standalone, YARN o Kubernetes.
  ✓ Parámetros por línea de comandos (argparse), no rutas fijas
  ✓ main() con códigos de salida: 0 = OK, 1 = error (los orquestadores
    como Airflow deciden reintentos con esto)
  ✓ Logging en vez de print
  ✓ spark.stop() garantizado con try/finally
  ✓ Imprime la configuración efectiva (para depurar despliegues)

Ejecutar:
  # Local (equivalente a lo que hacías antes)
  spark-submit --master "local[4]" 01_job_spark_submit.py --salida /tmp/reporte

  # Clúster standalone (./cluster_local.sh start)
  spark-submit --master spark://127.0.0.1:7077 \\
      --executor-cores 1 --executor-memory 1g --total-executor-cores 4 \\
      01_job_spark_submit.py --salida /tmp/reporte --anio 2024

  # También funciona con python (usa local[*] por defecto)
  python 01_job_spark_submit.py --salida /tmp/reporte
"""

import argparse
import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("job_regiones")

SCHEMA_VENTAS = "sale_id INT, employee_id INT, product_id INT, amount DOUBLE, sale_date DATE, region STRING"
SCHEMA_PRODUCTOS = "product_id INT, name STRING, category STRING, price DOUBLE, stock INT"


def parsear_args(argv=None):
    p = argparse.ArgumentParser(description="Reporte de ventas por región y categoría")
    p.add_argument("--ventas",    default=os.path.join(BASE, "datasets", "csv", "sales.csv"))
    p.add_argument("--productos", default=os.path.join(BASE, "datasets", "csv", "products.csv"))
    p.add_argument("--salida",    required=True, help="Directorio de salida (Parquet)")
    p.add_argument("--anio",      type=int, default=None, help="Filtrar un año concreto")
    p.add_argument("--min-ventas", type=int, default=1,
                   help="Descarta grupos con menos ventas que este umbral")
    return p.parse_args(argv)


def mostrar_configuracion(spark):
    sc = spark.sparkContext
    conf = dict(sc.getConf().getAll())
    log.info("─── Configuración efectiva ───")
    log.info("master              = %s", sc.master)
    log.info("deploy mode         = %s", conf.get("spark.submit.deployMode", "client"))
    log.info("app id              = %s", sc.applicationId)
    log.info("defaultParallelism  = %s", sc.defaultParallelism)
    for clave in ["spark.executor.memory", "spark.executor.cores",
                  "spark.executor.instances", "spark.cores.max",
                  "spark.driver.memory", "spark.sql.shuffle.partitions",
                  "spark.sql.adaptive.enabled", "spark.dynamicAllocation.enabled"]:
        valor = conf.get(clave)
        if valor is None and clave.startswith("spark.sql."):
            valor = spark.conf.get(clave)     # configs SQL tienen default en runtime
        log.info("%-34s = %s", clave, valor or "(default)")
    log.info("Spark UI            = %s", sc.uiWebUrl)


def construir_reporte(spark, args):
    ventas = spark.read.csv(args.ventas, header=True, schema=SCHEMA_VENTAS)
    productos = spark.read.csv(args.productos, header=True, schema=SCHEMA_PRODUCTOS) \
                     .select("product_id", "category")

    if args.anio:
        ventas = ventas.filter(F.year("sale_date") == args.anio)

    return ventas.filter(F.col("amount") > 0) \
        .join(F.broadcast(productos), "product_id") \
        .groupBy("region", "category") \
        .agg(F.round(F.sum("amount"), 2).alias("total"),
             F.count("*").alias("num_ventas"),
             F.round(F.avg("amount"), 2).alias("ticket_promedio")) \
        .filter(F.col("num_ventas") >= args.min_ventas)


def main(argv=None):
    args = parsear_args(argv)

    # Solo appName y configuraciones LÓGICAS del job.
    # Recursos (memoria, cores, master) → spark-submit.
    spark = SparkSession.builder \
        .appName("reporte_regiones") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        mostrar_configuracion(spark)

        reporte = construir_reporte(spark, args)
        reporte.cache()
        n = reporte.count()
        if n == 0:
            log.error("El reporte quedó vacío (¿año sin datos?). Abortando.")
            return 1

        reporte.coalesce(1).write.mode("overwrite").parquet(args.salida)
        log.info("Reporte escrito en %s (%d filas)", args.salida, n)
        reporte.orderBy(F.desc("total")).show(5, truncate=False)
        return 0
    except Exception:
        log.exception("El job falló")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
