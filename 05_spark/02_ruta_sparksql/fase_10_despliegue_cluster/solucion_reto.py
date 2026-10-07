"""
FASE 10 — SOLUCIÓN del Reto: Job de Producción Desplegado en Clúster
=====================================================================
⚠️ Revisa esto SOLO si ya intentaste resolver reto.py por tu cuenta.

Despliegue:
  ./cluster_local.sh start
  spark-submit --master spark://127.0.0.1:7077 \\
      --executor-cores 2 --executor-memory 1g --total-executor-cores 4 \\
      solucion_reto.py --salida /tmp/metricas_moneda --status completed --status refunded
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
log = logging.getLogger("metricas_moneda")


def parsear_args(argv):
    p = argparse.ArgumentParser(description="Métricas de transacciones por moneda y mes")
    p.add_argument("--entrada", default=os.path.join(BASE, "datasets", "parquet", "transactions.parquet"))
    p.add_argument("--salida", required=True)
    p.add_argument("--status", action="append", default=None,
                   help="Status a incluir (repetible). Default: completed")
    p.add_argument("--particiones", type=int, default=8)
    args = p.parse_args(argv)
    args.status = args.status or ["completed"]
    return args


def calcular_metricas(spark, args):
    return spark.read.parquet(args.entrada) \
        .filter(F.col("status").isin(args.status)) \
        .withColumn("anio_mes", F.date_format("ts", "yyyy-MM")) \
        .groupBy("currency", "anio_mes") \
        .agg(F.count("*").alias("num_tx"),
             F.round(F.sum("amount"), 2).alias("total"),
             F.round(F.avg("amount"), 2).alias("ticket_promedio"),
             F.max("amount").alias("max_tx"))


def validar_calidad(df):
    """Devuelve una lista de problemas encontrados (vacía = OK)."""
    problemas = []
    # Una sola pasada sobre los datos para todas las reglas
    r = df.agg(F.sum(F.when(F.col("total") < 0, 1).otherwise(0)).alias("negativos"),
               F.sum(F.when(F.col("currency").isNull(), 1).otherwise(0)).alias("sin_moneda"),
               F.count("*").alias("filas")).first()
    if r["filas"] == 0:
        problemas.append("resultado vacío")
    if r["negativos"]:
        problemas.append(f"{r['negativos']} filas con total negativo")
    if r["sin_moneda"]:
        problemas.append(f"{r['sin_moneda']} filas sin currency")
    return problemas, r["filas"]


def main(argv=None):
    args = parsear_args(argv)

    spark = SparkSession.builder \
        .appName("metricas_moneda") \
        .config("spark.sql.shuffle.partitions", str(args.particiones)) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        log.info("master=%s status=%s particiones=%s",
                 spark.sparkContext.master, args.status, args.particiones)

        metricas = calcular_metricas(spark, args).cache()
        problemas, filas = validar_calidad(metricas)
        if problemas:
            for p in problemas:
                log.error("Calidad: %s", p)
            return 1

        # repartition por la misma columna de partitionBy → 1 archivo por carpeta
        metricas.repartition("currency") \
            .write.mode("overwrite") \
            .partitionBy("currency") \
            .parquet(args.salida)
        log.info("%d filas escritas en %s", filas, args.salida)
        return 0
    except Exception:
        log.exception("El job falló")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

# Respuestas de despliegue (clúster local, 2 workers × 2 cores):
#   b) 2 executors (uno por worker) con 2 cores cada uno: el master reparte
#      --total-executor-cores 4 en bloques de --executor-cores 2.
#   c) Con AQE activo el stage del groupBy NO muestra 8 tasks: AQE fusiona
#      las particiones post-shuffle pequeñas (aparece "AQEShuffleRead coalesced").
#   d) Con spark.sql.adaptive.enabled=false salen exactamente --particiones (8).
