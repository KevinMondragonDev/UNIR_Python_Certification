"""
FASE 8 — RETO: Optimizar un Pipeline Lento
============================================
Sin guía. El pipeline de abajo FUNCIONA, pero está escrito con
todos los anti-patrones típicos que verás en producción.

Tu trabajo: reescribirlo en la función `pipeline_optimizado()` para que
produzca EXACTAMENTE el mismo resultado, pero de forma eficiente.

Anti-patrones escondidos (encuéntralos todos — hay al menos 8):
  ▢ Lectura con inferSchema en un CSV que se lee varias veces
  ▢ UDF Python donde existe una función built-in
  ▢ Un DataFrame reutilizado en varias acciones sin cache()
  ▢ Join grande ⋈ pequeña sin broadcast (con autoBroadcast desactivado)
  ▢ shuffle.partitions = 200 para un dataset de miles de filas
  ▢ repartition() innecesario que añade un shuffle extra
  ▢ collect() al driver para luego volver a crear un DataFrame
  ▢ Escritura que genera un archivo por partición sin control
  ▢ ... ¿algo más? (pista: el orden de filter y join)

Requisitos de tu solución:
  1. pipeline_optimizado(spark) devuelve un DataFrame con las columnas:
       region, category, nivel_venta, total, num_ventas
  2. Mismo contenido que pipeline_lento() (el validador lo compara)
  3. El plan físico debe contener 'BroadcastHashJoin'
  4. El plan NO debe contener 'PythonUDF' ni 'BatchEvalPython'
  5. Debe ser al menos tan rápido como el original
  6. En un comentario al final, explica en 3-5 líneas qué cambiaste y por qué

Pista sobre explain(): busca 'Exchange' — cada uno es un shuffle.
¿Cuántos tiene el original? ¿Cuántos tiene el tuyo?

Valida: python validar.py   (incluye checks del reto)
Solución: solucion_reto.py  (⚠️ solo si te atascas)
"""

import os
import sys
import time
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")


def pipeline_lento(spark):
    """⚠️ NO MODIFICAR — es la referencia que debes mejorar."""
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    ventas = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"), header=True, inferSchema=True)
    productos = spark.read.csv(os.path.join(CSV_DIR, "products.csv"), header=True, inferSchema=True)

    def nivel(amount):
        if amount is None:
            return "N/A"
        if amount > 3000:
            return "Alto"
        if amount > 1000:
            return "Medio"
        return "Bajo"
    nivel_udf = F.udf(nivel, StringType())

    ventas = ventas.repartition(50)
    enriquecido = ventas.join(productos, "product_id") \
                        .withColumn("nivel_venta", nivel_udf(F.col("amount"))) \
                        .filter(F.col("amount") > 0)

    print(f"  filas enriquecidas: {enriquecido.count()}")
    print(f"  regiones: {enriquecido.select('region').distinct().count()}")

    filas = enriquecido.groupBy("region", "category", "nivel_venta") \
                       .agg(F.round(F.sum("amount"), 2).alias("total"),
                            F.count("*").alias("num_ventas")) \
                       .collect()
    resultado = spark.createDataFrame(filas)
    return resultado.orderBy("region", "category", "nivel_venta")


def pipeline_optimizado(spark):
    # TU CÓDIGO AQUÍ
    return None


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Reto_Fase8") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("Pipeline LENTO:")
    t0 = time.time()
    lento = pipeline_lento(spark)
    lento.count()
    t_lento = time.time() - t0
    print(f"  ⏱  {t_lento:.2f}s")

    print("\nPipeline OPTIMIZADO:")
    t0 = time.time()
    rapido = pipeline_optimizado(spark)
    if rapido is not None:
        rapido.count()
        t_rapido = time.time() - t0
        print(f"  ⏱  {t_rapido:.2f}s  (speedup x{t_lento / max(t_rapido, 0.001):.1f})")
        rapido.show(10)
        rapido.explain()

    spark.stop()

# Explica aquí qué cambiaste y por qué:
#
#
