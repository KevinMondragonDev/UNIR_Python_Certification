"""
FASE 8 — SOLUCIÓN del Reto: Optimizar un Pipeline Lento
=========================================================
⚠️ Revisa esto SOLO si ya intentaste resolver reto.py por tu cuenta.
"""

import os
import sys
import time
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
OUT_DIR  = os.path.join(BASE, "fase_08_optimizacion_y_udfs", "output_reto")

# FIX 1: schema explícito → Spark no lee el archivo una vez extra para inferir tipos
SCHEMA_VENTAS = "sale_id INT, employee_id INT, product_id INT, amount DOUBLE, sale_date STRING, region STRING"
SCHEMA_PRODUCTOS = "product_id INT, name STRING, category STRING, price DOUBLE, stock INT"


def pipeline_optimizado(spark):
    # FIX 2: shuffle.partitions acorde al volumen (miles de filas, no TB)
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    # FIX 3: AQE ajusta particiones y estrategia de join en runtime
    spark.conf.set("spark.sql.adaptive.enabled", "true")

    ventas = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"), header=True, schema=SCHEMA_VENTAS)
    productos = spark.read.csv(os.path.join(CSV_DIR, "products.csv"), header=True, schema=SCHEMA_PRODUCTOS) \
                     .select("product_id", "category")   # FIX 4: solo las columnas necesarias

    # FIX 5: built-in F.when en lugar de UDF Python → sin serialización JVM↔Python,
    #        Catalyst puede optimizarlo y generar código (WholeStageCodegen)
    nivel = F.when(F.col("amount").isNull(), "N/A") \
             .when(F.col("amount") > 3000, "Alto") \
             .when(F.col("amount") > 1000, "Medio") \
             .otherwise("Bajo")

    # FIX 6: filtrar ANTES del join (menos filas que mover)
    # FIX 7: sin repartition(50): era un shuffle extra que no aportaba nada
    # FIX 8: broadcast de la tabla pequeña (100 filas) → ventas no se shufflea para el join
    enriquecido = ventas.filter(F.col("amount") > 0) \
                        .join(F.broadcast(productos), "product_id") \
                        .withColumn("nivel_venta", nivel)

    # FIX 9: sin collect() + createDataFrame: el resultado se queda distribuido.
    #        collect() trae todo al driver → cuello de botella y riesgo de OOM.
    return enriquecido.groupBy("region", "category", "nivel_venta") \
                      .agg(F.round(F.sum("amount"), 2).alias("total"),
                           F.count("*").alias("num_ventas")) \
                      .orderBy("region", "category", "nivel_venta")


def escribir_resultado(df):
    # FIX 10: coalesce(1) para un reporte pequeño → 1 archivo, no 200 archivos vacíos
    df.coalesce(1).write.mode("overwrite").parquet(OUT_DIR)


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from reto import pipeline_lento

    spark = SparkSession.builder \
        .appName("Solucion_Reto_Fase8") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("Pipeline LENTO:")
    t0 = time.time()
    lento = pipeline_lento(spark)
    filas_lento = lento.collect()
    t_lento = time.time() - t0
    print(f"  ⏱  {t_lento:.2f}s")

    print("\nPipeline OPTIMIZADO:")
    t0 = time.time()
    rapido = pipeline_optimizado(spark)
    filas_rapido = rapido.collect()
    t_rapido = time.time() - t0
    print(f"  ⏱  {t_rapido:.2f}s  (speedup x{t_lento / max(t_rapido, 0.001):.1f})")

    print(f"\nMismo resultado: {[tuple(r) for r in filas_lento] == [tuple(r) for r in filas_rapido]}")
    rapido.show(10)
    print("Plan optimizado (1 BroadcastExchange + 2 Exchange: groupBy y orderBy):")
    rapido.explain()

    escribir_resultado(rapido)
    print(f"Archivos parquet escritos: "
          f"{len([f for f in os.listdir(OUT_DIR) if f.endswith('.parquet')])}")

    spark.stop()

# Qué cambié y por qué:
#   - Eliminé la UDF (F.when), el repartition(50) y el collect(): los tres
#     obligaban a mover datos (a Python, por la red o al driver) sin necesidad.
#   - Broadcast de productos: 100 filas viajan a cada executor en lugar de
#     hacer shuffle de las 2.000 ventas para el join.
#   - Filtro antes del join, schema explícito, solo columnas necesarias y
#     shuffle.partitions=8 + AQE: menos trabajo y menos tasks vacías.
