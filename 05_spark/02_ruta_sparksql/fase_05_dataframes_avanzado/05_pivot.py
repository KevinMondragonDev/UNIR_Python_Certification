"""
FASE 5 — Ejemplo 5: Pivot y Unpivot
======================================
Aprenderás:
  - pivot() — transformar valores de filas a columnas
  - unpivot (stack) — columnas a filas
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Pivot_Unpivot") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

df_sales = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"), header=True, inferSchema=True)
df_sales = df_sales.withColumn("anio", F.year(F.to_date("sale_date", "yyyy-MM-dd")))

# ─────────────────────────────────────────
# 1. pivot() básico — filas a columnas
# ─────────────────────────────────────────
print("=" * 55)
print("1. pivot() — ventas por región a columnas")
print("=" * 55)

print("ANTES del pivot (formato long):")
df_sales.groupBy("anio", "region").agg(F.round(F.sum("amount"), 2).alias("total")).show(12)

print("DESPUÉS del pivot (formato wide):")
regiones = [r[0] for r in df_sales.select("region").distinct().collect()]
pivot_tabla = df_sales.groupBy("anio") \
    .pivot("region", regiones) \
    .agg(F.round(F.sum("amount"), 2))
pivot_tabla.orderBy("anio").show()

# ─────────────────────────────────────────
# 2. pivot() con múltiples funciones
# ─────────────────────────────────────────
print("=" * 55)
print("2. pivot() con sum y count")
print("=" * 55)

df_sales.groupBy("anio") \
    .pivot("region", ["Norte", "Sur"]) \
    .agg(
        F.round(F.sum("amount"), 2).alias("total"),
        F.count("sale_id").alias("num")   # pivot no admite count("*") en Spark 4
    ) \
    .orderBy("anio").show()

# ─────────────────────────────────────────
# 3. Unpivot — stack() de columnas a filas
# ─────────────────────────────────────────
print("=" * 55)
print("3. unpivot con stack()")
print("=" * 55)

# Datos simples para el ejemplo
df_wide = spark.createDataFrame([
    (2022, 10000.0, 8000.0, 6000.0),
    (2023, 12000.0, 9500.0, 7200.0),
    (2024, 13500.0, 10000.0, 8000.0),
], ["anio", "Norte", "Sur", "Este"])

print("Formato WIDE (columnas por región):")
df_wide.show()

df_long = df_wide.select(
    "anio",
    F.expr("stack(3, 'Norte', Norte, 'Sur', Sur, 'Este', Este) as (region, total)")
)
print("Formato LONG (filas por región):")
df_long.orderBy("anio", "region").show()

spark.stop()
print("\n✅ Pivot demostrado")
