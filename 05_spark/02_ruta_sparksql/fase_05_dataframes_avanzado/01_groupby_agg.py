"""
FASE 5 — Ejemplo 1: groupBy y Agregaciones
============================================
Aprenderás a:
  - groupBy con una y varias columnas
  - Múltiples funciones de agregación en agg()
  - HAVING (filter sobre groupBy)
  - rollup y cube
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("GroupBy_Agg") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

df_emp  = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True)
df_sales = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"),    header=True, inferSchema=True)
df_prod  = spark.read.csv(os.path.join(CSV_DIR, "products.csv"), header=True, inferSchema=True)

df_emp_clean = df_emp.filter(F.col("salary").isNotNull())

# ─────────────────────────────────────────
# 1. groupBy básico + count
# ─────────────────────────────────────────
print("=" * 55)
print("1. groupBy + count")
print("=" * 55)

df_emp_clean.groupBy("department") \
    .count() \
    .orderBy(F.col("count").desc()) \
    .show()

# ─────────────────────────────────────────
# 2. Múltiples funciones en agg()
# ─────────────────────────────────────────
print("=" * 55)
print("2. Múltiples funciones en agg()")
print("=" * 55)

df_emp_clean.groupBy("department").agg(
    F.count("*").alias("total_empleados"),
    F.avg("salary").alias("salario_promedio"),
    F.max("salary").alias("salario_max"),
    F.min("salary").alias("salario_min"),
    F.sum("salary").alias("masa_salarial"),
    F.stddev("salary").alias("desv_std"),
    F.countDistinct("country").alias("paises_distintos")
).orderBy(F.col("salario_promedio").desc()) \
 .select("department", "total_empleados", "salario_promedio",
         F.round("salario_promedio", 2).alias("avg_redondeado"),
         "salario_max", "salario_min") \
 .show()

# ─────────────────────────────────────────
# 3. groupBy con múltiples columnas
# ─────────────────────────────────────────
print("=" * 55)
print("3. groupBy múltiples columnas")
print("=" * 55)

df_emp_clean.groupBy("department", "country").agg(
    F.count("*").alias("count"),
    F.avg("salary").alias("avg_salary")
).orderBy("department", F.col("avg_salary").desc()).show(15)

# ─────────────────────────────────────────
# 4. HAVING — filtrar resultados del groupBy
# ─────────────────────────────────────────
print("=" * 55)
print("4. HAVING (filter después de groupBy)")
print("=" * 55)

df_emp_clean.groupBy("department").agg(
    F.count("*").alias("total"),
    F.avg("salary").alias("avg_salary")
).filter(F.col("total") >= 30) \
 .filter(F.col("avg_salary") > 60000) \
 .orderBy(F.col("avg_salary").desc()) \
 .show()

# ─────────────────────────────────────────
# 5. collect_list y collect_set
# ─────────────────────────────────────────
print("=" * 55)
print("5. collect_list y collect_set")
print("=" * 55)

df_emp_clean.groupBy("department").agg(
    F.collect_set("country").alias("paises"),
    F.count("*").alias("total")
).show(truncate=False)

# ─────────────────────────────────────────
# 6. Ventas: total y promedio por región
# ─────────────────────────────────────────
print("=" * 55)
print("6. Análisis de ventas por región")
print("=" * 55)

df_sales.groupBy("region").agg(
    F.count("*").alias("num_ventas"),
    F.sum("amount").alias("total_monto"),
    F.avg("amount").alias("monto_promedio"),
    F.max("amount").alias("venta_max"),
    F.min("amount").alias("venta_min")
).orderBy(F.col("total_monto").desc()).show()

# ─────────────────────────────────────────
# 7. rollup — subtotales jerárquicos
# ─────────────────────────────────────────
print("=" * 55)
print("7. rollup() — subtotales")
print("=" * 55)

df_emp_clean.rollup("department", "country").agg(
    F.count("*").alias("total"),
    F.avg("salary").alias("avg_sal")
).orderBy("department", "country").show(20)

spark.stop()
print("\n✅ groupBy y Agregaciones demostradas")
