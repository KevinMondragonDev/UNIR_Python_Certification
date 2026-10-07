"""
FASE 4 — Ejemplo 4: Manipulación de Columnas
==============================================
Aprenderás:
  - withColumn, withColumnRenamed, drop
  - Funciones built-in de strings, fechas y matemáticas
  - cast() para cambiar tipos
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

spark = SparkSession.builder \
    .appName("Manipulacion_Columnas") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

df = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"),
                    header=True, inferSchema=True)

# ─────────────────────────────────────────
# 1. withColumn — agregar nuevas columnas
# ─────────────────────────────────────────
print("=" * 55)
print("1. withColumn() — agregar columnas")
print("=" * 55)

df2 = df \
    .withColumn("salary_anual", F.col("salary") * 12) \
    .withColumn("salary_bono",  F.col("salary") * 0.10) \
    .withColumn("senior",       F.col("salary") > 70000)

df2.select("name", "salary", "salary_anual", "salary_bono", "senior").show(5)

# withColumn reemplaza si la columna ya existe
df_upper = df.withColumn("name", F.upper(F.col("name")))
df_upper.select("name").show(5)

# ─────────────────────────────────────────
# 2. withColumnRenamed — renombrar columnas
# ─────────────────────────────────────────
print("=" * 55)
print("2. withColumnRenamed()")
print("=" * 55)

df_renamed = df \
    .withColumnRenamed("employee_id", "id") \
    .withColumnRenamed("name", "nombre") \
    .withColumnRenamed("salary", "salario")

print(f"Columnas antes: {df.columns}")
print(f"Columnas después: {df_renamed.columns}")
df_renamed.show(3)

# ─────────────────────────────────────────
# 3. drop() — eliminar columnas
# ─────────────────────────────────────────
print("=" * 55)
print("3. drop()")
print("=" * 55)

df_sin_fecha = df.drop("hire_date")
print(f"Sin hire_date: {df_sin_fecha.columns}")

df_minimo = df.drop("hire_date", "country", "employee_id")
print(f"Solo esenciales: {df_minimo.columns}")

# ─────────────────────────────────────────
# 4. Funciones de String
# ─────────────────────────────────────────
print("=" * 55)
print("4. Funciones de String")
print("=" * 55)

df.select(
    "name",
    F.upper("name").alias("mayusculas"),
    F.lower("name").alias("minusculas"),
    F.length("name").alias("longitud"),
    F.trim("name").alias("sin_espacios"),
    F.substring("name", 1, 3).alias("primeras_3"),
    F.split("name", " ").alias("partes"),
    F.concat(F.col("name"), F.lit(" ["), F.col("department"), F.lit("]")).alias("completo")
).show(5, truncate=False)

# ─────────────────────────────────────────
# 5. Funciones de Fecha
# ─────────────────────────────────────────
print("=" * 55)
print("5. Funciones de Fecha")
print("=" * 55)

df_fechas = df.withColumn("hire_date_dt", F.to_date("hire_date", "yyyy-MM-dd"))

df_fechas.select(
    "name",
    "hire_date_dt",
    F.year("hire_date_dt").alias("anio"),
    F.month("hire_date_dt").alias("mes"),
    F.dayofmonth("hire_date_dt").alias("dia"),
    F.dayofweek("hire_date_dt").alias("dia_semana"),
    F.date_format("hire_date_dt", "MMM yyyy").alias("mes_anio"),
    F.datediff(F.current_date(), F.col("hire_date_dt")).alias("dias_trabajando")
).show(5)

# ─────────────────────────────────────────
# 6. Funciones Matemáticas
# ─────────────────────────────────────────
print("=" * 55)
print("6. Funciones Matemáticas")
print("=" * 55)

df.filter(F.col("salary").isNotNull()).select(
    "name",
    "salary",
    F.round(F.col("salary"), -3).alias("salary_redondeado"),
    F.ceil(F.col("salary") / 1000).alias("miles_ceil"),
    F.floor(F.col("salary") / 1000).alias("miles_floor"),
    F.abs(F.col("salary") - 60000).alias("diferencia_media"),
    F.log(F.col("salary")).alias("log_salary"),
).show(5)

# ─────────────────────────────────────────
# 7. Columna literal y when/otherwise
# ─────────────────────────────────────────
print("=" * 55)
print("7. lit() + when/otherwise (CASE WHEN)")
print("=" * 55)

df.withColumn("nivel", F.lit("nuevo")) \
  .select("name", "nivel").show(3)

df.filter(F.col("salary").isNotNull()).withColumn(
    "rango_salario",
    F.when(F.col("salary") < 40000, "Bajo")
     .when(F.col("salary") < 70000, "Medio")
     .when(F.col("salary") < 100000, "Alto")
     .otherwise("Muy Alto")
).select("name", "salary", "rango_salario").show(10)

spark.stop()
print("\n✅ Manipulación de columnas demostrada")
