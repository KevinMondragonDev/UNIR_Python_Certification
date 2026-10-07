"""
FASE 4 — Ejemplo 2: Schema, Tipos y Casting
=============================================
Aprenderás a:
  - Inspeccionar el schema
  - Castear columnas a otros tipos
  - Manejar tipos de datos complejos
  - Inferir vs definir schema
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DoubleType, DateType, TimestampType, LongType, BooleanType
)

spark = SparkSession.builder \
    .appName("Schema_y_Tipos") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

# ─────────────────────────────────────────
# 1. Inspección del schema
# ─────────────────────────────────────────
print("=" * 55)
print("1. Inspección del schema")
print("=" * 55)

df = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"),
                    header=True, inferSchema=True)

print("--- printSchema() ---")
df.printSchema()

print("--- dtypes (lista de tuplas) ---")
print(df.dtypes)

print("\n--- columns (lista de nombres) ---")
print(df.columns)

print("\n--- schema (StructType) ---")
print(df.schema)

# Acceder a tipo de una columna específica
tipo_salario = dict(df.dtypes)["salary"]
print(f"\nTipo de 'salary': {tipo_salario}")

# ─────────────────────────────────────────
# 2. Casting de tipos
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("2. Casting — cast()")
print("=" * 55)

# Problema: salary puede leerse como string
df_str = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"),
                        header=True, inferSchema=False)  # todo como string
print("Schema con inferSchema=False (todo STRING):")
df_str.printSchema()

# Cast a tipos correctos
df_cast = df_str.withColumn("employee_id", F.col("employee_id").cast(IntegerType())) \
                .withColumn("salary",       F.col("salary").cast(DoubleType()))

print("\nDespués de casting:")
df_cast.select("employee_id", "salary").printSchema()
df_cast.select("employee_id", "salary").show(5)

# ─────────────────────────────────────────
# 3. Fechas y Timestamps
# ─────────────────────────────────────────
print("=" * 55)
print("3. Tipos de fecha")
print("=" * 55)

df_dates = df.withColumn("hire_date_dt", F.to_date(F.col("hire_date"), "yyyy-MM-dd")) \
             .withColumn("hire_year",    F.year(F.col("hire_date"))) \
             .withColumn("hire_month",   F.month(F.col("hire_date")))

df_dates.select("name", "hire_date", "hire_date_dt", "hire_year", "hire_month").show(5)

# Timestamps desde parquet
df_tx = spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet"))
df_tx.select("ts").printSchema()
df_tx.withColumn("year", F.year("ts")) \
     .withColumn("hour", F.hour("ts")) \
     .select("ts", "year", "hour") \
     .show(5)

# ─────────────────────────────────────────
# 4. Nullable — campos que pueden ser nulos
# ─────────────────────────────────────────
print("=" * 55)
print("4. Nullable y nulos")
print("=" * 55)

print(f"Filas con salary nulo: {df.filter(F.col('salary').isNull()).count()}")
print(f"Filas con salary no nulo: {df.filter(F.col('salary').isNotNull()).count()}")

# ─────────────────────────────────────────
# 5. Tipos complejos — Array (desde parquet de events)
# ─────────────────────────────────────────
print("=" * 55)
print("5. ArrayType — columna de listas")
print("=" * 55)

df_ev = spark.read.parquet(os.path.join(PARQ_DIR, "events.parquet"))
df_ev.printSchema()
df_ev.show(3, truncate=False)

# Acceder a elementos del array
df_ev.select(
    "event_id",
    "tags",
    F.col("tags")[0].alias("primer_tag"),
    F.size("tags").alias("num_tags")
).show(5)

# Explotar array (una fila por elemento)
df_ev.select("event_id", F.explode("tags").alias("tag")).show(8)

# ─────────────────────────────────────────
# 6. Estadísticas descriptivas
# ─────────────────────────────────────────
print("=" * 55)
print("6. describe() y summary()")
print("=" * 55)

df.select("salary", "employee_id").describe().show()
df.select("salary").summary().show()

spark.stop()
print("\n✅ Schema y tipos demostrados")
