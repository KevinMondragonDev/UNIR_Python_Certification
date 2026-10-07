import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, avg, max, min, count, round

spark = SparkSession.builder.appName("ParquetDemo").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ── 1. Crear datos de ejemplo y guardar como Parquet ──────────────────────────

datos = [
    (1, "Ana",      "Ventas",     55000.0),
    (2, "Carlos",   "IT",         72000.0),
    (3, "María",    "Ventas",     61000.0),
    (4, "Luis",     "IT",         68000.0),
    (5, "Sofía",    "RRHH",       48000.0),
    (6, "Pedro",    "IT",         75000.0),
    (7, "Elena",    "Ventas",     59000.0),
    (8, "Miguel",   "RRHH",       51000.0),
]

esquema = StructType([
    StructField("id",           IntegerType(), False),
    StructField("nombre",       StringType(),  False),
    StructField("departamento", StringType(),  False),
    StructField("salario",      DoubleType(),  False),
])

df_origen = spark.createDataFrame(datos, schema=esquema)

PARQUET_PATH = "empleados.parquet"
df_origen.write.mode("overwrite").parquet(PARQUET_PATH)
print(f"\n✔ Archivo Parquet guardado en: {PARQUET_PATH}\n")

# ── 2. Leer el archivo Parquet ────────────────────────────────────────────────

df = spark.read.parquet(PARQUET_PATH)

print("=== Schema inferido desde Parquet ===")
df.printSchema()

print("=== Datos completos ===")
df.show()

# ── 3. Filtrar empleados con salario > 60,000 ─────────────────────────────────

print("=== Empleados con salario > 60,000 ===")
df.filter(col("salario") > 60000).show()

# ── 4. Agrupar por departamento: promedio, máx y mín de salario ───────────────

print("=== Estadísticas de salario por departamento ===")
df.groupBy("departamento").agg(
    count("id").alias("total_empleados"),
    round(avg("salario"), 2).alias("salario_promedio"),
    max("salario").alias("salario_maximo"),
    min("salario").alias("salario_minimo"),
).orderBy("departamento").show()

# ── 5. Seleccionar columnas específicas y ordenar ─────────────────────────────

print("=== Empleados ordenados por salario (descendente) ===")
df.select("nombre", "departamento", "salario") \
  .orderBy(col("salario").desc()) \
  .show()

spark.stop()
