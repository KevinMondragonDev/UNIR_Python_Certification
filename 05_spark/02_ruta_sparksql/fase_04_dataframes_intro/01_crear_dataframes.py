"""
FASE 4 — Ejemplo 1: Crear DataFrames
======================================
Aprenderás todas las formas de crear DataFrames en PySpark.
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DoubleType, DateType, TimestampType
)

spark = SparkSession.builder \
    .appName("Crear_DataFrames") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

# ─────────────────────────────────────────
# 1. Desde lista de listas con nombres de columnas
# ─────────────────────────────────────────
print("=" * 55)
print("1. Desde lista + nombres de columnas")
print("=" * 55)

datos = [(1, "Ana", 50000.0), (2, "Luis", 35000.0), (3, "Maria", 72000.0)]
df1 = spark.createDataFrame(datos, ["id", "nombre", "salario"])
df1.printSchema()
df1.show()

# ─────────────────────────────────────────
# 2. Con StructType (schema explícito)
# ─────────────────────────────────────────
print("=" * 55)
print("2. Con StructType — schema explícito")
print("=" * 55)

schema = StructType([
    StructField("id",      IntegerType(), nullable=False),
    StructField("nombre",  StringType(),  nullable=True),
    StructField("salario", DoubleType(),  nullable=True),
])
df2 = spark.createDataFrame(datos, schema)
df2.printSchema()
df2.show()

# ─────────────────────────────────────────
# 3. Schema como string DDL (más corto)
# ─────────────────────────────────────────
print("=" * 55)
print("3. Schema como DDL string")
print("=" * 55)

schema_ddl = "id INT NOT NULL, nombre STRING, salario DOUBLE"
df3 = spark.createDataFrame(datos, schema_ddl)
df3.printSchema()

# ─────────────────────────────────────────
# 4. Desde RDD con toDF()
# ─────────────────────────────────────────
print("=" * 55)
print("4. Desde RDD con toDF()")
print("=" * 55)

rdd = sc.parallelize([(10, "x"), (20, "y"), (30, "z")])
df4 = rdd.toDF(["numero", "letra"])
df4.show()

# ─────────────────────────────────────────
# 5. Desde CSV con inferSchema
# ─────────────────────────────────────────
print("=" * 55)
print("5. Desde CSV — inferSchema=True")
print("=" * 55)

df_emp = spark.read.csv(
    os.path.join(CSV_DIR, "employees.csv"),
    header=True,
    inferSchema=True
)
df_emp.printSchema()
print(f"Filas: {df_emp.count()}, Columnas: {len(df_emp.columns)}")
df_emp.show(5)

# ─────────────────────────────────────────
# 6. Desde CSV con schema manual (más robusto)
# ─────────────────────────────────────────
print("=" * 55)
print("6. Desde CSV — schema manual")
print("=" * 55)

emp_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name",        StringType(),  True),
    StructField("department",  StringType(),  True),
    StructField("salary",      DoubleType(),  True),
    StructField("hire_date",   StringType(),  True),
    StructField("country",     StringType(),  True),
])
df_emp2 = spark.read.csv(
    os.path.join(CSV_DIR, "employees.csv"),
    header=True,
    schema=emp_schema
)
df_emp2.printSchema()
df_emp2.show(3)

# ─────────────────────────────────────────
# 7. Desde Parquet (preserva schema automáticamente)
# ─────────────────────────────────────────
print("=" * 55)
print("7. Desde Parquet — schema automático")
print("=" * 55)

df_tx = spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet"))
df_tx.printSchema()
print(f"Filas: {df_tx.count()}")
df_tx.show(3, truncate=True)

# ─────────────────────────────────────────
# 8. spark.range() — forma rápida para testing
# ─────────────────────────────────────────
print("=" * 55)
print("8. spark.range() — para testing rápido")
print("=" * 55)

spark.range(5).show()
spark.range(0, 20, 2).show()  # de 0 a 20 de 2 en 2

# ─────────────────────────────────────────
# 9. Guardar DataFrame (write)
# ─────────────────────────────────────────
print("=" * 55)
print("9. Guardar DataFrame")
print("=" * 55)

output = "/tmp/df_prueba_fase4"
import shutil
if os.path.exists(output):
    shutil.rmtree(output)

df_emp2.filter("salary > 80000").write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output + "_csv")

df_emp2.write \
    .mode("overwrite") \
    .parquet(output + "_parquet")

print(f"✓ CSV guardado en:     {output}_csv")
print(f"✓ Parquet guardado en: {output}_parquet")

spark.stop()
print("\n✅ Creación de DataFrames demostrada")
