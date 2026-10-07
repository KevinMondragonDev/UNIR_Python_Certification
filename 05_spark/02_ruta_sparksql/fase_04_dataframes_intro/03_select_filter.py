"""
FASE 4 — Ejemplo 3: select() y filter()
=========================================
Aprenderás a:
  - Usar select() de diferentes formas
  - Filtrar con condiciones simples y complejas
  - Combinar select y filter en pipelines
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Select_Filter") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

df = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"),
                    header=True, inferSchema=True)

print(f"Dataset: {df.count()} filas, {len(df.columns)} columnas")
df.printSchema()

# ─────────────────────────────────────────
# 1. select() — básico
# ─────────────────────────────────────────
print("=" * 55)
print("1. select() — formas básicas")
print("=" * 55)

# Por nombre de columna (string)
df.select("name", "department").show(5)

# Por col()
df.select(F.col("name"), F.col("salary")).show(5)

# Calcular nueva columna en select
df.select("name", "salary",
          (F.col("salary") * 1.10).alias("salario_aumento")).show(5)

# SQL inline con expr()
df.select(F.expr("name"), F.expr("salary * 12 as salario_anual")).show(5)

# Seleccionar todas las columnas
df.select("*").show(3)

# Excluir columnas
cols_sin_id = [c for c in df.columns if c != "employee_id"]
df.select(cols_sin_id).show(3)

# ─────────────────────────────────────────
# 2. filter() / where() — condiciones simples
# ─────────────────────────────────────────
print("=" * 55)
print("2. filter() — condiciones simples")
print("=" * 55)

# Numérico
df.filter(F.col("salary") > 80000).select("name", "salary").show(5)

# String — SQL inline
df.filter("department = 'Engineering'").select("name", "department").show(5)

# String con like
df.filter(F.col("name").like("Ana%")).select("name").show(5)

# isin
df.filter(F.col("country").isin("Mexico", "USA")).select("name", "country").show(5)

# between
df.filter(F.col("salary").between(50000, 70000)).select("name", "salary").show(5)

# ─────────────────────────────────────────
# 3. filter() — condiciones múltiples
# ─────────────────────────────────────────
print("=" * 55)
print("3. filter() — condiciones múltiples")
print("=" * 55)

# AND
df.filter(
    (F.col("salary") > 60000) & (F.col("department") == "Engineering")
).select("name", "salary", "department").show(5)

# OR
df.filter(
    (F.col("country") == "Mexico") | (F.col("country") == "USA")
).select("name", "country").show(5)

# NOT
df.filter(~(F.col("department") == "HR")).select("name", "department").show(5)

# Nulos
print(f"Empleados con salary nulo: {df.filter(F.col('salary').isNull()).count()}")
df.filter(F.col("salary").isNull()).select("name", "salary").show(5)

# ─────────────────────────────────────────
# 4. Encadenar select + filter
# ─────────────────────────────────────────
print("=" * 55)
print("4. Pipeline: filter → select → ordenar")
print("=" * 55)

resultado = df \
    .filter(F.col("salary").isNotNull()) \
    .filter(F.col("salary") > 70000) \
    .filter(F.col("department").isin("Engineering", "Finance")) \
    .select("name", "department", "salary", "country") \
    .orderBy(F.col("salary").desc())

print(f"Empleados bien pagados en Eng/Finance: {resultado.count()}")
resultado.show(10)

# ─────────────────────────────────────────
# 5. distinct() y dropDuplicates()
# ─────────────────────────────────────────
print("=" * 55)
print("5. distinct() y dropDuplicates()")
print("=" * 55)

# Todos los países únicos
paises = df.select("country").distinct().orderBy("country")
paises.show()

# dropDuplicates por columnas específicas
df.dropDuplicates(["department", "country"]) \
  .select("department", "country") \
  .orderBy("department", "country") \
  .show()

# ─────────────────────────────────────────
# 6. limit() y orderBy()
# ─────────────────────────────────────────
print("=" * 55)
print("6. orderBy() y limit()")
print("=" * 55)

df.filter(F.col("salary").isNotNull()) \
  .orderBy(F.col("salary").desc()) \
  .select("name", "salary", "department") \
  .limit(10) \
  .show()

spark.stop()
print("\n✅ select() y filter() demostrados")
