"""
FASE 5 — Ejemplo 4: Manejo de Nulos
=====================================
Aprenderás:
  - Detectar nulos
  - dropna, fillna, replace
  - coalesce para valores por defecto
  - Estrategias de limpieza
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Manejo_Nulos") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

df = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True)
df_cust = spark.read.csv(os.path.join(CSV_DIR, "customers.csv"), header=True, inferSchema=True)

# ─────────────────────────────────────────
# 1. Detectar nulos
# ─────────────────────────────────────────
print("=" * 55)
print("1. Detectar nulos por columna")
print("=" * 55)

print("Conteo de nulos por columna:")
df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df.columns
]).show()

print("Porcentaje de nulos:")
total = df.count()
df.select([
    F.round(F.count(F.when(F.col(c).isNull(), c)) / total * 100, 2).alias(c)
    for c in df.columns
]).show()

# ─────────────────────────────────────────
# 2. dropna — eliminar filas con nulos
# ─────────────────────────────────────────
print("=" * 55)
print("2. dropna()")
print("=" * 55)

print(f"Total filas: {df.count()}")

# Eliminar si CUALQUIER columna es nula
df_sin_nulos = df.dropna()
print(f"Tras dropna() completo: {df_sin_nulos.count()}")

# Eliminar si TODAS las columnas son nulas
df_sin_todo_nulo = df.dropna(how="all")
print(f"Tras dropna(how='all'): {df_sin_todo_nulo.count()}")

# Eliminar solo si salary o email es nulo
df_sin_nulo_salary = df.dropna(subset=["salary"])
print(f"Tras dropna(subset=['salary']): {df_sin_nulo_salary.count()}")

# Mínimo N valores no-nulos
df_min_vals = df.dropna(thresh=5)  # al menos 5 columnas no-nulas
print(f"Tras dropna(thresh=5): {df_min_vals.count()}")

# ─────────────────────────────────────────
# 3. fillna — rellenar nulos
# ─────────────────────────────────────────
print("=" * 55)
print("3. fillna()")
print("=" * 55)

# Rellenar todo con un solo valor (solo aplica a columnas del tipo correcto)
df_fill_0 = df.fillna(0)
print(f"salary nulos tras fillna(0): {df_fill_0.filter(F.col('salary').isNull()).count()}")

# Rellenar por columna específica
df_fill_dict = df.fillna({
    "salary": 0.0,
    "name": "DESCONOCIDO",
    "department": "SIN_DEPT",
    "country": "UNKNOWN"
})
print("Schema tras fillna por columna:")
df_fill_dict.filter(F.col("salary") == 0.0).select("name", "salary", "department").show(5)

# Rellenar con el promedio
avg_salary = df.filter(F.col("salary").isNotNull()).agg(F.avg("salary")).first()[0]
print(f"\nPromedio de salary: ${avg_salary:,.2f}")
df_fill_avg = df.fillna({"salary": round(avg_salary, 2)})
print(f"Nulos en salary tras fillna(avg): {df_fill_avg.filter(F.col('salary').isNull()).count()}")

# ─────────────────────────────────────────
# 4. coalesce — primer valor no-nulo
# ─────────────────────────────────────────
print("=" * 55)
print("4. coalesce() — primer valor no-nulo")
print("=" * 55)

df_coalesce = df.withColumn(
    "salary_limpio",
    F.coalesce(F.col("salary"), F.lit(avg_salary))
)
print(f"Nulos en salary_limpio: {df_coalesce.filter(F.col('salary_limpio').isNull()).count()}")
df_coalesce.filter(F.col("salary").isNull()) \
           .select("name", "salary", "salary_limpio").show(5)

# ─────────────────────────────────────────
# 5. replace — reemplazar valores específicos
# ─────────────────────────────────────────
print("=" * 55)
print("5. replace()")
print("=" * 55)

df_cust_clean = df_cust.replace("", None, subset=["email"])
print(f"Emails nulos tras replace('', None): {df_cust_clean.filter(F.col('email').isNull()).count()}")

# ─────────────────────────────────────────
# 6. Estrategia completa de limpieza
# ─────────────────────────────────────────
print("=" * 55)
print("6. Pipeline de limpieza completo")
print("=" * 55)

df_clean = df \
    .fillna({"salary": avg_salary, "country": "UNKNOWN"}) \
    .withColumn("name", F.when(F.col("name").isNull(), F.lit("ANÓNIMO")).otherwise(F.col("name"))) \
    .withColumn("salary_flag", F.col("salary").isNull().cast("int")) \
    .dropna(subset=["department"])

print(f"Dataset limpio: {df_clean.count()} filas")
print("Nulos restantes:")
df_clean.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_clean.columns
]).show()

spark.stop()
print("\n✅ Manejo de nulos demostrado")
