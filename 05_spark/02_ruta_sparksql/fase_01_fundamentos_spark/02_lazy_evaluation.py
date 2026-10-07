"""
FASE 1 — Ejemplo 2: Lazy Evaluation y DAG
==========================================
Aprenderás a:
  - Ver qué pasa cuando defines transformaciones (NADA)
  - Ver qué pasa cuando llamas una acción (SE EJECUTA TODO)
  - Inspeccionar el plan de ejecución con explain()
  - Entender el concepto de Job, Stage y Task
"""

import os
import sys
import time
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Lazy_Evaluation") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ─────────────────────────────────────────
# 1. Demostración: Las transformaciones NO ejecutan
# ─────────────────────────────────────────
print("=" * 55)
print("PASO 1: Definiendo transformaciones (sin ejecutar)")
print("=" * 55)

t0 = time.time()
df = spark.range(0, 10_000_000)           # 10 millones de números
df_filtrado = df.filter(F.col("id") % 2 == 0)   # filtrar pares
df_calculado = df_filtrado.withColumn("cuadrado", F.col("id") ** 2)
t1 = time.time()

print(f"Tiempo en definir transformaciones: {t1 - t0:.4f} seg")
print("→ Casi 0 segundos porque Spark NO ejecutó nada todavía")
print(f"  Tipo de objeto: {type(df_calculado)}")

# ─────────────────────────────────────────
# 2. Las acciones SÍ ejecutan
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("PASO 2: Llamando acción count() — ahora SÍ ejecuta")
print("=" * 55)

t0 = time.time()
resultado = df_calculado.count()
t1 = time.time()

print(f"Resultado: {resultado:,} números pares")
print(f"Tiempo de ejecución: {t1 - t0:.2f} seg")
print("→ Aquí Spark ejecutó TODAS las transformaciones previas")

# ─────────────────────────────────────────
# 3. El plan de ejecución — explain()
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("PASO 3: Plan de ejecución (lo que Spark hará)")
print("=" * 55)

df_pequeno = spark.range(100) \
    .filter(F.col("id") > 10) \
    .withColumn("doble", F.col("id") * 2) \
    .select("id", "doble")

print("--- Plan físico (simple) ---")
df_pequeno.explain()

print("--- Plan extendido (logical + physical) ---")
df_pequeno.explain(extended=True)

# ─────────────────────────────────────────
# 4. Transformaciones vs Acciones
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("PASO 4: Catálogo — Transformaciones vs Acciones")
print("=" * 55)

print("""
TRANSFORMACIONES (lazy — construyen el DAG):
  DataFrame → DataFrame
  ─────────────────────────────────────────
  select()      filter()/where()   groupBy()
  withColumn()  join()             union()
  orderBy()     drop()             distinct()
  limit()       sample()           repartition()

ACCIONES (eager — disparan la ejecución):
  DataFrame → resultado
  ─────────────────────────────────────────
  show()        collect()          count()
  first()       take(n)            head(n)
  write.*()     foreach()          toPandas()
""")

# ─────────────────────────────────────────
# 5. DAG — cada acción crea un Job
# ─────────────────────────────────────────
print("=" * 55)
print("PASO 5: Múltiples acciones = múltiples Jobs")
print("=" * 55)

datos = spark.range(1000).withColumn("val", F.rand(seed=42))

print("Acción 1 — count:")
print(f"  Total filas: {datos.count()}")

print("Acción 2 — show (primeras 3):")
datos.show(3)

print("Acción 3 — first:")
print(f"  Primera fila: {datos.first()}")

print("\n→ Cada .count(), .show(), .first() es un Job separado")
print("  Puedes verlos en Spark UI: http://localhost:4040/jobs")

spark.stop()
print("\n✅ Lazy evaluation demostrada correctamente")
