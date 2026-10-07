"""
FASE 8 — Ejemplo 1: Caching y Persistencia
============================================
Aprenderás a:
  - Usar cache() y persist()
  - Medir el impacto en performance
  - Elegir el StorageLevel correcto
  - Liberar caché
"""

import os
import sys
import time
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel

spark = SparkSession.builder \
    .appName("Caching") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

df_tx    = spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet"))
df_sales = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"), header=True, inferSchema=True)
df_emp   = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True)

# ─────────────────────────────────────────
# 1. Sin caché — recomputa cada vez
# ─────────────────────────────────────────
print("=" * 55)
print("1. Sin caché — recomputa en cada acción")
print("=" * 55)

df_filtrado = df_tx.filter(F.col("status") == "completed") \
                   .filter(F.col("amount") > 100) \
                   .join(df_emp.select("employee_id"), 
                         df_tx.customer_id == df_emp.employee_id, "left")

t0 = time.time()
count1 = df_filtrado.count()
t1 = time.time()
t2 = time.time()
avg1 = df_filtrado.agg(F.avg("amount")).first()[0]
t3 = time.time()
t4 = time.time()
max1 = df_filtrado.agg(F.max("amount")).first()[0]
t5 = time.time()

print(f"count()  → {count1} ({t1-t0:.2f}s)")
print(f"avg()    → {avg1:.2f} ({t3-t2:.2f}s)")
print(f"max()    → {max1:.2f} ({t5-t4:.2f}s)")
print(f"Total sin caché:  {t5-t0:.2f}s")

# ─────────────────────────────────────────
# 2. Con caché — lee una vez, reutiliza
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("2. Con cache() — persist en memoria")
print("=" * 55)

df_filtrado.cache()

t0 = time.time()
count2 = df_filtrado.count()   # primera acción: ejecuta y guarda en caché
t1 = time.time()
t2 = time.time()
avg2 = df_filtrado.agg(F.avg("amount")).first()[0]
t3 = time.time()
t4 = time.time()
max2 = df_filtrado.agg(F.max("amount")).first()[0]
t5 = time.time()

print(f"count()  → {count2} ({t1-t0:.2f}s) ← primera lectura (materializa caché)")
print(f"avg()    → {avg2:.2f} ({t3-t2:.2f}s) ← desde caché")
print(f"max()    → {max2:.2f} ({t5-t4:.2f}s) ← desde caché")
print(f"Total con caché: {t5-t0:.2f}s")

print(f"\n¿Está en caché? {df_filtrado.is_cached}")

# ─────────────────────────────────────────
# 3. Niveles de StorageLevel
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("3. Niveles de StorageLevel")
print("=" * 55)

df_filtrado.unpersist()

print("""
StorageLevel                 RAM   Disco  Serializado  Réplicas
─────────────────────────────────────────────────────────────
MEMORY_ONLY                  ✓     ✗      ✗            1
MEMORY_AND_DISK              ✓     ✓      ✗            1   ← default cache()
MEMORY_ONLY_SER              ✓     ✗      ✓            1
MEMORY_AND_DISK_SER          ✓     ✓      ✓            1
DISK_ONLY                    ✗     ✓      ✓            1
MEMORY_AND_DISK_2            ✓     ✓      ✗            2   ← replicado
OFF_HEAP                     Heap externo (Tungsten)
""")

# Usar persist() con nivel específico
df_sales_cached = df_sales.filter(F.col("amount") > 500) \
                           .persist(StorageLevel.MEMORY_AND_DISK)
print(f"StorageLevel usado: {df_sales_cached.storageLevel}")

_ = df_sales_cached.count()  # materializar

# ─────────────────────────────────────────
# 4. Cuándo usar caché
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("4. Patrón correcto de uso de caché")
print("=" * 55)

print("""
USAR cache() cuando:
  ✓ El DF se usa en múltiples acciones (count + show + write)
  ✓ Es resultado de un join o transformación costosa
  ✓ Se itera sobre el mismo DF en un loop

NO usar cache() cuando:
  ✗ El DF solo se usa una vez
  ✗ El DF cabe en memoria pero es simple de recomputar
  ✗ El DF es muy grande y puede agotar la RAM
""")

# Patrón: cache + múltiples operaciones + unpersist
df_analisis = df_tx.filter(F.col("status").isin("completed", "refunded")) \
                   .withColumn("anio", F.year("ts")) \
                   .cache()

print("Usando df_analisis en múltiples queries:")
print(f"  Total registros: {df_analisis.count()}")
print(f"  Monto promedio: {df_analisis.agg(F.avg('amount')).first()[0]:.2f}")
print(f"  Monedas únicas: {df_analisis.select('currency').distinct().count()}")

df_analisis.unpersist()
df_sales_cached.unpersist()

print(f"\nCaché liberada. ¿En caché? {df_analisis.is_cached}")

spark.stop()
print("\n✅ Caching y persistencia demostrados")
