"""
FASE 8 — Ejemplo 2: Particionamiento y Shuffle
================================================
Aprenderás a:
  - Qué es una partición y por qué es la UNIDAD de paralelismo
  - Diferenciar transformaciones NARROW vs WIDE (shuffle)
  - Ver los Stages que genera un shuffle (Exchange en el plan)
  - repartition() vs coalesce() y cuándo usar cada uno
  - Ajustar spark.sql.shuffle.partitions
  - Trabajar por partición con mapPartitions()
  - Escribir datos particionados en disco con partitionBy()

Idea central:
  1 partición  →  1 task  →  1 core
  Si tienes 8 cores y 2 particiones, 6 cores están OCIOSOS.
"""

import os
import sys
import time
import shutil
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Particionamiento") \
    .master("local[4]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
# AQE desactivado para que veas el comportamiento "puro" del shuffle.
# En 05_optimizacion_joins.py verás qué cambia al activarlo.

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")
OUT_DIR  = os.path.join(BASE, "fase_08_optimizacion_y_udfs", "output_particiones")

df_tx    = spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet"))
df_sales = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"), header=True, inferSchema=True)


def filas_por_particion(df):
    """Devuelve una lista con cuántas filas tiene cada partición."""
    return df.rdd.glom().map(len).collect()


# ─────────────────────────────────────────
# 1. ¿Qué es una partición?
# ─────────────────────────────────────────
print("=" * 60)
print("1. Particiones: la unidad de paralelismo")
print("=" * 60)

print(f"Cores disponibles (defaultParallelism): {spark.sparkContext.defaultParallelism}")
print(f"Particiones de transactions.parquet:   {df_tx.rdd.getNumPartitions()}")
print(f"Filas por partición:                   {filas_por_particion(df_tx)}")

print("""
Cada partición se procesa en una TASK independiente, en un core de un executor.
  - Pocas particiones  → cores ociosos, tareas enormes, riesgo de OOM
  - Demasiadas         → overhead de scheduling (miles de tasks de 1 KB)
  - Regla práctica     → 2-4 particiones por core, ~128 MB cada una
""")

# ─────────────────────────────────────────
# 2. Narrow vs Wide
# ─────────────────────────────────────────
print("=" * 60)
print("2. Transformaciones NARROW vs WIDE")
print("=" * 60)

print("""
NARROW (sin shuffle): cada partición de salida depende de UNA de entrada.
    filter, select, withColumn, map, union, coalesce
    → Spark las encadena en un mismo Stage (pipelining).

WIDE (con shuffle): una partición de salida necesita datos de VARIAS de entrada.
    groupBy, join, orderBy, distinct, repartition, window (partitionBy)
    → Spark corta el Stage, escribe a disco y mueve datos por la red.

     Stage 1 (narrow)                     Stage 2
  ┌─────────────────────┐   SHUFFLE   ┌──────────────┐
  │ P0: filter→withCol  │──┐       ┌─►│ P0: agg      │
  │ P1: filter→withCol  │──┼───X───┼─►│ P1: agg      │
  │ P2: filter→withCol  │──┘       └─►│ P2: agg      │
  └─────────────────────┘             └──────────────┘
""")

df_narrow = df_tx.filter(F.col("amount") > 100) \
                 .withColumn("monto_usd", F.round(F.col("amount") / 18, 2)) \
                 .select("tx_id", "status", "monto_usd")

print("Plan NARROW (no verás 'Exchange'):")
df_narrow.explain()

df_wide = df_narrow.groupBy("status").agg(F.sum("monto_usd").alias("total"))
print("Plan WIDE (busca 'Exchange hashpartitioning' = SHUFFLE):")
df_wide.explain()

print("Particiones antes del groupBy:", df_narrow.rdd.getNumPartitions())
print("Particiones después del groupBy:", df_wide.rdd.getNumPartitions(),
      "← viene de spark.sql.shuffle.partitions")

# ─────────────────────────────────────────
# 3. repartition() vs coalesce()
# ─────────────────────────────────────────
print("\n" + "=" * 60)
print("3. repartition() vs coalesce()")
print("=" * 60)

df_rep = df_tx.repartition(8)
print(f"repartition(8) → {df_rep.rdd.getNumPartitions()} particiones")
print(f"  filas por partición: {filas_por_particion(df_rep)}  ← balanceado (round-robin)")

df_coal = df_rep.coalesce(3)
print(f"coalesce(3)    → {df_coal.rdd.getNumPartitions()} particiones")
print(f"  filas por partición: {filas_por_particion(df_coal)}  ← junta particiones vecinas, puede desbalancear")

df_rep_col = df_tx.repartition(4, "status")
print(f"repartition(4, 'status') → filas: {filas_por_particion(df_rep_col)}")
print("  ← hash por columna: todas las filas del mismo status en la misma partición")
print("  ⚠️ 4 valores de status y 4 particiones NO garantiza 1 valor por partición:")
print("     hash(status) % 4 puede colisionar → particiones vacías y otras dobles (= skew)")

print("\nPlanes comparados:")
print("  repartition(8):")
df_tx.repartition(8).explain()
print("  coalesce(1):")
df_tx.coalesce(1).explain()

print("""
┌──────────────────┬───────────────┬─────────────────────────────────────┐
│                  │ repartition   │ coalesce                            │
├──────────────────┼───────────────┼─────────────────────────────────────┤
│ Shuffle          │ SÍ (Exchange) │ NO (solo combina particiones)       │
│ Subir particiones│ SÍ            │ NO (coalesce(100) sobre 4 = 4)      │
│ Balanceo         │ Uniforme      │ Puede quedar desbalanceado          │
│ Uso típico       │ Antes de join/│ Antes de escribir, para no generar  │
│                  │ agg pesada    │ miles de archivos pequeños          │
└──────────────────┴───────────────┴─────────────────────────────────────┘
""")

# ⚠️ coalesce(N) mayor que el actual NO hace nada:
print(f"coalesce(100) sobre {df_tx.rdd.getNumPartitions()} particiones → "
      f"{df_tx.coalesce(100).rdd.getNumPartitions()}")

# ─────────────────────────────────────────
# 4. spark.sql.shuffle.partitions
# ─────────────────────────────────────────
print("\n" + "=" * 60)
print("4. spark.sql.shuffle.partitions (default = 200)")
print("=" * 60)

for n in [1, 4, 50, 200]:
    spark.conf.set("spark.sql.shuffle.partitions", str(n))
    t0 = time.time()
    res = df_sales.groupBy("region", "product_id").agg(F.sum("amount").alias("total"))
    res.count()
    print(f"  shuffle.partitions={n:<4} → {res.rdd.getNumPartitions():<4} particiones, "
          f"{time.time() - t0:.2f}s")

spark.conf.set("spark.sql.shuffle.partitions", "4")
print("""
Con 2.000 filas, 200 particiones = 200 tasks casi vacías → más lento.
Con 2 TB, 4 particiones = 500 GB por task → OOM.
Fórmula orientativa: tamaño_shuffle / 128 MB  (y múltiplo del nº de cores).
En Spark 3+, AQE ajusta esto automáticamente (ver 05_optimizacion_joins.py).
""")

# ─────────────────────────────────────────
# 5. mapPartitions — trabajo por partición
# ─────────────────────────────────────────
print("=" * 60)
print("5. mapPartitions(): inicializar recursos UNA vez por partición")
print("=" * 60)

def procesar_particion(filas):
    # Aquí abrirías una conexión a BD, cargarías un modelo, etc.
    # Con map() lo harías por CADA fila; con mapPartitions, una vez por partición.
    conexion_simulada = {"tasa_usd": 18.0}
    total, cuenta = 0.0, 0
    for fila in filas:
        total += fila["amount"] / conexion_simulada["tasa_usd"]
        cuenta += 1
    yield (cuenta, round(total, 2))

resumen = df_tx.rdd.mapPartitions(procesar_particion).collect()
print(f"(filas, total_usd) por partición: {resumen}")

# ─────────────────────────────────────────
# 6. partitionBy() al escribir (particionado en DISCO)
# ─────────────────────────────────────────
print("\n" + "=" * 60)
print("6. partitionBy(): particionado físico en disco")
print("=" * 60)

if os.path.exists(OUT_DIR):
    shutil.rmtree(OUT_DIR)

df_tx.withColumn("anio", F.year("ts")) \
     .repartition("anio") \
     .write.mode("overwrite") \
     .partitionBy("anio", "status") \
     .parquet(OUT_DIR)
# repartition("anio") antes de partitionBy → 1 archivo por carpeta, no N*M archivos pequeños

for raiz, dirs, archivos in sorted(os.walk(OUT_DIR)):
    nivel = raiz.replace(OUT_DIR, "").count(os.sep)
    if nivel <= 2 and raiz != OUT_DIR:
        parquets = [a for a in archivos if a.endswith(".parquet")]
        print(f"{'  ' * nivel}{os.path.basename(raiz)}/  ({len(parquets)} archivos)")

df_leido = spark.read.parquet(OUT_DIR).filter((F.col("anio") == 2024) & (F.col("status") == "completed"))
print("\nPartition pruning — busca 'PartitionFilters' en el plan:")
df_leido.explain()
print(f"Filas leídas: {df_leido.count()} (Spark solo abrió anio=2024/status=completed)")

shutil.rmtree(OUT_DIR)

print("""
RESUMEN
  • Partición = task. Más particiones que cores, pero no miles de diminutas.
  • Narrow se encadena en un stage; Wide (Exchange) crea un stage nuevo.
  • repartition = shuffle completo; coalesce = sin shuffle, solo reduce.
  • shuffle.partitions controla cuántas particiones salen de cada shuffle.
  • partitionBy en escritura habilita partition pruning en lectura.
""")

spark.stop()
print("✅ Particionamiento demostrado")
