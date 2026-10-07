"""
FASE 8 — Ejemplo 5: Optimización de Joins, AQE y Skew
=======================================================
Aprenderás a:
  - Reconocer las estrategias de join en el plan físico
      SortMergeJoin   → shuffle de AMBAS tablas
      BroadcastHashJoin → la tabla pequeña se copia a cada executor
  - Forzar / desactivar broadcast (umbral, F.broadcast, hints SQL)
  - Qué hace Adaptive Query Execution (AQE) en Spark 3+
  - Resolver un join con skew usando salting
"""

import os
import sys
import io
import time
from contextlib import redirect_stdout
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Optimizacion_Joins") \
    .master("local[4]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

df_sales = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"),    header=True, inferSchema=True)
df_prod  = spark.read.csv(os.path.join(CSV_DIR, "products.csv"), header=True, inferSchema=True)
df_emp   = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True)
df_tx    = spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet"))
df_cust  = spark.read.csv(os.path.join(CSV_DIR, "customers.csv"), header=True, inferSchema=True)


def plan_de(df):
    """Captura el texto de explain() en lugar de imprimirlo."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        df.explain()
    return buf.getvalue()


def estrategia_join(df):
    plan = plan_de(df)
    for nombre in ["BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin",
                   "BroadcastNestedLoopJoin", "CartesianProduct"]:
        if nombre in plan:
            return nombre
    return "¿?"


# ─────────────────────────────────────────
# 1. SortMergeJoin — el join "por defecto" entre tablas grandes
# ─────────────────────────────────────────
print("=" * 60)
print("1. SortMergeJoin (broadcast desactivado)")
print("=" * 60)

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # -1 = nunca broadcast automático

df_smj = df_sales.join(df_prod, "product_id")
print(f"Estrategia: {estrategia_join(df_smj)}")
df_smj.explain()
print("""
Lee el plan de abajo hacia arriba:
  1. FileScan de cada tabla
  2. Exchange hashpartitioning(product_id)  ← SHUFFLE de AMBAS tablas
  3. Sort por product_id en cada partición
  4. SortMergeJoin recorre ambas listas ordenadas en paralelo
Costo: 2 shuffles + 2 sorts. Escala a cualquier tamaño, pero es caro.
""")

# ─────────────────────────────────────────
# 2. BroadcastHashJoin — tabla pequeña copiada a todos
# ─────────────────────────────────────────
print("=" * 60)
print("2. BroadcastHashJoin")
print("=" * 60)

df_bhj = df_sales.join(F.broadcast(df_prod), "product_id")
print(f"Estrategia con F.broadcast(): {estrategia_join(df_bhj)}")
df_bhj.explain()
print("""
  1. El driver recoge df_prod completo (BroadcastExchange)
  2. Lo envía a CADA executor como una hash table en memoria
  3. Cada partición de df_sales hace lookup local → SIN shuffle de df_sales

    Executor 1: [ventas P0] + [productos completo] → join local
    Executor 2: [ventas P1] + [productos completo] → join local

⚠️ Solo si la tabla pequeña cabe holgadamente en memoria del driver y executors.
   Un broadcast de 2 GB tumba el driver con OutOfMemoryError.
""")

# Umbral automático
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))  # 10 MB (default)
df_auto = df_sales.join(df_prod, "product_id")
print(f"Con autoBroadcastJoinThreshold=10MB, sin hint: {estrategia_join(df_auto)}")
print("  ← Catalyst estimó que products.csv < 10 MB y eligió broadcast solo")

# Hints en SQL
df_sales.createOrReplaceTempView("ventas")
df_prod.createOrReplaceTempView("productos")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
for hint in ["BROADCAST(p)", "MERGE(p)", "SHUFFLE_HASH(p)"]:
    q = spark.sql(f"SELECT /*+ {hint} */ v.*, p.name FROM ventas v JOIN productos p ON v.product_id = p.product_id")
    print(f"  Hint /*+ {hint:<16} */ → {estrategia_join(q)}")

# ─────────────────────────────────────────
# 3. Benchmark
# ─────────────────────────────────────────
print("\n" + "=" * 60)
print("3. Benchmark SortMerge vs Broadcast")
print("=" * 60)

df_tx_grande = df_tx.crossJoin(spark.range(20).withColumnRenamed("id", "copia"))  # 100k filas
df_tx_grande.cache().count()

for etiqueta, df_c in [("SortMergeJoin", df_tx_grande.join(df_cust, "customer_id")),
                       ("BroadcastHash", df_tx_grande.join(F.broadcast(df_cust), "customer_id"))]:
    t0 = time.time()
    n = df_c.groupBy("country").agg(F.sum("amount")).count()
    print(f"  {etiqueta:<14} {estrategia_join(df_c):<18} {time.time() - t0:.2f}s  ({n} países)")
print("  En local la diferencia es pequeña; en un clúster el shuffle viaja por la RED.")

# ─────────────────────────────────────────
# 4. AQE — Adaptive Query Execution
# ─────────────────────────────────────────
print("\n" + "=" * 60)
print("4. AQE — Adaptive Query Execution (activo por defecto desde 3.2)")
print("=" * 60)

print("""
Sin AQE, Catalyst decide el plan ANTES de ejecutar, con estimaciones.
Con AQE, Spark RE-OPTIMIZA entre stages usando estadísticas REALES del shuffle:

  1. Coalesce de particiones  → 200 particiones post-shuffle diminutas se fusionan
  2. Cambio de estrategia     → SortMerge pasa a Broadcast si un lado resultó pequeño
  3. Skew join                → parte particiones gigantes en sub-particiones

Configuraciones clave:
  spark.sql.adaptive.enabled                         = true
  spark.sql.adaptive.coalescePartitions.enabled      = true
  spark.sql.adaptive.advisoryPartitionSizeInBytes    = 64MB
  spark.sql.adaptive.skewJoin.enabled                = true
  spark.sql.adaptive.skewJoin.skewedPartitionFactor  = 5
""")

spark.conf.set("spark.sql.shuffle.partitions", "200")

spark.conf.set("spark.sql.adaptive.enabled", "false")
r_sin = df_sales.groupBy("region").agg(F.sum("amount"))
r_sin.collect()
print(f"AQE OFF → particiones tras groupBy: {r_sin.rdd.getNumPartitions()}")

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
r_con = df_sales.groupBy("region").agg(F.sum("amount"))
r_con.collect()
print(f"AQE ON  → particiones tras groupBy: {r_con.rdd.getNumPartitions()}  ← fusionadas en runtime")
print("Plan final con AQE (busca 'AdaptiveSparkPlan isFinalPlan=true' y 'AQEShuffleRead'):")
r_con.explain()

spark.conf.set("spark.sql.shuffle.partitions", "8")

# ─────────────────────────────────────────
# 5. Skew en joins y salting
# ─────────────────────────────────────────
print("=" * 60)
print("5. Skew: una clave concentra casi todos los datos")
print("=" * 60)

spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Simulamos skew: 90% de las transacciones caen en el cliente 1
df_skew = df_tx_grande.withColumn(
    "customer_id",
    F.when(F.rand(seed=42) < 0.9, F.lit(1)).otherwise(F.col("customer_id"))
)

def tamanos_particiones(df):
    return sorted(df.rdd.glom().map(len).collect(), reverse=True)

join_skew = df_skew.join(df_cust, "customer_id")
print(f"Filas por partición tras join SIN salting: {tamanos_particiones(join_skew)}")
print("  ← una task procesa ~90% de los datos; el resto termina y espera (straggler)")

SALT = 8
# Lado grande: agrega un sufijo aleatorio 0..SALT-1 a cada fila
df_skew_salt = df_skew.withColumn("salt", (F.rand(seed=7) * SALT).cast("int"))
# Lado pequeño: replica cada fila SALT veces, una por cada valor de salt
df_cust_salt = df_cust.crossJoin(spark.range(SALT).withColumnRenamed("id", "salt")) \
                      .withColumn("salt", F.col("salt").cast("int"))

join_salt = df_skew_salt.join(df_cust_salt, ["customer_id", "salt"]).drop("salt")
print(f"Filas por partición CON salting ({SALT}):  {tamanos_particiones(join_salt)}")
print(f"Mismo resultado: {join_skew.count() == join_salt.count()}")
print("  La partición más grande se reduce a la mitad o menos. No queda perfecto porque")
print("  hash((1, salt)) % 8 colisiona; con más particiones/SALT el reparto mejora.")

print("""
Salting = la clave caliente se reparte en SALT sub-claves:
    (1, 0), (1, 1), ... (1, 7) → 8 particiones distintas
El precio: el lado pequeño se multiplica por SALT.

Alternativas antes de hacer salting a mano:
  1. ¿El lado pequeño cabe en memoria?  → broadcast (no hay shuffle, no hay skew)
  2. ¿Spark 3+?                         → AQE skewJoin lo hace automáticamente
  3. ¿La clave caliente es null/""?     → filtrarla y procesarla aparte
""")

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "1KB")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "1KB")
join_aqe = df_skew.join(df_cust, "customer_id")
join_aqe.collect()   # explain() tras una acción muestra el plan FINAL de AQE
plan_final = plan_de(join_aqe)
print(f"AQE skewJoin detectó skew: {'skew=true' in plan_final}  ← busca 'SortMergeJoin(skew=true)'")
print("  (umbral bajado a 1KB solo para que se active con datos de juguete)")

df_tx_grande.unpersist()

print("""
RESUMEN — Elegir estrategia de join
┌────────────────────────────┬────────────────────────────────────────┐
│ Situación                  │ Estrategia                             │
├────────────────────────────┼────────────────────────────────────────┤
│ grande ⋈ pequeña (<~100MB) │ BroadcastHashJoin (F.broadcast)        │
│ grande ⋈ grande            │ SortMergeJoin (default)                │
│ grande ⋈ grande con skew   │ AQE skewJoin → si no basta, salting    │
│ join repetido misma clave  │ bucketBy al escribir (evita shuffle)   │
└────────────────────────────┴────────────────────────────────────────┘
""")

spark.stop()
print("✅ Optimización de joins demostrada")
