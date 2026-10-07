"""
FASE 8 — Ejercicios: Optimización y UDFs
==========================================
Completa donde dice # TU CÓDIGO AQUÍ
Ejecuta: python validar.py para verificar
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, DoubleType
import pandas as pd
import time

spark = SparkSession.builder \
    .appName("Ejercicios_Fase8") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

df_emp   = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True)
df_sales = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"),    header=True, inferSchema=True)
df_tx    = spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet"))
df_prod  = spark.read.csv(os.path.join(CSV_DIR, "products.csv"), header=True, inferSchema=True)

# ─────────────────────────────────────────
# EJERCICIO 1
# Haz cache() de df_emp filtrado (solo salary no nulo).
# Realiza 3 operaciones sobre él (count, avg, describe)
# y luego libera la caché con unpersist().
# ─────────────────────────────────────────
print("Ejercicio 1: cache() + múltiples operaciones + unpersist()")

df_emp_clean = None  # TU CÓDIGO AQUÍ (filter + cache)
# TU CÓDIGO AQUÍ (3 operaciones)
# TU CÓDIGO AQUÍ (unpersist)

# ─────────────────────────────────────────
# EJERCICIO 2
# Crea una UDF que reciba el nombre de un país
# y devuelva el código de 2 letras:
#   Mexico → MX, Colombia → CO, Argentina → AR
#   Chile → CL, Peru → PE, España → ES, USA → US
#   Cualquier otro → XX
# Aplícala sobre df_emp.
# ─────────────────────────────────────────
print("\nEjercicio 2: UDF — código de país")

pais_codigo = {
    "Mexico": "MX", "Colombia": "CO", "Argentina": "AR",
    "Chile": "CL", "Peru": "PE", "España": "ES", "USA": "US"
}

def get_codigo_pais(country):
    None  # TU CÓDIGO AQUÍ

codigo_udf = None  # TU CÓDIGO AQUÍ (F.udf)
df_codigos = None  # TU CÓDIGO AQUÍ (withColumn)
if df_codigos:
    df_codigos.select("name", "country", "codigo_pais").show(8)

# ─────────────────────────────────────────
# EJERCICIO 3
# Registra la UDF del ejercicio 2 para SQL.
# Úsala en un spark.sql().
# ─────────────────────────────────────────
print("\nEjercicio 3: Registrar UDF para SQL")

df_emp.createOrReplaceTempView("empleados")
# TU CÓDIGO AQUÍ (spark.udf.register)
# TU CÓDIGO AQUÍ (spark.sql con la UDF)

# ─────────────────────────────────────────
# EJERCICIO 4
# Crea una Pandas UDF que calcule:
#   precio_con_descuento = price * (1 - 0.10)
# (descuento del 10% a todos los productos)
# Aplícala sobre df_prod.
# ─────────────────────────────────────────
print("\nEjercicio 4: Pandas UDF — precio con descuento")

@pandas_udf(DoubleType())
def aplicar_descuento(price: pd.Series) -> pd.Series:
    return None  # TU CÓDIGO AQUÍ

df_descuento = None  # TU CÓDIGO AQUÍ
if df_descuento:
    df_descuento.select("name", "price", "precio_descuento").show(5)

# ─────────────────────────────────────────
# EJERCICIO 5
# Compara el tiempo de:
#   a) Usar UDF Python para clasificar amount en "Alto"/"Bajo"
#   b) Usar F.when() built-in para lo mismo
# Umbral: amount > 2500 → Alto, sino Bajo
# ─────────────────────────────────────────
print("\nEjercicio 5: Benchmark UDF vs built-in")

df_bench = df_sales.cache()
_ = df_bench.count()  # materializar

# UDF Python
clasificar_udf = None  # TU CÓDIGO AQUÍ

t0 = time.time()
# TU CÓDIGO AQUÍ (withColumn + count)
t_udf = time.time() - t0

# Built-in
t0 = time.time()
# TU CÓDIGO AQUÍ (F.when + count)
t_builtin = time.time() - t0

print(f"UDF Python: {t_udf:.3f}s")
print(f"Built-in:   {t_builtin:.3f}s")
df_bench.unpersist()

# ─────────────────────────────────────────
# EJERCICIO 6
# Reparticiona df_tx a 8 particiones, luego
# usa coalesce(2) para reducirlo.
# Verifica el número de particiones en cada paso.
# ─────────────────────────────────────────
print("\nEjercicio 6: repartition y coalesce")

# TU CÓDIGO AQUÍ
particiones_original = None
particiones_8 = None
particiones_2 = None
print(f"Original: {particiones_original}, repartition(8): {particiones_8}, coalesce(2): {particiones_2}")

# ─────────────────────────────────────────
# EJERCICIO 7
# Fuerza un broadcast join entre df_sales y df_prod.
# Verifica en el explain plan que usa BroadcastHashJoin.
# ─────────────────────────────────────────
print("\nEjercicio 7: Broadcast join")

df_broadcast_join = None  # TU CÓDIGO AQUÍ (join con F.broadcast)
if df_broadcast_join:
    print("Plan de ejecución (busca BroadcastHashJoin):")
    df_broadcast_join.explain()

# ─────────────────────────────────────────
# EJERCICIO 8
# Detecta skew en la columna 'region' de df_sales.
# Muestra la distribución (cuántos registros por región).
# ¿Hay skew severo?
# ─────────────────────────────────────────
print("\nEjercicio 8: Detectar skew en región")

df_skew = None  # TU CÓDIGO AQUÍ (groupBy + count + orderBy)
if df_skew: df_skew.show()
# Respuesta: # ¿Hay skew severo? ___

# ─────────────────────────────────────────
# EJERCICIO 9  (ver 02_particionamiento.py)
# Para cada transformación, escribe "narrow" o "wide".
# Luego compruébalo: llama explain() y busca 'Exchange'.
# ─────────────────────────────────────────
print("\nEjercicio 9: Narrow vs Wide")

clasificacion = {
    "filter":      None,  # TU CÓDIGO AQUÍ ("narrow" / "wide")
    "withColumn":  None,
    "groupBy":     None,
    "join":        None,
    "orderBy":     None,
    "coalesce":    None,
    "repartition": None,
    "distinct":    None,
}
# TU CÓDIGO AQUÍ: verifica 2 de ellas con explain()
print(clasificacion)

# ─────────────────────────────────────────
# EJERCICIO 10  (ver 02_particionamiento.py)
# a) Fija spark.sql.shuffle.partitions = 6 y AQE desactivado.
# b) Haz groupBy("region").count() sobre df_sales.
# c) Guarda en n_particiones_agg cuántas particiones tiene el resultado.
# d) Activa AQE y repite. ¿Cuántas quedan ahora? ¿Por qué?
# ─────────────────────────────────────────
print("\nEjercicio 10: shuffle.partitions y AQE")

n_particiones_agg = None      # TU CÓDIGO AQUÍ
n_particiones_agg_aqe = None  # TU CÓDIGO AQUÍ
print(f"Sin AQE: {n_particiones_agg} | Con AQE: {n_particiones_agg_aqe}")
# Respuesta: # ¿Por qué cambian? ___

# ─────────────────────────────────────────
# EJERCICIO 11  (ver 05_optimizacion_joins.py)
# Con autoBroadcastJoinThreshold = -1:
#   a) Haz un join df_sales ⋈ df_prod SIN hint → ¿qué estrategia aparece?
#   b) Haz el mismo join con el hint SQL /*+ BROADCAST(p) */
# Guarda el nombre de la estrategia de cada uno.
# ─────────────────────────────────────────
print("\nEjercicio 11: Estrategias de join")

estrategia_sin_hint = None  # TU CÓDIGO AQUÍ ("SortMergeJoin" / "BroadcastHashJoin" ...)
estrategia_con_hint = None  # TU CÓDIGO AQUÍ
print(f"Sin hint: {estrategia_sin_hint} | Con hint: {estrategia_con_hint}")

# ─────────────────────────────────────────
# EJERCICIO 12  (ver 05_optimizacion_joins.py)
# Salting manual. Dado df_skew (90% de filas con customer_id = 1):
#   a) Añade una columna "salt" entera aleatoria entre 0 y 3
#   b) Replica df_cust 4 veces con una columna "salt" 0..3
#   c) Haz el join por ["customer_id", "salt"] y elimina "salt"
# El conteo debe ser igual al del join sin salting.
# ─────────────────────────────────────────
print("\nEjercicio 12: Salting")

df_cust = spark.read.csv(os.path.join(CSV_DIR, "customers.csv"), header=True, inferSchema=True)
df_skew = df_tx.withColumn("customer_id",
    F.when(F.rand(seed=42) < 0.9, F.lit(1)).otherwise(F.col("customer_id")))

SALT = 4
df_join_salt = None  # TU CÓDIGO AQUÍ
if df_join_salt:
    print(f"Sin salting: {df_skew.join(df_cust, 'customer_id').count()} | "
          f"Con salting: {df_join_salt.count()}")

spark.stop()
print("\nEjercicios completados. Ejecuta: python validar.py")
