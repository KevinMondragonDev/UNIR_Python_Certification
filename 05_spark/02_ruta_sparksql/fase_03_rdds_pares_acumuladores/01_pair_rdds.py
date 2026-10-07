"""
FASE 3 — Ejemplo 1: Pair RDDs
================================
Aprenderás:
  - Crear Pair RDDs (clave, valor)
  - reduceByKey, groupByKey, aggregateByKey
  - sortByKey, mapValues, keys, values
  - countByKey
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Pair_RDDs") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

# ─────────────────────────────────────────
# 1. Crear Pair RDDs
# ─────────────────────────────────────────
print("=" * 55)
print("1. Crear Pair RDDs")
print("=" * 55)

pares = sc.parallelize([("a", 1), ("b", 2), ("a", 3), ("c", 1), ("b", 4), ("a", 5)])
print(f"Pair RDD: {pares.collect()}")
print(f"Claves: {pares.keys().collect()}")
print(f"Valores: {pares.values().collect()}")

# Crear pair RDD desde RDD normal
numeros = sc.parallelize(range(1, 11))
par_num = numeros.map(lambda x: (x % 3, x))  # clave = residuo mod 3
print(f"\nPair por módulo 3: {par_num.collect()}")

# ─────────────────────────────────────────
# 2. reduceByKey — PREFERIDO
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("2. reduceByKey()")
print("=" * 55)

ventas = sc.parallelize([
    ("Norte", 100), ("Sur", 200), ("Norte", 150),
    ("Este", 80), ("Sur", 120), ("Norte", 200)
])

total_por_region = ventas.reduceByKey(lambda a, b: a + b)
print(f"Total por región: {sorted(total_por_region.collect())}")

maximo_por_region = ventas.reduceByKey(lambda a, b: max(a, b))
print(f"Máximo por región: {sorted(maximo_por_region.collect())}")

# Word Count clásico
texto = sc.parallelize([
    "spark rdd dataframe spark",
    "sql spark udf rdd rdd"
])
word_count = texto.flatMap(lambda l: l.split()) \
                  .map(lambda w: (w, 1)) \
                  .reduceByKey(lambda a, b: a + b) \
                  .sortBy(lambda kv: kv[1], ascending=False)
print(f"\nWord Count: {word_count.collect()}")

# ─────────────────────────────────────────
# 3. groupByKey — usar solo cuando necesitas todos los valores
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("3. groupByKey() — agrupa todos los valores")
print("=" * 55)

pares2 = sc.parallelize([("a", 10), ("b", 20), ("a", 30), ("b", 40), ("c", 15)])
agrupado = pares2.groupByKey().mapValues(list)
print(f"Agrupado: {agrupado.collect()}")

agrupado_ordenado = pares2.groupByKey().mapValues(sorted)
print(f"Agrupado y ordenado: {agrupado_ordenado.collect()}")

# ─────────────────────────────────────────
# 4. aggregateByKey — promedio por clave
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("4. aggregateByKey() — promedio por departamento")
print("=" * 55)

dept_salarios = sc.parallelize([
    ("Eng", 80000), ("Eng", 90000), ("Eng", 70000),
    ("Sales", 50000), ("Sales", 55000),
    ("HR", 45000)
])

promedio_dept = dept_salarios.aggregateByKey(
    (0.0, 0),                                   # (suma, count) inicial
    lambda acc, v: (acc[0] + v, acc[1] + 1),    # combinar valor
    lambda a, b: (a[0] + b[0], a[1] + b[1])     # combinar acumuladores
).mapValues(lambda sc_: sc_[0] / sc_[1])

print("Salario promedio por departamento:")
for dept, prom in sorted(promedio_dept.collect()):
    print(f"  {dept}: ${prom:,.2f}")

# ─────────────────────────────────────────
# 5. sortByKey
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("5. sortByKey()")
print("=" * 55)

datos_ord = sc.parallelize([("z", 1), ("a", 2), ("m", 3), ("d", 4)])
print(f"Original:   {datos_ord.collect()}")
print(f"sortByKey asc:  {datos_ord.sortByKey().collect()}")
print(f"sortByKey desc: {datos_ord.sortByKey(ascending=False).collect()}")

# ─────────────────────────────────────────
# 6. mapValues() — transformar solo los valores
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("6. mapValues()")
print("=" * 55)

precios = sc.parallelize([("laptop", 1000), ("phone", 500), ("tablet", 300)])
con_iva = precios.mapValues(lambda p: round(p * 1.16, 2))
print(f"Precios originales: {precios.collect()}")
print(f"Con IVA (16%):      {con_iva.collect()}")

# ─────────────────────────────────────────
# 7. countByKey() — contar por clave
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("7. countByKey()")
print("=" * 55)

acceso_logs = sc.parallelize([
    ("user1", "/home"), ("user2", "/login"), ("user1", "/profile"),
    ("user1", "/logout"), ("user3", "/home"), ("user2", "/profile")
])
accesos_por_usuario = acceso_logs.countByKey()
print(f"Accesos por usuario: {dict(accesos_por_usuario)}")

spark.stop()
print("\n✅ Pair RDDs demostrados")
