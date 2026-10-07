"""
FASE 3 — Validador Automático
==============================
Ejecuta: python validar.py
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

passed = 0
failed = 0

def check(desc, cond):
    global passed, failed
    if cond:
        print(f"  ✅ {desc}"); passed += 1
    else:
        print(f"  ❌ {desc}"); failed += 1

print("=" * 55)
print("VALIDADOR — FASE 3: Pair RDDs y Variables Compartidas")
print("=" * 55)

spark = SparkSession.builder \
    .appName("Validador_Fase3") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

# ─── Test 1: reduceByKey
print("\n[1] reduceByKey()")
ventas_rdd = sc.parallelize([
    ("Norte", 500), ("Sur", 300), ("Norte", 700),
    ("Este", 200), ("Sur", 400)
])
total = ventas_rdd.reduceByKey(lambda a, b: a + b)
total_dict = dict(total.collect())
check("Norte = 1200", total_dict.get("Norte") == 1200)
check("Sur = 700",    total_dict.get("Sur") == 700)
check("Este = 200",   total_dict.get("Este") == 200)

# ─── Test 2: aggregateByKey promedio
print("\n[2] aggregateByKey() — promedio")
califs = sc.parallelize([
    ("Ana", 90), ("Luis", 70), ("Ana", 80), ("Luis", 90)
])
promedios = califs.aggregateByKey(
    (0.0, 0),
    lambda acc, v: (acc[0]+v, acc[1]+1),
    lambda a, b: (a[0]+b[0], a[1]+b[1])
).mapValues(lambda x: x[0]/x[1])
prom_dict = dict(promedios.collect())
check("Promedio Ana = 85.0",  prom_dict.get("Ana") == 85.0)
check("Promedio Luis = 80.0", prom_dict.get("Luis") == 80.0)

# ─── Test 3: join inner
print("\n[3] join() — inner")
emp = sc.parallelize([(1, "Ana"), (2, "Luis"), (3, "Maria")])
dep = sc.parallelize([(1, "Eng"), (2, "Sales")])
joined = emp.join(dep)
joined_dict = dict(joined.collect())
check("Ana en join",   joined_dict.get(1) == ("Ana", "Eng"))
check("Luis en join",  joined_dict.get(2) == ("Luis", "Sales"))
check("Maria NO está en inner join", 3 not in joined_dict)

# ─── Test 4: leftOuterJoin
print("\n[4] leftOuterJoin()")
left = emp.leftOuterJoin(dep)
left_dict = dict(left.collect())
check("Ana en left join",   left_dict.get(1) == ("Ana", "Eng"))
check("Maria en left join con None", left_dict.get(3) == ("Maria", None))

# ─── Test 5: Acumuladores
print("\n[5] Acumuladores")
rdd_prods = sc.textFile(os.path.join(CSV_DIR, "products.csv"))
h = rdd_prods.first()
prods = rdd_prods.filter(lambda l: l != h).map(lambda l: l.split(","))

acc_sin_stock = sc.accumulator(0)
def cnt(r):
    if int(r[4]) == 0:
        acc_sin_stock.add(1)
prods.foreach(cnt)
check("Acumulador cuenta productos sin stock", acc_sin_stock.value >= 0)
check("Sin stock < 100 (no todos tienen stock 0)", acc_sin_stock.value < 100)

# ─── Test 6: Broadcast
print("\n[6] Broadcast")
iva_map = {"Electronics": 0.16, "Food": 0.00, "Other": 0.08}
bd = sc.broadcast(iva_map)
check("Broadcast tiene valor",        bd.value is not None)
check("Electronics iva = 0.16",       bd.value.get("Electronics") == 0.16)
check("Food iva = 0.00",              bd.value.get("Food") == 0.00)
bd.unpersist()

# ─── Test 7: Word Count de regiones
print("\n[7] Word Count de regiones (sales.csv)")
rdd_ventas = sc.textFile(os.path.join(CSV_DIR, "sales.csv"))
hv = rdd_ventas.first()
regiones_wc = rdd_ventas.filter(lambda l: l != hv) \
    .map(lambda l: l.split(",")[5].strip()) \
    .map(lambda r: (r, 1)) \
    .reduceByKey(lambda a, b: a + b)
regiones_dict = dict(regiones_wc.collect())
total_ventas = sum(regiones_dict.values())
check("Total ventas = 2000", total_ventas == 2000)
check("Al menos 5 regiones distintas", len(regiones_dict) >= 5)

# ─── Test 8: sortByKey
print("\n[8] sortByKey()")
pares = sc.parallelize([("z", 1), ("a", 2), ("m", 3)])
ordenado = pares.sortByKey().keys().collect()
check("sortByKey asc: ['a','m','z']", ordenado == ["a", "m", "z"])

spark.stop()

print("\n" + "=" * 55)
total = passed + failed
print(f"RESULTADO: {passed}/{total} checks pasados")
if failed == 0:
    print("🎉 ¡Fase 3 completada! Puedes avanzar a la Fase 4.")
else:
    print(f"⚠️  {failed} check(s) fallaron. Revisa tu código.")
print("=" * 55)
