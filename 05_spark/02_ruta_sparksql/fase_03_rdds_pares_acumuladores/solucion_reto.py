"""
FASE 3 — SOLUCIÓN DEL RETO
============================
⚠️  INTENTA RESOLVER EL RETO ANTES DE VER ESTO ⚠️
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Solucion_Reto_Fase3") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

# Cargar datos
rdd_ventas = sc.textFile(os.path.join(CSV_DIR, "sales.csv"))
h_v = rdd_ventas.first()
ventas = rdd_ventas.filter(lambda l: l != h_v).map(lambda l: l.split(","))

rdd_prods = sc.textFile(os.path.join(CSV_DIR, "products.csv"))
h_p = rdd_prods.first()

# 1. Total ventas por región
total_region = ventas.map(lambda r: (r[5].strip(), float(r[3]))) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda kv: kv[1], ascending=False)
print("1. Total ventas por región:")
for r, t in total_region.collect():
    print(f"   {r}: ${t:,.2f}")

# 2. Número de transacciones por región
tx_region = ventas.map(lambda r: (r[5].strip(), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda kv: kv[1], ascending=False)
print("\n2. Transacciones por región:")
for r, c in tx_region.collect():
    print(f"   {r}: {c}")

# 3. Promedio de venta por región
suma_count = ventas.map(lambda r: (r[5].strip(), (float(r[3]), 1))) \
    .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
    .mapValues(lambda sc_: sc_[0]/sc_[1])
print("\n3. Promedio por región:")
for r, avg in suma_count.sortBy(lambda kv: kv[1], ascending=False).collect():
    print(f"   {r}: ${avg:,.2f}")

# 4. Categoría más vendida — necesita join con productos
lookup_cat = {}
for line in rdd_prods.filter(lambda l: l != h_p).collect():
    c = line.split(",")
    lookup_cat[int(c[0])] = c[2]
cat_bd = sc.broadcast(lookup_cat)

ventas_cat = ventas.map(
    lambda r: (cat_bd.value.get(int(r[2]), "UNKNOWN"), float(r[3]))
).reduceByKey(lambda a, b: a + b)
mejor_cat = ventas_cat.top(1, key=lambda x: x[1])
print(f"\n4. Categoría más vendida: {mejor_cat[0][0]} (${mejor_cat[0][1]:,.2f})")

# 5. Venta individual más grande
max_venta = ventas.map(lambda r: (r[5].strip(), float(r[3]))) \
    .top(1, key=lambda x: x[1])
print(f"\n5. Venta más grande: Región={max_venta[0][0]}, Monto=${max_venta[0][1]:,.2f}")

# 6. Top 3 regiones por transacciones
top3 = tx_region.take(3)
print("\n6. Top 3 regiones (transacciones):")
for r, c in top3:
    print(f"   {r}: {c}")

# 7. Acumulador — ventas > 3000
ventas_altas = sc.accumulator(0)
def contar_altas(r):
    if float(r[3]) > 3000:
        ventas_altas.add(1)
ventas.foreach(contar_altas)
print(f"\n7. Ventas con monto > 3,000: {ventas_altas.value}")

# 8. Monto por categoría con broadcast
print("\n8. Monto total por categoría (broadcast):")
for cat, tot in ventas_cat.sortBy(lambda kv: kv[1], ascending=False).collect():
    print(f"   {cat}: ${tot:,.2f}")

cat_bd.unpersist()
spark.stop()
print("\n✅ Reto Fase 3 completado")
