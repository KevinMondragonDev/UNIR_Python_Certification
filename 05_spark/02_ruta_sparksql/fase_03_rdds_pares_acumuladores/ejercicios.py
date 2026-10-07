"""
FASE 3 — Ejercicios Guiados: Pair RDDs y Variables Compartidas
================================================================
Completa donde dice # TU CÓDIGO AQUÍ
Ejecuta: python validar.py para verificar
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ejercicios_Fase3") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

# ─────────────────────────────────────────
# EJERCICIO 1
# Dado el siguiente RDD de ventas (region, monto),
# calcula el total de ventas por región.
# ─────────────────────────────────────────
print("Ejercicio 1: reduceByKey - ventas por región")

ventas_rdd = sc.parallelize([
    ("Norte", 500), ("Sur", 300), ("Norte", 700),
    ("Este", 200), ("Sur", 400), ("Norte", 100),
    ("Este", 600), ("Sur", 800)
])

ventas_por_region = None  # TU CÓDIGO AQUÍ (reduceByKey)
print(f"Ventas por región: {ventas_por_region.collect() if ventas_por_region else 'PENDIENTE'}")

# ─────────────────────────────────────────
# EJERCICIO 2
# Sobre ventas_rdd, encuentra la región
# con mayor venta total. Guarda en region_max.
# ─────────────────────────────────────────
print("\nEjercicio 2: región con mayor venta")

region_max = None  # TU CÓDIGO AQUÍ (top(1, key=...))
print(f"Región con mayor venta: {region_max}")

# ─────────────────────────────────────────
# EJERCICIO 3
# Dado el siguiente RDD, calcula el promedio
# de calificaciones por alumno usando aggregateByKey.
# ─────────────────────────────────────────
print("\nEjercicio 3: aggregateByKey - promedio de calificaciones")

califs = sc.parallelize([
    ("Ana", 90), ("Luis", 70), ("Ana", 85),
    ("Maria", 95), ("Luis", 80), ("Ana", 78), ("Maria", 88)
])

promedios = None  # TU CÓDIGO AQUÍ
print(f"Promedios: {promedios.collect() if promedios else 'PENDIENTE'}")

# ─────────────────────────────────────────
# EJERCICIO 4
# Une los dos siguientes RDDs con un INNER JOIN
# para obtener: (id, (nombre, departamento))
# ─────────────────────────────────────────
print("\nEjercicio 4: join() - empleados con departamento")

empleados_r = sc.parallelize([(1, "Ana"), (2, "Luis"), (3, "Maria"), (4, "Pedro")])
depts_r     = sc.parallelize([(1, "Eng"), (2, "Sales"), (3, "HR")])

emp_dept = None  # TU CÓDIGO AQUÍ
print(f"Join result: {emp_dept.collect() if emp_dept else 'PENDIENTE'}")

# ─────────────────────────────────────────
# EJERCICIO 5
# Repite el join anterior pero ahora usa
# leftOuterJoin para que Pedro también aparezca.
# ─────────────────────────────────────────
print("\nEjercicio 5: leftOuterJoin")

emp_dept_left = None  # TU CÓDIGO AQUÍ
print(f"Left join: {emp_dept_left.collect() if emp_dept_left else 'PENDIENTE'}")

# ─────────────────────────────────────────
# EJERCICIO 6
# Crea un acumulador que cuente cuántos
# productos tienen stock igual a 0.
# Usa el archivo products.csv
# ─────────────────────────────────────────
print("\nEjercicio 6: Acumulador - productos sin stock")

sin_stock = sc.accumulator(0)  # ya creado

rdd_prods = sc.textFile(os.path.join(CSV_DIR, "products.csv"))
h_p = rdd_prods.first()
rdd_prods_data = rdd_prods.filter(lambda l: l != h_p).map(lambda l: l.split(","))

def contar_sin_stock(campos):
    None  # TU CÓDIGO AQUÍ (campos[4] es el stock)

# TU CÓDIGO AQUÍ (foreach + función)
print(f"Productos sin stock: {sin_stock.value}")

# ─────────────────────────────────────────
# EJERCICIO 7
# Crea un broadcast de un diccionario
# {category: iva_rate} y úsalo para calcular
# el precio con IVA de cada producto.
# Electronics=16%, Food=0%, Others=8%
# ─────────────────────────────────────────
print("\nEjercicio 7: Broadcast - precios con IVA por categoría")

iva_rates = {"Electronics": 0.16, "Food": 0.00, "Clothing": 0.08,
             "Sports": 0.08, "Books": 0.08, "Tools": 0.08, "Health": 0.08}

iva_bd = None  # TU CÓDIGO AQUÍ (broadcast)

rdd_prods_precio = rdd_prods_data.map(
    lambda r: (r[1], float(r[3]), iva_bd.value.get(r[2], 0.08))
).map(
    lambda t: (t[0], round(t[1] * (1 + t[2]), 2))  # (nombre, precio_con_iva)
)

print("Primeros 5 productos con IVA:")
for nombre, precio in rdd_prods_precio.take(5):
    print(f"  {nombre}: ${precio}")

# ─────────────────────────────────────────
# EJERCICIO 8
# Word Count: Lee sales.csv, extrae la columna 'region'
# y cuenta cuántas veces aparece cada región.
# Ordena de mayor a menor.
# ─────────────────────────────────────────
print("\nEjercicio 8: Word Count de regiones en ventas")

rdd_ventas = sc.textFile(os.path.join(CSV_DIR, "sales.csv"))
h_v = rdd_ventas.first()

conteo_regiones = None  # TU CÓDIGO AQUÍ (map, reduceByKey, sortBy)
print("Ventas por región (frecuencia):")
if conteo_regiones:
    for region, cnt in conteo_regiones.collect():
        print(f"  {region}: {cnt}")

spark.stop()
print("\nEjercicios completados. Ejecuta: python validar.py")
