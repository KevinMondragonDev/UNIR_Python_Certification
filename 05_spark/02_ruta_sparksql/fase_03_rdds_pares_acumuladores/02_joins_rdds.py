"""
FASE 3 — Ejemplo 2: Joins entre RDDs
======================================
Aprenderás:
  - join (inner), leftOuterJoin, rightOuterJoin, fullOuterJoin
  - cogroup
  - Cuándo usar joins en RDDs
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Joins_RDDs") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

# ─────────────────────────────────────────
# Datos de ejemplo
# ─────────────────────────────────────────
empleados = sc.parallelize([
    (1, "Ana"),
    (2, "Luis"),
    (3, "Maria"),
    (4, "Pedro")   # Pedro no tiene salario asignado
])

salarios = sc.parallelize([
    (1, 80000),
    (2, 50000),
    (3, 95000),
    (5, 60000)    # ID 5 no está en empleados
])

departamentos = sc.parallelize([
    (1, "Engineering"),
    (2, "Sales"),
    (3, "Engineering"),
    (4, "Marketing")
])

# ─────────────────────────────────────────
# 1. join — INNER (solo coincidencias en ambos)
# ─────────────────────────────────────────
print("=" * 55)
print("1. join() — INNER JOIN")
print("=" * 55)

inner = empleados.join(salarios)
print("Empleados con salario (inner):")
for emp_id, (nombre, salario) in inner.collect():
    print(f"  ID={emp_id}: {nombre} → ${salario:,}")
print(f"→ Pedro (ID=4) no aparece (no tiene salario)")
print(f"→ ID=5 no aparece (no está en empleados)")

# ─────────────────────────────────────────
# 2. leftOuterJoin — todos de la izquierda
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("2. leftOuterJoin() — todos de empleados")
print("=" * 55)

left = empleados.leftOuterJoin(salarios)
print("Todos los empleados (con o sin salario):")
for emp_id, (nombre, salario) in left.collect():
    sal_str = f"${salario:,}" if salario else "SIN SALARIO"
    print(f"  ID={emp_id}: {nombre} → {sal_str}")

# ─────────────────────────────────────────
# 3. rightOuterJoin — todos de la derecha
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("3. rightOuterJoin() — todos de salarios")
print("=" * 55)

right = empleados.rightOuterJoin(salarios)
print("Todos los registros de salarios:")
for emp_id, (nombre, salario) in right.collect():
    nom_str = nombre if nombre else "EMPLEADO_DESCONOCIDO"
    print(f"  ID={emp_id}: {nom_str} → ${salario:,}")

# ─────────────────────────────────────────
# 4. fullOuterJoin — todos de ambos
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("4. fullOuterJoin() — todos de ambas fuentes")
print("=" * 55)

full = empleados.fullOuterJoin(salarios)
print("Union completa:")
for emp_id, (nombre, salario) in full.collect():
    nom_str = nombre if nombre else "N/A"
    sal_str = f"${salario:,}" if salario else "N/A"
    print(f"  ID={emp_id}: {nom_str} → {sal_str}")

# ─────────────────────────────────────────
# 5. Join encadenado — 3 RDDs
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("5. Join encadenado (3 RDDs)")
print("=" * 55)

emp_sal = empleados.join(salarios)
emp_sal_dept = emp_sal.join(departamentos)

print("Empleados con salario y departamento:")
for emp_id, ((nombre, salario), dept) in emp_sal_dept.collect():
    print(f"  ID={emp_id}: {nombre} | ${salario:,} | {dept}")

# ─────────────────────────────────────────
# 6. cogroup — agrupar valores de ambos por clave
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("6. cogroup()")
print("=" * 55)

cursos_tomados = sc.parallelize([
    ("Ana", "Spark"), ("Ana", "Python"), ("Luis", "SQL"), ("Maria", "Spark")
])
calificaciones = sc.parallelize([
    ("Ana", 95), ("Ana", 87), ("Luis", 78), ("Pedro", 92)
])

cogrupado = cursos_tomados.cogroup(calificaciones)
print("cogroup — cursos y calificaciones por alumno:")
for nombre, (cursos, califs) in cogrupado.collect():
    print(f"  {nombre}: cursos={list(cursos)}, califs={list(califs)}")

# ─────────────────────────────────────────
# 7. Caso práctico: enriquecer ventas con productos
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("7. Caso real: ventas + info de productos")
print("=" * 55)

rdd_ventas = sc.textFile(os.path.join(CSV_DIR, "sales.csv"))
h_v = rdd_ventas.first()
ventas_data = rdd_ventas.filter(lambda l: l != h_v) \
    .map(lambda l: l.split(",")) \
    .map(lambda r: (int(r[2]), float(r[3])))  # (product_id, amount)

rdd_prods = sc.textFile(os.path.join(CSV_DIR, "products.csv"))
h_p = rdd_prods.first()
prods_data = rdd_prods.filter(lambda l: l != h_p) \
    .map(lambda l: l.split(",")) \
    .map(lambda r: (int(r[0]), r[2]))  # (product_id, category)

ventas_con_cat = ventas_data.join(prods_data)

total_por_cat = ventas_con_cat \
    .map(lambda kv: (kv[1][1], kv[1][0])) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda kv: kv[1], ascending=False)

print("Ventas totales por categoría:")
for cat, total in total_por_cat.collect():
    print(f"  {cat}: ${total:,.2f}")

spark.stop()
print("\n✅ Joins en RDDs demostrados")
