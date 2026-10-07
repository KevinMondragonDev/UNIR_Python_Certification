"""
FASE 2 — SOLUCIÓN DEL RETO
============================
⚠️  INTENTA RESOLVER EL RETO ANTES DE VER ESTO ⚠️
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Solucion_Reto_Fase2") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

ruta = os.path.join(CSV_DIR, "employees.csv")
rdd_raw = sc.textFile(ruta)
header  = rdd_raw.first()
rdd     = rdd_raw.filter(lambda l: l != header).map(lambda l: l.split(","))

# 1. Total empleados
total = rdd.count()
print(f"1. Total empleados: {total}")

# 2. Departamentos únicos
departamentos = rdd.map(lambda r: r[2]).distinct()
print(f"2. Departamentos únicos: {departamentos.count()}")
print(f"   {sorted(departamentos.collect())}")

# 3. Empleados por departamento
por_dept = rdd.map(lambda r: (r[2], 1)) \
              .reduceByKey(lambda a, b: a + b) \
              .sortBy(lambda kv: kv[1], ascending=False)
print("3. Empleados por departamento:")
for dept, cnt in por_dept.collect():
    print(f"   {dept}: {cnt}")

# 4. Salario promedio
rdd_salarios = rdd.map(lambda r: r[3]).filter(lambda s: s.strip() != "").map(float)
n = rdd_salarios.count()
suma = rdd_salarios.sum()
print(f"4. Salario promedio: ${suma/n:,.2f}")

# 5. Empleados con salario > 80,000
altos = rdd_salarios.filter(lambda s: s > 80000).count()
print(f"5. Empleados con salario > 80,000: {altos}")

# 6. Empleados de México
mexico = rdd.filter(lambda r: r[5].strip() == "Mexico").count()
print(f"6. Empleados de México: {mexico}")

# 7. Empleado con mayor salario
rdd_nombre_sal = rdd.filter(lambda r: r[3].strip() != "") \
                    .map(lambda r: (r[1], float(r[3])))
top1 = rdd_nombre_sal.top(1, key=lambda x: x[1])
print(f"7. Empleado con mayor salario: {top1[0][0]} (${top1[0][1]:,.2f})")

# 8. Top 5 salarios
top5 = rdd_nombre_sal.top(5, key=lambda x: x[1])
print("8. Top 5 empleados por salario:")
for nombre, sal in top5:
    print(f"   {nombre}: ${sal:,.2f}")

spark.stop()
print("\n✅ Reto Fase 2 completado")
