"""
FASE 6 — Ejercicios Guiados: Spark SQL Básico
===============================================
Completa las queries SQL donde dice -- TU QUERY AQUÍ
Ejecuta: python validar.py para verificar
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ejercicios_Fase6") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True) \
     .createOrReplaceTempView("empleados")
spark.read.csv(os.path.join(CSV_DIR, "sales.csv"),    header=True, inferSchema=True) \
     .createOrReplaceTempView("ventas")
spark.read.csv(os.path.join(CSV_DIR, "products.csv"), header=True, inferSchema=True) \
     .createOrReplaceTempView("productos")
spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet")) \
     .createOrReplaceTempView("transacciones")

# ─────────────────────────────────────────
# EJERCICIO 1
# Selecciona nombre, departamento y salario
# de los 10 empleados mejor pagados.
# ─────────────────────────────────────────
print("Ejercicio 1: Top 10 salarios")

spark.sql("""
    -- TU QUERY AQUÍ
    SELECT 'PENDIENTE' AS resultado
""").show()

# ─────────────────────────────────────────
# EJERCICIO 2
# Cuenta cuántos empleados hay por país.
# Solo países con más de 60 empleados.
# Ordena de mayor a menor.
# ─────────────────────────────────────────
print("\nEjercicio 2: Empleados por país (HAVING)")

spark.sql("""
    -- TU QUERY AQUÍ
    SELECT 'PENDIENTE' AS resultado
""").show()

# ─────────────────────────────────────────
# EJERCICIO 3
# Calcula el salario promedio por departamento
# y muestra solo los que tienen avg_salary > 65000.
# Renombra columnas a: departamento, promedio.
# ─────────────────────────────────────────
print("\nEjercicio 3: AVG por dept con HAVING")

spark.sql("""
    -- TU QUERY AQUÍ
    SELECT 'PENDIENTE' AS resultado
""").show()

# ─────────────────────────────────────────
# EJERCICIO 4
# Muestra el nombre del empleado en MAYÚSCULAS,
# los primeros 3 caracteres de su departamento,
# y cuántos días lleva en la empresa.
# ─────────────────────────────────────────
print("\nEjercicio 4: Funciones de string y fecha")

spark.sql("""
    -- TU QUERY AQUÍ
    SELECT 'PENDIENTE' AS resultado
""").show(5)

# ─────────────────────────────────────────
# EJERCICIO 5
# Clasifica a los empleados con CASE WHEN:
#   salary < 40000    → 'Junior'
#   salary < 70000    → 'Mid'
#   salary >= 70000   → 'Senior'
#   NULL              → 'Sin dato'
# Muestra cuántos hay de cada nivel.
# ─────────────────────────────────────────
print("\nEjercicio 5: CASE WHEN + GROUP BY")

spark.sql("""
    -- TU QUERY AQUÍ
    SELECT 'PENDIENTE' AS resultado
""").show()

# ─────────────────────────────────────────
# EJERCICIO 6
# JOIN: muestra las 10 ventas más grandes
# con el nombre del empleado que las hizo.
# Columnas: venta_id, empleado, monto, región, fecha.
# ─────────────────────────────────────────
print("\nEjercicio 6: JOIN ventas + empleados")

spark.sql("""
    -- TU QUERY AQUÍ
    SELECT 'PENDIENTE' AS resultado
""").show()

# ─────────────────────────────────────────
# EJERCICIO 7
# Subquery: empleados con salario mayor
# al promedio de su departamento.
# Muestra: nombre, departamento, salary, avg_dept.
# ─────────────────────────────────────────
print("\nEjercicio 7: Subquery — sobre el promedio del dept")

spark.sql("""
    -- TU QUERY AQUÍ
    SELECT 'PENDIENTE' AS resultado
""").show(10)

# ─────────────────────────────────────────
# EJERCICIO 8
# CTE: Calcula total de ventas por empleado,
# luego join con empleados para mostrar:
# nombre, departamento, total_ventas.
# Solo los 10 con mayor total.
# ─────────────────────────────────────────
print("\nEjercicio 8: CTE + JOIN")

spark.sql("""
    -- TU QUERY AQUÍ
    SELECT 'PENDIENTE' AS resultado
""").show()

# ─────────────────────────────────────────
# EJERCICIO 9
# Usa transacciones. Calcula por mes (yyyy-MM)
# y moneda (currency): total de transacciones
# completadas y monto promedio.
# ─────────────────────────────────────────
print("\nEjercicio 9: Análisis de transacciones")

spark.sql("""
    -- TU QUERY AQUÍ
    SELECT 'PENDIENTE' AS resultado
""").show(10)

# ─────────────────────────────────────────
# EJERCICIO 10
# CTE doble: 
#   1. Calcula ventas totales por categoría de producto
#   2. Calcula el porcentaje de cada categoría sobre el total global
# ─────────────────────────────────────────
print("\nEjercicio 10: CTE doble — porcentaje por categoría")

spark.sql("""
    -- TU QUERY AQUÍ
    SELECT 'PENDIENTE' AS resultado
""").show()

spark.stop()
print("\nEjercicios completados. Ejecuta: python validar.py")
