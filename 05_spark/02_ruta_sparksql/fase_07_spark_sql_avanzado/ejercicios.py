"""
FASE 7 — Ejercicios: Spark SQL Avanzado
========================================
Completa las queries SQL donde dice -- TU QUERY AQUÍ
Ejecuta: python validar.py para verificar
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ejercicios_Fase7") \
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
spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet")) \
     .createOrReplaceTempView("transacciones")
spark.read.parquet(os.path.join(PARQ_DIR, "events.parquet")) \
     .createOrReplaceTempView("eventos")

# ─────────────────────────────────────────
# EJERCICIO 1
# Rankea a todos los empleados por salary
# dentro de cada departamento (mayor primero).
# Muestra: department, name, salary, rank, dense_rank
# ─────────────────────────────────────────
print("Ejercicio 1: RANK y DENSE_RANK por departamento")

spark.sql("""
    -- TU QUERY AQUÍ
    SELECT 'PENDIENTE' AS resultado
""").show()

# ─────────────────────────────────────────
# EJERCICIO 2
# Obtén el empleado #1 (por salary) de cada
# departamento usando ROW_NUMBER en CTE.
# ─────────────────────────────────────────
print("\nEjercicio 2: Top-1 por dept con ROW_NUMBER")

spark.sql("""
    -- TU QUERY AQUÍ
    SELECT 'PENDIENTE' AS resultado
""").show()

# ─────────────────────────────────────────
# EJERCICIO 3
# Calcula el salario acumulado (running sum)
# de todos los empleados ordenados por salary ASC.
# Columnas: name, salary, salary_acumulado
# ─────────────────────────────────────────
print("\nEjercicio 3: Suma acumulada de salarios")

spark.sql("""
    -- TU QUERY AQUÍ
    SELECT 'PENDIENTE' AS resultado
""").show(10)

# ─────────────────────────────────────────
# EJERCICIO 4
# Calcula la diferencia de ventas mes a mes
# (MoM) usando LAG para la región "Sur".
# Muestra: mes, total, mes_anterior, diferencia
# ─────────────────────────────────────────
print("\nEjercicio 4: LAG MoM para región Sur")

spark.sql("""
    -- TU QUERY AQUÍ
    SELECT 'PENDIENTE' AS resultado
""").show()

# ─────────────────────────────────────────
# EJERCICIO 5
# Clasifica a los empleados en cuartiles
# salariales usando NTILE(4).
# Muestra cuántos hay en cada cuartil.
# ─────────────────────────────────────────
print("\nEjercicio 5: NTILE — cuartiles salariales")

spark.sql("""
    -- TU QUERY AQUÍ
    SELECT 'PENDIENTE' AS resultado
""").show()

# ─────────────────────────────────────────
# EJERCICIO 6
# En la tabla eventos, cuenta cuántos eventos
# de cada tipo (event_type) hay.
# Luego usa EXPLODE sobre tags y cuenta la
# frecuencia de cada tag.
# ─────────────────────────────────────────
print("\nEjercicio 6: EXPLODE tags y contar frecuencia")

spark.sql("""
    -- TU QUERY AQUÍ (frecuencia de tags)
    SELECT 'PENDIENTE' AS resultado
""").show()

# ─────────────────────────────────────────
# EJERCICIO 7
# Filtra los eventos que contienen el tag 'paid'.
# Muestra cuántos usuarios únicos tienen eventos 'paid'.
# ─────────────────────────────────────────
print("\nEjercicio 7: ARRAY_CONTAINS")

spark.sql("""
    -- TU QUERY AQUÍ
    SELECT 'PENDIENTE' AS resultado
""").show()

# ─────────────────────────────────────────
# EJERCICIO 8
# Para cada usuario, usa COLLECT_SET para
# obtener todos los tipos de eventos únicos que hizo.
# Muestra solo usuarios con más de 3 tipos distintos.
# ─────────────────────────────────────────
print("\nEjercicio 8: COLLECT_SET por usuario")

spark.sql("""
    -- TU QUERY AQUÍ
    SELECT 'PENDIENTE' AS resultado
""").show()

# ─────────────────────────────────────────
# EJERCICIO 9
# Lee el explain plan de esta query y
# responde en comentario: ¿qué tipo de join usa?
# query: ventas JOIN empleados ON employee_id
# ─────────────────────────────────────────
print("\nEjercicio 9: Explain plan")

spark.sql("""
    SELECT e.name, SUM(v.amount) as total
    FROM ventas v JOIN empleados e ON v.employee_id = e.employee_id
    GROUP BY e.name
""").explain()

# Respuesta: # El join que usa es: ___________

# ─────────────────────────────────────────
# EJERCICIO 10 BONUS
# Construye una query con 3 CTEs que:
#   1. Calcule total de ventas por empleado
#   2. Haga join con empleados para obtener dept
#   3. Calcule el ranking dentro de cada dept
# Muestra el top-1 vendedor por departamento.
# ─────────────────────────────────────────
print("\nEjercicio 10 BONUS: 3 CTEs + Window ranking")

spark.sql("""
    -- TU QUERY AQUÍ
    SELECT 'PENDIENTE' AS resultado
""").show()

spark.stop()
print("\nEjercicios completados. Ejecuta: python validar.py")
