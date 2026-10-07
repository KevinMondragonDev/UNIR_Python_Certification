"""
FASE 4 — Ejercicios Guiados: DataFrames Intro
===============================================
Completa donde dice # TU CÓDIGO AQUÍ
Ejecuta: python validar.py para verificar
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("Ejercicios_Fase4") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

# ─────────────────────────────────────────
# EJERCICIO 1
# Crea un DataFrame desde esta lista con schema explícito.
# Columnas: producto (STRING), precio (DOUBLE), stock (INT)
# ─────────────────────────────────────────
print("Ejercicio 1: Crear DataFrame con schema explícito")

datos = [("Laptop", 1200.0, 50), ("Mouse", 25.5, 300), ("Teclado", 45.0, 150)]
schema = None  # TU CÓDIGO AQUÍ (StructType)
df_prod = None  # TU CÓDIGO AQUÍ (createDataFrame)
if df_prod:
    df_prod.printSchema()
    df_prod.show()

# ─────────────────────────────────────────
# EJERCICIO 2
# Lee employees.csv con inferSchema=True.
# Imprime: schema, número de filas, columnas.
# ─────────────────────────────────────────
print("\nEjercicio 2: Leer CSV")

df_emp = None  # TU CÓDIGO AQUÍ
if df_emp:
    df_emp.printSchema()
    print(f"Filas: {df_emp.count()}, Columnas: {len(df_emp.columns)}")

# ─────────────────────────────────────────
# EJERCICIO 3
# Selecciona solo name, department y salary de df_emp.
# Renombra salary a salario. Muestra 5 filas.
# ─────────────────────────────────────────
print("\nEjercicio 3: select + alias")

df_sel = None  # TU CÓDIGO AQUÍ
if df_sel: df_sel.show(5)

# ─────────────────────────────────────────
# EJERCICIO 4
# Filtra empleados del departamento "Engineering"
# con salario mayor a 60,000. Muestra cuántos son.
# ─────────────────────────────────────────
print("\nEjercicio 4: filter con múltiples condiciones")

df_eng = None  # TU CÓDIGO AQUÍ
print(f"Ingenieros con salario > 60k: {df_eng.count() if df_eng else 'PENDIENTE'}")

# ─────────────────────────────────────────
# EJERCICIO 5
# Agrega una columna 'salario_mensual' = salary / 12
# y otra 'es_senior' = True si salary > 70000
# ─────────────────────────────────────────
print("\nEjercicio 5: withColumn")

df_cols = None  # TU CÓDIGO AQUÍ
if df_cols:
    df_cols.select("name", "salary", "salario_mensual", "es_senior").show(5)

# ─────────────────────────────────────────
# EJERCICIO 6
# Crea una columna 'nivel' con when/otherwise:
#   salary < 40000 → "Junior"
#   salary < 70000 → "Mid"
#   salary >= 70000 → "Senior"
#   nulos → "Sin dato"
# ─────────────────────────────────────────
print("\nEjercicio 6: when/otherwise")

df_nivel = None  # TU CÓDIGO AQUÍ
if df_nivel:
    df_nivel.select("name", "salary", "nivel") \
            .groupBy("nivel").count().show()

# ─────────────────────────────────────────
# EJERCICIO 7
# Convierte hire_date (string) a tipo Date.
# Luego extrae: año, mes y días desde contratación.
# ─────────────────────────────────────────
print("\nEjercicio 7: to_date + funciones de fecha")

df_fechas = None  # TU CÓDIGO AQUÍ
if df_fechas:
    df_fechas.select("name", "hire_date", "anio", "mes", "dias_en_empresa").show(5)

# ─────────────────────────────────────────
# EJERCICIO 8
# Lee transactions.parquet e imprime:
# - Schema
# - Número de transacciones completadas (status = 'completed')
# - Monto promedio de transacciones completadas
# ─────────────────────────────────────────
print("\nEjercicio 8: Leer Parquet + filtros")

df_tx = None  # TU CÓDIGO AQUÍ (leer parquet)
# TU CÓDIGO AQUÍ (filtrar y calcular)

# ─────────────────────────────────────────
# EJERCICIO 9
# Sobre df_emp: usa dropDuplicates() para obtener
# todas las combinaciones únicas de department + country.
# ¿Cuántas hay?
# ─────────────────────────────────────────
print("\nEjercicio 9: dropDuplicates")

df_unique = None  # TU CÓDIGO AQUÍ
print(f"Combinaciones dept+country únicas: {df_unique.count() if df_unique else 'PENDIENTE'}")

# ─────────────────────────────────────────
# EJERCICIO 10
# Ordena df_emp por salary descendente (ignorando nulos)
# y muestra los 5 empleados mejor pagados.
# ─────────────────────────────────────────
print("\nEjercicio 10: orderBy + limit + filter nulos")

df_top5 = None  # TU CÓDIGO AQUÍ
if df_top5: df_top5.show()

spark.stop()
print("\nEjercicios completados. Ejecuta: python validar.py")
