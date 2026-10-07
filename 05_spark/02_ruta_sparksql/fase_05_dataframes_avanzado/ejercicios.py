"""
FASE 5 — Ejercicios Guiados: DataFrames Avanzado
==================================================
Completa donde dice # TU CÓDIGO AQUÍ
Ejecuta: python validar.py para verificar
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Ejercicios_Fase5") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

df_emp   = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True)
df_sales = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"),    header=True, inferSchema=True)
df_prod  = spark.read.csv(os.path.join(CSV_DIR, "products.csv"), header=True, inferSchema=True)
df_cust  = spark.read.csv(os.path.join(CSV_DIR, "customers.csv"),header=True, inferSchema=True)
df_tx    = spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet"))

# ─────────────────────────────────────────
# EJERCICIO 1
# Cuenta cuántos empleados hay por departamento
# y calcula el salario promedio por departamento.
# Ordena de mayor a menor promedio.
# (Ignora nulos en salary)
# ─────────────────────────────────────────
print("Ejercicio 1: groupBy - empleados por departamento")

df_dept = None  # TU CÓDIGO AQUÍ
if df_dept: df_dept.show()

# ─────────────────────────────────────────
# EJERCICIO 2
# Calcula por región en sales.csv:
#   - Número de ventas
#   - Monto total
#   - Monto máximo
#   - Monto promedio
# Solo muestra regiones con más de 250 ventas.
# ─────────────────────────────────────────
print("\nEjercicio 2: groupBy + HAVING")

df_region = None  # TU CÓDIGO AQUÍ
if df_region: df_region.show()

# ─────────────────────────────────────────
# EJERCICIO 3
# Haz un INNER JOIN entre sales y employees
# para obtener: sale_id, nombre del empleado,
# monto y región. Muestra 10 filas.
# ─────────────────────────────────────────
print("\nEjercicio 3: INNER JOIN ventas + empleados")

df_join = None  # TU CÓDIGO AQUÍ
if df_join: df_join.show(10)

# ─────────────────────────────────────────
# EJERCICIO 4
# Haz LEFT JOIN entre customers y transactions
# para ver qué clientes han hecho transacciones.
# Cuenta cuántos clientes NO tienen ninguna transacción.
# ─────────────────────────────────────────
print("\nEjercicio 4: LEFT JOIN + ANTI count")

sin_transacciones = None  # TU CÓDIGO AQUÍ
print(f"Clientes sin transacciones: {sin_transacciones}")

# ─────────────────────────────────────────
# EJERCICIO 5
# Usa una Window Function para rankear a los
# empleados dentro de su departamento por salary
# (mayor primero). Muestra los top-2 de cada dept.
# ─────────────────────────────────────────
print("\nEjercicio 5: Window Function - top 2 por dept")

ventana = None  # TU CÓDIGO AQUÍ (Window.partitionBy...orderBy)
df_rank = None  # TU CÓDIGO AQUÍ (withColumn + row_number)
df_top2 = None  # TU CÓDIGO AQUÍ (filter row_number <= 2)
if df_top2: df_top2.select("department", "name", "salary", "rn").show(20)

# ─────────────────────────────────────────
# EJERCICIO 6
# Calcula la diferencia de cada salario
# con el promedio de su departamento.
# Columna: diff_vs_avg (salary - avg_dept)
# ─────────────────────────────────────────
print("\nEjercicio 6: Window - diferencia vs promedio")

ventana_avg = None  # TU CÓDIGO AQUÍ
df_diff = None  # TU CÓDIGO AQUÍ
if df_diff:
    df_diff.filter(F.col("salary").isNotNull()) \
           .select("name", "department", "salary", "avg_dept", "diff_vs_avg") \
           .orderBy("department", F.col("diff_vs_avg").desc()) \
           .show(10)

# ─────────────────────────────────────────
# EJERCICIO 7
# Usa lag() para calcular cuánto cambió
# el monto de una transacción respecto
# a la anterior del mismo cliente.
# (ordena por ts dentro de cada customer_id)
# ─────────────────────────────────────────
print("\nEjercicio 7: lag() en transactions")

ventana_tx = None  # TU CÓDIGO AQUÍ
df_lag = None  # TU CÓDIGO AQUÍ
if df_lag:
    df_lag.select("customer_id", "ts", "amount", "monto_anterior", "cambio") \
          .filter(F.col("monto_anterior").isNotNull()) \
          .show(10)

# ─────────────────────────────────────────
# EJERCICIO 8
# Rellena los nulos de salary en employees
# con el promedio de su departamento.
# (Pista: Window + avg + coalesce)
# ─────────────────────────────────────────
print("\nEjercicio 8: fillna con promedio del grupo (Window)")

ventana_dept = None  # TU CÓDIGO AQUÍ
df_filled = None  # TU CÓDIGO AQUÍ
if df_filled:
    nulos_restantes = df_filled.filter(F.col("salary_clean").isNull()).count()
    print(f"Nulos restantes en salary_clean: {nulos_restantes}")

# ─────────────────────────────────────────
# EJERCICIO 9
# Crea una tabla pivot con el total de ventas
# por año (filas) y región (columnas).
# ─────────────────────────────────────────
print("\nEjercicio 9: pivot - ventas por año y región")

df_sales_yr = df_sales.withColumn("anio", F.year(F.to_date("sale_date", "yyyy-MM-dd")))
df_pivot = None  # TU CÓDIGO AQUÍ
if df_pivot: df_pivot.orderBy("anio").show()

# ─────────────────────────────────────────
# EJERCICIO 10: BONUS
# Encuentra el producto más vendido (por monto total)
# en cada región. Resultado: (region, producto, total).
# Requiere: join sales+products, groupBy, window rank.
# ─────────────────────────────────────────
print("\nEjercicio 10 BONUS: producto top por región")

df_bonus = None  # TU CÓDIGO AQUÍ
if df_bonus: df_bonus.show()

spark.stop()
print("\nEjercicios completados. Ejecuta: python validar.py")
