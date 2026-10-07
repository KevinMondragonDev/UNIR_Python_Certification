"""
FASE 2 — Ejercicios Guiados: RDDs Core
========================================
Completa donde dice # TU CÓDIGO AQUÍ
Ejecuta: python validar.py para verificar
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ejercicios_Fase2") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

# ─────────────────────────────────────────
# EJERCICIO 1
# Crea un RDD con los números del 1 al 50
# usando parallelize y con 5 particiones.
# Imprime cuántas particiones tiene.
# ─────────────────────────────────────────
print("Ejercicio 1: parallelize con particiones")

rdd_50 = None  # TU CÓDIGO AQUÍ
# TU CÓDIGO AQUÍ (imprimir particiones)

# ─────────────────────────────────────────
# EJERCICIO 2
# Sobre rdd_50, crea un nuevo RDD donde
# cada número esté elevado al cuadrado.
# Guarda en rdd_cuadrados.
# ─────────────────────────────────────────
print("\nEjercicio 2: map() - cuadrados")

rdd_cuadrados = None  # TU CÓDIGO AQUÍ
print(f"Primeros 5 cuadrados: {rdd_cuadrados.take(5) if rdd_cuadrados else 'PENDIENTE'}")

# ─────────────────────────────────────────
# EJERCICIO 3
# Filtra rdd_50 para quedarte solo con
# los múltiplos de 7. Guarda en rdd_mult7.
# ─────────────────────────────────────────
print("\nEjercicio 3: filter() - múltiplos de 7")

rdd_mult7 = None  # TU CÓDIGO AQUÍ
print(f"Múltiplos de 7 en 1-50: {rdd_mult7.collect() if rdd_mult7 else 'PENDIENTE'}")

# ─────────────────────────────────────────
# EJERCICIO 4
# Dado este RDD de frases, usa flatMap para
# obtener una lista plana de todas las palabras.
# ─────────────────────────────────────────
print("\nEjercicio 4: flatMap() - palabras")

frases = sc.parallelize([
    "El motor Spark procesa datos en memoria",
    "Los RDDs son la base de Spark",
    "Python es el lenguaje más popular en data"
])

rdd_palabras = None  # TU CÓDIGO AQUÍ
print(f"Total palabras: {rdd_palabras.count() if rdd_palabras else 'PENDIENTE'}")
print(f"Primeras 5: {rdd_palabras.take(5) if rdd_palabras else 'PENDIENTE'}")

# ─────────────────────────────────────────
# EJERCICIO 5
# Encuentra cuántas palabras únicas hay
# en rdd_palabras (usa distinct).
# ─────────────────────────────────────────
print("\nEjercicio 5: distinct()")

rdd_unicas = None  # TU CÓDIGO AQUÍ
total_unicas = None  # TU CÓDIGO AQUÍ (count)
print(f"Palabras únicas: {total_unicas}")

# ─────────────────────────────────────────
# EJERCICIO 6
# Lee el archivo employees.csv con textFile.
# Quita el header. Cuenta cuántos empleados hay.
# ─────────────────────────────────────────
print("\nEjercicio 6: textFile() y quitar header")

ruta_emp = os.path.join(CSV_DIR, "employees.csv")
rdd_emp_raw = None  # TU CÓDIGO AQUÍ (textFile)
header = None       # TU CÓDIGO AQUÍ (first())
rdd_emp = None      # TU CÓDIGO AQUÍ (filter sin header)
total_empleados = None  # TU CÓDIGO AQUÍ (count)
print(f"Total empleados: {total_empleados}")

# ─────────────────────────────────────────
# EJERCICIO 7
# Sobre rdd_emp (sin header), parsea cada
# línea por coma y extrae solo el nombre
# (índice 1). Guarda en rdd_nombres.
# ─────────────────────────────────────────
print("\nEjercicio 7: map() + parseo CSV")

rdd_nombres = None  # TU CÓDIGO AQUÍ
print(f"Primeros 5 nombres: {rdd_nombres.take(5) if rdd_nombres else 'PENDIENTE'}")

# ─────────────────────────────────────────
# EJERCICIO 8
# Calcula: suma total de IDs de empleados,
# y el salario máximo (índice 3, castear a float,
# ignorar vacíos con filter).
# ─────────────────────────────────────────
print("\nEjercicio 8: reduce() y estadísticas")

rdd_salarios = None  # TU CÓDIGO AQUÍ (parsear, filtrar vacíos, castear a float)
salario_max = None   # TU CÓDIGO AQUÍ (max() o top(1))
salario_min = None   # TU CÓDIGO AQUÍ
print(f"Salario máximo: {salario_max}")
print(f"Salario mínimo: {salario_min}")

spark.stop()
print("\nEjercicios completados. Ejecuta: python validar.py")
