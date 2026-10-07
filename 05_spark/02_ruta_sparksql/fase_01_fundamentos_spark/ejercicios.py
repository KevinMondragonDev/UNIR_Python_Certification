"""
FASE 1 — Ejercicios Guiados
============================
Completa cada ejercicio donde dice # TU CÓDIGO AQUÍ
Luego ejecuta: python validar.py para verificar tus respuestas.
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─────────────────────────────────────────
# EJERCICIO 1
# Crea una SparkSession con:
#   - appName = "Ejercicios_Fase1"
#   - master  = "local[2]"          (usa 2 cores)
#   - shuffle partitions = 4
#   - logs silenciados (setLogLevel ERROR)
# ─────────────────────────────────────────
print("Ejercicio 1: Crear SparkSession")

spark = None  # TU CÓDIGO AQUÍ


# ─────────────────────────────────────────
# EJERCICIO 2
# Usa spark.range() para crear un DataFrame
# con números del 1 al 20 (inclusive).
# Muestra el resultado con show().
# ─────────────────────────────────────────
print("\nEjercicio 2: spark.range()")

df_rango = None  # TU CÓDIGO AQUÍ
# TU CÓDIGO AQUÍ (mostrar)


# ─────────────────────────────────────────
# EJERCICIO 3
# A partir de df_rango, aplica un filtro para
# quedarte solo con los números pares.
# Guarda el resultado en df_pares.
# (No hagas show todavía — practica lazy eval)
# ─────────────────────────────────────────
print("\nEjercicio 3: Filtrar pares (lazy)")

df_pares = None  # TU CÓDIGO AQUÍ


# ─────────────────────────────────────────
# EJERCICIO 4
# Cuenta cuántos números pares hay en df_pares.
# Guarda el resultado en variable total_pares.
# ─────────────────────────────────────────
print("\nEjercicio 4: Acción count()")

total_pares = None  # TU CÓDIGO AQUÍ
print(f"Total pares: {total_pares}")


# ─────────────────────────────────────────
# EJERCICIO 5
# Inspecciona el plan de ejecución de df_pares
# usando explain(). Luego responde en comentario:
# ¿Qué significa "Filter" en el plan?
# ─────────────────────────────────────────
print("\nEjercicio 5: explain()")

# TU CÓDIGO AQUÍ

# Respuesta: # ...


# ─────────────────────────────────────────
# Limpieza
# ─────────────────────────────────────────
if spark:
    spark.stop()
print("\nEjercicios completados. Ejecuta: python validar.py")
