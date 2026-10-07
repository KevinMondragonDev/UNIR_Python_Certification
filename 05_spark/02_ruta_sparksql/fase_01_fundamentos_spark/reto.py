"""
FASE 1 — RETO
==============
Sin guía. Resuelve solo.

Objetivo:
  Crear una SparkSession bien configurada y demostrar que entiendes
  lazy evaluation con un pipeline de transformaciones + acciones.

Requisitos:
  1. SparkSession con:
       - appName = "Reto_Fase1"
       - master  = local[*]
       - shuffle partitions = 2
       - log level = ERROR

  2. Crear un DataFrame con números del 0 al 99 usando spark.range().

  3. Aplicar las siguientes transformaciones (en orden, SIN ejecutar aún):
       a) Filtrar: solo números divisibles por 3
       b) Agregar columna "cubo" = id ** 3
       c) Filtrar: solo cubo < 10000

  4. ANTES de ejecutar cualquier acción, imprimir el plan con explain().

  5. Ejecutar las siguientes acciones y mostrar resultados:
       a) count()  → cuántos números quedan
       b) show(5)  → primeras 5 filas
       c) first()  → primera fila

  6. Imprimir la versión de Spark y el número de cores disponibles.

  7. Cerrar la sesión.

Pistas:
  - col("id") ** 3  para elevar al cubo
  - spark.sparkContext.defaultParallelism  para ver los cores
  - spark.version para la versión

Valida: python validar.py
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# TU CÓDIGO AQUÍ
