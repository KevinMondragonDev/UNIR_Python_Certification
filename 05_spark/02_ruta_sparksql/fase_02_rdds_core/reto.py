"""
FASE 2 — RETO: Análisis de Empleados con RDD Puro
===================================================
Sin guía. Resuelve solo usando SOLO RDDs (sin DataFrame).

Dataset: datasets/csv/employees.csv
Columnas: employee_id, name, department, salary, hire_date, country

Preguntas a responder:
  1. ¿Cuántos empleados hay en total?
  2. ¿Cuántos departamentos únicos existen?
  3. ¿Cuántos empleados tiene cada departamento?
     (imprimir como lista ordenada descendente por cantidad)
  4. ¿Cuál es el salario promedio? (ignorar registros sin salario)
  5. ¿Cuántos empleados tienen salario mayor a 80,000?
  6. ¿Cuántos empleados son de México?
  7. ¿Cuál es el nombre del empleado con el salario más alto?
  8. Lista los 5 empleados con mayor salario (nombre + salario)

Reglas:
  - Solo usar RDDs (sc.textFile, map, filter, flatMap, reduce, etc.)
  - No usar spark.read, DataFrame ni SQL
  - Para salarios vacíos, ignorarlos en cálculos numéricos
  - Imprime cada respuesta claramente

Pistas:
  - Parsea por coma: linea.split(",")
  - Índices: [0]=employee_id, [1]=name, [2]=department,
             [3]=salary, [4]=hire_date, [5]=country
  - Para promedios: usa reduce() o sum()/count()
  - Para top 5: usa top(5, key=lambda x: x[1])

Valida: python validar.py
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

# TU CÓDIGO AQUÍ
