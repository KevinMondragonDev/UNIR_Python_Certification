"""
FASE 5 — RETO: Reporte Ejecutivo de Ventas
===========================================
Sin guía. Resuelve solo.

Datasets: sales.csv, employees.csv, products.csv, customers.csv, orders.parquet

Construye un reporte ejecutivo que responda:

ANÁLISIS DE VENTAS (usar sales.csv + products.csv + employees.csv):
  1. Top 5 empleados por monto total de ventas
  2. Ranking de categorías de producto por monto total
  3. Monto de ventas por región y año (tabla pivot)
  4. Empleado con la venta individual más alta en cada región
     (window function: row_number por región ordenado por amount desc)
  5. Crecimiento MoM (mes a mes) por región usando lag()
     (solo mostrar meses con datos completos, no el primer mes)

ANÁLISIS DE CLIENTES (usar customers.csv + transactions.parquet):
  6. Clientes con más de 20 transacciones (join + groupBy + HAVING)
  7. Monto promedio por cliente, solo clientes con status='completed'
  8. Top 3 clientes por monto total (usa window o groupBy+orderBy)

LIMPIEZA:
  9. Rellena nulos de salary en employees con el promedio del departamento
  10. En products, marca stock=0 como "Agotado" y stock>0 como "Disponible"

Formato de salida: para cada pregunta imprime el número, una descripción y el resultado.

Valida: python validar.py
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

# TU CÓDIGO AQUÍ
