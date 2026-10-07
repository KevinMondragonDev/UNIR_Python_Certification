"""
FASE 3 — RETO: Análisis de Ventas por Región
==============================================
Sin guía. Resuelve solo usando SOLO RDDs.

Datasets:
  - datasets/csv/sales.csv     (sale_id, employee_id, product_id, amount, sale_date, region)
  - datasets/csv/products.csv  (product_id, name, category, price, stock)

Preguntas a responder (usa Pair RDDs):
  1. Total de ventas (monto acumulado) por región, ordenado de mayor a menor
  2. Número de transacciones por región
  3. Monto promedio de venta por región
  4. Categoría de producto más vendida (por monto total)
  5. Región con mayor venta individual (la venta más grande registrada)
  6. Top 3 regiones por cantidad de transacciones

Variables compartidas:
  7. Usa un acumulador para contar cuántas ventas tienen monto > 3,000
  8. Usa broadcast con el lookup de productos para enriquecer las ventas
     con la categoría, luego calcula el monto total por categoría

Reglas:
  - Solo RDDs (no DataFrames, no SQL)
  - Para joins, usa Pair RDDs con join() o leftOuterJoin()
  - Para el broadcast, construye el dict de productos en el driver primero
  - Imprime cada resultado claramente numerado

Valida: python validar.py
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

# TU CÓDIGO AQUÍ
