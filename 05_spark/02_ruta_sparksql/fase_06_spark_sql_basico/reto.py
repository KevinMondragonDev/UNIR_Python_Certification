"""
FASE 6 — RETO: Análisis Completo en Spark SQL
===============================================
Sin guía. Resuelve solo usando spark.sql() exclusivamente.

Registra estas vistas:
  - empleados (employees.csv)
  - ventas    (sales.csv)
  - productos (products.csv)
  - clientes  (customers.csv)
  - transacciones (transactions.parquet)

Responde las siguientes preguntas usando SOLO spark.sql():

  1. ¿Cuál es el top 5 de departamentos por masa salarial total?
     (Excluir nulos en salary)

  2. ¿Qué categoría de producto genera más ingresos?
     (JOIN ventas + productos, SUM de amount por category)

  3. ¿Cuántos clientes de cada país han hecho al menos una transacción?
     (JOIN clientes + transacciones, GROUP BY country)

  4. Crea una query con CTE que:
       a) calcule el promedio mensual de transacciones por moneda
       b) filtre solo los meses con promedio > 500
       c) ordene por moneda y mes

  5. ¿Qué empleados no han registrado ninguna venta?
     (EXISTS / NOT EXISTS o LEFT ANTI)

  6. Muestra para cada departamento:
       - Salario mínimo, máximo, promedio
       - Empleado con mayor salario (nombre)
     PISTA: usa CTE + subquery o window en SQL

  7. Calcula el porcentaje de transacciones por status
     (completed, pending, failed, refunded) sobre el total.

  8. ¿En qué mes del año se generan más ventas en total?
     (GROUP BY month, SUM amount, ORDER BY DESC)

Bonus:
  9. Construye el query más limpio posible con 3 CTEs que responda:
     "¿Cuál es el empleado más valioso? (mayor suma de ventas)"
     Incluye: nombre, departamento, país, total_ventas, num_ventas.

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
