"""
FASE 7 — RETO: Reporte Ejecutivo SQL Avanzado
==============================================
Sin guía. Resuelve solo. Usar spark.sql() exclusivamente.

PARTE A — Window Functions:
  1. Top 3 empleados por salario dentro de cada departamento
     (usa DENSE_RANK para manejar empates)
  2. Promedio móvil de 3 meses de ventas por región
     (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
  3. Empleados cuyo salario está en el cuartil superior (NTILE = 4)
  4. Para cada empleado, muestra cuánto gana más/menos que
     el mejor pagado de su departamento (FIRST_VALUE)

PARTE B — Arrays y Structs:
  5. ¿Cuál es el tag más común en la tabla eventos?
  6. Usuarios que tienen el tag 'organic' Y 'paid' en algún evento
     (ARRAY_CONTAINS en WHERE con AND)
  7. Para cada tipo de evento, calcula cuántos tags distintos
     se usan en promedio (EXPLODE + GROUP BY + AVG)

PARTE C — Explain Plan:
  8. Escribe la query más eficiente posible para:
     "Top 5 clientes por monto total en transacciones completadas"
     Luego muestra su explain() y explica en comentario
     qué tipo de join/aggregation usa.

Bonus:
  9. Crea una query con 4 CTEs que construya un
     "cuadro de mando mensual" por región con:
     - Total ventas del mes
     - Crecimiento vs mes anterior (LAG)
     - Ranking del mes (RANK por monto)
     - Promedio móvil 3 meses

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
