"""
FASE 4 — RETO: Exploración y Limpieza de Employees
====================================================
Sin guía. Resuelve solo.

Dataset: datasets/csv/employees.csv
Columnas: employee_id, name, department, salary, hire_date, country

Tareas:
  1. Carga el CSV con schema explícito (define los tipos correctos).
  2. Muestra el schema y describe() del DataFrame.
  3. ¿Cuántos registros tienen salary nulo? Cuéntalos con un filtro.
  4. Crea un DataFrame limpio (sin nulos en salary). Llámalo df_clean.
  5. Agrega estas columnas a df_clean:
       - salary_anual: salary * 12
       - nivel: Junior/Mid/Senior/Lead según rangos que tú definas
       - anios_empresa: años desde hire_date hasta hoy
       - nombre_upper: name en mayúsculas
  6. ¿Cuántos empleados hay por departamento? (sin groupBy — usa select + distinct)
     PISTA: sc.textFile + filter + map también vale
  7. Filtra los empleados contratados después del 2020-01-01 con salary > 50000.
  8. ¿Cuál es el departamento con mayor salario promedio?
     (puedes usar groupBy().agg() aunque se ve en Fase 5 — inténtalo)
  9. Guarda df_clean enriquecido en /tmp/employees_clean.parquet

Pistas:
  - Para anios_empresa: F.datediff(F.current_date(), fecha) / 365
  - Para fechas: F.to_date("hire_date", "yyyy-MM-dd")
  - Para guardar: df.write.mode("overwrite").parquet(path)

Valida: python validar.py
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

# TU CÓDIGO AQUÍ
