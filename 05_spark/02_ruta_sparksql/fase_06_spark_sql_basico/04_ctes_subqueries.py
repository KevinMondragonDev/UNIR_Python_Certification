"""
FASE 6 — Ejemplo 4: CTEs y Subqueries
========================================
Aprenderás a:
  - Escribir CTEs con WITH
  - Usar subqueries en WHERE, FROM y SELECT
  - Combinar CTEs con JOINs
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CTEs_Subqueries") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True) \
     .createOrReplaceTempView("empleados")
spark.read.csv(os.path.join(CSV_DIR, "sales.csv"),    header=True, inferSchema=True) \
     .createOrReplaceTempView("ventas")
spark.read.csv(os.path.join(CSV_DIR, "products.csv"), header=True, inferSchema=True) \
     .createOrReplaceTempView("productos")
spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet")) \
     .createOrReplaceTempView("transacciones")

# ─────────────────────────────────────────
# 1. Subquery en WHERE
# ─────────────────────────────────────────
print("=" * 55)
print("1. Subquery en WHERE")
print("=" * 55)

# Empleados con salario mayor al promedio global
spark.sql("""
    SELECT name, department, salary
    FROM empleados
    WHERE salary > (SELECT AVG(salary) FROM empleados WHERE salary IS NOT NULL)
      AND salary IS NOT NULL
    ORDER BY salary DESC
    LIMIT 10
""").show()

# Empleados del departamento más numeroso
spark.sql("""
    SELECT name, department
    FROM empleados
    WHERE department = (
        SELECT department
        FROM empleados
        GROUP BY department
        ORDER BY COUNT(*) DESC
        LIMIT 1
    )
    LIMIT 10
""").show()

# ─────────────────────────────────────────
# 2. Subquery en FROM (tabla derivada)
# ─────────────────────────────────────────
print("=" * 55)
print("2. Subquery en FROM (tabla derivada)")
print("=" * 55)

spark.sql("""
    SELECT dept, avg_sal, total
    FROM (
        SELECT
            department AS dept,
            ROUND(AVG(salary), 2) AS avg_sal,
            COUNT(*) AS total
        FROM empleados
        WHERE salary IS NOT NULL
        GROUP BY department
    ) resumen_dept
    WHERE avg_sal > 60000
    ORDER BY avg_sal DESC
""").show()

# ─────────────────────────────────────────
# 3. Subquery en SELECT (scalar subquery)
# ─────────────────────────────────────────
print("=" * 55)
print("3. Scalar subquery en SELECT")
print("=" * 55)

spark.sql("""
    SELECT
        name,
        department,
        salary,
        (SELECT ROUND(AVG(salary), 2) FROM empleados WHERE salary IS NOT NULL)
            AS avg_global,
        salary - (SELECT AVG(salary) FROM empleados WHERE salary IS NOT NULL)
            AS diff_global
    FROM empleados
    WHERE salary IS NOT NULL
    ORDER BY diff_global DESC
    LIMIT 10
""").show()

# ─────────────────────────────────────────
# 4. CTEs — WITH
# ─────────────────────────────────────────
print("=" * 55)
print("4. CTE básico con WITH")
print("=" * 55)

spark.sql("""
    WITH stats_dept AS (
        SELECT
            department,
            ROUND(AVG(salary), 2) AS avg_sal,
            COUNT(*) AS total
        FROM empleados
        WHERE salary IS NOT NULL
        GROUP BY department
    )
    SELECT *
    FROM stats_dept
    WHERE avg_sal > 60000
    ORDER BY avg_sal DESC
""").show()

# ─────────────────────────────────────────
# 5. CTEs encadenados
# ─────────────────────────────────────────
print("=" * 55)
print("5. Múltiples CTEs encadenados")
print("=" * 55)

spark.sql("""
    WITH
    ventas_por_empleado AS (
        SELECT employee_id, SUM(amount) AS total_ventas, COUNT(*) AS num_ventas
        FROM ventas
        GROUP BY employee_id
    ),
    empleados_con_ventas AS (
        SELECT e.name, e.department, v.total_ventas, v.num_ventas
        FROM empleados e
        INNER JOIN ventas_por_empleado v ON e.employee_id = v.employee_id
    ),
    top_vendedores AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY department ORDER BY total_ventas DESC) AS rn
        FROM empleados_con_ventas
    )
    SELECT department, name, ROUND(total_ventas, 2) AS total_ventas, num_ventas
    FROM top_vendedores
    WHERE rn = 1
    ORDER BY total_ventas DESC
""").show()

# ─────────────────────────────────────────
# 6. EXISTS y NOT EXISTS
# ─────────────────────────────────────────
print("=" * 55)
print("6. EXISTS y NOT EXISTS")
print("=" * 55)

spark.sql("""
    SELECT name, department
    FROM empleados e
    WHERE EXISTS (
        SELECT 1 FROM ventas v
        WHERE v.employee_id = e.employee_id
    )
    LIMIT 10
""").show()

spark.sql("""
    SELECT name, department
    FROM empleados e
    WHERE NOT EXISTS (
        SELECT 1 FROM ventas v
        WHERE v.employee_id = e.employee_id
    )
    LIMIT 10
""").show()

# ─────────────────────────────────────────
# 7. IN vs EXISTS — cuándo usar cada uno
# ─────────────────────────────────────────
print("=" * 55)
print("7. IN vs JOIN vs EXISTS — resultados equivalentes")
print("=" * 55)

print("Con IN:")
spark.sql("""
    SELECT name FROM empleados
    WHERE employee_id IN (SELECT employee_id FROM ventas)
    LIMIT 5
""").show()

print("Con JOIN (semi join):")
spark.sql("""
    SELECT DISTINCT e.name FROM empleados e
    INNER JOIN ventas v ON e.employee_id = v.employee_id
    LIMIT 5
""").show()

spark.stop()
print("\n✅ CTEs y Subqueries demostrados")
