"""
FASE 6 — Ejemplo 2: Queries SQL Básicas
=========================================
SELECT, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, JOINS en SQL
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Queries_Basicas_SQL") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

# Registrar vistas
spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True) \
     .createOrReplaceTempView("empleados")
spark.read.csv(os.path.join(CSV_DIR, "sales.csv"),    header=True, inferSchema=True) \
     .createOrReplaceTempView("ventas")
spark.read.csv(os.path.join(CSV_DIR, "products.csv"), header=True, inferSchema=True) \
     .createOrReplaceTempView("productos")
spark.read.csv(os.path.join(CSV_DIR, "customers.csv"),header=True, inferSchema=True) \
     .createOrReplaceTempView("clientes")

# ─────────────────────────────────────────
# 1. SELECT básico
# ─────────────────────────────────────────
print("=" * 55)
print("1. SELECT")
print("=" * 55)

spark.sql("SELECT name, department, salary FROM empleados LIMIT 5").show()
spark.sql("SELECT DISTINCT department FROM empleados ORDER BY department").show()
spark.sql("SELECT COUNT(*) as total FROM empleados").show()

# ─────────────────────────────────────────
# 2. WHERE — filtros
# ─────────────────────────────────────────
print("=" * 55)
print("2. WHERE")
print("=" * 55)

spark.sql("""
    SELECT name, department, salary, country
    FROM empleados
    WHERE salary > 80000
      AND department = 'Engineering'
""").show(10)

spark.sql("""
    SELECT name, salary
    FROM empleados
    WHERE salary BETWEEN 50000 AND 70000
    ORDER BY salary DESC
    LIMIT 10
""").show()

spark.sql("""
    SELECT name, country
    FROM empleados
    WHERE country IN ('Mexico', 'USA', 'Colombia')
    ORDER BY country
""").show(10)

spark.sql("""
    SELECT name, salary
    FROM empleados
    WHERE salary IS NULL
    LIMIT 5
""").show()

# LIKE
spark.sql("""
    SELECT name FROM empleados
    WHERE name LIKE 'Ana%'
    LIMIT 5
""").show()

# ─────────────────────────────────────────
# 3. GROUP BY + funciones de agregación
# ─────────────────────────────────────────
print("=" * 55)
print("3. GROUP BY + agregaciones")
print("=" * 55)

spark.sql("""
    SELECT
        department,
        COUNT(*)                          AS total,
        ROUND(AVG(salary), 2)             AS avg_salary,
        MAX(salary)                       AS max_salary,
        MIN(salary)                       AS min_salary,
        ROUND(SUM(salary), 2)             AS masa_salarial,
        COUNT(DISTINCT country)           AS paises
    FROM empleados
    WHERE salary IS NOT NULL
    GROUP BY department
    ORDER BY avg_salary DESC
""").show()

# ─────────────────────────────────────────
# 4. HAVING — filtrar grupos
# ─────────────────────────────────────────
print("=" * 55)
print("4. HAVING")
print("=" * 55)

spark.sql("""
    SELECT department, COUNT(*) as total, AVG(salary) as avg_sal
    FROM empleados
    WHERE salary IS NOT NULL
    GROUP BY department
    HAVING COUNT(*) >= 30 AND AVG(salary) > 60000
    ORDER BY avg_sal DESC
""").show()

# ─────────────────────────────────────────
# 5. ORDER BY y LIMIT
# ─────────────────────────────────────────
print("=" * 55)
print("5. ORDER BY y LIMIT")
print("=" * 55)

spark.sql("""
    SELECT name, salary, department
    FROM empleados
    WHERE salary IS NOT NULL
    ORDER BY salary DESC
    LIMIT 10
""").show()

# Múltiples columnas
spark.sql("""
    SELECT department, country, ROUND(AVG(salary),2) as avg_sal
    FROM empleados
    WHERE salary IS NOT NULL
    GROUP BY department, country
    ORDER BY department ASC, avg_sal DESC
""").show(15)

# ─────────────────────────────────────────
# 6. JOINS en SQL
# ─────────────────────────────────────────
print("=" * 55)
print("6. JOINs en SQL")
print("=" * 55)

# INNER JOIN
spark.sql("""
    SELECT v.sale_id, e.name, v.amount, v.region, v.sale_date
    FROM ventas v
    INNER JOIN empleados e ON v.employee_id = e.employee_id
    LIMIT 10
""").show()

# LEFT JOIN
spark.sql("""
    SELECT e.name, e.department, COUNT(v.sale_id) as num_ventas
    FROM empleados e
    LEFT JOIN ventas v ON e.employee_id = v.employee_id
    WHERE e.salary IS NOT NULL
    GROUP BY e.name, e.department
    ORDER BY num_ventas DESC
    LIMIT 10
""").show()

# JOIN 3 tablas
spark.sql("""
    SELECT
        e.name          AS vendedor,
        e.department,
        p.category,
        ROUND(SUM(v.amount), 2) AS total_ventas
    FROM ventas v
    INNER JOIN empleados e  ON v.employee_id = e.employee_id
    INNER JOIN productos  p ON v.product_id  = p.product_id
    GROUP BY e.name, e.department, p.category
    ORDER BY total_ventas DESC
    LIMIT 10
""").show()

spark.stop()
print("\n✅ Queries SQL básicas demostradas")
