"""
FASE 7 — Ejemplo 1: Window Functions en SQL
============================================
Ranking, LAG/LEAD, sumas acumuladas y promedios móviles en SQL puro.
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Window_SQL") \
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
spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet")) \
     .createOrReplaceTempView("transacciones")

# ─────────────────────────────────────────
# 1. RANK, DENSE_RANK, ROW_NUMBER
# ─────────────────────────────────────────
print("=" * 55)
print("1. RANK vs DENSE_RANK vs ROW_NUMBER")
print("=" * 55)

spark.sql("""
    SELECT
        department, name, salary,
        RANK()        OVER (PARTITION BY department ORDER BY salary DESC) AS rnk,
        DENSE_RANK()  OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rnk,
        ROW_NUMBER()  OVER (PARTITION BY department ORDER BY salary DESC) AS row_num
    FROM empleados
    WHERE salary IS NOT NULL
    ORDER BY department, rnk
""").show(20)

# ─────────────────────────────────────────
# 2. Top-N por grupo con ROW_NUMBER
# ─────────────────────────────────────────
print("=" * 55)
print("2. Top-2 por departamento")
print("=" * 55)

spark.sql("""
    WITH ranked AS (
        SELECT
            department, name, salary,
            ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn
        FROM empleados
        WHERE salary IS NOT NULL
    )
    SELECT department, name, ROUND(salary, 2) AS salary
    FROM ranked
    WHERE rn <= 2
    ORDER BY department, salary DESC
""").show(20)

# ─────────────────────────────────────────
# 3. PERCENT_RANK y NTILE (cuartiles)
# ─────────────────────────────────────────
print("=" * 55)
print("3. PERCENT_RANK y NTILE — cuartiles salariales")
print("=" * 55)

spark.sql("""
    SELECT
        name, salary,
        ROUND(PERCENT_RANK() OVER (ORDER BY salary), 3) AS pct_rank,
        NTILE(4) OVER (ORDER BY salary)                 AS cuartil
    FROM empleados
    WHERE salary IS NOT NULL
    ORDER BY salary
    LIMIT 20
""").show()

# ─────────────────────────────────────────
# 4. LAG y LEAD — análisis temporal
# ─────────────────────────────────────────
print("=" * 55)
print("4. LAG() y LEAD() — ventas mensuales por región")
print("=" * 55)

spark.sql("""
    WITH ventas_mes AS (
        SELECT
            region,
            DATE_FORMAT(TO_DATE(sale_date, 'yyyy-MM-dd'), 'yyyy-MM') AS mes,
            ROUND(SUM(amount), 2) AS total
        FROM ventas
        GROUP BY region, DATE_FORMAT(TO_DATE(sale_date, 'yyyy-MM-dd'), 'yyyy-MM')
    )
    SELECT
        region, mes, total,
        LAG(total, 1)  OVER (PARTITION BY region ORDER BY mes) AS mes_anterior,
        LEAD(total, 1) OVER (PARTITION BY region ORDER BY mes) AS mes_siguiente,
        ROUND(
            (total - LAG(total, 1) OVER (PARTITION BY region ORDER BY mes))
            / LAG(total, 1) OVER (PARTITION BY region ORDER BY mes) * 100
        , 2) AS crecimiento_pct
    FROM ventas_mes
    ORDER BY region, mes
    LIMIT 20
""").show()

# ─────────────────────────────────────────
# 5. FIRST_VALUE y LAST_VALUE
# ─────────────────────────────────────────
print("=" * 55)
print("5. FIRST_VALUE y LAST_VALUE")
print("=" * 55)

spark.sql("""
    SELECT
        department, name, salary,
        FIRST_VALUE(name) OVER (PARTITION BY department ORDER BY salary DESC) AS mejor_pagado,
        FIRST_VALUE(name) OVER (PARTITION BY department ORDER BY salary ASC)  AS menor_pagado
    FROM empleados
    WHERE salary IS NOT NULL
    ORDER BY department
    LIMIT 15
""").show()

# ─────────────────────────────────────────
# 6. Suma acumulada (Running Total)
# ─────────────────────────────────────────
print("=" * 55)
print("6. Suma acumulada — ROWS BETWEEN")
print("=" * 55)

spark.sql("""
    WITH ventas_mes AS (
        SELECT
            region,
            DATE_FORMAT(TO_DATE(sale_date, 'yyyy-MM-dd'), 'yyyy-MM') AS mes,
            ROUND(SUM(amount), 2) AS total
        FROM ventas
        GROUP BY region, DATE_FORMAT(TO_DATE(sale_date, 'yyyy-MM-dd'), 'yyyy-MM')
    )
    SELECT
        region, mes, total,
        ROUND(SUM(total) OVER (
            PARTITION BY region
            ORDER BY mes
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2) AS total_acumulado,
        ROUND(AVG(total) OVER (
            PARTITION BY region
            ORDER BY mes
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2) AS prom_movil_3m
    FROM ventas_mes
    WHERE region = 'Norte'
    ORDER BY region, mes
""").show()

# ─────────────────────────────────────────
# 7. Diferencia con el promedio del grupo
# ─────────────────────────────────────────
print("=" * 55)
print("7. Diferencia salarial vs promedio del departamento")
print("=" * 55)

spark.sql("""
    SELECT
        name, department, salary,
        ROUND(AVG(salary) OVER (PARTITION BY department), 2) AS avg_dept,
        ROUND(salary - AVG(salary) OVER (PARTITION BY department), 2) AS diff_avg,
        CASE
            WHEN salary > AVG(salary) OVER (PARTITION BY department) THEN 'Sobre promedio'
            ELSE 'Bajo promedio'
        END AS status_sal
    FROM empleados
    WHERE salary IS NOT NULL
    ORDER BY department, diff_avg DESC
    LIMIT 20
""").show()

spark.stop()
print("\n✅ Window Functions SQL demostradas")
