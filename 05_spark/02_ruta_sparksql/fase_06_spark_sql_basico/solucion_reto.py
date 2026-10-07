"""
FASE 6 — SOLUCIÓN DEL RETO: Análisis Completo en Spark SQL
============================================================
⚠️  INTENTA RESOLVER EL RETO ANTES DE VER ESTO ⚠️
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Solucion_Reto_Fase6") \
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
spark.read.csv(os.path.join(CSV_DIR, "customers.csv"),header=True, inferSchema=True) \
     .createOrReplaceTempView("clientes")
spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet")) \
     .createOrReplaceTempView("transacciones")

print("1. Top 5 departamentos por masa salarial:")
spark.sql("""
    SELECT department, ROUND(SUM(salary),2) AS masa_salarial
    FROM empleados WHERE salary IS NOT NULL
    GROUP BY department ORDER BY masa_salarial DESC LIMIT 5
""").show()

print("2. Categoría de producto con más ingresos:")
spark.sql("""
    SELECT p.category, ROUND(SUM(v.amount),2) AS total_ingresos
    FROM ventas v INNER JOIN productos p ON v.product_id = p.product_id
    GROUP BY p.category ORDER BY total_ingresos DESC
""").show()

print("3. Clientes activos por país:")
spark.sql("""
    SELECT c.country, COUNT(DISTINCT c.customer_id) AS clientes_activos
    FROM clientes c
    INNER JOIN transacciones t ON c.customer_id = t.customer_id
    GROUP BY c.country ORDER BY clientes_activos DESC
""").show()

print("4. Promedio mensual de transacciones por moneda (promedio > 500):")
spark.sql("""
    WITH mensual AS (
        SELECT currency,
               DATE_FORMAT(ts, 'yyyy-MM') AS mes,
               ROUND(AVG(amount), 2) AS avg_mes
        FROM transacciones
        WHERE status = 'completed'
        GROUP BY currency, DATE_FORMAT(ts, 'yyyy-MM')
    )
    SELECT * FROM mensual WHERE avg_mes > 500
    ORDER BY currency, mes
""").show(15)

print("5. Empleados sin ninguna venta:")
spark.sql("""
    SELECT name, department
    FROM empleados e
    WHERE NOT EXISTS (SELECT 1 FROM ventas v WHERE v.employee_id = e.employee_id)
    LIMIT 10
""").show()

print("6. Departamento con mín/máx/avg salary y mejor pagado:")
spark.sql("""
    WITH stats AS (
        SELECT department,
               MIN(salary) AS sal_min,
               ROUND(AVG(salary),2) AS sal_avg,
               MAX(salary) AS sal_max
        FROM empleados WHERE salary IS NOT NULL
        GROUP BY department
    ),
    top_emp AS (
        SELECT department, name AS mejor_pagado, salary,
               ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn
        FROM empleados WHERE salary IS NOT NULL
    )
    SELECT s.department, s.sal_min, s.sal_avg, s.sal_max, t.mejor_pagado
    FROM stats s JOIN top_emp t ON s.department = t.department AND t.rn = 1
    ORDER BY s.sal_avg DESC
""").show()

print("7. Porcentaje de transacciones por status:")
spark.sql("""
    SELECT status,
           COUNT(*) AS total,
           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS porcentaje
    FROM transacciones
    GROUP BY status ORDER BY total DESC
""").show()

print("8. Mes con más ventas:")
spark.sql("""
    SELECT MONTH(TO_DATE(sale_date,'yyyy-MM-dd')) AS mes,
           ROUND(SUM(amount),2) AS total_ventas
    FROM ventas
    GROUP BY mes ORDER BY total_ventas DESC
    LIMIT 3
""").show()

print("9 BONUS — Empleado más valioso:")
spark.sql("""
    WITH ventas_empleado AS (
        SELECT employee_id, ROUND(SUM(amount),2) AS total_ventas, COUNT(*) AS num_ventas
        FROM ventas GROUP BY employee_id
    ),
    empleados_info AS (
        SELECT e.employee_id, e.name, e.department, e.country, v.total_ventas, v.num_ventas
        FROM empleados e INNER JOIN ventas_empleado v ON e.employee_id = v.employee_id
    ),
    ranking AS (
        SELECT *, ROW_NUMBER() OVER (ORDER BY total_ventas DESC) AS rn
        FROM empleados_info
    )
    SELECT name, department, country, total_ventas, num_ventas
    FROM ranking WHERE rn = 1
""").show()

spark.stop()
print("\n✅ Reto Fase 6 completado")
