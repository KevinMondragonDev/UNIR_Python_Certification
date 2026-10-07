"""
FASE 9 — Módulo 4: Análisis con Spark SQL
==========================================
Responde todas las preguntas de negocio usando spark.sql()
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def registrar_vistas(spark, clean):
    for nombre, df in clean.items():
        df.createOrReplaceTempView(nombre)
    print("  Vistas registradas:", list(clean.keys()))


def analisis_ventas(spark):
    print("\n--- MÓDULO VENTAS ---")

    print("\n[Q1] Top 5 vendedores por ingresos totales:")
    spark.sql("""
        WITH ventas_emp AS (
            SELECT employee_id, ROUND(SUM(amount), 2) AS total_ventas, COUNT(*) AS num_ventas
            FROM ventas GROUP BY employee_id
        )
        SELECT e.name, e.department, v.total_ventas, v.num_ventas
        FROM ventas_emp v
        INNER JOIN empleados e ON v.employee_id = e.employee_id
        ORDER BY v.total_ventas DESC
        LIMIT 5
    """).show()

    print("\n[Q2] Crecimiento MoM por región:")
    spark.sql("""
        WITH vm AS (
            SELECT region, anio_mes, ROUND(SUM(amount), 2) AS total
            FROM ventas GROUP BY region, anio_mes
        ),
        con_lag AS (
            SELECT region, anio_mes, total,
                   LAG(total) OVER (PARTITION BY region ORDER BY anio_mes) AS mes_ant
            FROM vm
        )
        SELECT region, anio_mes, total,
               ROUND((total - mes_ant) / mes_ant * 100, 2) AS crecimiento_pct
        FROM con_lag
        WHERE mes_ant IS NOT NULL
        ORDER BY crecimiento_pct DESC
        LIMIT 10
    """).show()

    print("\n[Q3] Producto más vendido por categoría:")
    spark.sql("""
        WITH ventas_prod AS (
            SELECT p.category, p.name AS producto, ROUND(SUM(v.amount), 2) AS total,
                   ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY SUM(v.amount) DESC) AS rn
            FROM ventas v INNER JOIN productos p ON v.product_id = p.product_id
            GROUP BY p.category, p.name
        )
        SELECT category, producto, total
        FROM ventas_prod WHERE rn = 1
        ORDER BY total DESC
    """).show()


def analisis_clientes(spark):
    print("\n--- MÓDULO CLIENTES ---")

    print("\n[Q4] Clientes activos por país (con al menos 1 transacción):")
    spark.sql("""
        SELECT c.country, COUNT(DISTINCT c.customer_id) AS clientes_activos
        FROM clientes c
        INNER JOIN transacciones t ON c.customer_id = t.customer_id
        GROUP BY c.country
        ORDER BY clientes_activos DESC
    """).show()

    print("\n[Q5] LTV promedio por cliente (transacciones completadas):")
    spark.sql("""
        WITH ltv AS (
            SELECT customer_id, ROUND(SUM(amount), 2) AS gasto_total
            FROM transacciones WHERE status = 'completed'
            GROUP BY customer_id
        )
        SELECT ROUND(AVG(gasto_total), 2) AS ltv_promedio,
               MAX(gasto_total) AS ltv_max,
               MIN(gasto_total) AS ltv_min
        FROM ltv
    """).show()

    print("\n[Q6] Segmentación de clientes por gasto:")
    spark.sql("""
        WITH gasto AS (
            SELECT customer_id, ROUND(SUM(amount), 2) AS total
            FROM transacciones WHERE status = 'completed'
            GROUP BY customer_id
        ),
        percentiles AS (
            SELECT
                PERCENTILE_APPROX(total, 0.33) AS p33,
                PERCENTILE_APPROX(total, 0.66) AS p66
            FROM gasto
        )
        SELECT
            CASE
                WHEN g.total > p.p66 THEN 'Alto Valor'
                WHEN g.total > p.p33 THEN 'Medio'
                ELSE 'Bajo'
            END AS segmento,
            COUNT(*) AS num_clientes,
            ROUND(AVG(g.total), 2) AS gasto_promedio
        FROM gasto g CROSS JOIN percentiles p
        GROUP BY
            CASE WHEN g.total > p.p66 THEN 'Alto Valor'
                 WHEN g.total > p.p33 THEN 'Medio' ELSE 'Bajo' END
        ORDER BY gasto_promedio DESC
    """).show()


def analisis_empleados(spark):
    print("\n--- MÓDULO EMPLEADOS ---")

    print("\n[Q7] Eficiencia de ventas por departamento (ventas/empleado):")
    spark.sql("""
        WITH ventas_dept AS (
            SELECT e.department,
                   COUNT(DISTINCT e.employee_id)     AS num_empleados,
                   ROUND(SUM(v.amount), 2)           AS total_ventas
            FROM empleados e
            LEFT JOIN ventas v ON e.employee_id = v.employee_id
            GROUP BY e.department
        )
        SELECT department, num_empleados, total_ventas,
               ROUND(total_ventas / num_empleados, 2) AS ventas_por_empleado
        FROM ventas_dept
        ORDER BY ventas_por_empleado DESC
    """).show()

    print("\n[Q8] Distribución salarial por departamento y nivel:")
    spark.sql("""
        SELECT department, nivel,
               COUNT(*)                    AS total,
               ROUND(MIN(salary), 2)       AS sal_min,
               ROUND(AVG(salary), 2)       AS sal_avg,
               ROUND(MAX(salary), 2)       AS sal_max
        FROM empleados
        GROUP BY department, nivel
        ORDER BY department, sal_avg DESC
    """).show(20)


def ejecutar_analisis(spark, clean):
    print("\n=== Análisis SQL ===")
    registrar_vistas(spark, clean)
    analisis_ventas(spark)
    analisis_clientes(spark)
    analisis_empleados(spark)
    print("\n✅ Análisis completado")


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from _modulos import cargar
    ingesta = cargar("01_ingesta")
    limpieza = cargar("02_limpieza")

    spark = ingesta.crear_spark()
    spark.sparkContext.setLogLevel("ERROR")
    BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    clean = limpieza.limpiar_todos(ingesta.cargar_datasets(spark, BASE))
    ejecutar_analisis(spark, clean)
    spark.stop()
