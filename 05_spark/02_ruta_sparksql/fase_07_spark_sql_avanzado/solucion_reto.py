"""
FASE 7 — SOLUCIÓN del Reto: Reporte Ejecutivo SQL Avanzado
============================================================
⚠️ Revisa esto SOLO si ya intentaste resolver reto.py por tu cuenta.
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Solucion_Reto_Fase7") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True) \
     .createOrReplaceTempView("empleados")
spark.read.csv(os.path.join(CSV_DIR, "sales.csv"), header=True, inferSchema=True) \
     .createOrReplaceTempView("ventas")
spark.read.parquet(os.path.join(PARQ_DIR, "events.parquet")) \
     .createOrReplaceTempView("eventos")
spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet")) \
     .createOrReplaceTempView("transacciones")

# ═════════════════════════════════════════
# PARTE A — Window Functions
# ═════════════════════════════════════════
print("=" * 60)
print("1. Top 3 salarios por departamento (DENSE_RANK)")
print("=" * 60)
# DENSE_RANK: si dos empatan en el 1°, el siguiente es 2° (RANK saltaría al 3°)
spark.sql("""
    WITH ranked AS (
        SELECT department, name, salary,
               DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rk
        FROM empleados
        WHERE salary IS NOT NULL
    )
    SELECT department, rk, name, salary
    FROM ranked
    WHERE rk <= 3
    ORDER BY department, rk
""").show(30, truncate=False)

print("=" * 60)
print("2. Promedio móvil de 3 meses por región")
print("=" * 60)
# Primero agregamos a nivel mes; la ventana se aplica sobre el resultado agregado
spark.sql("""
    WITH mensual AS (
        SELECT region, DATE_FORMAT(sale_date, 'yyyy-MM') AS mes,
               ROUND(SUM(amount), 2) AS total
        FROM ventas
        GROUP BY region, DATE_FORMAT(sale_date, 'yyyy-MM')
    )
    SELECT region, mes, total,
           ROUND(AVG(total) OVER (
               PARTITION BY region ORDER BY mes
               ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
           ), 2) AS media_movil_3m
    FROM mensual
    ORDER BY region, mes
""").show(12)

print("=" * 60)
print("3. Empleados en el cuartil superior de salario (NTILE)")
print("=" * 60)
spark.sql("""
    WITH cuartiles AS (
        SELECT name, department, salary,
               NTILE(4) OVER (ORDER BY salary DESC) AS cuartil
        FROM empleados
        WHERE salary IS NOT NULL
    )
    SELECT name, department, salary
    FROM cuartiles
    WHERE cuartil = 1
    ORDER BY salary DESC
""").show(10)
# ⚠️ NTILE sin PARTITION BY manda TODOS los datos a una sola partición
#    (verás un warning "No Partition Defined for Window operation").
#    Con millones de filas, usa percentile_approx en su lugar:
spark.sql("""
    SELECT COUNT(*) AS en_cuartil_superior
    FROM empleados
    WHERE salary >= (SELECT percentile_approx(salary, 0.75) FROM empleados)
""").show()

print("=" * 60)
print("4. Diferencia vs el mejor pagado del departamento (FIRST_VALUE)")
print("=" * 60)
spark.sql("""
    SELECT department, name, salary,
           FIRST_VALUE(salary) OVER (PARTITION BY department ORDER BY salary DESC) AS max_dept,
           ROUND(salary - FIRST_VALUE(salary) OVER (
               PARTITION BY department ORDER BY salary DESC), 2) AS diferencia
    FROM empleados
    WHERE salary IS NOT NULL
    ORDER BY department, salary DESC
""").show(10)

# ═════════════════════════════════════════
# PARTE B — Arrays y Structs
# ═════════════════════════════════════════
print("=" * 60)
print("5. Tag más común")
print("=" * 60)
spark.sql("""
    SELECT tag, COUNT(*) AS veces
    FROM eventos
    LATERAL VIEW EXPLODE(tags) t AS tag
    GROUP BY tag
    ORDER BY veces DESC
    LIMIT 1
""").show()

print("=" * 60)
print("6. Usuarios con 'organic' Y 'paid' en un mismo evento")
print("=" * 60)
spark.sql("""
    SELECT DISTINCT user_id
    FROM eventos
    WHERE ARRAY_CONTAINS(tags, 'organic') AND ARRAY_CONTAINS(tags, 'paid')
    ORDER BY user_id
""").show(10)

print("=" * 60)
print("7. Tags distintos promedio por tipo de evento")
print("=" * 60)
# Interpretación: por cada evento contamos sus tags distintos y promediamos por event_type
spark.sql("""
    WITH tags_por_evento AS (
        SELECT event_id, event_type, COUNT(DISTINCT tag) AS n_tags
        FROM eventos
        LATERAL VIEW EXPLODE(tags) t AS tag
        GROUP BY event_id, event_type
    )
    SELECT event_type, ROUND(AVG(n_tags), 2) AS avg_tags_distintos
    FROM tags_por_evento
    GROUP BY event_type
    ORDER BY avg_tags_distintos DESC
""").show()
# Alternativa sin EXPLODE (más eficiente: no multiplica filas):
#   AVG(SIZE(ARRAY_DISTINCT(tags)))

# ═════════════════════════════════════════
# PARTE C — Explain Plan
# ═════════════════════════════════════════
print("=" * 60)
print("8. Top 5 clientes — query eficiente + explain")
print("=" * 60)
top5 = spark.sql("""
    SELECT customer_id, ROUND(SUM(amount), 2) AS total
    FROM transacciones
    WHERE status = 'completed'
    GROUP BY customer_id
    ORDER BY total DESC
    LIMIT 5
""")
top5.show()
top5.explain()
# Lectura del plan (de abajo hacia arriba):
#   - FileScan parquet con PushedFilters [EqualTo(status,completed)] y
#     ReadSchema solo de customer_id, amount, status → predicate + column pruning.
#   - HashAggregate(partial_sum) ANTES del Exchange: agregación parcial local
#     (como reduceByKey) → por la red viaja 1 fila por cliente y partición.
#   - Exchange hashpartitioning(customer_id) → único shuffle para el groupBy.
#   - TakeOrderedAndProject(limit=5): ORDER BY + LIMIT se fusionan; cada
#     partición calcula su top-5 local y el driver combina. No hay sort global.
#   - No hay join: es la forma más barata de responder la pregunta.

# ═════════════════════════════════════════
# BONUS — Cuadro de mando mensual con 4 CTEs
# ═════════════════════════════════════════
print("=" * 60)
print("9. Cuadro de mando mensual por región")
print("=" * 60)
spark.sql("""
    WITH base AS (
        SELECT region, DATE_FORMAT(sale_date, 'yyyy-MM') AS mes, amount
        FROM ventas
    ),
    mensual AS (
        SELECT region, mes, ROUND(SUM(amount), 2) AS total
        FROM base
        GROUP BY region, mes
    ),
    con_lag AS (
        SELECT region, mes, total,
               LAG(total) OVER (PARTITION BY region ORDER BY mes) AS mes_anterior,
               ROUND(AVG(total) OVER (
                   PARTITION BY region ORDER BY mes
                   ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS media_movil_3m
        FROM mensual
    ),
    tablero AS (
        SELECT region, mes, total, media_movil_3m,
               ROUND((total - mes_anterior) / mes_anterior * 100, 2) AS crecimiento_pct,
               RANK() OVER (PARTITION BY mes ORDER BY total DESC) AS ranking_mes
        FROM con_lag
    )
    SELECT *
    FROM tablero
    ORDER BY mes DESC, ranking_mes
""").show(14)

spark.stop()
print("✅ Reto Fase 7 resuelto")
