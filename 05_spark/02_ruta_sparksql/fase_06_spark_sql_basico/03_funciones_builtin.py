"""
FASE 6 — Ejemplo 3: Funciones Built-in en SQL
===============================================
String, fecha, matemáticas, condicionales y nulos en SQL puro.
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Funciones_Builtin_SQL") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True) \
     .createOrReplaceTempView("empleados")
spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet")) \
     .createOrReplaceTempView("transacciones")
spark.read.csv(os.path.join(CSV_DIR, "sales.csv"), header=True, inferSchema=True) \
     .createOrReplaceTempView("ventas")

# ─────────────────────────────────────────
# 1. Funciones de String
# ─────────────────────────────────────────
print("=" * 55)
print("1. Funciones de String")
print("=" * 55)

spark.sql("""
    SELECT
        name,
        UPPER(name)                           AS nombre_upper,
        LOWER(name)                           AS nombre_lower,
        LENGTH(name)                          AS longitud,
        TRIM(name)                            AS sin_espacios,
        SUBSTR(name, 1, 4)                    AS primeras_4,
        CONCAT(name, ' [', department, ']')   AS nombre_dept,
        CONCAT_WS(' - ', department, country) AS dept_pais,
        REPLACE(department, 'Engineering', 'Eng') AS dept_corto
    FROM empleados
    LIMIT 8
""").show(truncate=False)

# SPLIT — acceder a partes
spark.sql("""
    SELECT
        name,
        SPLIT(name, ' ')[0] AS primer_nombre,
        SPLIT(name, ' ')[1] AS apellido
    FROM empleados
    LIMIT 5
""").show()

# REGEXP_REPLACE
spark.sql("""
    SELECT name, REGEXP_REPLACE(name, '[aeiouAEIOU]', '*') AS sin_vocales
    FROM empleados LIMIT 5
""").show()

# ─────────────────────────────────────────
# 2. Funciones de Fecha
# ─────────────────────────────────────────
print("=" * 55)
print("2. Funciones de Fecha")
print("=" * 55)

spark.sql("""
    SELECT
        name,
        hire_date,
        TO_DATE(hire_date, 'yyyy-MM-dd')                AS hire_date_dt,
        YEAR(TO_DATE(hire_date, 'yyyy-MM-dd'))          AS anio,
        MONTH(TO_DATE(hire_date, 'yyyy-MM-dd'))         AS mes,
        DAY(TO_DATE(hire_date, 'yyyy-MM-dd'))           AS dia,
        DAYOFWEEK(TO_DATE(hire_date, 'yyyy-MM-dd'))     AS dia_semana,
        DATE_FORMAT(TO_DATE(hire_date, 'yyyy-MM-dd'), 'MMM yyyy') AS mes_anio,
        DATEDIFF(CURRENT_DATE(), TO_DATE(hire_date, 'yyyy-MM-dd')) AS dias_empresa
    FROM empleados
    LIMIT 5
""").show(truncate=False)

# Sobre timestamps (parquet)
spark.sql("""
    SELECT
        tx_id,
        ts,
        YEAR(ts)           AS anio,
        MONTH(ts)          AS mes,
        HOUR(ts)           AS hora,
        DATE_FORMAT(ts, 'EEEE') AS dia_semana,
        DATE_TRUNC('month', ts) AS inicio_mes
    FROM transacciones
    LIMIT 5
""").show(truncate=False)

# ─────────────────────────────────────────
# 3. Funciones Matemáticas
# ─────────────────────────────────────────
print("=" * 55)
print("3. Funciones Matemáticas")
print("=" * 55)

spark.sql("""
    SELECT
        name,
        salary,
        ROUND(salary, -3)          AS redondeado_miles,
        CEIL(salary / 1000.0)      AS miles_ceil,
        FLOOR(salary / 1000.0)     AS miles_floor,
        ABS(salary - 60000)        AS diff_60k,
        MOD(CAST(salary AS INT), 1000) AS residuo_1000,
        SQRT(salary)               AS raiz,
        POW(salary / 10000, 2)     AS potencia
    FROM empleados
    WHERE salary IS NOT NULL
    LIMIT 5
""").show()

# ─────────────────────────────────────────
# 4. Condicionales — CASE WHEN, IF, COALESCE
# ─────────────────────────────────────────
print("=" * 55)
print("4. Condicionales")
print("=" * 55)

spark.sql("""
    SELECT
        name,
        salary,
        CASE
            WHEN salary < 40000 THEN 'Junior'
            WHEN salary < 60000 THEN 'Mid'
            WHEN salary < 90000 THEN 'Senior'
            WHEN salary >= 90000 THEN 'Lead'
            ELSE 'Sin dato'
        END AS nivel,
        IF(salary > 70000, 'Alto', 'Normal')        AS categoria,
        COALESCE(salary, 0)                         AS salary_safe,
        IFNULL(CAST(salary AS STRING), 'N/A')       AS salary_str
    FROM empleados
    LIMIT 10
""").show()

# ─────────────────────────────────────────
# 5. Funciones de agregación avanzadas
# ─────────────────────────────────────────
print("=" * 55)
print("5. Funciones de agregación en SQL")
print("=" * 55)

spark.sql("""
    SELECT
        department,
        COUNT(*)                        AS total,
        COUNT(salary)                   AS con_salary,
        COUNT(*) - COUNT(salary)        AS sin_salary,
        ROUND(AVG(salary), 2)           AS avg_sal,
        ROUND(STDDEV(salary), 2)        AS stddev_sal,
        PERCENTILE_APPROX(salary, 0.5)  AS mediana_sal
    FROM empleados
    GROUP BY department
    ORDER BY avg_sal DESC
""").show()

# ─────────────────────────────────────────
# 6. Combinar funciones
# ─────────────────────────────────────────
print("=" * 55)
print("6. Query combinada — análisis por región y mes")
print("=" * 55)

spark.sql("""
    SELECT
        region,
        DATE_FORMAT(TO_DATE(sale_date, 'yyyy-MM-dd'), 'yyyy-MM') AS mes,
        COUNT(*)                              AS num_ventas,
        ROUND(SUM(amount), 2)                AS total,
        ROUND(AVG(amount), 2)                AS promedio,
        ROUND(MAX(amount), 2)                AS maximo
    FROM ventas
    GROUP BY region, DATE_FORMAT(TO_DATE(sale_date, 'yyyy-MM-dd'), 'yyyy-MM')
    ORDER BY region, mes
    LIMIT 15
""").show()

spark.stop()
print("\n✅ Funciones built-in SQL demostradas")
