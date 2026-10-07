"""
FASE 7 — Ejemplo 3: Arrays y Structs en SQL
============================================
Aprenderás a trabajar con tipos complejos (Array, Map, Struct) en SQL.
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Arrays_Structs_SQL") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")
CSV_DIR  = os.path.join(BASE, "datasets", "csv")

spark.read.parquet(os.path.join(PARQ_DIR, "events.parquet")) \
     .createOrReplaceTempView("eventos")
spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True) \
     .createOrReplaceTempView("empleados")

# ─────────────────────────────────────────
# 1. Inspeccionar schema de la tabla events
# ─────────────────────────────────────────
print("=" * 55)
print("1. Schema con tipos complejos")
print("=" * 55)

spark.sql("DESCRIBE eventos").show()
spark.sql("SELECT * FROM eventos LIMIT 3").show(truncate=False)

# ─────────────────────────────────────────
# 2. Acceder a elementos del Array
# ─────────────────────────────────────────
print("=" * 55)
print("2. Acceder a elementos de Array")
print("=" * 55)

spark.sql("""
    SELECT
        event_id,
        tags,
        tags[0]        AS primer_tag,
        tags[1]        AS segundo_tag,
        SIZE(tags)     AS num_tags
    FROM eventos
    LIMIT 8
""").show(truncate=False)

# ─────────────────────────────────────────
# 3. ARRAY_CONTAINS — filtrar por elemento
# ─────────────────────────────────────────
print("=" * 55)
print("3. ARRAY_CONTAINS")
print("=" * 55)

spark.sql("""
    SELECT event_id, user_id, event_type, tags
    FROM eventos
    WHERE ARRAY_CONTAINS(tags, 'promo')
    LIMIT 10
""").show(truncate=False)

spark.sql("""
    SELECT COUNT(*) as eventos_con_promo
    FROM eventos
    WHERE ARRAY_CONTAINS(tags, 'promo')
""").show()

# ─────────────────────────────────────────
# 4. EXPLODE — desanidar array (una fila por elemento)
# ─────────────────────────────────────────
print("=" * 55)
print("4. EXPLODE — una fila por tag")
print("=" * 55)

spark.sql("""
    SELECT event_id, user_id, EXPLODE(tags) AS tag
    FROM eventos
    LIMIT 15
""").show()

# Contar frecuencia de tags
spark.sql("""
    SELECT tag, COUNT(*) AS frecuencia
    FROM (
        SELECT EXPLODE(tags) AS tag FROM eventos
    )
    GROUP BY tag
    ORDER BY frecuencia DESC
""").show()

# ─────────────────────────────────────────
# 5. EXPLODE_OUTER — mantiene filas con array vacío/nulo
# ─────────────────────────────────────────
print("=" * 55)
print("5. POSEXPLODE — con índice de posición")
print("=" * 55)

spark.sql("""
    SELECT event_id, pos, tag
    FROM eventos
    LATERAL VIEW POSEXPLODE(tags) t AS pos, tag
    LIMIT 10
""").show()

# ─────────────────────────────────────────
# 6. COLLECT_LIST y COLLECT_SET en GROUP BY
# ─────────────────────────────────────────
print("=" * 55)
print("6. COLLECT_LIST y COLLECT_SET")
print("=" * 55)

spark.sql("""
    SELECT
        user_id,
        COLLECT_LIST(event_type) AS todos_eventos,
        COLLECT_SET(event_type)  AS eventos_unicos,
        COUNT(*) AS total_eventos
    FROM eventos
    GROUP BY user_id
    ORDER BY total_eventos DESC
    LIMIT 5
""").show(truncate=False)

# ─────────────────────────────────────────
# 7. ARRAY_DISTINCT, ARRAY_SORT, ARRAY_UNION
# ─────────────────────────────────────────
print("=" * 55)
print("7. Funciones de Array — ARRAY_DISTINCT, ARRAY_SORT")
print("=" * 55)

spark.sql("""
    SELECT
        event_id,
        tags,
        SORT_ARRAY(tags)          AS tags_ordenados,
        ARRAY_DISTINCT(tags)      AS tags_unicos,
        SIZE(ARRAY_DISTINCT(tags)) AS num_unicos
    FROM eventos
    LIMIT 5
""").show(truncate=False)

# ─────────────────────────────────────────
# 8. Crear arrays y structs en SELECT
# ─────────────────────────────────────────
print("=" * 55)
print("8. Crear STRUCT y ARRAY en SELECT")
print("=" * 55)

spark.sql("""
    SELECT
        name,
        STRUCT(name, department, salary) AS info_empleado,
        ARRAY(name, department, country) AS datos_lista
    FROM empleados
    WHERE salary IS NOT NULL
    LIMIT 5
""").show(truncate=False)

# Acceder a campo de struct
spark.sql("""
    SELECT
        info.name,
        info.department,
        info.salary
    FROM (
        SELECT STRUCT(name, department, salary) AS info
        FROM empleados
        WHERE salary IS NOT NULL
    )
    LIMIT 5
""").show()

spark.stop()
print("\n✅ Arrays y Structs en SQL demostrados")
