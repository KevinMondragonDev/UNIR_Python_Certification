"""
FASE 7 — Ejemplo 2: Explain Plan
===================================
Aprenderás a leer el plan de ejecución de Spark
para entender cómo optimiza tus queries.
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Explain_Plan") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

df_emp   = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True)
df_sales = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"),    header=True, inferSchema=True)
df_emp.createOrReplaceTempView("empleados")
df_sales.createOrReplaceTempView("ventas")

# ─────────────────────────────────────────
# 1. explain() básico — DataFrame API
# ─────────────────────────────────────────
print("=" * 55)
print("1. explain() — plan físico (DataFrame API)")
print("=" * 55)

df_simple = df_emp.filter(F.col("salary") > 60000).select("name", "salary")
print("Query: filter(salary > 60000).select(name, salary)")
df_simple.explain()

print("\nNodos del plan físico:")
print("""
  *(1) Project       → SELECT — elige columnas
     *(1) Filter     → WHERE  — aplica filtro
        FileScan     → lee el archivo CSV/Parquet
""")

# ─────────────────────────────────────────
# 2. explain("extended") — plan lógico + físico
# ─────────────────────────────────────────
print("=" * 55)
print("2. explain('extended') — plan lógico + físico")
print("=" * 55)

df_emp.groupBy("department") \
      .agg(F.avg("salary").alias("avg_sal")) \
      .explain("extended")

# ─────────────────────────────────────────
# 3. explain("formatted") — más legible
# ─────────────────────────────────────────
print("=" * 55)
print("3. explain('formatted') — formato árbol")
print("=" * 55)

df_join = df_emp.join(df_sales, "employee_id", "inner")
df_join.explain("formatted")

# ─────────────────────────────────────────
# 4. Interpretar nodos comunes
# ─────────────────────────────────────────
print("=" * 55)
print("4. Glosario de nodos del plan")
print("=" * 55)

print("""
NODO              SIGNIFICADO
─────────────────────────────────────────────────────
FileScan          Leer archivo (CSV, Parquet, etc.)
Filter            Aplicar WHERE — preferible cerca de FileScan
Project           Seleccionar columnas (SELECT)
HashAggregate     GROUP BY con hash (2 fases: parcial + final)
Exchange          Shuffle de datos entre particiones
SortMergeJoin     Join con ordenamiento — costoso (shuffle)
BroadcastHashJoin Join con broadcast — muy eficiente
BroadcastExchange Enviar tabla pequeña a todos los executors
Sort              ORDER BY
TakeOrderedAndProject LIMIT + ORDER BY optimizado
*(n)              n = ID del stage
""")

# ─────────────────────────────────────────
# 5. Diferencia: con y sin broadcast
# ─────────────────────────────────────────
print("=" * 55)
print("5. Sin broadcast vs Con broadcast — diferencia en plan")
print("=" * 55)

# SIN broadcast (SortMergeJoin)
print("--- Sin broadcast (SortMergeJoin) ---")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # deshabilitar
df_emp.join(df_sales, "employee_id").explain()

# CON broadcast (BroadcastHashJoin)
print("\n--- Con broadcast (BroadcastHashJoin) ---")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)  # 10MB
df_emp.join(df_sales, "employee_id").explain()

# ─────────────────────────────────────────
# 6. EXPLAIN en spark.sql()
# ─────────────────────────────────────────
print("=" * 55)
print("6. EXPLAIN directamente en SQL")
print("=" * 55)

spark.sql("""
    EXPLAIN
    SELECT department, AVG(salary) as avg_sal
    FROM empleados
    WHERE salary IS NOT NULL
    GROUP BY department
    ORDER BY avg_sal DESC
""").show(truncate=False)

spark.stop()
print("\n✅ Explain Plan demostrado")
