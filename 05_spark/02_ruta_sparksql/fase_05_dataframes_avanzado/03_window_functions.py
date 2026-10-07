"""
FASE 5 — Ejemplo 3: Window Functions
======================================
Aprenderás:
  - rank, dense_rank, row_number, ntile, percent_rank
  - lag, lead
  - Agregaciones con ventana (suma acumulada, promedio móvil)
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Window_Functions") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

df_emp   = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True)
df_sales = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"),    header=True, inferSchema=True)
df_emp_clean = df_emp.filter(F.col("salary").isNotNull())

# ─────────────────────────────────────────
# 1. rank, dense_rank, row_number
# ─────────────────────────────────────────
print("=" * 55)
print("1. rank vs dense_rank vs row_number")
print("=" * 55)

ventana_dept = Window.partitionBy("department").orderBy(F.col("salary").desc())

df_ranking = df_emp_clean.withColumn("rank",        F.rank().over(ventana_dept)) \
                         .withColumn("dense_rank",  F.dense_rank().over(ventana_dept)) \
                         .withColumn("row_number",  F.row_number().over(ventana_dept))

print("Top 3 por departamento (rank, dense_rank, row_number):")
df_ranking.filter(F.col("rank") <= 3) \
          .select("department", "name", "salary", "rank", "dense_rank", "row_number") \
          .orderBy("department", "rank") \
          .show(25)

# ─────────────────────────────────────────
# 2. Filtrar top-N por grupo (ANTI PATTERN con groupBy)
# ─────────────────────────────────────────
print("=" * 55)
print("2. Top-1 por departamento (el mejor pagado)")
print("=" * 55)

top1_por_dept = df_ranking.filter(F.col("row_number") == 1) \
    .select("department", "name", "salary") \
    .orderBy("department")
top1_por_dept.show()

# ─────────────────────────────────────────
# 3. percent_rank y ntile
# ─────────────────────────────────────────
print("=" * 55)
print("3. percent_rank y ntile")
print("=" * 55)

ventana_global = Window.orderBy(F.col("salary").asc())
ventana_cuartil = Window.orderBy(F.col("salary").asc())

df_emp_clean.withColumn("pct_rank",  F.percent_rank().over(ventana_global)) \
            .withColumn("cuartil",   F.ntile(4).over(ventana_cuartil)) \
            .select("name", "salary",
                    F.round("pct_rank", 3).alias("pct_rank"),
                    "cuartil") \
            .orderBy("salary") \
            .show(10)

# ─────────────────────────────────────────
# 4. lag y lead — comparar con período anterior/siguiente
# ─────────────────────────────────────────
print("=" * 55)
print("4. lag() y lead() — ventas por región")
print("=" * 55)

df_sales_dt = df_sales.withColumn("sale_date_dt", F.to_date("sale_date", "yyyy-MM-dd"))

ventas_mensuales = df_sales_dt.withColumn("anio_mes",
    F.date_format("sale_date_dt", "yyyy-MM")).groupBy("region", "anio_mes").agg(
    F.sum("amount").alias("total_mes")
).orderBy("region", "anio_mes")

ventana_lag = Window.partitionBy("region").orderBy("anio_mes")

ventas_con_lag = ventas_mensuales \
    .withColumn("mes_anterior",    F.lag("total_mes", 1).over(ventana_lag)) \
    .withColumn("mes_siguiente",   F.lead("total_mes", 1).over(ventana_lag)) \
    .withColumn("crecimiento",
        F.round((F.col("total_mes") - F.col("mes_anterior")) / F.col("mes_anterior") * 100, 2))

print("Ventas mensuales con lag/lead (primeras 15):")
ventas_con_lag.show(15)

# ─────────────────────────────────────────
# 5. Suma acumulada (running total)
# ─────────────────────────────────────────
print("=" * 55)
print("5. Suma acumulada (running total)")
print("=" * 55)

ventana_acum = Window.partitionBy("region") \
    .orderBy("anio_mes") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

ventas_acumuladas = ventas_mensuales.withColumn(
    "total_acumulado", F.sum("total_mes").over(ventana_acum)
).withColumn(
    "promedio_movil_3m",
    F.avg("total_mes").over(
        Window.partitionBy("region")
              .orderBy("anio_mes")
              .rowsBetween(-2, 0)
    )
)

ventas_acumuladas.filter(F.col("region") == "Norte").show()

# ─────────────────────────────────────────
# 6. Diferencia con el promedio del grupo
# ─────────────────────────────────────────
print("=" * 55)
print("6. Diferencia salarial con el promedio del departamento")
print("=" * 55)

ventana_avg = Window.partitionBy("department")

df_emp_clean.withColumn("avg_dept", F.avg("salary").over(ventana_avg)) \
            .withColumn("diff_avg",
                F.round(F.col("salary") - F.col("avg_dept"), 2)) \
            .select("name", "department", "salary",
                    F.round("avg_dept", 2).alias("avg_dept"), "diff_avg") \
            .orderBy("department", F.col("diff_avg").desc()) \
            .show(20)

spark.stop()
print("\n✅ Window Functions demostradas")
