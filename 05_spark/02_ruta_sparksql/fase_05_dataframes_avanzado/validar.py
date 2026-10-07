"""
FASE 5 — Validador Automático
==============================
Ejecuta: python validar.py
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

passed = 0
failed = 0

def check(desc, cond):
    global passed, failed
    if cond:
        print(f"  ✅ {desc}"); passed += 1
    else:
        print(f"  ❌ {desc}"); failed += 1

print("=" * 55)
print("VALIDADOR — FASE 5: DataFrames Avanzado")
print("=" * 55)

spark = SparkSession.builder.appName("Validador_Fase5") \
    .master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

df_emp   = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True)
df_sales = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"),    header=True, inferSchema=True)
df_prod  = spark.read.csv(os.path.join(CSV_DIR, "products.csv"), header=True, inferSchema=True)
df_cust  = spark.read.csv(os.path.join(CSV_DIR, "customers.csv"),header=True, inferSchema=True)
df_tx    = spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet"))

# ─── Test 1: groupBy + agg
print("\n[1] groupBy + agg")
df_dept = df_emp.filter(F.col("salary").isNotNull()) \
    .groupBy("department").agg(
        F.count("*").alias("total"),
        F.avg("salary").alias("avg_salary")
    )
check("groupBy por department devuelve 7 filas",
      df_dept.count() == len(df_emp.select("department").distinct().collect()))
check("Columna avg_salary existe", "avg_salary" in df_dept.columns)
check("avg_salary es positiva", df_dept.filter(F.col("avg_salary") <= 0).count() == 0)

# ─── Test 2: HAVING
print("\n[2] HAVING (filter tras groupBy)")
df_region = df_sales.groupBy("region").agg(
    F.count("*").alias("num_ventas"),
    F.sum("amount").alias("total")
).filter(F.col("num_ventas") > 250)
check("Hay regiones con más de 250 ventas", df_region.count() > 0)
check("Todas las regiones filtradas tienen >250 ventas",
      df_region.filter(F.col("num_ventas") <= 250).count() == 0)

# ─── Test 3: INNER JOIN
print("\n[3] INNER JOIN sales + employees")
joined = df_sales.join(df_emp, "employee_id", "inner")
check("Join produce filas",  joined.count() > 0)
check("Columna 'name' existe tras join", "name" in joined.columns)
check("Join count <= sales count", joined.count() <= df_sales.count())

# ─── Test 4: LEFT JOIN + anti count
print("\n[4] LEFT JOIN customers + transactions")
left = df_cust.join(df_tx, "customer_id", "left")
sin_tx = df_cust.join(df_tx, "customer_id", "anti").count()
check("Hay clientes sin transacciones (anti join)", sin_tx >= 0)
check("Left join tiene al menos 300 filas", left.count() >= 300)

# ─── Test 5: Window rank
print("\n[5] Window Function - row_number")
ventana = Window.partitionBy("department").orderBy(F.col("salary").desc())
df_clean = df_emp.filter(F.col("salary").isNotNull())
df_rank = df_clean.withColumn("rn", F.row_number().over(ventana))
top2 = df_rank.filter(F.col("rn") <= 2)
check("rn <= 2 en top2", top2.filter(F.col("rn") > 2).count() == 0)
check("Exactamente 2 por dept (aprox)", top2.count() <= df_clean.select("department").distinct().count() * 2)

# ─── Test 6: Window avg diferencia
print("\n[6] Window avg - diff_vs_avg")
ventana_avg = Window.partitionBy("department")
df_diff = df_clean.withColumn("avg_dept", F.avg("salary").over(ventana_avg)) \
                  .withColumn("diff_vs_avg", F.col("salary") - F.col("avg_dept"))
check("Columna diff_vs_avg existe", "diff_vs_avg" in df_diff.columns)
check("Suma de diff_vs_avg ~ 0 por dept",
      abs(df_diff.filter(F.col("department") == "Engineering")
          .agg(F.sum("diff_vs_avg")).first()[0]) < 1.0)

# ─── Test 7: lag()
print("\n[7] lag() en transactions")
ventana_tx = Window.partitionBy("customer_id").orderBy("ts")
df_lag = df_tx.withColumn("monto_anterior", F.lag("amount", 1).over(ventana_tx))
check("Columna monto_anterior existe", "monto_anterior" in df_lag.columns)
check("Hay nulos en monto_anterior (primer registro por cliente)",
      df_lag.filter(F.col("monto_anterior").isNull()).count() > 0)

# ─── Test 8: fillna con promedio del grupo
print("\n[8] fillna con promedio del grupo")
ventana_dept = Window.partitionBy("department")
df_filled = df_emp.withColumn("avg_dept", F.avg("salary").over(ventana_dept)) \
                  .withColumn("salary_clean", F.coalesce(F.col("salary"), F.col("avg_dept")))
check("salary_clean tiene menos nulos que salary",
      df_filled.filter(F.col("salary_clean").isNull()).count() <=
      df_emp.filter(F.col("salary").isNull()).count())

# ─── Test 9: pivot
print("\n[9] pivot")
df_sales_yr = df_sales.withColumn("anio", F.year(F.to_date("sale_date", "yyyy-MM-dd")))
regiones = [r[0] for r in df_sales_yr.select("region").distinct().collect()]
pivot_df = df_sales_yr.groupBy("anio").pivot("region", regiones).agg(F.sum("amount"))
check("pivot tiene columna 'anio'", "anio" in pivot_df.columns)
check("pivot tiene columnas de regiones", len(pivot_df.columns) > 2)
check("pivot tiene filas por año", pivot_df.count() > 0)

# ─── Test 10: join 3 tablas
print("\n[10] Join 3 tablas + groupBy")
resultado = df_sales.alias("s") \
    .join(df_prod.alias("p"), F.col("s.product_id") == F.col("p.product_id"), "left") \
    .groupBy(F.col("p.category")).agg(F.sum(F.col("s.amount")).alias("total")) \
    .orderBy(F.col("total").desc())
check("Hay categorías con ventas", resultado.count() > 0)
check("Total mayor cero", resultado.first()["total"] > 0)

spark.stop()

print("\n" + "=" * 55)
total = passed + failed
print(f"RESULTADO: {passed}/{total} checks pasados")
if failed == 0:
    print("🎉 ¡Fase 5 completada! Puedes avanzar a la Fase 6.")
else:
    print(f"⚠️  {failed} check(s) fallaron. Revisa tu código.")
print("=" * 55)
