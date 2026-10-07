"""
FASE 5 — SOLUCIÓN DEL RETO: Reporte Ejecutivo de Ventas
=========================================================
⚠️  INTENTA RESOLVER EL RETO ANTES DE VER ESTO ⚠️
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Solucion_Reto_Fase5") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

df_emp   = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True)
df_sales = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"),    header=True, inferSchema=True)
df_prod  = spark.read.csv(os.path.join(CSV_DIR, "products.csv"), header=True, inferSchema=True)
df_cust  = spark.read.csv(os.path.join(CSV_DIR, "customers.csv"),header=True, inferSchema=True)
df_tx    = spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet"))

# ─── 1. Top 5 empleados por ventas
print("1. Top 5 vendedores:")
df_sales.join(df_emp, "employee_id", "inner") \
    .groupBy("name", "department") \
    .agg(F.round(F.sum("amount"), 2).alias("total_ventas")) \
    .orderBy(F.col("total_ventas").desc()) \
    .limit(5).show()

# ─── 2. Ranking de categorías
print("2. Categorías por ingreso:")
df_sales.join(df_prod, "product_id", "left") \
    .groupBy("category") \
    .agg(F.round(F.sum("amount"), 2).alias("total")) \
    .orderBy(F.col("total").desc()).show()

# ─── 3. Pivot año x región
print("3. Pivot ventas por año y región:")
df_s = df_sales.withColumn("anio", F.year(F.to_date("sale_date", "yyyy-MM-dd")))
regiones = [r[0] for r in df_s.select("region").distinct().collect()]
df_s.groupBy("anio").pivot("region", regiones).agg(F.round(F.sum("amount"), 2)) \
    .orderBy("anio").show()

# ─── 4. Empleado con venta más alta por región (window)
print("4. Venta individual más grande por región:")
ventana = Window.partitionBy("region").orderBy(F.col("amount").desc())
df_sales.join(df_emp, "employee_id", "left") \
    .withColumn("rn", F.row_number().over(ventana)) \
    .filter(F.col("rn") == 1) \
    .select("region", "name", F.round("amount", 2).alias("max_venta")) \
    .orderBy("region").show()

# ─── 5. Crecimiento MoM por región con lag
print("5. Crecimiento MoM por región:")
df_sales_m = df_sales.withColumn("mes", F.date_format(F.to_date("sale_date","yyyy-MM-dd"), "yyyy-MM"))
vm = df_sales_m.groupBy("region","mes").agg(F.round(F.sum("amount"),2).alias("total"))
vent_lag = Window.partitionBy("region").orderBy("mes")
vm.withColumn("anterior", F.lag("total").over(vent_lag)) \
  .withColumn("crecimiento_pct",
    F.round((F.col("total")-F.col("anterior"))/F.col("anterior")*100, 2)) \
  .filter(F.col("anterior").isNotNull()) \
  .orderBy("region","mes").show(15)

# ─── 6. Clientes con > 20 transacciones
print("6. Clientes con > 20 transacciones:")
df_cust.join(
    df_tx.groupBy("customer_id").count().filter(F.col("count") > 20),
    "customer_id", "inner"
).select("name", "country", "count").orderBy(F.col("count").desc()).show(10)

# ─── 7. Monto promedio clientes completados
print("7. Monto promedio por cliente (completados):")
df_tx.filter(F.col("status") == "completed") \
    .groupBy("customer_id") \
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount")) \
    .agg(F.round(F.avg("avg_amount"), 2).alias("ltv_promedio")) \
    .show()

# ─── 8. Top 3 clientes por monto total
print("8. Top 3 clientes por monto total:")
df_tx.join(df_cust, "customer_id", "left") \
    .groupBy("customer_id", "name") \
    .agg(F.round(F.sum("amount"), 2).alias("total")) \
    .orderBy(F.col("total").desc()).limit(3).show()

# ─── 9. Rellena nulos de salary con promedio del dept
print("9. Salary relleno con promedio del departamento:")
vent_dept = Window.partitionBy("department")
df_emp.withColumn("salary_clean",
    F.coalesce(F.col("salary"), F.avg("salary").over(vent_dept))) \
    .agg(F.count(F.when(F.col("salary_clean").isNull(), 1)).alias("nulos_restantes")) \
    .show()

# ─── 10. Productos: disponibilidad
print("10. Disponibilidad de productos:")
df_prod.withColumn("estado",
    F.when(F.col("stock") == 0, "Agotado").otherwise("Disponible")) \
    .groupBy("estado").count().show()

spark.stop()
print("\n✅ Reto Fase 5 completado")
