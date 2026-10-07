"""
FASE 9 — Pipeline Maestro
===========================
Orquesta todos los módulos del proyecto integrador.
Ejecutar: python pipeline.py
"""

import os
import sys
import time
import shutil
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")
OUT_DIR  = os.path.join(BASE, "fase_09_proyecto_integrador", "output")

# ─────────────────────────────────────────
# Inicializar Spark
# ─────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Proyecto_Integrador_PySpark") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

t_inicio = time.time()
print("=" * 60)
print("PIPELINE: Proyecto Integrador PySpark")
print("=" * 60)

# ─────────────────────────────────────────
# PASO 1: Ingesta
# ─────────────────────────────────────────
print("\n[1/5] Ingesta de datos...")
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

emp_schema = "employee_id INT, name STRING, department STRING, salary DOUBLE, hire_date STRING, country STRING"
datasets = {
    "empleados":     spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, schema=emp_schema),
    "ventas":        spark.read.csv(os.path.join(CSV_DIR, "sales.csv"),    header=True, inferSchema=True),
    "productos":     spark.read.csv(os.path.join(CSV_DIR, "products.csv"), header=True, inferSchema=True),
    "clientes":      spark.read.csv(os.path.join(CSV_DIR, "customers.csv"),header=True, inferSchema=True),
    "transacciones": spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet")),
    "ordenes":       spark.read.parquet(os.path.join(PARQ_DIR, "orders.parquet")),
    "eventos":       spark.read.parquet(os.path.join(PARQ_DIR, "events.parquet")),
}
print("  ✅ Datasets cargados:", {k: v.count() for k, v in datasets.items()})

# ─────────────────────────────────────────
# PASO 2: Limpieza
# ─────────────────────────────────────────
print("\n[2/5] Limpieza y normalización...")

avg_sal = datasets["empleados"].filter(F.col("salary").isNotNull()).agg(F.avg("salary")).first()[0]
ventana_dept = Window.partitionBy("department")

df_emp = datasets["empleados"] \
    .withColumn("avg_dept", F.avg("salary").over(ventana_dept)) \
    .withColumn("salary",   F.coalesce(F.col("salary"), F.col("avg_dept"), F.lit(avg_sal))) \
    .withColumn("hire_date_dt", F.to_date("hire_date", "yyyy-MM-dd")) \
    .withColumn("anios_empresa", F.round(F.datediff(F.current_date(), F.col("hire_date_dt")) / 365, 1)) \
    .withColumn("nivel",
        F.when(F.col("salary") < 40000, "Junior")
         .when(F.col("salary") < 65000, "Mid")
         .when(F.col("salary") < 90000, "Senior")
         .otherwise("Lead")) \
    .drop("avg_dept")

df_ventas = datasets["ventas"] \
    .withColumn("sale_date_dt", F.to_date("sale_date", "yyyy-MM-dd")) \
    .withColumn("year",     F.year("sale_date_dt")) \
    .withColumn("month",    F.month("sale_date_dt")) \
    .withColumn("anio_mes", F.date_format("sale_date_dt", "yyyy-MM"))

df_prod = datasets["productos"] \
    .withColumn("disponible",   F.col("stock") > 0) \
    .withColumn("rango_precio",
        F.when(F.col("price") < 50, "Económico")
         .when(F.col("price") < 200, "Medio")
         .otherwise("Premium"))

df_cust = datasets["clientes"] \
    .withColumn("email",
        F.when(F.col("email").isNull() | (F.col("email") == ""), "sin_email")
         .otherwise(F.col("email"))) \
    .withColumn("signup_dt", F.to_date("signup_date", "yyyy-MM-dd"))

df_tx = datasets["transacciones"] \
    .withColumn("year",     F.year("ts")) \
    .withColumn("month",    F.month("ts")) \
    .withColumn("anio_mes", F.date_format("ts", "yyyy-MM"))

print("  ✅ Limpieza completada")

# ─────────────────────────────────────────
# PASO 3: Transformación (join + enriquecimiento)
# ─────────────────────────────────────────
print("\n[3/5] Transformaciones y enriquecimiento...")

ventas_ricas = df_ventas.alias("v") \
    .join(df_emp.alias("e"),  F.col("v.employee_id") == F.col("e.employee_id"), "left") \
    .join(df_prod.alias("p"), F.col("v.product_id")  == F.col("p.product_id"),  "left") \
    .select(
        F.col("v.sale_id"), F.col("v.employee_id"),
        F.col("e.name").alias("vendedor"),
        F.col("e.department"), F.col("e.nivel"),
        F.col("v.product_id"),
        F.col("p.name").alias("producto"),
        F.col("p.category"),
        F.col("v.amount"), F.col("v.region"),
        F.col("v.year"), F.col("v.month"), F.col("v.anio_mes")
    )

ventas_ricas.cache()
print(f"  ✅ ventas_ricas: {ventas_ricas.count()} filas")

tx_ricas = df_tx.alias("t") \
    .join(df_cust.alias("c"), F.col("t.customer_id") == F.col("c.customer_id"), "left") \
    .select(
        F.col("t.tx_id"), F.col("t.customer_id"),
        F.col("c.name").alias("cliente"),
        F.col("c.country"), F.col("t.amount"),
        F.col("t.status"), F.col("t.currency"),
        F.col("t.year"), F.col("t.month"), F.col("t.anio_mes")
    )
print(f"  ✅ tx_ricas: {tx_ricas.count()} filas")

# ─────────────────────────────────────────
# PASO 4: Análisis SQL
# ─────────────────────────────────────────
print("\n[4/5] Análisis SQL...")

ventas_ricas.createOrReplaceTempView("ventas_ricas")
tx_ricas.createOrReplaceTempView("tx_ricas")
df_emp.createOrReplaceTempView("empleados")

print("\n--- Top 5 Vendedores ---")
spark.sql("""
    SELECT vendedor, department, ROUND(SUM(amount),2) AS total_ventas, COUNT(*) AS num_ventas
    FROM ventas_ricas GROUP BY vendedor, department ORDER BY total_ventas DESC LIMIT 5
""").show()

print("--- Ventas por Categoría ---")
spark.sql("""
    SELECT category, ROUND(SUM(amount),2) AS total, COUNT(*) AS transacciones
    FROM ventas_ricas GROUP BY category ORDER BY total DESC
""").show()

print("--- Segmentación Clientes ---")
spark.sql("""
    WITH gasto AS (
        SELECT customer_id, ROUND(SUM(amount),2) AS total
        FROM tx_ricas WHERE status='completed' GROUP BY customer_id
    )
    SELECT CASE WHEN total > 10000 THEN 'Alto' WHEN total > 5000 THEN 'Medio' ELSE 'Bajo' END AS seg,
           COUNT(*) AS clientes, ROUND(AVG(total),2) AS avg_gasto
    FROM gasto GROUP BY CASE WHEN total>10000 THEN 'Alto' WHEN total>5000 THEN 'Medio' ELSE 'Bajo' END
    ORDER BY avg_gasto DESC
""").show()

print("--- Rankings de Salario por Dept ---")
spark.sql("""
    SELECT department, nivel, COUNT(*) AS total, ROUND(AVG(salary),2) AS avg_sal
    FROM empleados GROUP BY department, nivel ORDER BY department, avg_sal DESC
""").show(20)

# ─────────────────────────────────────────
# PASO 5: Escritura
# ─────────────────────────────────────────
print("\n[5/5] Escritura de resultados...")

if os.path.exists(OUT_DIR):
    shutil.rmtree(OUT_DIR)
os.makedirs(OUT_DIR, exist_ok=True)

# Parquet particionado por año
ventas_ricas.write.mode("overwrite") \
    .partitionBy("year") \
    .parquet(os.path.join(OUT_DIR, "ventas_enriquecidas.parquet"))

# Reportes CSV
reportes_dir = os.path.join(OUT_DIR, "reportes_csv")

spark.sql("""
    SELECT vendedor, department, ROUND(SUM(amount),2) AS total_ventas
    FROM ventas_ricas GROUP BY vendedor, department ORDER BY total_ventas DESC LIMIT 20
""").coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(os.path.join(reportes_dir, "top_vendedores"))

spark.sql("""
    SELECT category, producto, ROUND(SUM(amount),2) AS total
    FROM ventas_ricas GROUP BY category, producto ORDER BY category, total DESC
""").coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(os.path.join(reportes_dir, "top_productos"))

spark.sql("""
    SELECT region, anio_mes, ROUND(SUM(amount),2) AS total, COUNT(*) AS num_ventas
    FROM ventas_ricas GROUP BY region, anio_mes ORDER BY region, anio_mes
""").coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(os.path.join(reportes_dir, "metricas_regiones"))

print(f"  ✅ Resultados guardados en: {OUT_DIR}")

ventas_ricas.unpersist()

t_fin = time.time()
print("\n" + "=" * 60)
print(f"✅ PIPELINE COMPLETADO en {t_fin - t_inicio:.1f} segundos")
print(f"   Ejecuta: python validar_proyecto.py para verificar")
print("=" * 60)

spark.stop()
