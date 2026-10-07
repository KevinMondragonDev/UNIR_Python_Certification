"""
FASE 6 — Ejemplo 1: Vistas Temporales
========================================
Aprenderás a:
  - Crear vistas temporales y globales
  - Listar y eliminar vistas
  - Usar spark.sql() sobre las vistas
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Temp_Views") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

# Cargar DataFrames
df_emp   = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True)
df_sales = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"),    header=True, inferSchema=True)
df_prod  = spark.read.csv(os.path.join(CSV_DIR, "products.csv"), header=True, inferSchema=True)
df_cust  = spark.read.csv(os.path.join(CSV_DIR, "customers.csv"),header=True, inferSchema=True)
df_tx    = spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet"))
df_orders= spark.read.parquet(os.path.join(PARQ_DIR, "orders.parquet"))

# ─────────────────────────────────────────
# 1. createOrReplaceTempView
# ─────────────────────────────────────────
print("=" * 55)
print("1. createOrReplaceTempView()")
print("=" * 55)

df_emp.createOrReplaceTempView("empleados")
df_sales.createOrReplaceTempView("ventas")
df_prod.createOrReplaceTempView("productos")
df_cust.createOrReplaceTempView("clientes")
df_tx.createOrReplaceTempView("transacciones")
df_orders.createOrReplaceTempView("ordenes")

print("Vistas creadas. Probando con SQL:")
result = spark.sql("SELECT COUNT(*) as total FROM empleados")
result.show()

# ─────────────────────────────────────────
# 2. Listar vistas disponibles
# ─────────────────────────────────────────
print("=" * 55)
print("2. Listar vistas en el catálogo")
print("=" * 55)

tablas = spark.catalog.listTables()
print("Vistas disponibles:")
for tabla in tablas:
    print(f"  - {tabla.name} (tipo: {tabla.tableType})")

# ─────────────────────────────────────────
# 3. spark.sql() básico
# ─────────────────────────────────────────
print("=" * 55)
print("3. spark.sql() básico")
print("=" * 55)

spark.sql("SELECT * FROM empleados LIMIT 5").show()
spark.sql("SELECT COUNT(*) as total FROM ventas").show()
spark.sql("DESCRIBE empleados").show()

# ─────────────────────────────────────────
# 4. spark.sql() devuelve DataFrame
# ─────────────────────────────────────────
print("=" * 55)
print("4. spark.sql() devuelve DataFrame — se puede encadenar")
print("=" * 55)

df_result = spark.sql("""
    SELECT department, AVG(salary) as avg_salary
    FROM empleados
    WHERE salary IS NOT NULL
    GROUP BY department
""")

print(f"Tipo de resultado: {type(df_result)}")
df_result.filter(F.col("avg_salary") > 60000).orderBy("avg_salary").show()

# ─────────────────────────────────────────
# 5. createOrReplace — actualizar una vista
# ─────────────────────────────────────────
print("=" * 55)
print("5. createOrReplaceTempView — actualizar vista")
print("=" * 55)

df_emp_clean = df_emp.filter(F.col("salary").isNotNull())
df_emp_clean.createOrReplaceTempView("empleados")  # reemplaza la vista

nuevo_total = spark.sql("SELECT COUNT(*) as total FROM empleados").first()["total"]
print(f"Vista 'empleados' actualizada. Ahora tiene {nuevo_total} filas (sin nulos en salary)")

# ─────────────────────────────────────────
# 6. Vista global (cross-session)
# ─────────────────────────────────────────
print("=" * 55)
print("6. createGlobalTempView")
print("=" * 55)

try:
    df_prod.createGlobalTempView("productos_global")
    result = spark.sql("SELECT COUNT(*) FROM global_temp.productos_global")
    result.show()
    print("Vista global creada correctamente")
except Exception as e:
    print(f"Vista global ya existía: {e}")
    spark.sql("SELECT COUNT(*) FROM global_temp.productos_global").show()

# ─────────────────────────────────────────
# 7. Eliminar una vista
# ─────────────────────────────────────────
print("=" * 55)
print("7. Eliminar vista")
print("=" * 55)

spark.catalog.dropTempView("ordenes")
tablas_after = [t.name for t in spark.catalog.listTables()]
print(f"Vistas disponibles tras eliminar 'ordenes': {tablas_after}")

spark.stop()
print("\n✅ Vistas temporales demostradas")
