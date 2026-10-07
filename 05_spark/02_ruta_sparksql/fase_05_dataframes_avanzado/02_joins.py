"""
FASE 5 — Ejemplo 2: Joins en DataFrames
=========================================
Aprenderás todos los tipos de join disponibles en PySpark.
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Joins_DataFrames") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

df_emp    = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True)
df_sales  = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"),    header=True, inferSchema=True)
df_prod   = spark.read.csv(os.path.join(CSV_DIR, "products.csv"), header=True, inferSchema=True)
df_cust   = spark.read.csv(os.path.join(CSV_DIR, "customers.csv"),header=True, inferSchema=True)
df_orders = spark.read.parquet(os.path.join(PARQ_DIR, "orders.parquet"))

# Datos pequeños para demo visual
empleados = spark.createDataFrame([
    (1, "Ana", "Eng"),  (2, "Luis", "Sales"),
    (3, "Maria", "HR"), (4, "Pedro", "Eng")
], ["id", "nombre", "dept"])

salarios = spark.createDataFrame([
    (1, 80000.0), (2, 50000.0), (5, 60000.0), (6, 70000.0)
], ["emp_id", "salario"])

# ─────────────────────────────────────────
# 1. INNER JOIN — solo las coincidencias
# ─────────────────────────────────────────
print("=" * 55)
print("1. INNER JOIN")
print("=" * 55)

inner = empleados.join(salarios, empleados.id == salarios.emp_id, "inner")
inner.show()
print(f"→ Solo {inner.count()} filas (Ana y Luis coinciden)")

# Forma corta cuando la columna se llama igual
# df1.join(df2, on="columna", how="inner")

# ─────────────────────────────────────────
# 2. LEFT JOIN — todos de la izquierda
# ─────────────────────────────────────────
print("=" * 55)
print("2. LEFT JOIN (left outer)")
print("=" * 55)

left = empleados.join(salarios, empleados.id == salarios.emp_id, "left")
left.show()
print(f"→ {left.count()} filas (todos los empleados, nulos si no hay salario)")

# ─────────────────────────────────────────
# 3. RIGHT JOIN
# ─────────────────────────────────────────
print("=" * 55)
print("3. RIGHT JOIN")
print("=" * 55)

right = empleados.join(salarios, empleados.id == salarios.emp_id, "right")
right.show()
print(f"→ {right.count()} filas (todos los salarios)")

# ─────────────────────────────────────────
# 4. FULL OUTER JOIN
# ─────────────────────────────────────────
print("=" * 55)
print("4. FULL OUTER JOIN")
print("=" * 55)

full = empleados.join(salarios, empleados.id == salarios.emp_id, "full")
full.show()
print(f"→ {full.count()} filas (todos de ambos lados)")

# ─────────────────────────────────────────
# 5. SEMI JOIN — filas de A que tienen match en B
# ─────────────────────────────────────────
print("=" * 55)
print("5. SEMI JOIN (left semi)")
print("=" * 55)

semi = empleados.join(salarios, empleados.id == salarios.emp_id, "semi")
semi.show()
print(f"→ {semi.count()} filas — solo cols de empleados, solo si tienen salario")

# ─────────────────────────────────────────
# 6. ANTI JOIN — filas de A que NO tienen match en B
# ─────────────────────────────────────────
print("=" * 55)
print("6. ANTI JOIN (left anti)")
print("=" * 55)

anti = empleados.join(salarios, empleados.id == salarios.emp_id, "anti")
anti.show()
print(f"→ {anti.count()} filas — empleados SIN salario asignado")

# ─────────────────────────────────────────
# 7. Join con ambigüedad de columnas — usar alias
# ─────────────────────────────────────────
print("=" * 55)
print("7. Resolver ambigüedad con alias")
print("=" * 55)

emp_a = df_emp.alias("emp")
sales_a = df_sales.alias("ventas")

resultado = emp_a.join(
    sales_a,
    F.col("emp.employee_id") == F.col("ventas.employee_id"),
    "inner"
).select(
    F.col("emp.name").alias("empleado"),
    F.col("emp.department"),
    F.col("ventas.amount"),
    F.col("ventas.region")
)
resultado.show(10)

# ─────────────────────────────────────────
# 8. Join de 3 tablas — ventas enriquecidas
# ─────────────────────────────────────────
print("=" * 55)
print("8. Join de 3 tablas: ventas + empleados + productos")
print("=" * 55)

ventas_enriquecidas = df_sales.alias("s") \
    .join(df_emp.alias("e"),
          F.col("s.employee_id") == F.col("e.employee_id"), "left") \
    .join(df_prod.alias("p"),
          F.col("s.product_id") == F.col("p.product_id"), "left") \
    .select(
        F.col("s.sale_id"),
        F.col("e.name").alias("vendedor"),
        F.col("e.department"),
        F.col("p.name").alias("producto"),
        F.col("p.category"),
        F.col("s.amount"),
        F.col("s.region"),
        F.col("s.sale_date")
    )

print(f"Ventas enriquecidas: {ventas_enriquecidas.count()} filas")
ventas_enriquecidas.show(10)

print("\nResumen: ventas por categoría y región:")
ventas_enriquecidas.groupBy("category", "region").agg(
    F.count("*").alias("num_ventas"),
    F.round(F.sum("amount"), 2).alias("total")
).orderBy(F.col("total").desc()).show(10)

spark.stop()
print("\n✅ Joins en DataFrames demostrados")
