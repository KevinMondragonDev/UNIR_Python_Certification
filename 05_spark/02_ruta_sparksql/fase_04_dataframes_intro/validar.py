"""
FASE 4 — Validador Automático
==============================
Ejecuta: python validar.py
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

passed = 0
failed = 0

def check(desc, cond):
    global passed, failed
    if cond:
        print(f"  ✅ {desc}"); passed += 1
    else:
        print(f"  ❌ {desc}"); failed += 1

print("=" * 55)
print("VALIDADOR — FASE 4: DataFrames Intro")
print("=" * 55)

spark = SparkSession.builder.appName("Validador_Fase4") \
    .master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

# ─── Test 1: Crear DataFrame con schema
print("\n[1] Crear DataFrame con schema")
schema = StructType([
    StructField("producto", StringType(), True),
    StructField("precio",   DoubleType(), True),
    StructField("stock",    IntegerType(),True),
])
datos = [("Laptop", 1200.0, 50), ("Mouse", 25.5, 300)]
df_p = spark.createDataFrame(datos, schema)
check("DataFrame creado con 2 filas", df_p.count() == 2)
check("Columna precio es DoubleType", dict(df_p.dtypes).get("precio") == "double")
check("Columna stock es IntegerType", dict(df_p.dtypes).get("stock") == "int")

# ─── Test 2: Leer CSV
print("\n[2] Leer employees.csv")
df = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True)
check("500 filas cargadas", df.count() == 500)
check("6 columnas", len(df.columns) == 6)
check("Columna 'salary' existe", "salary" in df.columns)
check("Columna 'employee_id' existe", "employee_id" in df.columns)

# ─── Test 3: select y alias
print("\n[3] select + alias")
df_sel = df.select("name", F.col("salary").alias("salario"))
check("Solo 2 columnas tras select", len(df_sel.columns) == 2)
check("Columna renombrada a 'salario'", "salario" in df_sel.columns)

# ─── Test 4: filter múltiple
print("\n[4] filter con múltiples condiciones")
df_eng = df.filter(F.col("salary").isNotNull()) \
           .filter(F.col("salary") > 60000) \
           .filter(F.col("department") == "Engineering")
check("Hay ingenieros con salary > 60k", df_eng.count() > 0)
check("Solo Engineering en resultado",
      df_eng.filter(F.col("department") != "Engineering").count() == 0)

# ─── Test 5: withColumn
print("\n[5] withColumn")
df_col = df.filter(F.col("salary").isNotNull()) \
           .withColumn("salary_anual", F.col("salary") * 12)
check("Columna salary_anual existe", "salary_anual" in df_col.columns)
first_row = df_col.select("salary", "salary_anual").first()
check("salary_anual = salary * 12",
      abs(first_row["salary_anual"] - first_row["salary"] * 12) < 0.01)

# ─── Test 6: when/otherwise
print("\n[6] when/otherwise")
df_nivel = df.filter(F.col("salary").isNotNull()).withColumn(
    "nivel",
    F.when(F.col("salary") < 40000, "Junior")
     .when(F.col("salary") < 70000, "Mid")
     .otherwise("Senior")
)
niveles = set(df_nivel.select("nivel").distinct().toPandas()["nivel"].tolist())
check("Columna 'nivel' existe", "nivel" in df_nivel.columns)
check("Niveles: Junior, Mid, Senior",
      {"Junior", "Mid", "Senior"}.issubset(niveles))

# ─── Test 7: Fechas
print("\n[7] to_date + year/month")
df_f = df.withColumn("hire_date_dt", F.to_date("hire_date", "yyyy-MM-dd")) \
         .withColumn("anio", F.year("hire_date_dt"))
check("Columna hire_date_dt existe", "hire_date_dt" in df_f.columns)
check("Anio entre 2015 y 2023",
      df_f.filter((F.col("anio") < 2015) | (F.col("anio") > 2023)).count() == 0)

# ─── Test 8: Parquet
print("\n[8] Leer transactions.parquet")
df_tx = spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet"))
check("5000 transacciones", df_tx.count() == 5000)
check("Columna 'status' existe", "status" in df_tx.columns)
check("Columna 'ts' es timestamp", "timestamp" in dict(df_tx.dtypes).get("ts",""))

# ─── Test 9: dropDuplicates
print("\n[9] dropDuplicates")
df_uniq = df.select("department", "country").dropDuplicates()
check("Combinaciones únicas dept+country < 500", df_uniq.count() < 500)
check("Al menos 10 combinaciones", df_uniq.count() >= 10)

# ─── Test 10: orderBy + limit
print("\n[10] orderBy + filter isNotNull")
df_top = df.filter(F.col("salary").isNotNull()) \
           .orderBy(F.col("salary").desc()) \
           .limit(5)
check("Top 5 filas", df_top.count() == 5)
salarios = [r["salary"] for r in df_top.collect()]
check("Ordenado de mayor a menor", salarios == sorted(salarios, reverse=True))

spark.stop()

print("\n" + "=" * 55)
total = passed + failed
print(f"RESULTADO: {passed}/{total} checks pasados")
if failed == 0:
    print("🎉 ¡Fase 4 completada! Puedes avanzar a la Fase 5.")
else:
    print(f"⚠️  {failed} check(s) fallaron. Revisa tu código.")
print("=" * 55)
