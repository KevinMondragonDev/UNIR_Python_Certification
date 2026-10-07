"""
FASE 9 — Validador del Proyecto Integrador
============================================
25 asserts de calidad de datos y pipeline.
Ejecutar DESPUÉS de pipeline_modular.py:
  python pipeline_modular.py && python validar_proyecto.py
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

print("=" * 60)
print("VALIDADOR PROYECTO INTEGRADOR — 25 checks")
print("=" * 60)

spark = SparkSession.builder.appName("Validador_Proyecto") \
    .master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")
OUT_DIR  = os.path.join(BASE, "fase_09_proyecto_integrador", "output")

df_emp   = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True)
df_sales = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"),    header=True, inferSchema=True)
df_prod  = spark.read.csv(os.path.join(CSV_DIR, "products.csv"), header=True, inferSchema=True)
df_cust  = spark.read.csv(os.path.join(CSV_DIR, "customers.csv"),header=True, inferSchema=True)
df_tx    = spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet"))

# ─── SECCIÓN 1: Validación de datasets fuente
print("\n[1-4] Datasets fuente")
check("employees.csv: 500 filas",      df_emp.count() == 500)
check("sales.csv: 2000 filas",         df_sales.count() == 2000)
check("products.csv: 100 filas",       df_prod.count() == 100)
check("transactions.parquet: 5000 tx", df_tx.count() == 5000)

# ─── SECCIÓN 2: Calidad de datos
print("\n[5-8] Calidad de datos")
check("Hay nulos en salary (dataset tiene imperfecciones)",
      df_emp.filter(F.col("salary").isNull()).count() > 0)
check("Sales: todos los amounts > 0",
      df_sales.filter(F.col("amount") <= 0).count() == 0)
check("Productos: product_id es único",
      df_prod.count() == df_prod.dropDuplicates(["product_id"]).count())
check("Transacciones: status solo tiene 4 valores",
      df_tx.select("status").distinct().count() == 4)

# ─── SECCIÓN 3: Transformaciones
print("\n[9-12] Transformaciones")

# Nivel de empleados
avg_sal = df_emp.filter(F.col("salary").isNotNull()).agg(F.avg("salary")).first()[0]
ventana = Window.partitionBy("department")
df_emp_clean = df_emp \
    .withColumn("avg_dept", F.avg("salary").over(ventana)) \
    .withColumn("salary", F.coalesce(F.col("salary"), F.col("avg_dept"), F.lit(avg_sal))) \
    .withColumn("nivel",
        F.when(F.col("salary") < 40000, "Junior")
         .when(F.col("salary") < 65000, "Mid")
         .when(F.col("salary") < 90000, "Senior")
         .otherwise("Lead")) \
    .withColumn("hire_date_dt", F.to_date("hire_date", "yyyy-MM-dd"))

check("Todos los salarios rellenos tras limpieza",
      df_emp_clean.filter(F.col("salary").isNull()).count() == 0)
check("Columna 'nivel' creada correctamente",
      "nivel" in df_emp_clean.columns)
check("Niveles válidos: Junior/Mid/Senior/Lead",
      df_emp_clean.filter(~F.col("nivel").isin("Junior","Mid","Senior","Lead")).count() == 0)
check("hire_date_dt es tipo date",
      "date" in dict(df_emp_clean.dtypes).get("hire_date_dt", ""))

# ─── SECCIÓN 4: Joins
print("\n[13-16] Joins y enriquecimiento")

df_sales_enriched = df_sales.alias("v") \
    .join(df_emp_clean.alias("e"), F.col("v.employee_id") == F.col("e.employee_id"), "left") \
    .join(df_prod.alias("p"), F.col("v.product_id") == F.col("p.product_id"), "left")

check("Join ventas+emp+prod no pierde ventas",
      df_sales_enriched.count() == 2000)
check("Columna 'department' disponible tras join",
      "department" in df_sales_enriched.columns)
check("Columna 'category' disponible tras join",
      "category" in df_sales_enriched.columns)

df_tx_enriched = df_tx.alias("t").join(
    df_cust.alias("c"), F.col("t.customer_id") == F.col("c.customer_id"), "left"
)
check("Join tx+clientes funciona (al menos 5000 filas)",
      df_tx_enriched.count() >= 5000)

# ─── SECCIÓN 5: Análisis SQL
print("\n[17-20] Análisis SQL")

# Tras el join hay dos columnas employee_id (v y e): seleccionamos con alias
# explícitos para que la vista no tenga nombres ambiguos.
df_sales_enriched.select(
    F.col("v.employee_id").alias("e_employee_id"),
    F.col("v.amount").alias("amount"),
    F.col("p.category").alias("category"),
    F.col("e.department").alias("department"),
).createOrReplaceTempView("ve")
df_tx_enriched.select(
    F.col("t.customer_id").alias("customer_id"),
    F.col("t.amount").alias("amount"),
    F.col("t.status").alias("status"),
).createOrReplaceTempView("txe")

top_vendedor = spark.sql("""
    SELECT e_employee_id, SUM(amount) AS total
    FROM ve GROUP BY e_employee_id ORDER BY total DESC LIMIT 1
""")

check("top vendedor tiene monto > 0",
      top_vendedor.filter(F.col("total") > 0).count() > 0)

cat_ventas = spark.sql("SELECT category, SUM(amount) as tot FROM ve GROUP BY category")
check("Hay ventas por categoría",
      cat_ventas.filter(F.col("tot") > 0).count() > 0)

seg_clientes = spark.sql("""
    WITH g AS (SELECT customer_id, SUM(amount) AS total FROM txe WHERE status='completed' GROUP BY customer_id)
    SELECT CASE WHEN total>10000 THEN 'Alto' WHEN total>5000 THEN 'Medio' ELSE 'Bajo' END AS seg, COUNT(*) AS n
    FROM g GROUP BY CASE WHEN total>10000 THEN 'Alto' WHEN total>5000 THEN 'Medio' ELSE 'Bajo' END
""")
check("Segmentación produce 3 grupos", seg_clientes.count() == 3)

# Verificar output del pipeline (si fue ejecutado)
parquet_out = os.path.join(OUT_DIR, "ventas_enriquecidas.parquet")
if os.path.exists(parquet_out):
    df_out = spark.read.parquet(parquet_out)
    check("Output Parquet existe y tiene filas", df_out.count() > 0)
    check("Output particionado por 'year'", "year" in [d.split("=")[0] for d in os.listdir(parquet_out) if "=" in d])
else:
    check("Output Parquet existe (ejecuta pipeline.py primero)", False)
    check("Output particionado por year", False)

# ─── SECCIÓN 6: Salidas de 03_transformacion + 05_escritura (pipeline_modular.py)
print("\n[21-25] Salidas del pipeline modular")
seg_out = os.path.join(OUT_DIR, "clientes_segmentados.parquet")
dept_out = os.path.join(OUT_DIR, "reporte_departamentos.parquet")
if os.path.exists(seg_out) and os.path.exists(dept_out):
    df_seg = spark.read.parquet(seg_out)
    check("clientes_segmentados: 1 fila por cliente",
          df_seg.count() == df_seg.select("customer_id").distinct().count())
    check("Segmentos válidos: Alto Valor / Medio / Bajo",
          df_seg.filter(~F.col("segmento").isin("Alto Valor", "Medio", "Bajo")).count() == 0)
    df_dept = spark.read.parquet(dept_out)
    check("reporte_departamentos tiene ventas_por_empleado",
          "ventas_por_empleado" in df_dept.columns)
else:
    check("clientes_segmentados existe (ejecuta pipeline_modular.py)", False)
    check("Segmentos válidos", False)
    check("reporte_departamentos existe (ejecuta pipeline_modular.py)", False)

parquets_ventas = [f for _, _, fs in os.walk(parquet_out) for f in fs if f.endswith(".parquet")] \
    if os.path.exists(parquet_out) else []
check("Sin small files: ≤ 1 archivo parquet por año en ventas_enriquecidas",
      0 < len(parquets_ventas) <= 4)

spark.stop()

print("\n" + "=" * 60)
total = passed + failed
print(f"RESULTADO FINAL: {passed}/{total} checks pasados")
if failed == 0:
    print("🏆 ¡PROYECTO INTEGRADOR COMPLETADO!")
    print("   Has dominado PySpark desde RDDs hasta SQL Avanzado.")
elif passed >= 20:
    print(f"🥈 Casi completo — {failed} check(s) menores fallaron.")
else:
    print(f"⚠️  {failed} check(s) fallaron. Revisa pipeline.py y vuelve a ejecutar.")
print("=" * 60)
