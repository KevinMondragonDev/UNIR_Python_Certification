"""
FASE 8 — Validador Automático
==============================
Ejecuta: python validar.py
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, DoubleType
import pandas as pd
import io
from contextlib import redirect_stdout

passed = 0
failed = 0

def check(desc, cond):
    global passed, failed
    if cond:
        print(f"  ✅ {desc}"); passed += 1
    else:
        print(f"  ❌ {desc}"); failed += 1

print("=" * 55)
print("VALIDADOR — FASE 8: Optimización y UDFs")
print("=" * 55)

spark = SparkSession.builder.appName("Validador_Fase8") \
    .master("local[*]") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

df_emp   = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True)
df_sales = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"),    header=True, inferSchema=True)
df_prod  = spark.read.csv(os.path.join(CSV_DIR, "products.csv"), header=True, inferSchema=True)
df_tx    = spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet"))

# ─── Test 1: cache y unpersist
print("\n[1] cache() y unpersist()")
df_c = df_emp.filter(F.col("salary").isNotNull()).cache()
_ = df_c.count()
check("is_cached True tras cache()", df_c.is_cached)
df_c.unpersist()
check("is_cached False tras unpersist()", not df_c.is_cached)

# ─── Test 2: UDF Python
print("\n[2] UDF Python")
pais_map = {"Mexico":"MX","Colombia":"CO","USA":"US","España":"ES"}
def cod_pais(c):
    return pais_map.get(c, "XX") if c else "XX"
cod_udf = F.udf(cod_pais, StringType())
df_cod = df_emp.withColumn("codigo", cod_udf(F.col("country")))
check("Columna 'codigo' existe", "codigo" in df_cod.columns)
sample = df_cod.filter(F.col("country") == "Mexico").select("codigo").first()
if sample:
    check("Mexico → MX", sample["codigo"] == "MX")
else:
    check("Mexico → MX", False)
sample_xx = df_cod.filter(~F.col("country").isin(list(pais_map.keys()))) \
                  .select("codigo").first()
check("Países desconocidos → XX",
      sample_xx is None or sample_xx["codigo"] == "XX")

# ─── Test 3: UDF en SQL
print("\n[3] UDF registrada en SQL")
spark.udf.register("cod_p", cod_pais, StringType())
df_emp.createOrReplaceTempView("empleados_test")
r = spark.sql("SELECT cod_p('Mexico') as c").first()["c"]
check("UDF en SQL: cod_p('Mexico') = 'MX'", r == "MX")
r2 = spark.sql("SELECT cod_p('Desconocido') as c").first()["c"]
check("UDF en SQL: desconocido → 'XX'", r2 == "XX")

# ─── Test 4: Pandas UDF
print("\n[4] Pandas UDF")
@pandas_udf(DoubleType())
def descuento_10(price: pd.Series) -> pd.Series:
    return (price * 0.90).round(2)

df_desc = df_prod.withColumn("precio_desc", descuento_10(F.col("price")))
check("Columna precio_desc existe", "precio_desc" in df_desc.columns)
row = df_desc.select("price", "precio_desc").first()
check("precio_desc = price * 0.90",
      abs(row["precio_desc"] - row["price"] * 0.90) < 0.02)

# ─── Test 5: repartition y coalesce
print("\n[5] repartition y coalesce")
df_8 = df_tx.repartition(8)
check("repartition(8) → 8 particiones", df_8.rdd.getNumPartitions() == 8)
df_2 = df_8.coalesce(2)
check("coalesce(2) → 2 particiones", df_2.rdd.getNumPartitions() == 2)

# ─── Test 6: Broadcast join
print("\n[6] Broadcast join")
df_bj = df_sales.join(F.broadcast(df_prod), "product_id", "inner")
check("Broadcast join devuelve filas", df_bj.count() > 0)
buf = io.StringIO()
with redirect_stdout(buf):
    df_bj.explain()
plan = buf.getvalue()
check("Plan contiene 'Broadcast' o 'broadcast'",
      "Broadcast" in plan or "broadcast" in plan)

# ─── Test 7: groupBy para detectar skew
print("\n[7] Detección de skew (distribución por región)")
dist_region = df_sales.groupBy("region").count().orderBy(F.col("count").desc())
max_region = dist_region.first()["count"]
min_region = dist_region.orderBy("count").first()["count"]
check("Hay distribución por región", dist_region.count() > 0)
check("max - min < 500 (distribución razonablemente uniforme)",
      abs(max_region - min_region) < 500)

# ─── Test 8: Catalyst built-in vs UDF
print("\n[8] built-in F.when() funciona correctamente")
df_cat = df_sales.withColumn("cat",
    F.when(F.col("amount") > 2500, "Alto").otherwise("Bajo"))
check("Columna 'cat' existe", "cat" in df_cat.columns)
check("Solo 'Alto' y 'Bajo' como valores",
      df_cat.filter(~F.col("cat").isin("Alto", "Bajo")).count() == 0)

# ─── Test 9: Narrow vs Wide (Exchange en el plan)
print("\n[9] Narrow vs Wide")
def plan_txt(df):
    b = io.StringIO()
    with redirect_stdout(b):
        df.explain()
    return b.getvalue()
spark.conf.set("spark.sql.adaptive.enabled", "false")
check("filter + withColumn NO generan Exchange (narrow)",
      "Exchange" not in plan_txt(df_tx.filter(F.col("amount") > 0).withColumn("x", F.lit(1))))
check("groupBy SÍ genera Exchange (wide)",
      "Exchange" in plan_txt(df_tx.groupBy("status").count()))
check("coalesce NO genera Exchange",
      "Exchange" not in plan_txt(df_tx.coalesce(1)))

# ─── Test 10: shuffle.partitions y AQE
print("\n[10] shuffle.partitions y AQE")
spark.conf.set("spark.sql.shuffle.partitions", "6")
agg_sin = df_sales.groupBy("region").count()
check("Sin AQE: groupBy produce shuffle.partitions (6) particiones",
      agg_sin.rdd.getNumPartitions() == 6)
spark.conf.set("spark.sql.adaptive.enabled", "true")
agg_con = df_sales.groupBy("region").count()
agg_con.collect()
check("Con AQE: el plan final es adaptativo (AdaptiveSparkPlan)",
      "AdaptiveSparkPlan" in plan_txt(agg_con))

# ─── Test 11: Estrategias de join
print("\n[11] Estrategias de join")
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
check("Sin broadcast automático → SortMergeJoin",
      "SortMergeJoin" in plan_txt(df_sales.join(df_prod, "product_id")))
df_sales.createOrReplaceTempView("v_val"); df_prod.createOrReplaceTempView("p_val")
check("Hint /*+ BROADCAST */ → BroadcastHashJoin",
      "BroadcastHashJoin" in plan_txt(spark.sql(
          "SELECT /*+ BROADCAST(p) */ * FROM v_val v JOIN p_val p ON v.product_id = p.product_id")))

# ─── Test 12: Salting conserva el resultado
print("\n[12] Salting")
df_cust = spark.read.csv(os.path.join(CSV_DIR, "customers.csv"), header=True, inferSchema=True)
df_skew = df_tx.withColumn("customer_id",
    F.when(F.rand(seed=42) < 0.9, F.lit(1)).otherwise(F.col("customer_id")))
S = 4
salt_l = df_skew.withColumn("salt", (F.rand(seed=1) * S).cast("int"))
salt_r = df_cust.crossJoin(spark.range(S).select(F.col("id").cast("int").alias("salt")))
n_salt = salt_l.join(salt_r, ["customer_id", "salt"]).count()
check("Join con salting = mismo nº de filas que sin salting",
      n_salt == df_skew.join(df_cust, "customer_id").count())

# ─── Test 13: Reto — pipeline_optimizado()
print("\n[13] Reto: pipeline optimizado (reto.py)")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    import reto
    esperado = [tuple(r) for r in reto.pipeline_lento(spark).collect()]
    df_opt = reto.pipeline_optimizado(spark)
    if df_opt is None:
        check("pipeline_optimizado() implementado", False)
    else:
        check("Columnas correctas",
              df_opt.columns == ["region", "category", "nivel_venta", "total", "num_ventas"])
        check("Mismo resultado que pipeline_lento()",
              [tuple(r) for r in df_opt.collect()] == esperado)
        plan = plan_txt(df_opt)
        check("Usa BroadcastHashJoin", "BroadcastHashJoin" in plan)
        check("Sin UDF Python (PythonUDF / BatchEvalPython)",
              "PythonUDF" not in plan and "BatchEvalPython" not in plan)
except Exception as e:
    check(f"reto.py se ejecuta sin errores: {e}", False)

spark.stop()

print("\n" + "=" * 55)
total = passed + failed
print(f"RESULTADO: {passed}/{total} checks pasados")
if failed == 0:
    print("🎉 ¡Fase 8 completada! Puedes avanzar a la Fase 9.")
else:
    print(f"⚠️  {failed} check(s) fallaron. Revisa tu código.")
print("=" * 55)
