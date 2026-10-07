"""
FASE 8 — Ejemplo 4: Pandas UDFs (Vectorizadas)
================================================
Aprenderás a:
  - Crear Pandas UDFs Scalar (Series → Series)
  - Crear Pandas UDFs GroupMap (GroupedData → DataFrame)
  - Comparar performance vs UDF normal
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, StringType, LongType
import pandas as pd
import time

spark = SparkSession.builder \
    .appName("Pandas_UDFs") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

df_emp = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True)

# ─────────────────────────────────────────
# 1. Pandas UDF Scalar — Series → Series
# ─────────────────────────────────────────
print("=" * 55)
print("1. Pandas UDF Scalar (Series → Series)")
print("=" * 55)

@pandas_udf(DoubleType())
def calcular_bono_panda(salary: pd.Series) -> pd.Series:
    return (salary * 0.15).round(2)

@pandas_udf(StringType())
def clasificar_salary_panda(salary: pd.Series) -> pd.Series:
    result = pd.Series("Sin dato", index=salary.index)
    result = result.where(salary.isna(), "Junior")
    result = result.where(salary.isna() | (salary >= 40000), "Junior")
    result[salary >= 40000] = "Mid"
    result[salary >= 70000] = "Senior"
    result[salary >= 100000] = "Lead"
    result[salary.isna()] = "Sin dato"
    return result

df_emp.withColumn("bono",  calcular_bono_panda(F.col("salary"))) \
      .withColumn("nivel", clasificar_salary_panda(F.col("salary"))) \
      .select("name", "salary", "bono", "nivel") \
      .show(8)

# ─────────────────────────────────────────
# 2. Pandas UDF para normalización
# ─────────────────────────────────────────
print("=" * 55)
print("2. Normalización z-score con Pandas UDF")
print("=" * 55)

@pandas_udf(DoubleType())
def zscore(series: pd.Series) -> pd.Series:
    mean = series.mean()
    std  = series.std()
    if std == 0:
        return pd.Series([0.0] * len(series))
    return ((series - mean) / std).round(4)

df_emp.filter(F.col("salary").isNotNull()) \
      .withColumn("salary_zscore", zscore(F.col("salary"))) \
      .select("name", "salary", "salary_zscore") \
      .orderBy(F.col("salary_zscore").desc()) \
      .show(10)

# ─────────────────────────────────────────
# 3. Comparación de performance: UDF vs Pandas UDF
# ─────────────────────────────────────────
print("=" * 55)
print("3. Performance: UDF Python vs Pandas UDF")
print("=" * 55)

df_large = spark.range(0, 200_000).withColumn("salary", F.rand(42) * 120000)

# UDF Python estándar
from pyspark.sql.types import StringType as ST
clasificar_normal = F.udf(
    lambda s: "Junior" if s < 40000 else ("Mid" if s < 70000 else "Senior"),
    ST()
)

t0 = time.time()
df_large.withColumn("nivel", clasificar_normal(F.col("salary"))).count()
t_udf = time.time() - t0

# Pandas UDF
@pandas_udf(StringType())
def clasificar_pandas(s: pd.Series) -> pd.Series:
    return pd.cut(s, bins=[-1, 40000, 70000, float("inf")],
                  labels=["Junior", "Mid", "Senior"]).astype(str)

t0 = time.time()
df_large.withColumn("nivel", clasificar_pandas(F.col("salary"))).count()
t_pandas = time.time() - t0

print(f"UDF Python normal:  {t_udf:.2f}s")
print(f"Pandas UDF:         {t_pandas:.2f}s")
if t_udf > t_pandas:
    print(f"Pandas UDF es {t_udf/t_pandas:.1f}x más rápida")
else:
    print("(En datasets pequeños la diferencia es menor)")

# ─────────────────────────────────────────
# 4. Registrar Pandas UDF para SQL
# ─────────────────────────────────────────
print("=" * 55)
print("4. Registrar Pandas UDF para SQL")
print("=" * 55)

spark.udf.register("bono_panda", calcular_bono_panda)
df_emp.createOrReplaceTempView("empleados")

spark.sql("""
    SELECT name, salary, bono_panda(salary) AS bono
    FROM empleados
    WHERE salary IS NOT NULL
    LIMIT 5
""").show()

spark.stop()
print("\n✅ Pandas UDFs demostradas")
