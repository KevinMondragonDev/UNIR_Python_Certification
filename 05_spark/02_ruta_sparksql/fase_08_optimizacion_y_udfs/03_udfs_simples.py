"""
FASE 8 — Ejemplo 3: UDFs Python
=================================
Aprenderás a crear, registrar y usar UDFs en PySpark.
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType, BooleanType

spark = SparkSession.builder \
    .appName("UDFs_Simples") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

df_emp = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True)

# ─────────────────────────────────────────
# 1. UDF básica con udf()
# ─────────────────────────────────────────
print("=" * 55)
print("1. UDF básica — clasificar salario")
print("=" * 55)

def clasificar_salario(salary):
    if salary is None:
        return "Sin dato"
    if salary < 40000:
        return "Junior"
    if salary < 70000:
        return "Mid"
    if salary < 100000:
        return "Senior"
    return "Lead"

clasificar_udf = F.udf(clasificar_salario, StringType())

df_emp.withColumn("nivel", clasificar_udf(F.col("salary"))) \
      .select("name", "salary", "nivel") \
      .show(10)

# ─────────────────────────────────────────
# 2. UDF con decorador @udf
# ─────────────────────────────────────────
print("=" * 55)
print("2. UDF con decorador @udf")
print("=" * 55)

@F.udf(returnType=DoubleType())
def calcular_bono(salary, pct=0.15):
    if salary is None:
        return 0.0
    return round(salary * pct, 2)

@F.udf(returnType=StringType())
def formato_moneda(valor):
    if valor is None:
        return "N/A"
    return f"${valor:,.0f} MXN"

df_emp.filter(F.col("salary").isNotNull()) \
      .withColumn("bono",      calcular_bono(F.col("salary"))) \
      .withColumn("sal_fmt",   formato_moneda(F.col("salary"))) \
      .select("name", "salary", "bono", "sal_fmt") \
      .show(5)

# ─────────────────────────────────────────
# 3. Registrar UDF para spark.sql()
# ─────────────────────────────────────────
print("=" * 55)
print("3. Registrar UDF para usar en SQL")
print("=" * 55)

df_emp.createOrReplaceTempView("empleados")

# Registrar la función
spark.udf.register("clasificar_sal", clasificar_salario, StringType())
spark.udf.register("calc_bono", lambda s: round(s * 0.10, 2) if s else 0.0, DoubleType())

spark.sql("""
    SELECT name, salary,
           clasificar_sal(salary) AS nivel,
           calc_bono(salary)       AS bono_10pct
    FROM empleados
    WHERE salary IS NOT NULL
    LIMIT 8
""").show()

# ─────────────────────────────────────────
# 4. UDF con múltiples argumentos
# ─────────────────────────────────────────
print("=" * 55)
print("4. UDF con múltiples argumentos")
print("=" * 55)

@F.udf(returnType=StringType())
def crear_id_complejo(emp_id, dept, country):
    if any(v is None for v in [emp_id, dept, country]):
        return "ID_INVALIDO"
    dept_code = dept[:3].upper()
    country_code = country[:2].upper()
    return f"{dept_code}-{country_code}-{emp_id:05d}"

df_emp.withColumn("id_complejo",
    crear_id_complejo(F.col("employee_id"), F.col("department"), F.col("country"))) \
    .select("employee_id", "department", "country", "id_complejo") \
    .show(8)

# ─────────────────────────────────────────
# 5. Por qué las UDFs son lentas — demostración
# ─────────────────────────────────────────
print("=" * 55)
print("5. UDF vs función built-in — comparación")
print("=" * 55)

import time

df_large = spark.range(0, 500_000).withColumn("salary", F.rand(42) * 120000)

# Con UDF Python
t0 = time.time()
df_large.withColumn("nivel_udf", clasificar_udf(F.col("salary"))).count()
t_udf = time.time() - t0

# Con funciones built-in (Catalyst optimizado)
t0 = time.time()
df_large.withColumn("nivel_builtin",
    F.when(F.col("salary") < 40000, "Junior")
     .when(F.col("salary") < 70000, "Mid")
     .when(F.col("salary") < 100000, "Senior")
     .otherwise("Lead")
).count()
t_builtin = time.time() - t0

print(f"UDF Python:      {t_udf:.2f}s")
print(f"Función builtin: {t_builtin:.2f}s")
print(f"Ratio: {t_udf/t_builtin:.1f}x más lento con UDF")
print("\n→ Siempre preferir funciones built-in sobre UDFs cuando sea posible")

spark.stop()
print("\n✅ UDFs Python demostradas")
