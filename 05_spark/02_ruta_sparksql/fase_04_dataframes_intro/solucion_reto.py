"""
FASE 4 — SOLUCIÓN DEL RETO
============================
⚠️  INTENTA RESOLVER EL RETO ANTES DE VER ESTO ⚠️
"""

import os
import sys
import shutil
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder.appName("Solucion_Reto_Fase4") \
    .master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

# 1. Schema explícito
schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name",        StringType(),  True),
    StructField("department",  StringType(),  True),
    StructField("salary",      DoubleType(),  True),
    StructField("hire_date",   StringType(),  True),
    StructField("country",     StringType(),  True),
])
df = spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, schema=schema)

# 2. Inspección
df.printSchema()
df.describe().show()

# 3. Nulos en salary
nulos = df.filter(F.col("salary").isNull()).count()
print(f"3. Registros con salary nulo: {nulos}")

# 4. DataFrame limpio
df_clean = df.filter(F.col("salary").isNotNull())
print(f"4. Registros en df_clean: {df_clean.count()}")

# 5. Columnas enriquecidas
df_rich = df_clean \
    .withColumn("salary_anual", F.round(F.col("salary") * 12, 2)) \
    .withColumn("nivel",
        F.when(F.col("salary") < 40000, "Junior")
         .when(F.col("salary") < 60000, "Mid")
         .when(F.col("salary") < 90000, "Senior")
         .otherwise("Lead")) \
    .withColumn("hire_date_dt", F.to_date("hire_date", "yyyy-MM-dd")) \
    .withColumn("anios_empresa",
        F.round(F.datediff(F.current_date(), F.col("hire_date_dt")) / 365, 1)) \
    .withColumn("nombre_upper", F.upper("name"))

df_rich.select("name", "nombre_upper", "salary", "salary_anual",
               "nivel", "anios_empresa").show(10)

# 6. Departamentos únicos
depts = df_clean.select("department").distinct().orderBy("department")
print(f"6. Departamentos ({depts.count()}):")
depts.show()

# 7. Contratados post-2020 con salary > 50000
df_recientes = df_rich.filter(
    (F.col("hire_date_dt") > F.lit("2020-01-01")) & (F.col("salary") > 50000)
)
print(f"7. Empleados recientes y bien pagados: {df_recientes.count()}")
df_recientes.select("name", "hire_date", "salary").show(10)

# 8. Departamento con mayor salario promedio
mejor_dept = df_clean.groupBy("department") \
    .agg(F.avg("salary").alias("avg_salary")) \
    .orderBy(F.col("avg_salary").desc())
print("8. Departamentos por salario promedio:")
mejor_dept.show()

# 9. Guardar
out = "/tmp/employees_clean.parquet"
if os.path.exists(out):
    shutil.rmtree(out)
df_rich.write.mode("overwrite").parquet(out)
print(f"9. Guardado en: {out}")

spark.stop()
print("\n✅ Reto Fase 4 completado")
