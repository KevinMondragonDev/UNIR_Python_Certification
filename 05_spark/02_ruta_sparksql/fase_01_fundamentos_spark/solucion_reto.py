"""
FASE 1 — SOLUCIÓN DEL RETO
============================
⚠️  INTENTA RESOLVER EL RETO ANTES DE VER ESTO ⚠️
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. SparkSession
spark = SparkSession.builder \
    .appName("Reto_Fase1") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 2. DataFrame 0..99
df = spark.range(0, 100)

# 3. Transformaciones (lazy)
df_div3    = df.filter(F.col("id") % 3 == 0)
df_con_cubo = df_div3.withColumn("cubo", F.col("id") ** 3)
df_final   = df_con_cubo.filter(F.col("cubo") < 10000)

# 4. Plan de ejecución
print("=== Plan de ejecución ===")
df_final.explain()

# 5. Acciones
print(f"\n5a) Total filas: {df_final.count()}")
print("\n5b) Primeras 5 filas:")
df_final.show(5)
print(f"\n5c) Primera fila: {df_final.first()}")

# 6. Info del sistema
print(f"\n6) Versión de Spark: {spark.version}")
print(f"   Cores disponibles: {spark.sparkContext.defaultParallelism}")

# 7. Cerrar
spark.stop()
print("\n✅ Reto Fase 1 completado")
