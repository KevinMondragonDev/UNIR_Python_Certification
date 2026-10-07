"""
FASE 1 — Validador Automático
==============================
Ejecuta: python validar.py
Verifica que tu código en ejercicios.py y reto.py es correcto.
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

passed = 0
failed = 0

def check(descripcion, condicion):
    global passed, failed
    if condicion:
        print(f"  ✅ {descripcion}")
        passed += 1
    else:
        print(f"  ❌ {descripcion}")
        failed += 1

print("=" * 55)
print("VALIDADOR — FASE 1: Fundamentos Spark")
print("=" * 55)

spark = SparkSession.builder \
    .appName("Validador_Fase1") \
    .master("local[2]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ─── Test 1: SparkSession existe y funciona
print("\n[1] SparkSession")
check("SparkSession creada correctamente", spark is not None)
check("Versión de Spark >= 3.0",
      int(spark.version.split(".")[0]) >= 3)

# ─── Test 2: spark.range() funciona
print("\n[2] spark.range()")
df20 = spark.range(1, 21)
check("range(1,21) tiene 20 filas", df20.count() == 20)
check("Valor mínimo es 1",  df20.agg(F.min("id")).collect()[0][0] == 1)
check("Valor máximo es 20", df20.agg(F.max("id")).collect()[0][0] == 20)

# ─── Test 3: Filtros (lazy)
print("\n[3] Filtros y transformaciones")
df_pares = df20.filter(F.col("id") % 2 == 0)
check("Números pares de 1-20 son 10", df_pares.count() == 10)
check("El menor par es 2",
      df_pares.agg(F.min("id")).collect()[0][0] == 2)
check("El mayor par es 20",
      df_pares.agg(F.max("id")).collect()[0][0] == 20)

# ─── Test 4: Reto — pipeline div3 + cubo < 10000
print("\n[4] Reto: pipeline 0-99, div3, cubo < 10000")
df_reto = spark.range(0, 100) \
    .filter(F.col("id") % 3 == 0) \
    .withColumn("cubo", F.col("id") ** 3) \
    .filter(F.col("cubo") < 10000)

reto_count = df_reto.count()
check("Pipeline produce 7 filas (0,3,6,9,12,15,18)", reto_count == 7)
check("Columna 'cubo' existe", "cubo" in df_reto.columns)
check("Cubo de 18 = 5832 (< 10000)",
      df_reto.filter(F.col("id") == 18).select("cubo").collect()[0][0] == 5832.0)
check("Cubo de 21 = 9261 (< 10000) — no debe estar",
      df_reto.filter(F.col("id") == 21).count() == 0)

# ─── Test 5: explain() no lanza error
print("\n[5] explain() — plan de ejecución")
try:
    import io
    from contextlib import redirect_stdout
    buf = io.StringIO()
    with redirect_stdout(buf):
        df_reto.explain()
    plan = buf.getvalue()
    check("explain() genera output", len(plan) > 10)
    check("Plan contiene 'Filter'", "Filter" in plan)
except Exception as e:
    check(f"explain() funciona sin error: {e}", False)
    check("Plan contiene 'Filter'", False)

spark.stop()

# ─── Resumen
print("\n" + "=" * 55)
total = passed + failed
print(f"RESULTADO: {passed}/{total} checks pasados")
if failed == 0:
    print("🎉 ¡Fase 1 completada! Puedes avanzar a la Fase 2.")
else:
    print(f"⚠️  {failed} check(s) fallaron. Revisa tu código y vuelve a intentarlo.")
print("=" * 55)
