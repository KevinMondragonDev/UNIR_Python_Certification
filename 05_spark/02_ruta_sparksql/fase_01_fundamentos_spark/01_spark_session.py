"""
FASE 1 — Ejemplo 1: Crear y configurar SparkSession
=====================================================
Aprenderás a:
  - Crear una SparkSession básica
  - Configurar parámetros importantes
  - Acceder al SparkContext desde la SparkSession
  - Revisar la configuración activa
  - Detener la sesión correctamente
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

# ─────────────────────────────────────────
# 1. SparkSession mínima
# ─────────────────────────────────────────
spark_basica = SparkSession.builder \
    .appName("Fase1_Ejemplo") \
    .getOrCreate()

print("=== SparkSession básica ===")
print(f"Versión de Spark: {spark_basica.version}")
print(f"App Name:         {spark_basica.sparkContext.appName}")

# ─────────────────────────────────────────
# 2. SparkSession con configuración detallada
# ─────────────────────────────────────────
spark_basica.stop()  # Siempre cerrar la sesión anterior

spark = SparkSession.builder \
    .appName("Aprendizaje_PySpark") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.sql.repl.eagerEval.enabled", True) \
    .getOrCreate()

# Reducir logs a solo errores
spark.sparkContext.setLogLevel("ERROR")

print("\n=== SparkSession con config ===")
print(f"Versión:           {spark.version}")
print(f"Master:            {spark.sparkContext.master}")
print(f"App ID:            {spark.sparkContext.applicationId}")
print(f"Cores disponibles: {spark.sparkContext.defaultParallelism}")

# ─────────────────────────────────────────
# 3. Acceder al SparkContext
# ─────────────────────────────────────────
sc = spark.sparkContext

print("\n=== SparkContext ===")
print(f"SparkContext: {sc}")
print(f"Versión Spark: {sc.version}")

# ─────────────────────────────────────────
# 4. Revisar configuraciones activas
# ─────────────────────────────────────────
print("\n=== Configuraciones activas (selección) ===")
configs_interes = [
    "spark.app.name",
    "spark.master",
    "spark.sql.shuffle.partitions",
]
for key in configs_interes:
    val = spark.conf.get(key, "NO_DEFINIDA")
    print(f"  {key} = {val}")

# ─────────────────────────────────────────
# 5. getOrCreate — idempotente
# ─────────────────────────────────────────
# Si ya existe una sesión, devuelve la misma (NO crea una nueva)
spark2 = SparkSession.builder.appName("OtraApp").getOrCreate()
print(f"\n¿spark es spark2? {spark is spark2}")  # True — misma sesión

# ─────────────────────────────────────────
# 6. Prueba mínima: crear un DataFrame simple
# ─────────────────────────────────────────
df_prueba = spark.range(5)  # DataFrame con columna 'id' del 0 al 4
df_prueba.show()

print("\n✅ SparkSession configurada correctamente")
print("   Spark UI disponible en: http://localhost:4040")

spark.stop()
