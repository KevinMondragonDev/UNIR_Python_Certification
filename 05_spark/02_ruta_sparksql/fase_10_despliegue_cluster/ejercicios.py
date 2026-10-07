"""
FASE 10 — Ejercicios: Despliegue en Clúster
=============================================
Completa donde dice # TU CÓDIGO AQUÍ
Ejecuta: python validar.py para verificar
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import importlib.util

def _cargar(nombre):
    ruta = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{nombre}.py")
    spec = importlib.util.spec_from_file_location(nombre.lstrip("0123456789_"), ruta)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m

anatomia = _cargar("02_anatomia_job")
tuning   = _cargar("03_tuning_recursos")
job      = _cargar("01_job_spark_submit")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

# ─────────────────────────────────────────
# EJERCICIO 1 — Predecir jobs / stages / tasks
# Con 4 particiones de entrada, shuffle.partitions = 6 y AQE apagado,
# PREDICE (sin ejecutar) cuántos jobs, stages y tasks genera:
#
#     df.filter(F.col("amount") > 0).groupBy("region").count().collect()
#
# donde df = CSV pequeño (1 partición) + repartition(4)
# Pista: repasa los casos 1 y 2 de 02_anatomia_job.py
# ─────────────────────────────────────────
prediccion_jobs   = None  # TU CÓDIGO AQUÍ
prediccion_stages = None  # TU CÓDIGO AQUÍ
prediccion_tasks  = None  # TU CÓDIGO AQUÍ

# ─────────────────────────────────────────
# EJERCICIO 2 — Verificar tu predicción con statusTracker
# Usa anatomia.medir(spark, grupo, descripcion, accion)
# ─────────────────────────────────────────
print("Ejercicio 2: medir jobs/stages/tasks")
spark = SparkSession.builder.appName("Ejercicios_Fase10") \
    .master("local[4]") \
    .config("spark.sql.shuffle.partitions", "6") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"), header=True, inferSchema=True).repartition(4)

medido_jobs, medido_stages, medido_tasks = None, None, None
# TU CÓDIGO AQUÍ
print(f"Predicción: {prediccion_jobs}/{prediccion_stages}/{prediccion_tasks}  "
      f"Medido: {medido_jobs}/{medido_stages}/{medido_tasks}")

spark.stop()

# ─────────────────────────────────────────
# EJERCICIO 3 — Dimensionar un clúster
# Clúster: 10 nodos × 32 cores × 128 GB, executors de 5 cores.
# Usa tuning.dimensionar() y guarda los valores.
# Luego CALCULA A MANO (en un comentario) para comprobar que entiendes cada paso.
# ─────────────────────────────────────────
print("\nEjercicio 3: dimensionar executors")
num_executors   = None  # TU CÓDIGO AQUÍ
executor_memory = None  # TU CÓDIGO AQUÍ (GB, entero)
# Cálculo a mano:
#   cores útiles/nodo = ...
#   executors/nodo    = ...
#   ...
print(f"--num-executors {num_executors} --executor-memory {executor_memory}g")

# ─────────────────────────────────────────
# EJERCICIO 4 — Particiones de shuffle
# Con la configuración del ejercicio 3, ¿cuántas shuffle.partitions
# usarías para un shuffle de 300 GB? (usa tuning.particiones_shuffle)
# ─────────────────────────────────────────
print("\nEjercicio 4: shuffle.partitions para 300 GB")
particiones_300gb = None  # TU CÓDIGO AQUÍ
print(particiones_300gb)

# ─────────────────────────────────────────
# EJERCICIO 5 — Ejecutar el job como lo haría un orquestador
# Llama job.main([...]) (no spark-submit) con:
#   a) --anio 2023 y una --salida temporal  → debe devolver 0
#   b) --anio 1999                          → debe devolver 1 (reporte vacío)
# ─────────────────────────────────────────
print("\nEjercicio 5: códigos de salida del job")
codigo_ok = None     # TU CÓDIGO AQUÍ
codigo_vacio = None  # TU CÓDIGO AQUÍ
print(f"2023 → {codigo_ok} | 1999 → {codigo_vacio}")

# ─────────────────────────────────────────
# EJERCICIO 6 — Escribir el comando spark-submit
# Para el clúster local (./cluster_local.sh start) con:
#   - 2 executors de 2 cores y 1 GB cada uno (pista: --total-executor-cores)
#   - shuffle.partitions = 8
#   - job 01_job_spark_submit.py con --anio 2024 --salida /tmp/r
# ─────────────────────────────────────────
comando_submit = """
TU CÓDIGO AQUÍ
"""

# ─────────────────────────────────────────
# EJERCICIO 7 — Preguntas de concepto (responde con "client" o "cluster")
# ─────────────────────────────────────────
respuestas = {
    # ¿En qué modo el driver corre en TU máquina y si cierras la terminal el job muere?
    "driver_en_mi_maquina": None,
    # ¿Qué modo usarías para un job nocturno programado en Airflow sobre YARN?
    "job_produccion": None,
    # ¿Qué modo necesitas para usar pyspark interactivo / un notebook?
    "notebook": None,
}

print("\nEjercicios completados. Ejecuta: python validar.py")
