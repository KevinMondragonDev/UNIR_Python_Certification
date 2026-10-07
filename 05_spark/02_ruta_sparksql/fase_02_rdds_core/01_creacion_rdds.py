"""
FASE 2 — Ejemplo 1: Crear RDDs
================================
Aprenderás a:
  - Crear RDDs desde colecciones Python
  - Crear RDDs desde archivos
  - Inspeccionar particiones
  - Ver el contenido de un RDD
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Creacion_RDDs") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

# ─────────────────────────────────────────
# 1. parallelize() — desde lista Python
# ─────────────────────────────────────────
print("=" * 50)
print("1. parallelize() desde lista")
print("=" * 50)

rdd_numeros = sc.parallelize([10, 20, 30, 40, 50])
print(f"Elementos: {rdd_numeros.collect()}")
print(f"Particiones: {rdd_numeros.getNumPartitions()}")
print(f"Tipo: {type(rdd_numeros)}")

# ─────────────────────────────────────────
# 2. parallelize() con particiones específicas
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("2. parallelize() con numSlices (particiones)")
print("=" * 50)

rdd_4p = sc.parallelize(range(20), numSlices=4)
print(f"Total elementos: {rdd_4p.count()}")
print(f"Número de particiones: {rdd_4p.getNumPartitions()}")
print("Contenido por partición:")
for i, particion in enumerate(rdd_4p.glom().collect()):
    print(f"  Partición {i}: {particion}")

# ─────────────────────────────────────────
# 3. Tipos variados
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("3. RDDs de diferentes tipos")
print("=" * 50)

rdd_strings = sc.parallelize(["hola", "mundo", "spark", "python"])
print(f"Strings: {rdd_strings.collect()}")

rdd_tuplas = sc.parallelize([(1, "Ana"), (2, "Luis"), (3, "Maria")])
print(f"Tuplas:  {rdd_tuplas.collect()}")

rdd_mixto = sc.parallelize([1, "dos", 3.0, True, None])
print(f"Mixto:   {rdd_mixto.collect()}")

# ─────────────────────────────────────────
# 4. textFile() — desde archivo CSV
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("4. textFile() desde CSV (cada línea es un string)")
print("=" * 50)

rdd_emp = sc.textFile(os.path.join(CSV_DIR, "employees.csv"))
print(f"Total líneas (incluyendo header): {rdd_emp.count()}")
print(f"Primera línea (header): {rdd_emp.first()}")
print("Primeras 3 líneas:")
for linea in rdd_emp.take(3):
    print(f"  {linea}")

# ─────────────────────────────────────────
# 5. Quitar el header de un CSV leído con textFile
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("5. Quitar el header del CSV")
print("=" * 50)

header = rdd_emp.first()  # guardar header
rdd_datos = rdd_emp.filter(lambda linea: linea != header)
print(f"Total filas de datos (sin header): {rdd_datos.count()}")
print(f"Primera fila de datos: {rdd_datos.first()}")

# ─────────────────────────────────────────
# 6. Parsear CSV manualmente con RDD
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("6. Parsear CSV → list de fields")
print("=" * 50)

rdd_parseado = rdd_datos.map(lambda linea: linea.split(","))
print("Primeras 3 filas parseadas:")
for fila in rdd_parseado.take(3):
    print(f"  {fila}")

# ─────────────────────────────────────────
# 7. Información del RDD
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("7. Información del RDD")
print("=" * 50)
print(f"Particiones de rdd_emp: {rdd_emp.getNumPartitions()}")
print(f"Tipo de RDD:            {rdd_emp.id()}")

spark.stop()
print("\n✅ Creación de RDDs completada")
