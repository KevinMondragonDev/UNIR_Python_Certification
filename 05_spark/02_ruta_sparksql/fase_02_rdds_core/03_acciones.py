"""
FASE 2 — Ejemplo 3: Acciones en RDDs
======================================
Aprenderás a:
  - Usar las acciones principales
  - Entender cuándo usar cada una
  - Evitar collect() en datasets grandes
  - Guardar resultados
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Acciones_RDDs") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

datos = sc.parallelize([15, 3, 42, 7, 99, 1, 55, 28, 6, 83])

# ─────────────────────────────────────────
# 1. collect() — trae TODO al driver
# ─────────────────────────────────────────
print("=" * 50)
print("1. collect() — traer todos los datos")
print("=" * 50)
todos = datos.collect()
print(f"Resultado: {todos}")
print(f"Tipo Python: {type(todos)}")
print("⚠️  En producción, solo usar con datasets pequeños")

# ─────────────────────────────────────────
# 2. count() — contar elementos
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("2. count()")
print("=" * 50)
print(f"Total elementos: {datos.count()}")

# ─────────────────────────────────────────
# 3. first() y take(n)
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("3. first() y take(n)")
print("=" * 50)
print(f"first(): {datos.first()}")
print(f"take(3): {datos.take(3)}")
print(f"take(5): {datos.take(5)}")

# ─────────────────────────────────────────
# 4. top(n) — los N más grandes
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("4. top(n) — los N mayores")
print("=" * 50)
print(f"top(3): {datos.top(3)}")
print(f"top(1): {datos.top(1)}")

# ─────────────────────────────────────────
# 5. min(), max(), sum(), mean()
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("5. Funciones estadísticas básicas")
print("=" * 50)
print(f"min():  {datos.min()}")
print(f"max():  {datos.max()}")
print(f"sum():  {datos.sum()}")
print(f"mean(): {datos.mean():.2f}")

# stats() — estadísticas completas
stats = datos.stats()
print(f"\nstats():")
print(f"  Count: {stats.count()}")
print(f"  Mean:  {stats.mean():.2f}")
print(f"  Stdev: {stats.stdev():.2f}")
print(f"  Min:   {stats.min()}")
print(f"  Max:   {stats.max()}")

# ─────────────────────────────────────────
# 6. reduce() — agregar con función binaria
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("6. reduce()")
print("=" * 50)

numeros = sc.parallelize([1, 2, 3, 4, 5])
suma = numeros.reduce(lambda a, b: a + b)
producto = numeros.reduce(lambda a, b: a * b)
maximo = numeros.reduce(lambda a, b: a if a > b else b)

print(f"reduce(suma):     {suma}")      # 15
print(f"reduce(producto): {producto}")  # 120
print(f"reduce(máximo):   {maximo}")    # 5

# reduce con strings
palabras = sc.parallelize(["Hola", "desde", "Apache", "Spark"])
frase = palabras.reduce(lambda a, b: a + " " + b)
print(f"reduce(concat):   {frase}")

# ─────────────────────────────────────────
# 7. countByValue() — frecuencia de cada valor
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("7. countByValue()")
print("=" * 50)

colores = sc.parallelize(["rojo", "azul", "rojo", "verde", "azul", "rojo"])
freq = colores.countByValue()
print(f"Frecuencias: {dict(freq)}")

# ─────────────────────────────────────────
# 8. foreach() — aplicar función en cada executor
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("8. foreach() — ejecutar función en executors")
print("=" * 50)
print("(El output puede aparecer en logs del executor, no en driver)")
numeros.foreach(lambda x: None)  # En local sí se imprime
print("foreach completado (sin retorno de datos)")

# ─────────────────────────────────────────
# 9. saveAsTextFile() — guardar a disco
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("9. saveAsTextFile()")
print("=" * 50)

output_path = "/tmp/rdd_output_fase2"
import shutil
if os.path.exists(output_path):
    shutil.rmtree(output_path)

resultado = datos.map(lambda x: str(x))
resultado.saveAsTextFile(output_path)

archivos = os.listdir(output_path)
print(f"Archivos generados en {output_path}:")
for f in sorted(archivos):
    print(f"  {f}")
print("Contenido del primer part:")
with open(os.path.join(output_path, "part-00000")) as f:
    print(f.read())

spark.stop()
print("\n✅ Acciones RDD demostradas")
