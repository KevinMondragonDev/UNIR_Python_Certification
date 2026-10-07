"""
FASE 2 — Ejemplo 2: Transformaciones en RDDs
=============================================
Aprenderás a:
  - Usar map, flatMap, filter, distinct, union
  - Entender la diferencia map vs flatMap
  - Encadenar transformaciones
  - Ordenar y muestrear RDDs
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Transformaciones_RDDs") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

# ─────────────────────────────────────────
# 1. map() — transforma cada elemento
# ─────────────────────────────────────────
print("=" * 50)
print("1. map()")
print("=" * 50)

numeros = sc.parallelize([1, 2, 3, 4, 5])

dobles   = numeros.map(lambda x: x * 2)
cuadrados = numeros.map(lambda x: x ** 2)
como_str = numeros.map(lambda x: f"num_{x}")

print(f"Original: {numeros.collect()}")
print(f"Dobles:   {dobles.collect()}")
print(f"Cuadrados:{cuadrados.collect()}")
print(f"Strings:  {como_str.collect()}")

# map sobre tuplas
tuplas = sc.parallelize([(1, "Ana", 50000), (2, "Luis", 35000), (3, "Maria", 70000)])
nombres = tuplas.map(lambda t: t[1])
salarios = tuplas.map(lambda t: t[2])
print(f"Nombres:  {nombres.collect()}")
print(f"Salarios: {salarios.collect()}")

# ─────────────────────────────────────────
# 2. flatMap() — map + aplanar resultados
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("2. flatMap() vs map()")
print("=" * 50)

frases = sc.parallelize(["Apache Spark es rápido", "Scala y Python son soportados"])

# map → lista de listas
con_map = frases.map(lambda f: f.split(" "))
print(f"Con map:    {con_map.collect()}")

# flatMap → lista plana de palabras
con_flatmap = frases.flatMap(lambda f: f.split(" "))
print(f"Con flatMap: {con_flatmap.collect()}")

# Caso de uso: word count base
conteo_palabras_rdd = frases.flatMap(lambda f: f.split(" "))
print(f"Total palabras: {conteo_palabras_rdd.count()}")

# flatMap también puede filtrar (devolver lista vacía = elemento ignorado)
numeros2 = sc.parallelize([1, 2, 3, 4, 5, 6])
pares_via_flatmap = numeros2.flatMap(lambda x: [x] if x % 2 == 0 else [])
print(f"Pares vía flatMap: {pares_via_flatmap.collect()}")

# ─────────────────────────────────────────
# 3. filter() — mantener elementos que cumplen condición
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("3. filter()")
print("=" * 50)

datos = sc.parallelize(range(1, 21))

mayores_10    = datos.filter(lambda x: x > 10)
multiplos_3   = datos.filter(lambda x: x % 3 == 0)
entre_5_y_15  = datos.filter(lambda x: 5 <= x <= 15)

print(f"Mayores de 10:  {mayores_10.collect()}")
print(f"Múltiplos de 3: {multiplos_3.collect()}")
print(f"Entre 5 y 15:   {entre_5_y_15.collect()}")

# filter en strings
nombres_rdd = sc.parallelize(["Ana", "Luis", "Alberto", "Alex", "Beatriz", "Alejandro"])
empieza_a   = nombres_rdd.filter(lambda n: n.lower().startswith("a"))
print(f"Empiezan con A: {empieza_a.collect()}")

# ─────────────────────────────────────────
# 4. distinct() — eliminar duplicados
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("4. distinct()")
print("=" * 50)

con_dupes = sc.parallelize([1, 2, 2, 3, 3, 3, 4, 4, 4, 4])
unicos    = con_dupes.distinct()
print(f"Con duplicados: {con_dupes.collect()}")
print(f"Sin duplicados: {sorted(unicos.collect())}")

# ─────────────────────────────────────────
# 5. union(), intersection(), subtract()
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("5. Operaciones de conjunto")
print("=" * 50)

a = sc.parallelize([1, 2, 3, 4, 5])
b = sc.parallelize([3, 4, 5, 6, 7])

print(f"A:                {a.collect()}")
print(f"B:                {b.collect()}")
print(f"union (A∪B):      {sorted(a.union(b).distinct().collect())}")
print(f"intersection(A∩B):{sorted(a.intersection(b).collect())}")
print(f"subtract(A-B):    {sorted(a.subtract(b).collect())}")

# ─────────────────────────────────────────
# 6. sortBy() — ordenar
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("6. sortBy()")
print("=" * 50)

empleados = sc.parallelize([
    ("Ana", 50000),
    ("Luis", 35000),
    ("Maria", 70000),
    ("Pedro", 45000)
])

por_salario_asc  = empleados.sortBy(lambda e: e[1])
por_salario_desc = empleados.sortBy(lambda e: e[1], ascending=False)
por_nombre       = empleados.sortBy(lambda e: e[0])

print(f"Por salario asc:  {por_salario_asc.collect()}")
print(f"Por salario desc: {por_salario_desc.collect()}")
print(f"Por nombre:       {por_nombre.collect()}")

# ─────────────────────────────────────────
# 7. sample() — muestreo aleatorio
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("7. sample()")
print("=" * 50)

grande = sc.parallelize(range(1000))
muestra = grande.sample(withReplacement=False, fraction=0.05, seed=42)
print(f"Total: {grande.count()}")
print(f"Muestra (~5%): {muestra.count()} elementos")
print(f"Primeros 10 de la muestra: {muestra.take(10)}")

spark.stop()
print("\n✅ Transformaciones RDD demostradas")
