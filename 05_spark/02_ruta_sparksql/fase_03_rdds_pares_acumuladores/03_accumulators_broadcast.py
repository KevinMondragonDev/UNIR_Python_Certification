"""
FASE 3 — Ejemplo 3: Acumuladores y Broadcasts
===============================================
Aprenderás:
  - Crear y usar Accumulators para métricas distribuidas
  - Usar Broadcast para distribuir datos de referencia
  - Cuándo y por qué usar cada uno
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Accumulators_Broadcast") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

# ─────────────────────────────────────────
# PARTE 1: ACUMULADORES
# ─────────────────────────────────────────
print("=" * 55)
print("PARTE 1: Acumuladores (Accumulators)")
print("=" * 55)

# ─────────────────────────────────────────
# 1.1 Acumulador básico — contador
# ─────────────────────────────────────────
print("\n[1.1] Acumulador básico: contador de nulos")

rdd_raw = sc.textFile(os.path.join(CSV_DIR, "employees.csv"))
header  = rdd_raw.first()
rdd     = rdd_raw.filter(lambda l: l != header).map(lambda l: l.split(","))

contador_sin_salario = sc.accumulator(0)

def procesar_empleado(campos):
    global contador_sin_salario
    if campos[3].strip() == "":
        contador_sin_salario.add(1)
    return campos

rdd.foreach(procesar_empleado)
print(f"Empleados sin salario: {contador_sin_salario.value}")

# ─────────────────────────────────────────
# 1.2 Múltiples acumuladores
# ─────────────────────────────────────────
print("\n[1.2] Múltiples acumuladores: contadores por condición")

total_procesados = sc.accumulator(0)
mexico_count     = sc.accumulator(0)
salario_alto     = sc.accumulator(0)   # salario > 80,000

def analizar(campos):
    total_procesados.add(1)
    if campos[5].strip() == "Mexico":
        mexico_count.add(1)
    if campos[3].strip() != "":
        try:
            if float(campos[3]) > 80000:
                salario_alto.add(1)
        except ValueError:
            pass

rdd.foreach(analizar)
print(f"Total procesados:       {total_procesados.value}")
print(f"Empleados de México:    {mexico_count.value}")
print(f"Salario alto (>80k):    {salario_alto.value}")

# ─────────────────────────────────────────
# 1.3 Acumulador con suma (no solo count)
# ─────────────────────────────────────────
print("\n[1.3] Acumulador de suma")

suma_salarios    = sc.accumulator(0.0)
count_salarios   = sc.accumulator(0)

def sumar_salarios(campos):
    if campos[3].strip() != "":
        try:
            suma_salarios.add(float(campos[3]))
            count_salarios.add(1)
        except ValueError:
            pass

rdd.foreach(sumar_salarios)
if count_salarios.value > 0:
    promedio = suma_salarios.value / count_salarios.value
    print(f"Suma total salarios: ${suma_salarios.value:,.2f}")
    print(f"Promedio salario:    ${promedio:,.2f}")

# ─────────────────────────────────────────
# PARTE 2: VARIABLES BROADCAST
# ─────────────────────────────────────────
print("\n" + "=" * 55)
print("PARTE 2: Variables Broadcast")
print("=" * 55)

# ─────────────────────────────────────────
# 2.1 Sin broadcast — problema de serialización
# ─────────────────────────────────────────
print("\n[2.1] Sin broadcast (inefficiente para dicts grandes)")

pais_a_region = {
    "Mexico": "LATAM",
    "Colombia": "LATAM",
    "Argentina": "LATAM",
    "Chile": "LATAM",
    "Peru": "LATAM",
    "España": "EMEA",
    "USA": "NAM"
}

rdd_paises = rdd.map(lambda r: r[5].strip())
# Sin broadcast: pais_a_region se serializa en CADA tarea
regiones = rdd_paises.map(lambda p: pais_a_region.get(p, "UNKNOWN"))
print(f"Regiones encontradas: {set(regiones.collect())}")

# ─────────────────────────────────────────
# 2.2 Con broadcast — eficiente
# ─────────────────────────────────────────
print("\n[2.2] Con broadcast (eficiente)")

pais_bd = sc.broadcast(pais_a_region)

regiones_bd = rdd_paises.map(lambda p: pais_bd.value.get(p, "UNKNOWN"))
print(f"Regiones (broadcast): {set(regiones_bd.collect())}")

# Contar empleados por región usando broadcast
por_region = rdd_paises \
    .map(lambda p: (pais_bd.value.get(p, "UNKNOWN"), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda kv: kv[1], ascending=False)
print("Empleados por región:")
for region, cnt in por_region.collect():
    print(f"  {region}: {cnt}")

# Liberar broadcast cuando ya no se necesita
pais_bd.unpersist()

# ─────────────────────────────────────────
# 2.3 Broadcast de tabla de lookup grande
# ─────────────────────────────────────────
print("\n[2.3] Broadcast de tabla de lookup — productos")

rdd_prods = sc.textFile(os.path.join(CSV_DIR, "products.csv"))
h_p = rdd_prods.first()
# Construir diccionario de referencia en el driver
lookup_productos = {}
for line in rdd_prods.filter(lambda l: l != h_p).collect():
    campos = line.split(",")
    lookup_productos[int(campos[0])] = campos[2]  # {product_id: category}

# Broadcast del lookup
prod_bd = sc.broadcast(lookup_productos)

rdd_ventas = sc.textFile(os.path.join(CSV_DIR, "sales.csv"))
h_v = rdd_ventas.first()
ventas = rdd_ventas.filter(lambda l: l != h_v).map(lambda l: l.split(","))

categorias_ventas = ventas.map(
    lambda r: (prod_bd.value.get(int(r[2]), "UNKNOWN"), float(r[3]))
).reduceByKey(lambda a, b: a + b)

print("Ventas totales por categoría (con broadcast):")
for cat, total in sorted(categorias_ventas.collect(), key=lambda x: -x[1]):
    print(f"  {cat}: ${total:,.2f}")

prod_bd.unpersist()
spark.stop()
print("\n✅ Acumuladores y Broadcasts demostrados")
