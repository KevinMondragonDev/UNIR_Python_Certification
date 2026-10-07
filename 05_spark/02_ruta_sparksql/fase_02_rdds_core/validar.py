"""
FASE 2 — Validador Automático
==============================
Ejecuta: python validar.py
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

passed = 0
failed = 0

def check(desc, cond):
    global passed, failed
    if cond:
        print(f"  ✅ {desc}"); passed += 1
    else:
        print(f"  ❌ {desc}"); failed += 1

print("=" * 55)
print("VALIDADOR — FASE 2: RDDs Core")
print("=" * 55)

spark = SparkSession.builder \
    .appName("Validador_Fase2") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")

# ─── Test 1: parallelize y particiones
print("\n[1] parallelize()")
rdd = sc.parallelize(range(1, 51), 5)
check("range(1,50) tiene 50 elementos", rdd.count() == 50)
check("5 particiones configuradas", rdd.getNumPartitions() == 5)

# ─── Test 2: map() cuadrados
print("\n[2] map() — cuadrados")
cuad = sc.parallelize(range(1, 6)).map(lambda x: x ** 2)
check("Cuadrados de 1-5 son [1,4,9,16,25]",
      cuad.collect() == [1, 4, 9, 16, 25])

# ─── Test 3: filter() múltiplos de 7
print("\n[3] filter() — múltiplos de 7")
mult7 = sc.parallelize(range(1, 51)).filter(lambda x: x % 7 == 0)
check("Múltiplos de 7 en 1-50: [7,14,21,28,35,42,49]",
      sorted(mult7.collect()) == [7, 14, 21, 28, 35, 42, 49])

# ─── Test 4: flatMap()
print("\n[4] flatMap() — palabras")
frases = sc.parallelize(["hola mundo", "spark es bueno"])
palabras = frases.flatMap(lambda s: s.split())
check("flatMap produce lista plana", isinstance(palabras.first(), str))
check("Total 5 palabras", palabras.count() == 5)
check("'hola' está en las palabras", "hola" in palabras.collect())

# ─── Test 5: distinct()
print("\n[5] distinct()")
con_dupes = sc.parallelize([1, 2, 2, 3, 3, 3, 4])
unicos = con_dupes.distinct()
check("distinct() da 4 elementos únicos", unicos.count() == 4)
check("Elementos únicos son {1,2,3,4}",
      set(unicos.collect()) == {1, 2, 3, 4})

# ─── Test 6: textFile y filtro header
print("\n[6] textFile() + quitar header")
ruta = os.path.join(CSV_DIR, "employees.csv")
rdd_raw = sc.textFile(ruta)
header = rdd_raw.first()
rdd_datos = rdd_raw.filter(lambda l: l != header)
check("Header empieza con 'employee_id'", "employee_id" in header)
check("500 filas de datos", rdd_datos.count() == 500)

# ─── Test 7: parseo y extracción de columna
print("\n[7] Parseo CSV — extracción de nombres")
rdd_nombres = rdd_datos.map(lambda l: l.split(",")[1])
check("Primer nombre no está vacío", len(rdd_nombres.first()) > 0)
check("500 nombres extraídos", rdd_nombres.count() == 500)

# ─── Test 8: estadísticas de salarios
print("\n[8] Estadísticas salarios")
rdd_sal = rdd_datos.map(lambda l: l.split(",")[3]) \
                   .filter(lambda s: s.strip() != "") \
                   .map(float)
check("Hay salarios válidos (> 470)", rdd_sal.count() > 470)
check("Salario máximo < 120001", rdd_sal.max() <= 120001)
check("Salario mínimo >= 24999", rdd_sal.min() >= 24999)
check("Media entre 60k y 75k", 60000 <= rdd_sal.mean() <= 75000)

# ─── Test 9: reduce()
print("\n[9] reduce()")
suma = sc.parallelize([1, 2, 3, 4, 5]).reduce(lambda a, b: a + b)
check("reduce(suma) de 1-5 = 15", suma == 15)

spark.stop()

print("\n" + "=" * 55)
total = passed + failed
print(f"RESULTADO: {passed}/{total} checks pasados")
if failed == 0:
    print("🎉 ¡Fase 2 completada! Puedes avanzar a la Fase 3.")
else:
    print(f"⚠️  {failed} check(s) fallaron. Revisa tu código.")
print("=" * 55)
