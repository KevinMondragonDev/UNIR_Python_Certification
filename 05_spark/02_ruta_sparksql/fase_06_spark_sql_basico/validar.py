"""
FASE 6 — Validador Automático
==============================
Ejecuta: python validar.py
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

passed = 0
failed = 0

def check(desc, cond):
    global passed, failed
    if cond:
        print(f"  ✅ {desc}"); passed += 1
    else:
        print(f"  ❌ {desc}"); failed += 1

print("=" * 55)
print("VALIDADOR — FASE 6: Spark SQL Básico")
print("=" * 55)

spark = SparkSession.builder.appName("Validador_Fase6") \
    .master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True) \
     .createOrReplaceTempView("empleados")
spark.read.csv(os.path.join(CSV_DIR, "sales.csv"),    header=True, inferSchema=True) \
     .createOrReplaceTempView("ventas")
spark.read.csv(os.path.join(CSV_DIR, "products.csv"), header=True, inferSchema=True) \
     .createOrReplaceTempView("productos")
spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet")) \
     .createOrReplaceTempView("transacciones")

# ─── Test 1: Vistas temporales
print("\n[1] createOrReplaceTempView")
tablas = {t.name for t in spark.catalog.listTables()}
check("Vista 'empleados' existe",      "empleados" in tablas)
check("Vista 'ventas' existe",         "ventas" in tablas)
check("Vista 'productos' existe",      "productos" in tablas)
check("Vista 'transacciones' existe",  "transacciones" in tablas)

# ─── Test 2: SELECT básico
print("\n[2] SELECT básico")
r = spark.sql("SELECT COUNT(*) as c FROM empleados").first()["c"]
check("SELECT COUNT(*) FROM empleados = 500", r == 500)
r2 = spark.sql("SELECT COUNT(*) as c FROM ventas").first()["c"]
check("SELECT COUNT(*) FROM ventas = 2000", r2 == 2000)

# ─── Test 3: WHERE + funciones
print("\n[3] WHERE y HAVING")
r3 = spark.sql("SELECT COUNT(*) as c FROM empleados WHERE salary > 80000 AND salary IS NOT NULL").first()["c"]
check("Empleados con salary > 80000 existen", r3 > 0)
r4 = spark.sql("""
    SELECT COUNT(*) as c FROM (
        SELECT department FROM empleados WHERE salary IS NOT NULL
        GROUP BY department HAVING COUNT(*) > 30
    )
""").first()["c"]
check("Hay departamentos con más de 30 empleados", r4 > 0)

# ─── Test 4: Funciones de string
print("\n[4] Funciones de string")
r5 = spark.sql("SELECT UPPER('hola') as u").first()["u"]
check("UPPER('hola') = 'HOLA'", r5 == "HOLA")
r6 = spark.sql("SELECT LENGTH('spark') as l").first()["l"]
check("LENGTH('spark') = 5", r6 == 5)
r7 = spark.sql("SELECT CONCAT('Data', 'Frame') as c").first()["c"]
check("CONCAT devuelve 'DataFrame'", r7 == "DataFrame")

# ─── Test 5: Funciones de fecha
print("\n[5] Funciones de fecha")
r8 = spark.sql("SELECT YEAR(TO_DATE('2023-06-15', 'yyyy-MM-dd')) as y").first()["y"]
check("YEAR(2023-06-15) = 2023", r8 == 2023)
r9 = spark.sql("SELECT MONTH(TO_DATE('2023-06-15', 'yyyy-MM-dd')) as m").first()["m"]
check("MONTH(2023-06-15) = 6", r9 == 6)

# ─── Test 6: CASE WHEN
print("\n[6] CASE WHEN")
r10 = spark.sql("""
    SELECT COUNT(*) as c FROM (
        SELECT CASE WHEN salary < 40000 THEN 'Junior'
                    WHEN salary < 70000 THEN 'Mid'
                    ELSE 'Senior' END AS nivel
        FROM empleados WHERE salary IS NOT NULL
    ) WHERE nivel = 'Senior'
""").first()["c"]
check("CASE WHEN genera nivel Senior", r10 > 0)

# ─── Test 7: JOIN en SQL
print("\n[7] JOIN SQL")
r11 = spark.sql("""
    SELECT COUNT(*) as c FROM ventas v
    INNER JOIN empleados e ON v.employee_id = e.employee_id
""").first()["c"]
check("JOIN ventas+empleados tiene filas", r11 > 0)
check("JOIN count <= ventas count", r11 <= 2000)

# ─── Test 8: Subquery
print("\n[8] Subquery")
r12 = spark.sql("""
    SELECT COUNT(*) as c FROM empleados
    WHERE salary > (SELECT AVG(salary) FROM empleados WHERE salary IS NOT NULL)
    AND salary IS NOT NULL
""").first()["c"]
check("Hay empleados sobre el promedio global", r12 > 0)
check("Empleados sobre promedio < 500", r12 < 500)

# ─── Test 9: CTE
print("\n[9] CTE (WITH)")
r13 = spark.sql("""
    WITH ventas_emp AS (
        SELECT employee_id, SUM(amount) as total
        FROM ventas GROUP BY employee_id
    )
    SELECT COUNT(*) as c FROM ventas_emp WHERE total > 5000
""").first()["c"]
check("CTE funciona y devuelve conteo", r13 >= 0)

# ─── Test 10: Funciones de agregación
print("\n[10] Funciones de agregación SQL")
r14 = spark.sql("""
    SELECT ROUND(AVG(amount), 2) as avg FROM ventas
""").first()["avg"]
check("AVG(amount) en ventas es positivo", r14 > 0)
r15 = spark.sql("SELECT COUNT(DISTINCT region) as c FROM ventas").first()["c"]
check("Hay múltiples regiones en ventas", r15 >= 5)

spark.stop()

print("\n" + "=" * 55)
total = passed + failed
print(f"RESULTADO: {passed}/{total} checks pasados")
if failed == 0:
    print("🎉 ¡Fase 6 completada! Puedes avanzar a la Fase 7.")
else:
    print(f"⚠️  {failed} check(s) fallaron. Revisa tu código.")
print("=" * 55)
