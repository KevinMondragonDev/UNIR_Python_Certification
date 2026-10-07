"""
FASE 7 — Validador Automático
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
print("VALIDADOR — FASE 7: Spark SQL Avanzado")
print("=" * 55)

spark = SparkSession.builder.appName("Validador_Fase7") \
    .master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR  = os.path.join(BASE, "datasets", "csv")
PARQ_DIR = os.path.join(BASE, "datasets", "parquet")

spark.read.csv(os.path.join(CSV_DIR, "employees.csv"), header=True, inferSchema=True) \
     .createOrReplaceTempView("empleados")
spark.read.csv(os.path.join(CSV_DIR, "sales.csv"), header=True, inferSchema=True) \
     .createOrReplaceTempView("ventas")
spark.read.parquet(os.path.join(PARQ_DIR, "events.parquet")) \
     .createOrReplaceTempView("eventos")
spark.read.parquet(os.path.join(PARQ_DIR, "transactions.parquet")) \
     .createOrReplaceTempView("transacciones")

# ─── Test 1: RANK / DENSE_RANK
print("\n[1] RANK y DENSE_RANK")
r1 = spark.sql("""
    SELECT COUNT(*) as c FROM (
        SELECT department, name, salary,
               RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rnk
        FROM empleados WHERE salary IS NOT NULL
    ) WHERE rnk = 1
""").first()["c"]
check("RANK=1 existe por dept", r1 > 0)

# ─── Test 2: ROW_NUMBER top-1
print("\n[2] ROW_NUMBER top-1")
r2 = spark.sql("""
    WITH ranked AS (
        SELECT department, name, salary,
               ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn
        FROM empleados WHERE salary IS NOT NULL
    ) SELECT COUNT(*) as c FROM ranked WHERE rn = 1
""").first()["c"]
n_depts = spark.sql("SELECT COUNT(DISTINCT department) as c FROM empleados").first()["c"]
check(f"Exactamente {n_depts} top-1 (uno por dept)", r2 == n_depts)

# ─── Test 3: Suma acumulada
print("\n[3] Suma acumulada (ROWS BETWEEN)")
r3 = spark.sql("""
    SELECT COUNT(*) as c FROM (
        SELECT salary,
               SUM(salary) OVER (ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
               AS sal_acum
        FROM empleados WHERE salary IS NOT NULL
    ) WHERE sal_acum > 0
""").first()["c"]
check("Suma acumulada genera valores positivos", r3 > 0)

# ─── Test 4: LAG
print("\n[4] LAG — diferencia MoM")
r4 = spark.sql("""
    WITH vm AS (
        SELECT region, DATE_FORMAT(TO_DATE(sale_date,'yyyy-MM-dd'),'yyyy-MM') AS mes,
               SUM(amount) AS total
        FROM ventas GROUP BY region, DATE_FORMAT(TO_DATE(sale_date,'yyyy-MM-dd'),'yyyy-MM')
    )
    SELECT COUNT(*) as c FROM (
        SELECT region, mes, total,
               LAG(total) OVER (PARTITION BY region ORDER BY mes) AS ant
        FROM vm
    ) WHERE ant IS NOT NULL
""").first()["c"]
check("LAG produce valores no nulos (meses con prev)", r4 > 0)

# ─── Test 5: NTILE
print("\n[5] NTILE — cuartiles")
r5 = spark.sql("""
    SELECT COUNT(DISTINCT cuartil) as c FROM (
        SELECT NTILE(4) OVER (ORDER BY salary) AS cuartil
        FROM empleados WHERE salary IS NOT NULL
    )
""").first()["c"]
check("NTILE(4) produce exactamente 4 cuartiles", r5 == 4)

# ─── Test 6: EXPLODE
print("\n[6] EXPLODE")
r6 = spark.sql("SELECT COUNT(*) as c FROM (SELECT EXPLODE(tags) as tag FROM eventos)").first()["c"]
r6_orig = spark.sql("SELECT COUNT(*) as c FROM eventos").first()["c"]
check("EXPLODE produce más filas que el original", r6 > r6_orig)

# ─── Test 7: ARRAY_CONTAINS
print("\n[7] ARRAY_CONTAINS")
r7 = spark.sql("SELECT COUNT(*) as c FROM eventos WHERE ARRAY_CONTAINS(tags, 'promo')").first()["c"]
check("ARRAY_CONTAINS('promo') devuelve resultados", r7 > 0)
check("Menos resultados que total eventos", r7 < r6_orig)

# ─── Test 8: COLLECT_SET
print("\n[8] COLLECT_SET")
r8 = spark.sql("""
    SELECT COUNT(*) as c FROM (
        SELECT user_id, COLLECT_SET(event_type) as tipos
        FROM eventos GROUP BY user_id
    ) WHERE SIZE(tipos) > 3
""").first()["c"]
check("Hay usuarios con más de 3 tipos de evento", r8 > 0)

# ─── Test 9: explain plan funciona
print("\n[9] explain() funciona")
try:
    import io
    from contextlib import redirect_stdout
    buf = io.StringIO()
    with redirect_stdout(buf):
        spark.sql("SELECT name, salary FROM empleados WHERE salary > 50000").explain()
    plan = buf.getvalue()
    check("explain() produce output", len(plan) > 20)
    check("Plan contiene 'Filter' o 'Project'",
          "Filter" in plan or "Project" in plan)
except Exception as e:
    check(f"explain() sin error: {e}", False)
    check("Plan válido", False)

# ─── Test 10: CTEs encadenados
print("\n[10] 3 CTEs encadenados")
r10 = spark.sql("""
    WITH paso1 AS (
        SELECT employee_id, SUM(amount) as total FROM ventas GROUP BY employee_id
    ),
    paso2 AS (
        SELECT e.name, e.department, p.total
        FROM empleados e JOIN paso1 p ON e.employee_id = p.employee_id
    ),
    paso3 AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY department ORDER BY total DESC) AS rn
        FROM paso2
    )
    SELECT COUNT(*) as c FROM paso3 WHERE rn = 1
""").first()["c"]
check("3 CTEs encadenados funcionan", r10 == n_depts)

spark.stop()

print("\n" + "=" * 55)
total = passed + failed
print(f"RESULTADO: {passed}/{total} checks pasados")
if failed == 0:
    print("🎉 ¡Fase 7 completada! Puedes avanzar a la Fase 8.")
else:
    print(f"⚠️  {failed} check(s) fallaron. Revisa tu código.")
print("=" * 55)
