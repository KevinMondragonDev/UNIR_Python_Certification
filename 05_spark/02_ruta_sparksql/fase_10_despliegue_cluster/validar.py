"""
FASE 10 — Validador Automático
===============================
Ejecuta: python validar.py

Si el clúster local está levantado (./cluster_local.sh start),
también envía el job con spark-submit y verifica que corre en él.
"""

import os
import sys
import ast
import socket
import shutil
import subprocess
import tempfile
import importlib.util
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

DIR = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.dirname(DIR)
CSV_DIR = os.path.join(BASE, "datasets", "csv")
TMP = tempfile.mkdtemp(prefix="fase10_")

def cargar(nombre):
    spec = importlib.util.spec_from_file_location(nombre.lstrip("0123456789_"),
                                                  os.path.join(DIR, f"{nombre}.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m

print("=" * 55)
print("VALIDADOR — FASE 10: Despliegue en Clúster")
print("=" * 55)

anatomia = cargar("02_anatomia_job")
tuning   = cargar("03_tuning_recursos")
job      = cargar("01_job_spark_submit")

# ─── Test 1: Anatomía jobs/stages/tasks
print("\n[1] Jobs, stages y tasks (statusTracker)")
spark = SparkSession.builder.appName("Validador_Fase10") \
    .master("local[4]") \
    .config("spark.sql.shuffle.partitions", "6") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"), header=True, inferSchema=True).repartition(4)
_, jobs, stages, tasks = anatomia.medir(spark, "v1", "groupBy",
    lambda: df.filter(F.col("amount") > 0).groupBy("region").count().collect())
check("1 acción → 1 job", len(jobs) == 1)
check("lectura + repartition + groupBy → 3 stages", len(stages) == 3)
check("tasks = 1 + 4 + 6 = 11", tasks == 11)

agg = df.groupBy("region").count().cache()
_, _, _, t_cache = anatomia.medir(spark, "v2", "cache", lambda: (agg.count(), agg.collect()))
agg.unpersist()   # antes de medir sin caché: un plan idéntico reutilizaría la caché
agg2 = df.groupBy("region").count()
_, _, _, t_sin = anatomia.medir(spark, "v3", "sin cache", lambda: (agg2.count(), agg2.collect()))
check("cache() reduce las tasks de la 2ª acción", t_cache < t_sin)

# Ejercicio 1-2 del alumno
ej = {}
try:
    src = open(os.path.join(DIR, "ejercicios.py"), encoding="utf-8").read()
    for nodo in ast.walk(ast.parse(src)):
        if isinstance(nodo, ast.Assign) and isinstance(nodo.targets[0], ast.Name) \
                and isinstance(nodo.value, ast.Constant):
            ej[nodo.targets[0].id] = nodo.value.value
    check("Ejercicio 1: tu predicción (jobs, stages, tasks) = (1, 3, 11)",
          (ej.get("prediccion_jobs"), ej.get("prediccion_stages"), ej.get("prediccion_tasks")) == (1, 3, 11))
except Exception as e:
    check(f"ejercicios.py legible: {e}", False)

spark.stop()

# ─── Test 2: Dimensionamiento
print("\n[2] Dimensionamiento de executors")
cfg = tuning.dimensionar(10, 32, 128)
check("10×32c×128GB → 6 executors/nodo", cfg["executors_por_nodo"] == 6)
check("→ 59 executors (60 - 1 para driver/AM)", cfg["num_executors"] == 59)
check("→ executor-memory 19 GB (21.2 GB - 10% overhead)", cfg["executor_memory_gb"] == 19)
n = tuning.particiones_shuffle(300, cfg["cores_totales"])
check("shuffle.partitions es múltiplo de los cores totales", n % cfg["cores_totales"] == 0)
check("~128 MB por partición (entre 100 y 140 MB)", 100 <= 300 * 1024 / n <= 140)
check("Ejercicio 3: num_executors / executor_memory correctos",
      ej.get("num_executors") == 59 and ej.get("executor_memory") == 19)

# ─── Test 3: Job de producción
print("\n[3] Job parametrizable (01_job_spark_submit.py)")
codigo = job.main(["--salida", os.path.join(TMP, "ok"), "--anio", "2023"])
check("--anio 2023 → exit code 0", codigo == 0)
check("Salida parquet escrita",
      any(f.endswith(".parquet") for f in os.listdir(os.path.join(TMP, "ok"))))
check("--anio 1999 → exit code 1 (reporte vacío)",
      job.main(["--salida", os.path.join(TMP, "vacio"), "--anio", "1999"]) == 1)
src_job = open(os.path.join(DIR, "01_job_spark_submit.py"), encoding="utf-8").read()
check("El job NO fija .master() en el código", ".master(" not in src_job.split('"""', 2)[2])

# ─── Test 4: Reto
print("\n[4] Reto (reto.py)")
reto_src = open(os.path.join(DIR, "reto.py"), encoding="utf-8").read()
try:
    reto = cargar("reto")
    salida = os.path.join(TMP, "reto")
    r = reto.main(["--salida", salida, "--status", "completed", "--status", "refunded"])
    if r is None:
        check("reto.main() implementado", False)
    else:
        check("reto.main() devuelve 0", r == 0)
        carpetas = sorted(d for d in os.listdir(salida) if d.startswith("currency="))
        check("Salida particionada por currency", len(carpetas) >= 2)
        archivos = [f for d in carpetas for f in os.listdir(os.path.join(salida, d)) if f.endswith(".parquet")]
        check("Un archivo parquet por carpeta", len(archivos) == len(carpetas))
        s = SparkSession.builder.master("local[2]").getOrCreate()
        s.sparkContext.setLogLevel("ERROR")
        cols = set(s.read.parquet(salida).columns)
        s.stop()
        check("Columnas: currency, anio_mes, num_tx, total, ticket_promedio, max_tx",
              {"currency", "anio_mes", "num_tx", "total", "ticket_promedio", "max_tx"} <= cols)
        check("Sin .master() en reto.py", ".master(" not in reto_src.split('"""', 2)[2])
        check("Usa try/finally con spark.stop()", "finally" in reto_src and "spark.stop()" in reto_src)
        r_mal = reto.main(["--salida", os.path.join(TMP, "reto_vacio"), "--status", "no_existe"])
        check("Resultado vacío → exit code 1 sin escribir",
              r_mal == 1 and not os.path.exists(os.path.join(TMP, "reto_vacio", "_SUCCESS")))
except Exception as e:
    check(f"reto.py se ejecuta sin errores: {e}", False)

# ─── Test 5: Clúster real (opcional)
print("\n[5] Clúster standalone (opcional)")
sock = socket.socket()
sock.settimeout(1)
cluster_arriba = sock.connect_ex(("127.0.0.1", 7077)) == 0
sock.close()
if not cluster_arriba:
    print("  ⏭  Clúster no detectado en :7077 — ejecuta ./cluster_local.sh start para este test")
else:
    spark_submit = shutil.which("spark-submit") or os.path.join(os.path.dirname(sys.executable), "spark-submit")
    res = subprocess.run(
        [spark_submit, "--master", "spark://127.0.0.1:7077",
         "--executor-cores", "1", "--executor-memory", "1g", "--total-executor-cores", "2",
         os.path.join(DIR, "01_job_spark_submit.py"), "--salida", os.path.join(TMP, "cluster")],
        capture_output=True, text=True, timeout=300)
    check("spark-submit al clúster termina con exit code 0", res.returncode == 0)
    check("El job reporta master spark://", "master              = spark://" in res.stderr + res.stdout)
    check("Se añadieron executors en los workers", "Executor added" in res.stderr + res.stdout)

shutil.rmtree(TMP, ignore_errors=True)

print("\n" + "=" * 55)
total = passed + failed
print(f"RESULTADO: {passed}/{total} checks pasados")
if failed == 0:
    print("🎉 ¡Fase 10 completada! Sabes llevar PySpark a un clúster.")
else:
    print(f"⚠️  {failed} check(s) fallaron. Revisa tu código.")
print("=" * 55)
