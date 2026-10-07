"""
FASE 10 — Ejemplo 2: Anatomía de una Aplicación Spark
=======================================================
Aprenderás a:
  - Distinguir Application → Job → Stage → Task
  - Predecir cuántos jobs, stages y tasks genera tu código
  - Medirlo desde código con statusTracker() (lo mismo que ves en la Spark UI)
  - Etiquetar jobs con setJobGroup / setJobDescription para encontrarlos en la UI

Jerarquía:
  Application (1 SparkSession)
   └── Job       → 1 por cada ACCIÓN (count, collect, write, show...)
        └── Stage   → se corta en cada SHUFFLE (Exchange)
             └── Task → 1 por PARTICIÓN del stage, corre en 1 core de 1 executor

Ejecutar:
  python 02_anatomia_job.py                 # local
  python 02_anatomia_job.py --pausa         # deja la UI abierta en :4040 para explorarla
  spark-submit --master spark://127.0.0.1:7077 02_anatomia_job.py --pausa
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE, "datasets", "csv")


def medir(spark, grupo, descripcion, accion):
    """Ejecuta `accion` dentro de un job group y devuelve jobs/stages/tasks usados."""
    sc = spark.sparkContext
    sc.setJobGroup(grupo, descripcion)
    resultado = accion()
    sc.setJobGroup("", "")  # limpiar etiqueta

    tracker = sc.statusTracker()
    jobs = sorted(tracker.getJobIdsForGroup(grupo))
    stages, tasks = [], 0
    for job_id in jobs:
        info = tracker.getJobInfo(job_id)
        for stage_id in info.stageIds:
            st = tracker.getStageInfo(stage_id)
            # Stages "skipped" (sus datos ya estaban en caché o en shuffle files)
            # aparecen en el job pero no ejecutan ninguna task → los descartamos
            if st is not None and st.numCompletedTasks > 0:
                stages.append((stage_id, st.numCompletedTasks, st.name.split(" at ")[0]))
                tasks += st.numCompletedTasks
    return resultado, jobs, stages, tasks


def reporte(titulo, jobs, stages, tasks):
    print(f"\n▶ {titulo}")
    print(f"   jobs={len(jobs)}  stages={len(stages)}  tasks={tasks}")
    for sid, n, nombre in stages:
        print(f"     stage {sid:<3} {n:>3} tasks  ({nombre})")


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Anatomia_Job") \
        .config("spark.sql.shuffle.partitions", "6") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    # AQE desactivado: con AQE los números de stages/tasks cambian en runtime
    # (coalesce de particiones) y el ejercicio de PREDECIR pierde sentido.

    print("=" * 60)
    print(f"Master: {spark.sparkContext.master}")
    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    print("=" * 60)

    df = spark.read.csv(os.path.join(CSV_DIR, "sales.csv"), header=True,
                        schema="sale_id INT, employee_id INT, product_id INT, "
                               "amount DOUBLE, sale_date STRING, region STRING") \
              .repartition(4)  # fijamos 4 particiones para que los números sean predecibles

    # ─────────────────────────────────────────
    # 1. Solo transformaciones narrow + 1 acción
    # ─────────────────────────────────────────
    print("""
CASO 1: filter + withColumn + count()
  Predicción: 1 job (una acción) con 3 stages:
    stage A: leer el CSV          → 1 task  (archivo pequeño = 1 partición)
    ── shuffle por repartition(4) ──
    stage B: filter + withColumn + count parcial → 4 tasks (narrow: van juntas)
    ── shuffle: count() suma los parciales en 1 partición ──
    stage C: count final          → 1 task""")
    _, j, s, t = medir(spark, "caso1", "narrow + count",
        lambda: df.filter(F.col("amount") > 1000).withColumn("iva", F.col("amount") * 0.16).count())
    reporte("Caso 1", j, s, t)

    # ─────────────────────────────────────────
    # 2. groupBy → un shuffle más
    # ─────────────────────────────────────────
    print("""
CASO 2: groupBy(region).sum + collect()
  Igual que el caso 1, pero el último stage tiene shuffle.partitions (6) tasks:
    1 (lectura) + 4 (sum parcial) + 6 (sum final) = 11 tasks""")
    _, j, s, t = medir(spark, "caso2", "groupBy + collect",
        lambda: df.groupBy("region").agg(F.sum("amount")).collect())
    reporte("Caso 2", j, s, t)

    # ─────────────────────────────────────────
    # 3. Dos acciones sobre el mismo DF sin caché → recomputa
    # ─────────────────────────────────────────
    print("""
CASO 3: dos acciones (count + collect) sobre el mismo DF SIN cache
  Cada acción = un job independiente que recalcula desde el origen.""")
    agg = df.groupBy("region").agg(F.sum("amount").alias("total"))
    _, j, s, t = medir(spark, "caso3", "2 acciones sin cache",
        lambda: (agg.count(), agg.collect()))
    reporte("Caso 3", j, s, t)

    # ─────────────────────────────────────────
    # 4. Mismo caso con cache()
    # ─────────────────────────────────────────
    print("""
CASO 4: lo mismo con cache()
  La 1ª acción calcula y guarda en memoria las 6 particiones del resultado.
  La 2ª lee de memoria: sus stages previos salen como 'skipped' en la UI
  → compara el total de tasks con el caso 3.""")
    agg_c = df.groupBy("region").agg(F.sum("amount").alias("total")).cache()
    _, j, s, t = medir(spark, "caso4", "2 acciones con cache",
        lambda: (agg_c.count(), agg_c.collect()))
    reporte("Caso 4", j, s, t)
    agg_c.unpersist()

    # ─────────────────────────────────────────
    # 5. Join sin broadcast vs con broadcast
    # ─────────────────────────────────────────
    df_prod = spark.read.csv(os.path.join(CSV_DIR, "products.csv"), header=True, inferSchema=True)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    print("""
CASO 5a: join SortMerge → shuffle de AMBOS lados (más stages y tasks)""")
    _, j, s, t = medir(spark, "caso5a", "join sortmerge",
        lambda: df.join(df_prod, "product_id").count())
    reporte("Caso 5a", j, s, t)

    print("""
CASO 5b: join Broadcast → el lado grande no se shufflea
  Fíjate: 2 jobs con UNA sola acción. El broadcast lanza su propio job
  para leer productos y enviarlo a los executors.""")
    _, j, s, t = medir(spark, "caso5b", "join broadcast",
        lambda: df.join(F.broadcast(df_prod), "product_id").count())
    reporte("Caso 5b", j, s, t)

    print("""
──────────────────────────────────────────────────────────────
CÓMO LEER LA SPARK UI (http://localhost:4040)
  Jobs        → 1 fila por acción. Usa la columna Description (setJobGroup).
  Stages      → Duración, Input, Shuffle Read/Write, Spill (memory/disk).
                Spill > 0 = las particiones no caben en memoria → más particiones
                o más memoria por executor.
  Stage detail→ "Summary Metrics": compara Median vs Max de Duration.
                Max >> Median (p. ej. 10x) = SKEW: una task hace casi todo.
  SQL / DataFrame → el plan físico con métricas reales por operador.
  Executors   → memoria usada, GC time (> 10% del tiempo = poca memoria),
                tasks fallidas.
  Environment → la configuración EFECTIVA (útil para depurar spark-submit).
──────────────────────────────────────────────────────────────""")

    if "--pausa" in sys.argv:
        input(f"\n⏸  UI disponible en {spark.sparkContext.uiWebUrl} — Enter para terminar...")

    spark.stop()
    print("✅ Anatomía de un job demostrada")
