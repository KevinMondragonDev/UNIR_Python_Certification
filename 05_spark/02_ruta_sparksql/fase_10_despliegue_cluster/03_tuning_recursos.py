"""
FASE 10 — Ejemplo 3: Dimensionar Executors (Resource Tuning)
==============================================================
Aprenderás a:
  - Calcular --num-executors, --executor-cores y --executor-memory
    a partir del hardware del clúster
  - Entender memoryOverhead y por qué el contenedor es MÁS grande que -Xmx
  - Cómo se reparte la memoria dentro de un executor (unified memory)
  - Elegir spark.sql.shuffle.partitions a partir del volumen de datos

No necesita Spark: es aritmética. Ejecuta: python 03_tuning_recursos.py
"""

import math

# ─────────────────────────────────────────
# Reglas de dimensionamiento (las clásicas de Cloudera/Databricks)
# ─────────────────────────────────────────
CORES_RESERVADOS_SO = 1       # por nodo, para SO + daemons (NodeManager, DataNode)
GB_RESERVADOS_SO = 1          # por nodo
CORES_POR_EXECUTOR = 5        # > 5 satura el throughput de HDFS/S3 por JVM
OVERHEAD_FRACCION = 0.10      # spark.executor.memoryOverhead = max(384MB, 10%)
OVERHEAD_MIN_GB = 0.384
RESERVA_DRIVER_EXECUTORS = 1  # en YARN, 1 "slot" se lo queda el ApplicationMaster/driver


def dimensionar(nodos, cores_por_nodo, ram_gb_por_nodo, cores_por_executor=CORES_POR_EXECUTOR):
    """Devuelve la configuración recomendada para un clúster homogéneo."""
    cores_utiles = cores_por_nodo - CORES_RESERVADOS_SO
    ram_util = ram_gb_por_nodo - GB_RESERVADOS_SO

    executors_por_nodo = max(1, cores_utiles // cores_por_executor)
    ram_por_executor_total = ram_util / executors_por_nodo           # heap + overhead
    overhead = max(OVERHEAD_MIN_GB, ram_por_executor_total * OVERHEAD_FRACCION)
    heap = math.floor(ram_por_executor_total - overhead)

    num_executors = executors_por_nodo * nodos - RESERVA_DRIVER_EXECUTORS
    return {
        "executors_por_nodo": executors_por_nodo,
        "num_executors": num_executors,
        "executor_cores": cores_por_executor,
        "executor_memory_gb": heap,
        "overhead_gb": round(overhead, 2),
        "cores_totales": num_executors * cores_por_executor,
        "cores_ociosos_por_nodo": cores_utiles - executors_por_nodo * cores_por_executor,
    }


def particiones_shuffle(gb_shuffle, cores_totales, mb_por_particion=128):
    """Nº de particiones: ~128 MB cada una, redondeado a múltiplo de los cores."""
    por_tamano = math.ceil(gb_shuffle * 1024 / mb_por_particion)
    return max(cores_totales, math.ceil(por_tamano / cores_totales) * cores_totales)


def memoria_executor(heap_gb, memory_fraction=0.6, storage_fraction=0.5):
    """Reparto interno del heap (Unified Memory Manager, Spark ≥ 1.6)."""
    reservada = 0.3                                   # 300 MB fijos
    usable = heap_gb - reservada
    unificada = usable * memory_fraction              # execution + storage
    return {
        "reservada": reservada,
        "usuario": round(usable * (1 - memory_fraction), 2),   # objetos Python/UDF, metadatos
        "unificada": round(unificada, 2),
        "storage_min": round(unificada * storage_fraction, 2),  # cache() protegida
        "execution_min": round(unificada * (1 - storage_fraction), 2),  # shuffles, joins, sorts
    }


def comando(cfg, master="yarn", deploy_mode="cluster", script="job.py"):
    return (f"spark-submit \\\n"
            f"  --master {master} --deploy-mode {deploy_mode} \\\n"
            f"  --num-executors {cfg['num_executors']} \\\n"
            f"  --executor-cores {cfg['executor_cores']} \\\n"
            f"  --executor-memory {cfg['executor_memory_gb']}g \\\n"
            f"  --conf spark.executor.memoryOverhead={math.ceil(cfg['overhead_gb'] * 1024)}m \\\n"
            f"  --driver-memory 4g \\\n"
            f"  --conf spark.sql.shuffle.partitions={cfg.get('shuffle_partitions', 200)} \\\n"
            f"  {script}")


if __name__ == "__main__":
    print("=" * 62)
    print("1. Clúster de ejemplo: 6 nodos × 16 cores × 64 GB")
    print("=" * 62)
    cfg = dimensionar(nodos=6, cores_por_nodo=16, ram_gb_por_nodo=64)
    print(f"""
Paso a paso:
  cores útiles por nodo   = 16 - {CORES_RESERVADOS_SO} (SO)          = 15
  executors por nodo      = 15 // {CORES_POR_EXECUTOR} cores          = {cfg['executors_por_nodo']}
  RAM útil por nodo       = 64 - {GB_RESERVADOS_SO} (SO)          = 63 GB
  RAM por executor        = 63 / {cfg['executors_por_nodo']}                = {63 / cfg['executors_por_nodo']:.1f} GB  (heap + overhead)
  overhead (10%)          =                       {cfg['overhead_gb']} GB
  --executor-memory       = {63 / cfg['executors_por_nodo']:.1f} - {cfg['overhead_gb']} ≈            {cfg['executor_memory_gb']} GB
  --num-executors         = {cfg['executors_por_nodo']} × 6 - 1 (driver/AM)    = {cfg['num_executors']}
  cores totales           = {cfg['num_executors']} × {CORES_POR_EXECUTOR}               = {cfg['cores_totales']}
""")

    print("¿Por qué no 1 executor gigante de 15 cores por nodo? (\"fat executor\")")
    print("  - Heaps enormes → pausas largas de Garbage Collection")
    print("  - >5 tasks concurrentes por JVM saturan el I/O a HDFS/S3")
    print("¿Por qué no 15 executors de 1 core? (\"tiny executor\")")
    print("  - Broadcasts y caché se replican 15 veces por nodo")
    print("  - El overhead fijo (384 MB) se paga 15 veces")
    print("  → 3-5 cores por executor es el punto medio.\n")

    print("=" * 62)
    print("2. Comparativa de estrategias en el mismo clúster")
    print("=" * 62)
    print(f"{'cores/exec':>10} {'execs':>6} {'heap GB':>8} {'cores tot':>10} {'ociosos/nodo':>13}")
    for c in [1, 2, 3, 4, 5, 8, 15]:
        r = dimensionar(6, 16, 64, cores_por_executor=c)
        print(f"{c:>10} {r['num_executors']:>6} {r['executor_memory_gb']:>8} "
              f"{r['cores_totales']:>10} {r['cores_ociosos_por_nodo']:>13}")

    print("\n" + "=" * 62)
    print("3. Particiones de shuffle según volumen")
    print("=" * 62)
    for gb in [1, 50, 500, 2000]:
        n = particiones_shuffle(gb, cfg["cores_totales"])
        print(f"  shuffle de {gb:>5} GB → spark.sql.shuffle.partitions = {n:>6}  "
              f"({gb * 1024 / n:.0f} MB/partición, {n / cfg['cores_totales']:.0f} oleadas)")
    print("  'Oleadas' = cuántas rondas de tasks necesita cada core para terminar el stage.")

    print("\n" + "=" * 62)
    print(f"4. Reparto de memoria dentro de un executor de {cfg['executor_memory_gb']} GB")
    print("=" * 62)
    m = memoria_executor(cfg["executor_memory_gb"])
    print(f"""
  Contenedor YARN/K8s ............ {cfg['executor_memory_gb'] + cfg['overhead_gb']:.1f} GB
  ├─ memoryOverhead ............... {cfg['overhead_gb']} GB   off-heap: procesos Python de UDFs, buffers de red
  └─ Heap JVM (--executor-memory) . {cfg['executor_memory_gb']} GB
     ├─ Reservada ................. {m['reservada']} GB
     ├─ Usuario (40%) ............. {m['usuario']} GB   estructuras propias, metadatos
     └─ Unificada (60%) ........... {m['unificada']} GB   spark.memory.fraction
        ├─ Storage ................ ≥{m['storage_min']} GB  cache()/persist(), broadcasts
        └─ Execution .............. ≥{m['execution_min']} GB  shuffles, joins, sorts, aggs
        (frontera elástica: execution puede desalojar bloques de caché)

  Errores típicos y su causa:
    "Container killed by YARN for exceeding memory limits"
        → falta memoryOverhead (común con UDFs Python / Pandas UDFs)
    "java.lang.OutOfMemoryError: Java heap space" en executor
        → particiones demasiado grandes: sube shuffle.partitions
    OutOfMemoryError en el DRIVER
        → collect()/toPandas() de muchos datos, o broadcast demasiado grande
""")

    print("=" * 62)
    print("5. Comando spark-submit resultante")
    print("=" * 62)
    cfg["shuffle_partitions"] = particiones_shuffle(500, cfg["cores_totales"])
    print(comando(cfg))

    print("""
Alternativa: Dynamic Allocation (YARN/K8s) — Spark pide y libera executors
según la cola de tasks pendientes:
  --conf spark.dynamicAllocation.enabled=true
  --conf spark.dynamicAllocation.minExecutors=2
  --conf spark.dynamicAllocation.maxExecutors=50
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true
""")
    print("✅ Dimensionamiento explicado")
