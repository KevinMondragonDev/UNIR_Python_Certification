# Fase 10 — Despliegue en Clúster

## Por qué esta fase

Hasta la fase 9 todo corrió con `.master("local[*]")`: un solo proceso JVM en el que
driver y executors comparten memoria. Funciona igual que un clúster **a nivel de API**,
pero esconde lo que en producción importa: la red, la memoria por executor, cuántos
procesos hay y dónde corre el driver.

En esta fase llevas el mismo código a un **clúster real** (master + workers en procesos
separados) y aprendes a dimensionarlo y a leer lo que pasa dentro.

---

## Arquitectura de un clúster Standalone

```
                   spark-submit (tu terminal)
                           │
                           ▼
  ┌─────────────────── DRIVER ───────────────────┐
  │  SparkSession · DAG Scheduler · Spark UI 4040 │
  └──────────────┬───────────────────────────────┘
                 │ pide recursos
                 ▼
  ┌────── MASTER (7077, UI 8080) ──────┐   ← Cluster Manager
  └──────┬──────────────────────┬──────┘     (Standalone / YARN / K8s)
         │ lanza executors       │
  ┌──────▼─────── WORKER 1 ┐  ┌──▼─────────── WORKER 2 ┐
  │ Executor 0 [core][core]│  │ Executor 1 [core][core]│
  │   task  task           │  │   task  task           │
  └────────────────────────┘  └────────────────────────┘
         ▲                            ▲
         └──── shuffle por la RED ────┘
```

| Pieza | Qué hace | Cuántos |
|---|---|---|
| **Driver** | Ejecuta tu `main()`, construye el DAG, reparte tasks, recibe resultados de `collect()` | 1 por aplicación |
| **Cluster Manager** | Asigna CPU/RAM de los nodos a las aplicaciones | 1 por clúster |
| **Worker** | Demonio en cada nodo que lanza executors | 1 por nodo |
| **Executor** | JVM que ejecuta tasks y guarda caché/shuffle | N por aplicación |
| **Task** | Unidad de trabajo: 1 partición en 1 core | 1 por partición y stage |

---

## Application → Job → Stage → Task

| Nivel | Se crea cuando… | Ejemplo |
|---|---|---|
| Application | `SparkSession.getOrCreate()` | 1 por `spark-submit` |
| Job | se llama una **acción** | `count()`, `collect()`, `write`, `show()` |
| Stage | hay un **shuffle** (`Exchange`) | `groupBy`, `join`, `repartition` |
| Task | por cada **partición** del stage | 200 particiones → 200 tasks |

`02_anatomia_job.py` lo mide con `statusTracker()`, así que puedes **predecir** y luego **comprobar**.

---

## spark-submit

```bash
spark-submit \
  --master spark://host:7077 \       # local[4] | spark://... | yarn | k8s://https://...
  --deploy-mode client \             # client | cluster
  --name reporte_regiones \
  --executor-cores 2 \               # cores (tasks simultáneas) por executor
  --executor-memory 4g \             # heap de cada executor
  --total-executor-cores 8 \         # Standalone: tope de cores de la app
  --num-executors 4 \                # YARN/K8s: nº de executors
  --driver-memory 2g \
  --conf spark.sql.shuffle.partitions=64 \
  --conf spark.executor.memoryOverhead=1g \
  --py-files libs.zip \              # módulos Python propios para los executors
  --files config.yaml \              # ficheros auxiliares
  job.py --salida s3://bucket/out    # argumentos del job
```

### client vs cluster

| | `client` | `cluster` |
|---|---|---|
| Dónde corre el driver | En la máquina que hace `spark-submit` | Dentro del clúster (un worker / contenedor AM) |
| Si cierras la terminal | El job muere | El job sigue |
| Logs del driver | En tu terminal | En la UI / logs del clúster |
| Uso típico | Desarrollo, notebooks, `pyspark` interactivo | Producción (Airflow, cron) |

> En Standalone, `--deploy-mode cluster` **no está soportado para Python**. En YARN y K8s sí.

### Precedencia de configuración (de mayor a menor)

1. `.config()` en el código (`SparkSession.builder`)
2. `--conf` / flags de `spark-submit`
3. `conf/spark-defaults.conf`

**Regla:** en el código solo pon la configuración **lógica** del job (AQE, formatos).
Los **recursos** (master, memoria, cores) van en `spark-submit`, así el mismo archivo
corre en cualquier entorno. Ver `01_job_spark_submit.py`.

---

## Dimensionar executors

Ver `03_tuning_recursos.py`. Receta para un nodo de 16 cores y 64 GB:

```
cores útiles   = 16 - 1 (SO)         = 15
executors/nodo = 15 / 5 cores        = 3
RAM/executor   = (64 - 1) / 3        = 21 GB  (heap + overhead)
--executor-memory = 21 - 10%         ≈ 18 GB
```

- **3-5 cores por executor.** Más saturan el I/O y alargan las pausas de GC; menos replican broadcasts y caché demasiadas veces.
- **`memoryOverhead`** (10 %, mínimo 384 MB) es memoria fuera del heap: procesos Python de las UDFs y buffers de red. Si falta, verás el error *"Container killed for exceeding memory limits"*.
- **`shuffle.partitions`** ≈ tamaño del shuffle / 128 MB, redondeado a un múltiplo de los cores totales.

---

## Leer la Spark UI (http://localhost:4040)

| Pestaña | Qué mirar | Señal de problema |
|---|---|---|
| Jobs | Duración por acción | Una acción tarda mucho más que el resto |
| Stages | Shuffle Read/Write, **Spill** | Spill > 0: particiones demasiado grandes |
| Stage → Summary Metrics | **Median vs Max** de Duration | Max ≫ Median: **skew** |
| SQL / DataFrame | Plan con métricas reales | `SortMergeJoin` donde esperabas broadcast |
| Executors | GC Time, tasks fallidas | GC > 10 % del tiempo: poca memoria |
| Environment | Configuración efectiva | El `--conf` que pasaste no aparece |

---

## Levantar un clúster

### Opción A — Sin Docker (recomendada para empezar)

```bash
./cluster_local.sh start     # 1 master + 2 workers (2 cores, 2 GB c/u)
./cluster_local.sh status
spark-submit --master spark://127.0.0.1:7077 \
    --executor-cores 1 --executor-memory 1g --total-executor-cores 4 \
    01_job_spark_submit.py --salida /tmp/reporte --anio 2024
./cluster_local.sh stop
```

Usa el `spark-class` que trae `pip install pyspark`. Son procesos JVM reales y separados.

### Opción B — Docker Compose

```bash
docker compose up -d
docker exec -it spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 --total-executor-cores 4 \
    /opt/workspace/fase_10_despliegue_cluster/01_job_spark_submit.py \
    --salida /opt/workspace/fase_10_despliegue_cluster/output/reporte
docker compose down
```

El job se envía **desde dentro** del contenedor para que driver y executors usen la misma
versión de Spark y de Python (si no coinciden, el job falla al serializar).

---

## Archivos de esta fase

| Archivo | Descripción |
|---------|-------------|
| `01_job_spark_submit.py` | Job "de producción": argparse, logging, códigos de salida, sin `.master()` |
| `02_anatomia_job.py` | Predecir y medir jobs/stages/tasks con `statusTracker` |
| `03_tuning_recursos.py` | Calculadora de executors, memoria y `shuffle.partitions` |
| `cluster_local.sh` | Clúster standalone local sin Docker |
| `docker-compose.yml` | Clúster standalone con Docker |
| `ejercicios.py` | 7 ejercicios |
| `reto.py` | Job de métricas por moneda desplegado en el clúster |
| `solucion_reto.py` | Solución comentada |
| `validar.py` | Autoevaluación (incluye un test opcional contra el clúster) |

---

## Conceptos que debes dominar al terminar

- [ ] Explicar qué hacen el driver, el cluster manager, el worker y el executor
- [ ] Predecir cuántos jobs, stages y tasks genera un fragmento de código
- [ ] Elegir entre `client` y `cluster` como deploy mode
- [ ] Escribir un job sin `.master()` y parametrizado por línea de comandos
- [ ] Dimensionar `--executor-cores`, `--executor-memory` y `--num-executors`
- [ ] Explicar `memoryOverhead` y la memoria unificada (storage/execution)
- [ ] Diagnosticar skew, spill y GC desde la Spark UI
