# Fase 1 — Fundamentos de Apache Spark

## ¿Qué es Apache Spark?

Apache Spark es un **motor de procesamiento distribuido** de datos de alta velocidad.
A diferencia de Hadoop MapReduce (que escribe en disco entre cada paso), Spark trabaja
**en memoria (RAM)**, lo que lo hace hasta 100x más rápido para ciertos workloads.

### ¿Para qué sirve?
- Procesar datasets enormes (GB, TB, PB) distribuidos en clústeres
- ETL (Extract, Transform, Load)
- Machine Learning distribuido (MLlib)
- Streaming en tiempo real (Structured Streaming)
- Análisis SQL a escala (Spark SQL)

---

## Arquitectura de Spark

```
┌─────────────────────────────────────────────────────┐
│                   SPARK APPLICATION                 │
│                                                     │
│  ┌──────────────┐      ┌────────────────────────┐  │
│  │    DRIVER    │      │   CLUSTER MANAGER       │  │
│  │              │◄────►│  (Local / YARN /        │  │
│  │ SparkContext │      │   Mesos / Kubernetes)   │  │
│  │ SparkSession │      └────────────────────────┘  │
│  └──────┬───────┘                                   │
│         │ distribuye tareas                         │
│    ┌────▼──────────────────────────────────┐        │
│    │              EXECUTORS                │        │
│    │  ┌──────────┐  ┌──────────┐          │        │
│    │  │  Task 1  │  │  Task 2  │  ...     │        │
│    │  │ (core 1) │  │ (core 2) │          │        │
│    │  └──────────┘  └──────────┘          │        │
│    └───────────────────────────────────────┘        │
└─────────────────────────────────────────────────────┘
```

### Componentes clave:
- **Driver** — programa principal, coordina todo, crea el `SparkContext`
- **Executor** — proceso en cada nodo que ejecuta las tareas (Tasks)
- **Cluster Manager** — administra los recursos (CPU, RAM) del clúster
- **SparkContext** — punto de entrada a Spark (nivel bajo, para RDDs)
- **SparkSession** — punto de entrada unificado (nivel alto, para DataFrames + SQL)

---

## Modos de Ejecución

| Modo | Descripción | Uso |
|------|-------------|-----|
| `local` | Un solo proceso, una sola CPU | Desarrollo/testing |
| `local[N]` | Un proceso con N threads | Simular paralelismo local |
| `local[*]` | Usa todos los cores disponibles | Testing local robusto |
| `spark://...` | Clúster Spark standalone | Producción |
| `yarn` | Clúster Hadoop/YARN | Producción en Hadoop |
| `k8s://...` | Kubernetes | Cloud-native |

---

## Lazy Evaluation — El concepto más importante

Spark **NO ejecuta nada** cuando defines una transformación.
Solo ejecuta cuando llamas una **acción**.

```
TRANSFORMACIONES (lazy - no ejecutan)     ACCIONES (trigger de ejecución)
─────────────────────────────────────     ────────────────────────────────
map()       filter()    select()          collect()   show()    count()
flatMap()   groupBy()   join()            take()      save()    first()
withColumn()  orderBy()  union()          reduce()    foreach()
```

### Ejemplo del flujo:
1. Defines transformaciones → Spark construye un **DAG** (grafo de ejecución)
2. Llamas una acción → Spark **optimiza el DAG** con el Catalyst Optimizer
3. Spark ejecuta **solo lo necesario**

---

## SparkContext vs SparkSession

```python
# SparkContext — acceso a RDDs (nivel bajo)
from pyspark import SparkContext
sc = SparkContext("local", "MiApp")   # Antiguo

# SparkSession — todo en uno (recomendado)
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("MiApp") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext  # SparkContext accesible desde SparkSession
```

---

## Archivos de esta fase

| Archivo | Descripción |
|---------|-------------|
| `01_spark_session.py` | Crear y configurar SparkSession |
| `02_lazy_evaluation.py` | Demostrar lazy evaluation con ejemplos |
| `ejercicios.py` | 5 ejercicios guiados |
| `reto.py` | Reto sin guía |
| `solucion_reto.py` | Solución comentada |
| `validar.py` | Script de autoevaluación |

---

## Conceptos que debes dominar al terminar

- [ ] ¿Qué hace el Driver y qué hace el Executor?
- [ ] ¿Por qué Spark es más rápido que MapReduce?
- [ ] Diferencia entre transformación y acción
- [ ] Qué es Lazy Evaluation y por qué existe
- [ ] Crear SparkSession con parámetros personalizados
- [ ] Ver el DAG en la Spark UI (puerto 4040)
