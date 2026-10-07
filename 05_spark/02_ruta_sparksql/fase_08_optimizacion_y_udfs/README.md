# Fase 8 — Optimización y UDFs

## Por qué optimizar

En producción, una query mal escrita puede costar 10x más tiempo y dinero.
Entender las causas de lentitud y cómo evitarlas es clave para un Data Engineer.

---

## Caching — Persistir DataFrames

```python
# cache() — en memoria (nivel por defecto: MEMORY_AND_DISK)
df.cache()
df.persist()

# persist() con nivel explícito
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_ONLY)        # RAM pura, sin spill
df.persist(StorageLevel.MEMORY_AND_DISK)    # RAM + disco si no cabe
df.persist(StorageLevel.DISK_ONLY)          # Solo disco
df.persist(StorageLevel.MEMORY_AND_DISK_2)  # Replicado en 2 nodos

# Verificar si está en caché
df.is_cached   # True/False

# Liberar de la caché
df.unpersist()
```

**¿Cuándo hacer cache?**
- Cuando el mismo DataFrame se usa en múltiples acciones
- Datasets intermedios costosos de recomputar
- Iteración en ML o grafos

---

## Narrow vs Wide — Dónde aparece el shuffle

```
NARROW (sin shuffle)                 WIDE (con shuffle → nuevo Stage)
filter, select, withColumn,          groupBy, join, orderBy, distinct,
map, union, coalesce                 repartition, window(partitionBy)
```

- **Narrow:** cada partición de salida depende de UNA partición de entrada → Spark las encadena en el mismo Stage.
- **Wide:** una partición de salida necesita datos de MUCHAS → Spark escribe a disco, mueve por red (**shuffle**) y abre un Stage nuevo.
- En `explain()` cada shuffle aparece como **`Exchange`**. Contar `Exchange` = contar shuffles.

**Regla mental:** 1 partición = 1 task = 1 core. El shuffle es la operación más cara de Spark (disco + red + serialización).

---

## Particionamiento

```python
# Aumentar particiones — siempre hace shuffle
df.repartition(100)
df.repartition(100, "department")  # por columna (hash)

# Disminuir particiones — evita shuffle cuando puede
df.coalesce(10)

# Ver particiones
df.rdd.getNumPartitions()

# Al escribir — particionado en disco
df.write.partitionBy("year", "month").parquet("s3://bucket/path")
```

**Regla general:** 128MB de datos por partición es el tamaño ideal.

```python
# Cuántas particiones salen de CADA shuffle (default 200)
spark.conf.set("spark.sql.shuffle.partitions", "64")

# Trabajo por partición: inicializa recursos (conexión, modelo) 1 vez por partición
df.rdd.mapPartitions(procesar_particion)
```

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Shuffle | Sí | No |
| Puede aumentar particiones | Sí | No |
| Balanceo | Uniforme | Puede desbalancear |
| Uso típico | Antes de join/agg pesada | Antes de escribir (menos archivos) |

---

## UDFs — User Defined Functions

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType

# UDF simple
def clasificar_salario(salary):
    if salary is None: return "N/A"
    if salary < 40000: return "Junior"
    if salary < 70000: return "Mid"
    return "Senior"

# Registrar como UDF
clasificar_udf = F.udf(clasificar_salario, StringType())

# Usar en DataFrame
df.withColumn("nivel", clasificar_udf(F.col("salary")))

# Registrar para spark.sql()
spark.udf.register("clasificar", clasificar_salario, StringType())
spark.sql("SELECT clasificar(salary) FROM empleados")
```

**⚠️ Problema con UDFs Python:**
- Rompen la optimización del Catalyst
- Los datos salen de la JVM hacia Python y regresan (serialización costosa)
- En producción, prefiere funciones built-in de Spark siempre que sea posible

---

## Pandas UDFs (Vectorizadas) — Mucho más rápidas

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

@pandas_udf(DoubleType())
def calcular_bono(salary: pd.Series) -> pd.Series:
    return salary * 0.15

df.withColumn("bono", calcular_bono(F.col("salary")))
```

**Ventajas de Pandas UDFs:**
- Procesan datos en batches (vectorizados con Apache Arrow)
- 10-100x más rápidas que UDFs Python normales
- Mantienen los tipos de datos eficientemente

---

## Optimización de Joins

```python
# Broadcast join — para tablas pequeñas (< 10MB)
from pyspark.sql import functions as F
df_grande.join(F.broadcast(df_pequena), "id")

# Configurar umbral de broadcast automático
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)  # 10MB

# Hint en SQL
spark.sql("SELECT /*+ BROADCAST(p) */ * FROM ventas v JOIN productos p ON v.product_id = p.product_id")
```

---

## AQE — Adaptive Query Execution (Spark 3.2+: activo por defecto)

Re-optimiza el plan **entre stages** con estadísticas reales del shuffle:

1. **Coalesce de particiones**: fusiona particiones post-shuffle diminutas.
2. **Cambio de estrategia de join**: SortMergeJoin → BroadcastHashJoin si un lado resultó pequeño.
3. **Skew join**: divide particiones gigantes automáticamente.

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
```

En `explain()` tras ejecutar verás `AdaptiveSparkPlan isFinalPlan=true`, `AQEShuffleRead coalesced` y `SortMergeJoin(skew=true)`.

### Estrategias de join en el plan físico

| Estrategia | Shuffle | Cuándo |
|---|---|---|
| `BroadcastHashJoin` | Solo el lado pequeño viaja (broadcast) | grande ⋈ pequeña |
| `SortMergeJoin` | Ambos lados | grande ⋈ grande (default) |
| `ShuffledHashJoin` | Ambos lados, sin sort | un lado mediano que cabe en memoria por partición |

Hints SQL: `/*+ BROADCAST(t) */`, `/*+ MERGE(t) */`, `/*+ SHUFFLE_HASH(t) */`.

---

## Skew de Datos

```python
# Detectar skew
df.groupBy("key_column").count().orderBy(F.col("count").desc()).show()

# Técnica de salting — distribuir clave sesgada artificialmente
import random
salt = 5  # número de "buckets"
df_skewed = df.withColumn("salt", (F.rand() * salt).cast("int")) \
              .withColumn("salted_key", F.concat(F.col("key"), F.col("salt").cast("string")))
```

Orden recomendado ante skew: **broadcast** (si el lado pequeño cabe) → **AQE skewJoin** → **salting manual** (el lado pequeño se replica `salt` veces).

---

## Archivos de esta fase

| Archivo | Descripción |
|---------|-------------|
| `01_caching.py` | cache(), persist() y benchmarks |
| `02_particionamiento.py` | Narrow/wide, repartition, coalesce, shuffle.partitions, mapPartitions, partitionBy |
| `03_udfs_simples.py` | UDFs Python y registro en SQL |
| `04_pandas_udfs.py` | Pandas UDFs vectorizadas |
| `05_optimizacion_joins.py` | SortMerge vs Broadcast, hints, AQE, skew y salting |
| `ejercicios.py` | 12 ejercicios |
| `reto.py` | Optimizar un pipeline con 9 anti-patrones |
| `solucion_reto.py` | Solución |
| `validar.py` | Autoevaluación (incluye checks del reto) |

---

## Conceptos que debes dominar al terminar

- [ ] Cuándo y cómo usar `cache()` vs `persist()`
- [ ] Por qué las UDFs Python son lentas y cuándo evitarlas
- [ ] Escribir una Pandas UDF correctamente
- [ ] Forzar un broadcast join manualmente
- [ ] Detectar skew en los datos
- [ ] Leer el explain plan para identificar shuffles innecesarios
- [ ] Clasificar cualquier transformación como narrow o wide
- [ ] Elegir `repartition` vs `coalesce` y ajustar `shuffle.partitions`
- [ ] Explicar las 3 optimizaciones de AQE
- [ ] Identificar SortMergeJoin vs BroadcastHashJoin en un plan
- [ ] Aplicar salting a un join con skew
