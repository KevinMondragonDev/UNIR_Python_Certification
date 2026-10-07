# Rúbrica de Evaluación — Plan PySpark

Usa esta rúbrica para autoevaluarte honestamente.
**3 = Domino | 2 = Entiendo pero necesito referencia | 1 = En progreso | 0 = Sin ver**

---

## Bloque A — Fundamentos y RDDs (Fases 1-3)

| Habilidad | 0 | 1 | 2 | 3 |
|-----------|---|---|---|---|
| Explico qué es lazy evaluation y por qué importa | ☐ | ☐ | ☐ | ☐ |
| Creo una SparkSession con configuración | ☐ | ☐ | ☐ | ☐ |
| Distingo Driver, Executor y Cluster Manager | ☐ | ☐ | ☐ | ☐ |
| Creo RDDs con parallelize() y textFile() | ☐ | ☐ | ☐ | ☐ |
| Aplico map, flatMap, filter, distinct correctamente | ☐ | ☐ | ☐ | ☐ |
| Uso reduce, count, collect sin abusar de collect() | ☐ | ☐ | ☐ | ☐ |
| Creo Pair RDDs y uso reduceByKey, aggregateByKey | ☐ | ☐ | ☐ | ☐ |
| Explico la diferencia entre groupByKey y reduceByKey | ☐ | ☐ | ☐ | ☐ |
| Hago joins entre Pair RDDs (inner, left, full) | ☐ | ☐ | ☐ | ☐ |
| Uso Acumuladores para conteos distribuidos | ☐ | ☐ | ☐ | ☐ |
| Uso Broadcast para distribuir datos de referencia | ☐ | ☐ | ☐ | ☐ |

**Score A: ___/33**

---

## Bloque B — DataFrames (Fases 4-5)

| Habilidad | 0 | 1 | 2 | 3 |
|-----------|---|---|---|---|
| Creo DataFrames con schema explícito (StructType) | ☐ | ☐ | ☐ | ☐ |
| Leo CSV y Parquet con tipos correctos | ☐ | ☐ | ☐ | ☐ |
| Uso select(), col(), expr() y alias() correctamente | ☐ | ☐ | ☐ | ☐ |
| Filtro con condiciones simples y múltiples (&, |, ~) | ☐ | ☐ | ☐ | ☐ |
| Agrego, renombro y elimino columnas (withColumn, drop) | ☐ | ☐ | ☐ | ☐ |
| Uso funciones de string, fecha y matemáticas | ☐ | ☐ | ☐ | ☐ |
| Uso when/otherwise para lógica condicional | ☐ | ☐ | ☐ | ☐ |
| Hago groupBy con múltiples funciones en agg() | ☐ | ☐ | ☐ | ☐ |
| Distingo y uso los 6 tipos de join | ☐ | ☐ | ☐ | ☐ |
| Uso Window Functions (rank, lag, suma acumulada) | ☐ | ☐ | ☐ | ☐ |
| Manejo nulos con dropna, fillna y coalesce | ☐ | ☐ | ☐ | ☐ |
| Creo tablas pivot y unpivot | ☐ | ☐ | ☐ | ☐ |

**Score B: ___/36**

---

## Bloque C — Spark SQL (Fases 6-7)

| Habilidad | 0 | 1 | 2 | 3 |
|-----------|---|---|---|---|
| Creo vistas temporales y uso spark.sql() | ☐ | ☐ | ☐ | ☐ |
| Escribo queries con SELECT, WHERE, GROUP BY, HAVING | ☐ | ☐ | ☐ | ☐ |
| Uso funciones de string, fecha y CASE WHEN en SQL | ☐ | ☐ | ☐ | ☐ |
| Hago JOINs de 3+ tablas en SQL | ☐ | ☐ | ☐ | ☐ |
| Escribo CTEs limpias con WITH | ☐ | ☐ | ☐ | ☐ |
| Uso subqueries en WHERE, FROM y SELECT | ☐ | ☐ | ☐ | ☐ |
| Uso RANK, DENSE_RANK, ROW_NUMBER en SQL | ☐ | ☐ | ☐ | ☐ |
| Uso LAG/LEAD para análisis temporal | ☐ | ☐ | ☐ | ☐ |
| Calculo sumas acumuladas con ROWS BETWEEN | ☐ | ☐ | ☐ | ☐ |
| Leo un explain plan y entiendo sus nodos | ☐ | ☐ | ☐ | ☐ |
| Uso EXPLODE, ARRAY_CONTAINS, COLLECT_LIST | ☐ | ☐ | ☐ | ☐ |

**Score C: ___/33**

---

## Bloque D — Optimización y Proyecto (Fases 8-9)

| Habilidad | 0 | 1 | 2 | 3 |
|-----------|---|---|---|---|
| Sé cuándo usar cache() vs persist() | ☐ | ☐ | ☐ | ☐ |
| Escribo UDFs Python y las registro en SQL | ☐ | ☐ | ☐ | ☐ |
| Escribo Pandas UDFs vectorizadas | ☐ | ☐ | ☐ | ☐ |
| Explico por qué las UDFs son más lentas que built-ins | ☐ | ☐ | ☐ | ☐ |
| Fuerzo un broadcast join con F.broadcast() | ☐ | ☐ | ☐ | ☐ |
| Detecto skew en los datos | ☐ | ☐ | ☐ | ☐ |
| Construyo un pipeline E2E con ingesta-limpieza-análisis-escritura | ☐ | ☐ | ☐ | ☐ |
| Guardo datos particionados en Parquet | ☐ | ☐ | ☐ | ☐ |

**Score D: ___/24**

---

## Total Global

| Bloque | Score | Máximo |
|--------|-------|--------|
| A — RDDs | ___ | 33 |
| B — DataFrames | ___ | 36 |
| C — Spark SQL | ___ | 33 |
| D — Optimización | ___ | 24 |
| **TOTAL** | **___** | **126** |

**Porcentaje: ___% **

- 90-100% → Listo para entrevistas de Data Engineer con PySpark
- 75-89%  → Sólido, practica los puntos débiles
- 50-74%  → Buen progreso, repasa las fases con menor score
- <50%    → Vuelve a las fases iniciales y refuerza con los ejercicios
