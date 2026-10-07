# Fase 3 — RDDs de Pares, Acumuladores y Broadcasts

## RDDs de Pares (Pair RDDs)

Un **Pair RDD** es un RDD donde cada elemento es una **tupla `(clave, valor)`**.
Son fundamentales para operaciones de agrupamiento, conteo y joins.

```python
# Crear un Pair RDD
rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 3), ("c", 1)])
```

---

## Transformaciones de Pair RDDs

### reduceByKey vs groupByKey

```python
rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 3), ("b", 1)])

# reduceByKey — PREFERIDO (combina en cada partición PRIMERO)
rdd.reduceByKey(lambda a, b: a + b).collect()
# [("a", 4), ("b", 3)]

# groupByKey — EVITAR en datasets grandes (mueve TODOS los valores)
rdd.groupByKey().mapValues(list).collect()
# [("a", [1, 3]), ("b", [2, 1])]
```

**¿Por qué preferir `reduceByKey`?**
- `reduceByKey` hace una combinación parcial en cada partición antes del shuffle
- `groupByKey` mueve TODOS los datos antes de reducir → más datos en red

### aggregateByKey

```python
# Para agregaciones más complejas donde el tipo de acumulador
# es diferente al tipo de los valores
rdd = sc.parallelize([("dept1", 100), ("dept1", 200), ("dept2", 50)])

# aggregateByKey(valor_inicial, combinar_dentro, combinar_entre_particiones)
rdd.aggregateByKey(
    (0, 0),                              # (suma, count) inicial
    lambda acc, v: (acc[0]+v, acc[1]+1), # combinar valor
    lambda a, b: (a[0]+b[0], a[1]+b[1]) # combinar acumuladores
).mapValues(lambda x: x[0]/x[1]).collect()
# promedio por departamento
```

### sortByKey

```python
rdd.sortByKey()                   # orden ascendente
rdd.sortByKey(ascending=False)    # orden descendente
```

---

## Joins en RDDs

```python
empleados = sc.parallelize([(1, "Ana"), (2, "Luis"), (3, "Maria")])
salarios  = sc.parallelize([(1, 50000), (2, 35000), (4, 70000)])

# inner join
empleados.join(salarios).collect()
# [(1, ("Ana", 50000)), (2, ("Luis", 35000))]

# left outer join (todos los de empleados)
empleados.leftOuterJoin(salarios).collect()
# [(1, ("Ana", 50000)), (2, ("Luis", 35000)), (3, ("Maria", None))]

# right outer join (todos los de salarios)
empleados.rightOuterJoin(salarios).collect()
# [(1, ("Ana", 50000)), (2, ("Luis", 35000)), (4, (None, 70000))]

# cogroup — agrupa valores de ambos RDDs por clave
empleados.cogroup(salarios).collect()
```

---

## Acumuladores (Accumulators)

Son **contadores distribuidos** que los executors pueden incrementar,
pero solo el **Driver puede leer**.

```python
# Crear acumulador
contador_nulos = sc.accumulator(0)

def procesar(x):
    if x is None:
        contador_nulos.add(1)
    return x

rdd.foreach(procesar)
print(f"Nulos encontrados: {contador_nulos.value}")
```

**Reglas de acumuladores:**
- Los executors solo pueden **sumar** (`add`)
- Solo el Driver puede **leer** (`.value`)
- En transformaciones (lazy), puede ejecutarse más de una vez — úsalos en `foreach`

---

## Variables Broadcast

Permiten **enviar una copia de una variable a todos los executors** de forma eficiente,
en lugar de serializar la variable en cada tarea.

```python
# Sin broadcast: el diccionario se serializa N veces
dic = {"MX": "México", "CO": "Colombia"}
rdd.map(lambda x: dic[x])  # dic se envía con cada task

# Con broadcast: se envía UNA vez a cada executor
dic_bd = sc.broadcast({"MX": "México", "CO": "Colombia"})
rdd.map(lambda x: dic_bd.value[x])  # cada executor tiene una copia local
```

**¿Cuándo usar broadcast?**
- Dataset de referencia pequeño que necesitan todos los executors
- Tablas de lookup, diccionarios de mapeo, etc.

---

## Particionamiento

```python
rdd = sc.parallelize(range(100))

# repartition — puede aumentar o disminuir (siempre hace shuffle)
rdd_8p = rdd.repartition(8)

# coalesce — SOLO puede disminuir (evita shuffle cuando posible)
rdd_2p = rdd_8p.coalesce(2)

# HashPartitioner — para Pair RDDs
rdd_pair = rdd.map(lambda x: (x % 10, x))
rdd_partitioned = rdd_pair.partitionBy(10)  # 10 particiones por hash de clave
```

---

## Archivos de esta fase

| Archivo | Descripción |
|---------|-------------|
| `01_pair_rdds.py` | Operaciones básicas con key-value RDDs |
| `02_joins_rdds.py` | Joins entre RDDs |
| `03_accumulators_broadcast.py` | Variables compartidas |
| `ejercicios.py` | 8 ejercicios guiados |
| `reto.py` | Análisis de ventas por región |
| `solucion_reto.py` | Solución |
| `validar.py` | Autoevaluación |

---

## Conceptos que debes dominar al terminar

- [ ] Crear Pair RDDs desde datos normales
- [ ] Por qué `reduceByKey` es mejor que `groupByKey`
- [ ] Realizar joins entre dos RDDs
- [ ] Crear y usar acumuladores para métricas de conteo
- [ ] Usar broadcast para evitar serialización redundante
- [ ] Diferencia entre `repartition` y `coalesce`
