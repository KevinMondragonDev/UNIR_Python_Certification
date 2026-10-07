# Fase 2 — RDDs Core

## ¿Qué es un RDD?

**RDD** = Resilient Distributed Dataset

Es la abstracción de datos más fundamental en Spark:
- **Resilient** — se recupera automáticamente de fallos
- **Distributed** — particionado en múltiples nodos
- **Dataset** — colección de datos tipados

### Características
- Inmutable (cada transformación crea un nuevo RDD)
- Lazy (se evalúa solo cuando hay una acción)
- Particionado (el paralelismo se logra con particiones)
- Tipado en tiempo de ejecución (a diferencia de DataFrames que usan schemas)

---

## Crear RDDs

```python
sc = spark.sparkContext

# 1. Desde una colección Python
rdd1 = sc.parallelize([1, 2, 3, 4, 5])
rdd2 = sc.parallelize(range(100), numSlices=4)  # 4 particiones

# 2. Desde archivo de texto
rdd3 = sc.textFile("datasets/csv/employees.csv")  # cada línea es un elemento

# 3. Desde múltiples archivos
rdd4 = sc.wholeTextFiles("datasets/csv/")  # (nombre_archivo, contenido)
```

---

## Transformaciones Principales

| Transformación | Descripción | Ejemplo |
|----------------|-------------|---------|
| `map(f)` | Aplica f a cada elemento | `rdd.map(lambda x: x*2)` |
| `flatMap(f)` | Como map pero "aplana" listas | `rdd.flatMap(lambda x: x.split())` |
| `filter(f)` | Mantiene elementos donde f(x) es True | `rdd.filter(lambda x: x>5)` |
| `distinct()` | Elimina duplicados | `rdd.distinct()` |
| `union(rdd2)` | Une dos RDDs | `rdd1.union(rdd2)` |
| `intersection(rdd2)` | Elementos comunes | `rdd1.intersection(rdd2)` |
| `subtract(rdd2)` | Elementos en rdd1 pero no en rdd2 | `rdd1.subtract(rdd2)` |
| `sample(False, 0.1)` | Muestreo aleatorio (10%) | `rdd.sample(False, 0.1, seed=42)` |
| `sortBy(f)` | Ordenar por criterio | `rdd.sortBy(lambda x: x)` |

### map vs flatMap

```python
rdd = sc.parallelize(["hola mundo", "spark es bueno"])

# map → cada elemento produce UN resultado
rdd.map(lambda s: s.split()).collect()
# [['hola', 'mundo'], ['spark', 'es', 'bueno']]  ← listas dentro de lista

# flatMap → cada elemento produce CERO O MÁS resultados (aplana)
rdd.flatMap(lambda s: s.split()).collect()
# ['hola', 'mundo', 'spark', 'es', 'bueno']  ← lista plana
```

---

## Acciones Principales

| Acción | Descripción | Ejemplo |
|--------|-------------|---------|
| `collect()` | Trae todos los datos al Driver | `rdd.collect()` |
| `count()` | Cuenta elementos | `rdd.count()` |
| `first()` | Primer elemento | `rdd.first()` |
| `take(n)` | Primeros n elementos | `rdd.take(3)` |
| `top(n)` | Los n más grandes | `rdd.top(3)` |
| `reduce(f)` | Reduce a un valor | `rdd.reduce(lambda a,b: a+b)` |
| `sum()` | Suma todos | `rdd.sum()` |
| `min()` / `max()` | Mínimo / Máximo | `rdd.min()` |
| `mean()` | Promedio | `rdd.mean()` |
| `foreach(f)` | Aplica f en cada executor | `rdd.foreach(print)` |
| `saveAsTextFile(path)` | Guardar como texto | `rdd.saveAsTextFile("out/")` |

### ⚠️ Advertencia sobre collect()
```python
# PELIGROSO en producción:
rdd.collect()  # Trae TODOS los datos al Driver — puede explotar la RAM

# Mejor usar:
rdd.take(10)   # Solo los primeros 10
rdd.count()    # Solo el conteo
```

---

## Particiones

```python
rdd = sc.parallelize(range(100), 4)  # 4 particiones

rdd.getNumPartitions()   # → 4

# Ver qué hay en cada partición
rdd.glom().collect()     # Lista de listas, una por partición

# Cambiar particiones
rdd.repartition(8)       # Más particiones (shuffle)
rdd.coalesce(2)          # Menos particiones (sin shuffle si es posible)
```

---

## Archivos de esta fase

| Archivo | Descripción |
|---------|-------------|
| `01_creacion_rdds.py` | Formas de crear RDDs |
| `02_transformaciones.py` | map, flatMap, filter, distinct, etc. |
| `03_acciones.py` | collect, count, reduce, take, etc. |
| `ejercicios.py` | 8 ejercicios guiados |
| `reto.py` | Análisis de employees.csv con RDD puro |
| `solucion_reto.py` | Solución |
| `validar.py` | Autoevaluación |

---

## Conceptos que debes dominar al terminar

- [ ] Crear RDDs desde listas, rangos y archivos
- [ ] Diferencia entre `map` y `flatMap`
- [ ] Cuándo usar transformación vs acción
- [ ] Cómo ver el número de particiones
- [ ] Por qué `collect()` es peligroso en producción
- [ ] Usar `reduce()` para agregar valores
