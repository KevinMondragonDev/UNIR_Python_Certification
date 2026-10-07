# 🧱 RDDs — Resilient Distributed Datasets

> **Objetivo:** Comprender la estructura de datos fundamental de Spark: el RDD. Aprender a crearlo, transformarlo y ejecutar acciones sobre él.

---

## ¿Qué es un RDD?

Un **RDD (Resilient Distributed Dataset)** es la abstracción de datos más básica de Spark:

- **Resiliente:** Se recupera automáticamente ante fallos de nodos.
- **Distribuido:** Los datos se dividen en particiones repartidas entre los workers.
- **Dataset:** Es una colección de datos de solo lectura.

```
Lista Python  →  sc.parallelize()  →  RDD (particionado)
Archivo       →  sc.textFile()     →  RDD (cada línea = un elemento)
```

> ⚠️ En la práctica moderna se usan **DataFrames**, pero entender RDDs es esencial para comprender cómo funciona Spark internamente.

---

## Reto 1 — Crear RDDs

1. Crea un `SparkContext` accediendo a él desde tu `SparkSession` (`spark.sparkContext`).
2. Crea un RDD llamado `numeros_rdd` desde la lista: `[10, 20, 30, 40, 50, 60]`.
3. Crea un segundo RDD llamado `texto_rdd` desde la lista: `["spark", "es", "muy", "rapido"]`.
4. Imprime el número de particiones de `numeros_rdd` con `.getNumPartitions()`.
5. Imprime todos los elementos de `texto_rdd` con `.collect()`.

---

## Reto 2 — Transformaciones (Lazy)

> Las transformaciones en Spark son **perezosas (lazy)**: no se ejecutan hasta que se llama una acción.

Dado el RDD: `numeros_rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])`

Aplica las siguientes transformaciones **en cadena**:

1. **`.map()`** — Eleva cada número al cuadrado (`x ** 2`). Guárdalo en `cuadrados_rdd`.
2. **`.filter()`** — Filtra solo los cuadrados mayores a 25. Guárdalo en `grandes_rdd`.
3. **`.map()`** — Convierte cada número a string con el prefijo `"Valor: "`. Guárdalo en `texto_rdd`.
4. Ejecuta `.collect()` en `texto_rdd` para ver los resultados finales.

```python
numeros_rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

cuadrados_rdd = numeros_rdd.map(lambda x: x ** 2)
grandes_rdd   = cuadrados_rdd.filter(lambda x: x > 25)
texto_rdd     = grandes_rdd.map(lambda x: f"Valor: {x}")

print(texto_rdd.collect())
```

**Resultado esperado:**
```
['Valor: 36', 'Valor: 49', 'Valor: 64', 'Valor: 81', 'Valor: 100']
```

---

## Reto 3 — Acciones

Las acciones disparan la ejecución del plan y devuelven resultados al driver.

Dado: `datos_rdd = sc.parallelize([3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5])`

Aplica estas acciones:

| Acción                   | Qué retorna                          |
|--------------------------|--------------------------------------|
| `.count()`               | Número total de elementos            |
| `.sum()`                 | Suma de todos los elementos          |
| `.max()` / `.min()`      | Valor máximo y mínimo                |
| `.first()`               | Primer elemento del RDD              |
| `.take(3)`               | Lista con los primeros 3 elementos   |
| `.distinct().collect()`  | Lista sin valores repetidos          |

---

## Reto 4 — Word Count (El Clásico de Spark)

El **Word Count** es el "Hola Mundo" de Spark. Cuenta cuántas veces aparece cada palabra en un texto.

```python
texto = ["apache spark es rapido", "spark es facil de usar", "apache es open source"]
texto_rdd = sc.parallelize(texto)
```

1. Usa `.flatMap(lambda line: line.split(" "))` para separar cada línea en palabras individuales.
2. Usa `.map(lambda word: (word, 1))` para convertir cada palabra en un par `(palabra, 1)`.
3. Usa `.reduceByKey(lambda a, b: a + b)` para sumar los conteos por palabra.
4. Ordena el resultado por conteo descendente con `.sortBy(lambda x: x[1], ascending=False)`.
5. Imprime el resultado con `.collect()`.

**Resultado esperado:**
```
[('es', 3), ('apache', 2), ('spark', 2), ('rapido', 1), ('facil', 1), ...]
```

---

## Conceptos clave

| Concepto             | Tipo          | Descripción                                               |
|----------------------|---------------|-----------------------------------------------------------|
| `parallelize()`      | Creación      | Crea un RDD desde una colección local                     |
| `.map(func)`         | Transformación| Aplica una función a cada elemento                        |
| `.filter(func)`      | Transformación| Conserva solo los elementos que cumplen la condición      |
| `.flatMap(func)`     | Transformación| Como `map` pero aplana los resultados                     |
| `.reduceByKey(func)` | Transformación| Agrupa por clave y reduce los valores                     |
| `.collect()`         | Acción        | Trae todos los datos del cluster al driver                |
| `.count()`           | Acción        | Devuelve el número de elementos                           |
| `.take(n)`           | Acción        | Devuelve los primeros `n` elementos                       |
