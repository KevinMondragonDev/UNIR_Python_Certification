# 🔀 Transformaciones — Seleccionar, Filtrar y Transformar Columnas

> **Objetivo:** Dominar las transformaciones más usadas en el día a día de un Data Engineer: `select`, `filter`, `withColumn`, `alias`, `drop` y más.

> ⚡ **Práctica con Big Data (Opcional):** Si quieres trabajar con un dataset real de **100,000 registros** en formato Parquet, puedes cargar el archivo pregenerado con:
>
> ```python
> df = spark.read.parquet("./datos/empleados.parquet")
> ```

---

## Dataset de práctica|

Usa este DataFrame en todos los retos:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Transformaciones").getOrCreate()

data = [
    ("Ana",    32, "Ingeniería",  75000.0, "México"),
    ("Luis",   28, "Marketing",   55000.0, "Colombia"),
    ("Carla",  35, "Ingeniería",  90000.0, "México"),
    ("Pedro",  41, "Ventas",      60000.0, "Argentina"),
    ("Sofía",  29, "Marketing",   58000.0, "Colombia"),
    ("Jorge",  22, "Ingeniería",  45000.0, "México"),
    ("María",  38, "Ventas",      72000.0, "Argentina"),
]

schema = ["nombre", "edad", "departamento", "salario", "pais"]
df = spark.createDataFrame(data, schema)
df.show()
```

---

## Reto 1 — `select` y `alias`

1. Selecciona solo las columnas `nombre` y `salario`.
2. Selecciona `nombre`, `salario` y crea una columna `salario_mensual` que sea el salario anual dividido entre 12. Usa `col("salario") / 12`.
3. Renombra `departamento` como `area` usando `.alias("area")`.
4. Prueba ambas sintaxis equivalentes:
   ```python
   df.select("nombre", "salario")          # con string
   df.select(col("nombre"), col("salario")) # con col()
   ```

---

## Reto 2 — `filter` y `where`

> `filter()` y `where()` son equivalentes en Spark. Úsalos indistintamente.

1. Filtra los empleados con `salario > 60000`.
2. Filtra los empleados del departamento `"Ingeniería"` **y** con `edad < 35`.
3. Filtra los empleados de `"México"` **o** `"Argentina"`.
4. Filtra los empleados cuyo nombre **empiece con "M"** usando `.startswith("M")`.
5. Filtra los empleados cuyo `salario` esté **entre** 55000 y 75000 usando `.between()`.

```python
# Ejemplos de sintaxis
df.filter(col("salario") > 60000)
df.filter((col("departamento") == "Ingeniería") & (col("edad") < 35))
df.where(col("pais").isin("México", "Argentina"))
```

---

## Reto 3 — `withColumn` (Añadir y Modificar Columnas)

1. **Añade** una columna `salario_mensual` = `salario / 12`.
2. **Añade** una columna `es_senior` que sea `True` si `edad >= 35`, `False` si no. Usa `col("edad") >= 35`.
3. **Añade** una columna `categoria_sueldo` con lógica condicional usando `when` / `otherwise`:

   ```python
   from pyspark.sql.functions import when

   df.withColumn("categoria_sueldo",
       when(col("salario") >= 80000, "Alto")
       .when(col("salario") >= 60000, "Medio")
       .otherwise("Bajo")
   )
   ```

4. **Modifica** la columna `nombre` para que esté en mayúsculas usando `upper(col("nombre"))`.

---

## Reto 4 — `drop`, `withColumnRenamed` y `orderBy`

1. **Elimina** la columna `pais` con `.drop("pais")`.
2. **Renombra** `departamento` a `area` con `.withColumnRenamed("departamento", "area")`.
3. **Ordena** el DataFrame por `salario` de mayor a menor con `.orderBy(col("salario").desc())`.
4. **Ordena** por `departamento` ascendente y luego por `salario` descendente:
   ```python
   df.orderBy(col("departamento").asc(), col("salario").desc())
   ```

---

## Reto 5 — Encadenamiento de Transformaciones

> En Spark se pueden encadenar múltiples transformaciones en una sola expresión. Esto es más eficiente y legible.

Crea un pipeline que, en una sola cadena:

1. Filtre empleados de `"Ingeniería"` con `salario > 50000`.
2. Añada la columna `salario_mensual`.
3. Seleccione solo `nombre`, `edad` y `salario_mensual`.
4. Ordene por `salario_mensual` descendente.
5. Muestre el resultado.

```python
df \
    .filter((col("departamento") == "Ingeniería") & (col("salario") > 50000)) \
    .withColumn("salario_mensual", col("salario") / 12) \
    .select("nombre", "edad", "salario_mensual") \
    .orderBy(col("salario_mensual").desc()) \
    .show()
```

---

## Conceptos clave

| Transformación         | Descripción                                         |
| ---------------------- | --------------------------------------------------- |
| `.select(*cols)`       | Proyecta columnas específicas                       |
| `.filter(condicion)`   | Filtra filas según una condición booleana           |
| `.where(condicion)`    | Alias de `filter`                                   |
| `.withColumn(n, expr)` | Añade o reemplaza una columna con una expresión     |
| `.drop(*cols)`         | Elimina columnas del DataFrame                      |
| `.withColumnRenamed`   | Renombra una columna                                |
| `.orderBy(*cols)`      | Ordena el DataFrame por columnas                    |
| `col("nombre")`        | Referencia a una columna para usarla en expresiones |
| `when().otherwise()`   | Lógica condicional equivalente a `CASE WHEN` en SQL |
