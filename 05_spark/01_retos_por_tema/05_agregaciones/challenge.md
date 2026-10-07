# 📊 Agregaciones — groupBy, agg y Funciones de Resumen

> **Objetivo:** Aprender a agrupar datos y calcular métricas de resumen como conteos, sumas, promedios, máximos y mínimos.

> ⚡ **Práctica con Big Data (Opcional):** Si quieres trabajar con un dataset real de **100,000 registros** en formato Parquet, puedes cargar el archivo pregenerado con:
> ```python
> df = spark.read.parquet("./datos/ventas_trimestrales.parquet")
> ```

---

## Dataset de práctica

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min, round

spark = SparkSession.builder.appName("Agregaciones").getOrCreate()

data = [
    ("Ana",    "Ingeniería",  75000.0, "Q1", 2024),
    ("Luis",   "Marketing",   55000.0, "Q1", 2024),
    ("Carla",  "Ingeniería",  90000.0, "Q2", 2024),
    ("Pedro",  "Ventas",      60000.0, "Q1", 2024),
    ("Sofía",  "Marketing",   58000.0, "Q2", 2024),
    ("Jorge",  "Ingeniería",  45000.0, "Q2", 2024),
    ("María",  "Ventas",      72000.0, "Q1", 2024),
    ("Ricardo","Marketing",   63000.0, "Q1", 2024),
    ("Elena",  "Ventas",      81000.0, "Q2", 2024),
]

schema = ["nombre", "departamento", "salario", "trimestre", "anio"]
df = spark.createDataFrame(data, schema)
```

---

## Reto 1 — `groupBy` + Funciones de Agregación

1. Agrupa por `departamento` y calcula:
   - El **número de empleados** (`count`).
   - El **salario promedio** (`avg`), redondeado a 2 decimales.
   - El **salario máximo** (`max`).
   - El **salario mínimo** (`min`).

2. Usa `.agg()` para aplicar múltiples funciones en una sola operación:

```python
df.groupBy("departamento").agg(
    count("nombre").alias("num_empleados"),
    round(avg("salario"), 2).alias("salario_promedio"),
    max("salario").alias("salario_max"),
    min("salario").alias("salario_min"),
).show()
```

---

## Reto 2 — Agrupación por Múltiples Columnas

1. Agrupa por `departamento` **y** `trimestre`.
2. Calcula la suma total de salarios por grupo con `sum("salario")`.
3. Ordena el resultado por `departamento` y luego por `trimestre`.

```python
df.groupBy("departamento", "trimestre") \
    .agg(sum("salario").alias("total_salarios")) \
    .orderBy("departamento", "trimestre") \
    .show()
```

---

## Reto 3 — `countDistinct` y `sumDistinct`

1. Cuenta cuántos **departamentos únicos** existen en el DataFrame usando `countDistinct`.
2. Cuenta cuántos **trimestres únicos** aparecen.

```python
from pyspark.sql.functions import countDistinct

df.select(
    countDistinct("departamento").alias("departamentos_unicos"),
    countDistinct("trimestre").alias("trimestres_unicos"),
).show()
```

---

## Reto 4 — Agregar con `pivot` (Tabla Cruzada)

El **pivot** convierte valores de una columna en columnas separadas, ideal para reportes.

1. Agrupa por `departamento`, pivota por `trimestre` y suma los salarios:

```python
df.groupBy("departamento") \
    .pivot("trimestre", ["Q1", "Q2"]) \
    .agg(sum("salario")) \
    .show()
```

**Resultado esperado:**

```
+------------+-------+-------+
|departamento|     Q1|     Q2|
+------------+-------+-------+
| Ingeniería |  75000| 135000|
|  Marketing |  118000|  58000|
|     Ventas |  132000|  81000|
+------------+-------+-------+
```

---

## Reto 5 — `Window Functions` (Funciones de Ventana)

> Las Window Functions permiten calcular valores relativos al grupo sin colapsar las filas — como el `RANK()` de SQL.

1. Importa `Window` y `rank` desde `pyspark.sql.window` y `pyspark.sql.functions`.
2. Crea una ventana particionada por `departamento` y ordenada por `salario` descendente.
3. Añade una columna `ranking` que numere a los empleados dentro de su departamento por salario.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

ventana = Window.partitionBy("departamento").orderBy(col("salario").desc())

df.withColumn("ranking", rank().over(ventana)).show()
```

---

## Conceptos clave

| Función              | Descripción                                                      |
|----------------------|------------------------------------------------------------------|
| `.groupBy(*cols)`    | Agrupa filas por los valores de las columnas indicadas           |
| `.agg(*exprs)`       | Aplica múltiples funciones de agregación en un solo paso         |
| `count(col)`         | Cuenta filas (no ignora nulos en columna específica)             |
| `avg(col)`           | Promedio de los valores                                          |
| `sum(col)`           | Suma total de los valores                                        |
| `max(col)` / `min()` | Valor máximo / mínimo                                            |
| `countDistinct(col)` | Cuenta valores únicos                                            |
| `.pivot(col)`        | Convierte valores de una columna en columnas separadas           |
| `Window`             | Define el rango de filas para funciones de ventana               |
| `rank().over(w)`     | Calcula el ranking de cada fila dentro de su ventana             |
