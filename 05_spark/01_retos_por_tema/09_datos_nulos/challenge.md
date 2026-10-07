# 🕳️ Manejo de Datos Nulos — `null`, `NaN` y Datos Faltantes

> **Objetivo:** Detectar, analizar y resolver datos nulos o faltantes en DataFrames de Spark, una de las tareas más frecuentes en Data Engineering.

> ⚡ **Práctica con Big Data (Opcional):** Si quieres trabajar con un dataset real de **100,000 registros** en formato Parquet, puedes cargar el archivo pregenerado con:
> ```python
> df = spark.read.parquet("./datos/empleados_nulos.parquet")
> ```

---

## ¿Por qué son tan importantes los nulos?

En pipelines reales, los datos casi nunca llegan limpios. Valores `null` pueden venir de:
- Joins que no encuentran coincidencia.
- Archivos CSV con celdas vacías.
- APIs que no retornan todos los campos.
- Errores de ingesta de datos.

Un `null` no manejado puede **propagar errores silenciosos** a través de todo el pipeline.

---

## Dataset de práctica

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count

spark = SparkSession.builder.appName("Nulos").getOrCreate()

data = [
    ("Ana",   32,   "Ingeniería", 75000.0),
    ("Luis",  None, "Marketing",  55000.0),
    ("Carla", 35,   None,         90000.0),
    ("Pedro", 41,   "Ventas",     None),
    ("Sofía", None, None,         58000.0),
    ("Jorge", 22,   "Ingeniería", float("nan")),  # NaN ≠ null
    (None,    38,   "Ventas",     72000.0),
]

schema = ["nombre", "edad", "departamento", "salario"]
df = spark.createDataFrame(data, schema)
df.show()
```

---

## Reto 1 — Detectar Nulos

1. Usa `.isNull()` para filtrar filas donde `nombre` es nulo.
2. Usa `.isNotNull()` para filtrar filas donde `salario` **no** es nulo.
3. Crea un **reporte de nulos por columna** que muestre cuántos nulos hay en cada una:

```python
# Reporte de nulos por columna
df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df.columns
]).show()
```

4. Detecta los valores `NaN` (Not a Number) en la columna `salario` usando `isnan()`:
```python
from pyspark.sql.functions import isnan
df.filter(isnan(col("salario"))).show()
```

> ⚠️ `null` y `NaN` son cosas distintas en Spark. `isNull()` no detecta `NaN`.

---

## Reto 2 — Eliminar Filas con Nulos (`dropna`)

1. Elimina **todas** las filas que tengan al menos un nulo: `.na.drop()`.
2. Elimina solo las filas donde **todas** las columnas sean nulas: `.na.drop(how="all")`.
3. Elimina filas donde `nombre` **o** `salario` sean nulos:
   ```python
   df.na.drop(subset=["nombre", "salario"])
   ```
4. Elimina filas que tengan **menos de 3 valores no nulos** usando `thresh`:
   ```python
   df.na.drop(thresh=3)
   ```

---

## Reto 3 — Rellenar Nulos (`fillna` / `na.fill`)

1. Rellena todos los nulos de strings con `"Desconocido"` y de números con `0`.
2. Rellena nulos **por columna específica** con valores distintos:
   ```python
   df.na.fill({
       "nombre":        "Sin Nombre",
       "edad":           0,
       "departamento":  "Sin Asignar",
       "salario":        0.0,
   }).show()
   ```
3. Rellena nulos numéricos con el **promedio** de la columna usando `withColumn` + `avg`:
   ```python
   from pyspark.sql.functions import avg

   promedio_salario = df.select(avg("salario")).first()[0]
   df.withColumn("salario",
       when(col("salario").isNull(), promedio_salario)
       .otherwise(col("salario"))
   ).show()
   ```

---

## Reto 4 — Reemplazar Valores (`replace`)

1. Reemplaza el string `"Ingeniería"` por `"Engineering"` en la columna `departamento`.
2. Reemplaza el valor `0` por `None` en la columna `edad`.

```python
df.na.replace("Ingeniería", "Engineering", subset=["departamento"]).show()
```

---

## Reto 5 — `coalesce`: El Primer Valor No Nulo

La función `coalesce` devuelve el **primer valor no nulo** de una lista de columnas. Es ideal para combinar fuentes de datos.

```python
from pyspark.sql.functions import coalesce, lit

data_coalesce = [
    ("Ana",  None,    "Salario B"),
    ("Luis", "Sal A", None),
    (None,   None,    None),
]
df_c = spark.createDataFrame(data_coalesce, ["nombre", "fuente_a", "fuente_b"])

df_c.withColumn("fuente_final",
    coalesce(col("fuente_a"), col("fuente_b"), lit("Sin fuente"))
).show()
```

---

## Conceptos clave

| Función / Método          | Descripción                                                       |
|---------------------------|-------------------------------------------------------------------|
| `.isNull()`               | Filtra filas donde la columna es `null`                           |
| `.isNotNull()`            | Filtra filas donde la columna NO es `null`                        |
| `isnan(col)`              | Detecta valores `NaN` (flotantes inválidos)                       |
| `.na.drop(how, thresh)`   | Elimina filas con nulos según criterio                            |
| `.na.fill(value / dict)`  | Rellena nulos con un valor fijo o por columna                     |
| `.na.replace(old, new)`   | Reemplaza valores específicos                                     |
| `coalesce(*cols)`         | Devuelve el primer valor no nulo de una lista de columnas         |
| `when().otherwise()`      | Lógica condicional para tratar nulos en `withColumn`              |
