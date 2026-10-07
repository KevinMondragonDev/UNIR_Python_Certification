# Fase 4 — DataFrames: Introducción

## ¿Por qué DataFrames sobre RDDs?

| Aspecto | RDD | DataFrame |
|---------|-----|-----------|
| Abstracción | Bajo nivel | Alto nivel |
| Schema | Sin schema (cualquier tipo Python) | Con schema tipado |
| Optimización | Manual | Automática (Catalyst Optimizer) |
| Performance | Depende del código | Optimizado automáticamente |
| API | Funcional (lambda) | Declarativa (columnas) |
| SQL | No soporta | Soporta totalmente |
| Uso recomendado | Datos no estructurados | Datos estructurados |

---

## Crear DataFrames

### Desde listas Python
```python
data = [(1, "Ana", 50000), (2, "Luis", 35000)]
df = spark.createDataFrame(data, ["id", "nombre", "salario"])
```

### Con Schema explícito
```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema = StructType([
    StructField("id",      IntegerType(), nullable=False),
    StructField("nombre",  StringType(),  nullable=True),
    StructField("salario", DoubleType(),  nullable=True),
])
df = spark.createDataFrame(data, schema)
```

### Desde archivos
```python
# CSV
df = spark.read.csv("ruta.csv", header=True, inferSchema=True)
df = spark.read.option("header", True).option("inferSchema", True).csv("ruta.csv")

# Parquet (preserva schema automáticamente)
df = spark.read.parquet("ruta.parquet")

# JSON
df = spark.read.json("ruta.json")
```

### Desde RDD
```python
rdd = sc.parallelize([(1, "Ana"), (2, "Luis")])
df = rdd.toDF(["id", "nombre"])
df = spark.createDataFrame(rdd, schema)
```

---

## Inspección del DataFrame

```python
df.printSchema()      # Muestra el schema con tipos y nullable
df.show()             # Primeras 20 filas en tabla
df.show(5)            # Solo 5 filas
df.show(truncate=False) # Sin truncar strings
df.describe().show()  # Estadísticas descriptivas (count, mean, std, min, max)
df.summary().show()   # Como describe + percentiles
df.dtypes             # Lista de (nombre, tipo) como Python list
df.columns            # Lista de nombres de columnas
df.count()            # Número de filas
len(df.columns)       # Número de columnas
df.schema             # Schema completo como objeto StructType
```

---

## Selección de Columnas

```python
from pyspark.sql import functions as F

# select con nombres
df.select("nombre", "salario")

# select con col()
df.select(F.col("nombre"), F.col("salario"))

# select con expr() — permite SQL inline
df.select(F.expr("salario * 1.10 as salario_con_aumento"))

# Seleccionar todas
df.select("*")

# Seleccionar todas menos una
df.select([c for c in df.columns if c != "id"])
```

---

## Filtros

```python
# filter() y where() son equivalentes
df.filter(F.col("salario") > 50000)
df.where(F.col("salario") > 50000)

# SQL-style string
df.filter("salario > 50000")

# Múltiples condiciones
df.filter((F.col("salario") > 50000) & (F.col("department") == "Engineering"))
df.filter((F.col("salario") < 30000) | (F.col("country") == "Mexico"))

# Nulos
df.filter(F.col("salario").isNull())
df.filter(F.col("salario").isNotNull())

# isin
df.filter(F.col("country").isin("Mexico", "Colombia", "Argentina"))
```

---

## Manipulación de Columnas

```python
# withColumn — agregar o reemplazar columna
df.withColumn("salario_anual", F.col("salario") * 12)
df.withColumn("salario", F.col("salario").cast("double"))  # reemplaza

# withColumnRenamed — renombrar
df.withColumnRenamed("nombre", "name")

# drop — eliminar columna
df.drop("columna_innecesaria")
df.drop("col1", "col2")  # múltiples

# alias — renombrar en select
df.select(F.col("nombre").alias("name"), F.col("salario").alias("salary"))
```

---

## Tipos de Datos Principales

| PySpark | Python | SQL |
|---------|--------|-----|
| `IntegerType()` | int | INT |
| `LongType()` | int | BIGINT |
| `DoubleType()` | float | DOUBLE |
| `StringType()` | str | STRING |
| `BooleanType()` | bool | BOOLEAN |
| `DateType()` | date | DATE |
| `TimestampType()` | datetime | TIMESTAMP |
| `ArrayType(T)` | list | ARRAY |
| `MapType(K,V)` | dict | MAP |

---

## Archivos de esta fase

| Archivo | Descripción |
|---------|-------------|
| `01_crear_dataframes.py` | Todas las formas de crear DataFrames |
| `02_schema_y_tipos.py` | Schema, tipos y casting |
| `03_select_filter.py` | Selección y filtrado |
| `04_columnas.py` | withColumn, withColumnRenamed, drop |
| `ejercicios.py` | 10 ejercicios guiados |
| `reto.py` | Limpieza y exploración de employees |
| `solucion_reto.py` | Solución |
| `validar.py` | Autoevaluación |

---

## Conceptos que debes dominar al terminar

- [ ] Crear DataFrames desde listas, CSV y Parquet
- [ ] Definir schema explícito con StructType/StructField
- [ ] Diferencia entre `select()`, `col()` y `expr()`
- [ ] Filtrar con condiciones simples y múltiples
- [ ] Agregar, renombrar y eliminar columnas
- [ ] Inspeccionar schema y estadísticas básicas
