# 🏆 Reto Final — Gran Integrador: Pipeline de Análisis de Ventas

> **Objetivo:** Construir un pipeline de datos completo usando todos los conceptos del módulo: SparkSession, DataFrames, transformaciones, agregaciones, joins, Spark SQL, funciones built-in, manejo de nulos y lectura/escritura.

---

## Contexto

Eres Data Engineer en una empresa de e-commerce. Te han entregado dos datasets crudos:

1. **`ventas`** — Transacciones de ventas (puede tener nulos y valores inválidos).
2. **`productos`** — Catálogo de productos con sus categorías y precios.

Tu misión es construir un pipeline que:
1. **Ingeste** los datos.
2. **Los limpie** (nulos, tipos incorrectos).
3. **Los transforme** (nuevas columnas, join).
4. **Los analice** con agregaciones y Spark SQL.
5. **Exporte** el resultado en Parquet.

---

## Paso 1 — Ingesta de Datos (SparkSession + DataFrames)

> ⚡ **Práctica con Big Data (Opcional):** Si quieres construir tu pipeline utilizando los datasets a gran escala (**100,000 transacciones** y catálogo completo de productos) que hemos preparado en formato Parquet, puedes cargarlos con:
> ```python
> df_ventas = spark.read.parquet("./datos/ventas.parquet")
> df_productos = spark.read.parquet("./datos/productos.parquet")
> ```
> *(Si usas esta opción, puedes saltarte la creación de DataFrames manuales a partir de listas).*

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("Pipeline-Ventas") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Dataset de ventas (con nulos y problemas intencionales)
ventas_data = [
    (1,  101, "2024-01-15", 3,   None),
    (2,  102, "2024-01-20", 1,   "completado"),
    (3,  None,"2024-02-05", 5,   "completado"),
    (4,  103, "2024-02-18", 2,   "cancelado"),
    (5,  101, "2024-03-01", 4,   "completado"),
    (6,  104, "2024-03-10", 1,   "completado"),
    (7,  102, None,         3,   "pendiente"),
    (8,  103, "2024-04-05", None,"completado"),
]
ventas_schema = StructType([
    StructField("venta_id",     IntegerType(), True),
    StructField("producto_id",  IntegerType(), True),
    StructField("fecha",        StringType(),  True),
    StructField("cantidad",     IntegerType(), True),
    StructField("estado",       StringType(),  True),
])
df_ventas = spark.createDataFrame(ventas_data, ventas_schema)

# Catálogo de productos (limpio)
productos_data = [
    (101, "Laptop",   "Electrónica",  1200.0),
    (102, "Mouse",    "Periféricos",    45.0),
    (103, "Teclado",  "Periféricos",    80.0),
    (104, "Monitor",  "Electrónica",   350.0),
]
productos_schema = ["producto_id", "nombre", "categoria", "precio_unitario"]
df_productos = spark.createDataFrame(productos_data, productos_schema)
```

---

## Paso 2 — Exploración y Diagnóstico de Calidad

1. Imprime el esquema y las primeras filas de ambos DataFrames.
2. Genera un **reporte de nulos** por columna para `df_ventas`:
   ```python
   df_ventas.select([
       F.count(F.when(F.col(c).isNull(), c)).alias(c)
       for c in df_ventas.columns
   ]).show()
   ```
3. Cuenta cuántas ventas hay por `estado`.

---

## Paso 3 — Limpieza de Datos

1. **Elimina** las filas donde `producto_id` sea nulo (no podemos enriquecer sin ID).
2. **Elimina** las filas donde `fecha` sea nula.
3. **Rellena** los nulos de `cantidad` con `1` (asumimos al menos una unidad).
4. **Rellena** los nulos de `estado` con `"desconocido"`.
5. **Convierte** `fecha` de string a `DateType`.
6. **Filtra** solo las ventas con `estado` = `"completado"` para el análisis final.

```python
df_ventas_limpio = df_ventas \
    .na.drop(subset=["producto_id", "fecha"]) \
    .na.fill({"cantidad": 1, "estado": "desconocido"}) \
    .withColumn("fecha", F.to_date("fecha", "yyyy-MM-dd")) \
    .filter(F.col("estado") == "completado")
```

---

## Paso 4 — Transformaciones y Enriquecimiento

1. **Join** con el catálogo de productos (inner join por `producto_id`).
2. Añade la columna `ingreso_total` = `cantidad * precio_unitario`.
3. Añade la columna `mes` extraída de la fecha.
4. Añade la columna `trimestre` usando `when`:
   - Meses 1-3 → `"Q1"`, 4-6 → `"Q2"`, 7-9 → `"Q3"`, 10-12 → `"Q4"`.

```python
df_enriquecido = df_ventas_limpio \
    .join(df_productos, on="producto_id", how="inner") \
    .withColumn("ingreso_total", F.col("cantidad") * F.col("precio_unitario")) \
    .withColumn("mes", F.month("fecha")) \
    .withColumn("trimestre",
        F.when(F.col("mes").between(1, 3),  "Q1")
         .when(F.col("mes").between(4, 6),  "Q2")
         .when(F.col("mes").between(7, 9),  "Q3")
         .otherwise("Q4")
    )

df_enriquecido.show()
```

---

## Paso 5 — Análisis con Agregaciones y Spark SQL

### Con la API de DataFrames:

```python
# Ingresos totales por categoría
df_enriquecido.groupBy("categoria") \
    .agg(
        F.sum("ingreso_total").alias("ingreso_categoria"),
        F.count("venta_id").alias("num_ventas"),
        F.round(F.avg("ingreso_total"), 2).alias("ticket_promedio"),
    ) \
    .orderBy(F.col("ingreso_categoria").desc()) \
    .show()
```

### Con Spark SQL:

```python
df_enriquecido.createOrReplaceTempView("ventas_enriquecidas")

spark.sql("""
    SELECT
        trimestre,
        categoria,
        SUM(ingreso_total) AS ingreso_total,
        COUNT(*) AS num_transacciones
    FROM ventas_enriquecidas
    GROUP BY trimestre, categoria
    ORDER BY trimestre, ingreso_total DESC
""").show()
```

---

## Paso 6 — Exportar Resultados en Parquet

```python
# Exportar el DataFrame enriquecido completo
df_enriquecido.write \
    .mode("overwrite") \
    .partitionBy("categoria") \
    .parquet("./output/pipeline_ventas")

print("✅ Pipeline completado. Datos exportados en Parquet.")
spark.stop()
```

---

## Checklist del pipeline ✅

| Paso | Concepto aplicado                  | ¿Completado? |
|------|------------------------------------|-------------|
| 1    | SparkSession + DataFrames          | ⬜           |
| 2    | `.printSchema()`, reporte de nulos | ⬜           |
| 3    | `dropna`, `fillna`, `to_date`      | ⬜           |
| 4    | Join, `withColumn`, `when`         | ⬜           |
| 5    | `groupBy`, `agg`, Spark SQL        | ⬜           |
| 6    | Escritura Parquet + `partitionBy`  | ⬜           |

---

## ¿Qué sigue?

Una vez que domines este pipeline, los siguientes pasos naturales son:

- **Delta Lake** — Control de versiones y transacciones ACID sobre Parquet.
- **Spark Streaming** — Procesar datos en tiempo real.
- **AWS Glue / EMR** — Ejecutar tus pipelines en la nube.
- **Airflow** — Orquestar y programar tus pipelines.
