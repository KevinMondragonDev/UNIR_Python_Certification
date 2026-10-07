# 💾 Lectura y Escritura — CSV, JSON y Parquet

> **Objetivo:** Leer y escribir datos en los formatos más comunes del ecosistema de Data Engineering, entendiendo las opciones de configuración de cada uno.

---

## ¿Por qué importa el formato de archivo?

| Formato   | Legible por humanos | Compresión | Velocidad en Spark | Uso típico                        |
|-----------|---------------------|------------|---------------------|-----------------------------------|
| **CSV**   | ✅ Sí               | ❌ Poca    | 🐢 Lento            | Intercambio simple, exportaciones |
| **JSON**  | ✅ Sí               | ❌ Poca    | 🐢 Lento            | APIs, datos semi-estructurados    |
| **Parquet**| ❌ No              | ✅ Alta    | ⚡ Muy rápido        | Data Lakes, producción            |

> 💡 En producción, **Parquet** es el estándar. CSV y JSON son formatos de entrada/salida.

---

## Setup inicial

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName("LecturaEscritura").getOrCreate()

# Datos de prueba
data = [
    ("Ana",   32, "Ingeniería",  75000.0),
    ("Luis",  28, "Marketing",   55000.0),
    ("Carla", 35, "Ingeniería",  90000.0),
    ("Pedro", 41, "Ventas",      60000.0),
]
schema = ["nombre", "edad", "departamento", "salario"]
df = spark.createDataFrame(data, schema)
```

---

## Reto 1 — Escribir y Leer CSV

### Escritura

```python
df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("sep", ",") \
    .csv("./output/empleados_csv")
```

### Lectura

```python
df_csv = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", ",") \
    .csv("./output/empleados_csv")

df_csv.printSchema()
df_csv.show()
```

> ⚠️ Con `inferSchema=True`, Spark lee el archivo dos veces (una para inferir, otra para leer). En producción, siempre define el esquema explícitamente.

### Con esquema explícito

```python
schema_csv = StructType([
    StructField("nombre",       StringType(),  True),
    StructField("edad",         IntegerType(), True),
    StructField("departamento", StringType(),  True),
    StructField("salario",      DoubleType(),  True),
])

df_schema = spark.read \
    .schema(schema_csv) \
    .option("header", "true") \
    .csv("./output/empleados_csv")
```

---

## Reto 2 — Escribir y Leer JSON

### Escritura

```python
df.write \
    .mode("overwrite") \
    .json("./output/empleados_json")
```

### Lectura

```python
df_json = spark.read \
    .option("multiLine", "false") \
    .json("./output/empleados_json")

df_json.printSchema()
df_json.show()
```

> 💡 Por defecto, Spark espera **JSON Lines** (un objeto JSON por línea). Si tu archivo tiene JSON multilínea, usa `.option("multiLine", "true")`.

---

## Reto 3 — Escribir y Leer Parquet

### Escritura

```python
df.write \
    .mode("overwrite") \
    .parquet("./output/empleados_parquet")
```

### Lectura

```python
df_parquet = spark.read.parquet("./output/empleados_parquet")
df_parquet.printSchema()
df_parquet.show()
```

> ✅ Parquet preserva el esquema automáticamente — no necesitas `inferSchema` ni definirlo manualmente.

---

## Reto 4 — Modos de Escritura

Experimenta con los 4 modos al escribir un archivo:

| Modo          | Descripción                                                          |
|---------------|----------------------------------------------------------------------|
| `"overwrite"` | Borra el destino y escribe de nuevo                                  |
| `"append"`    | Añade los datos al destino existente                                 |
| `"ignore"`    | Si el destino existe, no hace nada y no lanza error                  |
| `"error"`     | (Por defecto) Lanza error si el destino ya existe                    |

```python
# Prueba cada modo
df.write.mode("overwrite").parquet("./output/test")
df.write.mode("append").parquet("./output/test")
df.write.mode("ignore").parquet("./output/test")
```

---

## Reto 5 — Particionado al Escribir

Particionar los datos al escribir permite leer solo las particiones necesarias después, lo que acelera enormemente las consultas filtradas.

```python
df.write \
    .mode("overwrite") \
    .partitionBy("departamento") \
    .parquet("./output/empleados_particionado")
```

Tras escribir, observa la estructura de carpetas creada:
```
empleados_particionado/
├── departamento=Ingeniería/
│   └── part-00000-...parquet
├── departamento=Marketing/
│   └── part-00000-...parquet
└── departamento=Ventas/
    └── part-00000-...parquet
```

Lee solo un departamento específico:
```python
spark.read.parquet("./output/empleados_particionado/departamento=Ingeniería").show()
```

---

## Conceptos clave

| Concepto               | Descripción                                                         |
|------------------------|---------------------------------------------------------------------|
| `spark.read`           | API de lectura de datos                                             |
| `df.write`             | API de escritura de datos                                           |
| `.mode("overwrite")`   | Controla qué hace Spark si el destino ya existe                     |
| `inferSchema`          | Spark deduce los tipos; costoso en producción                       |
| `.schema(struct)`      | Especifica el esquema explícitamente para mayor rendimiento         |
| `.partitionBy(col)`    | Divide los archivos de salida en carpetas por columna               |
| `header=true`          | Usa la primera fila del CSV como nombres de columnas                |
| Parquet                | Formato columnar binario, ideal para Data Lakes                     |
