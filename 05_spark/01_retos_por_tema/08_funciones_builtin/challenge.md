# 🛠️ Funciones Built-in — `pyspark.sql.functions`

> **Objetivo:** Conocer y usar las funciones predefinidas de Spark para manipular strings, fechas, números y estructuras de datos complejas.

> ⚡ **Práctica con Big Data (Opcional):** Si quieres trabajar con datasets reales de **100,000 registros** en formato Parquet, puedes cargar los archivos pregenerados con:
> ```python
> df = spark.read.parquet("./datos/usuarios.parquet")
> df_fechas = spark.read.parquet("./datos/fechas.parquet")
> ```

---

## Importación

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("Funciones").getOrCreate()
```

> 💡 Importar con alias `F` es la convención estándar en la industria: `F.upper()`, `F.to_date()`, etc.

---

## Reto 1 — Funciones de String

```python
data = [
    ("  ana torres  ", "ana.torres@empresa.com", "Ingeniería-Senior"),
    ("LUIS GOMEZ",     "luis@empresa.com",       "Marketing-Junior"),
    ("carla RODrigo",  "CARLA@EMPRESA.COM",      "Ventas-Senior"),
]
df = spark.createDataFrame(data, ["nombre", "email", "cargo"])
```

Aplica las siguientes funciones en columnas nuevas:

| Función              | Columna nueva       | Operación                                         |
|----------------------|---------------------|---------------------------------------------------|
| `F.trim()`           | `nombre_limpio`     | Elimina espacios al inicio y al final             |
| `F.lower()`          | `nombre_lower`      | Convierte a minúsculas                            |
| `F.upper()`          | `email_upper`       | Convierte el email a mayúsculas                   |
| `F.initcap()`        | `nombre_titulo`     | Primera letra de cada palabra en mayúscula        |
| `F.length()`         | `len_email`         | Longitud del string                               |
| `F.split(col, "-")`  | `cargo_parts`       | Divide el string en array por el separador `"-"`  |
| `F.substring(col, 1, 4)` | `primeras_4`   | Extrae los primeros 4 caracteres                  |
| `F.concat_ws(sep, *cols)` | `nombre_email` | Concatena columnas con separador                  |

---

## Reto 2 — Funciones de Fecha y Tiempo

```python
from datetime import date

data_fechas = [
    ("Ana",   "2024-01-15", "2024-06-20"),
    ("Luis",  "2023-11-03", "2024-03-10"),
    ("Carla", "2024-02-28", "2024-07-01"),
]
df_fechas = spark.createDataFrame(data_fechas, ["nombre", "fecha_inicio", "fecha_fin"])
```

1. Convierte `fecha_inicio` y `fecha_fin` a tipo `DateType` con `F.to_date(col, formato)`.
2. Calcula los **días transcurridos** entre ambas fechas con `F.datediff()`.
3. Extrae el **año**, **mes** y **día** de `fecha_inicio` con `F.year()`, `F.month()`, `F.dayofmonth()`.
4. Calcula la **fecha de 30 días después** de `fecha_fin` con `F.date_add()`.
5. Obtiene la fecha y hora actuales con `F.current_date()` y `F.current_timestamp()`.

```python
df_fechas \
    .withColumn("fecha_inicio", F.to_date("fecha_inicio", "yyyy-MM-dd")) \
    .withColumn("fecha_fin",    F.to_date("fecha_fin",    "yyyy-MM-dd")) \
    .withColumn("dias_duracion", F.datediff("fecha_fin", "fecha_inicio")) \
    .withColumn("anio_inicio",   F.year("fecha_inicio")) \
    .withColumn("mes_inicio",    F.month("fecha_inicio")) \
    .withColumn("fecha_vence",   F.date_add("fecha_fin", 30)) \
    .show()
```

---

## Reto 3 — Funciones Matemáticas y Numéricas

```python
data_num = [(1, 144.75), (2, 3.14159), (3, -25.0), (4, 0.0), (5, 1000.555)]
df_num = spark.createDataFrame(data_num, ["id", "valor"])
```

Añade columnas aplicando:

| Función          | Descripción                                    |
|------------------|------------------------------------------------|
| `F.round(col, 2)`| Redondea a N decimales                         |
| `F.abs(col)`     | Valor absoluto                                 |
| `F.sqrt(col)`    | Raíz cuadrada                                  |
| `F.log(col)`     | Logaritmo natural                              |
| `F.pow(col, 2)`  | Potencia                                       |
| `F.ceil(col)`    | Redondea hacia arriba (techo)                  |
| `F.floor(col)`   | Redondea hacia abajo (piso)                    |

---

## Reto 4 — Funciones sobre Arrays

```python
data_arr = [
    ("Ana",   [90, 85, 92, 78]),
    ("Luis",  [70, 65, 80, 88]),
    ("Carla", [95, 91, 97, 89]),
]
df_arr = spark.createDataFrame(data_arr, ["nombre", "calificaciones"])
```

1. Calcula la **cantidad de elementos** del array con `F.size()`.
2. Obtén el **primer** elemento con `F.element_at(col, 1)`.
3. Verifica si `90` está en el array con `F.array_contains()`.
4. Ordena el array con `F.sort_array()`.
5. "Explota" el array en filas individuales con `F.explode()`:

```python
df_arr.select("nombre", F.explode("calificaciones").alias("calificacion")).show()
```

---

## Conceptos clave

| Categoría  | Funciones más usadas                                              |
|------------|-------------------------------------------------------------------|
| Strings    | `upper`, `lower`, `trim`, `split`, `concat_ws`, `regexp_replace` |
| Fechas     | `to_date`, `datediff`, `year`, `month`, `date_add`, `now`        |
| Números    | `round`, `abs`, `sqrt`, `ceil`, `floor`, `pow`                   |
| Arrays     | `size`, `explode`, `array_contains`, `sort_array`, `element_at`  |
| Nulos      | `isNull`, `isNotNull`, `coalesce`, `nvl`                         |
| Condicional| `when`, `otherwise`, `coalesce`                                  |
