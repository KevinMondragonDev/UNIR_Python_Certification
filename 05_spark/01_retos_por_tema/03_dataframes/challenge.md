# 🗃️ DataFrames — La Estructura Estrella de Spark

> **Objetivo:** Crear DataFrames de distintas formas, explorar su esquema, y comprender cómo Spark organiza los datos en columnas tipadas.

> ⚡ **Práctica con Big Data (Opcional):** Si quieres trabajar con un dataset real de **100,000 registros** en formato Parquet, puedes cargar el archivo pregenerado con:
> ```python
> df = spark.read.parquet("./datos/empleados.parquet")
> ```

---

## ¿Qué es un DataFrame de Spark?

Un **DataFrame** es una colección distribuida de datos organizada en **columnas nombradas y tipadas**, similar a una tabla SQL o un DataFrame de pandas — pero distribuido y optimizado para billones de filas.

```
DataFrame de Spark
├── Columna: nombre  (StringType)
├── Columna: edad    (IntegerType)
├── Columna: salario (DoubleType)
└── ... particionado en N nodos del cluster
```

---

## Reto 1 — Crear un DataFrame desde una lista

1. Crea una lista de tuplas llamada `empleados_data`:
   ```python
   empleados_data = [
       ("Ana",    32, "Ingeniería",  75000.0),
       ("Luis",   28, "Marketing",   55000.0),
       ("Carla",  35, "Ingeniería",  90000.0),
       ("Pedro",  41, "Ventas",      60000.0),
       ("Sofía",  29, "Marketing",   58000.0),
   ]
   ```
2. Define el esquema como una lista de strings: `["nombre", "edad", "departamento", "salario"]`.
3. Crea el DataFrame con `spark.createDataFrame(data, schema)`.
4. Muestra los datos con `.show()`.
5. Muestra el esquema con `.printSchema()`.

---

## Reto 2 — Crear un DataFrame con esquema explícito (StructType)

> Definir el esquema explícitamente es la práctica recomendada en producción porque evita que Spark infiera tipos incorrectamente.

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("nombre",       StringType(),  nullable=False),
    StructField("edad",         IntegerType(), nullable=True),
    StructField("departamento", StringType(),  nullable=True),
    StructField("salario",      DoubleType(),  nullable=True),
])
```

1. Usa el mismo `empleados_data` del Reto 1, pero ahora pasa el `schema` de tipo `StructType`.
2. Verifica que el esquema se aplicó correctamente con `.printSchema()`.
3. Compara la diferencia de esquemas entre ambos métodos (con lista de strings vs `StructType`).

---

## Reto 3 — Crear un DataFrame desde un diccionario (via pandas)

1. Importa `pandas as pd` y crea un DataFrame de pandas:
   ```python
   import pandas as pd

   data_dict = {
       "producto": ["Laptop", "Mouse", "Teclado", "Monitor"],
       "precio":   [1200,     45,      80,         350],
       "stock":    [10,       150,     75,          20],
   }
   df_pandas = pd.DataFrame(data_dict)
   ```
2. Convierte el DataFrame de pandas a Spark con `spark.createDataFrame(df_pandas)`.
3. Usa `.show()` para imprimir la tabla.
4. Usa `.dtypes` para ver los tipos de columnas.

---

## Reto 4 — Inspeccionar un DataFrame

Dado cualquiera de los DataFrames anteriores, practica los siguientes comandos de exploración:

| Comando                 | ¿Qué hace?                                              |
|-------------------------|---------------------------------------------------------|
| `.show(n)`              | Muestra las primeras `n` filas (por defecto 20)         |
| `.show(truncate=False)` | Muestra filas sin recortar el contenido                 |
| `.printSchema()`        | Muestra los tipos y la anidación de columnas            |
| `.columns`              | Lista de nombres de columnas                            |
| `.dtypes`               | Lista de tuplas `(nombre, tipo)`                        |
| `.count()`              | Número total de filas                                   |
| `.describe()`           | Estadísticas básicas (count, mean, std, min, max)       |

Practica cada uno e imprime su resultado.

---

## Conceptos clave

| Concepto          | Descripción                                                              |
|-------------------|--------------------------------------------------------------------------|
| `createDataFrame`  | Crea un DataFrame desde datos locales, pandas o RDDs                    |
| `StructType`       | Define el esquema completo del DataFrame                                |
| `StructField`      | Define una columna: nombre, tipo y si acepta nulos                      |
| `StringType`       | Tipo de dato para texto                                                 |
| `IntegerType`      | Tipo de dato para enteros                                               |
| `DoubleType`       | Tipo de dato para números decimales                                     |
| `nullable`         | Si `True`, la columna puede contener valores `null`                     |
