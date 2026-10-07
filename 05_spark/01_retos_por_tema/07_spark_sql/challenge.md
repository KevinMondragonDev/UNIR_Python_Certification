# 🗄️ Spark SQL — Consultar DataFrames con SQL puro

> **Objetivo:** Usar la sintaxis SQL estándar directamente sobre DataFrames de Spark mediante vistas temporales.

> ⚡ **Práctica con Big Data (Opcional):** Si quieres trabajar con un dataset real de **100,000 registros** en formato Parquet, puedes cargar el archivo pregenerado con:
> ```python
> df = spark.read.parquet("./datos/empleados.parquet")
> ```

---

## ¿Por qué Spark SQL?

Spark SQL permite escribir consultas SQL clásicas sobre DataFrames distribuidos. Es especialmente útil para:
- Equipos con experiencia en SQL que transicionan a Spark.
- Consultas complejas que son más legibles en SQL que en la API de DataFrames.
- Combinar ambas APIs en el mismo pipeline.

```python
# Ambas formas son equivalentes y producen el mismo plan de ejecución:
df.filter(col("salario") > 60000).select("nombre", "salario")

spark.sql("SELECT nombre, salario FROM empleados WHERE salario > 60000")
```

---

## Setup inicial

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

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

# Registrar como vista temporal para usar con SQL
df.createOrReplaceTempView("empleados")
```

---

## Reto 1 — Consultas Básicas

Escribe consultas SQL equivalentes a estas operaciones comunes:

1. Selecciona todas las columnas de `empleados`.
2. Selecciona `nombre` y `salario` de empleados donde `salario > 60000`.
3. Selecciona empleados del departamento `"Ingeniería"` ordenados por `salario` descendente.
4. Limita los resultados a los 3 empleados mejor pagados.

```python
spark.sql("SELECT * FROM empleados").show()

spark.sql("""
    SELECT nombre, salario
    FROM empleados
    WHERE salario > 60000
    ORDER BY salario DESC
    LIMIT 3
""").show()
```

---

## Reto 2 — Agregaciones en SQL

1. Calcula el número de empleados y el salario promedio por `departamento`.
2. Filtra solo los departamentos con más de 1 empleado usando `HAVING`.
3. Añade una columna calculada `salario_mensual` dentro del `SELECT`.

```python
spark.sql("""
    SELECT
        departamento,
        COUNT(*) AS num_empleados,
        ROUND(AVG(salario), 2) AS salario_promedio,
        MAX(salario) AS salario_max
    FROM empleados
    GROUP BY departamento
    HAVING COUNT(*) > 1
    ORDER BY salario_promedio DESC
""").show()
```

---

## Reto 3 — Subconsultas y CASE WHEN

1. Escribe una consulta que clasifique el salario con `CASE WHEN`:
   - `>= 80000` → `"Alto"`
   - `>= 60000` → `"Medio"`
   - En caso contrario → `"Bajo"`

2. Usa una subconsulta para encontrar los empleados que ganan **más que el promedio de su departamento**:

```python
spark.sql("""
    SELECT nombre, departamento, salario,
        CASE
            WHEN salario >= 80000 THEN 'Alto'
            WHEN salario >= 60000 THEN 'Medio'
            ELSE 'Bajo'
        END AS categoria
    FROM empleados
""").show()

spark.sql("""
    SELECT e.nombre, e.departamento, e.salario
    FROM empleados e
    JOIN (
        SELECT departamento, AVG(salario) AS prom
        FROM empleados
        GROUP BY departamento
    ) p ON e.departamento = p.departamento
    WHERE e.salario > p.prom
""").show()
```

---

## Reto 4 — Vistas Globales vs Vistas Temporales

| Tipo                      | Método                              | Alcance                                   |
|---------------------------|-------------------------------------|-------------------------------------------|
| Vista temporal            | `.createOrReplaceTempView("nombre")`| Solo visible en la sesión actual          |
| Vista global              | `.createGlobalTempView("nombre")`   | Visible entre sesiones distintas          |

1. Crea una vista global del DataFrame con `createGlobalTempView("empleados_global")`.
2. Consúltala usando el prefijo especial `global_temp`:

```python
df.createGlobalTempView("empleados_global")
spark.sql("SELECT * FROM global_temp.empleados_global").show()
```

---

## Reto 5 — Mezclar SQL y API de DataFrames

Ejecuta una consulta SQL y luego aplica transformaciones de la API de DataFrames sobre el resultado:

```python
resultado_sql = spark.sql("""
    SELECT departamento, AVG(salario) AS promedio
    FROM empleados
    GROUP BY departamento
""")

# Continúa con la API de DataFrames
from pyspark.sql.functions import round, col

resultado_sql \
    .withColumn("promedio", round(col("promedio"), 0)) \
    .filter(col("promedio") > 60000) \
    .show()
```

---

## Conceptos clave

| Concepto                      | Descripción                                                         |
|-------------------------------|---------------------------------------------------------------------|
| `createOrReplaceTempView`     | Registra el DF como tabla temporal consultable con SQL              |
| `spark.sql("...")`            | Ejecuta una cadena SQL y devuelve un DataFrame                      |
| `HAVING`                      | Filtra grupos después del `GROUP BY` (no puede usarse con `WHERE`)  |
| `CASE WHEN ... END`           | Lógica condicional dentro de una consulta SQL                       |
| `global_temp`                 | Namespace de vistas globales accesibles entre sesiones              |
