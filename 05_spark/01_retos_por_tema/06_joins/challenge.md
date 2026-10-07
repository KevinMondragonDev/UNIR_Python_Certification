# 🔗 Joins — Combinar DataFrames

> **Objetivo:** Aprender a unir dos DataFrames usando diferentes tipos de join, igual que en SQL.

> ⚡ **Práctica con Big Data (Opcional):** Si quieres trabajar con datasets reales de **100,000 registros** en formato Parquet, puedes cargar los archivos pregenerados con:
> ```python
> df_empleados = spark.read.parquet("./datos/empleados.parquet")
> df_departamentos = spark.read.parquet("./datos/departamentos.parquet")
> ```

---

## Datasets de práctica

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Joins").getOrCreate()

# DataFrame de empleados
empleados_data = [
    (1, "Ana",    101),
    (2, "Luis",   102),
    (3, "Carla",  101),
    (4, "Pedro",  103),
    (5, "Sofía",  999),  # ← dept_id que NO existe en departamentos
]
df_empleados = spark.createDataFrame(empleados_data, ["id", "nombre", "dept_id"])

# DataFrame de departamentos
departamentos_data = [
    (101, "Ingeniería",  "CDMX"),
    (102, "Marketing",   "Bogotá"),
    (103, "Ventas",      "Buenos Aires"),
    (104, "RRHH",        "Lima"),  # ← departamento sin empleados
]
df_departamentos = spark.createDataFrame(departamentos_data, ["dept_id", "nombre_dept", "ciudad"])
```

---

## Reto 1 — INNER JOIN

El `inner join` devuelve **solo las filas que tienen coincidencia en ambos DataFrames**.

1. Une `df_empleados` con `df_departamentos` usando `dept_id` como clave.
2. Muestra el resultado con `nombre`, `nombre_dept` y `ciudad`.
3. ¿Cuántas filas devuelve? ¿Por qué Sofía y RRHH no aparecen?

```python
df_empleados.join(df_departamentos, on="dept_id", how="inner") \
    .select("nombre", "nombre_dept", "ciudad") \
    .show()
```

---

## Reto 2 — LEFT JOIN

El `left join` devuelve **todas las filas del DataFrame izquierdo**, y `null` donde no hay coincidencia en el derecho.

1. Realiza un left join entre `df_empleados` y `df_departamentos`.
2. Muestra todas las columnas.
3. ¿Por qué Sofía aparece pero con valores `null` en `nombre_dept` y `ciudad`?

```python
df_empleados.join(df_departamentos, on="dept_id", how="left").show()
```

---

## Reto 3 — RIGHT JOIN y FULL OUTER JOIN

1. **Right join:** Realiza un right join para que todos los departamentos aparezcan, incluso `RRHH` que no tiene empleados.
2. **Full outer join:** Realiza un full outer join. ¿Qué combinación de filas ves? Verifica que tanto Sofía como RRHH aparecen.

```python
# Right join
df_empleados.join(df_departamentos, on="dept_id", how="right").show()

# Full outer join
df_empleados.join(df_departamentos, on="dept_id", how="full").show()
```

---

## Reto 4 — JOIN con Múltiples Condiciones y Columnas Ambiguas

Cuando ambos DataFrames tienen columnas con el **mismo nombre** (como `nombre`), debes especificar de cuál DataFrame tomas la columna.

1. Realiza un inner join entre `df_empleados` y `df_departamentos` sobre `dept_id`.
2. Selecciona `df_empleados["nombre"]` y `df_departamentos["nombre_dept"]` explícitamente para evitar ambigüedad.
3. Añade una condición extra: filtra solo los empleados con `id > 1`.

```python
df_empleados.join(df_departamentos, on="dept_id", how="inner") \
    .filter(col("id") > 1) \
    .select(
        df_empleados["nombre"].alias("empleado"),
        df_departamentos["nombre_dept"].alias("departamento"),
        col("ciudad"),
    ) \
    .show()
```

---

## Reto 5 — SELF JOIN (Join de un DataFrame consigo mismo)

> Útil para comparar filas dentro del mismo conjunto de datos, como encontrar empleados del mismo departamento.

```python
df_a = df_empleados.alias("a")
df_b = df_empleados.alias("b")

df_a.join(df_b, on="dept_id", how="inner") \
    .filter(col("a.id") != col("b.id")) \
    .select(
        col("a.nombre").alias("empleado_1"),
        col("b.nombre").alias("empleado_2"),
        col("a.dept_id"),
    ) \
    .show()
```

---

## Resumen de tipos de Join

| Tipo         | Clave `how=`   | Descripción                                                        |
|--------------|----------------|--------------------------------------------------------------------|
| Inner Join   | `"inner"`      | Solo filas con coincidencia en **ambos** DataFrames                |
| Left Join    | `"left"`       | Todas las filas del **izquierdo**, `null` si no hay coincidencia   |
| Right Join   | `"right"`      | Todas las filas del **derecho**, `null` si no hay coincidencia     |
| Full Outer   | `"full"`       | Todas las filas de **ambos**, `null` donde no hay coincidencia     |
| Left Anti    | `"left_anti"`  | Solo filas del izquierdo que **NO** tienen coincidencia            |
| Left Semi    | `"left_semi"`  | Solo filas del izquierdo que **SÍ** tienen coincidencia (sin cols del derecho) |

---

## Conceptos clave

| Concepto              | Descripción                                                     |
|-----------------------|-----------------------------------------------------------------|
| `.join(df, on, how)`  | Une dos DataFrames por la columna `on` con el tipo `how`        |
| Columna ambigua       | Cuando ambos DFs tienen la misma columna; usa `df["col"]`       |
| `.alias("nombre")`    | Da un alias temporal a un DataFrame para resolver ambigüedades  |
| `left_anti`           | Equivalente al `NOT IN` de SQL                                  |
