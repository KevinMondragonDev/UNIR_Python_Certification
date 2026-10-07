# Fase 5 — DataFrames: Avanzado

## groupBy y Agregaciones

```python
from pyspark.sql import functions as F

# groupBy + función de agregación
df.groupBy("department").agg(
    F.count("*").alias("total"),
    F.avg("salary").alias("avg_salary"),
    F.max("salary").alias("max_salary"),
    F.min("salary").alias("min_salary"),
    F.sum("salary").alias("sum_salary"),
    F.stddev("salary").alias("std_salary"),
    F.collect_list("name").alias("nombres"),
    F.countDistinct("country").alias("paises_unicos")
).orderBy(F.col("avg_salary").desc())
```

---

## Joins en DataFrames

```python
# INNER (default)
df1.join(df2, on="id", how="inner")
df1.join(df2, df1.id == df2.emp_id, "inner")

# LEFT OUTER
df1.join(df2, on="id", how="left")

# RIGHT OUTER
df1.join(df2, on="id", how="right")

# FULL OUTER
df1.join(df2, on="id", how="full")

# SEMI — filas de df1 que tienen match en df2 (sin traer cols de df2)
df1.join(df2, on="id", how="semi")

# ANTI — filas de df1 que NO tienen match en df2
df1.join(df2, on="id", how="anti")

# CROSS — producto cartesiano (cuidado: N x M filas)
df1.crossJoin(df2)
```

---

## Window Functions (Funciones de Ventana)

Las funciones de ventana calculan valores **sobre un grupo de filas relacionadas**
sin colapsar el DataFrame (a diferencia de groupBy).

```python
from pyspark.sql.window import Window

# Definir ventana
ventana = Window.partitionBy("department").orderBy(F.col("salary").desc())

# Ranking dentro de cada departamento
df.withColumn("rank",        F.rank().over(ventana))
  .withColumn("dense_rank",  F.dense_rank().over(ventana))
  .withColumn("row_number",  F.row_number().over(ventana))
  .withColumn("percent_rank",F.percent_rank().over(ventana))
```

### rank vs dense_rank vs row_number

| Salario | rank | dense_rank | row_number |
|---------|------|------------|------------|
| 100     | 1    | 1          | 1          |
| 90      | 2    | 2          | 2          |
| 90      | 2    | 2          | 3 ← único  |
| 80      | 4 ← salta | 3    | 4          |

### Funciones de desplazamiento (lag/lead)

```python
ventana_lag = Window.partitionBy("region").orderBy("sale_date")

df.withColumn("venta_anterior", F.lag("amount", 1).over(ventana_lag))
  .withColumn("venta_siguiente", F.lead("amount", 1).over(ventana_lag))
```

### Agregaciones acumulativas

```python
ventana_acum = Window.partitionBy("region") \
    .orderBy("sale_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn("suma_acumulada", F.sum("amount").over(ventana_acum))
```

---

## Manejo de Nulos

```python
# Detectar
df.filter(F.col("salary").isNull())
df.filter(F.col("salary").isNotNull())

# Eliminar filas con nulos
df.dropna()                   # si hay ALGÚN nulo en cualquier columna
df.dropna(how="all")          # solo si TODAS las columnas son nulas
df.dropna(subset=["salary"])  # solo si 'salary' es nulo

# Rellenar nulos
df.fillna(0)                        # rellenar numéricos con 0
df.fillna("DESCONOCIDO")            # rellenar strings
df.fillna({"salary": 0, "name": "N/A"})  # por columna

# coalesce — toma el primer no-nulo
df.withColumn("sal_limpio", F.coalesce(F.col("salary"), F.lit(0)))

# replace — reemplazar valores específicos
df.replace("Old Value", "New Value", subset=["column"])
```

---

## Pivot y Unpivot

```python
# PIVOT — de filas a columnas
df.groupBy("year").pivot("region", ["Norte","Sur","Este"]).sum("amount")

# UNPIVOT (stack) — de columnas a filas
from pyspark.sql.functions import stack
df.select(
    "id",
    F.stack(2,
        F.lit("Norte"), F.col("ventas_norte"),
        F.lit("Sur"),   F.col("ventas_sur")
    ).alias("region", "ventas")
)
```

---

## Archivos de esta fase

| Archivo | Descripción |
|---------|-------------|
| `01_groupby_agg.py` | groupBy y todas las funciones de agregación |
| `02_joins.py` | Todos los tipos de join |
| `03_window_functions.py` | rank, lag, lead, acumulativos |
| `04_nulos.py` | dropna, fillna, coalesce |
| `05_pivot.py` | pivot y unpivot |
| `ejercicios.py` | 12 ejercicios guiados |
| `reto.py` | Reporte de ventas con rankings y nulos |
| `solucion_reto.py` | Solución |
| `validar.py` | Autoevaluación |

---

## Conceptos que debes dominar al terminar

- [ ] groupBy con múltiples funciones de agregación en un solo agg()
- [ ] Los 6 tipos de join y cuándo usar cada uno
- [ ] Diferencia entre rank, dense_rank y row_number
- [ ] Usar Window para calcular ranking dentro de grupos
- [ ] lag/lead para comparar con el período anterior/siguiente
- [ ] Estrategias para manejar nulos (dropna vs fillna vs coalesce)
- [ ] Crear una tabla pivot desde datos long-format
