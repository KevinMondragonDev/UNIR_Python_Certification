# ⚡ Retos Prácticos con Parquet en PySpark

En este reto práctico vas a trabajar con archivos en formato **Parquet**. A diferencia de los formatos planos como CSV o JSON, Parquet es un formato **columnar y binario** que conserva los metadatos y esquemas de los DataFrames, soportando optimizaciones críticas como la **fusión de esquemas (Schema Merging)** y la **lectura de datos particionados**.

Ya hemos generado los datos necesarios en la carpeta `datos/`. ¡Comencemos con los retos!

---

## 🛠️ Configuración Inicial de la SparkSession

Para estos retos, asegúrate de iniciar tu sesión de Spark importando las librerías necesarias:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Creamos la sesión de Spark
spark = SparkSession.builder \
    .appName("RetosParquet") \
    .getOrCreate()
```

---

## 🎯 Reto 1 — Carga y Consulta de Datos Particionados

Los datos en `datos/ventas_particionadas/` se encuentran organizados físicamente en directorios estructurados por la columna `pais` y luego por `anio` (`pais=.../anio=.../`).

### Tareas:
1. Lee todo el dataset particionado de ventas en un único DataFrame.
2. Imprime el esquema para comprobar que Spark detectó automáticamente `pais` y `anio` como columnas a pesar de ser carpetas.
3. Realiza un filtro para obtener solo las ventas de **México** en el año **2023** y muestra el resultado en consola.
4. Intenta cargar **únicamente** la carpeta correspondiente a `pais=España` de forma directa y muestra sus registros.

```python
# 💡 Escribe tu solución aquí:

```

---

## 🎯 Reto 2 — Optimización: Proyección de Columnas y Filtros (Predicate Pushdown)

Una de las mayores ventajas de Parquet es que Spark no necesita leer todo el archivo si solo requieres algunas columnas o si aplicas un filtro específico.

### Tareas:
1. Carga el dataset de ventas de nuevo, pero realiza una consulta que únicamente seleccione las columnas `producto` y `precio`, y filtra los registros cuyo `precio` sea mayor a `500`.
2. Utiliza la función `.explain(True)` al final de tu DataFrame para inspeccionar el plan físico de ejecución.
3. Busca en la salida del `.explain()` los términos `ReadSchema` (proyección de columnas) y `PushedFilters` (filtros empujados al nivel del archivo Parquet). Esto te demostrará que Spark no carga en memoria datos innecesarios.

```python
# 💡 Escribe tu solución aquí:

```

---

## 🎯 Reto 3 — Fusión de Esquemas (Schema Merging)

En entornos de Big Data y Data Lakes, los esquemas de los archivos pueden cambiar a lo largo del tiempo (por ejemplo, al añadir nuevos campos). Parquet permite unificar automáticamente esquemas distintos pero compatibles.

En la carpeta `datos/usuarios_esquema/` tenemos dos subcarpetas con esquemas diferentes:
*   `parte_a`: Contiene `id`, `nombre`, `email`.
*   `parte_b`: Contiene `id`, `nombre`, `telefono` (sin columna `email`).

### Tareas:
1. Intenta leer la carpeta raíz `datos/usuarios_esquema/` sin la opción `mergeSchema`. ¿Qué sucede con las columnas?
2. Lee la carpeta raíz activando la opción `mergeSchema`:
   ```python
   df_usuarios = spark.read \
       .option("mergeSchema", "true") \
       .parquet("./datos/usuarios_esquema")
   
   df_usuarios.printSchema()
   df_usuarios.show()
   ```
3. Observa cómo Spark unifica el esquema mostrando `null` en `email` para los usuarios de la parte B, y `null` en `telefono` para los de la parte A.

```python
# 💡 Escribe tu solución aquí:

```

---

## 🚀 ¡Sube tu código!
Una vez hayas resuelto los retos en un notebook o script de Python local, recuerda hacer un `git commit` y un `git push` para guardar tus soluciones en tu nuevo repositorio.
