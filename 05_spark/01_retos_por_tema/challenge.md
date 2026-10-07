# 1. Contexto y Fundamentos (Teoría breve)

## ¿Qué es PySpark?

**PySpark** es la API de **Apache Spark** para Python. Su propósito es procesar grandes volúmenes de datos de forma distribuida, permitiendo que cientos o miles de máquinas trabajen al mismo tiempo.

En consultoría y empresas es común encontrarlo en proyectos de:

* Ingeniería de Datos (Data Engineering)
* ETL (Extract, Transform, Load)
* Data Warehouses
* Machine Learning
* Big Data
* Procesamiento de logs
* Análisis financiero
* Sistemas bancarios
* Telecomunicaciones
* Retail

---

# SparkContext

## ¿Qué es?

Es el **punto de entrada** de Spark.

Cuando ejecutas un programa de Spark, lo primero que existe es un **SparkContext**, el cual se encarga de:

* Conectarse al clúster.
* Distribuir trabajo.
* Crear los primeros RDD.
* Administrar memoria.
* Coordinar los procesos.

Sin SparkContext, Spark no puede comenzar a trabajar.

Hoy en día normalmente no se crea directamente porque viene incluido dentro del **SparkSession**, aunque es importante entender que existe.

---

## Analogía

Imagina una empresa de paquetería.

* El **SparkContext** es el gerente general.
* Él sabe cuántos empleados existen.
* Decide quién hace cada tarea.
* Organiza el trabajo.

Los empleados serían los ejecutores (Executors).

---

## Caso empresarial

Una empresa recibe cada noche:

* 300 millones de registros bancarios.
* 80 GB de logs.
* 15 millones de transacciones.

SparkContext inicia el trabajo para repartir todos esos datos entre varios servidores.

---

## Sintaxis básica

Conceptos que debes reconocer:

* Crear una sesión Spark
* Obtener el SparkContext desde la sesión
* Leer archivos
* Paralelizar datos

No memorices todavía el código; solo identifica estos elementos.

---

# RDD (Resilient Distributed Dataset)

## ¿Qué es?

Es la estructura de datos original de Spark.

Un RDD es:

* Distribuido
* Paralelo
* Inmutable
* Tolerante a fallos

Cada transformación genera un nuevo RDD.

---

## Analogía

Imagina una enorme pila de cajas repartidas en distintos almacenes.

Cada almacén trabaja sobre sus propias cajas al mismo tiempo.

Eso es un RDD.

---

## ¿Por qué fue importante?

Antes de los DataFrames, todo Spark trabajaba con RDD.

Aún hoy aparecen en:

* Procesamiento muy personalizado
* Algoritmos
* Datos sin estructura
* Sistemas heredados

---

## Conceptos importantes

Transformaciones (no ejecutan trabajo):

* map
* filter
* flatMap
* distinct
* union

Acciones (ejecutan trabajo):

* collect
* count
* first
* take
* reduce

Spark utiliza **Lazy Evaluation**:

Nada se ejecuta hasta llamar una acción.

---

# DataFrame

## ¿Qué es?

Es la estructura moderna de Spark.

Organiza la información en:

* filas
* columnas
* esquema

Es muy similar a:

* una tabla SQL
* un DataFrame de Pandas

---

## ¿Por qué casi todas las empresas usan DataFrames?

Porque Spark puede optimizar automáticamente las consultas mediante el optimizador **Catalyst**.

Esto significa:

* menos memoria
* menos CPU
* menos tiempo de ejecución

Sin que el programador haga optimizaciones manuales.

---

## Analogía

Piensa en Excel.

Cada columna tiene un significado.

| Nombre | Edad | Ciudad |
| ------ | ---- | ------ |

Eso es un DataFrame.

---

## Operaciones frecuentes

* seleccionar columnas
* agregar columnas
* eliminar columnas
* ordenar
* filtrar
* agrupar
* unir tablas (Join)
* funciones agregadas

---

## Caso empresarial

Una consultora recibe diariamente:

* Clientes
* Ventas
* Productos

Todo se carga en DataFrames para generar reportes.

---

# Spark SQL

## ¿Qué es?

Es un módulo que permite consultar DataFrames usando SQL tradicional.

En lugar de escribir transformaciones con la API de PySpark, puedes escribir:

* SELECT
* FROM
* WHERE
* GROUP BY
* ORDER BY
* JOIN

---

## Analogía

Es como si tuvieras una base de datos, pero realmente los datos viven en Spark.

---

## Caso empresarial

Supongamos que un banco tiene:

* 900 millones de movimientos

Muchos analistas no saben Python, pero sí SQL.

Spark SQL les permite consultar esos datos sin cambiar de tecnología.

---

## Flujo típico

En proyectos reales suele seguirse este flujo:

1. Crear SparkSession.
2. Obtener SparkContext (si es necesario).
3. Leer archivos (CSV, Parquet, JSON, etc.).
4. Crear DataFrames.
5. Limpiar información.
6. Crear vistas temporales.
7. Consultar con Spark SQL.
8. Guardar resultados.

---

# 2. Retos de Aprendizaje Progresivo (Práctica)

## Reto 1 — Mi primer SparkContext

### Escenario

Una empresa quiere comenzar a usar Spark para distribuir datos.

### Datos iniciales

Una lista de números:

```
[12, 5, 18, 7, 30, 25, 4, 50]
```

### Reglas de negocio

* Debes crear el contexto de trabajo.
* Convertir la lista en un RDD.
* Contar cuántos elementos existen.
* Obtener únicamente los primeros tres.

### Objetivo

Familiarizarte con:

* SparkSession
* SparkContext
* parallelize
* count
* take

---

## Reto 2 — Transformaciones sobre RDD

### Escenario

Una empresa de logística quiere incrementar un costo fijo de envío.

### Datos iniciales

```
[100,150,90,75,300,220]
```

### Reglas de negocio

* Incrementar cada costo en un 10%.
* Conservar únicamente los costos mayores a 150.
* Ordenar el resultado.
* Contar cuántos quedaron.

### Objetivo

Practicar:

* map
* filter
* sort
* collect
* count

---

## Reto 3 — Del RDD al DataFrame

### Escenario

Recibes información de empleados.

### Datos iniciales

| id | nombre | edad | salario |
| -- | ------ | ---- | ------- |
| 1  | Ana    | 24   | 18000   |
| 2  | Luis   | 35   | 32000   |
| 3  | Pedro  | 29   | 27000   |
| 4  | María  | 41   | 45000   |
| 5  | Juan   | 22   | 16000   |

### Reglas

Construye un DataFrame con esquema adecuado.

Después:

* Mostrar el esquema.
* Mostrar las primeras filas.
* Contar empleados.

### Objetivo

Aprender la diferencia entre:

RDD

↓

DataFrame

---

## Reto 4 — Manipulación de DataFrames

### Escenario

La empresa quiere preparar un reporte para Recursos Humanos.

### Datos

Utiliza el DataFrame del reto anterior.

### Reglas

Debes obtener:

* Solo nombre y salario.
* Empleados mayores de 30 años.
* Una nueva columna con salario anual.
* Ordenar por salario descendente.

### Objetivo

Practicar operaciones típicas de DataFrames.

---

## Reto 5 — Agrupaciones

### Escenario

Una cadena de tiendas quiere conocer sus ventas.

### Datos iniciales

| producto   | categoría   | venta |
| ---------- | ----------- | ----- |
| Laptop     | Electrónica | 25000 |
| Mouse      | Electrónica | 500   |
| Teclado    | Electrónica | 900   |
| Silla      | Muebles     | 3200  |
| Mesa       | Muebles     | 5500  |
| Escritorio | Muebles     | 7200  |
| Monitor    | Electrónica | 6500  |

### Reglas

Obtener:

* Total vendido por categoría.
* Venta promedio.
* Producto más caro.
* Cantidad de productos por categoría.

### Objetivo

Practicar:

* groupBy
* agg
* sum
* avg
* count
* max

---

## Reto 6 — Spark SQL

### Escenario

Una empresa quiere que el área de negocio consulte información usando SQL.

### Datos

Utiliza el DataFrame del reto anterior.

### Reglas

Registrar el DataFrame como vista temporal.

Resolver mediante SQL:

* Todos los productos con ventas mayores a 5000.
* Promedio de ventas por categoría.
* Total vendido por categoría.
* Producto más costoso.
* Ordenar de mayor a menor venta.

### Objetivo

Aprender cuándo utilizar SQL en lugar de la API de DataFrame.

---

# 3. El Reto Integrador (Proyecto Final)

## Proyecto: Pipeline de análisis de ventas para una cadena de tiendas

### Contexto

Una empresa nacional cuenta con información diaria de ventas provenientes de múltiples sucursales. Tu tarea es construir un flujo de procesamiento utilizando PySpark para generar indicadores que serán consumidos por el área de negocio.

### Datos iniciales

Dispones de tres conjuntos de datos:

**Ventas**

| id_venta | id_cliente | id_producto | sucursal | cantidad | precio_unitario | fecha |
| -------- | ---------- | ----------- | -------- | -------- | --------------- | ----- |

**Clientes**

| id_cliente | nombre | ciudad | segmento |
| ---------- | ------ | ------ | -------- |

**Productos**

| id_producto | nombre_producto | categoría |
| ----------- | --------------- | --------- |

### Reglas de negocio

1. Crear la sesión de Spark y obtener el SparkContext.
2. Cargar los tres conjuntos de datos como DataFrames.
3. Validar que los esquemas sean correctos y que no existan registros duplicados.
4. Calcular el importe total de cada venta (`cantidad × precio_unitario`) y añadirlo como una nueva columna.
5. Unir (join) las tablas de ventas, clientes y productos para obtener un único conjunto enriquecido.
6. Obtener el total de ventas por categoría de producto.
7. Calcular el total de ventas por ciudad y por segmento de cliente.
8. Identificar el producto con mayor facturación y el cliente que más ha comprado.
9. Crear una vista temporal con el DataFrame final y resolver mediante Spark SQL:

   * Top 5 productos con mayor facturación.
   * Total de ventas por sucursal.
   * Ticket promedio por ciudad.
   * Categoría con mayor ingreso.
   * Número de ventas por segmento de cliente.
10. Guardar el resultado final en formato **Parquet**, listo para ser consumido por un proceso de analítica o un dashboard.

### Objetivo

Este proyecto integra todos los conceptos fundamentales de PySpark:

* **SparkContext** como punto de entrada y coordinación del trabajo distribuido.
* **RDD** para comprender el modelo de procesamiento distribuido y la evaluación perezosa (lazy evaluation).
* **DataFrames** para realizar transformaciones eficientes, limpiezas, agregaciones y uniones de datos.
* **Spark SQL** para ejecutar consultas declarativas sobre los datos procesados.

Al completar este reto habrás construido un flujo muy similar a los que implementan ingenieros de datos y consultores en proyectos de Big Data para sectores como banca, retail, telecomunicaciones y comercio electrónico.

---

# Reto 6 — Spark SQL sobre archivos Parquet

## Escenario

Una empresa de retail almacena todas sus ventas diarias en archivos **Parquet** dentro de su Data Lake.

El equipo de negocio necesita consultar la información utilizando SQL sin importar cómo fueron procesados los datos previamente.

Tu tarea consiste en leer los archivos Parquet, registrar una vista temporal y responder distintas preguntas mediante Spark SQL.

---

## Datos iniciales

Dispones del siguiente archivo:

```
ventas.parquet
```

El archivo contiene el siguiente esquema:

| id_venta | producto | categoria | ciudad | vendedor | cantidad | precio_unitario | fecha |
| -------- | -------- | --------- | ------ | -------- | -------- | --------------- | ----- |

---

## Reglas de negocio

1. Crear una SparkSession.
2. Leer el archivo **Parquet**.
3. Verificar el esquema del DataFrame.
4. Mostrar las primeras filas.
5. Registrar el DataFrame como una vista temporal llamada:

```
ventas
```

6. Resolver todas las consultas **únicamente utilizando Spark SQL**.

---

## Consultas a realizar

### Consulta 1

Obtener todas las ventas cuyo precio unitario sea mayor a **5000**.

---

### Consulta 2

Calcular el importe total de cada venta utilizando:

```
cantidad × precio_unitario
```

Mostrar:

* id_venta
* producto
* importe_total

---

### Consulta 3

Obtener el total vendido por categoría.

---

### Consulta 4

Calcular el promedio del precio unitario por categoría.

---

### Consulta 5

Mostrar las cinco ventas con mayor importe total.

---

### Consulta 6

Obtener el número de ventas realizadas por ciudad.

---

### Consulta 7

Encontrar el producto que más ingresos generó considerando la suma de todas sus ventas.

---

### Consulta 8

Ordenar todas las ventas desde el mayor importe hasta el menor.

---

## Objetivo

Al finalizar este reto deberás ser capaz de:

* Leer archivos **Parquet** con PySpark.
* Comprender por qué Parquet es el formato estándar en Data Lakes.
* Crear vistas temporales sobre DataFrames.
* Ejecutar consultas SQL directamente sobre datos distribuidos.
* Utilizar funciones de agregación, ordenamiento y filtrado mediante Spark SQL.

---

## Bonus Challenge (Nivel Consultoría)

La empresa ahora recibe un nuevo archivo cada día con el siguiente formato:

```
/ventas/
    ├── año=2025/
    │      ├── mes=01/
    │      ├── mes=02/
    │      └── ...
    ├── año=2026/
    │      ├── mes=01/
    │      └── ...
```

### Objetivo

Sin modificar el contenido de los archivos:

* Leer automáticamente todos los Parquet de una carpeta.
* Consultar únicamente las ventas correspondientes a un año específico mediante Spark SQL.
* Comparar el total de ventas entre dos años diferentes usando una sola consulta SQL.

> **Este reto refleja un escenario muy común en proyectos empresariales**, donde los datos se almacenan particionados por año, mes o día para mejorar el rendimiento de lectura en plataformas como Hadoop, Spark, Databricks, Amazon EMR o Azure Synapse.
