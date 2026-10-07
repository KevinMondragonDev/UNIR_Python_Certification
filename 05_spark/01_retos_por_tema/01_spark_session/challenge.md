# ⚡ Reto 1 — SparkSession: El Punto de Entrada

> **Objetivo:** Entender qué es Spark, cómo iniciar una sesión y explorar su entorno básico.

---

## ¿Qué es Apache Spark?

Apache Spark es un motor de procesamiento distribuido de datos a gran escala. En lugar de procesar los datos en una sola máquina, los distribuye entre múltiples nodos para lograr velocidades que serían imposibles con pandas o SQL tradicional.

```
Tu código Python (PySpark)
        ↓
    SparkSession
        ↓
    Spark Driver  →  Spark Executors (Workers)
                          ↓
                    Datos distribuidos (RDDs / DataFrames)
```

---

## Reto 1 — Crear tu primera SparkSession

Crea una SparkSession con las siguientes configuraciones:

1. Importa `SparkSession` desde `pyspark.sql`.
2. Crea una sesión con:
   - `appName` = `"MiPrimerApp"`
   - `master` = `"local[*]"` (usa todos los núcleos disponibles)
3. Guárdala en una variable llamada `spark`.
4. Imprime la versión de Spark con `spark.version`.
5. Al terminar, detén la sesión con `spark.stop()`.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MiPrimerApp") \
    .master("local[*]") \
    .getOrCreate()

print(f"Versión de Spark: {spark.version}")
spark.stop()
```

---

## Reto 2 — Configuraciones de SparkSession

Configura una SparkSession con parámetros adicionales:

1. Añade la configuración `.config("spark.sql.shuffle.partitions", "4")` para reducir las particiones en operaciones shuffle (útil en desarrollo local).
2. Añade `.config("spark.executor.memory", "1g")` para limitar el uso de memoria.
3. Imprime el valor de esa configuración con:
   ```python
   spark.conf.get("spark.sql.shuffle.partitions")
   ```
4. Explora la UI de Spark: cuando tu aplicación está corriendo, abre `http://localhost:4040` en tu navegador.

---

## Reto 3 — SparkContext vs SparkSession

> ⚠️ En versiones modernas de PySpark, `SparkSession` reemplaza a `SparkContext` para la mayoría de operaciones. Sin embargo, aún puedes acceder a él.

1. Accede al `SparkContext` interno de tu sesión con `spark.sparkContext`.
2. Guárdalo en una variable `sc`.
3. Imprime el número de núcleos disponibles: `sc.defaultParallelism`.
4. Crea un RDD simple desde una lista: `sc.parallelize([1, 2, 3, 4, 5])`.
5. Imprime los elementos del RDD con `.collect()`.

---

## Conceptos clave

| Concepto              | Descripción                                                             |
|-----------------------|-------------------------------------------------------------------------|
| `SparkSession`        | Punto de entrada unificado para trabajar con Spark                      |
| `builder`             | Patrón de construcción para configurar la sesión                        |
| `getOrCreate()`       | Crea una nueva sesión o reutiliza una existente                         |
| `local[*]`            | Corre Spark en modo local usando todos los núcleos del equipo           |
| `SparkContext`        | Punto de entrada para operaciones de bajo nivel (RDDs)                  |
| `spark.stop()`        | Libera los recursos cuando terminas tu trabajo                          |
