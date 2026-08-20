"""
Reto 17 — ETL con PySpark
===========================

Construye un pipeline ETL completo usando PySpark.

Entrada:

    data/customers.parquet

Pipeline:

    Read (Parquet)
        ↓
    Filter (eliminar registros inválidos)
        ↓
    Transform (normalizar y enriquecer datos)
        ↓
    Aggregate (agrupar por país)
        ↓
    Write (Parquet particionado)
        ↓
    output/customers_by_country/

Transformaciones requeridas:
    1. Filtrar registros donde age es NULL o < 0
    2. Normalizar nombres con initcap()
    3. Agregar columna processed_at con la fecha actual
    4. Calcular avg(age) y count() agrupado por country

Resultado esperado (output/customers_by_country/):

    country=Mexico/part-00000.parquet
    country=USA/part-00000.parquet
    country=Colombia/part-00000.parquet

Prácticas:
    - SparkSession (inicialización y configuración)
    - spark.read.parquet()
    - DataFrame.select()
    - DataFrame.filter() / .where()
    - DataFrame.withColumn()
    - DataFrame.groupBy().agg()
    - DataFrame.write.partitionBy().parquet()
    - F.col(), F.lit(), F.current_date(), F.initcap(), F.avg(), F.count()

Dependencias:

    pip install pyspark

Tip: Inicia con un dataset pequeño que crees manualmente con spark.createDataFrame().
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Tu código aquí
