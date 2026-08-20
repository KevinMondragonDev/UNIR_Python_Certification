"""
Reto 18 — Optimización de Spark
==================================

Crea un dataset grande y compara el rendimiento de distintas estrategias de Spark.

Objetivo:
    Entender por qué un Spark job puede ser lento y cómo diagnosticarlo.

Parte 1 — Implementación ineficiente:
    - Usa groupBy() sin considerar el shuffle
    - Genera una operación que cause muchos shuffles innecesarios
    - Mide el tiempo de ejecución

Parte 2 — Implementación optimizada:
    - Usa broadcast join para tablas pequeñas
    - Ajusta el número de particiones con repartition() / coalesce()
    - Usa cache() / persist() para DataFrames reutilizados
    - Mide el tiempo de ejecución y compara

Métricas a investigar y documentar:

    ┌──────────────────────────┬──────────────┬──────────────┐
    │ Operación                │ Ineficiente  │ Optimizada   │
    ├──────────────────────────┼──────────────┼──────────────┤
    │ groupBy sin control      │ ???          │ N/A          │
    │ groupBy + repartition    │ N/A          │ ???          │
    │ Join sin broadcast       │ ???          │ N/A          │
    │ broadcast join           │ N/A          │ ???          │
    └──────────────────────────┴──────────────┴──────────────┘

Conceptos a investigar y documentar como comentarios en tu código:

    1. ¿Qué es un shuffle y por qué es costoso?
    2. ¿Cuándo usar repartition() vs coalesce()?
    3. ¿Cuándo y por qué usar broadcast()?
    4. ¿Qué hace cache() / persist() y cuándo conviene usarlos?
    5. ¿Cómo leer el plan de ejecución con df.explain()?

Objetivo final:
    Que puedas responder en una entrevista:
    "¿Por qué este Spark job es lento y cómo lo optimizarías?"

Prácticas:
    - pyspark.sql.functions.broadcast()
    - DataFrame.repartition() / .coalesce()
    - DataFrame.cache() / .persist()
    - DataFrame.explain()
    - time.time() para medir duración

Dependencias:

    pip install pyspark
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Tu código aquí
