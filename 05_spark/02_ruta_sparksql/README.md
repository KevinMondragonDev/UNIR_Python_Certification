# Plan de Aprendizaje PySpark — De Cero a SQL Avanzado

## Estructura del proyecto

```
02_ruta_sparksql/
├── datasets/
│   ├── csv/           ← employees, products, customers, sales
│   ├── parquet/       ← transactions, orders, events
│   └── generar_datasets.py
│
├── fase_01_fundamentos_spark/
├── fase_02_rdds_core/
├── fase_03_rdds_pares_acumuladores/
├── fase_04_dataframes_intro/
├── fase_05_dataframes_avanzado/
├── fase_06_spark_sql_basico/
├── fase_07_spark_sql_avanzado/
├── fase_08_optimizacion_y_udfs/
├── fase_09_proyecto_integrador/
├── fase_10_despliegue_cluster/
│
└── supervision/
    ├── test_global.py   ← ejecuta todos los validadores
    ├── progreso.md      ← registra tu avance
    └── rubrica.md       ← autoevaluación de habilidades
```

---

## Inicio rápido

### 1. Generar los datasets
```bash
cd 05_spark/02_ruta_sparksql
python datasets/generar_datasets.py
```

### 2. Verificar instalación de PySpark
```bash
python -c "import pyspark; print(pyspark.__version__)"
```

### 3. Empezar por la Fase 1
```bash
python fase_01_fundamentos_spark/01_spark_session.py
```

### 4. Verificar tu progreso global
```bash
python supervision/test_global.py
# o solo fases específicas:
python supervision/test_global.py --fase 4 5 6
```

---

## Mapa de aprendizaje

| Fase | Tema | Archivos clave |
|------|------|----------------|
| 1 | Fundamentos: SparkSession, Lazy Evaluation | `01_spark_session.py` |
| 2 | RDDs: map, filter, reduce, acciones | `02_transformaciones.py` |
| 3 | Pair RDDs, Joins RDD, Broadcast | `01_pair_rdds.py` |
| 4 | DataFrames: schema, select, filter | `03_select_filter.py` |
| 5 | DataFrame avanzado: groupBy, joins, window | `03_window_functions.py` |
| 6 | Spark SQL: vistas, CTEs, subqueries | `04_ctes_subqueries.py` |
| 7 | SQL avanzado: window, explain, arrays | `01_window_sql.py` |
| 8 | Optimización: particiones, shuffle, joins, AQE, skew, UDFs | `02_particionamiento.py`, `05_optimizacion_joins.py` |
| 9 | Proyecto integrador E2E (modular) | `pipeline_modular.py` |
| 10 | Despliegue: spark-submit, clúster, tuning, Spark UI | `01_job_spark_submit.py`, `cluster_local.sh` |

---

## Convención de archivos por fase

| Archivo | Propósito |
|---------|-----------|
| `README.md` | Teoría, conceptos y referencia |
| `01_*.py`, `02_*.py` ... | Ejemplos ejecutables con explicaciones |
| `ejercicios.py` | Código con `# TU CÓDIGO AQUÍ` para completar |
| `reto.py` | Challenge sin guía ni solución visible |
| `solucion_reto.py` | Solución con comentarios ⚠️ ver solo si te atascas |
| `validar.py` | Autoevaluación con asserts automáticos |

---

## Dependencias requeridas

```txt
pyspark>=3.3.0
pandas>=1.5.0
pyarrow>=8.0.0  # para Pandas UDFs
```

Instalar con:
```bash
pip install pyspark pandas pyarrow
```
