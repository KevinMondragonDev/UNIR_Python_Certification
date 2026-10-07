# 🐍 UNIR · Certificación de Python para Big Data

[![CI](https://github.com/KevinMondragonDev/UNIR-Certification-Python/actions/workflows/ci.yml/badge.svg)](https://github.com/KevinMondragonDev/UNIR-Certification-Python/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![PySpark](https://img.shields.io/badge/pyspark-3.5%20%7C%204.x-orange)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Repositorio de estudio y práctica para la certificación de Python del programa de **Maestría en Big Data (UNIR)**.

Va de los fundamentos del lenguaje hasta el **procesamiento distribuido con PySpark**. Cada bloque combina teoría breve, ejercicios para completar y retos integradores. La ruta de Spark incluye además validadores automáticos para comprobar tu avance.

---

## 🗺️ Ruta de aprendizaje

```
01 Teoría y ejercicios ─► 02 Retos prácticos ─► 03 Estructuras de datos ─► 04 Funciones y excepciones ─► 05 Spark
   (Python, NumPy,          (20 mini-proyectos      (colecciones y           (funciones, errores,          (retos por tema +
    Pandas, gráficos)        de data engineering)    control de flujo)        POO)                          ruta completa a clúster)
```

| # | Módulo | Qué aprendes | Formato |
|---|---|---|---|
| 01 | [`01_teoria_y_ejercicios/`](01_teoria_y_ejercicios) | Funciones, POO, regex, NumPy, Pandas, EDA, series de tiempo, Matplotlib, Seaborn, Plotly | `.py` con teoría en el docstring y `# Tu código aquí` |
| 02 | [`02_retos_practicos/`](02_retos_practicos) | 20 retos progresivos: validadores, ETL, data quality, S3, PySpark, código de producción | Un `.py` por reto |
| 03 | [`03_data_structures/`](03_data_structures) | Listas, tuplas, diccionarios, sets, condicionales y bucles | `challenge.md` por tema |
| 04 | [`04_funciones_y_excepciones/`](04_funciones_y_excepciones) | Funciones, excepciones, POO y un integrador | `challenge.md` por tema |
| 05 | [`05_spark/`](05_spark) | PySpark de cero a clúster (ver abajo) | Retos + ejemplos ejecutables + validadores |
| 99 | [`99_notas/`](99_notas) | Comandos de inicio y convenciones de código | Referencia |

### ⚡ Módulo 05 — Spark

Tiene dos rutas complementarias:

**`01_retos_por_tema/`**: 11 retos cortos, uno por API. Son SparkSession, RDDs, DataFrames, transformaciones, agregaciones, joins, Spark SQL, funciones built-in, nulos, lectura/escritura (CSV, JSON, Parquet, particionado) y un gran integrador.

**`02_ruta_sparksql/`**: una ruta completa en 10 fases. Cada fase tiene ejemplos ejecutables, `ejercicios.py`, `reto.py`, `solucion_reto.py` y `validar.py`.

| Fase | Tema |
|---|---|
| 01 | Arquitectura (driver, executors, cluster manager), lazy evaluation, DAG |
| 02–03 | RDDs, pair RDDs, `reduceByKey` vs `groupByKey`, acumuladores y broadcast |
| 04–05 | DataFrames, esquemas, joins, window functions, pivot, nulos |
| 06–07 | Spark SQL: vistas, CTEs, subqueries, window en SQL, `explain()`, arrays y structs |
| 08 | **Optimización:** particiones, narrow vs wide, shuffle, `repartition`/`coalesce`, broadcast vs sort-merge join, **AQE**, skew y salting, caché, UDFs y Pandas UDFs |
| 09 | Proyecto integrador E2E modular: ingesta → limpieza → transformación → SQL → escritura particionada |
| 10 | **Despliegue en clúster:** `spark-submit`, client vs cluster, dimensionamiento de executors, memoria, Spark UI, clúster standalone local y con Docker |

---

## 🚀 Configuración

### Opción A — Conda (recomendada: incluye Java para Spark)

```bash
conda env create -f environment.yml
conda activate desarrollo
```

### Opción B — pip

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
java -version          # Spark necesita Java 17 o superior
```

Comprueba la instalación:

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

---

## ▶️ Cómo usarlo

```bash
# Ejercicios de Python: abre el archivo, completa "# Tu código aquí" y ejecútalo
python 01_teoria_y_ejercicios/01_core_python/01_funciones/01_definicion_funciones.py

# Ruta de Spark: genera los datasets una vez
cd 05_spark/02_ruta_sparksql
python datasets/generar_datasets.py

# Estudia una fase y valida tu avance
python fase_08_optimizacion_y_udfs/02_particionamiento.py
python fase_08_optimizacion_y_udfs/validar.py

# Reporte global de tu avance (validadores de cada fase)
python supervision/test_global.py
python supervision/test_global.py --fase 8 9 10

# Comprobar que todo el material de referencia funciona (lo mismo que ejecuta la CI)
python supervision/smoke_test.py

# Clúster Spark real en tu máquina (fase 10), sin Docker
./fase_10_despliegue_cluster/cluster_local.sh start     # UI: http://localhost:8080
spark-submit --master spark://127.0.0.1:7077 --total-executor-cores 4 \
    fase_10_despliegue_cluster/01_job_spark_submit.py --salida /tmp/reporte
./fase_10_despliegue_cluster/cluster_local.sh stop
```

---

## 📁 Estructura

```
.
├── 00_pruebas/                         Sandbox para probar código suelto
├── 01_teoria_y_ejercicios/
│   ├── 01_core_python/
│   │   ├── 01_funciones/               01_definicion_funciones.py … 07_funciones_anonimas.py
│   │   ├── 02_organizacion_y_poo/      01_modulos_paquetes.py … 05_poo_patrones_diseno.py
│   │   └── 03_python_avanzado/         01_expresiones_regulares.py … 03_comprension_listas.py
│   ├── 02_analisis_datos/              01_numpy.py … 07_series_de_tiempo.py
│   └── 03_visualizacion/               01_matplotlib.py … 06_proyecto_eda_visual.py
├── 02_retos_practicos/                 reto_01_fundamentos_pipeline.py … reto_20_codigo_produccion.py
├── 03_data_structures/
│   ├── 01_data/                        01_listas … 05_gran_integrador
│   └── 02_control_de_flujo/            01_condicionales … 05_gran_integrador
├── 04_funciones_y_excepciones/         01_funciones … 04_gran_integrador
├── 05_spark/
│   ├── 01_retos_por_tema/              01_spark_session … 11_gran_integrador
│   └── 02_ruta_sparksql/
│       ├── datasets/                   CSV y Parquet sintéticos
│       ├── fase_01_fundamentos_spark/
│       ├── …
│       ├── fase_10_despliegue_cluster/
│       └── supervision/                test_global.py, progreso.md, rubrica.md
├── 99_notas/                           comandos_de_inicio.md, convenciones.md
├── .github/workflows/ci.yml            CI: sintaxis + smoke test de PySpark
├── requirements.txt                    Dependencias pip
├── environment.yml                     Entorno Conda (Python 3.10 + Java 17)
└── LICENSE                             MIT
```

### Convención de nombres

- Carpetas y archivos en `snake_case` con prefijo `NN_` (ver [`99_notas/convenciones.md`](99_notas/convenciones.md)).
- El prefijo fija el **orden de estudio** y vuelve a empezar en `01` dentro de cada carpeta.
- `99_notas` queda siempre al final como material de referencia.

---

## 🛠️ Tecnologías

**Python 3.10+** · **NumPy** · **Pandas** · **Matplotlib** · **Seaborn** · **Plotly** · **PySpark** · **Apache Arrow** · **Conda** · **Docker** (opcional, para el clúster de la fase 10)

---

## ✅ Integración continua

En cada push, GitHub Actions:

1. Compila todos los `.py` del repositorio.
2. Ejecuta `05_spark/02_ruta_sparksql/supervision/smoke_test.py` con Java 17 y Python 3.10. Corre los ejemplos de cada fase, las soluciones de los retos, el pipeline del proyecto integrador con su validador y el job de producción de la fase 10.

Los `ejercicios.py` y los validadores de alumno no forman parte de la CI, porque dependen de que completes tu código.

---

## 📄 Licencia

[MIT](LICENSE) © Kevin Mondragón Fresco

---

## 👤 Autor

**Kevin Mondragón Fresco**
Programa de Maestría en Big Data — UNIR
