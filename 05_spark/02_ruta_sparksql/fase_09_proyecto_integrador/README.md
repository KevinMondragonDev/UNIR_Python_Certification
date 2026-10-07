# Fase 9 — Proyecto Integrador

## Descripción

Construirás un **pipeline de análisis de datos E2E** (End-to-End) para una empresa
ficticia de comercio electrónico, usando todo lo aprendido en las fases 1-8.

---

## Arquitectura del Pipeline

```
[Ingesta]         [Limpieza]        [Transformación]      [Análisis SQL]     [Salida]
   CSV               Nulos              Joins                CTEs              Parquet
   Parquet    →      Duplicados   →     Window Fn     →      Subqueries   →    Particionado
   Schema            Tipos              Agregaciones         Views             CSV reportes
```

---

## Preguntas de Negocio a Responder

### Módulo de Ventas
1. ¿Cuál es el top 5 de vendedores por ingresos totales?
2. ¿Qué región tiene la mayor tasa de crecimiento MoM?
3. ¿Cuál es el producto más vendido por categoría?

### Módulo de Clientes
4. ¿Cuántos clientes activos hay por país? (con al menos 1 transacción)
5. ¿Cuál es el valor de vida del cliente (LTV) promedio?
6. Segmenta clientes en: Alto Valor, Medio, Bajo según su gasto total

### Módulo de Empleados
7. ¿Qué departamento tiene la mayor eficiencia de ventas (ventas/empleado)?
8. ¿Cómo se distribuyen los salarios por departamento y nivel?

---

## Archivos

| Archivo | Descripción |
|---------|-------------|
| `01_ingesta.py` | Leer todos los datasets con schema validado |
| `02_limpieza.py` | Normalizar, quitar nulos y duplicados |
| `03_transformacion.py` | Joins y enriquecimiento |
| `04_analisis_sql.py` | Responder preguntas de negocio con SQL |
| `05_escritura.py` | Guardar resultados particionados |
| `pipeline.py` | Versión monolítica (todo en un archivo) |
| `pipeline_modular.py` | Orquestador que ejecuta los 5 módulos en orden |
| `_modulos.py` | Helper para importar archivos que empiezan con número |
| `validar_proyecto.py` | 25 asserts de calidad de datos y salidas |

Cada módulo se puede ejecutar solo (`python 03_transformacion.py`) y encadena los pasos anteriores.

---

## Decisiones de procesamiento distribuido

| Paso | Decisión | Por qué |
|------|----------|---------|
| Ingesta | Schema explícito | Evita una pasada extra de `inferSchema` |
| Transformación | `F.broadcast()` en dimensiones | Hechos sin shuffle en el join |
| Transformación | Seleccionar columnas antes del join | Menos bytes en broadcast y memoria |
| Transformación | `cache()` de `ventas_ricas` | La reutilizan análisis, reportes y escritura |
| Escritura | `repartition("year")` + `partitionBy("year")` | 1 archivo por carpeta, sin *small files* |
| Escritura | `coalesce(1)` en reportes | Un CSV legible, sin shuffle |

---

## Criterios de Éxito

- [ ] Pipeline completo se ejecuta sin errores (`python pipeline_modular.py`)
- [ ] `validar_proyecto.py` pasa todos los checks
- [ ] Datos de salida guardados en Parquet particionado
- [ ] Todas las preguntas de negocio respondidas
- [ ] Código limpio con CTEs y sin collect() innecesarios

---

## Dataset de Salida Esperado

```
output/
├── ventas_enriquecidas.parquet/
│   ├── year=2021/
│   ├── year=2022/
│   ├── year=2023/
│   └── year=2024/
├── clientes_segmentados.parquet/      ← 1 archivo
├── reporte_departamentos.parquet/     ← 1 archivo
└── reportes_csv/
    ├── top_vendedores.csv
    ├── top_productos.csv
    └── metricas_regiones.csv
```
