# Fase 7 — Spark SQL: Avanzado

## Window Functions en SQL

```sql
-- Sintaxis general
funcion() OVER (
    PARTITION BY columna       -- divide en grupos (opcional)
    ORDER BY columna ASC/DESC  -- ordena dentro del grupo
    ROWS/RANGE BETWEEN ...     -- tamaño de la ventana (opcional)
)
```

### Funciones de ranking
```sql
RANK()         OVER (PARTITION BY dept ORDER BY salary DESC)
DENSE_RANK()   OVER (PARTITION BY dept ORDER BY salary DESC)
ROW_NUMBER()   OVER (PARTITION BY dept ORDER BY salary DESC)
PERCENT_RANK() OVER (ORDER BY salary)
NTILE(4)       OVER (ORDER BY salary)   -- cuartiles
```

### Funciones de desplazamiento
```sql
LAG(salary, 1, 0)   OVER (PARTITION BY dept ORDER BY hire_date)
LEAD(salary, 1, 0)  OVER (PARTITION BY dept ORDER BY hire_date)
FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY salary DESC)
LAST_VALUE(salary)  OVER (PARTITION BY dept ORDER BY salary)
```

### Agregaciones con ventana (sin colapsar filas)
```sql
SUM(amount) OVER (PARTITION BY region ORDER BY sale_date
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
-- → suma acumulada

AVG(amount) OVER (PARTITION BY region ORDER BY sale_date
                  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
-- → promedio móvil de 3 períodos
```

---

## Explain Plan

```python
# Ver plan de ejecución
spark.sql("SELECT ...").explain()           # plan físico
spark.sql("SELECT ...").explain("extended") # logical + physical
spark.sql("SELECT ...").explain("cost")     # con estadísticas de costo
spark.sql("SELECT ...").explain("formatted") # más legible

# Desde SQL
spark.sql("EXPLAIN SELECT ...")
spark.sql("EXPLAIN EXTENDED SELECT ...")
```

---

## Catálogo

```python
spark.catalog.listTables()          # vistas registradas
spark.catalog.listColumns("tabla")  # columnas de una tabla
spark.catalog.listDatabases()       # bases de datos
spark.catalog.tableExists("nombre") # verificar existencia
spark.catalog.isCached("tabla")     # si está en caché
```

---

## Arrays y Structs en SQL

```sql
-- Explotar array (una fila por elemento)
SELECT event_id, EXPLODE(tags) as tag FROM events

-- Tamaño del array
SELECT event_id, SIZE(tags) as num_tags FROM events

-- Verificar si contiene elemento
SELECT event_id FROM events WHERE ARRAY_CONTAINS(tags, 'promo')

-- Acceder por índice
SELECT tags[0] as primer_tag FROM events

-- Agregar arrays
SELECT COLLECT_LIST(name) FROM empleados GROUP BY department

-- Struct
SELECT STRUCT(name, salary) as empleado_info FROM empleados
SELECT empleado_info.name FROM vista_con_struct
```

---

## Archivos de esta fase

| Archivo | Descripción |
|---------|-------------|
| `01_window_sql.py` | Window functions completas en SQL |
| `02_explain_plan.py` | Leer e interpretar planes de ejecución |
| `03_catalogo.py` | Catálogo de Spark |
| `04_arrays_structs.py` | Tipos complejos en SQL |
| `ejercicios.py` | 10 ejercicios avanzados |
| `reto.py` | Reporte ejecutivo con window + explain |
| `solucion_reto.py` | Solución |
| `validar.py` | Autoevaluación |

---

## Conceptos que debes dominar al terminar

- [ ] Window functions con PARTITION BY y ORDER BY en SQL
- [ ] Diferencia entre RANK, DENSE_RANK y ROW_NUMBER
- [ ] LAG/LEAD para análisis temporal
- [ ] Sumas acumuladas y promedios móviles con ROWS BETWEEN
- [ ] Leer un explain plan y entender sus nodos
- [ ] EXPLODE para desanidar arrays
- [ ] COLLECT_LIST/COLLECT_SET en agregaciones
