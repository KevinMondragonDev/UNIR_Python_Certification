# Fase 6 — Spark SQL: Básico

## ¿Qué es Spark SQL?

Spark SQL permite ejecutar **queries SQL estándar** directamente sobre DataFrames
registrando **vistas temporales** y usando `spark.sql()`.

---

## Vistas Temporales

```python
# Vista local (solo en la SparkSession actual)
df.createOrReplaceTempView("empleados")

# Vista global (disponible en todas las SparkSessions de la JVM)
df.createGlobalTempView("empleados_global")
# Acceso: SELECT * FROM global_temp.empleados_global

# Listar vistas
spark.catalog.listTables()

# Eliminar vista
spark.catalog.dropTempView("empleados")
```

---

## spark.sql() — Ejecutar Queries

```python
result = spark.sql("""
    SELECT department, COUNT(*) as total, AVG(salary) as avg_salary
    FROM empleados
    WHERE salary IS NOT NULL
    GROUP BY department
    ORDER BY avg_salary DESC
""")
result.show()
```

`spark.sql()` **devuelve un DataFrame** — puedes encadenar operaciones sobre él.

---

## Funciones Built-in esenciales

### String
```sql
UPPER(col)   LOWER(col)   TRIM(col)   LENGTH(col)
SUBSTR(col, start, len)
CONCAT(col1, col2)    CONCAT_WS(sep, col1, col2)
REPLACE(col, 'old', 'new')
SPLIT(col, delimiter)[0]     -- primer elemento
REGEXP_REPLACE(col, pattern, replacement)
```

### Fecha
```sql
CURRENT_DATE()    CURRENT_TIMESTAMP()
TO_DATE(col, 'yyyy-MM-dd')
DATE_FORMAT(col, 'MMM yyyy')
YEAR(col)   MONTH(col)   DAY(col)   HOUR(col)
DATEDIFF(fecha1, fecha2)
DATE_ADD(col, n)    DATE_SUB(col, n)
```

### Matemáticas
```sql
ROUND(col, n)   CEIL(col)   FLOOR(col)
ABS(col)        MOD(col, n)
SQRT(col)       POW(col, n)
```

### Condicionales
```sql
CASE WHEN salary < 40000 THEN 'Junior'
     WHEN salary < 70000 THEN 'Mid'
     ELSE 'Senior'
END AS nivel

IF(salary > 50000, 'Alto', 'Bajo')
COALESCE(salary, 0)
NULLIF(col, 0)   -- devuelve NULL si col == 0
IFNULL(col, 'N/A')
```

---

## CTEs (Common Table Expressions)

```sql
WITH ventas_region AS (
    SELECT region, SUM(amount) as total
    FROM ventas
    GROUP BY region
),
top_regiones AS (
    SELECT * FROM ventas_region
    WHERE total > 100000
)
SELECT * FROM top_regiones ORDER BY total DESC
```

---

## Subqueries

```sql
-- En WHERE
SELECT name, salary FROM empleados
WHERE salary > (SELECT AVG(salary) FROM empleados)

-- En FROM (tabla derivada)
SELECT dept, avg_sal
FROM (
    SELECT department as dept, AVG(salary) as avg_sal
    FROM empleados
    GROUP BY department
) sub
WHERE avg_sal > 60000

-- EXISTS
SELECT * FROM empleados e
WHERE EXISTS (
    SELECT 1 FROM ventas v WHERE v.employee_id = e.employee_id
)
```

---

## Archivos de esta fase

| Archivo | Descripción |
|---------|-------------|
| `01_temp_views.py` | Crear y gestionar vistas temporales |
| `02_queries_basicas.py` | SELECT, WHERE, GROUP BY, ORDER BY |
| `03_funciones_builtin.py` | String, fecha, math, condicionales |
| `04_ctes_subqueries.py` | CTEs y subqueries |
| `ejercicios.py` | 10 ejercicios SQL con placeholders |
| `reto.py` | Análisis completo en SQL puro |
| `solucion_reto.py` | Solución |
| `validar.py` | Autoevaluación |

---

## Conceptos que debes dominar al terminar

- [ ] Crear vistas temporales y globales
- [ ] Ejecutar queries SQL complejas con spark.sql()
- [ ] Usar funciones de fecha, string y matemáticas en SQL
- [ ] Escribir CTEs para organizar queries largas
- [ ] Usar subqueries en WHERE y FROM
- [ ] Combinar spark.sql() con la API de DataFrame
