# 05 — Spark

Dos rutas complementarias:

| Carpeta | Formato | Cuándo usarla |
|---|---|---|
| `01_retos_por_tema/` | Un `challenge.md` por tema, resuelves desde cero | Práctica rápida de cada API |
| `02_ruta_sparksql/` | Ejemplos ejecutables, `ejercicios.py`, `reto.py` y `validar.py` por fase | Ruta completa, de RDDs a despliegue en clúster |

Orden sugerido: lee `01_retos_por_tema/challenge.md` y haz los retos `01`-`11`; después sigue
`02_ruta_sparksql/` desde la `fase_01` (ver su README).

Para regenerar los datos de los retos por tema:

```bash
python 01_retos_por_tema/generar_todos_los_datasets.py
python 01_retos_por_tema/10_lectura_escritura/generar_datos.py
```
