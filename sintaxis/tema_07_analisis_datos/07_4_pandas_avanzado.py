"""
Tema 7 — Análisis de datos | Ejercicio 4: Pandas Avanzado
===========================================================

TEORÍA:
    Operaciones avanzadas de Pandas que se usan a diario en Data Engineering:

    MERGE / JOIN (combinar DataFrames):
        pd.merge(df1, df2, on="id", how="inner")  # inner, left, right, outer

    PIVOT TABLE (tabla dinámica):
        df.pivot_table(values="ventas", index="mes", columns="producto", aggfunc="sum")

    APPLY (aplicar función a columnas/filas):
        df["col"].apply(lambda x: x * 2)
        df.apply(func, axis=1)   # axis=1 = por fila

    MELT (formato wide → long):
        pd.melt(df, id_vars=["id"], value_vars=["ene","feb","mar"])

    GROUPBY avanzado:
        df.groupby("col").agg({"ventas": ["sum","mean","max"], "clientes": "count"})
        df.groupby("col").transform("mean")  # mantiene el índice original

    WINDOW FUNCTIONS (ventana deslizante):
        df["rolling_avg"] = df["ventas"].rolling(window=3).mean()
        df["cumsum"]      = df["ventas"].cumsum()

    pip install pandas

EJERCICIOS:

    Datos disponibles al final del archivo.

1. MERGE — Combina df_empleados y df_departamentos con un inner join por "dept_id".
   Luego haz un left join y explica la diferencia en un comentario.
   ¿Cuántos empleados quedaron sin departamento en el left join?

2. PIVOT TABLE — Con df_ventas crea una tabla dinámica que muestre:
   - Filas: mes
   - Columnas: producto
   - Valores: ventas (suma)
   - Totales de fila y columna (margins=True)
   ¿Qué producto vendió más en total? ¿En qué mes se vendió más?

3. APPLY — Sobre df_empleados:
   a) Crea columna "categoria_salario" usando apply:
      - "Alto"   si salario >= 60,000
      - "Medio"  si 40,000 <= salario < 60,000
      - "Bajo"   si salario < 40,000
   b) Crea columna "iniciales" con la primera letra de cada palabra del nombre
      Ejemplo: "Kevin Mondragón" → "K.M."

4. MELT — df_ventas está en formato wide (un mes por columna).
   Conviértelo a formato long (una fila por venta).
   Resultado esperado:

       producto  mes   ventas
       Laptop    enero  1200
       Laptop    feb    1350
       ...

5. WINDOW FUNCTIONS — Sobre df_ventas_mensuales:
   a) Calcula el promedio móvil de 3 meses (rolling mean)
   b) Calcula la suma acumulada (cumsum)
   c) Calcula el cambio porcentual mes a mes (pct_change)
   d) Marca con True las filas donde las ventas superan la media del mes anterior
"""

import pandas as pd
import numpy as np

# --- Datos ---

df_empleados = pd.DataFrame({
    "emp_id":  [1, 2, 3, 4, 5, 6],
    "nombre":  ["Kevin Mondragón", "Ana García", "Luis Torres", "Pedro Sánchez", "María López", "Carlos Ruiz"],
    "dept_id": [101, 102, 101, 103, 102, 101],
    "salario": [52000, 61000, 55000, 38000, 68000, 59000],
})

df_departamentos = pd.DataFrame({
    "dept_id":  [101, 102, 104],           # 103 falta intencionalmente
    "nombre_dept": ["Ingeniería", "Datos", "RRHH"],
})

df_ventas = pd.DataFrame({
    "producto": ["Laptop", "Mouse", "Teclado", "Monitor"],
    "enero":    [1200, 350, 280, 800],
    "febrero":  [1350, 420, 310, 750],
    "marzo":    [980,  390, 340, 900],
    "abril":    [1500, 460, 295, 870],
})

df_ventas_mensuales = pd.DataFrame({
    "mes":    pd.date_range("2025-01-01", periods=12, freq="MS"),
    "ventas": [45000, 52000, 48000, 61000, 55000, 70000,
               68000, 72000, 65000, 80000, 75000, 95000],
})


# Ejercicio 1 — MERGE


# Ejercicio 2 — PIVOT TABLE


# Ejercicio 3 — APPLY


# Ejercicio 4 — MELT


# Ejercicio 5 — WINDOW FUNCTIONS
