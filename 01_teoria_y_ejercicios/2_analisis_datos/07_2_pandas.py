"""
Tema 7 — Análisis de datos | Ejercicio 2: Pandas
==================================================

TEORÍA:
    Pandas es la librería estándar para análisis de datos tabulares.
    Sus estructuras principales son:

    - Series:    array 1D etiquetado (como una columna de tabla)
    - DataFrame: tabla 2D con filas y columnas etiquetadas

    Operaciones fundamentales:
        pd.DataFrame(data)        → crear DataFrame
        df.head(n)                → primeras n filas
        df.info()                 → resumen de tipos y nulls
        df.describe()             → estadísticas descriptivas
        df["columna"]             → seleccionar columna (Series)
        df[["col1","col2"]]       → seleccionar múltiples columnas
        df.loc[fila, col]         → acceso por etiqueta
        df.iloc[fila, col]        → acceso por posición
        df[df["col"] > valor]     → filtrado
        df.groupby("col").agg()   → agrupación
        df.sort_values("col")     → ordenar
        df.dropna()               → eliminar filas con nulos
        df.fillna(valor)          → rellenar nulos
        df.rename(columns={})     → renombrar columnas

    pip install pandas

EJERCICIOS:

1. Crea un DataFrame con estos datos y muestra:
   - Las primeras 3 filas
   - El resumen con info()
   - Las estadísticas con describe()

       datos = {
           "nombre": ["Kevin", "Ana", "Luis", "Pedro", "María"],
           "edad":   [23, 25, 31, 28, 22],
           "pais":   ["México", "Colombia", "México", "Argentina", "Colombia"],
           "salario":[45000, 52000, 61000, 48000, 55000],
       }

2. Sobre el mismo DataFrame, realiza:
   a) Filtrar solo usuarios de México
   b) Filtrar usuarios con salario mayor a 50,000
   c) Ordenar por salario de mayor a menor
   d) Agregar una columna "salario_anual" (salario * 12)

3. Agrupaciones:
   a) Cuenta de usuarios por país
   b) Salario promedio por país
   c) Salario mínimo y máximo por país (usa agg con dict)

4. Trabajo con nulos:
   Crea este DataFrame con nulos y:

       df_nulos = pd.DataFrame({
           "id":     [1, 2, 3, 4, 5],
           "nombre": ["Kevin", None, "Luis", "Pedro", None],
           "edad":   [23, 25, None, 28, 22],
           "email":  ["k@x.com", "a@x.com", None, "p@x.com", "m@x.com"],
       })

   a) Cuenta cuántos nulos hay por columna
   b) Elimina filas donde "nombre" es nulo
   c) Rellena la edad nula con la edad promedio

5. Transforma y limpia:
   - Renombra la columna "nombre" a "name" y "pais" a "country"
   - Convierte todos los nombres a mayúsculas
   - Agrega una columna "es_senior" = True si edad >= 28
"""

import pandas as pd


datos = {
    "nombre":  ["Kevin", "Ana", "Luis", "Pedro", "María"],
    "edad":    [23, 25, 31, 28, 22],
    "pais":    ["México", "Colombia", "México", "Argentina", "Colombia"],
    "salario": [45000, 52000, 61000, 48000, 55000],
}

df_nulos = pd.DataFrame({
    "id":     [1, 2, 3, 4, 5],
    "nombre": ["Kevin", None, "Luis", "Pedro", None],
    "edad":   [23, 25, None, 28, 22],
    "email":  ["k@x.com", "a@x.com", None, "p@x.com", "m@x.com"],
})


# Ejercicio 1 — Tu código aquí


# Ejercicio 2 — Tu código aquí


# Ejercicio 3 — Tu código aquí


# Ejercicio 4 — Tu código aquí


# Ejercicio 5 — Tu código aquí
