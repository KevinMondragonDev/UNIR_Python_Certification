"""
Tema 4 — Análisis de datos | Ejercicio 5: Manejo de archivos CSV
======================================================================

TEORÍA:
    Pandas simplifica enormemente la lectura y escritura de archivos CSV.

    Lectura:
        df = pd.read_csv("archivo.csv")
        df = pd.read_csv("archivo.csv", sep=";", encoding="utf-8")
        df = pd.read_csv("archivo.csv", usecols=["id", "nombre"])
        df = pd.read_csv("archivo.csv", dtype={"id": int, "edad": float})
        df = pd.read_csv("archivo.csv", na_values=["N/A", "NULL", "-"])

    Escritura:
        df.to_csv("salida.csv", index=False)
        df.to_csv("salida.csv", index=False, encoding="utf-8-sig")  # para Excel en español

    Inspección rápida:
        df.head()     → primeras 5 filas
        df.tail()     → últimas 5 filas
        df.shape      → (filas, columnas)
        df.columns    → nombre de columnas
        df.dtypes     → tipo de cada columna

    pip install pandas

EJERCICIOS:

    ANTES DE EMPEZAR: Crea manualmente el archivo data/employees.csv con este contenido:

    id,nombre,departamento,salario,fecha_ingreso
    1,Kevin Mondragón,Ingeniería,52000,2024-03-15
    2,Ana García,Datos,61000,2023-07-01
    3,Luis Torres,Ingeniería,55000,2024-01-20
    4,Pedro Sánchez,Marketing,42000,2022-11-05
    5,María López,Datos,68000,2021-09-30
    6,Carlos Ruiz,Ingeniería,59000,2023-04-12
    7,,Marketing,38000,2023-08-01
    8,Sandra Flores,Datos,,2024-06-15

1. Lee el archivo data/employees.csv y muestra:
   - Las primeras 5 filas
   - El shape del DataFrame
   - Los tipos de dato de cada columna
   - Cuántos nulos hay por columna

2. Limpia el DataFrame:
   a) Elimina filas donde "nombre" está vacío
   b) Rellena salarios nulos con el salario promedio del departamento
   c) Convierte "fecha_ingreso" a tipo datetime

3. Enriquece el DataFrame:
   a) Agrega columna "antiguedad_dias" (días desde fecha_ingreso hasta hoy)
   b) Agrega columna "nivel" según salario:
      - "Junior"  si salario < 50,000
      - "Mid"     si 50,000 <= salario < 60,000
      - "Senior"  si salario >= 60,000

4. Genera reportes:
   a) Guarda el DataFrame limpio en output/employees_clean.csv (sin índice)
   b) Guarda solo los empleados de Ingeniería en output/engineering_team.csv
   c) Guarda un resumen por departamento en output/department_summary.csv:
      columnas: departamento, total_empleados, salario_promedio, salario_max

5. Lee el archivo output/employees_clean.csv que acabas de crear
   y verifica que tiene exactamente las mismas filas que el DataFrame en memoria.
   Imprime "✓ Verificación exitosa" o "✗ Error: diferente número de filas".

RETO INTEGRADOR:
6. Utiliza `csv.DictReader` y `csv.DictWriter` para leer un archivo CSV de transacciones, aplicar transformaciones y filtrado de datos, y escribir un nuevo archivo CSV formateado.
"""

import pandas as pd
import os
from datetime import date

os.makedirs("data",   exist_ok=True)
os.makedirs("output", exist_ok=True)


# Ejercicio 1 — Leer el CSV y explorar


# Ejercicio 2 — Limpiar


# Ejercicio 3 — Enriquecer


# Ejercicio 4 — Guardar reportes


# Ejercicio 5 — Verificar


# Reto Integrador — Tu código aquí
