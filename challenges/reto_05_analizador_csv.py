"""
Reto 5 — Analizador de CSV
============================

Estructura de archivos:

    challenges/
    └── reto_05_analizador_csv/
        ├── data/
        │   └── users.csv       <- Archivo de entrada
        ├── output/
        │   └── summary.txt     <- Archivo de salida generado por tu programa
        └── reto_05_analizador_csv.py

Contenido de data/users.csv:

    id,name,age
    1,Kevin,23
    2,Ana,25
    3,Luis,31

Tu programa debe:
    1. Leer el archivo CSV
    2. Contar el número de usuarios
    3. Calcular la edad promedio
    4. Encontrar al usuario de mayor edad
    5. Guardar un resumen en output/summary.txt

Formato del resumen (ejemplo):

    Total usuarios: 3
    Edad promedio: 26.33
    Usuario mayor: Luis (31 años)

Prácticas:
    - Estructura de carpetas
    - Lectura y escritura de archivos
    - with open()
    - Funciones
    - Excepciones (maneja el caso en que el archivo no exista)
"""

import csv
import os


# Tu código aquí
