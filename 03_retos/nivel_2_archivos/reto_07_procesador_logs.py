"""
Reto 7 — Procesador de logs
=============================

Estructura de archivos:

    challenges/
    └── reto_07_procesador_logs/
        ├── logs/
        │   └── app.log            <- Archivo de log de entrada
        ├── output/
        │   └── error_report.txt   <- Reporte de errores generado
        └── reto_07_procesador_logs.py

Contenido de logs/app.log:

    2026-08-19 INFO  User logged in
    2026-08-19 ERROR Database connection failed
    2026-08-19 INFO  User logged out
    2026-08-19 ERROR Timeout

Tu programa debe:
    1. Leer el archivo de log
    2. Contar cuántas líneas hay por cada nivel (INFO, ERROR, WARNING, etc.)
    3. Imprimir el conteo:

        INFO:  2
        ERROR: 2

    4. Generar output/error_report.txt con sólo las líneas de nivel ERROR

Prácticas:
    - Lectura de archivos de texto
    - Diccionarios para conteo
    - Filtrado de líneas
    - Escritura de archivos
    - with open()
"""

import os


# Tu código aquí
