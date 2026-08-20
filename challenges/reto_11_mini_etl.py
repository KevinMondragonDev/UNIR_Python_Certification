"""
Reto 11 — Mini ETL
====================

Construye un pipeline ETL (Extract → Transform → Load) simple pero completo.

Estructura de archivos:

    challenges/
    └── reto_11_mini_etl/
        ├── data/
        │   └── customers.csv      <- Archivo de entrada (datos crudos)
        ├── src/
        │   ├── extract.py         <- Módulo de extracción
        │   ├── transform.py       <- Módulo de transformación
        │   └── load.py            <- Módulo de carga
        ├── output/
        │   └── customers_clean.csv <- Resultado del pipeline
        └── main.py                <- Orquestador del pipeline

Flujo del pipeline:

    CSV (crudo)
        ↓
    Extract  — leer el CSV y retornar una lista de diccionarios
        ↓
    Transform — aplicar todas las transformaciones
        ↓
    Load     — guardar el resultado limpio en output/
        ↓
    CSV (limpio)

Transformaciones requeridas en transform.py:
    1. Eliminar registros duplicados (por id)
    2. Normalizar nombres: .strip().title()
    3. Validar edades: deben ser números entre 0 y 120
    4. Eliminar registros con campos vacíos o None

Ejemplo de datos de entrada (data/customers.csv):

    id,name,age
    1,kevin,23
    2, ANA ,25
    3,Luis,31
    2, ANA ,25
    4,,28
    5,Pedro,-5

Resultado esperado (output/customers_clean.csv):

    id,name,age
    1,Kevin,23
    2,Ana,25
    3,Luis,31

Prácticas:
    - Separación de responsabilidades (módulos independientes)
    - Funciones con type hints
    - Docstrings
    - Manejo de excepciones
    - Logging en cada etapa del pipeline
"""

# Este archivo es el punto de entrada — edita los módulos en src/
# y orquesta el pipeline desde aquí (main.py)

import sys
import os


# Tu código aquí (o ejecuta directamente main.py)
