"""
Reto 6 — Validador de CSV
==========================

El CSV de entrada debe tener las columnas:

    id, name, age, email

Tu programa debe detectar registros con:
    - Columnas faltantes
    - Edades inválidas (no numéricas, negativas o mayores a 120)
    - Emails vacíos o sin formato válido
    - IDs duplicados

Estructura de archivos:

    challenges/
    └── reto_06_validador_csv/
        ├── data/
        │   └── users_dirty.csv   <- CSV con datos mixtos (válidos e inválidos)
        ├── output/
        │   ├── valid_records.csv   <- Registros que pasaron la validación
        │   ├── invalid_records.csv <- Registros que fallaron la validación
        │   └── errors.log          <- Detalle de cada error encontrado
        └── reto_06_validador_csv.py

Formato de errors.log (ejemplo):

    [ERROR] Fila 2: email vacío — {id: 2, name: Ana, age: 25, email: ""}
    [ERROR] Fila 4: ID duplicado — id=1
    [ERROR] Fila 5: edad inválida — age=-5

Prácticas:
    - Validación de datos
    - Separación de registros válidos e inválidos
    - Logging de errores
    - Manejo de excepciones

Nota: Este reto es muy bueno para un perfil de Data Engineering.
"""

import csv
import os


# Tu código aquí
