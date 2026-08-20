"""
Reto 12 — ETL con JSON
========================

Similar al Reto 11, pero el formato de entrada y salida es JSON.

Estructura de archivos:

    challenges/
    └── reto_12_etl_json/
        ├── data/
        │   └── customers.json      <- Archivo JSON de entrada
        ├── output/
        │   └── customers_clean.json <- JSON transformado y limpio
        │   └── stats.json           <- Estadísticas del proceso
        └── reto_12_etl_json.py

Contenido de data/customers.json:

    [
        {"id": 1, "name": "kevin",  "age": 23, "email": "kevin@example.com"},
        {"id": 2, "name": " ANA ",  "age": 25, "email": ""},
        {"id": 3, "name": "Luis",   "age": 31, "email": "luis@example.com"},
        {"id": 2, "name": " ANA ",  "age": 25, "email": ""},
        {"id": 4, "name": null,     "age": -1, "email": null}
    ]

Pipeline:

    JSON (crudo)
        ↓
    Cargar JSON con json.load()
        ↓
    Transformar (limpiar, validar, deduplicar)
        ↓
    Guardar JSON limpio
        ↓
    Guardar estadísticas del proceso

Contenido de output/stats.json (ejemplo):

    {
        "total_input":    5,
        "total_valid":    2,
        "total_invalid":  3,
        "duplicates":     1,
        "null_records":   1,
        "invalid_emails": 1
    }

Prácticas:
    - json.load() / json.dump()
    - Validación de datos
    - Logging con módulo logging
    - Manejo de excepciones (FileNotFoundError, json.JSONDecodeError)
    - Estadísticas del proceso (muy común en pipelines reales)
"""

import json
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# Tu código aquí
