"""
Reto 14 — Procesamiento incremental
======================================

Tienes un directorio con archivos CSV diarios:

    challenges/
    └── reto_14_procesamiento_incremental/
        ├── data/
        │   ├── 2026-08-17.csv    <- Archivo del día 1
        │   ├── 2026-08-18.csv    <- Archivo del día 2
        │   └── 2026-08-19.csv    <- Archivo del día 3
        ├── state/
        │   └── processed_files.json   <- Registro de archivos ya procesados
        ├── output/
        │   └── (archivos procesados)
        └── reto_14_procesamiento_incremental.py

Tu programa debe:
    1. Leer el estado actual desde state/processed_files.json
    2. Listar todos los archivos en data/
    3. Detectar cuáles aún NO han sido procesados
    4. Procesar sólo los archivos nuevos
    5. Actualizar state/processed_files.json con los archivos procesados

Formato de state/processed_files.json:

    {
        "processed": [
            {"file": "2026-08-17.csv", "processed_at": "2026-08-18T10:00:00"},
            {"file": "2026-08-18.csv", "processed_at": "2026-08-19T09:30:00"}
        ]
    }

Comportamiento esperado al ejecutar el programa dos veces:

    Primera ejecución:
        [INFO] Detectados 3 archivos nuevos
        [INFO] Procesando 2026-08-17.csv...
        [INFO] Procesando 2026-08-18.csv...
        [INFO] Procesando 2026-08-19.csv...

    Segunda ejecución:
        [INFO] Detectados 0 archivos nuevos. Nada que procesar.

Prácticas:
    - Idempotencia (concepto clave en pipelines de datos)
    - Estado persistido en JSON
    - Detección de archivos nuevos
    - datetime para timestamps
    - logging
    - os.listdir() / pathlib.Path

Nota: Idempotencia es uno de los conceptos más importantes en Data Engineering.
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# Tu código aquí
