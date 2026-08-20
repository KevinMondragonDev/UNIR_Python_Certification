"""
Reto 16 — S3 Data Validator
==============================

Construye un validador automático de archivos nuevos en S3.

Flujo:

    s3://bucket/raw/
          ↓
       Validator
          ↓
    ┌─────┴──────┐
    ↓            ↓
 valid/       invalid/

Tu programa debe:
    1. Listar todos los archivos en s3://bucket/raw/
    2. Descargar cada archivo
    3. Validar su contenido (columnas, tipos, nulls, duplicados)
    4. Si pasa la validación → copiar a s3://bucket/valid/
    5. Si falla la validación  → copiar a s3://bucket/invalid/
    6. Registrar todos los errores en logs/validation.log

Reglas de validación:
    - El archivo debe ser un CSV o Parquet válido
    - Debe contener las columnas: ["id", "name", "age", "email"]
    - No debe haber IDs duplicados
    - La edad debe estar entre 0 y 120
    - El email no debe estar vacío

Estructura de logs/validation.log:

    2026-08-19 10:00:01 [INFO]  Procesando: customers_001.csv
    2026-08-19 10:00:02 [INFO]  customers_001.csv → VÁLIDO → copiado a valid/
    2026-08-19 10:00:03 [INFO]  Procesando: customers_002.csv
    2026-08-19 10:00:04 [ERROR] customers_002.csv → INVÁLIDO: columna 'email' faltante
    2026-08-19 10:00:04 [INFO]  customers_002.csv → copiado a invalid/

Prácticas:
    - boto3 (S3 operations)
    - s3.copy_object() para mover archivos sin descargarlos
    - logging a archivo (FileHandler)
    - Validación reutilizable (puedes importar del Reto 13)
    - Separación de responsabilidades

Dependencias:

    pip install boto3 pandas pyarrow
"""

import logging
import os

import boto3

# Configurar logging a archivo
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/validation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# Tu código aquí
