"""
Reto 15 — S3 File Processor
==============================

Usando boto3, construye un pipeline que procese archivos Parquet desde S3.

Flujo:

    S3 (raw/)
        ↓
    Listar archivos .parquet
        ↓
    Descargar y procesar cada archivo
        ↓
    Generar resultado
        ↓
    Subir a S3 (processed/)

Estructura de prefijos en S3:

    s3://mi-bucket/
    ├── raw/
    │   └── customers/
    │       └── customers_2026-08-19.parquet
    └── processed/
        └── customers/
            └── customers_2026-08-19_processed.parquet

Tu programa debe implementar:

    def list_parquet_files(bucket: str, prefix: str) -> list[str]:
        \"\"\"Lista todos los archivos .parquet en un prefijo de S3.\"\"\"
        ...

    def download_file(bucket: str, key: str, local_path: str) -> None:
        \"\"\"Descarga un archivo de S3 al sistema local.\"\"\"
        ...

    def process_file(local_path: str) -> str:
        \"\"\"Procesa el archivo localmente y retorna la ruta del resultado.\"\"\"
        ...

    def upload_file(local_path: str, bucket: str, key: str) -> None:
        \"\"\"Sube un archivo local a S3.\"\"\"
        ...

Configuración requerida:
    - Configura tus credenciales AWS con: aws configure
    - O usando variables de entorno: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

Dependencia:

    pip install boto3 pandas pyarrow

Prácticas:
    - boto3 (S3 client)
    - s3.list_objects_v2()
    - s3.download_file() / s3.upload_file()
    - pandas para procesar Parquet
    - Manejo de excepciones de AWS (botocore.exceptions)
    - logging

Nota: Para testear localmente puedes usar LocalStack o Minio como S3 mock.
"""

import logging
import os
import tempfile

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# Tu código aquí
