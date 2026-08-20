"""
Reto 9 — Sistema de configuración
===================================

Estructura de archivos:

    challenges/
    └── reto_09_sistema_configuracion/
        ├── config/
        │   ├── dev.yaml    <- Configuración para entorno de desarrollo
        │   └── prod.yaml   <- Configuración para entorno de producción
        └── reto_09_sistema_configuracion.py

Contenido de config/dev.yaml:

    environment: dev
    bucket: my-dev-bucket
    max_retries: 3
    log_level: DEBUG

Contenido de config/prod.yaml:

    environment: prod
    bucket: my-prod-bucket
    max_retries: 5
    log_level: WARNING

Tu programa debe implementar una función:

    def load_config(config_path: str) -> dict:
        ...

Que permita:

    config = load_config("config/dev.yaml")
    print(config["bucket"])       # my-dev-bucket
    print(config["max_retries"])  # 3
    print(config["environment"])  # dev

Bonus:
    - Crea una función get_config(environment: str) -> dict que reciba
      "dev" o "prod" y cargue el archivo correspondiente automáticamente.
    - Maneja el caso en que el archivo no exista con una excepción clara.

Dependencia requerida:

    pip install pyyaml

Prácticas:
    - Módulos
    - Archivos
    - YAML
    - Funciones
    - Configuración externa
    - Manejo de excepciones
"""

import yaml
import os


# Tu código aquí
