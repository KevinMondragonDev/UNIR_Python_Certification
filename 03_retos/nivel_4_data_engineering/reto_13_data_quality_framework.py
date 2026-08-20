"""
Reto 13 — Data Quality Framework
===================================

Construye una función general de validación de datos:

    def validate_data(data: list[dict]) -> dict:
        ...

Que revise:

    ✓ Nulls          — campos con valor None o string vacío ""
    ✓ Duplicates     — registros completamente duplicados o por id
    ✓ Data types     — edades deben ser int/float, emails deben ser string
    ✓ Required cols  — los campos ["id", "name", "age", "email"] deben existir
    ✓ Invalid values — edades negativas o > 120, emails sin "@"

Resultado esperado:

    {
        "valid": False,
        "total_records":     1000,
        "null_records":        15,
        "duplicate_records":    8,
        "type_errors":          3,
        "invalid_values":      12,
        "missing_columns":     [],
        "errors": [
            {"row": 4,  "field": "age",   "issue": "valor negativo: -5"},
            {"row": 7,  "field": "email", "issue": "formato inválido: 'not-an-email'"},
            {"row": 12, "field": "name",  "issue": "valor nulo"},
            ...
        ]
    }

Datos de prueba para validar tu función:

    data = [
        {"id": 1, "name": "Kevin", "age": 23,  "email": "kevin@example.com"},
        {"id": 2, "name": "Ana",   "age": -5,  "email": "ana@example.com"},
        {"id": 3, "name": None,    "age": 31,  "email": "luis@example.com"},
        {"id": 4, "name": "Pedro", "age": 28,  "email": "not-an-email"},
        {"id": 2, "name": "Ana",   "age": -5,  "email": "ana@example.com"},  # duplicado
        {"id": 5, "name": "María", "age": 200, "email": ""},
    ]

Prácticas:
    - Funciones con type hints y docstrings
    - Validación exhaustiva de datos
    - Construcción de reportes estructurados
    - Lógica condicional y bucles
    - Manejo de None

Nota: Este reto es muy cercano al trabajo real de un Data Engineer.
"""

from typing import Any


# Tu código aquí
