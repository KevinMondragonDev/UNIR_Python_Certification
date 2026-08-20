"""
Reto 4 — Limpieza de datos
============================

Dado:

    names = [
        " kevin ",
        "ANA",
        " Luis",
        "",
        "  Pedro",
        None
    ]

Devuelve:

    ["Kevin", "Ana", "Luis", "Pedro"]

Reglas de limpieza:
    1. Eliminar espacios al inicio y al final (.strip())
    2. Normalizar capitalización (.title() o .lower() + .title())
    3. Eliminar strings vacíos ("")
    4. Eliminar valores None

Prácticas:
    - None
    - .strip()
    - .lower()
    - .title()
    - Validaciones

Nota: Este reto empieza a parecerse a Data Engineering real.
"""

names = [
    " kevin ",
    "ANA",
    " Luis",
    "",
    "  Pedro",
    None
]


# Tu código aquí
