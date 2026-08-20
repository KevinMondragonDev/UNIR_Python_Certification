"""
Tema 6 — Aspectos avanzados | Ejercicio 1: Expresiones regulares (regex)
=========================================================================

TEORÍA:
    Las expresiones regulares son patrones para buscar y manipular texto.
    Se usan con el módulo `re`.

    Funciones principales:
        re.match(patron, texto)   → busca al inicio del string
        re.search(patron, texto)  → busca en cualquier parte
        re.findall(patron, texto) → retorna lista de todas las coincidencias
        re.sub(patron, reemplazo, texto) → sustituye coincidencias

    Metacaracteres comunes:
        .   → cualquier carácter
        *   → 0 o más repeticiones
        +   → 1 o más repeticiones
        ?   → 0 o 1 repetición
        ^   → inicio del string
        $   → fin del string
        []  → conjunto de caracteres. Ej: [a-z], [0-9]
        \d  → dígito (equivale a [0-9])
        \w  → carácter de palabra (letras, dígitos, _)
        \s  → espacio en blanco
        {n} → exactamente n repeticiones

EJERCICIOS:

1. Valida si los siguientes strings son emails válidos (formato: algo@algo.algo):

       emails = [
           "kevin@example.com",
           "not-an-email",
           "ana@empresa.mx",
           "@sin-usuario.com",
           "sin-dominio@",
       ]

2. Extrae todos los números de este string:

       texto = "En 2026 procesamos 15,000 registros con 3 pipelines en 8 horas."

3. Limpia este texto reemplazando múltiples espacios por uno solo
   y eliminando caracteres especiales (deja solo letras, números y espacios):

       texto_sucio = "python  --  data   ##engineering!!   2026"

4. Extrae todas las fechas en formato YYYY-MM-DD de este log:

       log = \"\"\"
       2026-08-17 INFO  Pipeline started
       2026-08-18 ERROR Connection failed
       2026-08-19 INFO  Pipeline completed
       \"\"\"

5. Crea una función `es_password_seguro(password)` que retorne True si:
   - Tiene al menos 8 caracteres
   - Contiene al menos una letra mayúscula
   - Contiene al menos un dígito
   - Contiene al menos un carácter especial (@, #, $, %, !)
"""

import re


emails = [
    "kevin@example.com",
    "not-an-email",
    "ana@empresa.mx",
    "@sin-usuario.com",
    "sin-dominio@",
]

texto_numeros = "En 2026 procesamos 15,000 registros con 3 pipelines en 8 horas."

texto_sucio = "python  --  data   ##engineering!!   2026"

log = """
2026-08-17 INFO  Pipeline started
2026-08-18 ERROR Connection failed
2026-08-19 INFO  Pipeline completed
"""


# Ejercicio 1 — Tu código aquí


# Ejercicio 2 — Tu código aquí


# Ejercicio 3 — Tu código aquí


# Ejercicio 4 — Tu código aquí


# Ejercicio 5 — Tu código aquí
