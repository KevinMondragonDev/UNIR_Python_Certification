"""
Tema 3 — Python Avanzado | Ejercicio 3: Comprensión de listas
===================================================================================

TEORÍA:
    Las comprehensions son una forma compacta y legible de crear colecciones.
    Son más rápidas que los bucles for equivalentes.

    LIST comprehension:
        [expresión for elemento in iterable if condición]

    DICT comprehension:
        {clave: valor for elemento in iterable if condición}

    SET comprehension:
        {expresión for elemento in iterable}

    GENERATOR expression (perezosa, no crea lista en memoria):
        (expresión for elemento in iterable)

    Ejemplos:
        cuadrados  = [x**2 for x in range(10)]
        pares      = [x for x in range(20) if x % 2 == 0]
        longitudes = {palabra: len(palabra) for palabra in ["hola", "mundo"]}

EJERCICIOS:

1. Reescribe estos bucles como list comprehensions:

       # Versión con for:
       cuadrados = []
       for x in range(1, 11):
           cuadrados.append(x ** 2)

       # Versión con for:
       mayusculas = []
       for nombre in ["kevin", "ana", "luis"]:
           mayusculas.append(nombre.upper())

       # Versión con for:
       adultos = []
       for usuario in usuarios:
           if usuario["age"] >= 18:
               adultos.append(usuario["name"])

2. Usando dict comprehension, crea un diccionario que mapee
   cada palabra a su longitud:

       palabras = ["python", "data", "engineering", "spark"]
       # Resultado: {"python": 6, "data": 4, "engineering": 11, "spark": 5}

3. Usa una comprehension anidada para aplanar esta lista de listas:

       matriz = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
       # Resultado: [1, 2, 3, 4, 5, 6, 7, 8, 9]

4. Usa set comprehension para obtener las iniciales únicas de estos nombres:

       nombres = ["Kevin", "Ana", "Luis", "Karla", "Alberto", "Laura"]
       # Resultado: {"K", "A", "L"} (orden no garantizado)

5. Compara el rendimiento de for vs comprehension vs generator:
   - Crea la suma de cuadrados de 1 a 1,000,000
   - Con un bucle for
   - Con list comprehension + sum()
   - Con generator expression + sum()
   - Usa el módulo `time` para medir cada uno y comparar

RETO INTEGRADOR:
4. Usando List Comprehensions y Dict Comprehensions compuestas con condiciones anidadas, transforma una lista de diccionarios de transacciones en un diccionario resumido por categoría.
"""

import time

usuarios = [
    {"name": "Kevin", "age": 23},
    {"name": "Ana",   "age": 17},
    {"name": "Luis",  "age": 31},
]
palabras = ["python", "data", "engineering", "spark"]
matriz   = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
nombres  = ["Kevin", "Ana", "Luis", "Karla", "Alberto", "Laura"]


# Ejercicio 1 — Tu código aquí


# Ejercicio 2 — Tu código aquí


# Ejercicio 3 — Tu código aquí


# Ejercicio 4 — Tu código aquí


# Ejercicio 5 — Tu código aquí


# Reto Integrador — Tu código aquí
