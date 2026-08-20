"""
Tema 4 — Funciones | Ejercicio 7: Funciones anónimas (lambda)
===============================================================

TEORÍA:
    Una función lambda es una función pequeña, de una sola expresión,
    sin nombre (anónima). Se usa en lugar de definir una función completa
    cuando la lógica es simple y se usa en un solo lugar.

    Sintaxis:
        lambda parámetros: expresión

    Equivalencia:
        # Con def:
        def doble(x):
            return x * 2

        # Con lambda:
        doble = lambda x: x * 2

    Uso más común — como argumento de sorted(), map(), filter():

        numeros = [3, 1, 4, 1, 5]
        sorted(numeros, key=lambda x: -x)   # orden descendente

        palabras = ["hola", "adiós", "mundo"]
        sorted(palabras, key=lambda s: len(s))  # orden por longitud

EJERCICIOS:

1. Crea una lambda que reciba un número y retorne su cuadrado.
   Asígnala a una variable y llámala con el número 7.

2. Ordena esta lista de diccionarios por edad (de menor a mayor):

       usuarios = [
           {"nombre": "Kevin", "edad": 23},
           {"nombre": "Ana",   "edad": 17},
           {"nombre": "Luis",  "edad": 31},
       ]

3. Usa filter() con una lambda para obtener solo los números pares de:

       numeros = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

4. Usa map() con una lambda para convertir esta lista de precios a MXN
   (supon que 1 USD = 17.5 MXN):

       precios_usd = [10.0, 25.5, 100.0, 3.99]

5. ¿Cuándo NO usar lambda? Escribe un ejemplo donde una lambda sería
   mala práctica y refactorízalo con `def`. Explica en comentario por qué.
"""

usuarios    = [{"nombre": "Kevin", "edad": 23}, {"nombre": "Ana", "edad": 17}, {"nombre": "Luis", "edad": 31}]
numeros     = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
precios_usd = [10.0, 25.5, 100.0, 3.99]


# Ejercicio 1 — Tu código aquí


# Ejercicio 2 — Tu código aquí


# Ejercicio 3 — Tu código aquí


# Ejercicio 4 — Tu código aquí


# Ejercicio 5 — Tu código aquí
