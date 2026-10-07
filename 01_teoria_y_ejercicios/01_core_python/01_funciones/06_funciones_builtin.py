"""
Tema 1 — Funciones | Ejercicio 6: Funciones built-in
=============================================================================

TEORÍA:
    Python incluye funciones listas para usar sin importar nada.
    Las más útiles para Data Engineering:

    Numéricas:   abs(), round(), sum(), min(), max(), pow()
    Secuencias:  len(), sorted(), reversed(), enumerate(), zip(), range()
    Tipos:       int(), float(), str(), bool(), list(), dict(), tuple(), set()
    Entrada/sal: print(), input()
    Otros:       type(), isinstance(), id(), help()

EJERCICIOS:

1. Dada la lista: numeros = [4, -2, 7, 0, -5, 3, 9, -1]
   Usa funciones built-in para obtener:
   - El valor máximo
   - El valor mínimo
   - La suma de todos
   - El valor absoluto de cada negativo (sin un bucle explícito)
   - La lista ordenada de mayor a menor

2. Dada la lista: nombres = ["carlos", "ANA", "  Pedro  ", "luis"]
   Normaliza todos los nombres (sin espacios, primera letra mayúscula).
   Pista: usa map() o list comprehension con .strip().title()

3. Usa zip() para combinar estas dos listas en una lista de tuplas:

       claves = ["id", "nombre", "edad"]
       valores = [1, "Kevin", 23]

   Resultado esperado: [(''id'', 1), (''nombre'', ''Kevin''), (''edad'', 23)]
   Luego conviértelo a un diccionario con dict().

4. Usa enumerate() para imprimir cada elemento de una lista con su índice:

       frutas = ["manzana", "banana", "naranja"]

   Resultado esperado:
       0: manzana
       1: banana
       2: naranja

5. Usa isinstance() para crear una función `tipo_dato(valor)` que retorne
   "entero", "flotante", "texto", "lista", "diccionario" o "desconocido"
   según el tipo del valor recibido.

RETO INTEGRADOR:
6. Usa `zip()`, `enumerate()`, `sorted()` y `filter()` para procesar una lista de logs de eventos, filtrar fallos y retornar las 3 mayores latencias con sus índices.
"""

numeros = [4, -2, 7, 0, -5, 3, 9, -1]
nombres = ["carlos", "ANA", "  Pedro  ", "luis"]
claves  = ["id", "nombre", "edad"]
valores = [1, "Kevin", 23]
frutas  = ["manzana", "banana", "naranja"]


# Ejercicio 1 — Tu código aquí


# Ejercicio 2 — Tu código aquí


# Ejercicio 3 — Tu código aquí


# Ejercicio 4 — Tu código aquí


# Ejercicio 5 — Tu código aquí


# Reto Integrador — Tu código aquí
