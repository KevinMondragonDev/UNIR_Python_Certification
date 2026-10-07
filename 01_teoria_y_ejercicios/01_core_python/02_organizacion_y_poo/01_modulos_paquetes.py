"""
Tema 2 — Organización del código y POO | Ejercicio 1: Módulos y paquetes
====================================================================

TEORÍA:
    MÓDULO: Un archivo .py es un módulo. Se importa con `import`.
    PAQUETE: Una carpeta con __init__.py es un paquete.

    Formas de importar:
        import math                    # importa el módulo completo
        from math import sqrt          # importa solo una función
        from math import sqrt as raiz  # con alias
        from math import *             # importa todo (NO recomendado)

    Módulos de la biblioteca estándar más útiles:
        math, random, os, sys, datetime, json, csv, re, collections, itertools

    Para crear tu propio módulo:
        # archivo: mi_modulo.py
        def mi_funcion():
            pass

        # en otro archivo:
        import mi_modulo
        mi_modulo.mi_funcion()

EJERCICIOS:

1. Usa el módulo `math` para:
   - Calcular la raíz cuadrada de 144
   - Calcular el valor de π
   - Calcular log10 de 1000
   - Redondear 3.7 hacia arriba (ceil) y hacia abajo (floor)

2. Usa el módulo `random` para:
   - Generar un número entero aleatorio entre 1 y 100
   - Seleccionar un elemento aleatorio de esta lista: ["python", "spark", "sql", "pandas"]
   - Mezclar la lista anterior aleatoriamente (shuffle)

3. Usa el módulo `datetime` para:
   - Obtener la fecha y hora actual
   - Calcular cuántos días faltan para el 31 de diciembre de 2026
   - Formatear la fecha actual como "dd/mm/yyyy"

4. Usa el módulo `collections` para contar la frecuencia de cada letra en:

       texto = "data engineering python"

   Usa Counter y muestra las 5 letras más frecuentes.

5. Usa el módulo `os` para:
   - Obtener el directorio de trabajo actual
   - Listar los archivos del directorio actual
   - Verificar si existe una carpeta llamada "output" y crearla si no existe

RETO INTEGRADOR:
6. Crea un script de procesamiento que utilice los módulos `datetime`, `collections.Counter`, `json` y `os` para parsear una cadena JSON de eventos, contar la frecuencia de cada estado y medir días transcurridos.
"""

import math
import random
from datetime import datetime, date
from collections import Counter
import os


# Ejercicio 1 — Tu código aquí


# Ejercicio 2 — Tu código aquí


# Ejercicio 3 — Tu código aquí


# Ejercicio 4 — Tu código aquí


# Ejercicio 5 — Tu código aquí


# Reto Integrador — Tu código aquí
