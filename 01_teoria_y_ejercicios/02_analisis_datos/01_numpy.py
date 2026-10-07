"""
Tema 4 — Análisis de datos | Ejercicio 1: NumPy básico
=================================================

TEORÍA:
    NumPy (Numerical Python) es la librería base para computación numérica.
    Su estructura principal es el ndarray (arreglo N-dimensional).

    Ventajas sobre listas de Python:
    - Operaciones vectorizadas (mucho más rápidas)
    - Menos memoria
    - Funciones matemáticas optimizadas

    Conceptos clave:
        np.array()        → crear arreglo
        arr.shape         → dimensiones (filas, columnas)
        arr.dtype         → tipo de dato
        arr.ndim          → número de dimensiones
        arr[fila, col]    → indexación
        arr[1:3, 0:2]     → slicing

    Operaciones vectorizadas (sin bucles):
        arr * 2           → multiplica cada elemento por 2
        arr + arr2        → suma elemento a elemento
        arr > 5           → máscara booleana

    pip install numpy

EJERCICIOS:

1. Crea los siguientes arrays y muestra su shape, dtype y ndim:
   a) Un array 1D con los números del 1 al 10
   b) Una matriz 3x3 con ceros
   c) Una matriz identidad 4x4
   d) Un array de 5 números aleatorios entre 0 y 1

2. Dada esta matriz, obtén:

       matriz = np.array([
           [1,  2,  3,  4],
           [5,  6,  7,  8],
           [9, 10, 11, 12],
       ])

   a) La segunda fila completa
   b) La tercera columna completa
   c) El subarray de las primeras 2 filas y últimas 2 columnas
   d) Todos los elementos mayores a 6 (máscara booleana)

3. Dado el array de temperaturas diarias (°C):

       temps = np.array([22.5, 19.0, 25.3, 21.7, 28.1, 17.4, 23.9])

   Calcula: media, mediana, desviación estándar, mínimo y máximo.
   ¿Cuántos días superaron los 23°C?

4. Simula 1,000 lanzamientos de un dado (valores 1-6) con np.random.randint.
   Calcula la frecuencia de cada valor y verifica que cada uno aparezca
   aproximadamente el 16.7% del tiempo.

5. Compara el rendimiento de NumPy vs Python puro:
   - Suma de cuadrados de un array de 1,000,000 elementos
   - Con lista Python y bucle for
   - Con NumPy vectorizado
   Mide el tiempo con `time` y muestra cuánto más rápido es NumPy.

    pip install numpy

RETO INTEGRADOR:
8. Crea una matriz 2D de NumPy (10x5) con lecturas de sensores, calcula la media y desviación estándar por columna, y filtra las filas cuyos valores superen 2 desviaciones estándar (máscaras booleanas).
"""

import numpy as np
import time


# Ejercicio 1 — Tu código aquí


# Ejercicio 2 — Tu código aquí


# Ejercicio 3 — Tu código aquí


# Ejercicio 4 — Tu código aquí


# Ejercicio 5 — Tu código aquí


# Reto Integrador — Tu código aquí
