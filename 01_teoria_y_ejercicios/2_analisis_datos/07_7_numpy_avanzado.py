"""
Tema 7 — Análisis de datos | Ejercicio 7: NumPy avanzado — Estadística y Álgebra
==================================================================================

TEORÍA:
    NumPy es mucho más que arrays básicos. Para Data Engineering es clave entender:

    ESTADÍSTICA con NumPy:
        np.mean(), np.median(), np.std(), np.var()
        np.percentile(arr, [25, 50, 75])   → cuartiles
        np.corrcoef(x, y)                  → correlación de Pearson
        np.histogram(arr, bins=10)         → distribución de frecuencias
        np.random.*                        → distribuciones: normal, uniform, poisson, etc.

    ÁLGEBRA LINEAL (np.linalg):
        np.dot(A, B)           → producto de matrices
        np.linalg.inv(A)       → matriz inversa
        np.linalg.det(A)       → determinante
        np.linalg.eig(A)       → valores y vectores propios
        np.linalg.norm(v)      → norma (magnitud) del vector

    INDEXACIÓN AVANZADA:
        arr[arr > 5]           → filtrado booleano
        np.where(cond, a, b)   → condicional vectorizado (como IF en SQL)
        np.argsort(arr)        → índices que ordenarían el array
        np.unique(arr, return_counts=True) → valores únicos con frecuencia

    BROADCASTING:
        Operar arrays de distintas formas automáticamente:
        arr_2d - arr_1d        → resta a cada fila

    pip install numpy

EJERCICIOS:

1. ESTADÍSTICA DESCRIPTIVA:
   Dado el array de salarios, calcula SIN usar pandas:
   - Media, mediana, desviación estándar, varianza
   - Percentiles 25, 50, 75 (Q1, Q2, Q3)
   - IQR y límites de outliers (Q1 - 1.5*IQR, Q3 + 1.5*IQR)
   - ¿Cuántos salarios son outliers?

2. SIMULACIÓN MONTE CARLO:
   Simula 10,000 lanzamientos de 2 dados y calcula:
   - Frecuencia de cada suma posible (2 a 12)
   - ¿Cuál es la suma más probable? (debería ser 7)
   - ¿Qué probabilidad hay de sacar 12 (doble 6)?

3. NP.WHERE — CATEGORIZACIÓN VECTORIZADA:
   Dado el array de edades, crea un array de categorías SIN bucles:
   - "Menor"   si edad < 18
   - "Adulto"  si 18 <= edad < 65
   - "Senior"  si edad >= 65

   Luego cuenta cuántos hay en cada categoría.

4. CORRELACIÓN:
   Genera datos simulados de 100 casas con:
   - metros_cuadrados: entre 50 y 300
   - precio: metros * 15000 + ruido normal
   - habitaciones: metros // 30 + ruido entero
   Calcula la matriz de correlación entre las 3 variables.
   ¿Cuál par tiene mayor correlación?

5. BROADCASTING — NORMALIZACIÓN:
   Dada la matriz de métricas (cada columna es una variable distinta),
   normaliza cada columna a rango [0, 1] usando la fórmula:
       valor_normalizado = (x - min) / (max - min)
   Hazlo SIN bucles (broadcasting puro).
   Verifica que el mínimo de cada columna normalizada sea 0 y el máximo sea 1.
"""

import numpy as np

np.random.seed(42)

salarios = np.array([
    32000, 45000, 52000, 61000, 38000, 55000, 70000, 48000, 42000, 150000,
    35000, 58000, 63000, 41000, 29000, 68000, 180000, 53000, 47000, 66000,
])

edades = np.random.randint(10, 80, size=200)

metricas = np.random.rand(100, 5) * np.array([1000, 50, 1, 500, 100])


# Ejercicio 1 — ESTADÍSTICA


# Ejercicio 2 — MONTE CARLO


# Ejercicio 3 — NP.WHERE


# Ejercicio 4 — CORRELACIÓN


# Ejercicio 5 — BROADCASTING
