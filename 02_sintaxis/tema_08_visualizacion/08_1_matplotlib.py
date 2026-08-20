"""
Tema 8 — Visualización de datos | Ejercicio 1: Matplotlib
===========================================================

TEORÍA:
    Matplotlib es la librería base de visualización en Python.
    La interfaz más común es pyplot:

        import matplotlib.pyplot as plt

    Estructura típica de una gráfica:
        plt.figure(figsize=(10, 6))      → crear figura con tamaño
        plt.plot(x, y, ...)              → agregar datos
        plt.title("Título")             → título
        plt.xlabel("Eje X")             → etiqueta eje X
        plt.ylabel("Eje Y")             → etiqueta eje Y
        plt.legend()                    → leyenda
        plt.grid(True)                  → cuadrícula
        plt.tight_layout()              → ajuste automático
        plt.savefig("grafica.png")      → guardar imagen
        plt.show()                      → mostrar

    Tipos de gráficas:
        plt.plot()      → línea (series de tiempo)
        plt.bar()       → barras (categorías)
        plt.barh()      → barras horizontales
        plt.scatter()   → dispersión (correlaciones)
        plt.hist()      → histograma (distribuciones)
        plt.pie()       → pastel (proporciones)
        plt.boxplot()   → caja (distribución y outliers)

    pip install matplotlib

EJERCICIOS:

    Guarda todas las gráficas en la carpeta output/

1. Gráfica de línea — Temperatura semanal:
   Crea una gráfica de línea con estas temperaturas (°C) de lunes a domingo:
   [22.5, 19.0, 25.3, 21.7, 28.1, 17.4, 23.9]
   - Título: "Temperatura semanal"
   - Eje X: días de la semana
   - Eje Y: temperatura (°C)
   - Agrega marcadores en cada punto (marker='o')
   - Guarda como output/temperatura_semanal.png

2. Gráfica de barras — Salario por departamento:
   Visualiza estos datos:

       departamentos = ["Ingeniería", "Datos", "Marketing"]
       salarios_prom = [55333, 64500, 40000]

   - Barras de colores distintos
   - Muestra el valor encima de cada barra
   - Título: "Salario promedio por departamento"
   - Guarda como output/salarios_departamento.png

3. Histograma — Distribución de edades:
   Genera 200 edades aleatorias (distribución normal, media=35, std=8).
   Crea un histograma con 15 bins.
   - Título: "Distribución de edades"
   - Agrega una línea vertical en la media (color rojo, linestyle='--')
   - Guarda como output/distribucion_edades.png

4. Scatter plot — Salario vs Experiencia:
   Genera datos sintéticos de 50 empleados:
   - experiencia: números aleatorios entre 1 y 15 años
   - salario: experiencia * 3000 + ruido aleatorio
   Crea un scatter plot y agrega una línea de tendencia.
   Guarda como output/salario_vs_experiencia.png

5. Subplots — Dashboard con 4 gráficas:
   Crea una figura con 2x2 subplots que incluya las 4 gráficas anteriores
   en un solo archivo output/dashboard.png (figsize=(16, 12)).
"""

import matplotlib.pyplot as plt
import numpy as np
import os

os.makedirs("output", exist_ok=True)

dias = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
temperaturas = [22.5, 19.0, 25.3, 21.7, 28.1, 17.4, 23.9]

departamentos = ["Ingeniería", "Datos", "Marketing"]
salarios_prom = [55333, 64500, 40000]


# Ejercicio 1 — Tu código aquí


# Ejercicio 2 — Tu código aquí


# Ejercicio 3 — Tu código aquí


# Ejercicio 4 — Tu código aquí


# Ejercicio 5 — Tu código aquí
