"""
Tema 8 — Visualización de datos | Ejercicio 2: Plotly
=======================================================

TEORÍA:
    Plotly genera gráficas interactivas (hover, zoom, filtros) en HTML.
    Es ideal para dashboards y reportes web.

    Dos interfaces:
    1. plotly.express (px) → alto nivel, rápido de usar
    2. plotly.graph_objects (go) → bajo nivel, más control

    Uso típico con express:
        import plotly.express as px

        fig = px.bar(df, x="categoria", y="valor", title="Mi Gráfica")
        fig.update_layout(template="plotly_dark")
        fig.write_html("output/grafica.html")
        fig.show()

    Tipos de gráficas en px:
        px.line()       → línea
        px.bar()        → barras
        px.scatter()    → dispersión
        px.histogram()  → histograma
        px.box()        → boxplot
        px.pie()        → pastel
        px.choropleth() → mapa geográfico

    pip install plotly pandas

EJERCICIOS:

    Guarda todas las gráficas en output/ como archivos .html

1. Línea interactiva — Ventas mensuales:
   Crea un DataFrame con ventas de 12 meses para 3 productos
   y visualízalas con px.line().
   - Cada producto en una línea diferente
   - Título: "Ventas mensuales por producto"
   - Template: "plotly_dark"
   - Guarda como output/ventas_mensuales.html

2. Barras agrupadas — Comparación de departamentos:

       df_dept = pd.DataFrame({
           "departamento": ["Ingeniería","Datos","Marketing"] * 2,
           "año":          ["2025","2025","2025","2026","2026","2026"],
           "headcount":    [8, 5, 3, 12, 7, 4],
       })

   Usa px.bar(barmode="group") para comparar 2025 vs 2026.
   Guarda como output/headcount_departamentos.html

3. Scatter interactivo con hover:
   Genera 100 empleados sintéticos con: nombre, departamento, experiencia, salario.
   Crea un scatter donde:
   - Eje X: experiencia
   - Eje Y: salario
   - Color: departamento
   - Hover: nombre y departamento
   Guarda como output/scatter_empleados.html

4. Histograma con distribución:
   Genera 500 datos de salarios con distribución normal (media=55000, std=12000).
   Crea un histograma con la curva de densidad superpuesta (histnorm="density").
   Guarda como output/distribucion_salarios.html

5. Dashboard con subplots — usa make_subplots de plotly.subplots:
   Combina en una sola figura (2x2):
   - Gráfica de línea
   - Gráfica de barras
   - Scatter
   - Histograma
   Guarda como output/dashboard_plotly.html
   Nota: En plotly los subplots interactivos se llaman con graph_objects.
"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import os

os.makedirs("output", exist_ok=True)


# Ejercicio 1 — Tu código aquí


# Ejercicio 2 — Tu código aquí


# Ejercicio 3 — Tu código aquí


# Ejercicio 4 — Tu código aquí


# Ejercicio 5 — Tu código aquí
