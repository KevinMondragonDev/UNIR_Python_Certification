"""
Tema 5 — Visualización | Ejercicio 5: Plotly avanzado
=================================================================================

TEORÍA:
    Plotly permite crear visualizaciones interactivas de nivel profesional.

    PLOTLY EXPRESS (rápido):
        px.line, px.bar, px.scatter, px.histogram, px.box
        px.choropleth()   → mapa de coropletas (por país/región)
        px.sunburst()     → gráfico de sol (jerarquías)
        px.treemap()      → mapa de árbol
        px.funnel()       → embudo de conversión

    ANIMACIONES:
        px.scatter(df, animation_frame="año", animation_group="pais")

    SUBPLOTS CON GRAPH OBJECTS:
        from plotly.subplots import make_subplots
        import plotly.graph_objects as go

        fig = make_subplots(rows=2, cols=2,
                            subplot_titles=["A","B","C","D"],
                            specs=[[{"type":"xy"}, {"type":"pie"}],
                                   [{"type":"xy"}, {"type":"xy"}]])
        fig.add_trace(go.Bar(...), row=1, col=1)
        fig.add_trace(go.Pie(...), row=1, col=2)

    PERSONALIZACIÓN:
        fig.update_layout(
            template="plotly_dark",
            title_font_size=20,
            showlegend=True,
            hovermode="x unified",   → tooltip unificado en eje X
        )
        fig.update_traces(hovertemplate="<b>%{x}</b><br>Ventas: %{y:,.0f}")

    pip install plotly pandas numpy

EJERCICIOS:

    Guarda todas las gráficas en output/plotly/ como archivos .html

1. LÍNEA CON RANGESLIDER — Serie de tiempo interactiva:
   Usa df_ventas (definido al final). Crea una gráfica de línea que incluya:
   - La serie de ventas diarias
   - La media móvil de 7 días
   - Un range slider en la parte inferior para zoom temporal
   - Botones de rango: "1M", "3M", "6M", "YTD", "1Y"
   - Template: "plotly_dark"
   - Guarda como output/plotly/ventas_rangeslider.html

2. SUNBURST — Jerarquía región → país → categoría:
   Usa df_jerarquia (definido al final).
   Crea un gráfico sunburst que muestre:
   - Nivel 1: región
   - Nivel 2: país
   - Nivel 3: categoría
   - Valor: ventas
   - Guarda como output/plotly/sunburst_ventas.html

3. SCATTER ANIMADO — Evolución anual:
   Usa df_evolucion (definido al final).
   Crea un scatter animado por año donde:
   - Eje X: gdp_per_capita
   - Eje Y: satisfaccion
   - Tamaño: poblacion
   - Color: region
   - animation_frame: año
   - Guarda como output/plotly/scatter_animado.html

4. DASHBOARD — 4 gráficas en una figura:
   Crea un dashboard 2x2 con:
   - [1,1] Barras de ventas mensuales (go.Bar)
   - [1,2] Pie de participación por categoría (go.Pie)
   - [2,1] Línea de ventas diarias + media móvil (go.Scatter x2)
   - [2,2] Histograma de distribución de montos (go.Histogram)
   Aplica template "plotly_dark" al dashboard completo.
   Guarda como output/plotly/dashboard_ejecutivo.html

5. FUNNEL — Embudo de conversión:
   Simula un pipeline de datos con estas etapas y conteos:

       etapas  = ["Archivos detectados", "Descargados", "Validados", "Transformados", "Cargados"]
       conteos = [1000, 987, 943, 921, 915]

   Usa px.funnel() para visualizar el % de éxito en cada etapa.
   Agrega el porcentaje de pérdida entre cada etapa.
   Guarda como output/plotly/funnel_pipeline.html

RETO INTEGRADOR:
6. Desarrolla un dashboard interactivo multiejefigura usando `make_subplots` de Plotly, agregando selectores de rango temporal y botones de filtro interactivo.
"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import os

os.makedirs("output/plotly", exist_ok=True)
np.random.seed(42)

# --- Datos ---
fechas = pd.date_range("2025-01-01", "2025-12-31", freq="D")
df_ventas = pd.DataFrame({
    "fecha":  fechas,
    "ventas": np.round(
        40000 + 15000 * np.sin(np.linspace(0, 4*np.pi, len(fechas)))
        + np.random.normal(0, 3000, len(fechas)), 2
    ),
})
df_ventas["ma7"] = df_ventas["ventas"].rolling(7).mean()

regiones = {"LATAM": ["México","Colombia","Argentina"], "NA": ["EEUU","Canadá"]}
rows = []
for region, paises in regiones.items():
    for pais in paises:
        for cat in ["Electrónica","Ropa","Alimentos"]:
            rows.append({"region": region, "pais": pais, "categoria": cat,
                         "ventas": np.random.randint(10000, 100000)})
df_jerarquia = pd.DataFrame(rows)

años = range(2018, 2026)
paises_evol = ["México","Colombia","Argentina","Chile","Perú"]
rows_evol = []
for año in años:
    for pais in paises_evol:
        rows_evol.append({
            "año": str(año), "pais": pais,
            "region": "LATAM",
            "gdp_per_capita":   np.random.randint(5000, 20000),
            "satisfaccion":     np.random.uniform(4, 9),
            "poblacion":        np.random.randint(5_000_000, 130_000_000),
        })
df_evolucion = pd.DataFrame(rows_evol)


# Ejercicio 1 — RANGESLIDER


# Ejercicio 2 — SUNBURST


# Ejercicio 3 — SCATTER ANIMADO


# Ejercicio 4 — DASHBOARD


# Ejercicio 5 — FUNNEL


# Reto Integrador — Tu código aquí
