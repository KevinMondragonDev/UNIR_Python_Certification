"""
Tema 8 — Visualización | Ejercicio 5: Proyecto integrador — EDA Visual Completo
=================================================================================

TEORÍA:
    En la práctica real de Data Engineering y Data Science, el análisis visual
    es inseparable del análisis numérico. Este ejercicio simula un análisis
    completo de extremo a extremo, combinando pandas + matplotlib + plotly.

    Flujo típico de un EDA visual:
        1. Cargar y limpiar datos
        2. Estadísticas generales (describe, info)
        3. Distribuciones univariadas (histograma, boxplot)
        4. Relaciones bivariadas (scatter, correlación)
        5. Análisis temporal (línea + tendencia)
        6. Comparación por grupos (barras, violinplot)
        7. Resumen ejecutivo (dashboard)

    pip install pandas numpy matplotlib plotly

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONTEXTO DEL EJERCICIO:
    Eres Data Engineer en una empresa de e-commerce.
    Te dan el dataset df_ecommerce con transacciones de 2025.
    Tu jefe te pide un reporte visual para la reunión del lunes.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PASO 1 — Limpieza y preparación:
    1a. Elimina registros con monto nulo o negativo.
    1b. Normaliza los nombres de categoría (strip, title).
    1c. Convierte "fecha" a datetime y extrae: año, mes, día_semana, hora.
    1d. Imprime el resumen de calidad: total_registros, nulls, duplicados.

PASO 2 — Distribuciones (Matplotlib):
    2a. Histograma del monto de transacciones (usa log scale en X si es necesario).
    2b. Boxplot del monto por categoría (comparativo).
    2c. Guarda como output/eda/distribucion_montos.png

PASO 3 — Análisis temporal (Matplotlib):
    3a. Línea de ventas totales diarias con media móvil de 7 días.
    3b. Barras de ventas totales por mes.
    3c. Heatmap de ventas promedio: eje X = hora del día, eje Y = día de la semana.
        Esto revela cuándo venden más (útil para escalar infraestructura).
    3d. Guarda como output/eda/analisis_temporal.png (subplots 3x1)

PASO 4 — Análisis por segmento (Plotly):
    4a. Barras horizontales interactivas: top 10 categorías por ventas totales.
    4b. Scatter interactivo: monto vs hora del día, color por categoría.
    4c. Guarda como output/eda/segmentos.html

PASO 5 — Dashboard ejecutivo (Plotly):
    Crea un dashboard 2x3 con:
    - [1,1] KPI: ventas totales del año (go.Indicator)
    - [1,2] KPI: ticket promedio (go.Indicator)
    - [1,3] KPI: transacciones totales (go.Indicator)
    - [2,1] Línea de ventas mensuales
    - [2,2] Pie de distribución por categoría
    - [2,3] Barras de ventas por día de la semana
    Template: "plotly_dark"
    Guarda como output/eda/dashboard_ejecutivo.html

PASO 6 — Conclusiones:
    Responde estas preguntas en comentarios, basándote en tus gráficas:
    - ¿Cuál es la categoría con más ventas?
    - ¿Cuál es el día de la semana con más transacciones?
    - ¿A qué hora del día se generan más ventas?
    - ¿Hay tendencia creciente o decreciente en el año?
    - ¿Qué recomendarías al equipo de ingeniería de infraestructura?
"""

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import os

os.makedirs("output/eda", exist_ok=True)
plt.style.use("seaborn-v0_8-darkgrid")
np.random.seed(2025)

# ── Generación del dataset ────────────────────────────────────────────────────
n = 5000
categorias = ["Electrónica", "ropa ", "ALIMENTOS", "Hogar", "Deportes",
              "Juguetes", " Libros", "Salud"]

fechas_random = pd.to_datetime(
    np.random.randint(
        pd.Timestamp("2025-01-01").value,
        pd.Timestamp("2025-12-31").value,
        size=n
    )
)

df_ecommerce = pd.DataFrame({
    "id":          range(1, n + 1),
    "fecha":       fechas_random,
    "categoria":   np.random.choice(categorias, n),
    "monto":       np.round(np.random.exponential(scale=300, size=n), 2),
    "cliente_id":  np.random.randint(1000, 5000, n),
    "pais":        np.random.choice(["México","Colombia","Argentina","Chile"], n),
})

# Inyectar datos sucios
df_ecommerce.loc[np.random.choice(n, 50, replace=False), "monto"] = np.nan
df_ecommerce.loc[np.random.choice(n, 20, replace=False), "monto"] = -100.0


# ─── PASO 1 — Limpieza ────────────────────────────────────────────────────────


# ─── PASO 2 — Distribuciones ─────────────────────────────────────────────────


# ─── PASO 3 — Análisis temporal ──────────────────────────────────────────────


# ─── PASO 4 — Análisis por segmento ─────────────────────────────────────────


# ─── PASO 5 — Dashboard ejecutivo ───────────────────────────────────────────


# ─── PASO 6 — Conclusiones ───────────────────────────────────────────────────
# ¿Cuál es la categoría con más ventas?
#
# ¿Cuál es el día de la semana con más transacciones?
#
# ¿A qué hora del día se generan más ventas?
#
# ¿Hay tendencia creciente o decreciente en el año?
#
# ¿Qué recomendarías al equipo de infraestructura?
#
