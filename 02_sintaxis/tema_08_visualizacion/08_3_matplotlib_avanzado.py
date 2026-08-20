"""
Tema 8 — Visualización | Ejercicio 3: Matplotlib Avanzado
===========================================================

TEORÍA:
    Matplotlib permite personalización completa de cada elemento visual.

    ESTILOS PREDEFINIDOS:
        plt.style.use("seaborn-v0_8-darkgrid")
        plt.style.use("ggplot")
        plt.style.available   → lista todos los estilos

    COLORES Y PALETAS:
        color="steelblue"             → nombre de color
        color="#2E86AB"               → hex
        color=(0.2, 0.5, 0.8)         → RGB normalizado
        cmap="viridis"                → colormap para heatmaps

    ANOTACIONES:
        plt.annotate("texto", xy=(x,y), xytext=(x2,y2), arrowprops={...})
        plt.axhline(y=valor, color="red", linestyle="--")  → línea horizontal
        plt.axvline(x=valor, color="blue")                 → línea vertical
        plt.fill_between(x, y1, y2, alpha=0.3)            → área rellena

    MÚLTIPLES EJES (twin axis):
        fig, ax1 = plt.subplots()
        ax2 = ax1.twinx()     → segundo eje Y

    FIGURA DE OBJETO (API orientada a objetos — RECOMENDADA):
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(x, y)
        ax.set_title("título")
        ax.set_xlabel("x")

    pip install matplotlib numpy pandas

EJERCICIOS:

    Guarda todas las gráficas en output/matplotlib/

1. LÍNEA CON ÁREA RELLENA — Series de tiempo de ventas:
   Usa df_ventas (definido al final). Grafica las ventas diarias con:
   - Línea azul principal
   - Área rellena bajo la curva (fill_between con alpha=0.2)
   - Media móvil de 7 días como línea roja punteada
   - Línea horizontal en la media total
   - Anotación de la fecha con mayor venta
   - Título: "Ventas diarias 2025 con media móvil"
   - Guarda como output/matplotlib/ventas_area.png

2. BARRAS APILADAS — Composición de ventas por categoría y mes:
   Usa df_categorias (definido al final). Crea un gráfico de barras apiladas:
   - Eje X: mes
   - Barras apiladas por categoría con colores distintos
   - Leyenda fuera del gráfico (bbox_to_anchor)
   - Valores totales encima de cada barra
   - Guarda como output/matplotlib/barras_apiladas.png

3. HEATMAP — Correlación entre variables:
   Calcula la matriz de correlación del df_numericos (definido al final).
   Visualízala como heatmap usando imshow():
   - Colormap: "coolwarm" (azul=negativo, rojo=positivo)
   - Muestra el valor numérico en cada celda
   - Colorbar lateral
   - Títulos de ejes con nombres de columnas
   - Guarda como output/matplotlib/heatmap_correlacion.png

4. DOBLE EJE Y — Ventas y error rate:
   Usa df_operaciones (definido al final).
   Crea una gráfica con doble eje Y:
   - Eje Y izquierdo (azul): ventas diarias (barras)
   - Eje Y derecho (rojo): tasa de error % (línea)
   - Leyenda combinada de ambos ejes
   - Guarda como output/matplotlib/doble_eje.png

5. BOXPLOT COMPARATIVO — Distribución por categoría:
   Crea un boxplot que compare la distribución de montos por categoría.
   - Orientación vertical
   - Colores distintos por caja
   - Muestra los outliers como puntos naranjas
   - Agrega la media como triángulo verde dentro de cada caja (showmeans=True)
   - Título: "Distribución de montos por categoría"
   - Guarda como output/matplotlib/boxplot_categorias.png
"""

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import numpy as np
import os

os.makedirs("output/matplotlib", exist_ok=True)
plt.style.use("seaborn-v0_8-darkgrid")
np.random.seed(7)

# --- Datos ---
fechas = pd.date_range("2025-01-01", "2025-12-31", freq="D")
df_ventas = pd.DataFrame({
    "fecha":  fechas,
    "ventas": np.round(
        40000 + 15000 * np.sin(np.linspace(0, 4*np.pi, len(fechas)))
        + np.random.normal(0, 3000, len(fechas)), 2
    ),
})

meses = ["Ene", "Feb", "Mar", "Abr", "May", "Jun"]
df_categorias = pd.DataFrame({
    "mes":        meses * 5,
    "categoria":  np.repeat(["Electrónica","Ropa","Alimentos","Hogar","Deportes"], 6),
    "ventas":     np.random.randint(5000, 30000, 30),
})

df_numericos = pd.DataFrame(
    np.random.randn(200, 5),
    columns=["Ventas","Costo","Margen","Transacciones","Devoluciones"]
)
df_numericos["Margen"]       = df_numericos["Ventas"] * 0.3 + np.random.randn(200) * 0.1
df_numericos["Devoluciones"] = -df_numericos["Ventas"] * 0.1 + np.random.randn(200) * 0.2

df_operaciones = pd.DataFrame({
    "dia":        range(1, 31),
    "ventas":     np.random.randint(30000, 80000, 30),
    "error_rate": np.clip(np.random.normal(2.5, 1.5, 30), 0, 10),
})

categorias = np.repeat(["Electrónica","Ropa","Alimentos","Hogar","Deportes"], 80)
montos      = np.concatenate([
    np.random.exponential(500, 80),
    np.random.exponential(150, 80),
    np.random.exponential(80,  80),
    np.random.exponential(300, 80),
    np.random.exponential(200, 80),
])


# Ejercicio 1 — LÍNEA CON ÁREA RELLENA


# Ejercicio 2 — BARRAS APILADAS


# Ejercicio 3 — HEATMAP


# Ejercicio 4 — DOBLE EJE Y


# Ejercicio 5 — BOXPLOT
