"""
Tema 8 — Visualización | Ejercicio 6: Seaborn — Visualización estadística
===========================================================================

TEORÍA:
    Seaborn es una librería construida sobre Matplotlib, especializada en
    visualizaciones estadísticas con una sintaxis más limpia y gráficas
    más elegantes por defecto.

    GRÁFICAS RELACIONALES:
        sns.scatterplot(data=df, x="col1", y="col2", hue="categoria")
        sns.lineplot(data=df, x="fecha", y="ventas", hue="region")

    GRÁFICAS DE DISTRIBUCIÓN:
        sns.histplot(data=df, x="col", kde=True)     → histograma + curva
        sns.kdeplot(data=df, x="col", hue="cat")     → solo densidad
        sns.boxplot(data=df, x="cat", y="val")        → boxplot
        sns.violinplot(data=df, x="cat", y="val")     → violín

    GRÁFICAS CATEGÓRICAS:
        sns.barplot(data=df, x="cat", y="val", ci="sd")  → barras con error
        sns.countplot(data=df, x="cat")                   → frecuencia

    GRÁFICAS DE MATRICES:
        sns.heatmap(df.corr(), annot=True, cmap="coolwarm")
        sns.pairplot(df, hue="categoria")  → scatter matrix

    CONFIGURACIÓN:
        sns.set_theme(style="darkgrid", palette="muted")
        sns.set_palette("husl")

    pip install seaborn matplotlib pandas numpy

EJERCICIOS:

    Guarda todas las gráficas en output/seaborn/

1. DISTRIBUCIONES COMPARADAS — histplot + kde:
   Crea un gráfico que muestre la distribución del monto de transacciones
   para cada categoría superpuesta en el mismo eje.
   - Usa sns.kdeplot con hue="categoria"
   - Agrega una línea vertical en la media de cada categoría (axvline)
   - Guarda como output/seaborn/distribuciones_categorias.png

2. VIOLINPLOT — Distribución por día de la semana:
   Muestra la distribución del monto por día de la semana usando violinplot.
   - Ordena los días correctamente (Lunes a Domingo)
   - Agrega swarmplot encima con alpha=0.3 para ver los puntos individuales
   - Guarda como output/seaborn/violin_dias.png

3. PAIRPLOT — Relaciones entre variables numéricas:
   Con df_numerico (definido al final), crea un pairplot que:
   - Muestre scatter en las celdas off-diagonal
   - Muestre histograma/kde en la diagonal
   - Use hue="segmento"
   - Guarda como output/seaborn/pairplot_variables.png

4. HEATMAP — Ventas por hora y día:
   Construye una tabla pivot: filas=día_semana, columnas=hora_del_dia, valores=ventas_promedio.
   Visualízala como heatmap con:
   - Anotaciones de valores (annot=True, fmt=".0f")
   - Colormap "YlOrRd"
   - Título: "Ventas promedio por hora y día de la semana"
   - Guarda como output/seaborn/heatmap_hora_dia.png

5. REGPLOT — Relación monto vs cliente_frecuencia:
   Genera datos donde la frecuencia de compra del cliente tenga correlación
   positiva con el monto promedio (con algo de ruido).
   - Usa sns.regplot() para mostrar la línea de regresión y el intervalo de confianza
   - Agrega el coeficiente de correlación de Pearson en el título
   - Guarda como output/seaborn/regresion_frecuencia_monto.png
"""

import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

os.makedirs("output/seaborn", exist_ok=True)
sns.set_theme(style="darkgrid", palette="muted")
np.random.seed(99)

# --- Datos compartidos ---
n = 2000
dias_semana = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]
categorias  = ["Electrónica","Ropa","Alimentos","Hogar","Deportes"]

df_transacciones = pd.DataFrame({
    "monto":        np.round(np.random.exponential(300, n), 2),
    "categoria":    np.random.choice(categorias, n),
    "dia_semana":   np.random.choice(dias_semana, n),
    "hora":         np.random.randint(0, 24, n),
    "cliente_id":   np.random.randint(100, 1000, n),
})

df_numerico = pd.DataFrame({
    "ventas":        np.random.normal(50000, 10000, 300),
    "costo":         np.random.normal(30000, 8000, 300),
    "margen":        np.random.normal(20000, 5000, 300),
    "transacciones": np.random.randint(50, 500, 300),
    "segmento":      np.random.choice(["Premium","Estándar","Básico"], 300),
})

frecuencia = np.random.randint(1, 50, 200)
df_clientes = pd.DataFrame({
    "frecuencia_compras": frecuencia,
    "monto_promedio":     frecuencia * 120 + np.random.normal(0, 800, 200),
})


# Ejercicio 1 — DISTRIBUCIONES KDE


# Ejercicio 2 — VIOLINPLOT


# Ejercicio 3 — PAIRPLOT


# Ejercicio 4 — HEATMAP


# Ejercicio 5 — REGPLOT
