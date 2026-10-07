"""
Tema 4 — Análisis de datos | Ejercicio 6: EDA exploratorio
=======================================================================

TEORÍA:
    El Análisis Exploratorio de Datos (EDA) es el primer paso en cualquier
    proyecto de Data Engineering o Data Science. Su objetivo es entender
    la estructura, calidad y distribución de los datos antes de transformarlos.

    Pasos típicos de un EDA:
        1. Carga y vista general       → shape, dtypes, head()
        2. Calidad de datos            → nulls, duplicados, tipos incorrectos
        3. Estadísticas descriptivas   → describe(), value_counts()
        4. Distribuciones              → histogramas, boxplots
        5. Correlaciones               → df.corr(), heatmap
        6. Valores atípicos (outliers) → IQR, z-score
        7. Relaciones entre variables  → scatter matrix, groupby

    Métodos clave de pandas para EDA:
        df.info()               → tipos y nulls
        df.describe()           → estadísticas por columna numérica
        df.isnull().sum()       → conteo de nulls por columna
        df.duplicated().sum()   → número de duplicados
        df["col"].value_counts()→ frecuencia de valores
        df.corr()               → matriz de correlación
        df.nunique()            → valores únicos por columna
        df["col"].skew()        → asimetría de la distribución

    pip install pandas numpy matplotlib seaborn

EJERCICIOS:

    Usa el dataset df_transacciones definido al final del archivo.
    Simula un pipeline de EDA real — sección por sección.

SECCIÓN 1 — Vista general:
    1a. Imprime el shape, los tipos de datos y las primeras 5 filas.
    1b. Identifica qué columnas son numéricas y cuáles categóricas.
    1c. Imprime un resumen formateado así:

        ══════════════════════════════
         RESUMEN DEL DATASET
        ══════════════════════════════
         Registros :  1000
         Columnas  :  7
         Nulls     :  35 (3.50%)
         Duplicados:  12
        ══════════════════════════════

SECCIÓN 2 — Calidad de datos:
    2a. Cuenta nulls por columna y calcula el % de nulls de cada una.
    2b. Detecta duplicados exactos y muéstralos.
    2c. Detecta valores anómalos en "edad" (< 0 o > 100) y en "monto" (< 0).

SECCIÓN 3 — Estadísticas descriptivas:
    3a. Usa describe() para las columnas numéricas.
    3b. Usa value_counts() para ver la distribución de "categoria" y "pais".
    3c. Calcula el monto promedio y total por categoría.

SECCIÓN 4 — Correlaciones:
    4a. Calcula la matriz de correlación entre variables numéricas.
    4b. Identifica el par de variables con mayor correlación positiva
        y el par con mayor correlación negativa.

SECCIÓN 5 — Outliers:
    5a. Detecta outliers en "monto" usando el método IQR:
        - Q1 = percentil 25
        - Q3 = percentil 75
        - IQR = Q3 - Q1
        - Outlier si monto < Q1 - 1.5*IQR  o  monto > Q3 + 1.5*IQR
    5b. ¿Qué porcentaje de registros son outliers?
    5c. Filtra el DataFrame eliminando los outliers y compara los describe().

SECCIÓN 6 — Reporte final:
    6a. Genera un diccionario con el resumen del EDA:
        {
          "total_registros": ...,
          "columnas": [...],
          "nulls_por_columna": {...},
          "duplicados": ...,
          "outliers_monto": ...,
          "correlacion_mas_alta": ("col1", "col2", valor),
        }
    6b. Guarda el reporte en output/eda_report.json

RETO INTEGRADOR:
7. Realiza un EDA completo en Pandas sobre un dataset sintético: auditoría de valores nulos (`isnull().sum()`), estadísticas descriptivas (`describe()`), detección de outliers y matriz de correlación.
"""

import pandas as pd
import numpy as np
import json
import os

os.makedirs("output", exist_ok=True)

np.random.seed(42)
n = 1000

categorias = ["Electrónica", "Ropa", "Alimentos", "Hogar", "Deportes"]
paises     = ["México", "Colombia", "Argentina", "Chile", "Perú"]

df_transacciones = pd.DataFrame({
    "id":          range(1, n + 1),
    "fecha":       pd.date_range("2025-01-01", periods=n, freq="6h"),
    "cliente_id":  np.random.randint(100, 500, n),
    "categoria":   np.random.choice(categorias, n),
    "pais":        np.random.choice(paises, n),
    "monto":       np.round(np.random.exponential(scale=500, size=n), 2),
    "edad_cliente":np.random.randint(18, 70, n).astype(float),
})

# Inyectar nulls, duplicados y anomalías realistas
df_transacciones.loc[np.random.choice(n, 20, replace=False), "edad_cliente"] = np.nan
df_transacciones.loc[np.random.choice(n, 15, replace=False), "monto"]        = np.nan
df_transacciones.loc[np.random.choice(n, 5,  replace=False), "monto"]        = -999.0
df_transacciones = pd.concat([df_transacciones, df_transacciones.sample(12)], ignore_index=True)


# ─── SECCIÓN 1 — Vista general ────────────────────────────────────────────────


# ─── SECCIÓN 2 — Calidad de datos ────────────────────────────────────────────


# ─── SECCIÓN 3 — Estadísticas descriptivas ───────────────────────────────────


# ─── SECCIÓN 4 — Correlaciones ───────────────────────────────────────────────


# ─── SECCIÓN 5 — Outliers ────────────────────────────────────────────────────


# ─── SECCIÓN 6 — Reporte final ───────────────────────────────────────────────


# Reto Integrador — Tu código aquí
