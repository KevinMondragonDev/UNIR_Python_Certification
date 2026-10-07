"""
Tema 4 — Análisis de datos | Ejercicio 7: Series de tiempo
=======================================================================

TEORÍA:
    Pandas tiene soporte nativo para series de tiempo (Time Series).
    Es esencial en Data Engineering para analizar logs, métricas, ventas, etc.

    Conceptos clave:
        pd.to_datetime()           → convertir strings a fechas
        df.set_index("fecha")      → usar fecha como índice
        df.resample("M").sum()     → agrupar por período (D, W, M, Q, Y)
        df.resample("M").agg()     → múltiples agregaciones por período
        df.shift(n)                → desplazar n períodos
        df.diff()                  → diferencia entre período actual y anterior
        df.pct_change()            → cambio porcentual
        df.rolling(7).mean()       → media móvil de 7 períodos
        df.ewm(span=7).mean()      → media móvil exponencial

    Frecuencias de resample:
        "D"  → diario
        "W"  → semanal
        "M"  → mensual (fin de mes)
        "MS" → mensual (inicio de mes)
        "Q"  → trimestral
        "Y"  → anual

    pip install pandas numpy

EJERCICIOS:

    Usa df_metricas definido al final del archivo (365 días de métricas de pipeline).

1. PREPARACIÓN:
   a) Convierte "fecha" a datetime y úsala como índice.
   b) Verifica que el índice tiene frecuencia diaria (sin huecos).
   c) Si hay fechas faltantes, rellénalas con forward fill (ffill).

2. RESAMPLE — Agrupación temporal:
   a) Calcula las ventas totales por semana.
   b) Calcula por mes: ventas totales, transacciones totales, ticket promedio.
   c) Calcula por trimestre el crecimiento % vs trimestre anterior.

3. MÉTRICAS DE CAMBIO:
   a) Agrega columna "ventas_ayer" (shift de 1 día).
   b) Agrega columna "diferencia_dia" (diff de ventas).
   c) Agrega columna "cambio_pct" (pct_change).
   d) ¿Cuál fue el día con mayor caída de ventas? ¿Y el de mayor crecimiento?

4. MEDIAS MÓVILES:
   a) Media móvil simple de 7 días (rolling 7).
   b) Media móvil simple de 30 días (rolling 30).
   c) Media móvil exponencial de 7 días (ewm span=7).
   d) ¿En qué períodos las ventas estuvieron por debajo de su media móvil de 30 días?
      Esto simula detectar períodos de bajo rendimiento.

5. ANÁLISIS DE ESTACIONALIDAD:
   a) Agrega columna "dia_semana" (0=lunes, 6=domingo) y "mes".
   b) Calcula las ventas promedio por día de la semana. ¿Qué día vende más?
   c) Calcula las ventas promedio por mes. ¿Hay estacionalidad?
   d) Genera un resumen como diccionario:
      {
        "mejor_dia_semana": "Viernes",
        "peor_dia_semana":  "Domingo",
        "mejor_mes": "Diciembre",
        "peor_mes":  "Enero",
        "tendencia": "Creciente"  # si la media del último trimestre > primer trimestre
      }

RETO INTEGRADOR:
8. Convierte una columna temporal a `datetime`, establécela como índice, realiza un re-muestreo diario (`resample("D")`), imputa faltantes con `.ffill()` y calcula una media móvil de 7 días (`.rolling(7).mean()`).
"""

import pandas as pd
import numpy as np
import json
import os

os.makedirs("output", exist_ok=True)

np.random.seed(0)
fechas = pd.date_range("2025-01-01", "2025-12-31", freq="D")

# Simular ventas con tendencia creciente + estacionalidad semanal + ruido
tendencia     = np.linspace(30000, 50000, len(fechas))
estacionalidad= 5000 * np.sin(np.linspace(0, 4 * np.pi, len(fechas)))
ruido         = np.random.normal(0, 2000, len(fechas))
# Viernes y sábado venden más
boost_fin_semana = np.array([3000 if d.weekday() >= 4 else 0 for d in fechas])

df_metricas = pd.DataFrame({
    "fecha":        fechas,
    "ventas":       np.round(tendencia + estacionalidad + ruido + boost_fin_semana, 2),
    "transacciones":np.random.randint(100, 400, len(fechas)),
    "errores_pipeline": np.random.poisson(lam=2, size=len(fechas)),
})

# Inyectar 5 fechas faltantes
drop_idx = np.random.choice(len(df_metricas), 5, replace=False)
df_metricas = df_metricas.drop(index=drop_idx).reset_index(drop=True)


# Ejercicio 1 — PREPARACIÓN


# Ejercicio 2 — RESAMPLE


# Ejercicio 3 — MÉTRICAS DE CAMBIO


# Ejercicio 4 — MEDIAS MÓVILES


# Ejercicio 5 — ESTACIONALIDAD


# Reto Integrador — Tu código aquí
