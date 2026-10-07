"""
FASE 10 — RETO: Job de Producción Desplegado en Clúster
=========================================================
Sin guía. Escribe un job listo para spark-submit y despliégalo en el
clúster local (./cluster_local.sh start) o en Docker (docker compose up -d).

ENUNCIADO
  El equipo de finanzas necesita, cada día, las métricas de transacciones
  por moneda y mes a partir de datasets/parquet/transactions.parquet.

  Implementa main(argv) para que acepte:
      --entrada      ruta del parquet (default: el dataset del proyecto)
      --salida       directorio de salida (obligatorio)
      --status       status a incluir (default: completed); se puede repetir
                     (--status completed --status refunded)
      --particiones  valor de spark.sql.shuffle.partitions (default: 8)

  Y produzca un Parquet con columnas:
      currency, anio_mes, num_tx, total, ticket_promedio, max_tx
  PARTICIONADO EN DISCO por currency, con UN archivo por carpeta.

REGLAS DE PRODUCCIÓN (todas verificadas por validar.py)
  1. Sin .master() en el código
  2. main() devuelve 0 si todo bien, 1 si falla o si el resultado está vacío
  3. spark.stop() siempre (try/finally)
  4. Validación de calidad antes de escribir:
       - ningún total negativo
       - ninguna currency nula
     Si falla → log de error y return 1, SIN escribir nada
  5. Logging (no print) indicando master, nº de filas y ruta de salida

DESPLIEGUE (responde en los comentarios del final)
  a) Lánzalo con 2 executors de 2 cores y 1 GB en el clúster standalone.
  b) Abre http://localhost:8080: ¿cuántos executors y en qué workers?
  c) Abre http://localhost:4040 → Stages: ¿cuántas tasks tuvo el stage
     del groupBy? ¿Coincide con --particiones? (pista: AQE)
  d) Repite con --conf spark.sql.adaptive.enabled=false y compara.

Valida: python validar.py
Solución: solucion_reto.py (⚠️ solo si te atascas)
"""

import sys


def main(argv=None):
    # TU CÓDIGO AQUÍ
    return None


if __name__ == "__main__":
    sys.exit(main())

# Respuestas de despliegue:
#   comando usado: ...
#   b) ...
#   c) ...
#   d) ...
