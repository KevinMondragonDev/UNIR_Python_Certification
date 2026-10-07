"""
FASE 9 — Pipeline Modular
===========================
Misma lógica que pipeline.py, pero orquestando los 5 módulos
independientes. Así se estructura un job real: cada paso es una
función testeable por separado.

    01_ingesta → 02_limpieza → 03_transformacion → 04_analisis_sql → 05_escritura

Ejecutar: python pipeline_modular.py && python validar_proyecto.py
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _modulos import cargar

ingesta        = cargar("01_ingesta")
limpieza       = cargar("02_limpieza")
transformacion = cargar("03_transformacion")
analisis       = cargar("04_analisis_sql")
escritura      = cargar("05_escritura")

BASE    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(BASE, "fase_09_proyecto_integrador", "output")


def main():
    t0 = time.time()
    spark = ingesta.crear_spark()
    spark.sparkContext.setLogLevel("ERROR")

    pasos = []
    try:
        t = time.time()
        datasets = ingesta.cargar_datasets(spark, BASE)
        if not ingesta.validar_ingesta(datasets):
            raise RuntimeError("La ingesta no pasó la validación")
        pasos.append(("1. Ingesta", time.time() - t))

        t = time.time()
        clean = limpieza.limpiar_todos(datasets)
        pasos.append(("2. Limpieza", time.time() - t))

        t = time.time()
        enriquecidos = transformacion.transformar_todos(clean)
        pasos.append(("3. Transformación", time.time() - t))

        t = time.time()
        analisis.ejecutar_analisis(spark, clean)
        pasos.append(("4. Análisis SQL", time.time() - t))

        t = time.time()
        escritura.escribir_resultados(spark, enriquecidos, OUT_DIR)
        pasos.append(("5. Escritura", time.time() - t))
    finally:
        spark.stop()

    print("\n" + "=" * 50)
    print("RESUMEN DEL PIPELINE")
    print("=" * 50)
    for nombre, seg in pasos:
        print(f"  {nombre:<20} {seg:6.1f}s")
    print(f"  {'TOTAL':<20} {time.time() - t0:6.1f}s")
    print("\nSiguiente: python validar_proyecto.py")


if __name__ == "__main__":
    main()
