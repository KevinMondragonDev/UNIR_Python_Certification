"""
FASE 9 — Módulo 5: Escritura de Resultados
============================================
Guarda las salidas del pipeline siguiendo buenas prácticas:

  - Hechos grandes  → Parquet particionado por columnas de fecha
                      (partition pruning en lecturas posteriores)
  - Dimensiones     → Parquet sin particionar, pocos archivos
  - Reportes        → CSV con coalesce(1) (un solo archivo legible)

Antipatrón que evitamos: escribir con 200 particiones de shuffle →
200 archivos diminutos por carpeta ("small files problem").
"""

import os
import sys
import shutil
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import functions as F


def _contar_archivos(ruta, extension):
    return sum(1 for _, _, archivos in os.walk(ruta)
               for a in archivos if a.endswith(extension))


def escribir_resultados(spark, t, out_dir):
    print("=== Escritura ===")
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir)

    # 1. Hechos: particionado en disco por año.
    #    repartition("year") agrupa cada año en una partición en memoria
    #    → 1 archivo por carpeta year=XXXX en lugar de N archivos pequeños.
    ruta_ventas = os.path.join(out_dir, "ventas_enriquecidas.parquet")
    t["ventas_ricas"].repartition("year") \
        .write.mode("overwrite") \
        .partitionBy("year") \
        .parquet(ruta_ventas)

    # 2. Dimensiones / agregados: pequeños → 1 archivo
    for nombre in ["clientes_segmentados", "reporte_departamentos"]:
        t[nombre].coalesce(1).write.mode("overwrite") \
            .parquet(os.path.join(out_dir, f"{nombre}.parquet"))

    # 3. Reportes CSV para negocio
    reportes_dir = os.path.join(out_dir, "reportes_csv")
    v = t["ventas_ricas"]
    reportes = {
        "top_vendedores": v.groupBy("vendedor", "department")
            .agg(F.round(F.sum("amount"), 2).alias("total_ventas"))
            .orderBy(F.desc("total_ventas")).limit(20),
        "top_productos": v.groupBy("category", "producto")
            .agg(F.round(F.sum("amount"), 2).alias("total"))
            .orderBy("category", F.desc("total")),
        "metricas_regiones": v.groupBy("region", "anio_mes")
            .agg(F.round(F.sum("amount"), 2).alias("total"),
                 F.count("*").alias("num_ventas"))
            .orderBy("region", "anio_mes"),
    }
    for nombre, df in reportes.items():
        df.coalesce(1).write.mode("overwrite").option("header", True) \
          .csv(os.path.join(reportes_dir, nombre))

    # Verificación: releer y contar archivos
    print(f"  ✅ ventas_enriquecidas: "
          f"{sorted(d for d in os.listdir(ruta_ventas) if d.startswith('year='))} "
          f"({_contar_archivos(ruta_ventas, '.parquet')} archivos)")
    for nombre in ["clientes_segmentados", "reporte_departamentos"]:
        ruta = os.path.join(out_dir, f"{nombre}.parquet")
        print(f"  ✅ {nombre}: {spark.read.parquet(ruta).count()} filas "
              f"({_contar_archivos(ruta, '.parquet')} archivo)")
    for nombre in reportes:
        print(f"  ✅ reportes_csv/{nombre}: "
              f"{_contar_archivos(os.path.join(reportes_dir, nombre), '.csv')} CSV")

    # Partition pruning: leer un solo año solo abre esa carpeta
    df_2024 = spark.read.parquet(ruta_ventas).filter(F.col("year") == 2024)
    print(f"  ✅ Lectura con pruning (year=2024): {df_2024.count()} filas")


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from _modulos import cargar

    ingesta = cargar("01_ingesta")
    limpieza = cargar("02_limpieza")
    transformacion = cargar("03_transformacion")

    spark = ingesta.crear_spark()
    spark.sparkContext.setLogLevel("ERROR")
    BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    OUT_DIR = os.path.join(BASE, "fase_09_proyecto_integrador", "output")

    clean = limpieza.limpiar_todos(ingesta.cargar_datasets(spark, BASE))
    t = transformacion.transformar_todos(clean)
    escribir_resultados(spark, t, OUT_DIR)
    spark.stop()
