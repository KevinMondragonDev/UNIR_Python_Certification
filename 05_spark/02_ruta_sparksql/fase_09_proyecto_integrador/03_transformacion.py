"""
FASE 9 — Módulo 3: Transformación y Enriquecimiento
=====================================================
Une hechos (ventas, transacciones) con dimensiones (empleados,
productos, clientes) y calcula métricas derivadas.

Decisiones de procesamiento distribuido:
  - Las dimensiones son pequeñas (≤ 500 filas) → broadcast join:
    las tablas de hechos NO se shufflean para unirse.
  - Solo se seleccionan las columnas necesarias ANTES del join
    (menos bytes en el broadcast y en memoria).
  - ventas_ricas se cachea porque la usan varios análisis y la escritura.
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def enriquecer_ventas(clean):
    emp = clean["empleados"].select(
        "employee_id", F.col("name").alias("vendedor"), "department", "nivel")
    prod = clean["productos"].select(
        "product_id", F.col("name").alias("producto"), "category", "rango_precio")

    ventas = clean["ventas"] \
        .join(F.broadcast(emp),  "employee_id", "left") \
        .join(F.broadcast(prod), "product_id",  "left")

    # Participación de cada venta dentro de su región-mes (window = 1 shuffle)
    w_region_mes = Window.partitionBy("region", "anio_mes")
    return ventas \
        .withColumn("pct_region_mes",
            F.round(F.col("amount") / F.sum("amount").over(w_region_mes) * 100, 2)) \
        .select("sale_id", "employee_id", "vendedor", "department", "nivel",
                "product_id", "producto", "category", "rango_precio",
                "amount", "region", "year", "month", "anio_mes", "pct_region_mes")


def enriquecer_transacciones(clean):
    cust = clean["clientes"].select(
        "customer_id", F.col("name").alias("cliente"), "country")
    return clean["transacciones"] \
        .join(F.broadcast(cust), "customer_id", "left") \
        .select("tx_id", "customer_id", "cliente", "country", "amount",
                "status", "currency", "source", "year", "month", "anio_mes")


def segmentar_clientes(tx_ricas):
    """Un registro por cliente con su gasto, nº de compras y segmento."""
    return tx_ricas.filter(F.col("status") == "completed") \
        .groupBy("customer_id", "cliente", "country") \
        .agg(F.round(F.sum("amount"), 2).alias("gasto_total"),
             F.count("*").alias("num_compras"),
             F.max("anio_mes").alias("ultima_compra")) \
        .withColumn("segmento",
            F.when(F.col("gasto_total") > 10000, "Alto Valor")
             .when(F.col("gasto_total") > 5000, "Medio")
             .otherwise("Bajo"))


def reporte_departamentos(clean, ventas_ricas):
    """Ventas por empleado de cada departamento (eficiencia comercial)."""
    plantilla = clean["empleados"].groupBy("department") \
        .agg(F.count("*").alias("num_empleados"),
             F.round(F.avg("salary"), 2).alias("salario_promedio"))
    ventas_dept = ventas_ricas.groupBy("department") \
        .agg(F.round(F.sum("amount"), 2).alias("total_ventas"),
             F.count("*").alias("num_ventas"))
    return plantilla.join(ventas_dept, "department", "left") \
        .fillna({"total_ventas": 0.0, "num_ventas": 0}) \
        .withColumn("ventas_por_empleado",
            F.round(F.col("total_ventas") / F.col("num_empleados"), 2))


def transformar_todos(clean):
    print("=== Transformación ===")
    ventas_ricas = enriquecer_ventas(clean).cache()
    tx_ricas = enriquecer_transacciones(clean).cache()

    resultado = {
        "ventas_ricas": ventas_ricas,
        "tx_ricas": tx_ricas,
        "clientes_segmentados": segmentar_clientes(tx_ricas),
        "reporte_departamentos": reporte_departamentos(clean, ventas_ricas),
    }

    n_ventas = ventas_ricas.count()       # materializa la caché
    n_tx = tx_ricas.count()
    assert n_ventas == clean["ventas"].count(), "El join de ventas perdió o duplicó filas"
    assert n_tx == clean["transacciones"].count(), "El join de transacciones perdió o duplicó filas"

    for nombre, df in resultado.items():
        print(f"  ✅ {nombre}: {df.count()} filas")
    return resultado


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from _modulos import cargar

    ingesta = cargar("01_ingesta")
    limpieza = cargar("02_limpieza")

    spark = ingesta.crear_spark()
    spark.sparkContext.setLogLevel("ERROR")
    BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    clean = limpieza.limpiar_todos(ingesta.cargar_datasets(spark, BASE))
    t = transformar_todos(clean)

    print("\nPlan de ventas_ricas (busca 2× BroadcastHashJoin y ningún SortMergeJoin):")
    enriquecer_ventas(clean).explain()
    t["clientes_segmentados"].groupBy("segmento").count().show()
    spark.stop()
