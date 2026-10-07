"""
FASE 9 — Módulo 2: Limpieza
==============================
Normaliza tipos, maneja nulos y elimina duplicados.
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def limpiar_empleados(df):
    avg_salary = df.filter(F.col("salary").isNotNull()) \
                   .agg(F.avg("salary")).first()[0]

    ventana_dept = Window.partitionBy("department")
    avg_por_dept = df.withColumn("avg_dept", F.avg("salary").over(ventana_dept))

    return avg_por_dept \
        .withColumn("salary",
            F.coalesce(F.col("salary"), F.col("avg_dept"), F.lit(avg_salary))) \
        .withColumn("hire_date_dt", F.to_date("hire_date", "yyyy-MM-dd")) \
        .withColumn("anios_empresa",
            F.round(F.datediff(F.current_date(), F.col("hire_date_dt")) / 365, 1)) \
        .withColumn("nivel",
            F.when(F.col("salary") < 40000, "Junior")
             .when(F.col("salary") < 65000, "Mid")
             .when(F.col("salary") < 90000, "Senior")
             .otherwise("Lead")) \
        .drop("avg_dept", "hire_date") \
        .dropDuplicates(["employee_id"])


def limpiar_ventas(df):
    return df \
        .filter(F.col("amount") > 0) \
        .withColumn("sale_date_dt", F.to_date("sale_date", "yyyy-MM-dd")) \
        .withColumn("year",  F.year("sale_date_dt")) \
        .withColumn("month", F.month("sale_date_dt")) \
        .withColumn("anio_mes", F.date_format("sale_date_dt", "yyyy-MM")) \
        .drop("sale_date") \
        .dropDuplicates(["sale_id"])


def limpiar_productos(df):
    return df \
        .withColumn("disponible", F.col("stock") > 0) \
        .withColumn("rango_precio",
            F.when(F.col("price") < 50, "Económico")
             .when(F.col("price") < 200, "Medio")
             .otherwise("Premium")) \
        .dropDuplicates(["product_id"])


def limpiar_clientes(df):
    return df \
        .withColumn("email",
            F.when((F.col("email").isNull()) | (F.col("email") == ""), F.lit("sin_email"))
             .otherwise(F.col("email"))) \
        .withColumn("signup_date_dt", F.to_date("signup_date", "yyyy-MM-dd")) \
        .drop("signup_date") \
        .dropDuplicates(["customer_id"])


def limpiar_transacciones(df):
    return df \
        .filter(F.col("amount") > 0) \
        .withColumn("year",  F.year("ts")) \
        .withColumn("month", F.month("ts")) \
        .withColumn("anio_mes", F.date_format("ts", "yyyy-MM")) \
        .dropDuplicates(["tx_id"])


def limpiar_todos(datasets):
    print("=== Limpieza ===")
    clean = {}
    clean["empleados"]     = limpiar_empleados(datasets["empleados"])
    clean["ventas"]        = limpiar_ventas(datasets["ventas"])
    clean["productos"]     = limpiar_productos(datasets["productos"])
    clean["clientes"]      = limpiar_clientes(datasets["clientes"])
    clean["transacciones"] = limpiar_transacciones(datasets["transacciones"])
    clean["ordenes"]       = datasets["ordenes"]
    clean["eventos"]       = datasets["eventos"]

    for nombre, df in clean.items():
        nulos = sum(df.filter(F.col(c).isNull()).count()
                    for c in df.columns if c in ["employee_id","sale_id","product_id","customer_id","tx_id"])
        print(f"  ✅ {nombre}: {df.count()} filas limpias | nulos en ID: {nulos}")

    return clean


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from _modulos import cargar
    ingesta = cargar("01_ingesta")

    spark = ingesta.crear_spark()
    spark.sparkContext.setLogLevel("ERROR")
    BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    limpiar_todos(ingesta.cargar_datasets(spark, BASE))
    spark.stop()
