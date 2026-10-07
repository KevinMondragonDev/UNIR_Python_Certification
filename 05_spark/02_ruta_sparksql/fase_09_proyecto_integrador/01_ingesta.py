"""
FASE 9 — Módulo 1: Ingesta
============================
Carga todos los datasets con schema explícito y validación inicial.
"""

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DoubleType, TimestampType, ArrayType
)


def crear_spark():
    return SparkSession.builder \
        .appName("Proyecto_Integrador") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()


def cargar_datasets(spark, base_dir):
    csv_dir  = os.path.join(base_dir, "datasets", "csv")
    parq_dir = os.path.join(base_dir, "datasets", "parquet")

    # Schemas explícitos
    emp_schema = StructType([
        StructField("employee_id", IntegerType(), True),
        StructField("name",        StringType(),  True),
        StructField("department",  StringType(),  True),
        StructField("salary",      DoubleType(),  True),
        StructField("hire_date",   StringType(),  True),
        StructField("country",     StringType(),  True),
    ])
    sales_schema = StructType([
        StructField("sale_id",     IntegerType(), True),
        StructField("employee_id", IntegerType(), True),
        StructField("product_id",  IntegerType(), True),
        StructField("amount",      DoubleType(),  True),
        StructField("sale_date",   StringType(),  True),
        StructField("region",      StringType(),  True),
    ])
    prod_schema = StructType([
        StructField("product_id",  IntegerType(), True),
        StructField("name",        StringType(),  True),
        StructField("category",    StringType(),  True),
        StructField("price",       DoubleType(),  True),
        StructField("stock",       IntegerType(), True),
    ])
    cust_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name",        StringType(),  True),
        StructField("email",       StringType(),  True),
        StructField("country",     StringType(),  True),
        StructField("signup_date", StringType(),  True),
    ])

    datasets = {
        "empleados":      spark.read.csv(os.path.join(csv_dir,  "employees.csv"), header=True, schema=emp_schema),
        "ventas":         spark.read.csv(os.path.join(csv_dir,  "sales.csv"),     header=True, schema=sales_schema),
        "productos":      spark.read.csv(os.path.join(csv_dir,  "products.csv"),  header=True, schema=prod_schema),
        "clientes":       spark.read.csv(os.path.join(csv_dir,  "customers.csv"), header=True, schema=cust_schema),
        "transacciones":  spark.read.parquet(os.path.join(parq_dir, "transactions.parquet")),
        "ordenes":        spark.read.parquet(os.path.join(parq_dir, "orders.parquet")),
        "eventos":        spark.read.parquet(os.path.join(parq_dir, "events.parquet")),
    }
    return datasets


def validar_ingesta(datasets):
    print("\n=== Validación de Ingesta ===")
    esperado = {"empleados": 500, "ventas": 2000, "productos": 100,
                "clientes": 300, "transacciones": 5000, "ordenes": 1000, "eventos": 3000}
    todos_ok = True
    for nombre, df in datasets.items():
        cnt = df.count()
        exp = esperado.get(nombre, -1)
        ok  = cnt == exp
        status = "✅" if ok else "❌"
        print(f"  {status} {nombre}: {cnt} filas (esperado: {exp})")
        if not ok:
            todos_ok = False
    return todos_ok


if __name__ == "__main__":
    spark = crear_spark()
    spark.sparkContext.setLogLevel("ERROR")
    BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    datasets = cargar_datasets(spark, BASE)
    ok = validar_ingesta(datasets)
    print(f"\nIngesta: {'✅ EXITOSA' if ok else '❌ CON ERRORES'}")
    spark.stop()
