"""
Generador de Datasets para el Plan de Aprendizaje PySpark
Ejecutar una sola vez: python generar_datasets.py
Requiere: PySpark instalado
"""

import os
import sys
import random
import csv
from datetime import datetime, timedelta

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(BASE_DIR, "csv")
PARQUET_DIR = os.path.join(BASE_DIR, "parquet")

os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(PARQUET_DIR, exist_ok=True)

random.seed(42)

# ─────────────────────────────────────────
# Datos de referencia
# ─────────────────────────────────────────
FIRST_NAMES = [
    "Ana", "Luis", "Carlos", "Maria", "Jose", "Laura", "Pedro", "Sofia",
    "Diego", "Valentina", "Jorge", "Camila", "Andres", "Isabella", "Miguel",
    "Daniela", "Fernando", "Gabriela", "Ricardo", "Alejandra", "Sergio",
    "Monica", "Rafael", "Natalia", "Hector", "Paola", "Eduardo", "Cristina"
]
LAST_NAMES = [
    "Garcia", "Rodriguez", "Martinez", "Lopez", "Gonzalez", "Perez",
    "Sanchez", "Ramirez", "Torres", "Flores", "Rivera", "Gomez", "Diaz",
    "Morales", "Vargas", "Castillo", "Jimenez", "Herrera", "Medina", "Cruz"
]
DEPARTMENTS = ["Engineering", "Sales", "Marketing", "Finance", "HR", "Operations", "Legal"]
COUNTRIES = ["Mexico", "Colombia", "Argentina", "Chile", "Peru", "España", "USA"]
REGIONS = ["Norte", "Sur", "Este", "Oeste", "Centro", "CDMX", "Monterrey"]
CATEGORIES = ["Electronics", "Clothing", "Food", "Sports", "Books", "Tools", "Health"]
EVENT_TYPES = ["click", "view", "purchase", "logout", "login", "search", "add_to_cart"]
STATUS_LIST = ["completed", "pending", "failed", "refunded"]

def random_date(start_year=2020, end_year=2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    return (start + timedelta(days=random.randint(0, delta.days))).strftime("%Y-%m-%d")

def random_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"

def random_email(name):
    clean = name.lower().replace(" ", ".").replace("á","a").replace("é","e")
    domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "empresa.mx"]
    return f"{clean}{random.randint(1,99)}@{random.choice(domains)}"

# ─────────────────────────────────────────
# 1. employees.csv  (500 filas)
# ─────────────────────────────────────────
print("Generando employees.csv ...")
employees = []
for i in range(1, 501):
    name = random_name()
    salary = round(random.uniform(25000, 120000), 2)
    hire_date = random_date(2015, 2023)
    dept = random.choice(DEPARTMENTS)
    country = random.choice(COUNTRIES)
    # ~5% de salarios nulos para ejercicios de nulos
    if random.random() < 0.05:
        salary = ""
    employees.append([i, name, dept, salary, hire_date, country])

with open(os.path.join(CSV_DIR, "employees.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["employee_id", "name", "department", "salary", "hire_date", "country"])
    writer.writerows(employees)
print("  ✓ employees.csv (500 filas)")

# ─────────────────────────────────────────
# 2. products.csv  (100 filas)
# ─────────────────────────────────────────
print("Generando products.csv ...")
products = []
product_names_by_cat = {
    "Electronics": ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse", "Headphones", "Speaker"],
    "Clothing":    ["T-Shirt", "Jeans", "Jacket", "Dress", "Shoes", "Hat", "Socks", "Scarf"],
    "Food":        ["Coffee", "Tea", "Chocolate", "Cookies", "Juice", "Water", "Snacks", "Cereal"],
    "Sports":      ["Football", "Basketball", "Tennis Racket", "Yoga Mat", "Weights", "Bicycle"],
    "Books":       ["Novel", "Textbook", "Comic", "Biography", "Guide", "Dictionary"],
    "Tools":       ["Hammer", "Screwdriver", "Drill", "Saw", "Wrench", "Pliers"],
    "Health":      ["Vitamins", "Protein", "Mask", "Gloves", "Thermometer", "Bandages"]
}
for i in range(1, 101):
    cat = random.choice(CATEGORIES)
    pname = f"{random.choice(product_names_by_cat[cat])} {random.randint(100,999)}"
    price = round(random.uniform(5, 1500), 2)
    stock = random.randint(0, 500)
    products.append([i, pname, cat, price, stock])

with open(os.path.join(CSV_DIR, "products.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["product_id", "name", "category", "price", "stock"])
    writer.writerows(products)
print("  ✓ products.csv (100 filas)")

# ─────────────────────────────────────────
# 3. customers.csv  (300 filas)
# ─────────────────────────────────────────
print("Generando customers.csv ...")
customers = []
for i in range(1, 301):
    name = random_name()
    email = random_email(name)
    country = random.choice(COUNTRIES)
    signup = random_date(2018, 2024)
    # ~3% emails nulos
    if random.random() < 0.03:
        email = ""
    customers.append([i, name, email, country, signup])

with open(os.path.join(CSV_DIR, "customers.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["customer_id", "name", "email", "country", "signup_date"])
    writer.writerows(customers)
print("  ✓ customers.csv (300 filas)")

# ─────────────────────────────────────────
# 4. sales.csv  (2000 filas)
# ─────────────────────────────────────────
print("Generando sales.csv ...")
sales = []
emp_ids = [e[0] for e in employees]
prod_ids = [p[0] for p in products]
for i in range(1, 2001):
    emp_id = random.choice(emp_ids)
    prod_id = random.choice(prod_ids)
    amount = round(random.uniform(10, 5000), 2)
    sale_date = random_date(2021, 2024)
    region = random.choice(REGIONS)
    sales.append([i, emp_id, prod_id, amount, sale_date, region])

with open(os.path.join(CSV_DIR, "sales.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["sale_id", "employee_id", "product_id", "amount", "sale_date", "region"])
    writer.writerows(sales)
print("  ✓ sales.csv (2000 filas)")

# ─────────────────────────────────────────
# Parquet files — usando PySpark
# ─────────────────────────────────────────
print("\nInicializando SparkSession para archivos Parquet ...")
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DoubleType, TimestampType, ArrayType, MapType, LongType
)
from pyspark.sql import Row
import datetime as dt

spark = (SparkSession.builder
         .appName("GeneradorDatasets")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# ─────────────────────────────────────────
# 5. transactions.parquet  (5000 filas)
# ─────────────────────────────────────────
print("Generando transactions.parquet ...")
cust_ids = [c[0] for c in customers]
tx_rows = []
for i in range(1, 5001):
    cust_id = random.choice(cust_ids)
    amount = round(random.uniform(1, 10000), 2)
    status = random.choice(STATUS_LIST)
    # timestamp aleatorio entre 2021 y 2024
    base = dt.datetime(2021, 1, 1)
    ts = base + dt.timedelta(
        days=random.randint(0, 1460),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    metadata = {"source": random.choice(["web", "mobile", "api"]),
                "currency": random.choice(["MXN", "USD", "EUR", "COP"])}
    tx_rows.append(Row(
        tx_id=i,
        customer_id=cust_id,
        amount=amount,
        status=status,
        ts=ts,
        source=metadata["source"],
        currency=metadata["currency"]
    ))

tx_schema = StructType([
    StructField("tx_id",       IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("amount",      DoubleType(),  True),
    StructField("status",      StringType(),  True),
    StructField("ts",          TimestampType(), True),
    StructField("source",      StringType(),  True),
    StructField("currency",    StringType(),  True),
])
tx_df = spark.createDataFrame(tx_rows, schema=tx_schema)
tx_df.write.mode("overwrite").parquet(os.path.join(PARQUET_DIR, "transactions.parquet"))
print("  ✓ transactions.parquet (5000 filas)")

# ─────────────────────────────────────────
# 6. orders.parquet  (1000 filas)
# ─────────────────────────────────────────
print("Generando orders.parquet ...")
order_rows = []
for i in range(1, 1001):
    cust_id = random.choice(cust_ids)
    prod_id = random.choice(prod_ids)
    quantity = random.randint(1, 20)
    base = dt.datetime(2022, 1, 1)
    order_date = (base + dt.timedelta(days=random.randint(0, 730))).date()
    order_rows.append(Row(
        order_id=i,
        customer_id=cust_id,
        product_id=prod_id,
        quantity=quantity,
        order_date=str(order_date),
        status=random.choice(["shipped", "delivered", "cancelled", "processing"])
    ))

orders_schema = StructType([
    StructField("order_id",    IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id",  IntegerType(), True),
    StructField("quantity",    IntegerType(), True),
    StructField("order_date",  StringType(),  True),
    StructField("status",      StringType(),  True),
])
orders_df = spark.createDataFrame(order_rows, schema=orders_schema)
orders_df.write.mode("overwrite").parquet(os.path.join(PARQUET_DIR, "orders.parquet"))
print("  ✓ orders.parquet (1000 filas)")

# ─────────────────────────────────────────
# 7. events.parquet  (3000 filas)
# ─────────────────────────────────────────
print("Generando events.parquet ...")
event_rows = []
user_ids = list(range(1, 201))
for i in range(1, 3001):
    user_id = random.choice(user_ids)
    event_type = random.choice(EVENT_TYPES)
    base = dt.datetime(2023, 1, 1)
    event_ts = base + dt.timedelta(
        days=random.randint(0, 364),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )
    tags = random.sample(["promo", "organic", "paid", "referral", "direct"], k=random.randint(1, 3))
    event_rows.append(Row(
        event_id=i,
        user_id=user_id,
        event_type=event_type,
        tags=tags,
        event_ts=event_ts,
        session_id=f"sess_{random.randint(10000,99999)}"
    ))

events_schema = StructType([
    StructField("event_id",   IntegerType(), False),
    StructField("user_id",    IntegerType(), True),
    StructField("event_type", StringType(),  True),
    StructField("tags",       ArrayType(StringType()), True),
    StructField("event_ts",   TimestampType(), True),
    StructField("session_id", StringType(),  True),
])
events_df = spark.createDataFrame(event_rows, schema=events_schema)
events_df.write.mode("overwrite").parquet(os.path.join(PARQUET_DIR, "events.parquet"))
print("  ✓ events.parquet (3000 filas)")

spark.stop()

print("\n✅ Todos los datasets generados correctamente en:")
print(f"   CSV     → {CSV_DIR}")
print(f"   Parquet → {PARQUET_DIR}")
print("\nPuedes comenzar con Fase 1.")
