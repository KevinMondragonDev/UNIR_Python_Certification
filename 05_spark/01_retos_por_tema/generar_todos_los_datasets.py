import os
import random
import pandas as pd
from datetime import datetime, timedelta

def main():
    base_spark_dir = os.path.dirname(os.path.abspath(__file__))
    print("Iniciando la generación de Datasets ASCII-safe en formato Parquet...")

    # Semilla fija para reproducibilidad
    random.seed(42)

    # Nombres y departamentos semilla (sin acentos ni caracteres especiales)
    nombres_semilla = ["Ana", "Luis", "Carla", "Pedro", "Sofia", "Jorge", "Maria", "Carlos", "Lucia", "Andres", "Gabriela", "Felipe", "Elena", "Santiago", "Valeria", "Mateo", "Ricardo", "Diana", "Tomas", "Isabel"]
    apellidos_semilla = ["Gomez", "Rodriguez", "Lopez", "Martinez", "Perez", "Gonzalez", "Sanchez", "Ramirez", "Diaz", "Hernandez", "Torres", "Flores", "Morales", "Castillo", "Vazquez", "Garcia"]
    departamentos = ["Ingenieria", "Marketing", "Ventas", "Finanzas", "Recursos Humanos", "Soporte"]
    paises = ["Mexico", "Colombia", "Argentina", "Espana", "Chile", "Peru"]
    trimestres = ["Q1", "Q2", "Q3", "Q4"]
    estados_venta = ["completado", "pendiente", "cancelado", None]

    # --- 1. Módulo 4.3 (DataFrames) ---
    print("\n[4.3] Generando empleados.parquet...")
    dir_43 = os.path.join(base_spark_dir, "03_dataframes", "datos")
    os.makedirs(dir_43, exist_ok=True)
    df_43 = pd.DataFrame([
        {
            "nombre": f"{random.choice(nombres_semilla)} {random.choice(apellidos_semilla)}",
            "edad": random.randint(18, 65),
            "departamento": random.choice(departamentos),
            "salario": round(random.uniform(30000.0, 140000.0), 2)
        } for _ in range(100000)
    ])
    df_43.to_parquet(os.path.join(dir_43, "empleados.parquet"), index=False)

    # --- 2. Módulo 4.4 (Transformaciones) ---
    print("[4.4] Generando empleados.parquet...")
    dir_44 = os.path.join(base_spark_dir, "04_transformaciones", "datos")
    os.makedirs(dir_44, exist_ok=True)
    df_44 = pd.DataFrame([
        {
            "nombre": f"{random.choice(nombres_semilla)} {random.choice(apellidos_semilla)}",
            "edad": random.randint(18, 65),
            "departamento": random.choice(departamentos),
            "salario": round(random.uniform(30000.0, 140000.0), 2),
            "pais": random.choice(paises)
        } for _ in range(100000)
    ])
    df_44.to_parquet(os.path.join(dir_44, "empleados.parquet"), index=False)

    # --- 3. Módulo 4.5 (Agregaciones) ---
    print("[4.5] Generando ventas_trimestrales.parquet...")
    dir_45 = os.path.join(base_spark_dir, "05_agregaciones", "datos")
    os.makedirs(dir_45, exist_ok=True)
    df_45 = pd.DataFrame([
        {
            "nombre": f"{random.choice(nombres_semilla)} {random.choice(apellidos_semilla)}",
            "departamento": random.choice(departamentos),
            "salario": round(random.uniform(30000.0, 140000.0), 2),
            "trimestre": random.choice(trimestres),
            "anio": random.choice([2023, 2024])
        } for _ in range(100000)
    ])
    df_45.to_parquet(os.path.join(dir_45, "ventas_trimestrales.parquet"), index=False)

    # --- 4. Módulo 4.6 (Joins) ---
    print("[4.6] Generando empleados.parquet y departamentos.parquet...")
    dir_46 = os.path.join(base_spark_dir, "06_joins", "datos")
    os.makedirs(dir_46, exist_ok=True)
    
    dept_ids = list(range(100, 200))
    ciudades = ["CDMX", "Bogota", "Buenos Aires", "Lima", "Santiago", "Madrid"]
    df_depts = pd.DataFrame([
        {
            "dept_id": d,
            "nombre_dept": f"Depto_{d}",
            "ciudad": random.choice(ciudades)
        } for d in dept_ids
    ])
    df_depts.to_parquet(os.path.join(dir_46, "departamentos.parquet"), index=False)

    df_emps_46 = pd.DataFrame([
        {
            "id": i,
            "nombre": f"{random.choice(nombres_semilla)} {random.choice(apellidos_semilla)}",
            "dept_id": random.choice(dept_ids) if random.random() > 0.05 else (999 if random.random() > 0.5 else None)
        } for i in range(1, 100001)
    ])
    df_emps_46.to_parquet(os.path.join(dir_46, "empleados.parquet"), index=False)

    # --- 5. Módulo 4.7 (Spark SQL) ---
    print("[4.7] Generando empleados.parquet...")
    dir_47 = os.path.join(base_spark_dir, "07_spark_sql", "datos")
    os.makedirs(dir_47, exist_ok=True)
    df_47 = pd.DataFrame([
        {
            "nombre": f"{random.choice(nombres_semilla)} {random.choice(apellidos_semilla)}",
            "edad": random.randint(18, 65),
            "departamento": random.choice(departamentos),
            "salario": round(random.uniform(30000.0, 140000.0), 2),
            "pais": random.choice(paises)
        } for _ in range(100000)
    ])
    df_47.to_parquet(os.path.join(dir_47, "empleados.parquet"), index=False)

    # --- 6. Módulo 4.8 (Funciones Built-in) ---
    print("[4.8] Generando usuarios.parquet y fechas.parquet...")
    dir_48 = os.path.join(base_spark_dir, "08_funciones_builtin", "datos")
    os.makedirs(dir_48, exist_ok=True)
    
    usr_rows = []
    cargos = ["Ingenieria-Senior", "Marketing-Junior", "Ventas-Senior", "Finanzas-Analyst", "Soporte-Helper", "RRHH-Manager"]
    for i in range(1, 100001):
        nombre_crudo = f"  {random.choice(nombres_semilla).lower()} {random.choice(apellidos_semilla).upper()}  "
        email = f"{random.choice(nombres_semilla)}.{random.choice(apellidos_semilla)}@empresa.com".replace(" ", "").lower()
        cargo = random.choice(cargos)
        usr_rows.append({"nombre": nombre_crudo, "email": email, "cargo": cargo})
    pd.DataFrame(usr_rows).to_parquet(os.path.join(dir_48, "usuarios.parquet"), index=False)

    fechas_rows = []
    for _ in range(100000):
        random_days_start = random.randint(0, 1500)
        random_days_len = random.randint(1, 365)
        fecha_ini = start_date = datetime(2020, 1, 1) + timedelta(days=random_days_start)
        fecha_f = fecha_ini + timedelta(days=random_days_len)
        fechas_rows.append({
            "nombre": f"{random.choice(nombres_semilla)} {random.choice(apellidos_semilla)}",
            "fecha_inicio": fecha_ini.strftime("%Y-%m-%d"),
            "fecha_fin": fecha_f.strftime("%Y-%m-%d")
        })
    pd.DataFrame(fechas_rows).to_parquet(os.path.join(dir_48, "fechas.parquet"), index=False)

    # --- 7. Módulo 4.9 (Datos Nulos) ---
    print("[4.9] Generando empleados_nulos.parquet...")
    dir_49 = os.path.join(base_spark_dir, "09_datos_nulos", "datos")
    os.makedirs(dir_49, exist_ok=True)
    nulos_rows = []
    for _ in range(100000):
        nombre = f"{random.choice(nombres_semilla)} {random.choice(apellidos_semilla)}" if random.random() > 0.08 else None
        edad = random.randint(18, 65) if random.random() > 0.1 else None
        depto = random.choice(departamentos) if random.random() > 0.12 else None
        salario = round(random.uniform(30000.0, 140000.0), 2) if random.random() > 0.15 else (float("nan") if random.random() > 0.5 else None)
        nulos_rows.append({
            "nombre": nombre,
            "edad": edad,
            "departamento": depto,
            "salario": salario
        })
    pd.DataFrame(nulos_rows).to_parquet(os.path.join(dir_49, "empleados_nulos.parquet"), index=False)

    # --- 8. Módulo 4.11 (Gran Integrador) ---
    print("[4.11] Generando ventas.parquet y productos.parquet...")
    dir_411 = os.path.join(base_spark_dir, "11_gran_integrador", "datos")
    os.makedirs(dir_411, exist_ok=True)
    
    prod_ids = list(range(1000, 1500))
    prod_cats = ["Electronica", "Perifericos", "Libros", "Muebles", "Ropa", "Deportes"]
    df_prods_411 = pd.DataFrame([
        {
            "producto_id": p,
            "nombre_producto": f"Prod_{p}",
            "categoria": random.choice(prod_cats),
            "precio_unidad": round(random.uniform(5.0, 2000.0), 2)
        } for p in prod_ids
    ])
    df_prods_411.to_parquet(os.path.join(dir_411, "productos.parquet"), index=False)

    ventas_rows = []
    start_date_sales = datetime(2024, 1, 1)
    for i in range(1, 100001):
        prod_id = random.choice(prod_ids) if random.random() > 0.05 else None
        fecha_venta = (start_date_sales + timedelta(days=random.randint(0, 180))).strftime("%Y-%m-%d") if random.random() > 0.05 else None
        cantidad = random.randint(1, 10) if random.random() > 0.05 else None
        estado = random.choice(estados_venta)
        ventas_rows.append({
            "venta_id": i,
            "producto_id": prod_id,
            "fecha": fecha_venta,
            "cantidad": cantidad,
            "estado": estado
        })
    pd.DataFrame(ventas_rows).to_parquet(os.path.join(dir_411, "ventas.parquet"), index=False)

    print("\n¡Proceso finalizado! Todos los módulos de Spark ahora cuentan con sus carpetas 'datos/' y archivos .parquet masivos y limpios.")

if __name__ == "__main__":
    main()
