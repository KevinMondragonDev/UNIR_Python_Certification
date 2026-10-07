import os
import random
import pandas as pd

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    datos_dir = os.path.join(base_dir, "datos")
    os.makedirs(datos_dir, exist_ok=True)

    print("Generando datos de ventas particionados grandes (50,000 filas)...")
    
    productos = ["Laptop", "Smartphone", "Monitor", "Teclado", "Mouse", "Auriculares", "Cargador", "Tablet"]
    paises = ["Espana", "Mexico", "Colombia", "Argentina", "Chile", "Peru"]
    anios = [2022, 2023, 2024]
    
    random.seed(123)
    ventas_data = []
    for i in range(1, 50001):
        ventas_data.append({
            "id": i,
            "producto": random.choice(productos),
            "precio": round(random.uniform(10.0, 1500.0), 2),
            "pais": random.choice(paises),
            "anio": random.choice(anios)
        })
        
    df_ventas = pd.DataFrame(ventas_data)
    
    path_ventas = os.path.join(datos_dir, "ventas_particionadas")
    df_ventas.to_parquet(
        path_ventas,
        partition_cols=["pais", "anio"],
        index=False
    )

    print("Generando datos de usuarios grandes para Schema Merging (20,000 filas en total)...")
    nombres_semilla = ["Alice", "Bob", "Charlie", "David", "Eva", "Frank", "Grace", "Heidi", "Ivan", "Judy"]
    
    # Parte A: id, nombre, email
    usr_a_data = []
    for i in range(1, 10001):
        nombre = f"{random.choice(nombres_semilla)}_{i}"
        usr_a_data.append({
            "id": i,
            "nombre": nombre,
            "email": f"{nombre.lower()}@example.com"
        })
    df_usr_a = pd.DataFrame(usr_a_data)
    
    path_usr_a = os.path.join(datos_dir, "usuarios_esquema", "parte_a")
    os.makedirs(path_usr_a, exist_ok=True)
    df_usr_a.to_parquet(os.path.join(path_usr_a, "data.parquet"), index=False)

    # Parte B: id, nombre, telefono (nueva columna, sin email)
    usr_b_data = []
    for i in range(10001, 20001):
        nombre = f"{random.choice(nombres_semilla)}_{i}"
        usr_b_data.append({
            "id": i,
            "nombre": nombre,
            "telefono": f"+34{random.randint(600000000, 699999999)}"
        })
    df_usr_b = pd.DataFrame(usr_b_data)
    
    path_usr_b = os.path.join(datos_dir, "usuarios_esquema", "parte_b")
    os.makedirs(path_usr_b, exist_ok=True)
    df_usr_b.to_parquet(os.path.join(path_usr_b, "data.parquet"), index=False)

    print("¡Archivos Parquet grandes generados correctamente en la carpeta 'datos'!")

if __name__ == "__main__":
    main()
