import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    # Inicializar la sesión de Spark
    # Nota: Desactivamos logs verbosos para ver la salida limpia
    spark = SparkSession.builder \
        .appName("SolucionRetosParquet") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    base_dir = os.path.dirname(os.path.abspath(__file__))
    datos_dir = os.path.join(base_dir, "datos")

    print("==================================================")
    print("🎯 SOLUCIÓN RETO 1: Carga de Datos Particionados")
    print("==================================================")
    
    # 1. Leer todo el dataset particionado
    path_ventas = os.path.join(datos_dir, "ventas_particionadas")
    df_ventas = spark.read.parquet(path_ventas)
    
    # 2. Imprimir esquema (debería incluir pais y anio automáticamente)
    print("\nEsquema de ventas detectado automáticamente por Spark:")
    df_ventas.printSchema()
    
    # 3. Filtrar por México y 2023
    print("\nVentas filtradas para México en 2023:")
    df_ventas.filter((col("pais") == "México") & (col("anio") == 2023)).show()
    
    # 4. Cargar directamente una sola partición
    print("\nCarga directa de la partición de España (todos los años):")
    path_espana = os.path.join(path_ventas, "pais=España")
    df_espana = spark.read.parquet(path_espana)
    df_espana.show()


    print("\n==================================================")
    print("🎯 SOLUCIÓN RETO 2: Proyección y Predicados (Explain)")
    print("==================================================")
    
    # Seleccionar columnas específicas y filtrar precio > 500
    df_optimizado = df_ventas.select("producto", "precio").filter(col("precio") > 500)
    
    print("\nDataFrame optimizado (producto y precio > 500):")
    df_optimizado.show()
    
    print("\nPlan físico detallado de Spark (.explain):")
    # Imprime el plan físico. Observa 'ReadSchema' y 'PushedFilters' en la salida.
    df_optimizado.explain(True)


    print("\n==================================================")
    print("🎯 SOLUCIÓN RETO 3: Fusión de Esquemas (Schema Merging)")
    print("==================================================")
    
    path_usuarios = os.path.join(datos_dir, "usuarios_esquema")
    
    # Intento 1: Leer sin mergeSchema (tomará el esquema de una de las partes y fallará en la otra)
    try:
        print("\nLeyendo SIN mergeSchema:")
        df_sin_merge = spark.read.parquet(path_usuarios)
        df_sin_merge.printSchema()
        df_sin_merge.show()
    except Exception as e:
        print(f"Error esperado al no fusionar esquemas: {e}")

    # Intento 2: Leer con mergeSchema activado
    print("\nLeyendo CON mergeSchema=true:")
    df_con_merge = spark.read \
        .option("mergeSchema", "true") \
        .parquet(path_usuarios)
        
    df_con_merge.printSchema()
    df_con_merge.show()

    spark.stop()

if __name__ == "__main__":
    main()
