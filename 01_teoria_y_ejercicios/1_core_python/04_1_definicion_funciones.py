"""
Tema 4 — Funciones | Ejercicio 1: Definición de funciones
===========================================================

TEORÍA:
    Una función es un bloque de código reutilizable que se ejecuta cuando se llama.
    Se define con la palabra clave `def`.

    def nombre_funcion():
        # cuerpo de la función
        pass

EJERCICIOS:

1. Define una función llamada `saludar` que imprima "¡Hola, mundo!".
   Llámala 3 veces.

2. Define una función `mostrar_separador` que imprima una línea de 40 guiones.
   Ejemplo: ----------------------------------------

3. Define una función `presentarme` que imprima tu nombre, edad y ocupación.


4. ¿Qué pasa si llamas una función antes de definirla? Pruébalo y explica el error
   en un comentario.
"""

# Ejercicio 1 — Tu código aquí
def saludar(cantidad_de_saludos:int):
   lista_saludos:list = []
   for saludos in range(cantidad_de_saludos):
      lista_saludos.append("Hello World")
   return lista_saludos



# Ejercicio 2 — Tu código aquí
def mostrar_separador(numero_lineas:int):
   for linea in range(numero_lineas):
      print("-", end="")

# Ejercicio 3 — Tu código aquí
def presentarme(nombre:str, apellido:str , edad: str):
   print("Mi nombre es: " + nombre)
   print("Me apellido: " + apellido)
   print("Mi edad es: " + edad )



# Ejercicio 4 — Tu código aquí (prueba el error y explícalo en comentario)
#"No existe no podemos llamar una funcion antes de crearla, porque ¿a que llamas?"



#Ejecucion
def main():
   saludos_para_la_banda = saludar(5)
   for saludos in range(len(saludos_para_la_banda)):
    print(saludos_para_la_banda[saludos])

   mostrar_separador(40)
if __name__ == "__main__":
    main()