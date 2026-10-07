"""
Tema 1 — Funciones | Ejercicio 3: Parámetros indeterminados (*args, **kwargs)
=============================================================

TEORÍA:
    Cuando no sabes cuántos argumentos recibirá una función, usas:

    *args   → captura argumentos posicionales como una TUPLA
    **kwargs → captura argumentos keyword como un DICCIONARIO

    def funcion(*args):
        for arg in args:
            print(arg)

    def funcion(**kwargs):
        for clave, valor in kwargs.items():
            print(f"{clave}: {valor}")

    def funcion(*args, **kwargs):
        pass  # combina ambos

EJERCICIOS:

1. Define una función `sumar_todo(*numeros)` que acepte cualquier cantidad
   de números y retorne su suma.
   Pruébala con: sumar_todo(1, 2), sumar_todo(1, 2, 3, 4, 5), sumar_todo(10)

2. Define una función `listar(*items)` que imprima cada item numerado:
       1. manzana
       2. banana
       3. naranja

3. Define una función `crear_perfil(**datos)` que imprima cada campo
   del perfil de usuario:
       nombre: Kevin
       edad: 23
       pais: México
   Llámala con: crear_perfil(nombre="Kevin", edad=23, pais="México")

4. Define una función `registrar(evento, *detalles, **metadata)` que:
   - Imprima el nombre del evento
   - Imprima cada detalle numerado
   - Imprima los metadatos como clave: valor

5. Explica con un comentario: ¿cuál es la diferencia entre *args y **kwargs?
   ¿Cuándo usarías uno u otro?

RETO INTEGRADOR:
6. Crea `agregar_metricas(nombre_pipeline: str, *valores_metricas, **tags)` que calcule la media de los valores pasados en *valores_metricas y devuelva un reporte en diccionario con el nombre, promedio y tags asociadas.
"""

# Ejercicio 1 — Tu código aquí


# Ejercicio 2 — Tu código aquí


# Ejercicio 3 — Tu código aquí


# Ejercicio 4 — Tu código aquí


# Ejercicio 5 — Explicación en comentario:
# ...


# Reto Integrador — Tu código aquí
