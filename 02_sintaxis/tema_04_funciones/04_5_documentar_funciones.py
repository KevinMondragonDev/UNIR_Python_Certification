"""
Tema 4 — Funciones | Ejercicio 5: Documentar funciones
========================================================

TEORÍA:
    Un docstring es una cadena de texto que documenta una función.
    Va inmediatamente después de la línea `def`, entre triple comillas.

    Formato Google Style (recomendado para Data Engineering):

        def mi_funcion(param1: int, param2: str) -> bool:
            \"\"\"
            Descripción breve de la función.

            Args:
                param1: Descripción del primer parámetro.
                param2: Descripción del segundo parámetro.

            Returns:
                Descripción de lo que retorna.

            Raises:
                ValueError: Cuando param1 es negativo.
            \"\"\"

    Para ver el docstring: help(mi_funcion) o mi_funcion.__doc__

EJERCICIOS:

1. Toma esta función y agrégale un docstring completo en Google Style:

       def calcular_imc(peso_kg, altura_m):
           return peso_kg / (altura_m ** 2)

2. Documenta esta función con type hints Y docstring:

       def filtrar_adultos(usuarios):
           return [u for u in usuarios if u["age"] >= 18]

3. Crea una función `convertir_temperatura(valor, de_unidad, a_unidad)` que:
   - Convierta entre Celsius, Fahrenheit y Kelvin
   - Tenga type hints completos
   - Tenga docstring con Google Style
   - Lance ValueError si la unidad no es válida

4. Usa help() para ver el docstring de las siguientes funciones built-in:
   - help(len)
   - help(sorted)
   - help(range)
   Copia en comentarios qué aprendiste de cada una.

5. ¿Por qué es importante documentar funciones? Escribe tu reflexión
   en un comentario de al menos 3 líneas.
"""

# Ejercicio 1 — Tu código aquí


# Ejercicio 2 — Tu código aquí


# Ejercicio 3 — Tu código aquí


# Ejercicio 4 — Tu código aquí


# Ejercicio 5 — Tu reflexión:
# ...
