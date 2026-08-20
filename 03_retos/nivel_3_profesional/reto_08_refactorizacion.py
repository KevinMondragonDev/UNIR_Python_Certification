"""
Reto 8 — Refactorización
==========================

Se te da intencionalmente código malo:

    def x(d):
        a = []
        for i in d:
            if i["age"] >= 18:
                a.append(i)
        return a

Problemas del código original:
    - Nombre de función no descriptivo (x)
    - Nombre de parámetro no descriptivo (d)
    - Variable acumuladora no descriptiva (a, i)
    - Sin type hints
    - Sin docstring
    - Lógica que se puede simplificar con una comprehension

Tu tarea:
    1. Refactorizar la función para que parezca código profesional:

        def get_adult_users(users: list[dict]) -> list[dict]:
            \"\"\"
            Filtra y retorna únicamente los usuarios mayores de edad.

            Args:
                users: Lista de diccionarios con claves 'name' y 'age'.

            Returns:
                Lista de usuarios con age >= 18.
            \"\"\"
            ...

    2. Agregar type hints en todos los parámetros y valor de retorno
    3. Agregar docstring con formato Google Style o NumPy Style
    4. Usar nombres descriptivos
    5. Simplificar con list comprehension donde sea posible

Prácticas:
    - Type hints
    - Docstrings
    - Nombres descriptivos
    - Estructura correcta de funciones
    - Código limpio (Clean Code)
"""

# Código original (NO modificar — solo úsalo como referencia)
def x(d):
    a = []
    for i in d:
        if i["age"] >= 18:
            a.append(i)
    return a


# Tu refactorización aquí
