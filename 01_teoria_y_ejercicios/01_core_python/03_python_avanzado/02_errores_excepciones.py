"""
Tema 3 — Python Avanzado | Ejercicio 2: Errores y excepciones
=================================================================

TEORÍA:
    Python usa excepciones para manejar errores en tiempo de ejecución.

    Estructura básica:
        try:
            # código que puede fallar
        except TipoDeError as e:
            # qué hacer si falla
        else:
            # se ejecuta si NO hubo error
        finally:
            # se ejecuta SIEMPRE (con o sin error)

    Excepciones comunes:
        ValueError      → valor incorrecto (int("abc"))
        TypeError       → tipo incorrecto (1 + "a")
        KeyError        → clave no existe en diccionario
        IndexError      → índice fuera de rango
        FileNotFoundError → archivo no encontrado
        ZeroDivisionError → división entre cero
        AttributeError  → atributo no existe

    Lanzar excepciones:
        raise ValueError("Mensaje descriptivo")

    Excepciones personalizadas:
        class MiError(Exception):
            pass

EJERCICIOS:

1. Maneja estos errores con try/except. Para cada uno escribe qué excepción
   captura y por qué:

       a) int("hola")
       b) [1, 2, 3][10]
       c) {"nombre": "Kevin"}["edad"]
       d) 10 / 0
       e) None.strip()

2. Crea una función `convertir_a_entero(valor)` que:
   - Intente convertir el valor a int
   - Si falla, retorne None (no que truene el programa)
   - Pruébala con: "42", "3.14", "abc", None, True

3. Crea una función `leer_archivo(ruta)` que:
   - Intente abrir y leer el archivo
   - Si no existe, retorne un string vacío y loguee el error
   - Si el archivo existe pero está vacío, retorne ""
   - Siempre cierre el archivo (usa finally o with)

4. Crea una clase de excepción personalizada `DatosInvalidosError`
   y úsala en una función `validar_edad(edad)` que:
   - Levante DatosInvalidosError si la edad no es un entero
   - Levante DatosInvalidosError si la edad < 0 o > 120
   - Retorne True si la edad es válida

5. Crea una función `pipeline_seguro(datos)` que procese una lista de registros
   y capture errores por registro sin detener el pipeline:
   - Si un registro falla, loguearlo y continuar con el siguiente
   - Al final retornar cuántos registros se procesaron y cuántos fallaron

RETO INTEGRADOR:
4. Crea `procesar_archivo_seguro(path: str)` que utilice la estructura `try-except-else-finally` capturando `FileNotFoundError` y `ValueError`, asegurando que el archivo siempre se cierre en el bloque `finally`.
"""

import logging

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# Ejercicio 1 — Tu código aquí


# Ejercicio 2 — Tu código aquí


# Ejercicio 3 — Tu código aquí


# Ejercicio 4 — Tu código aquí


# Ejercicio 5 — Tu código aquí


# Reto Integrador — Tu código aquí
