"""
Tema 2 — Organización del código y POO | Ejercicio 4: Métodos Mágicos (Dunder Methods)
========================================================================================

TEORÍA:
    Los métodos mágicos o "dunder methods" (double underscore: `__método__`) 
    permiten personalizar el comportamiento de los objetos en Python ante 
    operadores nativos, funciones built-in y sintaxis del lenguaje.

    Principales familias de Dunder Methods:

    1. Representación e Inspección:
       - `__str__(self)`: Representación amigable para usuario final (`print()`, `str()`).
       - `__repr__(self)`: Representación inequívoca para desarrolladores (`repr()`, depuración).

    2. Comparación y Hash (Hacer objetos ordenables o utilizables en Sets/Dicts):
       - `__eq__(self, other)`: Define la igualdad (`==`).
       - `__hash__(self)`: Permite usar el objeto como clave en diccionarios o en conjuntos `set`.
       - `__lt__(self, other)`: Define orden menor que (`<`), habilitando `sorted()` y `list.sort()`.

    3. Emulación de Contenedores (Comportamiento tipo Lista o Diccionario):
       - `__len__(self)`: Devuelve el tamaño (`len(obj)`).
       - `__getitem__(self, key)`: Permite acceso por índice o clave (`obj[key]`).
       - `__setitem__(self, key, value)`: Permite asignación (`obj[key] = val`).
       - `__iter__(self)` y `__next__(self)`: Hacen al objeto iterable en un bucle `for`.
       - `__contains__(self, item)`: Permite la sintaxis `item in obj`.

    4. Gestores de Contexto (`with` statement):
       - `__enter__(self)`: Inicializa el recurso al entrar al bloque `with`.
       - `__exit__(self, exc_type, exc_val, exc_tb)`: Libera recursos/limpia al salir.

    5. Invocación como Función:
       - `__call__(self, *args, **kwargs)`: Permite llamar al objeto como si fuera una función (`obj()`).

EJERCICIOS:

1. Crea una clase `DataRow` que represente una fila de un dataset:
   - Atributos: `row_id: str`, `timestamp: str`, `payload: dict`.
   - Implementa `__eq__` para que dos filas sean iguales si tienen el mismo `row_id`.
   - Implementa `__hash__` basado en `row_id` para poder insertar `DataRow` en un `set()`.
   - Implementa `__lt__` basado en `timestamp` para poder ordenar listas de `DataRow`.
   - Demuestra eliminar duplicados con `set()` y ordenar con `sorted()`.

2. Crea una clase `PartitionBuffer` que actúe como contenedor de datos:
   - Atributos: `partition_id: int`, `_records: list`.
   - Métodos dunder a implementar:
     * `__len__`: Retorna el número de registros en `_records`.
     * `__getitem__`: Permite indexar (`buffer[0]`) y hacer slicing (`buffer[1:3]`).
     * `__contains__`: Verifica si un registro específico está en la partición (`record in buffer`).
     * `__repr__`: Retorna `PartitionBuffer(partition_id=X, count=Y)`.
   - Realiza pruebas de cada método dunder.

3. Crea un Gestor de Contexto personalizado `PipelineTimer`:
   - En `__enter__`, registra el tiempo inicial (usa `time.perf_counter()`).
   - En `__exit__`, calcula la duración en segundos e imprime: "Etapa '[nombre]' ejecutada en X.XXXX segundos".
   - Soporta manejo de excepciones: si ocurre un error dentro del bloque `with`, imprime el error pero permite que continúe si un parámetro `ignore_errors=True`.
   - Prueba medir la ejecución de un bloque simulado con `time.sleep()`.

4. Crea una clase `FilterTransformer` que sea invocable (`__call__`):
   - Atributo: `predicado` (una función lambda o función estándar que recibe un elemento y devuelve un bool).
   - Implementa `__call__(self, dataset: list) -> list` de forma que al llamar `transformer(mi_lista)` filtre los elementos usando el predicado.
   - Crea dos instancias: `filtro_pares` y `filtro_no_nulos` y aplícalos llamando a los objetos como funciones.

5. ¿Por qué es fundamental que `__repr__` sea informativo en clases que representan fuentes de datos o estructuras de Big Data? Da 2 razones aplicadas a Data Engineering.

RETO INTEGRADOR:
6. Crea un contenedor de dataset profesional `DataFrameLite`:
   - Atributos: `nombre: str`, `_data: list` (lista de diccionarios).
   - Implementa `__len__`, `__getitem__` (acceso por índice o slicing), `__iter__` y `__repr__`.
   - Implementa `__call__(self, campo: str, valor: Any) -> list` para que al llamar `df("categoria", "A")` filtre las filas por valor de columna.
   - Implementa gestor de contexto (`__enter__` y `__exit__`) para mostrar logs de inicio y liberación de memoria.
"""

import time
from typing import Any, Callable, Dict, List, Optional


# Ejercicio 1 — Tu código aquí


# Ejercicio 2 — Tu código aquí


# Ejercicio 3 — Tu código aquí


# Ejercicio 4 — Tu código aquí


# Ejercicio 5 — Tu reflexión:
# ...


# Reto Integrador — Tu código aquí

