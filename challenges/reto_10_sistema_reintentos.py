"""
Reto 10 — Sistema de reintentos
=================================

Crea una función:

    retry_operation(function, max_retries: int = 3)

Que ejecute una función y la reintente automáticamente si falla.

Comportamiento esperado:

    Intento 1 → ERROR
    Intento 2 → ERROR
    Intento 3 → SUCCESS ✓

Si todos los intentos fallan, debe lanzar la última excepción.

Ejemplo de uso:

    import random

    def unstable_operation():
        if random.random() < 0.7:  # 70% de probabilidad de fallar
            raise ConnectionError("Simulated connection failure")
        return "Data successfully fetched"

    result = retry_operation(unstable_operation, max_retries=3)
    print(result)

Requisitos:
    1. Mostrar en consola cada intento con su número
    2. Mostrar el error de cada intento fallido
    3. Esperar 1 segundo entre intentos (usa time.sleep)
    4. Registrar con logging (no solo print)
    5. Si se agotan los reintentos, lanzar un RuntimeError descriptivo

Formato de logging esperado:

    [INFO]  Intento 1/3 — ejecutando operación...
    [ERROR] Intento 1/3 — falló: Simulated connection failure
    [INFO]  Intento 2/3 — ejecutando operación...
    [ERROR] Intento 2/3 — falló: Simulated connection failure
    [INFO]  Intento 3/3 — ejecutando operación...
    [INFO]  Intento 3/3 — exitoso.

Prácticas:
    - try / except / raise
    - Funciones como parámetros (first-class functions)
    - Parámetros con valores por defecto
    - logging
    - time.sleep para backoff
"""

import logging
import time

logging.basicConfig(level=logging.INFO, format="[%(levelname)s]  %(message)s")
logger = logging.getLogger(__name__)


# Tu código aquí
