"""
Utilidad para importar los módulos del proyecto.

Python no permite `import 01_ingesta` porque el nombre empieza con un número.
Usamos importlib para cargarlos por ruta:

    from _modulos import cargar
    ingesta = cargar("01_ingesta")
    ingesta.crear_spark()
"""

import os
import importlib.util

_DIR = os.path.dirname(os.path.abspath(__file__))


def cargar(nombre):
    ruta = os.path.join(_DIR, f"{nombre}.py")
    spec = importlib.util.spec_from_file_location(f"proyecto_{nombre}", ruta)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo
