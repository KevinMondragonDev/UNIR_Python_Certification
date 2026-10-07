"""
Tema 2 — Organización del código y POO | Ejercicio 3: POO Avanzado
========================================================================================

TEORÍA:
    La Programación Orientada a Objetos en Python ofrece características avanzadas 
    para construir componentes robustos, escalables y mantenibles en producción.

    1. Encapsulamiento con `@property`:
       Permite definir getters, setters y deleters controlando el acceso a los atributos
       y aplicando validaciones sin cambiar la sintaxis de acceso (sin necesidad de get_x()).

       Ejemplo:
           class Sensor:
               def __init__(self, temp: float):
                   self._temperatura = temp

               @property
               def temperatura(self) -> float:
                   return self._temperatura

               @temperatura.setter
               def temperatura(self, valor: float):
                   if valor < -273.15:
                       raise ValueError("Temperatura por debajo del cero absoluto.")
                   self._temperatura = valor

    2. Métodos de Clase (@classmethod) y Estáticos (@staticmethod):
       - `@classmethod`: Recibe `cls` como primer argumento. Se usa habitualmente para
         constructores alternativos (factories) o lógica que afecta a la clase completa.
       - `@staticmethod`: No recibe ni `self` ni `cls`. Es una función auxiliar pura
         agrupada dentro del namespace de la clase.

       Ejemplo:
           class DataRecord:
               def __init__(self, payload: dict):
                   self.payload = payload

               @classmethod
               def from_json_str(cls, json_str: str):
                   import json
                   data = json.loads(json_str)
                   return cls(data)

               @staticmethod
               def es_valido(payload: dict) -> bool:
                   return "id" in payload

    3. Clases Abstractas (`abc.ABC` y `@abstractmethod`):
       Definen interfaces o contratos que las subclases OBLIGATORIAMENTE deben implementar.
       No se pueden instanciar directamente.

       Ejemplo:
           from abc import ABC, abstractmethod

           class BaseExtractor(ABC):
               @abstractmethod
               def extraer(self) -> list:
                   pass

    4. Herencia Múltiple y Mixins:
       Una clase puede heredar de varias clases. Los Mixins son clases pequeñas que aportan
       funcionalidad reutilizable (ej. logging, serialización) a otras clases.

EJERCICIOS:

1. Crea una clase `ConfiguradorPipeline` con:
   - Atributos privados: `_max_workers` (int) y `_batch_size` (int).
   - Usa `@property` y `@setter` para ambos atributos:
     * `max_workers` debe ser entero entre 1 y 64 (lanzar ValueError si no lo es).
     * `batch_size` debe ser un número entero mayor a 0 y múltiplo de 100.
   - Instancia la clase, modifica los valores correctamente y prueba lanzar una excepción.

2. Crea una clase `DatabaseConnection` con:
   - Constructor `__init__(self, host: str, port: int, db_name: str)`.
   - `@classmethod` `from_dict(cls, config: dict)` que cree una instancia desde un diccionario.
   - `@classmethod` `from_uri(cls, uri: str)` que parse el string "postgresql://user:pass@host:port/db_name" e instancie la clase.
   - `@staticmethod` `validar_puerto(port: int) -> bool` que retorne True si el puerto está entre 1024 y 65535.

3. Crea una interfaz abstracta `BaseTransformer` usando `abc.ABC`:
   - Método abstracto `@abstractmethod` `transformar(self, data: list) -> list`.
   - Crea dos clases concretas que hereden de `BaseTransformer`:
     * `FiltroNulosTransformer`: Elimina elementos `None` o diccionarios vacíos de la lista.
     * `NormalizadorTextoTransformer`: Convierte todas las cadenas de texto dentro de la lista a minúsculas y elimina espacios sobrantes (`strip()`).
   - Prueba ambas transformaciones sobre una lista de datos de prueba.

4. Implementa el patrón Mixin con `AuditableMixin` y `PipelineTask`:
   - `AuditableMixin`: Posee un atributo `_logs` (lista) y métodos `log(mensaje: str)` y `obtener_logs() -> list`.
   - `BaseTask`: Clase base con método `ejecutar()`.
   - `ETLTask`: Hereda de `BaseTask` y `AuditableMixin`. En su método `ejecutar()`, debe registrar logs de inicio, procesamiento y fin.

5. ¿Cuál es la diferencia conceptual entre `@staticmethod`, `@classmethod` y un método de instancia normal (`self`)?
   Escribe un comentario explicando cuándo usarías cada uno en un entorno de Data Engineering.

RETO INTEGRADOR:
6. Diseña un conector de datos avanzado extensible `BaseCloudLoader`:
   - Clase abstracta `BaseCloudLoader(ABC)` con `@abstractmethod` `cargar(self, data: list) -> bool`.
   - Incluye una `@property` `is_connected` que devuelva un booleano privado `_is_connected`.
   - Incluye un `@classmethod` `from_env(cls)` que cree una instancia usando variables de entorno simuladas.
   - Crea una clase concreta `S3Loader(BaseCloudLoader, AuditableMixin)` que implemente `cargar()`, registre logs de auditoría y verifique que `data` no esté vacía.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import re


# Ejercicio 1 — Tu código aquí


# Ejercicio 2 — Tu código aquí


# Ejercicio 3 — Tu código aquí


# Ejercicio 4 — Tu código aquí


# Ejercicio 5 — Tu reflexión:
# ...


# Reto Integrador — Tu código aquí

