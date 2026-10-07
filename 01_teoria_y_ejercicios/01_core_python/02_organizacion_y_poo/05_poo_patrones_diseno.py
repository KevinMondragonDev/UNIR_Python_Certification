"""
Tema 2 — Organización del código y POO | Ejercicio 5: Patrones de Diseño POO
========================================================================================

TEORÍA:
    Los patrones de diseño son soluciones probadas a problemas recurrentes en el 
    diseño de software. En Data Engineering y arquitectura de pipelines, los siguientes
    patrones de POO son fundamentales:

    1. Singleton (Instancia Única):
       Garantiza que una clase tenga una sola instancia global y proporciona un punto 
       de acceso único a ella (ej. Gestión de conexiones a BD, SparkSession, Configuración).

    2. Factory Method (Fábrica de Objetos):
       Define una interfaz para crear objetos, pero permite a las subclases o métodos 
       decidir qué clase instanciar dinámicamente según parámetros (ej. Conectores S3 vs Postgres vs Kafka).

    3. Strategy Pattern (Estrategia):
       Permite definir una familia de algoritmos, encapsular cada uno y hacerlos intercambiables
       en tiempo de ejecución (ej. Algoritmos de Imputación de nulos, Serializadores JSON vs Parquet).

    4. Decorator Pattern (Decorador de Métodos/Clases):
       Añade responsabilidades o comportamientos adicionales a un objeto dinámicamente sin 
       modificar su código base (ej. Retry logic, Logging de auditoría, Medición de latencia).

    5. Observer Pattern (Observador / Publicador-Suscriptor):
       Establece una relación uno-a-muchos entre objetos de modo que cuando uno cambia de estado,
       todos sus dependientes son notificados automáticamente (ej. Alertas de pipeline a Slack, Email, CloudWatch).

EJERCICIOS:

1. Implementa el patrón Singleton para la clase `SparkSessionManager`:
   - Atributo privado de clase `_instance = None`.
   - Método de clase `get_instance(cls, app_name: str)` que devuelva la instancia existente
     o cree una nueva si es `None`.
   - Verifica que dos llamadas a `get_instance()` devuelvan exactamente el mismo objeto (`id(inst1) == id(inst2)`).

2. Implementa el patrón Factory Method para conectores de almacenamiento `StorageConnectorFactory`:
   - Clase base abstracta `BaseStorageConnector` con método `leer_datos(path: str) -> list`.
   - Clases concretas: `LocalStorageConnector`, `S3StorageConnector`, `GCSStorageConnector`.
   - Clase `StorageConnectorFactory` con un método estático `crear_conector(tipo_almacenamiento: str) -> BaseStorageConnector`
     que devuelva el conector adecuado según la cadena recibida ("local", "s3", "gcs"). Lanzar `ValueError` si el tipo no es soportado.

3. Implementa el patrón Strategy para un formateador de datos `DataExporter`:
   - Interfaz `ExportStrategy` (ABC) con método `exportar(data: list) -> str`.
   - Estrategias concretas:
     * `JSONExportStrategy`: Convierte la lista a un string JSON formateado.
     * `CSVExportStrategy`: Convierte una lista de diccionarios a una cadena CSV con encabezados.
   - Clase `DataExporter` que reciba una `ExportStrategy` en su constructor o mediante un setter y tenga un método `procesar_exportacion(data: list) -> str`.

4. Implementa el patrón Decorador (usando wrappers POO o decoradores de función) `retry_on_failure`:
   - Crea un decorador `retry_on_failure(max_intentos: int = 3, delay: float = 0.5)` que envuelva métodos de carga de datos.
   - Si la función decorada lanza una excepción, debe reintentar hasta `max_intentos` veces imprimiendo una advertencia antes de fallar definitivamente.
   - Simula una función `conectar_api_inestable()` que falle las primeras 2 veces y funcione a la tercera.

5. Implementa el patrón Observer para un sistema de alertas de ETL `PipelineNotifier`:
   - Interfaz `Observer` con método `notificar(evento: str, mensaje: str)`.
   - Clases observadoras concretas: `ConsoleLoggerObserver` y `SlackAlertObserver`.
   - Clase `ETLPipeline` (Sujeto):
     * Métodos `registrar_observador(obs)`, `eliminar_observador(obs)`, `notificar_observadores(evento, mensaje)`.
     * En su método `ejecutar_pipeline()`, notifica los eventos `"PIPELINE_STARTED"`, `"PIPELINE_SUCCESS"` o `"PIPELINE_FAILED"`.

RETO INTEGRADOR:
6. Diseña la arquitectura POO de un `FrameworkETLProduccion` combinando múltiples patrones:
   - Usa **Singleton** para gestionar una única instancia de `GlobalPipelineConfig`.
   - Usa **Factory Method** (`DataSourceFactory`) para instanciar el extractor adecuado ("s3", "sql").
   - Usa **Strategy Pattern** (`DataCleaningStrategy`) para aplicar transformaciones intercambiables.
   - Usa **Observer Pattern** (`PipelineNotifier`) para notificar el estado final del flujo a los observadores registrados.
"""

from abc import ABC, abstractmethod
import json
import time
from typing import Any, Callable, Dict, List, Optional


# Ejercicio 1 — Tu código aquí (Singleton)


# Ejercicio 2 — Tu código aquí (Factory Method)


# Ejercicio 3 — Tu código aquí (Strategy Pattern)


# Ejercicio 4 — Tu código aquí (Decorator Pattern)


# Ejercicio 5 — Tu código aquí (Observer Pattern)


# Reto Integrador — Tu código aquí

