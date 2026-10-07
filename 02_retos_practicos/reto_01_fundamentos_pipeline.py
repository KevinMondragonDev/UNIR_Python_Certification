"""
Reto 1 — Fundamentos de Python y estructuración de pipelines
==============================================================

Dado:
    raw_events = [
        {"event_id": "evt_101", "timestamp": "2026-08-21T10:00:00Z", "status": "SUCCESS", "execution_time_ms": 150},
        {"event_id": "evt_102", "timestamp": "2026-08-21T10:01:00Z", "status": "FAILED",  "execution_time_ms": 500},
        {"event_id": "evt_103", "timestamp": "2026-08-21T10:02:00Z", "status": "SUCCESS", "execution_time_ms": 200},
        {"event_id": "evt_104", "timestamp": "2026-08-21T10:03:00Z", "status": "SUCCESS", "execution_time_ms": 180},
        {"event_id": "evt_105", "timestamp": "2026-08-21T10:04:00Z", "status": "FAILED",  "execution_time_ms": 1200},
    ]

Crea funciones para:
    get_successful_events(events: list) -> list
        Retorna la lista de eventos cuyo status es "SUCCESS".

    get_failed_events(events: list) -> list
        Retorna la lista de eventos cuyo status es "FAILED".

    get_average_execution_time(events: list) -> float
        Retorna el tiempo promedio de ejecución en ms (redondeado a 2 decimales).

    filter_slow_events(events: list, threshold_ms: int = 400) -> list
        Retorna los eventos con execution_time_ms mayor al umbral especificado.

Prácticas:
    - Listas y Diccionarios
    - Estructuras de control y filtrado
    - Funciones con type hints y docstrings amigables
    - Comprehensions de lista
"""

raw_events = [
    {"event_id": "evt_101", "timestamp": "2026-08-21T10:00:00Z", "status": "SUCCESS", "execution_time_ms": 150},
    {"event_id": "evt_102", "timestamp": "2026-08-21T10:01:00Z", "status": "FAILED",  "execution_time_ms": 500},
    {"event_id": "evt_103", "timestamp": "2026-08-21T10:02:00Z", "status": "SUCCESS", "execution_time_ms": 200},
    {"event_id": "evt_104", "timestamp": "2026-08-21T10:03:00Z", "status": "SUCCESS", "execution_time_ms": 180},
    {"event_id": "evt_105", "timestamp": "2026-08-21T10:04:00Z", "status": "FAILED",  "execution_time_ms": 1200},
]


# Tu código aquí
