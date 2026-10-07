# 🎮 Reto 3 — Clases y POO: Tu Propio Motor de Videojuego

> **Objetivo:** Agrupar datos (atributos) y comportamientos (métodos) en una sola entidad usando Programación Orientada a Objetos (POO).

---

## Contexto

Vamos a crear la lógica base para un personaje de un juego de acción. Cada personaje tiene vida, energía y habilidades especiales.

## Instrucciones

1. Define una clase llamada `Personaje`.
2. Crea el método constructor `__init__` que reciba y asigne tres atributos:
   - `nombre`
   - `salud` (ej. `100`)
   - `energia_traje` (ej. `50`)
3. Añade un método llamado `recibir_dano(cantidad)`:
   - Resta la `cantidad` a la `salud`.
   - Si la salud llega a `0` o menos, imprime: *"¡[Nombre] ha sido derrotado!"*
4. Añade un método llamado `usar_habilidad_especial()`:
   - Cuesta `20` puntos de `energia_traje`.
   - Si tiene suficiente energía, réstala e imprime un mensaje épico de ataque.
   - Si no tiene energía suficiente, imprime: *"Energía insuficiente"*
5. **La Prueba:** Fuera de la clase, crea una instancia (un objeto) de tu personaje:
   - Haz que use su habilidad especial **tres veces seguidas**.
   - Luego haz que reciba **120 puntos de daño**.

## Estructura esperada del código

```python
class Personaje:
    def __init__(self, nombre, salud, energia_traje):
        ...

    def recibir_dano(self, cantidad):
        ...

    def usar_habilidad_especial(self):
        ...

# Prueba
heroe = Personaje("Iron Man", 100, 50)
heroe.usar_habilidad_especial()  # x3
heroe.recibir_dano(120)
```

## Conceptos clave

| Concepto     | Descripción                                               |
|--------------|-----------------------------------------------------------|
| `class`      | Define una nueva estructura/plantilla de objeto           |
| `__init__`   | Constructor: se ejecuta al crear una instancia            |
| `self`       | Referencia al objeto actual                               |
| Métodos      | Funciones que pertenecen a una clase                      |
| Instancia    | Un objeto concreto creado a partir de la clase            |
