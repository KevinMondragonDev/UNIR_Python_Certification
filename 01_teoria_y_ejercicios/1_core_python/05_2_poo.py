"""
Tema 5 — Organización del código | Ejercicio 2: Programación orientada a objetos (POO)
========================================================================================

TEORÍA:
    La POO organiza el código en CLASES (plantillas) y OBJETOS (instancias).

    Conceptos clave:
    - Clase:       Plantilla que define atributos y métodos
    - Objeto:      Instancia de una clase
    - __init__:    Constructor, se ejecuta al crear el objeto
    - self:        Referencia al objeto actual
    - Atributo:    Variable que pertenece a la clase/objeto
    - Método:      Función que pertenece a la clase

    Pilares de POO:
    - Encapsulamiento: ocultar detalles internos (atributos privados con _)
    - Herencia:        una clase hereda de otra
    - Polimorfismo:    misma interfaz, comportamientos distintos

    Ejemplo básico:
        class Persona:
            def __init__(self, nombre: str, edad: int):
                self.nombre = nombre
                self.edad = edad

            def saludar(self) -> str:
                return f"Hola, soy {self.nombre}"

        kevin = Persona("Kevin", 23)
        print(kevin.saludar())

EJERCICIOS:

1. Crea una clase `Rectangulo` con:
   - Atributos: ancho, alto
   - Método `area()` que retorne el área
   - Método `perimetro()` que retorne el perímetro
   - Método `__str__()` que retorne "Rectángulo de 5x3"
   Crea dos instancias y muestra sus medidas.

2. Crea una clase `CuentaBancaria` con:
   - Atributos: titular, saldo (privado: _saldo)
   - Métodos: depositar(cantidad), retirar(cantidad), consultar_saldo()
   - retirar() debe lanzar ValueError si el saldo es insuficiente
   Simula 3 operaciones.

3. Crea una clase base `Animal` con:
   - Atributos: nombre, especie
   - Método `hablar()` que retorne "..."
   Luego crea clases `Perro` y `Gato` que hereden de `Animal` y
   sobrescriban `hablar()` con el sonido correcto.
   Crea una lista con ambos animales y llama a hablar() para cada uno.

4. Crea una clase `Dataset` que represente un conjunto de datos:
   - Atributos: nombre, registros (lista de diccionarios)
   - Métodos:
     * agregar_registro(registro: dict)
     * total_registros() -> int
     * filtrar(campo: str, valor) -> list
     * __len__() que retorne el número de registros
     * __repr__() con info del dataset

5. ¿Cuándo usarías una clase en lugar de solo funciones?
   Escribe tu respuesta en un comentario con al menos 2 ejemplos concretos
   del mundo de Data Engineering.
"""

# Ejercicio 1 — Tu código aquí


# Ejercicio 2 — Tu código aquí


# Ejercicio 3 — Tu código aquí


# Ejercicio 4 — Tu código aquí


# Ejercicio 5 — Tu reflexión:
# ...
