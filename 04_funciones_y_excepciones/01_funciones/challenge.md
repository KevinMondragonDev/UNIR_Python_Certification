# 🔧 Reto 1 — Funciones: Calculadora de Costos de Nube

> **Objetivo:** Encapsular lógica para que sea reutilizable y modular.

---

## Contexto

Imagina que estás diseñando un script para calcular cuánto va a costar mantener encendidos ciertos recursos en la nube.

## Instrucciones

1. Crea una función llamada `calcular_costo_instancia` que reciba dos parámetros: `horas_uso` y `tarifa_por_hora`.
2. Dentro de la función, multiplica ambos valores para obtener el costo total y usa `return` para devolver ese resultado.
3. Crea una segunda función llamada `reporte_mensual` que reciba una lista de horas (por ejemplo: `[120, 50, 700]`).
4. Dentro de esta segunda función, usa un bucle `for` para recorrer la lista, y llama a tu primera función `calcular_costo_instancia` asumiendo una tarifa fija de `$0.05` por hora.
5. Imprime el costo de cada instancia y, al final, el costo total del mes.

## Conceptos clave

| Concepto     | Descripción                                              |
|--------------|----------------------------------------------------------|
| `def`        | Define una función reutilizable                          |
| `return`     | Devuelve un valor al punto donde se llamó la función     |
| Parámetros   | Variables de entrada que recibe la función               |
| Composición  | Llamar una función dentro de otra                        |
