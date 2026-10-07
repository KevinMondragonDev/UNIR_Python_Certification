# ⏳ Reto 3 — Bucle `while`: El Bucle Infinito (Controlado)

> **Objetivo:** Repetir una acción hasta que una condición específica cambie.

---

## Instrucciones

1. Crea una variable `contador = 10`.
2. Escribe un bucle `while` que se ejecute **mientras el contador sea mayor que 0**.
3. Dentro del bucle, imprime el valor actual del contador.
4. **⚠️ Crucial:** Réstale `1` al contador en cada vuelta. Si olvidas esto, crearás un bucle infinito y tu programa se quedará trabado.
5. Cuando el bucle termine (contador llegue a 0), imprime: `"¡Despegue!"`.

## Estructura esperada

```python
contador = 10

while contador > 0:
    print(contador)
    contador -= 1

print("¡Despegue!")
```

## Resultado esperado

```
10
9
8
7
6
5
4
3
2
1
¡Despegue!
```

## Conceptos clave

| Concepto       | Descripción                                                      |
|----------------|------------------------------------------------------------------|
| `while`        | Repite el bloque mientras la condición sea `True`                |
| Condición      | Expresión booleana que controla cuándo se detiene el bucle       |
| `contador -= 1`| Actualización de estado para evitar bucles infinitos             |
