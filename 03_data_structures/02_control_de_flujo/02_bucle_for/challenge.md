# 🔄 Reto 2 — Bucle `for`: El Analista de Datos

> **Objetivo:** Recorrer colecciones de datos y ejecutar código por cada elemento.

---

## Instrucciones

1. Crea una lista de números variados: `numeros = [12, 5, 8, 21, 30, 7]`.
2. Crea una variable `suma_pares = 0` **antes** del bucle.
3. Usa un bucle `for` para recorrer cada número de la lista.
4. Dentro del bucle, usa un `if` para verificar si el número es par (pista: usa el operador módulo `%`).
5. Si es par, súmalo a la variable `suma_pares`.
6. Al terminar el bucle, imprime el total de la suma.

## Estructura esperada

```python
numeros = [12, 5, 8, 21, 30, 7]
suma_pares = 0

for numero in numeros:
    if numero % 2 == 0:
        suma_pares += numero

print(f"Suma de pares: {suma_pares}")
```

## Resultado esperado

```
Suma de pares: 50
```

## Conceptos clave

| Concepto        | Descripción                                            |
|-----------------|--------------------------------------------------------|
| `for x in y`    | Itera sobre cada elemento de una colección             |
| `%` (módulo)    | Devuelve el resto de una división entera               |
| Acumulador      | Variable que guarda un resultado parcial en cada vuelta|
