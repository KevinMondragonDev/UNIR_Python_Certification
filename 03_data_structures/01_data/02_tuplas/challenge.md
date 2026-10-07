# 📍 Reto 2 — Tuplas: Coordenadas GPS (Estáticas)

> **Objetivo:** Comprender la inmutabilidad de las tuplas y el acceso por índice anidado.

---

## Instrucciones

1. Crea una tupla llamada `punto_gps` con dos valores: `(latitud, longitud)`.
2. Crea una segunda tupla llamada `ruta` que contenga **tres coordenadas** (tres tuplas dentro de ella).
3. Imprime la **latitud** (primer elemento) de la **segunda coordenada** en la ruta.
4. **Desafío:** Intenta modificar la longitud de la primera coordenada en `ruta`. Observa el error que aparece y explica por qué ocurre.

## Estructura esperada

```python
punto_gps = (19.4326, -99.1332)

ruta = (
    (19.4326, -99.1332),   # CDMX
    (20.9674, -89.5926),   # Mérida
    (25.6866, -100.3161),  # Monterrey
)

# Imprimir latitud de la segunda coordenada
print(ruta[1][0])

# Intentar modificar (esto lanzará un TypeError)
# ruta[0][1] = -99.9999  ← ¿Qué ocurre?
```

## Resultado esperado

```
20.9674
TypeError: 'tuple' object does not support item assignment
```

## Conceptos clave

| Concepto       | Descripción                                                    |
|----------------|----------------------------------------------------------------|
| Inmutabilidad  | Las tuplas no pueden modificarse tras ser creadas              |
| Indexación     | `tupla[i]` accede al elemento en la posición `i`               |
| Anidación      | `tupla[i][j]` accede a un elemento dentro de una tupla interna |
| `TypeError`    | Error que Python lanza al intentar modificar una tupla         |
