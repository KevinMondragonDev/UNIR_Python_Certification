# ⏭️ Reto 4 — Saltos de Control: El Buscador

> **Objetivo:** Alterar el comportamiento normal de un bucle sobre la marcha usando `break` y `continue`.

---

## Instrucciones

1. Crea una lista de palabras: `palabras = ["manzana", "pera", "ALERTA", "uva", "naranja"]`.
2. Haz un bucle `for` para recorrer la lista.
3. Si la palabra es `"pera"`, usa `continue` para **saltarte esa iteración** sin imprimir nada.
4. Si la palabra es `"ALERTA"`, usa `break` para **detener y salir del bucle** por completo.
5. Imprime las palabras que el bucle logre procesar normalmente.

## Estructura esperada

```python
palabras = ["manzana", "pera", "ALERTA", "uva", "naranja"]

for palabra in palabras:
    if palabra == "pera":
        continue
    if palabra == "ALERTA":
        break
    print(palabra)
```

## Resultado esperado

```
manzana
```

> `"pera"` se salta con `continue` y `"ALERTA"` detiene el bucle antes de llegar a `"uva"` y `"naranja"`.

## Conceptos clave

| Concepto    | Descripción                                                       |
|-------------|-------------------------------------------------------------------|
| `continue`  | Salta al siguiente ciclo del bucle sin ejecutar el resto          |
| `break`     | Sale completamente del bucle en ese momento                       |
| Flujo       | Ambos alteran el flujo de control de iteración                    |
