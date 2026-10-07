# 🔵 Reto 4 — Sets: Usuarios Únicos (Filtrado de Duplicados)

> **Objetivo:** Entender cómo los sets eliminan duplicados automáticamente y gestionar colecciones sin repeticiones.

---

## Instrucciones

Crea una lista con los siguientes números: `[1, 2, 2, 3, 4, 4, 4, 5]`.

1. Convierte esta lista en un `set` llamado `numeros_unicos`.
2. Agrega el número `6` al set con `.add()`.
3. Intenta agregar el número `1` de nuevo y observa qué sucede con el **tamaño** del set.
4. Imprime el set final.

## Estructura esperada

```python
numeros_lista = [1, 2, 2, 3, 4, 4, 4, 5]

numeros_unicos = set(numeros_lista)
print(numeros_unicos)  # {1, 2, 3, 4, 5}

numeros_unicos.add(6)
numeros_unicos.add(1)  # ¿Cambia el set?

print(numeros_unicos)
print(len(numeros_unicos))
```

## Resultado esperado

```
{1, 2, 3, 4, 5}
{1, 2, 3, 4, 5, 6}
6
```

> ⚠️ Agregar `1` de nuevo no cambia el set ni lanza error: simplemente lo ignora porque ya existe.

## Conceptos clave

| Concepto          | Descripción                                                      |
|-------------------|------------------------------------------------------------------|
| `set()`           | Convierte un iterable en un conjunto sin duplicados              |
| `.add(valor)`     | Agrega un elemento; si ya existe, no hace nada                   |
| Sin orden         | Los sets no garantizan un orden de impresión fijo                |
| Sin duplicados    | Propiedad fundamental: cada elemento es único                    |
