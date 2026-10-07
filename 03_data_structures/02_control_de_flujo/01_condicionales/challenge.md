# 🚦 Reto 1 — Condicionales: El Guardián

> **Objetivo:** Hacer que tu programa tome decisiones basadas en diferentes escenarios con `if`, `elif` y `else`.

---

## Instrucciones

1. Declara una variable `edad = 20` (o pídela con `input()` para mayor interactividad).
2. Si la edad es **menor de 13**, imprime: `"Eres un niño"`.
3. Si la edad está **entre 13 y 17** (inclusive), imprime: `"Eres adolescente"`.
4. Si la edad es **18 o mayor**, imprime: `"Eres adulto"`.
5. **Desafío extra:** Añade una condición anidada. Si es adulto, verifica además si tiene **65 años o más** y, si es así, imprime: `"Eres un adulto mayor"`.

## Estructura esperada

```python
edad = 20

if edad < 13:
    print("Eres un niño")
elif edad <= 17:
    print("Eres adolescente")
else:
    print("Eres adulto")
    if edad >= 65:
        print("Eres un adulto mayor")
```

## Conceptos clave

| Concepto      | Descripción                                             |
|---------------|---------------------------------------------------------|
| `if`          | Evalúa una condición; ejecuta si es `True`             |
| `elif`        | Condición alternativa si la anterior fue `False`        |
| `else`        | Bloque por defecto si ninguna condición fue `True`      |
| `if` anidado  | Un `if` dentro de otro para condiciones compuestas      |
