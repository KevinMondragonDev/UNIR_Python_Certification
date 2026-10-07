# 🗂️ Reto 3 — Diccionarios: El Inventario (Búsqueda por Clave)

> **Objetivo:** Dominar los diccionarios como estructuras clave-valor para búsqueda y gestión de datos.

---

## Instrucciones

Crea un diccionario llamado `precios_productos` donde las claves sean nombres de productos (strings) y los valores sean sus precios (números). Incluye al menos **3 productos** iniciales.

1. **Agrega** un producto nuevo al diccionario directamente por clave.
2. **Modifica** el precio de un producto existente accediendo a su clave.
3. **Elimina** un producto usando `del`.
4. **Verifica** si un producto específico existe en el diccionario usando el operador `in`.

## Estructura esperada

```python
precios_productos = {
    "Laptop": 1200,
    "Teclado": 80,
    "Monitor": 350,
}

# 1. Agregar
precios_productos["Mouse"] = 45

# 2. Modificar
precios_productos["Laptop"] = 1099

# 3. Eliminar
del precios_productos["Teclado"]

# 4. Verificar existencia
if "Monitor" in precios_productos:
    print("Monitor disponible")
```

## Conceptos clave

| Operación         | Sintaxis                        | Descripción                              |
|-------------------|---------------------------------|------------------------------------------|
| Crear             | `{clave: valor}`                | Define el diccionario con pares iniciales|
| Agregar / Editar  | `d[clave] = valor`              | Crea o sobreescribe el valor de esa clave|
| Eliminar          | `del d[clave]`                  | Elimina el par clave-valor               |
| Verificar         | `clave in d`                    | Retorna `True` si la clave existe        |
| Acceder           | `d[clave]`                      | Obtiene el valor de esa clave            |
