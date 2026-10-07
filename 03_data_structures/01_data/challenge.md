### El Desafío: Sistema de Gestión de Almacén

Imagina que eres el desarrollador de un sistema para un almacén automatizado. Debes gestionar productos que entran y salen, manteniendo la integridad de la información.

**Instrucciones:**

1. **Define tu Base de Datos (Diccionario):**
* Crea un diccionario llamado `almacen`.
* Cada clave será el `ID_producto` (un string) y el valor será una **tupla** que contenga `(Nombre, Precio, Categoría)`.
* *Ejemplo:* `{"P001": ("Laptop", 1200, "Electrónica")}`. Crea al menos 3 productos.


2. **Gestión de Stock (Lista):**
* Crea una lista llamada `inventario_actual` que contenga solo los `ID_producto` que tienen existencia.
* Añade un ID nuevo usando `.append()`.
* Elimina un ID usando `.remove()` cuando el producto se agote.


3. **Categorías Únicas (Set):**
* Extrae todas las categorías de los productos en tu diccionario.
* Almacénalas en un **set** llamado `categorias_disponibles`.
* Como el set elimina duplicados automáticamente, esto te dará una lista limpia de qué tipos de productos vendes, sin importar cuántos artículos haya de cada uno.


4. **Operación Compleja:**
* Crea una función (o un bloque de código) que reciba un ID de producto.
* **Primero:** Verifica con `if ID in inventario_actual` si el producto existe.
* **Segundo:** Si existe, accede al diccionario para imprimir los detalles del producto.
* **Tercero:** Intenta modificar el precio de ese producto en el diccionario. *Nota: ¿Qué pasa si intentas modificar el valor dentro de la tupla?* (Reflexiona sobre la mutabilidad aquí).



---

### ¿Por qué este ejercicio es complejo?

* **Anidación:** Estás poniendo tuplas *dentro* de valores de diccionarios.
* **Interdependencia:** La lista `inventario_actual` actúa como un índice para acceder al `almacen`.
* **Restricciones:** Estás forzado a usar las propiedades de cada estructura:
* El **diccionario** para la búsqueda rápida por ID.
* La **tupla** porque la descripción del producto (nombre y categoría) no debería cambiar una vez definida.
* El **set** para limpiar la redundancia de categorías.
* La **lista** para controlar el flujo dinámico de entrada/salida de stock.

