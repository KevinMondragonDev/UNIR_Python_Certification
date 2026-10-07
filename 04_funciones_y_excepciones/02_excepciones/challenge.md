# 🛡️ Reto 2 — Excepciones: El Servidor Resiliente

> **Objetivo:** Prevenir que tu programa se detenga abruptamente cuando el usuario o el sistema cometen un error.

---

## Contexto

Vas a simular un intento de conexión a un servidor mediante un puerto numérico. El programa debe ser capaz de manejar entradas inválidas sin colapsar.

## Instrucciones

1. Usa la función `input()` para pedirle al usuario: *"Ingresa el número de puerto para conectarte (ej. 8080): "*.
2. Envuelve la conversión de ese input a entero (`int()`) dentro de un bloque `try`.
3. Si el usuario escribe letras en lugar de números (por ejemplo, `"ocho"`), Python lanzará un `ValueError`. Usa un bloque `except ValueError:` para atraparlo e imprimir: *"Error: El puerto debe ser un número entero."*
4. Añade una validación manual: si el puerto es `0`, usa la palabra clave `raise` para lanzar una excepción genérica (`Exception("El puerto 0 está reservado")`).
5. Atrapa esa excepción genérica en otro bloque `except Exception as e:` e imprime el mensaje del error.
6. Añade un bloque `finally:` que imprima: *"Intento de conexión finalizado."*, el cual debe ejecutarse sin importar si hubo error o no.

## Estructura esperada del código

```python
try:
    # conversión de input
    # validación con raise
except ValueError:
    # manejo de tipo incorrecto
except Exception as e:
    # manejo de excepción genérica
finally:
    # siempre se ejecuta
```

## Conceptos clave

| Concepto          | Descripción                                                  |
|-------------------|--------------------------------------------------------------|
| `try / except`    | Bloque para capturar errores en tiempo de ejecución          |
| `ValueError`      | Error al convertir un tipo de dato incompatible              |
| `raise`           | Lanza manualmente una excepción                              |
| `finally`         | Se ejecuta siempre, haya error o no                          |
