# 🏆 Reto Final — Gran Integrador: Sistema de Análisis de Texto

> **Objetivo:** Combinar Funciones + Excepciones + POO en un único script robusto.

---

## Contexto

Este reto une los tres conceptos del módulo en un solo proyecto. Construirás una clase que analiza texto, expone métodos (funciones internas) y maneja errores de forma elegante.

## Instrucciones

### 1. La Clase

Crea una clase `AnalistaDeTexto`. En su `__init__`, debe recibir un string largo (un bloque de texto) y guardarlo como atributo `texto`.

### 2. El Método (Función interna)

Crea un método dentro de la clase llamado `contar_palabra(palabra_objetivo)`. Este método debe:
- Transformar todo el texto a minúsculas (`.lower()`).
- Separarlo en una lista de palabras (`.split()`).
- Contar cuántas veces aparece la `palabra_objetivo`.

### 3. La Excepción

Dentro de ese mismo método, verifica si la `palabra_objetivo` es un string vacío `""`. Si lo es, lanza un `ValueError` con el mensaje:

> *"No se puede buscar una palabra vacía"*

### 4. La Ejecución

```python
texto_prueba = "El servidor de la base de datos de la aplicación falló"
analista = AnalistaDeTexto(texto_prueba)

try:
    print(analista.contar_palabra("de"))   # Debe funcionar correctamente
    print(analista.contar_palabra(""))     # Debe lanzar ValueError
except ValueError as e:
    print(f"Error controlado: {e}")
```

## Resultado esperado

```
3
Error controlado: No se puede buscar una palabra vacía
```

## Conceptos integrados

| Concepto      | Aplicación en este reto                                    |
|---------------|------------------------------------------------------------|
| `class`       | `AnalistaDeTexto` como entidad con datos y comportamiento  |
| Métodos       | `contar_palabra()` como función interna de la clase        |
| `raise`       | Lanzar `ValueError` cuando el input es inválido            |
| `try/except`  | Capturar el error sin que el programa colapse              |
