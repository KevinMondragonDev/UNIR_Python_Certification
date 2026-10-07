# 🏆 Reto Final — Gran Integrador: Sistema de Gestión de Biblioteca

> **Objetivo:** Combinar Listas + Tuplas + Diccionarios + Sets en un sistema funcional real.

---

## Contexto

Vas a construir la lógica de una biblioteca digital. Cada estructura de datos tiene un rol específico y no puede ser reemplazada por otra, lo que te fuerza a entender cuándo y por qué usar cada una.

## Instrucciones

### 1. Estructura Base — Diccionario

Crea un diccionario llamado `biblioteca` donde:
- La **clave** es el ID del libro (ej. `"LIB001"`)
- El **valor** es una **tupla** con `(Título, Autor, Año)`

```python
biblioteca = {
    "LIB001": ("Cien Años de Soledad", "García Márquez", 1967),
    "LIB002": ("El Principito", "Antoine de Saint-Exupéry", 1943),
    "LIB003": ("1984", "George Orwell", 1949),
}
```

### 2. Estado Dinámico — Lista

Crea una lista llamada `libros_prestados` con los IDs de libros que actualmente **no están disponibles**:

```python
libros_prestados = ["LIB002"]
```

### 3. Gestión de Géneros — Set

Crea un set llamado `generos_disponibles` con los géneros de los libros registrados (sin duplicados):

```python
generos_disponibles = {"Realismo mágico", "Fantasía", "Distopía"}
```

### 4. Operación Compleja — Función de Préstamo

Crea una función (o bloque de código) que reciba un `id_libro`:

- Si el ID está en `libros_prestados` → imprime: *"El libro ya está prestado"*
- Si no está → busca el libro en `biblioteca`, imprime sus detalles y añade el ID a `libros_prestados`

```python
def prestar_libro(id_libro):
    if id_libro in libros_prestados:
        print("El libro ya está prestado")
    elif id_libro in biblioteca:
        titulo, autor, anio = biblioteca[id_libro]
        print(f"Libro: {titulo} | Autor: {autor} | Año: {anio}")
        libros_prestados.append(id_libro)
    else:
        print("ID no encontrado en la biblioteca")
```

## ¿Por qué cada estructura?

| Estructura       | Rol en este sistema                                                    |
|------------------|------------------------------------------------------------------------|
| **Diccionario**  | Búsqueda rápida de libros por ID                                       |
| **Tupla**        | Los datos del libro (título, autor, año) no deben cambiar              |
| **Lista**        | Control dinámico de préstamos: pueden añadirse y quitarse IDs          |
| **Set**          | Géneros únicos, sin importar cuántos libros de cada género haya        |
