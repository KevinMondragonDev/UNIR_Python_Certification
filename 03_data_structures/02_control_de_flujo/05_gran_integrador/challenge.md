# 🏆 Reto Final — Gran Integrador: Sistema Bancario Básico

> **Objetivo:** Combinar condicionales, bucles `while`, `for` y saltos de control en un caso de uso real.

---

## Contexto

Vamos a simular la lógica detrás de un cajero automático básico: un menú interactivo que se mantiene activo mientras el usuario no decida salir.

## Instrucciones

### 1. Configuración inicial

```python
saldo = 5000
sistema_activo = True
```

### 2. El Bucle Principal

Crea un bucle `while sistema_activo:` que mantendrá el menú activo.

### 3. El Menú

Dentro del bucle, imprime las opciones disponibles:

```
(1) Consultar saldo
(2) Retirar dinero
(3) Depositar dinero
(4) Salir
```

### 4. Toma de decisiones

Declara una variable `opcion` y ve cambiando su valor para probar cada caso. Usa `if / elif / else` para evaluar la opción:

| Opción | Acción                                                                                 |
|--------|----------------------------------------------------------------------------------------|
| `1`    | Imprime el saldo actual                                                                |
| `2`    | Declara `monto_retiro`. Si supera el saldo → *"Fondos insuficientes"*. Si no → réstalo |
| `3`    | Declara `monto_deposito`, súmalo al saldo e imprime la confirmación                   |
| `4`    | Cambia `sistema_activo = False` e imprime un mensaje de despedida                     |
| else   | Imprime: *"Opción no reconocida"*                                                      |

## Conceptos integrados

| Concepto        | Aplicación                                                    |
|-----------------|---------------------------------------------------------------|
| `while`         | Mantiene el sistema activo hasta que el usuario salga         |
| `if/elif/else`  | Maneja cada opción del menú                                   |
| `if` anidado    | Valida el monto antes de permitir el retiro                   |
| Variables       | `saldo`, `sistema_activo` como estado del programa            |
