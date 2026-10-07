# Modo de Supervisión

Esta carpeta es tu **centro de control** del plan de aprendizaje.

## Archivos

| Archivo | Propósito |
|---------|-----------|
| `progreso.md` | Registra tu avance por fase |
| `test_global.py` | Ejecuta todos los validadores de todas las fases |
| `rubrica.md` | Criterios de evaluación por habilidad |

---

## Cómo usar

```bash
# Ver tu progreso
cat supervision/progreso.md

# Ejecutar todos los validadores
python supervision/test_global.py

# Ver la rúbrica de evaluación
cat supervision/rubrica.md
```

---

## Flujo Recomendado por Fase

```
1. Lee el README.md de la fase
2. Ejecuta los scripts de ejemplos (01_, 02_, ...)
3. Completa ejercicios.py (donde dice # TU CÓDIGO AQUÍ)
4. Ejecuta validar.py → asegúrate de pasar todos los checks
5. Intenta el reto.py sin ver la solución
6. Solo si te atascas: mira solucion_reto.py
7. Actualiza progreso.md con tu avance
8. Pasa a la siguiente fase
```
