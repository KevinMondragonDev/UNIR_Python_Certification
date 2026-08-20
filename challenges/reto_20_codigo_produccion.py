"""
Reto 20 — "Código de producción"
===================================

Este es el reto más avanzado y el más cercano a trabajo real de Data Engineering.

Toma el Reto 19 como base y trabaja con requisitos ambiguos e intencionalmente
incompletos, como si fuera una tarea real de un equipo de ingeniería.

Los requisitos que te da tu tech lead:

    "Necesitamos un job que procese los archivos de clientes que llegan diario a S3.
    Hay algunos problemas con la calidad de los datos. Asegúrate de que el output
    sea confiable. El job debe poder correr en modo local y en producción.
    Ah, y hay un bug en el procesamiento de fechas que no hemos podido reproducir."

Lo que debes hacer:

    1. ENTENDER LOS REQUISITOS
       - Identificar qué está claro y qué es ambiguo
       - Hacer las preguntas correctas (documéntalas como comentarios)
       - Definir criterios de aceptación

    2. DISEÑAR LA ESTRUCTURA
       - Proponer arquitectura del pipeline
       - Definir interfaces entre módulos
       - Planificar manejo de errores

    3. ESCRIBIR EL CÓDIGO
       - Implementar el pipeline del Reto 19
       - Aplicar todos los estándares de código
       - Modo local (archivos) y modo producción (S3)

    4. DETECTAR Y CORREGIR ERRORES
       El siguiente código tiene bugs intencionales. Encuéntralos y corrígelos:

       def process_date(date_str):
           from datetime import datetime
           return datetime.strptime(date_str, "%Y/%m/%d")  # Bug 1

       def calculate_average(numbers):
           return sum(numbers) / len(numbers)  # Bug 2

       def load_records(file_path):
           f = open(file_path)            # Bug 3
           data = json.loads(f.read())
           return data

       def deduplicate(records):
           seen = set()
           unique = []
           for record in records:
               if record not in seen:  # Bug 4
                   seen.add(record)
                   unique.append(record)
           return unique

    5. ESCRIBIR TESTS
       - Tests unitarios para cada función con pytest
       - Tests de integración para el pipeline completo
       - Asegúrate de cubrir edge cases (lista vacía, archivo no encontrado, etc.)

    6. JUSTIFICAR DECISIONES TÉCNICAS
       Documenta en comentarios o en un DECISIONS.md:
       - ¿Por qué elegiste esta arquitectura?
       - ¿Qué trade-offs consideraste?
       - ¿Cómo manejarías escalabilidad?

Criterios de evaluación:

    ✓ ¿El pipeline es idempotente?
    ✓ ¿El código es legible y mantenible?
    ✓ ¿Los errores están bien manejados y logueados?
    ✓ ¿Los tests cubren los casos importantes?
    ✓ ¿Puedes explicar cada decisión técnica?
    ✓ ¿El código podría ser revisado por un colega sin tu explicación?

Nota: Este reto combina Python + Data Engineering + criterio de ingeniería.
Es el que más te prepara para trabajo real en un equipo de Data Engineering.
"""

# Tu implementación comienza aquí.
# Empieza documentando las preguntas que harías a tu tech lead.
