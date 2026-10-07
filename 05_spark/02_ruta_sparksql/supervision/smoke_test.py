"""
SUPERVISIÓN — Smoke Test del Material de Referencia
=====================================================
Ejecuta todo el código que DEBE funcionar sin intervención del alumno:
  - Los ejemplos numerados de cada fase (01_*.py, 02_*.py, ...)
  - Las soluciones de los retos (solucion_reto.py)
  - El pipeline del proyecto integrador + su validador (25/25)
  - El job de producción de la fase 10
  - La solución del reto de Parquet de los retos por tema

No ejecuta ejercicios.py ni los validadores de fases 1-8/10: esos
dependen de que el alumno complete su código.

Uso:
  python supervision/smoke_test.py            # todo
  python supervision/smoke_test.py --rapido   # solo soluciones y pipeline
Sale con código 1 si algo falla (lo usa la CI de GitHub Actions).
"""

import argparse
import glob
import os
import subprocess
import sys
import tempfile
import time

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPARK_DIR = os.path.dirname(BASE)


def scripts_de_referencia(rapido):
    pasos = []
    if not rapido:
        for ruta in sorted(glob.glob(os.path.join(BASE, "fase_*", "[0-9][0-9]_*.py"))):
            pasos.append(([ruta], None))
    for ruta in sorted(glob.glob(os.path.join(BASE, "fase_*", "solucion_reto.py"))):
        args = [ruta]
        if "fase_10" in ruta:
            args += ["--salida", os.path.join(tempfile.mkdtemp(), "metricas")]
        pasos.append((args, None))

    fase9 = os.path.join(BASE, "fase_09_proyecto_integrador")
    pasos.append(([os.path.join(fase9, "pipeline_modular.py")], None))
    pasos.append(([os.path.join(fase9, "validar_proyecto.py")], "❌"))

    fase10 = os.path.join(BASE, "fase_10_despliegue_cluster")
    pasos.append(([os.path.join(fase10, "01_job_spark_submit.py"),
                   "--salida", os.path.join(tempfile.mkdtemp(), "reporte")], None))

    pasos.append(([os.path.join(SPARK_DIR, "01_retos_por_tema", "10_lectura_escritura",
                                "solucion_retos_parquet.py")], None))
    # 01_job ya está incluido arriba con argumentos; evitar ejecutarlo sin --salida
    return [p for p in pasos if not (p[0][0].endswith("01_job_spark_submit.py") and len(p[0]) == 1)]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rapido", action="store_true")
    args = parser.parse_args()

    pasos = scripts_de_referencia(args.rapido)
    print(f"Smoke test: {len(pasos)} scripts\n")
    fallos = []
    t_total = time.time()
    for argv, marca_fallo in pasos:
        nombre = os.path.relpath(argv[0], SPARK_DIR)
        t0 = time.time()
        res = subprocess.run([sys.executable] + argv, capture_output=True, text=True,
                             timeout=600, cwd=os.path.dirname(argv[0]))
        salida = res.stdout + res.stderr
        ok = res.returncode == 0 and (marca_fallo is None or marca_fallo not in res.stdout)
        print(f"  {'✅' if ok else '❌'} {nombre:<75} {time.time() - t0:5.1f}s")
        if not ok:
            fallos.append((nombre, salida[-3000:]))

    print(f"\n{len(pasos) - len(fallos)}/{len(pasos)} scripts OK en {time.time() - t_total:.0f}s")
    for nombre, salida in fallos:
        print(f"\n{'=' * 70}\nFALLÓ: {nombre}\n{'=' * 70}\n{salida}")
    sys.exit(1 if fallos else 0)


if __name__ == "__main__":
    main()
