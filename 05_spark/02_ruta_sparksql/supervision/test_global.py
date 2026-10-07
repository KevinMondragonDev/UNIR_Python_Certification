"""
SUPERVISIÓN — Test Global
==========================
Ejecuta todos los validadores de todas las fases y muestra
un reporte consolidado de progreso.

Uso:
  python supervision/test_global.py              # todas las fases
  python supervision/test_global.py --fase 4     # solo fase 4
  python supervision/test_global.py --fase 4 5 6 # fases 4, 5 y 6
"""

import os
import sys
import subprocess
import argparse
import time

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

FASES = {
    1: ("fase_01_fundamentos_spark",          "validar.py"),
    2: ("fase_02_rdds_core",                  "validar.py"),
    3: ("fase_03_rdds_pares_acumuladores",     "validar.py"),
    4: ("fase_04_dataframes_intro",            "validar.py"),
    5: ("fase_05_dataframes_avanzado",         "validar.py"),
    6: ("fase_06_spark_sql_basico",            "validar.py"),
    7: ("fase_07_spark_sql_avanzado",          "validar.py"),
    8: ("fase_08_optimizacion_y_udfs",         "validar.py"),
    9: ("fase_09_proyecto_integrador",         "validar_proyecto.py"),
    10: ("fase_10_despliegue_cluster",         "validar.py"),
}

NOMBRES = {
    1: "Fundamentos Spark",
    2: "RDDs Core",
    3: "Pair RDDs y Variables Compartidas",
    4: "DataFrames Intro",
    5: "DataFrames Avanzado",
    6: "Spark SQL Básico",
    7: "Spark SQL Avanzado",
    8: "Optimización y UDFs",
    9: "Proyecto Integrador",
    10: "Despliegue en Clúster",
}


def ejecutar_validador(num_fase):
    carpeta, script = FASES[num_fase]
    script_path = os.path.join(BASE, carpeta, script)

    if not os.path.exists(script_path):
        return None, f"❓ Script no encontrado: {script_path}"

    t0 = time.time()
    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True, text=True, timeout=300
    )
    t = time.time() - t0

    output = result.stdout + result.stderr
    lines = output.strip().split("\n")

    # Extraer el resumen final
    resumen = ""
    for line in lines:
        if "RESULTADO" in line or "check" in line.lower() or "completad" in line.lower():
            resumen = line.strip()

    # Contar ✅ y ❌
    ok  = output.count("✅")
    err = output.count("❌")

    return {
        "fase": num_fase,
        "nombre": NOMBRES[num_fase],
        "ok": ok,
        "err": err,
        "total": ok + err,
        "tiempo": round(t, 1),
        "resumen": resumen,
        "returncode": result.returncode,
    }


def main():
    parser = argparse.ArgumentParser(description="Test Global PySpark Learning Plan")
    parser.add_argument("--fase", nargs="+", type=int,
                        help="Número(s) de fase a validar (1-10)")
    args = parser.parse_args()

    fases_a_ejecutar = sorted(args.fase) if args.fase else list(FASES.keys())

    print("=" * 65)
    print("SUPERVISIÓN GLOBAL — Plan de Aprendizaje PySpark")
    print("=" * 65)
    print(f"Ejecutando {len(fases_a_ejecutar)} fase(s): {fases_a_ejecutar}\n")

    resultados = []
    for num_fase in fases_a_ejecutar:
        if num_fase not in FASES:
            print(f"  ⚠️  Fase {num_fase} no existe (1-10)")
            continue
        print(f"▶ Fase {num_fase}: {NOMBRES[num_fase]}...", end=" ", flush=True)
        r = ejecutar_validador(num_fase)
        if r is None:
            print("❓ Script no encontrado")
            continue
        status = "✅" if r["err"] == 0 and r["ok"] > 0 else ("⚠️ " if r["ok"] > 0 else "❌")
        print(f"{status} {r['ok']}/{r['total']} checks ({r['tiempo']}s)")
        resultados.append(r)

    if not resultados:
        print("No hay resultados.")
        return

    print("\n" + "=" * 65)
    print("REPORTE FINAL")
    print("=" * 65)
    print(f"{'Fase':<4} {'Nombre':<35} {'Score':<10} {'Tiempo'}")
    print("-" * 65)

    total_ok = 0
    total_checks = 0
    for r in resultados:
        pct = f"{r['ok']}/{r['total']}"
        star = "🏆" if r["err"] == 0 and r["ok"] > 0 else ("🔧" if r["ok"] > 0 else "⏳")
        print(f"{r['fase']:<4} {r['nombre']:<35} {pct:<10} {r['tiempo']}s {star}")
        total_ok     += r["ok"]
        total_checks += r["total"]

    print("-" * 65)
    pct_global = round(total_ok / total_checks * 100, 1) if total_checks > 0 else 0
    print(f"{'TOTAL':<4} {'':35} {total_ok}/{total_checks}  ({pct_global}%)")
    print("=" * 65)

    if pct_global == 100:
        print("\n🏆 ¡PLAN COMPLETO! Has dominado PySpark SQL.")
    elif pct_global >= 80:
        print(f"\n🥈 Muy bien ({pct_global}%) — Termina los checks fallidos.")
    elif pct_global >= 50:
        print(f"\n🔧 En progreso ({pct_global}%) — Sigue adelante.")
    else:
        print(f"\n⏳ Iniciando ({pct_global}%) — Completa las fases pendientes.")


if __name__ == "__main__":
    main()
