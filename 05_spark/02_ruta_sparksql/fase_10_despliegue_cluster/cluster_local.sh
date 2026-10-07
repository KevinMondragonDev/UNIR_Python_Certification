#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# FASE 10 — Clúster Spark Standalone en tu máquina (sin Docker)
# ─────────────────────────────────────────────────────────────
# Levanta 1 Master + N Workers como procesos JVM independientes,
# usando el spark-class que viene dentro de `pip install pyspark`.
# Es un clúster REAL: el driver habla con el Master por red (7077)
# y los executors corren en procesos separados de los Workers.
#
# Uso:
#   ./cluster_local.sh start      # 1 master + 2 workers (2 cores, 2g c/u)
#   ./cluster_local.sh status
#   ./cluster_local.sh stop
#
# UIs:
#   Master  → http://localhost:8080
#   Worker  → http://localhost:8081, 8082
#   Driver  → http://localhost:4040  (solo mientras corre un job)
# ─────────────────────────────────────────────────────────────
set -euo pipefail

NUM_WORKERS=${NUM_WORKERS:-2}
WORKER_CORES=${WORKER_CORES:-2}
WORKER_MEMORY=${WORKER_MEMORY:-2g}
MASTER_HOST=127.0.0.1
MASTER_URL="spark://${MASTER_HOST}:7077"

DIR="$(cd "$(dirname "$0")" && pwd)"
RUN_DIR="$DIR/.cluster"
SPARK_HOME="${SPARK_HOME:-$(python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')}"
SPARK_CLASS="$SPARK_HOME/bin/spark-class"
export SPARK_LOCAL_IP=$MASTER_HOST
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$(command -v python)}"

start() {
    mkdir -p "$RUN_DIR"
    echo "▶ Master en $MASTER_URL (UI http://localhost:8080)"
    nohup "$SPARK_CLASS" org.apache.spark.deploy.master.Master \
        --host "$MASTER_HOST" --port 7077 --webui-port 8080 \
        > "$RUN_DIR/master.out" 2>&1 &
    echo $! > "$RUN_DIR/master.pid"
    sleep 4

    for i in $(seq 1 "$NUM_WORKERS"); do
        port=$((8080 + i))
        echo "▶ Worker $i: ${WORKER_CORES} cores, ${WORKER_MEMORY} (UI http://localhost:${port})"
        nohup "$SPARK_CLASS" org.apache.spark.deploy.worker.Worker \
            --cores "$WORKER_CORES" --memory "$WORKER_MEMORY" \
            --webui-port "$port" --work-dir "$RUN_DIR/work$i" \
            "$MASTER_URL" > "$RUN_DIR/worker$i.out" 2>&1 &
        echo $! > "$RUN_DIR/worker$i.pid"
    done
    sleep 4
    echo
    echo "✅ Clúster listo. Prueba:"
    echo "   spark-submit --master $MASTER_URL --executor-cores 1 --executor-memory 1g \\"
    echo "       $DIR/01_job_spark_submit.py --salida /tmp/reporte_regiones"
}

status() {
    for f in "$RUN_DIR"/*.pid; do
        [ -e "$f" ] || { echo "Clúster detenido"; return; }
        pid=$(cat "$f")
        if kill -0 "$pid" 2>/dev/null; then echo "🟢 $(basename "$f" .pid) (pid $pid)"
        else echo "🔴 $(basename "$f" .pid)"; fi
    done
}

stop() {
    for f in "$RUN_DIR"/worker*.pid "$RUN_DIR"/master.pid; do
        [ -e "$f" ] || continue
        kill "$(cat "$f")" 2>/dev/null && echo "■ $(basename "$f" .pid) detenido" || true
        rm -f "$f"
    done
}

case "${1:-}" in
    start)  start ;;
    status) status ;;
    stop)   stop ;;
    *) echo "Uso: $0 {start|status|stop}"; exit 1 ;;
esac
