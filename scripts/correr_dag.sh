#!/usr/bin/env bash
# Dispara un DAG, espera a que termine y lo vuelve a pausar.
# Uso (desde Ubuntu): scripts/correr_dag.sh <dag_id> [minutos_maximos]
set -euo pipefail
cd "$(dirname "$0")/.."

DAG="${1:?Uso: scripts/correr_dag.sh <dag_id> [minutos_maximos]}"
MAXIMO="${2:-90}"

consulta_airflow() {
  docker compose exec -T dw psql -U airflow -d airflow -tA -c "$1"
}

terminar() {
  if [[ "$DAG" != "pipeline_diario" ]]; then
    docker compose exec -T airflow-scheduler airflow dags pause "$DAG" > /dev/null
  fi
  exit "$1"
}

RUN_ID="manual__$(date -u +%Y%m%dT%H%M%S)"
docker compose exec -T airflow-scheduler airflow dags unpause "$DAG" > /dev/null
docker compose exec -T airflow-scheduler airflow dags trigger "$DAG" --run-id "$RUN_ID" > /dev/null
echo "Corrida ${RUN_ID} de ${DAG} disparada. Progreso en http://localhost:8080"

inicio=$(date +%s)
while true; do
  sleep 60
  estado=$(consulta_airflow "select state from dag_run where dag_id = '${DAG}' and run_id = '${RUN_ID}'")
  minutos=$(( ($(date +%s) - inicio) / 60 ))
  case "$estado" in
    success)
      echo "Listo: ${DAG} termino en ${minutos} min."
      terminar 0
      ;;
    failed)
      echo "${DAG} fallo despues de ${minutos} min. Estado de cada tarea:"
      consulta_airflow "select task_id || ': ' || coalesce(state, 'sin estado') from task_instance where dag_id = '${DAG}' and run_id = '${RUN_ID}' order by start_date nulls last"
      terminar 1
      ;;
    *)
      tarea=$(consulta_airflow "select task_id from task_instance where dag_id = '${DAG}' and run_id = '${RUN_ID}' and state = 'running' limit 1")
      echo "  ${minutos} min: ${estado:-en cola}${tarea:+ (${tarea})}"
      ;;
  esac
  if (( minutos >= MAXIMO )); then
    echo "${DAG} sigue corriendo despues de ${MAXIMO} min; revisar en http://localhost:8080"
    exit 2
  fi
done
