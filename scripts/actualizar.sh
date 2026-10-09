#!/usr/bin/env bash
# Actualiza todos los datos: dispara pipeline_diario y espera a que termine.
# Uso (desde Ubuntu): ~/proyectos/focos-calor-ar/scripts/actualizar.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# Consulta la base de metadatos de Airflow (mismo servidor Postgres, sin contrasena local)
consulta_airflow() {
  docker compose exec -T dw psql -U airflow -d airflow -tA -c "$1"
}

RUN_ID="manual__$(date -u +%Y%m%dT%H%M%S)"
docker compose exec -T airflow-scheduler airflow dags unpause pipeline_diario > /dev/null
docker compose exec -T airflow-scheduler airflow dags trigger pipeline_diario --run-id "$RUN_ID" > /dev/null
echo "Corrida ${RUN_ID} disparada. Progreso en http://localhost:8080 (tarda unos 20 minutos)."

inicio=$(date +%s)
while true; do
  sleep 60
  estado=$(consulta_airflow "select state from dag_run where dag_id = 'pipeline_diario' and run_id = '${RUN_ID}'")
  minutos=$(( ($(date +%s) - inicio) / 60 ))
  case "$estado" in
    success)
      echo "Listo: datos actualizados en ${minutos} min."
      exit 0
      ;;
    failed)
      echo "La corrida fallo despues de ${minutos} min. Estado de cada tarea:"
      consulta_airflow "select task_id || ': ' || coalesce(state, 'sin estado') from task_instance where dag_id = 'pipeline_diario' and run_id = '${RUN_ID}' order by start_date nulls last"
      exit 1
      ;;
    *)
      tarea=$(consulta_airflow "select task_id from task_instance where dag_id = 'pipeline_diario' and run_id = '${RUN_ID}' and state = 'running' limit 1")
      echo "  ${minutos} min: ${estado:-en cola}${tarea:+ (${tarea})}"
      ;;
  esac
  if (( minutos >= 90 )); then
    echo "Sigue corriendo despues de 90 min; revisar en http://localhost:8080"
    exit 2
  fi
done
