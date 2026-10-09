"""DAG manual de la etapa 6: reconstruye la historia de riesgo (SCD2) mes por mes."""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

RUN = "python /opt/airflow/proyecto/scripts/run.py"
DBT = "/opt/dbt-venv/bin/dbt"
ARGS = "--project-dir /opt/airflow/proyecto/dbt --profiles-dir /opt/airflow/proyecto/dbt"


def encadenar(tareas):
    """Crea las tareas (task_id, comando, minutos de timeout, reintentos) en serie."""
    anterior = None
    for task_id, comando, minutos, reintentos in tareas:
        tarea = BashOperator(
            task_id=task_id,
            bash_command=comando,
            execution_timeout=pendulum.duration(minutes=minutos),
            retries=reintentos,
        )
        if anterior is not None:
            anterior >> tarea
        anterior = tarea

SNAPSHOTS_POR_MES = (
    'M="$BACKFILL_FECHA_INICIO"; '
    'FIN=$(date -u -d "$(date -u +%Y-%m-01) -1 month" +%Y-%m-01); '
    'while [[ "$M" < "$FIN" || "$M" == "$FIN" ]]; do '
    f'echo "Snapshot del mes $M"; '
    f'{DBT} run --select riesgo_departamento_vigente --vars "{{mes_corte: $M}}" {ARGS} && '
    f'{DBT} snapshot --vars "{{mes_corte: $M}}" {ARGS} || exit 1; '
    'M=$(date -u -d "$M +1 month" +%Y-%m-01); '
    'done'
)

TAREAS = [
    ("borrar_snapshot_riesgo", f"{RUN} borrar_snapshot_riesgo", 5, 0),
    ("dbt_run_base", f"{DBT} run --select staging intermediate {ARGS}", 15, 0),
    ("snapshots_por_mes", SNAPSHOTS_POR_MES, 30, 0),
    ("dbt_run_marts", f"{DBT} run --select marts {ARGS}", 15, 0),
    ("dbt_test", f"{DBT} test {ARGS}", 15, 0),
]

with DAG(
    dag_id="backfill_riesgo",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=pendulum.duration(hours=1),
    tags=["focos", "etapa6"],
    default_args={"owner": "agustin", "retry_delay": pendulum.duration(minutes=10)},
):
    encadenar(TAREAS)
