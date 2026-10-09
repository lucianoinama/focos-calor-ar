"""DAG manual de la etapa 6: SP nuevos, snapshot del mes anterior y compactacion Delta."""
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

SNAPSHOT_MES_ANTERIOR = (
    'MES=$(date -u -d "$(date -u +%Y-%m-01) -1 month" +%Y-%m-01) && '
    f'{DBT} run --select riesgo_departamento_vigente --vars "{{mes_corte: $MES}}" {ARGS} && '
    f'{DBT} snapshot --vars "{{mes_corte: $MES}}" {ARGS}'
)

TAREAS = [
    ("firms_sp_mensual", f"{RUN} firms_sp_mensual", 60, 2),
    ("bronze_focos", f"{RUN} bronze_focos", 60, 0),
    ("silver_focos", f"{RUN} silver_focos", 60, 0),
    ("exportar_postgres", f"{RUN} exportar_postgres", 30, 0),
    ("dbt_run_base", f"{DBT} run --select staging intermediate {ARGS}", 15, 0),
    ("dbt_snapshot_mes", SNAPSHOT_MES_ANTERIOR, 15, 0),
    ("dbt_run_marts", f"{DBT} run --select marts {ARGS}", 15, 0),
    ("dbt_test", f"{DBT} test {ARGS}", 15, 0),
    ("compactar_delta", f"{RUN} compactar_delta", 30, 0),
]

with DAG(
    dag_id="mensual_sp_y_riesgo",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=pendulum.duration(hours=3),
    tags=["focos", "etapa6"],
    default_args={"owner": "agustin", "retry_delay": pendulum.duration(minutes=10)},
):
    encadenar(TAREAS)
