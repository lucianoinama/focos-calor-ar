"""DAG manual de la etapa 6: historico de clima con presupuesto de llamadas. Se repite hasta completar."""
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

TAREAS = [
    ("clima_backfill", f"{RUN} clima_backfill", 180, 0),
    ("bronze_clima", f"{RUN} bronze_clima", 30, 0),
    ("silver_clima", f"{RUN} silver_clima", 30, 0),
    ("exportar_postgres", f"{RUN} exportar_postgres", 30, 0),
]

with DAG(
    dag_id="backfill_clima",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=pendulum.duration(hours=4),
    tags=["focos", "etapa6"],
    default_args={"owner": "agustin", "retry_delay": pendulum.duration(minutes=10)},
):
    encadenar(TAREAS)
