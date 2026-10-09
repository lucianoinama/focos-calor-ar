"""DAG manual de la etapa 3: landing -> Bronze -> Silver -> Postgres."""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

RUN = "python /opt/airflow/proyecto/scripts/run.py"
COMANDOS = ["bronze_focos", "bronze_clima", "silver_focos", "silver_clima", "exportar_postgres"]

with DAG(
    dag_id="transformacion_manual",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["focos", "etapa3"],
    default_args={"owner": "agustin", "retries": 0},
):
    anterior = None
    for comando in COMANDOS:
        tarea = BashOperator(
            task_id=comando,
            bash_command=f"{RUN} {comando}",
            execution_timeout=pendulum.duration(minutes=30),
        )
        if anterior is not None:
            anterior >> tarea
        anterior = tarea
