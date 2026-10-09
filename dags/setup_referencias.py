"""DAG manual: carga los departamentos de Argentina en PostGIS."""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

RUN = "python /opt/airflow/proyecto/scripts/run.py"

with DAG(
    dag_id="setup_referencias",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["focos", "etapa2"],
    default_args={"owner": "agustin", "retries": 1, "retry_delay": pendulum.duration(minutes=5)},
):
    BashOperator(
        task_id="referencias",
        bash_command=f"{RUN} referencias",
        execution_timeout=pendulum.duration(minutes=20),
    )
