"""DAG manual de la etapa 2: descarga focos y clima recientes a landing."""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

RUN = "python /opt/airflow/proyecto/scripts/run.py"

with DAG(
    dag_id="extraccion_manual",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["focos", "etapa2"],
    default_args={"owner": "agustin", "retries": 1, "retry_delay": pendulum.duration(minutes=5)},
):
    firms_disponibilidad = BashOperator(
        task_id="firms_disponibilidad",
        bash_command=f"{RUN} firms_disponibilidad",
        execution_timeout=pendulum.duration(minutes=10),
    )
    firms_diario = BashOperator(
        task_id="firms_diario",
        bash_command=f"{RUN} firms_diario",
        execution_timeout=pendulum.duration(minutes=15),
    )
    clima_diario = BashOperator(
        task_id="clima_diario",
        bash_command=f"{RUN} clima_diario",
        execution_timeout=pendulum.duration(minutes=30),
    )

    firms_disponibilidad >> firms_diario >> clima_diario
