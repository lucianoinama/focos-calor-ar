"""Pipeline completo: extraccion -> Bronze -> Silver -> Postgres -> dbt. Por ahora se dispara a mano."""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

RUN = "python /opt/airflow/proyecto/scripts/run.py"
DBT = "/opt/dbt-venv/bin/dbt"
ARGS = "--project-dir /opt/airflow/proyecto/dbt --profiles-dir /opt/airflow/proyecto/dbt"

# (task_id, comando, minutos de timeout, reintentos)
TAREAS = [
    ("firms_diario", f"{RUN} firms_diario", 15, 2),
    ("clima_diario", f"{RUN} clima_diario", 30, 2),
    ("bronze_focos", f"{RUN} bronze_focos", 30, 0),
    ("bronze_clima", f"{RUN} bronze_clima", 30, 0),
    ("silver_focos", f"{RUN} silver_focos", 30, 0),
    ("silver_clima", f"{RUN} silver_clima", 30, 0),
    ("exportar_postgres", f"{RUN} exportar_postgres", 30, 0),
    ("dbt_run", f"{DBT} run {ARGS}", 15, 0),
    ("dbt_test", f"{DBT} test {ARGS}", 15, 0),
    ("dbt_freshness", f"{DBT} source freshness {ARGS}", 5, 0),
]

with DAG(
    dag_id="pipeline_diario",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule=None,  # manual por ahora; ver seccion 6 del handoff de la etapa 5
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=pendulum.duration(hours=2),
    tags=["focos", "etapa5"],
    default_args={"owner": "agustin", "retry_delay": pendulum.duration(minutes=10)},
):
    anterior = None
    for task_id, comando, minutos, reintentos in TAREAS:
        tarea = BashOperator(
            task_id=task_id,
            bash_command=comando,
            execution_timeout=pendulum.duration(minutes=minutos),
            retries=reintentos,
        )
        if anterior is not None:
            anterior >> tarea
        anterior = tarea
