"""DAG manual de la etapa 4: capa Gold con dbt."""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

DBT = "/opt/dbt-venv/bin/dbt"
ARGS = "--project-dir /opt/airflow/proyecto/dbt --profiles-dir /opt/airflow/proyecto/dbt"
# primer dia del mes anterior, en UTC
MES_ANTERIOR = 'MES=$(date -u -d "$(date -u +%Y-%m-01) -1 month" +%Y-%m-01)'

with DAG(
    dag_id="gold_manual",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["focos", "etapa4"],
    default_args={"owner": "agustin", "retries": 0},
):
    dbt_seed = BashOperator(task_id="dbt_seed", bash_command=f"{DBT} seed {ARGS}")
    dbt_run_base = BashOperator(
        task_id="dbt_run_base",
        bash_command=f"{DBT} run --select staging intermediate {ARGS}",
    )
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"{MES_ANTERIOR} && "
            f'{DBT} run --select riesgo_departamento_vigente --vars "{{mes_corte: $MES}}" {ARGS} && '
            f'{DBT} snapshot --vars "{{mes_corte: $MES}}" {ARGS}'
        ),
    )
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT} run --select marts {ARGS}",
    )
    dbt_test = BashOperator(task_id="dbt_test", bash_command=f"{DBT} test {ARGS}")

    dbt_seed >> dbt_run_base >> dbt_snapshot >> dbt_run_marts >> dbt_test
