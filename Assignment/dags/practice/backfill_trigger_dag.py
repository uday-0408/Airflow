from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

DB_URL = "postgresql://postgres:postgres@postgres:5432"


with DAG(
    dag_id="backfill_trigger_dag",
    schedule=None,
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["backfill"],
) as dag:

    trigger_backfill = BashOperator(
        task_id="trigger_backfill",
        bash_command=f"""
        export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN='{DB_URL}' &&

        airflow backfill create \
        --dag-id {{{{ dag_run.conf['dag_id'] }}}} \
        --from-date {{{{ dag_run.conf['date_start'] }}}} \
        --to-date {{{{ dag_run.conf['date_end'] }}}}
        """
    )

    trigger_backfill