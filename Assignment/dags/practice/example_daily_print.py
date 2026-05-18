from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def print_message(**context):
    logical_date = context["logical_date"]

    print("===================================")
    print(f"Running for date: {logical_date}")
    print("Hello from backfilled DAG!")
    print("===================================")


with DAG(
    dag_id="example_daily_print",
    start_date=datetime(2026, 5, 1),
    schedule="@daily",
    catchup=False,
    tags=["demo", "backfill"],
) as dag:

    task = PythonOperator(
        task_id="print_task",
        python_callable=print_message,
    )