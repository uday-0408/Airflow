from airflow.sdk import dag, task
from datetime import datetime, timedelta

@dag(
    dag_id="configured_pipeline",
    schedule="0 8 * * *",  # Run at 8:00 AM every day
    start_date=datetime(2024, 1, 1),
    catchup=False,  
    default_args={
        "owner": "Uday",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure":False,
        "email": ["uday.chauahn@kenexai.com"]
    },
    tags=["Assignment-1", "configured", "Uday"],
)
def dag_config():
    @task()
    def check_config(task_instance=None, **context):
        task=task_instance.task
        print(f"Owner: {task.owner}")
        print(f"Retries: {task.retries}")
        print(f"Retry Delay: {task.retry_delay}")
        print(f"Email on Failure: {task.email_on_failure}")
        print(f"Email: {task.email}")
    @task()
    def show_schedule(**context):
        # schedule=context['schedule']
        ds=context['ds']
        print(f"This pipeline runs every day at 8:00 AM")
        print(f"Execution date: {ds}")

    check_config()>>show_schedule()
configured_pipeline = dag_config()