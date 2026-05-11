from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="dependencies",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["Assignment-1", "dependencies"],
)
def dependencies():
    @task()
    def start(**context):
        print("Pipeline started — checking all systems")
        print(f"Execution date: {context['ds']}")
    
    @task()
    def task_a(**context):
        print("Task A running — processing customer data")
    @task()
    def task_b(**context):
        print("Task B running — processing order data")
    @task()
    def task_c(**context):
        print("Task C running — processing product data")

    @task()
    def finish(**context):
        print("Pipeline finished — all tasks completed")
        print(f"Execution date: {datetime.now()}")
    start() >> [task_a(), task_b(), task_c()] 

    [task_a(), task_b(), task_c()] >> finish()
dependencies = dependencies()