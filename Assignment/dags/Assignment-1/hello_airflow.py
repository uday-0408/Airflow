from airflow.decorators import dag, task
from datetime import datetime


@dag(
    dag_id="hello_airflow",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["Assignment-1", "hello", "Uday"],
)
def hello_airflow():

    @task()
    def say_hello():
        name="Uday"
        print(f"Hello from Airflow! My name is {name}")

    @task()
    def show_date(**context):
        ds = context['ds']
        print(f"Today's execution date is: {ds}")

    say_hello() >> show_date()


hello_airflow()