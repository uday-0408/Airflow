import sched

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random

def print_random_number():
    number = random.randint(1, 100)
    print(f"Random number: {number}")
    return number

def print_hello():
    print("Hello, Airflow!")
    return 'random_number'

FUNCTIONS = {
    "random_number": print_random_number
}

def random_list(**context):
    value=context['ti'].xcom_pull(task_ids='print_hello')
    value=FUNCTIONS[value]
    arr=[value() for i in range(5)]
    return arr

def result(**context):
    arr=context['ti'].xcom_pull(task_ids='random_list')
    print(f"Result: {arr}")

with DAG(
    dag_id='practice_dag',
    start_date=datetime(2024, 6, 1),
    # schedule='* * * * *'
    schedule='@daily',
) as dag:
    print_hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )
    random_list_task = PythonOperator(
        task_id='random_list',
        python_callable=random_list
    )
    result_task = PythonOperator(
        task_id='result',
        python_callable=result
    )
    print_hello_task >> random_list_task >> result_task