from airflow import DAG # type: ignore
from airflow.providers.standard.operators.python import PythonOperator # type: ignore
from airflow.providers.http.hooks.http import HttpHook # type: ignore
from datetime import datetime

def get_random_joke(**kwargs):
    hook = HttpHook(method="GET", http_conn_id="joke_api")
    response = hook.run("/random_joke")

    joke = response.json()
    print(
        f"Type: {joke['type']}\n"
        f"Setup: {joke['setup']}\n"
        f"Punchline: {joke['punchline']}",
        flush=True
    )

    return joke["setup"]

def get_ten_jokes(**kwargs):
    hook = HttpHook(method="GET", http_conn_id="joke_api")
    response = hook.run("/random_ten")

    jokes = response.json()
    count = len(jokes)

    print(f"Fetched {count} jokes")

    if count > 0:
        print(f"First setup: {jokes[0]['setup']}")

    return count

def show_results(**kwargs):
    ti = kwargs['ti']

    setup = ti.xcom_pull(task_ids="get_random_joke")
    count = ti.xcom_pull(task_ids="get_ten_jokes")

    print(f"Random joke setup: {setup}")
    print(f"Total jokes fetched across both tasks: {1 + count}")
    print("Connection used: joke_api — no URL written in this file.")

with DAG(
    dag_id="joke_fetch_demo",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    get_random_joke = PythonOperator(
        task_id="get_random_joke",
        python_callable=get_random_joke
    )

    get_ten_jokes = PythonOperator(
        task_id="get_ten_jokes",
        python_callable=get_ten_jokes
    )

    show_results = PythonOperator(
        task_id="show_results",
        python_callable=show_results
    )
    get_random_joke >> get_ten_jokes >> show_results #  type: ignore