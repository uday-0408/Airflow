from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

def use_json_variable():
    # 🔥 This is the correct way
    config = Variable.get("app_config", deserialize_json=True)

    # Access nested values
    name = config["user"]["name"]
    email = config["user"]["email"]
    skills = config["user"]["skills"]

    role = config["job"]["role"]
    experience = config["job"]["experience"]

    notifications = config["features"]["enable_notifications"]
    retries = config["features"]["max_retries"]

    print(f"User: {name}")
    print(f"Email: {email}")
    print(f"Skills: {skills}")
    print(f"Role: {role}")
    print(f"Experience: {experience} years")
    print(f"Notifications Enabled: {notifications}")
    print(f"Max Retries: {retries}")

with DAG(
    dag_id="json_variable_practice",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="use_json_variable_task",
        python_callable=use_json_variable
    )