from airflow import DAG # type: ignore
from airflow.providers.standard.operators.python import PythonOperator # type: ignore
# from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime


def name_the_file_func(**context):
    ds = context['ds']
    ds_nodash = context['ds_nodash']

    print(f"Naming file for date: {ds}")

    file_path = f"/reports/{ds_nodash}_daily_report.csv"
    print(file_path)

    return file_path

def explain_execution_date_func(**context):
    ds = context['ds']
    yesterday_ds = context['macros'].ds_add(ds, -1)

    print(f"1. This run's ds is: {ds}")
    print("2. ds is one day behind because Airflow only processes data AFTER the day is finished.")
    print(f"3. So this pipeline is processing data from: {yesterday_ds}")



with DAG(
    dag_id="daily_file_namer",
    schedule="0 8 * * *", 
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    show_template_vars = BashOperator(
        task_id="show_template_vars",
        bash_command="""
        echo "Run date: {{ ds }}"
        echo "Date no dashes: {{ ds_nodash }}"
        echo "Yesterday: {{ macros.ds_add(ds, -1) }}"
        echo "Tomorrow: {{ macros.ds_add(ds, 1) }}"
        echo "DAG: {{ dag.dag_id }}"
        echo "Task: {{ task.task_id }}"
        echo "Run ID: {{ run_id }}"
        """
    )

    name_the_file = PythonOperator(
        task_id="name_the_file",
        python_callable=name_the_file_func
    )

    explain_execution_date = PythonOperator(
        task_id="explain_execution_date",
        python_callable=explain_execution_date_func
    )

    show_template_vars >> name_the_file >> explain_execution_date # type: ignore