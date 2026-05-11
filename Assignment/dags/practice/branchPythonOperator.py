from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow import DAG
from datetime import datetime
import random


def get_file_size():
    """Simulate checking file size (in MB)"""
    print("Checking file size...")
    size = random.randint(10, 200)
    print(f"File size is: {size} MB")
    return size


def decide_processing_path(**context):
    """Return the task_id of the branch to take."""
    file_size_mb = get_file_size()

    if file_size_mb > 100:
        print("Large file detected → Using Spark path")
        return 'process_large_file'
    else:
        print("Small file detected → Using Pandas path")
        return 'process_small_file'


def process_large():
    print("Processing large file using Spark...")


def process_small():
    print("Processing small file using Pandas...")


def complete_pipeline():
    print("Pipeline finished successfully!")


with DAG(
    dag_id='branching_example',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['branching', 'example', 'etl']
) as dag:

    check = BranchPythonOperator(
        task_id='check_file_size',
        python_callable=decide_processing_path
    )

    large = PythonOperator(
        task_id='process_large_file',
        python_callable=process_large
    )

    small = PythonOperator(
        task_id='process_small_file',
        python_callable=process_small
    )

    complete = PythonOperator(
        task_id='complete',
        python_callable=complete_pipeline,
        trigger_rule='none_failed'  # important for branching
    )

    # Dependency (branching structure)
    check >> [large, small] >> complete