from airflow.sdk import dag, task
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


def count_tickets():
    import random
    count=random.randint(0, 20)
    print(f"Overnight tickets: {count}")
    return count
def check_workload(**context):
    if context['ti'].xcom_pull(task_ids='count_tickets') >= 10:
        print("High workload detected → Prioritizing urgent tasks")
        return 'busy_day'
    else:
        print("Workload manageable → Proceeding with regular tasks")
        return 'quiet_day'
def busy_day():
    print("BUSY DAY — calling in extra support staff.")
def quiet_day():
    print("QUIET DAY — normal staffing today.")
def daily_summary():
    print("Morning checklist complete. Have a good day!")


with DAG(
    dag_id="morning_checklist",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False
)as dag:
    
    system_check =BashOperator(
        task_id="system_check",
        bash_command="echo 'Office system check — all OK' && date"
    )

    count_tickets=PythonOperator(
        task_id="count_tickets",
        python_callable=count_tickets
    )

    check_workload=BranchPythonOperator(
        task_id="check_workload",
        python_callable=check_workload
        # provide_context=True
    )

    busy_day=PythonOperator(
        task_id="busy_day",
        python_callable=busy_day
    )

    quiet_day=PythonOperator(
        task_id="quiet_day",
        python_callable=quiet_day
    )

    daily_summary=PythonOperator(
        task_id="daily_summary",
        python_callable=daily_summary,
        trigger_rule="none_failed"
    )

    system_check>>count_tickets>>check_workload>>[busy_day,quiet_day]>>daily_summary


       

    