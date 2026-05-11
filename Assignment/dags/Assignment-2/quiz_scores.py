from airflow import DAG # type: ignore
from airflow.providers.standard.operators.python import PythonOperator # type: ignore
from datetime import datetime

def maths_score(**kwargs):
    ti = kwargs['ti']
    ti.xcom_push(key="maths", value=78)
    print("Maths score pushed: 78")

def english_score(**kwargs):
    ti = kwargs['ti']
    ti.xcom_push(key="english", value=85)
    print("English score pushed: 85")


def science_score(**kwargs):
    ti = kwargs['ti']
    ti.xcom_push(key="science", value=62)
    print("Science score pushed: 62")

def calculate_average(**kwargs):
    ti = kwargs['ti']
    maths = ti.xcom_pull(key="maths", task_ids="maths_score")
    english = ti.xcom_pull(key="english", task_ids="english_score")
    science = ti.xcom_pull(key="science", task_ids="science_score")

    average = (maths + english + science) / 3
    if average >= 90:
        grade = "A"
    elif average >= 75:
        grade = "B"
    elif average >= 60:
        grade = "C"
    else:
        grade = "D"
    print(
            f"""
Maths: {maths}\nEnglish: {english}\nScience: {science}\nAverage: {average}\nGrade: {grade}
    """,flush=True
        )
    return {"average": average, "grade": grade}

def print_report(**kwargs):
    ti = kwargs['ti']

    maths = ti.xcom_pull(task_ids="maths_score", key="maths")
    english = ti.xcom_pull(task_ids="english_score", key="english")
    science = ti.xcom_pull(task_ids="science_score", key="science")

    result = ti.xcom_pull(task_ids="calculate_average")

    print(
    f"""
----- QUIZ REPORT -----
Maths: {maths}\nEnglish: {english}\nScience: {science}\nAverage: {result['average']}\nGrade: {result['grade']}
------------------------
    """,flush=True)
with DAG(
    dag_id="quiz_scores",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    maths_score = PythonOperator(
        task_id="maths_score",
        python_callable=maths_score
    )

    english_score = PythonOperator(
        task_id="english_score",
        python_callable=english_score
    )

    science_score = PythonOperator(
        task_id="science_score",
        python_callable=science_score
    )

    calculate_average = PythonOperator(
        task_id="calculate_average",
        python_callable=calculate_average,
        trigger_rule="all_done"
    )

    print_report = PythonOperator(
        task_id="print_report",
        python_callable=print_report,
        trigger_rule="all_done"
    )
    [maths_score, english_score, science_score] >> calculate_average >> print_report # type: ignore