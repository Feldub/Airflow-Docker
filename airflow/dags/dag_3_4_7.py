from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def my_func(hello, date, **context):
    print(hello)
    print(date)
    print(context["task"].task_id)


with DAG(
    "dag_3_4_7",
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2021, 1, 10),
) as dag:
    python_task = PythonOperator(
        task_id="python_task",
        python_callable=my_func,
        op_kwargs={
            "hello": "Hello World",
            "date": "{{ execution_date }}",
        }
    )
