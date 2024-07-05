from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    'dag_3_4_11',
    schedule_interval='@daily',
    start_date=datetime(2024, 7, 3),
    end_date=datetime(2024, 7, 8),
)


def func(date, **kwargs):
    print(date)


t1 = PythonOperator(
    task_id='t1',
    python_callable=func,
    dag=dag,
    op_kwargs={
        'date': '{{ ds }}',
    }
)
