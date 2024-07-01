from datetime import timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def f_callable(url, f_name):
    data = pd.read_csv(url)
    data.to_csv(f_name)


with DAG(
    'dag_3_2_13',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
) as dag:
    # Создадим оператор для исполнения python функции
    t1 = PythonOperator(
        task_id='download_file',
        python_callable=f_callable,
        op_kwargs={
                'url': (
                    'https://raw.githubusercontent.com/dm-novikov'
                    '/stepik_airflow_course/main/data/data.csv'
                ),
                'f_name': '/workspaces/Airflow-Docker/airflow/dags/3_2_13.csv',
            }
    )
