from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator


def extract_data(url, file_name, date) -> None:
    data = pd.read_csv(f'{url}/{date}.csv')
    data.to_csv(file_name.format(date), index=False)


def merge_data(data1_file, data2_file, key, file_name, date) -> None:
    data1 = pd.read_csv(data1_file.format(date))
    data2 = pd.read_csv(data2_file.format(date))
    data1.merge(data2, on=key).to_csv(file_name.format(date), index=False)


dag = DAG(
    'dag_3_4_12',
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2021, 1, 4),
    schedule_interval='@daily',
)

currency_op = PythonOperator(
    task_id='extract_currency',
    python_callable=extract_data,
    dag=dag,
    op_kwargs={
        'url': (
            'https://raw.githubusercontent.com/datanlnja'
            '/airflow_course/main/excangerate/'
        ),
        'file_name': '/workspaces/Airflow-Docker/data/currency_{0}.csv',
        'date': '{{ ds }}',
    },
)

data_op = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
    op_kwargs={
        'url': (
            'https://raw.githubusercontent.com/datanlnja'
            '/airflow_course/main/data/'
        ),
        'file_name': '/workspaces/Airflow-Docker/data/data_{0}.csv',
        'date': '{{ ds }}',
    },
)

mg_data_op = PythonOperator(
    task_id='merge_data',
    python_callable=merge_data,
    dag=dag,
    op_kwargs={
        'data1_file': '/workspaces/Airflow-Docker/data/currency_{0}.csv',
        'data2_file': '/workspaces/Airflow-Docker/data/data_{0}.csv',
        'key': 'date',
        'file_name': '/workspaces/Airflow-Docker/data/result_{0}.csv',
        'date': '{{ ds }}',
    },
)

[currency_op, data_op] >> mg_data_op
