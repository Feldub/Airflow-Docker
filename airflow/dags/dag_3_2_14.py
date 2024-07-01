import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def extract_data(url, file_name, date='2021-01-01') -> None:
    data = pd.read_csv(f'{url}/{date}.csv')
    data.to_csv(file_name, index=False)


def merge_data(data1_file, data2_file, key, file_name) -> None:
    data1 = pd.read_csv(data1_file)
    data2 = pd.read_csv(data2_file)
    data1.merge(data2, on=key).to_csv(file_name, index=False)


dag = DAG(
    'dag_3_2_14',
    start_date=days_ago(1),
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
        'file_name': '/workspaces/Airflow-Docker/data/currency.csv',
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
        'file_name': '/workspaces/Airflow-Docker/data/data.csv'
    },
)

mg_data_op = PythonOperator(
    task_id='merge_data',
    python_callable=merge_data,
    dag=dag,
    op_kwargs={
        'data1_file': '/workspaces/Airflow-Docker/data/currency.csv',
        'data2_file': '/workspaces/Airflow-Docker/data/data.csv',
        'key': 'date',
        'file_name': '/workspaces/Airflow-Docker/data/result.csv',
    },
)

[currency_op, data_op] >> mg_data_op
