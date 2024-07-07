from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Создаем DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
    'retries': 1,
}

dag = DAG(
    'dag_with_pools_example',
    default_args=default_args,
    schedule_interval='@daily',
)

# Task 1 с использованием pool_1
task1 = DummyOperator(
    task_id='task1',
    pool='pool_1',
    dag=dag,
)

# Task 2 с использованием pool_2
task2 = PythonOperator(
    task_id='task2',
    python_callable=lambda: print('Hello from task2!'),
    pool='pool_2',
    dag=dag,
)

# Task 3 с использованием pool_3
task3 = PythonOperator(
    task_id='task3',
    python_callable=lambda: print('Hello from task3!'),
    pool='pool_3',
    dag=dag,
)

# Определяем зависимости
task1 >> task2 >> task3
