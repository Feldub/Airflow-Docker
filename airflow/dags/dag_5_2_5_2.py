from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

default_args = {
    'start_date': datetime(2021, 1, 1),
}

dag = DAG('dag_5_2_5_2', schedule_interval='@daily', default_args=default_args)


# Функция для условия выбора нужного task_id
def branch_func(**kwargs):
    xcom_value = int(kwargs['ti'].xcom_pull(task_ids='start_task'))
    if xcom_value >= 5:
        return 'continue_task'
    else:
        return 'stop_task'


# Стартовый оператор который пуляет в xcom 10
start_op = BashOperator(
    task_id='start_task',
    bash_command="echo 10",
    dag=dag,
)

# Сам оператор условие
branch_op = BranchPythonOperator(
    task_id='branch_task',
    provide_context=True,
    python_callable=branch_func,
    dag=dag,
)

continue_op = EmptyOperator(task_id='continue_task', dag=dag)
stop_op = EmptyOperator(task_id='stop_task', dag=dag)

start_op >> branch_op >> [continue_op, stop_op]
