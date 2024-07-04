from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


# Функция которая положит в Xcom некотрое значение
def push_function(ti, **context):
    ti.xcom_push(key="key_name", value="Some text")


# Функция которая извлечет это значение
def pull_function(ti, **context):
    # Через task_instance обратимся по имени к Xcom
    value_pulled = ti.xcom_pull(key="key_name")
    print(value_pulled)


dag = DAG(
    "dag_3_4_8",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
)

push_task = PythonOperator(
    task_id="push_task",
    python_callable=push_function,
    dag=dag,
)

pull_task = PythonOperator(
    task_id="pull_task",
    python_callable=pull_function,
    dag=dag,
)

push_task >> pull_task
