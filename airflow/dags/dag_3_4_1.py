from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
   "owner": "airflow",
   "start_date": datetime(2018, 10, 1),
}

dag = DAG(
   dag_id="dag_3_4_1",
   default_args=default_args,
   schedule_interval="@daily",
)


# Функция использующая контекст
# **args/**context это словарь в который будет помещен
# Контекст задачи
def _print_exec_date(**context):
    print("Контекст", context)


print_exec_date = PythonOperator(
    task_id="print_exec_date",
    python_callable=_print_exec_date,
    dag=dag,
)
