from datetime import datetime
from random import randint

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 2, 16),
}

dag = DAG("dag_5_2_6", schedule_interval="@daily", default_args=default_args)


def rand(**kwargs):
    kwargs["ti"].xcom_push(key="rand", value=randint(0, 10))


def branch(**kwargs):
    if kwargs["ti"].xcom_pull(key="rand") > 5:
        return "higher"
    else:
        return "lower"


lower = EmptyOperator(
    task_id="lower",
    dag=dag,
)

higher = EmptyOperator(
    task_id="higher",
    dag=dag,
)

branch_op = BranchPythonOperator(
    task_id="branch_task",
    python_callable=branch,
    dag=dag,
)

random_number = PythonOperator(
    task_id="random_number",
    python_callable=rand,
    dag=dag,
)

random_number >> branch_op >> [lower, higher]
