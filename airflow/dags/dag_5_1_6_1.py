from airflow import DAG
from airflow.utils.dates import days_ago

from custom_operators.hello_operator import HelloOperator


dag = DAG(
    "dag_5_1_6_1",
    start_date=days_ago(1),
    schedule_interval="@daily",
)

hello_task = HelloOperator(
    task_id="hello_task",
    name="foo_bar",
    dag=dag,
)
