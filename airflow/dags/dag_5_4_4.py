from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

dag = DAG(
   'dag_5_4_4',
   schedule_interval=timedelta(days=1),
   start_date=days_ago(1),
)

tasks_list = []
for i in range(0, 10):
    tasks_list.append(EmptyOperator(task_id=f'task_{i}', dag=dag))
    if i:
        tasks_list[i-1] >> tasks_list[i]
