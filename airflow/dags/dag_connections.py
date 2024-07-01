from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago


dag = DAG(
    'connection_examples',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
)

task1 = DummyOperator(task_id='task_1', dag=dag)
task2 = DummyOperator(task_id='task_2', dag=dag)
task3 = DummyOperator(task_id='task_3', dag=dag)
task4 = DummyOperator(task_id='task_4', dag=dag)
task5 = DummyOperator(task_id='task_5', dag=dag)
task6 = DummyOperator(task_id='task_6', dag=dag)

# Вариант 1
[task1 >> task2 >> task4 >> task6, task1 >> task3 >> task5 >> task6]

# Вариант 2
# task1 >> task2 >> task4 >> task6
# task1 >> task3 >> task5 >> task6
