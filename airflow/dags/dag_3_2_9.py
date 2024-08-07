from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago


dag = DAG(
    'dag_3_2_9',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
)
t1 = EmptyOperator(task_id='task_1', dag=dag)
t2 = EmptyOperator(task_id='task_2', dag=dag)
t3 = EmptyOperator(task_id='task_3', dag=dag)
t4 = EmptyOperator(task_id='task_4', dag=dag)
t5 = EmptyOperator(task_id='task_5', dag=dag)
t6 = EmptyOperator(task_id='task_6', dag=dag)
t7 = EmptyOperator(task_id='task_7', dag=dag)

# Первый пример
# t1 >> t5 >> t7
# t2 >> [t5, t6] >> t7
# t4 >> t6 >> t7
# t4 >> t7
# t3 >> t6 >> t7

# Второй пример
t1 >> t4
t1 >> t2 >> [t4, t7]
t1 >> t3 >> t2 >> [t4, t7]
t1 >> t5 >> t3 >> t2 >> [t4, t7]
t1 >> t6 >> t2 >> [t4, t7]
