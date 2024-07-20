import random

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.dates import days_ago


class DummyOperator(BaseOperator):

    ui_color = "#e8f7e4"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        return random.randint(0, 9)


dag = DAG("dag_6_1_5", schedule_interval="@daily", start_date=days_ago(1))
t1 = DummyOperator(task_id="task_1", dag=dag)
t2 = DummyOperator(task_id="task_2", dag=dag)

t1 >> t2
