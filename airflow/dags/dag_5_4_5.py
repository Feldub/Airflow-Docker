import numpy as np

from airflow import DAG
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.dates import days_ago


SENSORS_NUM = 3


class CustomSensor(BaseSensorOperator):

    def poke(self, context):
        return_value = np.random.binomial(1, 0.3)
        return bool(return_value)


default_args = {
    "poke_interval": 4,
    "timeout": 50,
    "mode": "reschedule",
    "soft_fail": True,
}
# Здесь и далее код создание задачи-сенсора

dag = DAG(
    dag_id="dag_5_4_5",
    start_date=days_ago(1),
    schedule="@daily",
    default_args=default_args,
)

for sensor_num in range(1, SENSORS_NUM + 1):
    CustomSensor(task_id=f"sensor_{sensor_num}", dag=dag)
