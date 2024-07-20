from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.dates import days_ago


dag = DAG(
    'dag_6_1_8',
    schedule_interval='@daily',
    start_date=days_ago(1),
)


def response_check(response, task_instance):
    if int(response.text) == 5:
        return True


sensor = HttpSensor(
    task_id='http_sensor',
    http_conn_id='http_default',
    response_check=response_check,
    endpoint='integers/?num=1&min=1&max=5&col=1&base=10&format=plain',
    poke_interval=10,
    timeout=60,
    dag=dag,
)
