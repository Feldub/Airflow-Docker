from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago


def sensor_func():
    print("Scanning...")
    return False


dag = DAG(
    "dag_5_2_2",
    schedule_interval="@daily",
    start_date=days_ago(1),

)

sensor = PythonSensor(
    task_id="sensor_op",
    mode="poke",
    poke_interval=2,
    timeout=10,
    python_callable=sensor_func,
    dag=dag,
    soft_fail=True,
)

bash = BashOperator(
    task_id="bash",
    bash_command="echo 'Aboba'"
)

sensor >> bash
