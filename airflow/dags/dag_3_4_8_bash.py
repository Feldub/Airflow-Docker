from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

dag = DAG(
    "dag_3_4_8_bash",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
)

downloading_data = BashOperator(
    task_id="downloading_data",
    bash_command="echo 'Hello, I am a value!'",
    # Результат работы (то есть вывод команды echo ...) будет отправлен в Xcom,
    # в новых версиях это необязательный параметр и данные будут отправлены в
    # Xcom по умочланию
    do_xcom_push=True,
    dag=dag,
)

fetching_data = BashOperator(
    task_id="fetching_data",
    # Используя Jinja можно считать данные из xcom,
    # ti это тот же экземпляр задачи что и в PythonOperator
    bash_command=(
        "echo 'XCom fetched: "
        "{{ ti.xcom_pull(task_ids=[\'downloading_data\']) }}'"
    ),
    dag=dag,
)

fetching_data_2 = BashOperator(
    task_id="fetching_data_2",
    # Используя Jinja можно считать данные из xcom,
    # ti это тот же экземпляр задачи что и в PythonOperator
    bash_command=(
        "echo 'XCom fetched: "
        "{{ ti.xcom_pull(key=\'return_value\') }}'"
    ),
    dag=dag,
)

downloading_data >> fetching_data >> fetching_data_2
