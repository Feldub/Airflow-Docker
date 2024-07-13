from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago


def function_with_exception():
    raise AirflowException


def on_failure_callback(context):
    send_message = TelegramOperator(
        task_id="send_message_telegram",
        telegram_conn_id="my_telegram_conn",
        text="Oh no, everything's broken!!!",
    )
    return send_message.execute(context)


dag = DAG(
    "dag_5_1_7",
    schedule_interval="@daily",
    start_date=days_ago(1),
    on_failure_callback=on_failure_callback,
)

task_with_exception = PythonOperator(
    task_id="task_with_exception",
    python_callable=function_with_exception,
    dag=dag,
)
