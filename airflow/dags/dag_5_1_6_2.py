from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.telegram.operators.telegram import TelegramOperator

from custom_operators.hello_operator import HelloOperator


def on_success_callback(context):
    send_message = TelegramOperator(
        task_id="send_message_telegram",
        telegram_conn_id="telegram_id",
        chat_id="-1001525736146",
        text="Hello from Airflow!",
    )
    return send_message.execute(context)


dag = DAG(
    "dag_5_1_6_2.py",
    start_date=days_ago(1),
    on_success_callback=on_success_callback,
    schedule_interval="@daily",
)

hello_task = HelloOperator(task_id="hello_task", name="foo_bar", dag=dag)
