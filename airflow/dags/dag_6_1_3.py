from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago


dag = DAG(
    dag_id="dag_6_1_3",
    start_date=days_ago(1),
    schedule_interval="@daily",
)


def save_response(response):
    with open("/workspaces/Airflow-Docker/data/test.txt", "w") as file:
        file.write(response.text)


http_op = SimpleHttpOperator(
    task_id="http_operator",
    http_conn_id="http_random_org",
    method="GET",
    dag=dag,
    response_filter=save_response,
)
