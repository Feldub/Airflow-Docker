"""My first DAG."""

import sqlite3

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


CON = sqlite3.connect("example.db")


def extract_data(url, tmp_file, **context) -> None:
    """Extract CSV."""

    pd.read_csv(url).to_csv(tmp_file)


def transform_data(
    group,
    agreg,
    tmp_file,
    tmp_agg_file,
    **context,
) -> None:
    """Group by data."""

    data = pd.read_csv(tmp_file)
    data.groupby(group, as_index=False).agg(agreg).to_csv(tmp_agg_file)


def load_data(tmp_file, table_name, conn=CON, **context) -> None:
    data = pd.read_csv(tmp_file)
    data["insert_time"] = pd.to_datetime("now")
    data.to_sql(table_name, conn, if_exists="replace", index=False)


dag = DAG(
    dag_id="dag",
    schedule_interval="@daily",
    start_date=days_ago(1),
    default_args={"owner": "airflow"},
)

extract_data = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    dag=dag,
    op_kwargs={
        "url": (
            "https://raw.githubusercontent.com/dm-novikov/"
            "stepik_airflow_course/main/data/data.csv"
        ),
        "tmp_file": "/tmp/file.csv",
    },
)

transform_data = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
    op_kwargs={
        "tmp_file": "/tmp/file.csv",
        "tmp_agg_file": "tmp/file_agg.csv",
        "group": ["A", "B", "C"],
        "agreg": {"D": "sum"},
    },
)

load_data = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    dag=dag,
    op_kwargs={
        "tmp_file": "/tmp/file.csv",
        "table_name": "table",
    },
)

email_op = EmailOperator(
    task_id="send_email",
    # dag=dag,
    to="feldub@yandex.ru",
    subject="Airflow Email",
    html_content=None,
    files=["/tmp/file_agg.csv.csv"]
)

bash_op = BashOperator(
    task_id="bash",
    bash_command="cat /tmp/file_agg.csv",
    dag=dag,
)

dag.doc_md = __doc__

extract_data >> transform_data >> load_data >> bash_op
