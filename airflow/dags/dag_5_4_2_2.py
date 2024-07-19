from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago


def create_dag(dag_id, default_args, schedule="@daily"):

    dag = DAG(
        dag_id,
        schedule_interval=schedule,
        default_args=default_args,
    )

    with dag:
        _ = EmptyOperator(task_id="task")

    return dag


for n in range(1, 4):
    dag_id = f"dag_5_4_2_2_{n}"

    default_args = {
        "owner": "airflow",
        "start_date": days_ago(1),
    }

    globals()[dag_id] = create_dag(dag_id, default_args)
