from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.dates import days_ago

args = {
    "owner": "airflow",
}

with DAG(
    dag_id="dag_5_2_3_2",
    default_args=args,
    start_date=days_ago(1),
) as dag:

    cond_true = ShortCircuitOperator(
        task_id="condition_is_True",
        python_callable=lambda: True,
    )

    cond_false = ShortCircuitOperator(
        task_id="condition_is_False",
        python_callable=lambda: False,
    )

    # Сгенерируем много задач через цикл, чуть дальше мы изучим это подробнее
    ds_true = [EmptyOperator(task_id="true_" + str(i)) for i in range(2)]
    ds_false = [EmptyOperator(task_id="false_" + str(i)) for i in range(2)]

    # Создает цепочку из задач аналогично >> такой операции
    chain(cond_true, *ds_true)
    chain(cond_false, *ds_false)
