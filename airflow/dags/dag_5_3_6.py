from datetime import timedelta

import pandas as pd
from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.utils.dates import days_ago


class FileSQLiteTransferHook(SqliteHook):

    def get_pandas_df(self, url_or_path):
        """Ваш код который читает данные из файла."""
        return pd.read_csv(url_or_path)

    def insert_df_to_db(self, data):
        """
        Данный метод вставляет данные в БД
        self.get_conn() это готовый метод SqliteHook для создания подключения.
        """

        data.to_sql("table", con=self.get_conn())


class FileSQLiteTransferOperator(BaseOperator):

    def __init__(self, path, **kwargs):
        super().__init__(**kwargs)
        self.hook = None
        self.path = path  # Путь до файла

    def execute(self, context):

        # Создание объекта хука
        self.hook = FileSQLiteTransferHook()

        # Ваш код вызовите метод который
        # читает данные и затем записывает данные в БД
        data = self.hook.get_pandas_df(self.path)
        self.hook.insert_df_to_db(data)


# Запуск вашего Оператора

dag = DAG(
    "dag_5_3_6",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
)

t1 = FileSQLiteTransferOperator(
    task_id="transfer_data",
    path=(
        "https://raw.githubusercontent.com/"
        "dm-novikov/stepik_airflow_course/main/data_new/2021-01-04.csv"
    ),
    dag=dag,
)
