from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
from google.oauth2 import service_account


@dag(
    schedule_interval="0 10 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["load_gcp"],
)
def extract_and_load():

    @task()
    def sql_extract():
        hook = MsSqlHook(mssql_conn_id="sqlserver")
        sql = """SELECT t.name AS table_name
                  FROM sys.tables t
                  WHERE t.name IN ('sales');"""
        df = hook.get_pandas_df(sql)
        return df.to_dict("dict")

    @task()
    def gcp_load(tbl_dict: dict):
        credentials = service_account.Credentials.from_service_account_file("/opt/airflow/gcp-key.json")
        project_id = "your_project_id"
        dataset_ref = "your_dataset"

        for value in tbl_dict.values():
            for table_name in value.values():
                hook = MsSqlHook(mssql_conn_id="sqlserver")
                df = hook.get_pandas_df(f"SELECT * FROM {table_name}")
                df.to_gbq(
                    destination_table=f"{dataset_ref}.src_{table_name}",
                    project_id=project_id,
                    credentials=credentials,
                    if_exists="replace",
                )

    data = sql_extract()
    gcp_load(data)


extract_and_load()
