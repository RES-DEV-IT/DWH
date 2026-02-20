from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow import DAG
from icp_metrics import shifts, kks_vs_qty, fill_percent, date_correctness, \
    errors_in_date_type, two_weeks, yellow_dates
from gs import insert_to_gs


URL = "https://docs.google.com/spreadsheets/d/1Clbm3ie2e8HqLjHu0FLJVMUt51Y_4Fgb8Q8O6WfbpJw"

@task
def main_task():

    hook = PostgresHook(postgres_conn_id="resdb_connection")

    for manuf_name in ["DelVal", "Dembla", "HawaTubes", "LC", "RKC", "Nirmal", "EHO"]:
        records = shifts(hook, manuf_name)

        insert_to_gs(URL, records, f"Changes {manuf_name}", append=True)

    df = kks_vs_qty(hook)
    insert_to_gs(URL, df.values.tolist(), "KKS vs QTY", append=False)

    df = fill_percent(hook)
    insert_to_gs(URL, df.values.tolist(), "Fill percent", append=False)

    df = date_correctness(hook)
    insert_to_gs(URL, df.values.tolist(), "Date correctness", append=False)

    df = errors_in_date_type(hook)
    insert_to_gs(URL, df.values.tolist(), "Mistakes in date type", append=False)

    df = two_weeks(hook)
    insert_to_gs(URL, df.values.tolist(), "Two weeks", append=False)

    df = yellow_dates(hook)
    insert_to_gs(URL, df.values.tolist(), "Yellow dates", append=False)

with DAG(
    dag_id="icp_metrics_sheet",
    default_args={"owner": "DEV", "retries": 0},
    start_date=datetime(2025, 11, 27, 4, 0, 0, 0),
    schedule_interval="30 6-15/2 * * mon-fri",
    catchup=False
):
    main_task()