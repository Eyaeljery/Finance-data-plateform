from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="invoices_elt_daily",
    start_date=datetime(2025, 11, 1),
    schedule="@daily",
    catchup=False,
    tags=["saas", "raw", "elt"],
) as dag:

    extract_invoices = BashOperator(
        task_id="extract_invoices",
        bash_command="python /opt/airflow/scripts/extract_invoices.py --ds {{ ds }}",
    )

    load_invoices = BashOperator(
        task_id="load_invoices",
        bash_command="python /opt/airflow/scripts/load_invoices.py --ds {{ ds }}",
    )

    extract_invoices >> load_invoices
