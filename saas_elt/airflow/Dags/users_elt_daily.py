from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="users_elt_daily",
    start_date=datetime(2025, 11, 1),
    schedule="@daily",
    catchup=True,   # important pour backfill
    tags=["saas", "raw", "elt"],
) as dag:

    extract_users = BashOperator(
        task_id="extract_users",
        bash_command="python /opt/airflow/scripts/extract_users.py --ds {{ ds }}",
    )

    load_users = BashOperator(
        task_id="load_users",
        bash_command="python /opt/airflow/scripts/load_users.py --ds {{ ds }}",
    )

    extract_users >> load_users
