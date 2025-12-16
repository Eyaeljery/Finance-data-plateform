from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="subscriptions_elt_daily",
    start_date=datetime(2025, 12, 16),
    schedule="@daily",
    catchup=True,
    tags=["saas", "raw", "elt"],
) as dag:

    extract_subscriptions = BashOperator(
        task_id="extract_subscriptions",
        bash_command="python /opt/airflow/scripts/extract_subscriptions.py --ds {{ ds }}",
    )

    load_subscriptions = BashOperator(
        task_id="load_subscriptions",
        bash_command="python /opt/airflow/scripts/load_subscriptions.py --ds {{ ds }}",
    )

    extract_subscriptions >> load_subscriptions
