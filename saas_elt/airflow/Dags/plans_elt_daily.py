from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="plans_elt_daily",
    start_date=datetime(2025, 11, 1),
    schedule=None,
    catchup=False,
    tags=["saas", "raw", "elt"],
) as dag:

    extract_plans = BashOperator(
        task_id="extract_plans",
        bash_command="python /opt/airflow/scripts/extract_plans.py",
    )

    load_plans = BashOperator(
        task_id="load_plans",
        bash_command="python /opt/airflow/scripts/load_plans.py",
    )

    extract_plans >> load_plans
