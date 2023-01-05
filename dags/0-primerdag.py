from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="primer_dag",
    description="primer dag",
    start_date=datetime.now(),
    schedule_interval="@once"
) as dag:
    t1 = EmptyOperator(task_id="dummy")