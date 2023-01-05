from airflow import DAG
from datetime import datetime
from hellooperator import HelloOperator

with DAG(
    dag_id="custom_operator_dag",
    description="primer custom_operator_dag",
    start_date=datetime.now(),
    schedule_interval="@once"
) as dag:
    t = HelloOperator(
        task_id="hello_operator",
        name="Dani"
    )
    t
