from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="primer_bashoperator",
    description="primer bash operator",
    start_date=datetime.now(),
    schedule_interval="@once"
) as dag:
    t1 = BashOperator(
        task_id="hello_with_bash",
        bash_command="echo 'hi there'"
    )
    t1