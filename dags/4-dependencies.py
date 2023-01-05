from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def print_hello():
    print("hola gente de platzi")

with DAG(
    dag_id="primer_dependencies",
    description="primer dependencies operator",
    start_date=datetime.now(),
    schedule_interval="@once"
) as dag:
    t1 = EmptyOperator(task_id="t1_dummy")
    t2 = BashOperator(
        task_id="t2_hello_with_bash",
        bash_command="echo 'hi there'"
    )
    t3 = PythonOperator(
        task_id="t1_hello_with_python",
        python_callable=print_hello
    )
    t1 >> t2 >> t3
