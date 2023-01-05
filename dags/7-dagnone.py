from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("hola gente de platzi")

with DAG(
    dag_id="primer_pythonoperator_no",
    description="primer python operator no",
    start_date=datetime.now(),
    schedule_interval="@once"
) as dag:
    t1 = PythonOperator(
        task_id="hello_with_python_no",
        python_callable=print_hello
    )
    t1