from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("hola gente de platzi")

with DAG(
    dag_id="primer_pythonoperator_cron",
    description="primer python operator cron",
    start_date=datetime.now(),
    schedule_interval="0 * * * *",
    default_args={
        "depends_on_past":True
    }
) as dag:
    t1 = PythonOperator(
        task_id="hello_with_python_cron",
        python_callable=print_hello
    )
    t1