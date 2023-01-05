from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def print_hello():
    print("hola gente de platzi")

with DAG(
    dag_id="primer_pythonoperator_hourly",
    description="primer python operator hourly",
    start_date=datetime.now(),
    end_date=datetime.now()  + timedelta(days = 10),
    schedule_interval="@daily"
) as dag:
    t1 = PythonOperator(
        task_id="hello_with_python_hourly",
        python_callable=print_hello
    )
    t1