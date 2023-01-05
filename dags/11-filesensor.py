from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow import DAG
import os
import time

def write_file():
    time.sleep(10)
    f = open("/tmp/test.txt", "w")
    f.write("some text")
    f.close()

def remove_file():
    os.remove("/tmp/test.txt")

with DAG(
    dag_id="11-filesensor",
    description="filesensor",
    schedule_interval="*/5 * * * *",
    start_date=datetime.now(),
    end_date=datetime.now()  + timedelta(days = 10),
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="filesensor_with_python",
        python_callable=write_file
    )
    t1
