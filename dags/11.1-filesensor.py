from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow import DAG
import os
import time
from airflow.sensors.filesystem import FileSensor

def write_file():
    time.sleep(10)
    f = open("/tmp/test.txt")
    f.write("some text")
    f.close()

def remove_file():
    os.remove("/tmp/test.txt")

with DAG(
    dag_id="11-filesensor-executor",
    description="filesensor",
    schedule_interval="*/5 * * * *",
    start_date=datetime.now(),
    end_date=datetime.now()  + timedelta(days = 10),
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")
    end = PythonOperator(
        task_id="filesesor_remove_with_python",
        python_callable=remove_file
    )
    filesensor = FileSensor(
        task_id="filesensor_sample",
        poke_interval=30,
        fs_conn_id="fs_default",
        filepath="/tmp/test.txt",
    )
    start >> filesensor >> end
