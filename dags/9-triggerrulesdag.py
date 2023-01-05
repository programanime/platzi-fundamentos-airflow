from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule

def task2():
    print("hola gente de platzi")

def task1():
    raise Exception("there is an error")

with DAG(
    dag_id="primer_trigger_rules",
    description="primer_trigger_rules",
    start_date=datetime.now(),
    schedule_interval="@once",
    max_active_runs=1
) as dag:
    t1 = PythonOperator(
        task_id="primer_task1",
        python_callable=task1
    )

    t2 = PythonOperator(
        task_id="primer_task2",
        python_callable=task2,
        trigger_rule=TriggerRule.ALL_FAILED,
        retries=7,
        retry_delay=5,
        depends_on_past=False
    )

    t1 >> t2