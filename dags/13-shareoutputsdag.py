from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.xcom import XCom
from pprint import pprint
default_args = {"depends_on_past": True}

def myfunction(**context):
    result = int(context["ti"].xcom_pull(task_ids='xcom_tarea_2')) - 15
    print(result)
    return result

def myfunction2(**context):
    pprint(context)
    result = int(context["ti"].xcom_pull(task_ids='xcom_tarea_3')) - 15
    print(result)
    return result

with DAG(dag_id="9-XCom",
    description="Probando los XCom",
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 1),
	default_args=default_args,
    max_active_runs=1
) as dag:
    t1 = BashOperator(task_id="xcom_tarea_1",
					  bash_command="sleep 5 && echo $((3 * 15))")

    t2 = BashOperator(task_id="xcom_tarea_2",
					  bash_command="sleep 3 && echo {{ ti.xcom_pull(task_ids='xcom_tarea_1') }}")

    t3 = PythonOperator(task_id="xcom_tarea_3", 
                        python_callable=myfunction)
    
    t4 = PythonOperator(task_id="tarea_3", 
                        python_callable=myfunction2)

    t1 >> t2 >> t3 >> t4