from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, timedelta

default_args = {
'start_date':datetime.now() - timedelta(days = 100),
'end_date': datetime.now()  + timedelta(days = 100)
}

def _choose(**context):
    if context["logical_date"].date() < datetime.now():
        return "finish_22_june"
    return "start_23_june"

with DAG(dag_id="10-branching",
    schedule_interval="@daily",
	default_args=default_args,
    start_date=datetime.now() - timedelta(days = 100),
    end_date=datetime.now()  + timedelta(days = 100),
    catchup=True
) as dag:

    branching = BranchPythonOperator(task_id="branch",
	                                 python_callable=_choose)

    finish_22 = BashOperator(task_id="finish_22_june",
	                         bash_command="echo 'Running {{ds}}'")

    start_23 = BashOperator(task_id="start_23_june",
	                        bash_command="echo 'Running {{ds}}'")

    branching >> [finish_22, start_23]
