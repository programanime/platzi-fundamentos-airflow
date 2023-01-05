from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


with DAG(dag_id="7.1-externalTaskSensor",
    description="DAG principal",
    schedule_interval="*/5 * * * *",
    start_date=datetime.now(),
    end_date=datetime.now()  + timedelta(days = 10)
) as dag:

    t1 = BashOperator(task_id="tarea_1_external",
                      bash_command="sleep 10 && echo 'DAG finalizado!'",
                      depends_on_past=False)

    t1 