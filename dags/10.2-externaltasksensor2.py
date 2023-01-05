from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor


with DAG(dag_id="7.2-externalTaskSensor",
    description="DAG Secundario",
    schedule_interval="*/5 * * * *",
    start_date=datetime.now(),
    end_date=datetime.now()  + timedelta(days = 10),
    default_args={"depends_on_past":False}
) as dag:
    t1 = ExternalTaskSensor(task_id="waiting_dag",
							external_dag_id="7.1-externalTaskSensor",
							external_task_id="tarea_1_external",
							poke_interval=10 # Cada 10 segundos pregunta si ya termino la tarea
							)

    t2 = BashOperator(task_id="tarea_2_waiting",
					  bash_command="sleep 10 && echo 'DAG 2 finalizado!'",
					  depends_on_past=False)

    t1 >> t2