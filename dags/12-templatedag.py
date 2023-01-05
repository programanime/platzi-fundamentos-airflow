from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

template_command = """
{% for filename in params.filenames %}
    echo "{{ task  }}"
    echo "{{ task_instance }}"
    echo "{{ dag }}"
    echo "{{ ds }}"
    echo "{{ filename }}"
{% endfor %}
"""

with DAG(
    dag_id="primer_template",
    description="primer template",
    schedule_interval="@once",
    start_date=datetime.now()
) as dag:
    t1 = BashOperator(
        task_id="hello_with_bash",
        bash_command=template_command,
        params={"filenames":["file1.txt", "file2.txt"]}
    )
    t1