from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context

class HelloOperator(BaseOperator):
    def __init__(self, name: str, **kwargs):
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context: Context):
        print(f"hey there {self.name}")
