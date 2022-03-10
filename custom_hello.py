from airflow.models.baseoperator import BaseOperator

class HelloOperator(BaseOperator):

    template_fields = ["name"]

    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        message = f"Hello {self.name}"
        print(message)
        return message
