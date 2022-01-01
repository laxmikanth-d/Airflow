from airflow.models.baseoperator import BaseOperator
import os
import json
import csv

class JsonToCSVOperator(BaseOperator):

    def __init__(self, input_path: str, output_path: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.input_path = input_path
        self.output_path = output_path


    def execute(self, context):
        
        for file in os.listdir(self.input_path):
            if file.endswith('.json'):
                file_path = os.path.join(self.input_path, file)
                with open(file_path) as json_path:
                    data = json.load(json_path)

                csv_filename = os.path.join(self.output_path, f"{file.split('.')[0]}.csv")

                csv_file = open(csv_filename, 'w')
                writer = csv.writer(csv_file)

                count = 0

                for d in data:
                    if count == 0:
                        header = d.keys()
                        writer.writerow(header)
                        count += 1
                    writer.writerow(d.values())
                
                csv_file.close()