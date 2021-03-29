import sys
import os
from pipel.pipelines.pipeline import Pipelines

class CreatePipelines(Pipelines):

    def checkExistsPipelines(self):
        if self._checkDirPipelines() == True:
            raise ValueError(f"ETL Pipelines already exist with name {self.name}")

    def createDirPipelines(self):
        os.mkdir(self._pathPipelines())
    
    def createFilePipelines(self):
        extract_content = """

def extract_data():
    # Somting do here
    print(f"Hi, Iam Pipeline {opt['name']}")
    return "data from extract"
"""

        ingest_content = """

def ingest_data(data_extract):
    print("ingest data", data_extract)
"""

        with open(self._pathPipelines("extract.py"), 'w+') as f:
            f.write(extract_content)

        with open(self._pathPipelines("ingest.py"), 'w+') as f:
            f.write(ingest_content)

    def createConfigDefault(self):
        config = f"""
jobs:
  extract_data:
    scripts: extract:extract_data
  ingest_data:
    scripts: ingest:ingest_data
  print_success:
    type: sh
    scripts: 'echo "Process is success !!!"'

run_step_jobs:
  main:
    extract_data:
    ingest_data: 
      upstream: extract_data
    print_success: 
      upstream: ['extract_data', 'ingest_data']
        """
        with open(self._pathPipelines("config.yaml"), "w+") as f:
            f.write(config)

    def create(self):
        self.checkExistsPipelines()
        self.createDirPipelines()
        self.createFilePipelines()
        self.createConfigDefault()
