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
        main_content = """
from extract import extract_data

pipelines = {
    "extract": extract_data
}
""" 
        extract_content = """

def extract_data(opt):
    # Somting do here
    print(f"Hi, Iam Pipeline {opt['name']}")
"""
        with open(self._pathPipelines("__init__.py"), 'w+') as f:
            f.write(main_content)

        with open(self._pathPipelines("extract.py"), 'w+') as f:
            f.write(extract_content)
        
    def create(self):
        self.checkExistsPipelines()
        self.createDirPipelines()
        self.createFilePipelines()
