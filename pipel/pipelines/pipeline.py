import os
import uuid

class Pipelines:

    task_dir = "task"

    active_pipeline = None

    runner_id = None

    name = None

    @classmethod
    def setActivePipeline(cls, name):
        cls.active_pipeline = name

    @classmethod
    def generateRunnerId(cls):
        cls.runner_id = uuid.uuid4().hex

    def __init__(self, name):
        Pipelines.setActivePipeline(name)
        Pipelines.generateRunnerId()
        self.name = name

    def _pathPipelines(self, filename = ''):
        return f"{self.task_dir}/{self.name}/{filename}"

    def _checkDirPipelines(self):
        return os.path.exists(self._pathPipelines())
    
    def _checkMainPipelines(self):
        return os.path.exists(self._pathPipelines('__init__.py'))
