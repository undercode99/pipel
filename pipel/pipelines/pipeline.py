import os
import uuid
from pipel.logs import Logs

class Pipelines:

    task_dir = "task"

    active_pipeline = None

    runner_id = None

    name = None

    verbose = False

    logs = Logs()

    @classmethod
    def setActivePipeline(cls, name):
        cls.active_pipeline = name

    @classmethod
    def generateRunnerId(cls):
        cls.runner_id = uuid.uuid4().hex

    def __init__(self, name):
        self.name = name

    def _pathPipelines(self, filename = ''):
        return f"{self.task_dir}/{self.name}/{filename}"

    def _pathAbsPipelines(self, filename):
        return os.path.abspath(self._pathPipelines(filename))

    def _checkDirPipelines(self):
        return os.path.exists(self._pathPipelines())
    
    def _checkMainPipelines(self):
        return os.path.exists(self._pathPipelines('__init__.py'))

    def setVerbose(self, verbose):
        self.verbose = verbose