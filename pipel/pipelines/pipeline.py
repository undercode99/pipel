import os
import uuid
from pipel.logs import Logs
from datetime import datetime
from random import randint


class Pipelines:

    task_dir = "task"

    logs_dir = ".logs"

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
        cls.runner_id = "{}_{}".format(str(datetime.now()), randint(100, 999))
        cls.runner_id = cls.runner_id.replace(" ", "_")

    def __init__(self, name):
        self.name = name

    def _pathPipelines(self, filename=''):
        return f"{self.task_dir}/{self.name}/{filename}"

    def _pathAbsPipelines(self, filename):
        return os.path.abspath(self._pathPipelines(filename))

    def _pathLogsDirPipelines(self, filename=''):
        directory = "{}/{}".format(self.logs_dir, self.name)
        if not os.path.exists(directory):
            os.makedirs(directory)
        if not filename:
            return directory
        return "{}/{}".format(directory, filename)

    def _checkDirPipelines(self):
        return os.path.exists(self._pathPipelines())

    def _checkMainPipelines(self):
        return os.path.exists(self._pathPipelines('__init__.py'))

    def setVerbose(self, verbose):
        self.verbose = verbose

    def getNamePipeline(self):
        return self.name

