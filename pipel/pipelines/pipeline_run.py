import yaml
import sys
import os
from pipel.pipelines.pipeline import Pipelines
from pipel.pipelines.pipeline_validator import ValidateJobsPipeline, ValidateStepsJobPipeline
from pipel.logs import Logs
import traceback

import contextlib
import io
import importlib


class JobsManager(Pipelines):

    def __init__(self, runner_id):
        pass

    def hasDone(self, task_name):
        pass

    def get(self, task_name):
        pass

    def setError(self, task_name, message=""):
        pass

    def setSuccess(self, task_name, message=""):
        pass


class RunJobPipeline(Pipelines):

    def __init__(self, runner_id, task_name, script, script_type, retry_error=False, break_error=True):
        """
            {name, runner_id, script, script_type, retry_error, break_error}
        """
        self.runner_id = runner_id
        self.task_name = task_name
        self.script = script
        self.script_type = script_type
        self.retry_error = retry_error
        self.break_error = break_error




class RunStepsJobPipelines(Pipelines):

    jobs = {}

    steps_job = {}

    def run(self, jobs, steps_job):
        Pipelines.setActivePipeline(self.name)
        Pipelines.generateRunnerId()

        self.registerJobs(jobs)
        self.registerStepJob(steps_job)

        self.validateJobs()

        self.registerJobManager()

    def validateJobs(self):
        jobs = ValidateJobsPipeline(self.name)
        jobs.validate(jobs=self.jobs)

        steps_job = ValidateStepsJobPipeline(self.name)
        steps_job.validate(jobs=self.jobs, steps_job=self.steps_job)

    def registerJobs(self, jobs):
        self.jobs = jobs

    def registerStepJob(self, steps_job):
        self.steps_job = steps_job

    def registerJobManager(self):
        self.job_manager = JobsManager(self.runner_id)
