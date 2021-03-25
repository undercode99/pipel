
from pipel.pipelines.pipeline import Pipelines
from collections import Counter


import importlib
import os

class ValidateJobsPipeline(Pipelines):

    def validate(self, jobs):
        self.jobs = jobs
        for job in self.jobs:
            self.requiredKey(job)
            self.typeScriptValidate(job)
            self.scriptValidatePython(job)

        self.checkDuplicateName()




    def checkDuplicateName(self):
        jobs = [ job['name'] for job in self.jobs]
        duplicated = Counter(jobs) - Counter(set(jobs))
        duplicated = list(duplicated.keys())

        if len(duplicated) > 0:
            raise ValueError("Job name {} duplicated, job name must unique".format(duplicated))


    def requiredKey(self, job):
        key_jobs = list(job)
        for required_key in ['name', 'script']:
            if required_key not in key_jobs:
                raise ValueError("Job {} must have key '{}' ".format(job,required_key))


    def scriptValidatePython(self, job):
        if 'type' in job and job['type'] != 'python':
            return True

        module_split = job['script'].split(":")
        if len(module_split) != 2:
            raise ValueError(
                "value script {} invalid not have function to excute, Example script job file_script:name_function".format(job['script']))

        module_path = module_split[0].split(".")
        module_name = module_path[-1]

        full_path_to_module = self._pathAbsPipelines(
            "{path}.py".format(path="/".join(module_path)))

        spec = importlib.util.spec_from_file_location(module_name, full_path_to_module)
        module = spec.loader.load_module()
        result = getattr(module, module_split[-1])

        if not callable(result):
            raise ValueError("Script {} not callable".format(module_name))
            

    def typeScriptValidate(self, job):
        if 'type' not in job:
            return True
        type_allowed = ['command', 'python']
        if job['type'] not in type_allowed:
            raise ValueError("value type '{}' error, value only allowed value {}".format(
                job['type'], ", ".join(type_allowed)))



class ValidateStepsJobPipeline(Pipelines):

    steps_job = {}

    jobs = {}

    def validate(self, jobs, steps_job):
        self.steps_job = steps_job
        self.jobs = [job['name'] for job in jobs]

        for step in self.steps_job:
            self.requiredJobKey(step)
            self.stepJobsExistsInJobs(step)
            self.upstreamJobExistInJob(step)
            self.sleepTimeCheck(step)
            self.checkMustDoneAllCheck(step)
            self.checkBreakError(step)
            self.checkRetryError(step)


    def checkBreakError(self, step):
        if 'break_error' not in step:
            return
        
        if not isinstance(step['break_error'],bool):
            raise ValueError("Job name {} error value 'break_error: {}' must type bool not type {}".format(
        step['job'],
        step['break_error'],
        type(step['break_error'])
        ))

    
    def checkRetryError(self, step):
        if 'retry_error' not in step:
            return
        
        if not isinstance(step['retry_error'],int):
            raise ValueError("Job name {} error value 'retry_error: {}' must type int not type {}".format(
        step['job'],
        step['retry_error'],
        type(step['retry_error'])
        ))

    def sleepTimeCheck(self, step):
        if 'sleep_time' not in step:
            return True

        if isinstance(step['sleep_time'], (int, float)):
            return True

        if not step['sleep_time'].isnumeric():
            raise ValueError("Value sleep_time not a numeric {}, value must a numeric".format(
                step['sleep_time']))


    def requiredJobKey(self, step):
        if "job" not in step:
            raise ValueError("Config run_step_jobs must have key 'job' ")


    def stepJobsExistsInJobs(self, step):
        if self.jobIsExists(job_name=step['job']) == False:
            raise ValueError(
                "Step job name {} not exists in data jobs".format(step['job']))


    def upstreamJobExistInJob(self, step):
        if 'upstream' not in step:
            return

        upstream = step['upstream']
        if type(upstream) == str:
            upstream = [upstream]

        for up in upstream:
            if not self.jobIsExists(up):
                raise ValueError(
                    "Upstream name {} not exists in data jobs".format(up))


    def checkMustDoneAllCheck(self, step):
        if 'must_done_all_upstream' not in step:
            return True
        must_done_all_upstream = step['must_done_all_upstream']
        if not isinstance(must_done_all_upstream, bool):
            raise ValueError(
                "Step Job value 'must_done_all_upstream' {} Error , value must type data bool true/false".format(must_done_all_upstream))


    def jobIsExists(self, job_name):
        return (job_name in self.jobs)