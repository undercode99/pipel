from pipel.pipelines.pipeline_run import RunPipelines
from pipel.pipelines.pipeline_step_job import StepRunnerJobManagerPipeline, StepJob
from pipel.pipelines.pipeline_job import Jobs
from pipel.pipelines.pipeline_config import ConfigYamlPipeline
from pipel.pipelines.pipeline_create import CreatePipelines

from pipel.new import new_project
from pipel.config_loader import load_config
from pipel.reporting import create_report_pipeline

from pipel.storages.storage_config import StorageConfig
from pipel.storages.storage import Storage
from pipel.storages.storage_function import StorageFunction
from pipel.pipelines.pipeline import Pipelines



""" Pipeline services """
def run_pipeline(name, step='main', verbose=False):
    load_config()
    config = ConfigYamlPipeline(name)
    config.load()

    jobs = config.jobs()
    jobs = Jobs(jobs=jobs)
    steps_job = config.steps_job(step=step)
    
    
    step = RunPipelines(jobs, StepRunnerJobManagerPipeline(name))
    step.setVerbose(verbose)

    for name, option_step in steps_job.items():
        step_job = StepJob(name)

        if option_step is None:
            option_step = {}

        if 'upstream' in option_step:
          step_job.setUpstream(option_step['upstream'])
        
        if 'break_error' in option_step:
          step_job.setBreakError(option_step['break_error'])

        if 'retry_error' in option_step:
          step_job.setRetryError(option_step['retry_error'])

        if 'sleep_time' in option_step:
          step_job.setTimeSleep(option_step['sleep_time'])

        if 'must_done_all_upstream' in option_step:
          step_job.setMustDoneAllUpstream(option_step['must_done_all_upstream'])

        step.addStep(step_job)
    step.run()



def create_pipeline(name):
    load_config()
    createpipe = CreatePipelines(name)
    createpipe.create()

def create_new_project(name):
    new_project(name)

""" Storage services """
def set_storage(name):
    StorageConfig.use(name)

def get_config_storage():
    return StorageConfig.get()
    
def use_storage():
    options = get_config_storage()['option']
    pipeline = {
        'name_pipeline': Pipelines.active_pipeline,
        'runner_id': Pipelines.runner_id
    }
    storage = Storage()
    storage.setup({**options, **pipeline})
    return storage

def info_storage_pipeline(name):
    load_config()
    stf = StorageFunction()
    for info_storage in stf.checkInfoLogsData(name):
        print(info_storage)


def remove_storage_pipeline(name):
    load_config()
    stf = StorageFunction()
    stf.removeStorageByName(name)
