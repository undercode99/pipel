from pipel.pipelines.pipeline_run import RunPipelines
from pipel.pipelines.pipeline_create import CreatePipelines
from pipel.new import new_project
from pipel.config_loader import load_config
from pipel.reporting import create_report_pipeline

from pipel.storages.storage_config import StorageConfig
from pipel.storages.storage import Storage
from pipel.storages.storage_function import StorageFunction
from pipel.pipelines.pipeline import Pipelines



""" Pipeline services """
def run_pipeline(name, verbose=False):
    load_config()
    runpipe = RunPipelines(name)
    runpipe.setVerbose(verbose)
    runpipe.run()
    create_report_pipeline(name, runpipe.runner_id,runpipe.logs_pipeline)

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
