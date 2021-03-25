import unittest
from unittest import mock
from pipel.pipelines.pipeline_validator import ValidateStepsJobPipeline, ValidateJobsPipeline
from pipel.pipelines.pipeline_run import RunStepsJobPipelines
import yaml

string_yaml_config = """
jobs:
  - name: "get_profile_github"
    script: "script:foo"

  - name: "save_to_csv"
    script: "script:foo"

  - name: "save_to_json"
    script: "script:foo"
  
  - name: "ingest_database"
    type: "command"
    script: "./script.sh --more "

run_step_jobs: 
  main:
    - job: "get_profile_github"
      sleep_time: 10000
      retry_error: 4
      break_error: true

    - job: "save_to_csv"
      upstream: 'get_profile_github'
      sleep_time: "100"

    - job: "save_to_json"
      upstream: 'get_profile_github'

    - job: "ingest_database"
      upstream: [ 'save_to_csv', 'save_to_json' ]
      must_done_all_upstream: false
"""

def load_config_yaml():
    try:
        return yaml.safe_load(string_yaml_config)
    except yaml.YAMLError as exc:
        raise ValueError("Yaml CONFIG: ",exc)

config_yaml = load_config_yaml()

class TestRunPipeline(unittest.TestCase):

    def test_register_jobs(self):
        jobs = config_yaml['jobs']
        pipe = RunStepsJobPipelines("test_pipeline_running")
        pipe.registerJobs(jobs)
        self.assertEqual(jobs,pipe.jobs)
    
    def test_register_steps_job(self):
        steps_job_main = config_yaml['run_step_jobs']['main']
        pipe = RunStepsJobPipelines("test_pipeline_running")
        pipe.registerStepJob(steps_job_main)
        self.assertEqual(steps_job_main,pipe.steps_job)


    def test_validate_steps_job(self):
        steps_job_main = config_yaml['run_step_jobs']['main']
        jobs = config_yaml['jobs']
        ValidateStepsJobPipeline("test_pipeline_running").validate(
            steps_job=steps_job_main,
            jobs=jobs
        )

    def test_validate_jobs(self):
        jobs = config_yaml['jobs']
        ValidateJobsPipeline("test_pipeline_running").validate(jobs=jobs)



