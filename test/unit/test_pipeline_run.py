import unittest
from unittest import mock
from pipel.pipelines.pipeline_run import RunPipelines
from pipel.pipelines.pipeline_job import Jobs
from pipel.pipelines.pipeline_step_job import StepRunnerJobManagerPipeline, StepJob
from pipel.services import run_pipeline
import yaml

string_yaml_config = """
jobs:
  get_profile_github:
    scripts: "script:foo"

  save_to_csv:
    scripts: "script:foo"

  save_to_json:
    scripts: "script:foo"
  
  ingest_database:
    type: "sh"
    scripts: "bash script.sh --more "

run_step_jobs: 
  main:
    get_profile_github:

    save_to_csv:
      upstream: 'get_profile_github'
      time_sleep: "100"

    save_to_csv:
      upstream: 'get_profile_github'

    ingest_database:
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

    def test_runner_step_job(self):
      jobs = Jobs({
          "check_ip_addr": {
              "type": "sh",
              "scripts": [
                  "ip addr"
              ],
          },
          "run_script_foo": {
            "scripts": "script:foo"
          },
          "run_script_bar_payload":{
            "scripts": "script:send_return_to_bar"
          },
          "run_script_bar":{
            "scripts": "script:bar"
          }
      })

      step = RunPipelines(jobs, StepRunnerJobManagerPipeline("test_pipeline_running"))
      step.setVerbose(False)
      step.addSteps([
          StepJob("check_ip_addr"),
          StepJob("run_script_foo", upstream=["check_ip_addr"]),
          StepJob("run_script_bar_payload"),
          StepJob("run_script_bar", upstream='run_script_bar_payload'),
      ])
      step.run()

    def test_running_yaml(self):

      jobs = Jobs(config_yaml['jobs'])
      step = RunPipelines(jobs, StepRunnerJobManagerPipeline("test_pipeline_running"))
      step.setVerbose(False)
      for name, option_step in config_yaml['run_step_jobs']['main'].items():
        step_job = StepJob(name)

        if option_step is None:
            option_step = {}

        if 'upstream' in option_step:
          step_job.setUpstream(option_step['upstream'])
        
        if 'break_error' in option_step:
          step_job.setBreakError(option_step['break_error'])

        if 'retry_error' in option_step:
          step_job.setRetryError(option_step['retry_error'])

        if 'time_sleep' in option_step:
          step_job.setTimeSleep(option_step['time_sleep'])

        if 'must_done_all_upstream' in option_step:
          step_job.setMustDoneAllUpstream(option_step['must_done_all_upstream'])

        step.addStep(step_job)

      step.run()




