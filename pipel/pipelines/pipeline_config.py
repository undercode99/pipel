
import yaml
from pipel.pipelines.pipeline import Pipelines

class ConfigYamlPipeline(Pipelines):

    config = {}

    def load(self):
        config_file = self._pathPipelines("config.yaml")
        with open(config_file, 'r') as stream:
            try:
                self.config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                raise ValueError("Yaml CONFIG: ", exc)

    def jobs(self):
        if 'jobs' not in self.config:
            raise ValueError("Config jobs in file config.yaml not found")
        return self.config['jobs']

    def steps_job(self, step='main'):
        if 'run_step_jobs' not in self.config:
            raise ValueError("Config run_step_jobs in file config.yaml not found")

        if step not in self.config['run_step_jobs']:
            raise ValueError("Key {} in run_step_jobs not found".format(step))

        return self.config['run_step_jobs'][step]
