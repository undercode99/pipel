import multiprocessing
import time
from multiprocessing import Pool
import os
import psutil
import subprocess
import multiprocessing.pool

from pipel.pipelines.pipeline_run_scripts import RunBashScripts, RunPythonScripts
from pipel.pipelines.pipeline_job import Jobs
from pipel.pipelines.pipeline_step_job import StepJob, StepRunnerJobManagerPipeline


class NoDaemonProcess(multiprocessing.Process):
    @property
    def daemon(self):
        return False

    @daemon.setter
    def daemon(self, value):
        pass


class NoDaemonContext(type(multiprocessing.get_context())):
    Process = NoDaemonProcess

# We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# because the latter is only a wrapper function, not a proper class.


class NestablePool(multiprocessing.pool.Pool):
    def __init__(self, *args, **kwargs):
        kwargs['context'] = NoDaemonContext()
        super(NestablePool, self).__init__(*args, **kwargs)


class RunPipelines():

    __steps_jobs = []
    __jobs = None
    __job_step_runner_manager = None
    __verbose = True

    def __init__(self, jobs: Jobs, job_step_runner_manager: StepRunnerJobManagerPipeline):
        self.__jobs = jobs
        self.__job_step_runner_manager = job_step_runner_manager

        if not self.__job_step_runner_manager.getRunnerId():
            self.__job_step_runner_manager.generateRunnerId()
            
        print("Start task pipeline", self.__job_step_runner_manager.name,
              " with id ", self.__job_step_runner_manager.runner_id)

    def addSteps(self, steps_jobs: list):
        for step_job in steps_jobs:
            self.addStep(step_job)

    def addStep(self, step_job: StepJob):
        job_name = step_job.getJobName()

        for upstream in step_job.getUpstream():
            if upstream == job_name:
                raise ValueError(
                    "Name upstream {} not name with step job name".format(upstream))
            if not self.__jobs.exists(upstream):
                raise ValueError(
                    "Upstream '{}' not exists in name jobs".format(upstream))

        step_job.setJob(self.__jobs.get(job_name))
        self.__steps_jobs.append(step_job)


    def setVerbose(self, verbose):
        self.__verbose = verbose

    def run(self):
        for step in self.__steps_jobs:
            if step.haveUpstream():
                continue
            hasCompleted = self.runningScript(step)
            if hasCompleted:
                self.runningPararel(step.getJobName())

    def runningPararel(self, job_name):
        self.runningUpstream(job_name)

    def runningScript(self, step: StepJob, from_job_name=None):

        # Job starting
        job_manager = self.__job_step_runner_manager.startJob(step)

        time.sleep(step.getTimeSleep())

        def output(x):
            ou = ''.join(x)
            print(step.getJobName(), "=> ", ou)

        def outputError(x):
            output(x)

        process = psutil.Process(os.getpid())
        mb_proccess = (process.memory_info().rss/1000)/1000
        output("Running .. {} Mb,  PID: {}".format(
            round(mb_proccess), os.getpid()))

        

        if step.getJobType() == 'sh':
            run_script = RunBashScripts(step.getJobScripts())
        else:
            run_script = RunPythonScripts(step.getJobScripts())
            run_script.setParameterPayload(self.__job_step_runner_manager.getJobPayload(from_job_name))

        run_script.setCallbackOutput(func=output)
        run_script.setCallbackOutputError(func=outputError)
        run_script.setCwd(cwd=self.__job_step_runner_manager._pathAbsPipelines(''))
        run_script.setOption(from_job_name=from_job_name, step=step)
        run_script.setVerbose(self.__verbose)
        payload = run_script.run()

        if not run_script.isError():
            output("Done exit code : {}".format(
                run_script.getExitCode()))
            job_manager.saveDataPayload(payload)
            return True
        
        outputError("Failed exit code : {}".format(
            run_script.getExitCode()))

        if step.getRetryError() == 0:
            return False

        is_error = True
        for number_step in step.getRangeRetryError():
            output("Retry running job Times: {}".format(number_step))
            run_script.run()
            is_error = run_script.isError()

        if step.getBreakError() and is_error:
            outputError("Running Retry Failed exit code : {}".format(
                run_script.getExitCode()))
            return False

        job_manager.saveDataPayload(payload)
        output("Done exit code : {}".format(run_script.getExitCode()))
        return True

    def runningScriptMultiProccess(self, step, from_job_name):
        hasCompleted = self.runningScript(step, from_job_name)
        if hasCompleted:
            self.runningUpstream(step.getJobName())

    def runningUpstream(self, job_name):
        args = []
        for step in self.__steps_jobs:
            if job_name in step.getUpstream():
                args.append((step, job_name))

        proc = round((multiprocessing.cpu_count() * 50)/100)
        with NestablePool(processes=proc) as pool:
            pool.starmap(self.runningScriptMultiProccess, args)
            pool.close()
            pool.join()
