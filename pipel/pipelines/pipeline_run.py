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

from pipel.logs import Logs
import daemon
from daemon import pidfile

from datetime import datetime


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

    __logs = Logs()

    def __init__(self, jobs: Jobs, job_step_runner_manager: StepRunnerJobManagerPipeline):
        self.__jobs = jobs
        self.__job_step_runner_manager = job_step_runner_manager

        if not self.__job_step_runner_manager.getRunnerId():
            self.__job_step_runner_manager.generateRunnerId()
            
        
    
    def getJobManager(self):
        return self.__job_step_runner_manager

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

    def run(self, is_deamon=False):

        if not is_deamon:
            self.__logs.success("Running {} ID Timestapms: {}".format(self.__job_step_runner_manager.name,self.__job_step_runner_manager.runner_id))
            self.startingRunJob()
            return
        
        step_manager = self.getJobManager()

        path_pid = step_manager._pathLogsDirPipelines("{}.pid".format(step_manager.name))
        if os.path.exists(path_pid):
            pid = open(path_pid).read()
            pid = pid.replace("\n","")
            return self.__logs.error("Pipeline{} already running with pid {}".format(step_manager.name, pid))

        self.__logs.success("Running {} in background ID Timestapms: {}".format(self.__job_step_runner_manager.name,self.__job_step_runner_manager.runner_id))

        path_logs = step_manager._pathLogsDirPipelines()
        path_logs = "{}/{}".format(path_logs, "output_files")
        if not os.path.exists(path_logs):
            os.makedirs(path_logs)
        path_logs = "{}/{}.log".format(path_logs, step_manager.getRunnerId())
        output_file = open(path_logs, 'w+')

        

        with daemon.DaemonContext(
                stdout=output_file,
                stderr=output_file,
                working_directory=os.getcwd(),
                umask=0o002,
                pidfile=daemon.pidfile.TimeoutPIDLockFile(path_pid),
        ):
            self.startingRunJob()

    def startingRunJob(self):
        start = datetime.now()
        for step in self.__steps_jobs:
            if step.haveUpstream():
                continue
            hasCompleted = self.runningScript(step)
            if hasCompleted:
                self.runningPararel(step.getJobName())
        delta_time = datetime.now() - start
        self.__logs.success("Process pipeline {} has done, elapsed running time {}".format(self.getJobManager().getNamePipeline(), delta_time))



    def runningPararel(self, job_name):
        self.runningUpstream(job_name)

    def runningScript(self, step: StepJob, from_job_name=None):
        logs = Logs()

        # Job starting
        job_manager = self.__job_step_runner_manager.startJob(step)
        job_manager.setPathLogs(self.__job_step_runner_manager._pathLogsDirPipelines())

        def output(message):
            if self.__verbose:
                logs.default("{} => {} ".format(step.getJobName(), message))

        def outputError(message):
            message = "{} => {} ".format(step.getJobName(), message)
            job_manager.setErrorJob(message)
            logs.error(message)

        logs.success("{} => Running ..".format(step.getJobName()))
        logs.info("{} => Time Sleep {} Seconds".format(step.getJobName(), step.getTimeSleep()))


        time.sleep(step.getTimeSleep())

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
            message_log = "{} => Done".format(step.getJobName())
            job_manager.saveDataPayload(payload)
            job_manager.setFinishJob()

            message_log = "{} ...{}s".format(message_log,job_manager.getEplasedRunningTime().seconds)
            logs.success(message_log)
            return True
        
        message_log = "{} => Failed!".format(step.getJobName())
        job_manager.setErrorJob(message_log)
        logs.error(message_log)

        if step.getRetryError() == 0:
            job_manager.setFinishJob()
            return False

        is_error = True
        for number_step in step.getRangeRetryError():
            message_log = "{} => Retry running job times: {}".format(step.getJobName(),number_step)
            logs.info(message_log)
            job_manager.setTotalTimesRetry(number_step)
            run_script.run()
            is_error = run_script.isError()

        if step.getBreakError() and is_error:
            message_log = "{} => Running retry failed process stopped!".format(step.getJobName())
            job_manager.setErrorJob(message_log)
            job_manager.setHasBreakProccess()
            job_manager.setFinishJob()
            message_log = "{} ...{}s".format(message_log,job_manager.getEplasedRunningTime().seconds)
            logs.error(message_log)
            return False

        job_manager.saveDataPayload(payload)
        if is_error:
            message_log = "{} => Failed process continued".format(step.getJobName())
            job_manager.setErrorJob(message_log)
            job_manager.setFinishJob()
            message_log = "{} ...{}s".format(message_log,job_manager.getEplasedRunningTime().seconds)
            logs.error(message_log)
            return True

        
        job_manager.setFinishJob()
        logs.success("{} => Done {}s".format(step.getJobName(), job_manager.getEplasedRunningTime().seconds))
        
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
