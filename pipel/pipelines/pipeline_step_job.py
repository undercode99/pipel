from pipel.pipelines.pipeline_job import Job
from pipel.pipelines.pipeline import Pipelines
from datetime import datetime
from pipel.logs import Logs
import os
import json

class StepJob():

    __break_error = True

    __upstream = []

    __retry_error = 0

    __job_name = None

    __job = None

    __time_sleep = 0

    __must_done_all_upstream = False


    def __init__(self, job_name, upstream=[], retry_error=0, break_error=True, time_sleep=0):
        self.__job_name = job_name
        self.setTimeSleep(time_sleep)
        self.setUpstream(upstream)
        self.setRetryError(retry_error)
        self.setBreakError(break_error)

    def setTimeSleep(self, time_sleep: int):
        self.__time_sleep = time_sleep

    def setUpstream(self, name_job):
        if type(name_job) == str:
            name_job = [name_job]
        self.__upstream = name_job

    def haveUpstream(self):
        if self.__upstream == []:
            return False
        return True

    def setRetryError(self, retry_error: int):
        self.__retry_error = retry_error

    def getRetryError(self):
        return self.__retry_error

    def getRangeRetryError(self):
        return range(1, self.__retry_error+1)

    def getBreakError(self):
        return self.__break_error

    def setBreakError(self, break_error: bool):
        self.__break_error = break_error

    def setJob(self, job):
        self.__job = job

    def getJobScripts(self):
        return self.__job.getScripts()

    def getJobType(self):
        return self.__job.getType()

    def getJobName(self):
        return self.__job_name

    def getJob(self):
        return self.__job

    def getTimeSleep(self):
        return self.__time_sleep

    def getUpstream(self):
        return self.__upstream

    def setMustDoneAllUpstream(self, must_done_all_upstream = True):
        self.__must_done_all_upstream = must_done_all_upstream

    def getMustDoneAllUpstream(self):
        return self.__must_done_all_upstream


class StepJobManagerPipeline():

    __runner_id = None

    __payload = None

    __logs_file_json = None

    __job_data = {
        "job_name": None,
        "type": None,
        "scripts": None,


        "start_time": None,
        "finish_time": None,
        "elapsed_running_time": None,

        "message_error": [],
        "has_error": False,
        "has_break": False,
        "has_finish": False,
        "total_times_retry": 0,
    }

    def setRunnerId(self, runner_id):
        self.__runner_id = runner_id


    def __init__(self, step_job: StepJob):
        self.__job_data['job_name'] = step_job.getJobName()
        self.__job_data['type'] = step_job.getJobType()
        self.__job_data['scripts'] = step_job.getJobScripts()
        self.__job_data['start_time'] = datetime.now()

    def jobHasFinish(self):
        return self.__job_data['has_finish']

    def __saveData(self):
        job_data = self.__job_data.copy()
        job_data['start_time'] = str(job_data['start_time'])
        job_data['finish_time'] = str(job_data['finish_time'])
        job_data['elapsed_running_time'] = str(job_data['elapsed_running_time'])
        with open(self.__logs_file_json, "w+") as o:
            o.write(json.dumps(job_data))

    def getEplasedRunningTime(self):
        return self.__job_data['elapsed_running_time']

    def setFinishJob(self):
        self.__job_data['finish_time'] = datetime.now()
        self.__job_data['elapsed_running_time'] = self.__job_data['finish_time'] - self.__job_data['start_time']
        self.__job_data['has_finish'] = True
        self.__saveData()

    def setTotalTimesRetry(self, times_retry):
        self.__job_data['total_times_retry'] = times_retry
    
    def setHasBreakProccess(self):
        self.__job_data['has_break'] = True

    def setErrorJob(self, messages):
        if type(messages) != list:
            messages = [messages]
        for message in messages:
            self.__job_data['message_error'].append(message)

        self.__job_data['has_error'] = True


    def setPathLogs(self, path='.'):
        path_logs_json = "{}/json_logs".format(path)
        if not os.path.exists(path_logs_json):
            os.makedirs(path_logs_json)

        self.__logs_file_json = "{}/{}_{}.json".format(path_logs_json, self.__runner_id, self.__job_data['job_name'])


    def saveDataPayload(self, payload):
        self.__payload = payload
    
    def getPayload(self):
        return self.__payload



class StepRunnerJobManagerPipeline(Pipelines):

    __list_job = {}

    __runner_id = None

    def startJob(self, step_job: StepJob):
        self.__list_job[step_job.getJobName()] = StepJobManagerPipeline(step_job)
        self.__list_job[step_job.getJobName()].setRunnerId(self.runner_id)
        return self.__list_job[step_job.getJobName()]

    def getRunnerId(self):
        self.__runner_id = self.runner_id
        return self.__runner_id

    def getJobPayload(self, job_name):
        if job_name not in self.__list_job:
            return None
        return self.__list_job[job_name].getPayload()

    def allJobUpstreamHasFinish(self, job_names):
        has_finish = False
        for job_name in job_names:
            if job_name not in self.__list_job:
                continue
            if self.__list_job[job_name].jobHasFinish():
                has_finish = True
            else:
                has_finish = False
        return has_finish
            