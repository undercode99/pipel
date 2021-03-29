from pipel.pipelines.pipeline_job import Job
from pipel.pipelines.pipeline import Pipelines

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

    __job_name = None

    __job = None

    __payload = None

    __job_data = {
        "job_name": None,
        "type": None,


        "start_time": None,
        "finish_time": None,
        "running_time": None,

        "alloutput": None,
        "output": None,
        "error_output": None,
        "return_data": None,  # this data just on memory not saved data
        "has_error": False,
        "has_break": False,
        "times_retry": None,
    }

    def __init__(self, job: Job):
        pass

    def jobHasFinish(self):
        pass

    def allUpstreamHasFinish(self):
        pass

    def finishJob(self):
        pass

    def errorJob(self):
        pass

    def saveDataPayload(self, payload):
        self.__payload = payload
    
    def getPayload(self):
        return self.__payload


class StepRunnerJobManagerPipeline(Pipelines):

    __list_job = {}

    __runner_id = None

    def startJob(self, job: StepJob):
        self.__list_job[job.getJobName()] = StepJobManagerPipeline(job)
        return self.__list_job[job.getJobName()]

    def getRunnerId(self):
        self.__runner_id = self.runner_id
        return self.__runner_id

    def getJobPayload(self, job_name):
        if job_name not in self.__list_job:
            return None
        return self.__list_job[job_name].getPayload()
