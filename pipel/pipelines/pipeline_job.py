
class Job():

    __scripts = []
    __name = None
    __type_job = "python"
    __type_available = ["python", "sh"]

    def __init__(self, name: str, scripts: list, type_job="python"):
        self.__name = name
        self.setScripts(scripts)
        self.setTypeJob(type_job)

    def setTypeJob(self, type_job):
        if type_job not in self.__type_available:
            raise ValueError("Value Type job {} in job name {} invalid, Type available {}".format(
                type_job, self.__name, self.__type_available))
        self.__type_job = type_job

    def setScripts(self, scripts):
        if scripts == [] or not scripts:
            raise ValueError("Script must have a value")

        if type(scripts) != list:
            scripts = [scripts]
        self.__scripts = scripts

    def getScripts(self):
        return self.__scripts

    def getType(self):
        return self.__type_job

    def getName(self):
        return self.__name


class Jobs():

    __jobs = {}

    __options_default = {
        "type": "python",
        "scripts": []
    }

    def __init__(self, jobs):
        for name_job, options in jobs.items():
            self.add(name_job, options)

    def add(self, name_job, options):
        if name_job in self.__jobs:
            raise ValueError("Name job {} duplicated ...".format(name_job))

        options = {**self.__options_default, **options}
        self.__jobs[name_job] = Job(
            name=name_job, scripts=options['scripts'], type_job=options['type'])

    def get(self, name_job):
        if name_job not in self.__jobs:
            raise ValueError("Name job {} not exists".format(name_job))
        return self.__jobs[name_job]

    def exists(self, name_job):
        if name_job in self.__jobs:
            return True
        return False

    def getList(self):
        return self.__jobs.values()
