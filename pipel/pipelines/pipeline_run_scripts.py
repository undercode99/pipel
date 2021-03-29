import os
import psutil
import subprocess
import threading
import importlib

import contextlib
import io
import traceback
import sys
from pipel.logs import Logs


class RunScripts():

    __cwd = "."

    __scripts = []
    __is_error = False
    __exit_code = 0
    __outputs = None
    __func_callback = None
    __func_callback_err = None
    __verbose = True
    __option = {}

    __logs = Logs()

    def __init__(self, scripts):
        self.__scripts = scripts

    def setCallbackOutput(self, func):
        self.__func_callback = func

    def setCallbackOutputError(self, func):
        self.__func_callback_err = func

    def printOutput(self, data_output):
        if self.__func_callback == None:
            if self.isVerbose():
                self.__logs.default(data_output)
        else:
            self.__func_callback(data_output)

    def printOutputErr(self, data_output_err):
        if self.__func_callback_err == None:
            self.__logs.error(data_output_err)
        else:
            self.__func_callback_err(data_output_err)

    def isError(self):
        return self.__is_error

    def getOutputs(self):
        return self.__outputs

    def getExitCode(self):
        return self.__exit_code

    def run(self):
        self.printOutput("Running ...")

    def setCwd(self, cwd):
        self.__cwd = cwd

    def getPathCwd(self, path = ''):
        return "{}/{}".format(self.__cwd, path)

    def setError(self, is_error):
        self.__is_error = is_error
    
    def setMessageError(self, msg):
        self.setError(True)
        self.printOutputErr(msg)

    def isVerbose(self):
        return self.__verbose
    
    def setVerbose(self, verbose):
        self.__verbose = verbose


    def setOption(self, from_job_name, step):
        self.__option = {
            "from_job_name": from_job_name,
            "step": step
        }

    def getOption(self):
        return self.__option
        
    def getScript(self):
        return self.__scripts

    def setExitCode(self, exit_code):
        self.__exit_code = exit_code

    def setOutputs(self, output):
        self.__outputs = output


class RunBashScripts(RunScripts):

    __bash_save_output = []

    def run(self):
        self.runBashLinux()
        return self.getOutputs()

    def saveOutput(self, proccess,  stdtype):
        for line in proccess:
            self.__bash_save_output.append(line)
            if stdtype == 'OUT':
                self.printOutput(line.decode('utf-8').strip())
            else:
                self.printOutputErr(line.decode('utf-8').strip())

    def runBashLinux(self):
        outputs = []
        for script in self.getScript():
            self.__bash_save_output = []
            with subprocess.Popen(script, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd=self.getPathCwd()) as proccess:
                t1 = threading.Thread(target=self.saveOutput,
                                    args=(proccess.stdout, "OUT"))
                t2 = threading.Thread(target=self.saveOutput,
                                    args=(proccess.stderr, "ERR"))
                worker_list = [t1, t2]

                for worker in worker_list:
                    worker.start()
                for worker in worker_list:
                    worker.join()
                exit_code = proccess.wait()
                outputs.append(self.__bash_save_output)
                if exit_code != 0:
                    break

        self.setError((exit_code != 0))

        self.setOutputs(self.__bash_save_output)
        self.setExitCode(exit_code)



class RunPythonScripts(RunScripts): 

    __payload = None
    

    def execFunction(self, func, pipline_return,  opt = {}):
        if func.__code__.co_argcount > 1:
            return func(pipline_return, opt)
        elif func.__code__.co_argcount > 0:
            return func(pipline_return)
        else:
            return func()

    def runFunction(self, script, payload, option):

        module_split = script.split(":")
        if len(module_split) != 2:
            return self.setMessageError(
                "Value script {} invalid not have function to excute, Example script job file_script:name_function".format(script)
            )

        module_path = module_split[0].split(".")
        module_name = module_path[-1]

        full_path_to_module = self.getPathCwd(
            "{path}.py".format(path="/".join(module_path)))

        try:
            spec = importlib.util.spec_from_file_location(module_name, full_path_to_module)
            module = spec.loader.load_module()
            result = getattr(module, module_split[-1])

            if not callable(result):
                return self.setMessageError("Script {} not callable".format(module_name))

        except Exception as e:
            return self.setMessageError("Error {}".format(str(e)))

        try:
            if self.isVerbose():
                self.__payload = self.execFunction(result, payload, option)
            else:
                with contextlib.redirect_stdout(io.StringIO()) as o_io:
                    self.__payload = self.execFunction(result, payload, option)
                    print(o_io)
        except Exception as e:
            if self.isVerbose():
                self.setMessageError("Error: {}".format(traceback.format_exc()))
            else:
                self.setMessageError("Error: {}".format(str(e)))


    def run(self):
        for script in self.getScript():
            self.runFunction(
                script=script,
                payload=self.__payload,
                option=self.getOption()
            )
        return self.__payload

    def setParameterPayload(self, payload):
        self.__payload = payload    

    
    
