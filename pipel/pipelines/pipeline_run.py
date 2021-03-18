import sys
import os
from pipel.pipelines.pipeline import Pipelines
from pipel.logs import Logs
import traceback

import contextlib
import io

class RunPipelines(Pipelines):

    pipelines = {}

    pipline_return = None

    verbose = False

    logs_pipeline = []

    logs = Logs()

    def setVerbose(self, verbose):
        self.verbose = verbose

    def execFunction(self, func, opt):
        if func.__code__.co_argcount > 0:
            return func(opt)
        else:
            return func()

    def runFunction(self, func, opt):
        if self.verbose == True:
            self.pipline_return = self.execFunction(func, opt)
        else:
            with contextlib.redirect_stdout(io.StringIO()):
                self.pipline_return = self.execFunction(func, opt)

    def getOptionsPipeline(self,name,pipeline):
        option = {
            "func": None,
            "break_error": True,
            "retry_error": 0
        }

        if callable(pipeline):
            option['func'] = pipeline
            return option
        
        if "func" not in pipeline or type(pipeline) != dict:
            raise Exception(f"Pipeline {name} no have callable function")

        if not callable(pipeline['func']):
            raise Exception(f"Pipeline {name} key func not callable")
        
        
        return  {**option, **pipeline}


    """ Retry error """
    def retryErrorRunFunction(self, func, opt, range_try, number_pipeline):
        success_retry = False
        for total_try in range(0, range_try):
            self.logs.info(f"Running Re-try {total_try+1} {self.name}:{number_pipeline}_{opt['name']} ... ")
            try:
                self.runFunction(func, opt)
                success_retry = True
                break
            except Exception as e:
                error_message = f"Error Re-try {total_try+1}  {self.name}:{number_pipeline}_{opt['name']} With Message: {e}"
                trace = traceback.format_exc()
                self.errorDisplay(error_message, trace)

        return success_retry



    """ Exec function pipline """
    def execPipelines(self):
        parent_pipelines = None
        number_pipeline = 0 

        for key, function in self.pipelines.items():
            number_pipeline += 1
            opt = {
                "name": key,
                "parent": parent_pipelines,
                "return_from_parent": self.pipline_return
            }
            self.logs.info(f"Running {self.name}:{number_pipeline}_{key} ... ")

            pipeline = self.getOptionsPipeline(key, function)

            try:
                # run function 
                self.runFunction(pipeline['func'], opt)

                # Save logs pipeline
                self.logs_pipeline.append({
                    "number_pipeline": number_pipeline,
                    'runner_id': self.runner_id,
                    'name': self.name,
                    'name_pipeline': key,
                    'parent_pipeline': parent_pipelines,
                    'error': False
                })

            except Exception as e:
                trace = traceback.format_exc()
                error_message = f"Error {self.name}:{number_pipeline}_{key} With Message: {e}"
                self.errorDisplay(error_message, trace)

                # Retry function runtime
                success_retry = self.retryErrorRunFunction(pipeline['func'], opt, pipeline['retry_error'], number_pipeline)
                
                # Save logs pipline
                self.logs_pipeline.append({
                    "number_pipeline": number_pipeline,
                    'runner_id': self.runner_id,
                    'name': self.name,
                    'name_pipeline': key,
                    'parent_pipeline': parent_pipelines,
                    'error': not success_retry,
                    'error_break': pipeline['break_error'],
                    'error_message': error_message,
                    'error_trace': trace
                })

                # error break pipeline if retry failed
                if pipeline['break_error'] and success_retry == False:
                    break

            parent_pipelines = key
            self.logs.success(f"Running {self.name}:{number_pipeline}_{key} ... done ")


    """ Just display message """
    def errorDisplay(self, message, trace):
        if self.verbose:
            self.logs.error(trace)
        else:
            self.logs.error(message)


    """ Checking file etl pipeline """
    def checkExistsFilePipelines(self):
        if self._checkDirPipelines() == False:
            raise ValueError(f"ETL Pipelines not exists with name {self.name}")
        
        if self._checkMainPipelines() == False:
            raise ValueError(f"File __init__.py not exists in ETL Pipelines {self.name}")

    """ Import file pipeline """
    def importPipeline(self):
        try:
            sys.path.append(self._pathPipelines())
            from __init__ import pipelines
            if type(pipelines) != dict:
                raise ValueError("Variable pipelines must dict data")

            self.pipelines = pipelines
        except Exception:
            Logs().error(traceback.format_exc())

    """ Running pipelines """ 
    def run(self):
        self.checkExistsFilePipelines()
        self.importPipeline()
        self.execPipelines()

