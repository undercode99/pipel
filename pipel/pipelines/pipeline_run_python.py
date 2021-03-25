
from pipel.pipelines.pipeline import Pipelines
import contextlib
import io

class PipelineRunPython(Pipelines): 
    
    def __init__(self, options):
        """
            {runner_id, script, option}
        """
        pass

    def execFunction(self, func, pipline_return,  opt):
        if func.__code__.co_argcount > 1:
            return func(opt, pipline_return)
        elif func.__code__.co_argcount > 0:
            return func(opt)
        else:
            return func()

    def runFunction(self, func, pipline_return, opt):
        if self.verbose == True:
            self.pipline_return = self.execFunction(func,pipline_return, opt)
        else:
            with contextlib.redirect_stdout(io.StringIO()):
                self.pipline_return = self.execFunction(func,pipline_return, opt)

    
