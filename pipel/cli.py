"""Pipel 1.0.

Usage:
  pipel new <name> [--dir]
  pipel create <name>
  pipel run <name> [-d | --deamon]
  pipel stop <name>
  pipel logs <name>
  pipel list
  pipel storage info <name>
  pipel storage clean <name>
  
Help: 
  pipel (-h | --help)
  pipel --version

Options:
  -h --help     Show this screen.
  --version     Show version.
  -d --deamon   Running on deamon


"""
from docopt import docopt
from pipel.logs import Logs
from pipel.services import create_new_project, create_pipeline, run_pipeline,info_storage_pipeline,remove_storage_pipeline,stop_pipeline,tail_logs_pipelines
import traceback


def main():
    arguments = docopt(__doc__, version='Pipel 1.0')
    try:
        if arguments['run'] == True:
            run_pipeline(
              name=arguments['<name>'], 
              verbose=True,
              run_in_daemon=arguments['--deamon'])

        if arguments['stop'] == True:
            stop_pipeline(
              name=arguments['<name>']
            )

        if arguments['logs'] == True:
            tail_logs_pipelines(
              name=arguments['<name>']
            )

        if arguments['create'] == True:
            create_pipeline(arguments['<name>'])

        if arguments['new'] == True:
            create_new_project(arguments['<name>'])

        if arguments['storage'] and arguments['info']:
            info_storage_pipeline(arguments['<name>'])

        if arguments['storage'] and arguments['clean']:
            remove_storage_pipeline(arguments['<name>'])

    except Exception as e:
        l = Logs()
        l.error(e)
        l.error(traceback.format_exc())
