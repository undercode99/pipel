"""Pipel 1.0.

Usage:
  pipel new <name> [-d | --dir]
  pipel create <name>
  pipel run <name> [-v | --verbose] [-d | --dir]
  pipel list
  pipel storage info <name>
  pipel storage clean <name>
  
Help: 
  pipel (-h | --help)
  pipel --version

Options:
  -h --help     Show this screen.
  --version     Show version.
  -v --verbose  Show verbose data output
  -d --dir      Working dir location


"""
from docopt import docopt
from pipel.logs import Logs
from pipel.services import create_new_project, create_pipeline, run_pipeline,info_storage_pipeline,remove_storage_pipeline
import traceback


def main():
    arguments = docopt(__doc__, version='Pipel 1.0')
    try:
        if arguments['run'] == True:
            run_pipeline(arguments['<name>'], arguments['--verbose'])

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
        if arguments['--verbose']:
          l.error(traceback.format_exc())
