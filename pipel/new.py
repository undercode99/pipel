import sys
import os
from pipel.logs import Logs

def create_dir_project(dirname):
    project_exists = os.path.exists(dirname)
    if project_exists:
        raise Exception(f"Directory with name {dirname} already exists, try with another name")
    
    task_dir = dirname+"/task"
    os.makedirs(task_dir)

    storage_dir = dirname+"/storage"
    os.makedirs(storage_dir)

    logs_data = dirname+"/.logs_data"
    os.makedirs(logs_data)

    abs_storage = os.path.abspath(storage_dir)
    abs_logs_data = os.path.abspath(logs_data)
    config_file = f"""storages:
  default: 
    type: local
    dir_location: {abs_storage}

notification:
  telegram: 
    type: telegram
    token: ""
    
  email: 
    type: email

default_notification: telegram
default_storage: default
logs_data: {abs_logs_data}
"""
    with open(dirname+"/config.yml","w+") as o:
        o.write(config_file)

def new_project(dirname):
    create_dir_project(dirname)

    message = f"""Congrats, create project successfuly

Enter dir project:
cd {dirname}

Try create pipelines:
reetl create <name_pipeline>

Ex: reetl create scrap_web_data
    """
    Logs().success(message)


    

