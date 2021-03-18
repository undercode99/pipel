import json
import os
from datetime import datetime
import os
from pipel.config_loader import  use_config
from pipel.storages.storage_config import StorageConfig

def create_report_pipeline(name, runner_id, logs_pipeline):
    config = use_config()
    data_logs = {
        "name": name,
        "runner_id": runner_id,
        "logs": logs_pipeline,
        "storage": StorageConfig.get(),
        "running": str(datetime.now())
    }
    
    direct = f"{config['logs_data']}/{name}"
    if not os.path.exists(direct):
        os.makedirs(direct)

    path_file = f"{direct}/pipeline_runner_{runner_id}.json"
    with open(path_file, "w+") as o:
        o.write(json.dumps(data_logs))