from pipel.config_loader import use_config
from glob import glob
import json
import os
import shutil

class StorageLocalFunction:

    def __init__(self, option):
        self.option = option

    def path(self, pth=""):
        return f"{self.option['dir_location']}/{self.option['name_pipeline']}/{pth}"
    
    def removeStorage(self):
        if os.path.exists(self.path()):
            shutil.rmtree(self.path())

    def dataSize(self):
        def recursive_dir_size(path):
            size = 0

            for x in os.listdir(path):
                if not os.path.isdir(os.path.join(path,x)):
                    size += os.stat(os.path.join(path,x)).st_size
                else:
                    size += recursive_dir_size(os.path.join(path,x))
            return size
        return recursive_dir_size(self.path())

class StorageSftpFunction:
    pass

class StorageFtpFunction:
    pass

class StorageFunction:

    info_logs = []

    def __setup(self, option):
        print(option)
        if option['type'] == "local":
            self.object_storage = StorageLocalFunction(option)
        else:
            self.object_storage = StorageLocalFunction(option)

    def getPathConfig(self, name):
        cfg = use_config()
        print(cfg)
        return f"{cfg['logs_data']}/{name}/pipeline_runner_*.json"

    def checkInfoLogsData(self, name):
        
        file_path = self.getPathConfig(name)

        for json_runner in glob(file_path):
            dtjson = json.loads(open(json_runner).read())
            self.__setup({**dtjson['storage']['option'], **{"name_pipeline": name}})
            self.info_logs.append({
                "name": dtjson["name"],
                "finish_time": dtjson['running'],
                "data_size": self.object_storage.dataSize()
            })
        return self.info_logs

    def removeStorageByName(self, name):
        file_path = self.getPathConfig(name)
        for json_runner in glob(file_path):
            dtjson = json.loads(open(json_runner).read())
            self.__setup({**dtjson['storage']['option'], **{"name_pipeline": name}})
            self.object_storage.removeStorage()
            print(f"Removing storage {name} Success !!!")