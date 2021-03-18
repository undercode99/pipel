
import yaml
from pipel.path_loader import path_loader
from pipel.storages.storage_config import StorageConfig

class ConfigLoader:

    data_config = {}
    
    @classmethod
    def dictValidator(csl, config):
        if type(config) != dict:
            raise ValueError(f"{config}  Data config must dict value ..")

    @classmethod
    def new(cls, config_loader):
        config = config_loader()
        cls.dictValidator(config)
        cls.data_config = config

    @classmethod
    def add(cls, config_loader):
        cls.dictValidator(config_loader)
        cls.data_config = {**cls.data_config, **config_loader}

    @classmethod
    def get(cls):
        return cls.data_config

def config_loader():
    config_file = path_loader("config.yml")
    with open(config_file, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            raise ValueError("Yaml CONFIG: ",exc)



def load_config():
    ConfigLoader.new(config_loader)
    StorageConfig.initilize(ConfigLoader.get())

def use_config():
    return ConfigLoader.get()