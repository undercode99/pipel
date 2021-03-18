
class StorageConfig:

    data_config = {}

    key_config = None

    __seted_just_once = False

    __key_config_list_storages = "storages"
    __key_config_list_default_storage = "default_storage"

    @classmethod
    def use(cls, key):
        if cls.__seted_just_once:
            raise ValueError(f"Set storage just can once set")

        cls.validateKey(key)
        cls.key_config = key
        cls.__seted_just_once = True

    @classmethod
    def validateKey(cls, key):
        if key not in cls.getStorages():
            raise ValueError(f"Config storage {key} not found in config storages")

    @classmethod
    def getStorages(cls):
        data_config = cls.data_config
        key = cls.__key_config_list_storages
        if key not in data_config: 
            raise ValueError("Key storages not found in config")

        return data_config[key]

    @classmethod
    def getDefaultStorage(cls):
        data_config = cls.data_config
        key = cls.__key_config_list_default_storage
        if key not in data_config:
            raise ValueError("Key default_storage not found in config")

        return data_config[key]
    
    @classmethod
    def initilize(cls, data_config):
        cls.data_config = data_config

        default = cls.getDefaultStorage()
        cls.validateKey(default)
        cls.use(default)
        cls.__seted_just_once = False
    
    @classmethod
    def get(cls):
        options_storage = cls.data_config["storages"]
        return {
            "name": cls.key_config,
            "option": options_storage[cls.key_config]
        }
