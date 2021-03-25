import os


class StorageSftp:
    pass


class StorageFtp:
    pass


class StorageLocal:

    option = {}

    def __init__(self, opt):
        self.option = opt
        direct = self.option['dir_location']
        if not os.path.exists(direct):
            raise ValueError("Directory {direct} not exists")

        if not os.path.exists(self.path()):
            os.makedirs(self.path())

    def mkdir(self, direct, auto=True):
        if auto:
            return os.makedirs(self.path(direct))
        return os.mkdir(direct)

    def rm(self, file_name):
        os.remove(self.path(file_name))

    def exists(self, file_name):
        return os.path.exists(self.path(file_name))

    def path(self, pth=""):
        return f"{self.option['dir_location']}/{self.option['name_pipeline']}/{pth}"

    def write(self, file_name, content, mode="w+"):
        with open(self.path(file_name), mode) as w:
            w.write(content)


class Storage:

    def setup(self, option):
        if option['type'] == "local":
            self.object_storage = StorageLocal(option)
        else:
            self.object_storage = StorageLocal(option)

    def path(self, pth=""):
        return self.object_storage.path(pth)

    def mkdir(self, direct, auto=True):
        return self.object_storage.mkdir(direct, auto)

    def rm(self, file_name):
        return self.object_storage.rm(file_name)

    def exists(self, file_name):
        return self.object_storage.exists(file_name)

    def lock(self, file_name):
        pass

    def hashLock(self, file_name):
        pass

    def write(self, file_name, content, mode="w+"):
        return self.object_storage.write(file_name, content, mode)

    def __str__(self):
        return self.path()
