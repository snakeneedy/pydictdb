from . import storages


class Database(object):
    def __init__(self, storage=storages.MemoryStorage()):
        self.storage = storage
        if storage:
            self._tables = storage.read()
        else:
            self._tables = {}

    def commit(self):
        self.storage.write(self._tables)
