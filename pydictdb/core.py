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

    def table(self, kind):
        if kind not in self._tables:
            self._tables[kind] = {}

        table = Table(kind, self._tables[kind])
        return table


class Table(object):
    def __init__(self, kind, objects={}):
        self.kind = kind
        self.objects = objects
