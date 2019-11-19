import copy
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

    def _set_object(self, object_id, obj):
        self.objects[object_id] = dict(copy.deepcopy(obj))

    def _get_object(self, object_id):
        return copy.deepcopy(self.objects.get(object_id, None))

    def _delete_object(self, object_id):
        try:
            del self.objects[object_id]
        except KeyError:
            pass
