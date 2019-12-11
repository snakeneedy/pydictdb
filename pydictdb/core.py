import copy
import datetime
from collections import OrderedDict
from . import storages


class Database(object):
    def __init__(self, storage=storages.MemoryStorage(), auto_commit=True):
        self.storage = storage
        if storage:
            self._tables = storage.read()
        else:
            self._tables = OrderedDict()

        self.auto_commit = auto_commit

    def commit(self):
        self.storage.write(self._tables)

    def table(self, kind):
        if kind not in self._tables:
            self._tables[kind] = OrderedDict()

        table = Table(kind, self._tables[kind], self)
        return table


class Table(object):
    def __init__(self, kind, dictionary=None, database=None):
        self.kind = kind
        if dictionary is None:
            self.dictionary = OrderedDict()
        else:
            # bind dictionary to the argument one
            self.dictionary = dictionary

        self.database = database

    def _auto_commit(self):
        if self.database and self.database.auto_commit:
            self.database.commit()

    def _set_object(self, object_id, obj):
        self.dictionary[object_id] = dict(copy.deepcopy(obj))
        self._auto_commit()

    def _get_object(self, object_id):
        return copy.deepcopy(self.dictionary.get(object_id, None))

    def _delete_object(self, object_id):
        try:
            del self.dictionary[object_id]
            self._auto_commit()
        except KeyError:
            pass

    def _do_validate_id(self, object_id):
        if object_id not in self.dictionary:
            if isinstance(object_id, str):
                object_id = "'%s'" % object_id
            raise KeyError("invalid object_id %s" % str(object_id))

    @classmethod
    def _next_id(cls):
        return str(int(datetime.datetime.now().timestamp() * 1000000))

    def insert(self, obj):
        object_id = self.__class__._next_id()
        self._set_object(object_id, obj)
        return object_id

    def insert_multi(self, objects):
        return [self.insert(obj) for obj in objects]

    def get(self, object_id):
        return self._get_object(object_id)

    def get_multi(self, object_ids):
        return [self.get(object_id) for object_id in object_ids]

    def update(self, object_id, obj):
        self._do_validate_id(object_id)
        self._set_object(object_id, obj)
        return object_id

    def update_multi(self, object_ids, objects):
        if len(object_ids) != len(objects):
            raise ValueError("size of object_ids and objects must be the same")

        for object_id in object_ids:
            self._do_validate_id(object_id)

        for object_id, obj in zip(object_ids, objects):
            self._set_object(object_id, obj)

        return object_ids

    def update_or_insert(self, object_id, obj):
        self._set_object(object_id, obj)
        return object_id

    def update_or_insert_multi(self, object_ids, objects):
        if len(object_ids) != len(objects):
            raise ValueError("size of object_ids and objects must be the same")

        for object_id, obj in zip(object_ids, objects):
            self._set_object(object_id, obj)

        return object_ids

    def delete(self, object_id, ignore_exception=False):
        if not ignore_exception:
            self._do_validate_id(object_id)

        self._delete_object(object_id)

    def delete_multi(self, object_ids):
        for object_id in object_ids:
            self._do_validate_id(object_id)

        for object_id in object_ids:
            self._delete_object(object_id)

    def query(self, test_func=lambda obj: True):
        return Query(self.dictionary, test_func)


class Query(object):
    def __init__(self, dictionary, test_func=lambda obj: True):
        self.dictionary = dictionary
        self.test_func = test_func

    def fetch(self, ids_only=False):
        dictionary = copy.deepcopy(self.dictionary)
        if ids_only:
            return [object_id for object_id, obj in dictionary.items()
                    if self.test_func(obj)]
        else:
            return [obj for obj in dictionary.values() if self.test_func(obj)]
