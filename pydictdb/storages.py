import abc
import copy
import json
import os


class Storage(abc.ABC):
    @abc.abstractmethod
    def read(self):
        raise NotImplementedError

    @abc.abstractmethod
    def write(self, data):
        raise NotImplementedError


class MemoryStorage(Storage):
    def __init__(self):
        self._memory = {}

    def read(self):
        return copy.deepcopy(self._memory)

    def write(self, data):
        if not isinstance(data, dict):
            raise TypeError("argument 'data' must be dict, but %s" % (type(data).__name__))

        self._memory = copy.deepcopy(data)


class FileStorage(Storage):
    def __init__(self, path):
        path = os.path.abspath(path)
        # create if not existed
        if not os.path.exists(path):
            open(path, 'w').close()

        self._fp = open(path, 'r+')

    def close(self):
        self._fp.close()

    def read(self):
        self._fp.seek(0)
        content = self._fp.read()
        return self.__class__.decode(content)

    def write(self, data):
        content = self.__class__.encode(data)
        self._fp.seek(0)
        self._fp.truncate()
        self._fp.write(content)
        self._fp.flush()

    @classmethod
    def decode(cls, content):
        return content

    @classmethod
    def encode(cls, data):
        return data


class JsonStorage(FileStorage):
    @classmethod
    def decode(cls, content):
        return json.loads(content)

    @classmethod
    def encode(cls, data):
        return json.dumps(data)
