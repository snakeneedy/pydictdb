import abc
import copy
import json
import os
from collections import OrderedDict


class Storage(object):
    """This abstract class is to store data in different format. Implemented in
    :py:meth:`read` and :py:meth:`write`.
    """
    @abc.abstractmethod
    def read(self):
        raise NotImplementedError

    @abc.abstractmethod
    def write(self, data):
        raise NotImplementedError


class MemoryStorage(Storage):
    """This class is to read and write data in an isolated dict in memory.

    Attributes:
        _memory (dict): The stored data without specific form.
    """
    def __init__(self):
        self._memory = OrderedDict()

    def read(self):
        """Read data from the memory.

        Returns:
            (dict) -- A copy of :py:attr:`_memory`.
        """
        return copy.deepcopy(self._memory)

    def write(self, data):
        """Write data to the memory.

        Args:
            data (dict): The written data.
        """
        if not isinstance(data, dict):
            raise TypeError("argument 'data' must be dict, but %s" % (type(data).__name__))

        self._memory = copy.deepcopy(data)


class FileStorage(Storage):
    """This class is to read and write data in a file.

    Args:
        path (str): The absolute or relative path of the file, created if not
            existed.

    Attributes:
        _fp (io.TextIOWrapper): An I/O wrapper to handle a file.
    """
    def __init__(self, path):
        path = os.path.abspath(path)
        # create if not existed
        if not os.path.exists(path):
            open(path, 'w').close()

        self._fp = open(path, 'r+')

    def close(self):
        """Close the attribute :py:attr:`_fp`.
        """
        self._fp.close()

    def read(self):
        """Read and decode content from the file, which call :py:meth:`decode`
        on content.

        Returns:
            (dict) -- The decoded data.
        """
        self._fp.seek(0)
        content = self._fp.read()
        return self.__class__.decode(content)

    def write(self, data):
        """Write and encode data into the file, which call :py:meth:`encode` on
        data.

        Args:
            data (dict): The data to be written.
        """
        content = self.__class__.encode(data)
        self._fp.seek(0)
        self._fp.truncate()
        self._fp.write(content)
        self._fp.flush()

    @classmethod
    def decode(cls, content):
        """Decode method without implementation.

        Args:
            content (object): Any type object.

        Returns:
            (object) -- The argument content.
        """
        return content

    @classmethod
    def encode(cls, data):
        """Encode method without implementation.

        Args:
            data (object): Any type object.

        Returns:
            (object) -- The argument data.
        """
        return data


class JsonStorage(FileStorage):
    """This class is to read and write data in a JSON format file.
    """
    @classmethod
    def decode(cls, content):
        """Decode str to dict.

        Args:
            content (str): The encoded text content.

        Returns:
            (dict) -- The decoded data.
        """
        if content:
            return json.loads(content)

        return OrderedDict()

    @classmethod
    def encode(cls, data):
        """Encode dict to str.

        Args:
            data (dict): The decoded data.

        Returns:
            (str) -- The encoded text content.
        """
        return json.dumps(data)
