import os
import unittest

from pydictdb import storages


class StorageTestCase(unittest.TestCase):
    def test_init(self):
        with self.assertRaises(TypeError):
            sto = storages.Storage()


class MemoryStorageTestCase(unittest.TestCase):
    def test_init_read_write(self):
        data = {'User': {'001': {'name': 'Sam', 'score': 100, 'height': 180.0}}}
        sto = storages.MemoryStorage()
        sto.write(data)
        self.assertEqual(sto.read(), data)
        self.assertEqual(sto._memory, data)

        data['User']['001']['score'] = 200
        self.assertNotEqual(sto._memory, data)

        data = sto.read()
        data['User']['001']['score'] = 200
        self.assertNotEqual(sto._memory, data)


class FileStorageTestCase(unittest.TestCase):
    def setUp(self):
        self.path = os.path.abspath('.storage')

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_init(self):
        sto = storages.FileStorage(self.path)
        self.assertEqual(sto._fp.name, self.path)
        self.assertTrue(os.path.exists(self.path))
        self.assertFalse(sto._fp.closed)

    def test_read(self):
        content = r'content\n'
        fp = open(self.path, 'w')
        fp.write(content)
        fp.close()
        sto = storages.FileStorage(self.path)
        self.assertEqual(sto.read(), content)

    def test_write(self):
        content = r'content\n'
        sto = storages.FileStorage(self.path)
        sto.write(content)
        fp = open(self.path, 'r')
        self.assertEqual(fp.read(), content)
        fp.close()

    def test_close(self):
        sto = storages.FileStorage(self.path)
        self.assertFalse(sto._fp.closed)
        sto.close()
        self.assertTrue(sto._fp.closed)


class JsonStorageTestCase(unittest.TestCase):
    def setUp(self):
        self.path = os.path.abspath('.storage')
        self.data = {'name': 'Sam'}
        self.content = r'{"name": "Sam"}'

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_decode(self):
        self.assertEqual(storages.JsonStorage.encode(self.data), self.content)

    def test_encode(self):
        self.assertEqual(storages.JsonStorage.decode(self.content), self.data)

    def test_read(self):
        fp = open(self.path, 'w')
        fp.write(self.content)
        fp.close()
        sto = storages.JsonStorage(self.path)
        self.assertEqual(sto.read(), self.data)

    def test_write(self):
        sto = storages.JsonStorage(self.path)
        sto.write(self.data)
        fp = open(self.path, 'r')
        self.assertEqual(fp.read(), self.content)
        fp.close()
