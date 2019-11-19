import unittest

from pydictdb import core
from pydictdb import storages


class DatabaseTestCase(unittest.TestCase):
    def test_init_commit(self):
        sto = storages.MemoryStorage()
        sto._memory = {'User': {'name': 'Sam'}}
        database = core.Database(storage=sto)
        self.assertEqual(database._tables, sto._memory)

        database._tables['User']['name'] = 'Tom'
        self.assertNotEqual(database._tables, sto._memory)

        database.commit()
        self.assertEqual(database._tables, sto._memory)
