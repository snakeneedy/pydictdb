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

    def test_table(self):
        database = core.Database()
        kind = 'User'
        table = database.table(kind)
        self.assertEqual(table.kind, kind)

        table.objects[0] = object()
        self.assertEqual(table.objects[0], database._tables[kind][0])


class TableTestCase(unittest.TestCase):
    def test_set_get_delete(self):
        kind = 'User'
        table = core.Table(kind)
        obj = {'name': 'Sam', 'groups': ['A', 'B']}
        table._set_object(0, obj)
        self.assertEqual(table.objects[0], obj)
        self.assertEqual(table._get_object(0), obj)

        obj['groups'].remove('A')
        self.assertNotEqual(table.objects[0], obj)
        self.assertNotEqual(table._get_object(0), obj)

        obj = table._get_object(0)
        obj['groups'].remove('A')
        self.assertNotEqual(table.objects[0], obj)
        self.assertNotEqual(table._get_object(0), obj)

        table._delete_object(0)
        self.assertFalse(0 in table.objects)

        # delete a non-existed id without KeyError
        table._delete_object(0)
