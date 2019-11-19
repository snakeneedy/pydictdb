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

        table.dictionary[0] = object()
        self.assertEqual(table.dictionary[0], database._tables[kind][0])


class TableTestCase(unittest.TestCase):
    def setUp(self):
        self.kind = 'User'
        self.table = core.Table(self.kind)

    def tearDown(self):
        self.table.dictionary = {}

    def test_set_get_delete(self):
        obj = {'name': 'Sam', 'groups': ['A', 'B']}
        self.table._set_object(0, obj)
        self.assertEqual(self.table.dictionary[0], obj)
        self.assertEqual(self.table._get_object(0), obj)

        obj['groups'].remove('A')
        self.assertNotEqual(self.table.dictionary[0], obj)
        self.assertNotEqual(self.table._get_object(0), obj)

        obj = self.table._get_object(0)
        obj['groups'].remove('A')
        self.assertNotEqual(self.table.dictionary[0], obj)
        self.assertNotEqual(self.table._get_object(0), obj)

        self.table._delete_object(0)
        self.assertFalse(0 in self.table.dictionary)

        # delete a non-existed id without KeyError
        self.table._delete_object(0)

    def test_CRUD_methods(self):
        obj = {'name': 'Sam', 'groups': ['A', 'B']}
        object_id = self.table.insert(obj)
        self.assertEqual(self.table.dictionary[object_id], obj)
        self.assertEqual(self.table.get(object_id), obj)

        obj = {'name': 'Sam', 'groups': []}
        self.assertNotEqual(self.table.get(object_id), obj)
        self.table.update(object_id, obj)
        self.assertEqual(self.table.get(object_id), obj)

        self.table.delete(object_id)
        self.assertFalse(object_id in self.table.dictionary)
        self.assertIsNone(self.table.get(object_id))

        with self.assertRaises(KeyError):
            self.table.update(object_id, obj)

        with self.assertRaises(KeyError):
            self.table.delete(object_id)

        self.table.update_or_insert(0, obj)
        self.assertEqual(self.table.get(0), obj)

    def test_CRUD_multi_methods(self):
        objects = [
            {'name': 'Sam'},
            {'name': 'Tom'},
            {'name': 'John'},
        ]
        object_ids = self.table.insert_multi(objects)
        self.assertEqual(objects,
                [self.table.dictionary[object_id] for object_id in object_ids])
        self.assertEqual(objects, self.table.get_multi(object_ids))

        objects = [
            {'name': 'Sam', 'score': 99},
            {'name': 'Tom', 'score': 98},
            {'name': 'John', 'score': 97},
        ]
        self.table.update_multi(object_ids, objects)
        self.assertEqual(objects, self.table.get_multi(object_ids))

        self.table.update_or_insert_multi([0, 1, 2], objects)
        self.assertEqual(objects, self.table.get_multi([0, 1, 2]))

        self.table.delete_multi(object_ids)
        for object_id in object_ids:
            self.assertFalse(object_id in self.table.dictionary)

    def test_query(self):
        self.table.insert({'name': 'Sam'})
        query = self.table.query()
        self.assertIsInstance(query, core.Query)
        self.assertEqual(query.dictionary, self.table.dictionary)
