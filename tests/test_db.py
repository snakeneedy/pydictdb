import unittest

from pydictdb import db


class ModelInTestDB(db.Model):
    pass


class ModelTestCase(unittest.TestCase):
    def test_put(self):
        model = ModelInTestDB(name='Sam', score=90)
        key = model.put()
        self.assertTrue('ModelInTestDB' in db._database_in_use._tables)
        self.assertEqual(db._database_in_use.table(key.kind).get(key.object_id),
                model.to_dict(exclude=('key',)))


class KeyTestCase(unittest.TestCase):
    def test_get_class(self):
        self.assertEqual(db.Key._get_class('ModelInTestDB'), ModelInTestDB)
        with self.assertRaises(TypeError):
            db.Key._get_class('abcdefghijklmnopqrstuvwxyz')
