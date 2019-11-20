import unittest

from pydictdb import db


class User(db.Model):
    pass


class ModelTestCase(unittest.TestCase):
    def test_put(self):
        model = User(name='Sam', score=90)
        key = model.put()
        self.assertTrue('User' in db._database_in_use._tables)
        self.assertEqual(db._database_in_use.table(key.kind).get(key.object_id),
                model.to_dict(exclude=('key',)))


class KeyTestCase(unittest.TestCase):
    def test_get_class(self):
        self.assertEqual(db.Key._get_class('User'), User)
        with self.assertRaises(TypeError):
            db.Key._get_class('abcdefghijklmnopqrstuvwxyz')
