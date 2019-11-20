import unittest

from pydictdb import db


class ModelInTestDB(db.Model):
    pass


class AttributeTestCase(unittest.TestCase):
    def test_check_value_class(self):
        attr = db.GenericAttribute()
        attr._check_value_class(bool())
        attr._check_value_class(int())
        attr._check_value_class(None)
        attr._check_value_class(float())
        attr._check_value_class(str())
        with self.assertRaises(TypeError):
            attr._check_value_class(list())
        with self.assertRaises(TypeError):
            attr._check_value_class(tuple())
        with self.assertRaises(TypeError):
            attr._check_value_class(dict())
        with self.assertRaises(TypeError):
            attr._check_value_class(ModelInTestDB())


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
        self.assertIsNone(db.Key._get_class('abcdefghijklmnopqrstuvwxyz'))

    def test_get(self):
        model = ModelInTestDB(name='Sam', score=90)
        key = model.put()
        self.assertEqual(model, key.get())

        # Key with invalid object_id get None
        self.assertIsNone(db.Key('ModelInTestDB', 0).get())

        self.assertIsNone(db.Key('abcdefghijklmnopqrstuvwxyz', 0).get())
