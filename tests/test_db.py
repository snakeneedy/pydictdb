import datetime
import unittest

from pydictdb import core
from pydictdb import db


class ModelInTestDB(db.Model):
    name = db.StringAttribute()
    score = db.IntegerAttribute()


class AttributeTestCase(unittest.TestCase):
    def test_check_value_class(self):
        def test_attribute(attr, allowed, not_allowed):
            for value in allowed:
                attr._check_value_class(value)
            for value in not_allowed:
                with self.assertRaises(TypeError):
                    attr._check_value_class(value)

        attr = db.GenericAttribute()
        allowed = [bool(), int(), None, float(), str()]
        not_allowed = [list(), tuple(), dict(), ModelInTestDB()]
        test_attribute(attr, allowed, not_allowed)

        attr = db.BooleanAttribute()
        allowed = [bool(), None]
        not_allowed = [int(), float(), str(), list(), tuple(), dict(),
                ModelInTestDB()]
        test_attribute(attr, allowed, not_allowed)

        attr = db.IntegerAttribute()
        allowed = [int(), None]
        not_allowed = [bool(), float(), str(), list(), tuple(), dict(),
                ModelInTestDB()]
        test_attribute(attr, allowed, not_allowed)

        attr = db.FloatAttribute()
        allowed = [None, float()]
        not_allowed = [bool(), int(), str(), list(), tuple(), dict(),
                ModelInTestDB()]
        test_attribute(attr, allowed, not_allowed)

        attr = db.StringAttribute()
        allowed = [None, str()]
        not_allowed = [bool(), int(), float(), list(), tuple(), dict(),
                ModelInTestDB()]
        test_attribute(attr, allowed, not_allowed)

    def test_datetime_attribute(self):
        # NOTE: issubclass(datetime.datetime, datetime.date) returns True
        now = datetime.datetime.now()
        date_fmt = '%Y-%m-%d'
        date_attr = db.DateAttribute(fmt=date_fmt)
        date_str = now.strftime(date_fmt)
        self.assertEqual(date_attr.encode(now), date_str)
        self.assertIsInstance(date_attr.decode(date_str), datetime.date)
        self.assertNotIsInstance(date_attr.decode(date_str), datetime.datetime)

        datetime_fmt = '%Y-%m-%d %H:%M:%S.%f'
        datetime_attr = db.DatetimeAttribute(fmt=datetime_fmt)
        datetime_str = now.strftime(datetime_fmt)
        self.assertEqual(datetime_attr.encode(now), now.strftime(datetime_fmt))
        self.assertIsInstance(datetime_attr.decode(datetime_str),
                datetime.datetime)


class ModelTestCase(unittest.TestCase):
    def test_put(self):
        model = ModelInTestDB(name='Sam', score=90)
        key = model.put()
        self.assertTrue('ModelInTestDB' in db._database_in_use._tables)
        self.assertEqual(db._database_in_use.table(key.kind).get(key.object_id),
                model.to_dict(exclude=('key',)))

    def test_check_on_setattr(self):
        ModelInTestDB(name='Sam', score=90)
        with self.assertRaises(TypeError):
            # 'name' should be 'str'
            ModelInTestDB(name=12345)

        with self.assertRaises(TypeError):
            # 'score' should be 'int'
            ModelInTestDB(score='90')

        # pass on not-set attribute
        ModelInTestDB(height='180.0')

    def test_only_kept_attr(self):
        class ModelInTestCase(db.Model):
            name = db.StringAttribute()
            score = db.IntegerAttribute(kept=False)

        model = ModelInTestCase(name='Sam', score=100)
        key = model.put()
        self.assertTrue('score' in model.to_dict())
        self.assertFalse('score' in
                db._database_in_use._tables['ModelInTestCase'][key.object_id])

        # non-kept attribute still check type
        with self.assertRaises(TypeError):
            ModelInTestCase(name='Sam', score='100')

    def test_attr_default(self):
        class ModelInTestCase(db.Model):
            name = db.StringAttribute(default='Sam')
            score = db.IntegerAttribute(default=100)

        self.assertEqual(ModelInTestCase().name, 'Sam')
        self.assertEqual(ModelInTestCase().score, 100)

    def test_encode_when_put(self):
        class ModelInTestCase01(db.Model):
            birth = db.DateAttribute()
            created_at = db.DatetimeAttribute()

        birth = datetime.date(2009, 1, 1)
        created_at = datetime.datetime(2019, 1, 1, 13, 10, 30)
        model = ModelInTestCase01(birth=birth, created_at=created_at)
        key = model.put()
        obj = db._database_in_use._tables['ModelInTestCase01'][key.object_id]
        self.assertEqual(obj['birth'], '2009-01-01')
        self.assertEqual(obj['created_at'], '2019-01-01 13:10:30.000000')


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

    def test_decode_when_get(self):
        class ModelInTestCase01(db.Model):
            birth = db.DateAttribute()
            created_at = db.DatetimeAttribute()

        birth = datetime.date(2009, 1, 1)
        created_at = datetime.datetime(2019, 1, 1, 13, 10, 30)
        object_id = core.Table._next_id()
        db._database_in_use.table('ModelInTestCase01').dictionary[object_id] = {
            'birth': '2009-01-01', 'created_at': '2019-01-01 13:10:30.000000',
        }
        key = db.Key('ModelInTestCase01', object_id)
        self.assertEqual(key.get().birth, birth)
        self.assertEqual(key.get().created_at, created_at)

    def test_delete(self):
        class ModelInTestCase01(db.Model):
            pass

        model = ModelInTestCase01()
        key = model.put()
        self.assertIsNotNone(key.get())
        key.delete()
        self.assertIsNone(key.get())
