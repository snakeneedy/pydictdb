import datetime
import unittest

from pydictdb import core
from pydictdb import db
from pydictdb import storages


class ModelInTestDB(db.Model):
    name = db.StringAttribute()
    score = db.IntegerAttribute()


class AttributeTestCase(unittest.TestCase):
    def test_validate_value(self):
        def test_attribute(attr, allowed, not_allowed):
            for value in allowed:
                attr._do_validate_value(value)
            for value in not_allowed:
                with self.assertRaises(TypeError):
                    attr._do_validate_value(value)

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

    def test_repeated(self):
        db.IntegerAttribute(repeated=True, default=None)
        db.IntegerAttribute(repeated=True, default=[])
        db.IntegerAttribute(repeated=True, default=[1, 2]) # pass
        with self.assertRaises(TypeError):
            db.IntegerAttribute(repeated=True, default=[1, '2'])

        with self.assertRaises(TypeError):
            #  TypeError: 'int' object is not iterable
            db.IntegerAttribute(repeated=True, default=1)

    def test_repeated_encode_decode(self):
        now = datetime.datetime.now()
        attr = db.DatetimeAttribute(default=[], repeated=True)
        now_str = now.strftime(attr.fmt)
        self.assertEqual(attr.encode([now]), [now_str])
        self.assertEqual(attr.decode([now_str]), [now])

    def test_choices(self):
        db.IntegerAttribute(choices=None)
        with self.assertRaises(TypeError):
            db.IntegerAttribute(choices=['a'])

        attr = db.IntegerAttribute(choices=[1, 2, 3])
        with self.assertRaises(ValueError):
            attr._do_validate_value(0)

        with self.assertRaises(ValueError):
            db.IntegerAttribute(choices=[1, 2, 3], default=0)

    def test_key_attr(self):
        attr = db.KeyAttribute()
        attr._do_validate_value(db.Key('User', 0)) # pass
        with self.assertRaises(TypeError):
            attr._do_validate_value(0)

        attr = db.KeyAttribute(kind='User')
        attr._do_validate_value(db.Key('User', 0)) # pass
        with self.assertRaises(ValueError):
            attr._do_validate_value(db.Key('Group', 0))

        attr = db.KeyAttribute(kind='User', repeated=True)
        attr._do_validate_value([db.Key('User', 0), db.Key('User', 1)])
        with self.assertRaises(ValueError):
            attr._do_validate_value([db.Key('User', 0), db.Key('Group', 1)])


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

    def test_query(self):
        class ModelInTestCase03(db.Model):
            name = db.StringAttribute()
            score = db.IntegerAttribute(default=0)
            birth = db.DateAttribute()

        db.delete_multi(ModelInTestCase03.query().fetch(keys_only=True))
        models = [
            ModelInTestCase03(name='Sam', score=90,
                    birth=datetime.date(2009, 1, 1)),
            ModelInTestCase03(name='Tom', score=80,
                    birth=datetime.date(2010, 1, 1)),
            ModelInTestCase03(name='John', score=70,
                    birth=datetime.date(2011, 1, 1)),
        ]
        db.put_multi(models)
        self.assertEqual(models, ModelInTestCase03.query().fetch())

        query = ModelInTestCase03.query(lambda m: m.score >= 80)
        self.assertEqual(models[:2], query.fetch())

        query = ModelInTestCase03.query(
                lambda m: m.birth >= datetime.date(2010, 1, 1))
        self.assertEqual(models[1:], query.fetch())


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


class FunctionTestCase(unittest.TestCase):
    def test_multi(self):
        class ModelInTestCase02(db.Model):
            name = db.StringAttribute()
            score = db.IntegerAttribute()

        models = [
            ModelInTestCase02(name='Sam', score=100),
            ModelInTestCase02(name='Tom', score=90),
            ModelInTestCase02(name='John', score=80),
        ]
        keys = db.put_multi(models)
        for key, model in zip(keys, models):
            self.assertEqual(key.get(), model)

        self.assertEqual(db.get_multi(keys), models)

        db.delete_multi(keys)
        for key in keys:
            self.assertIsNone(key.get())

        # pass
        db.delete_multi(keys)

    def test_register_database(self):
        class ModelInTestCase04(db.Model):
            name = db.StringAttribute()
            score = db.IntegerAttribute()

        objects = [
            {'name': 'Sam', 'score': 90},
            {'name': 'Tom', 'score': 80},
            {'name': 'John', 'score': 70},
        ]
        database_1 = core.Database(storage=storages.MemoryStorage())
        database_2 = core.Database(storage=storages.MemoryStorage())

        db.register_database(database_1)
        models = [ModelInTestCase04(**obj) for obj in objects]
        keys = db.put_multi(models)
        self.assertEqual(db.get_multi(keys), models)

        db.register_database(database_2)
        self.assertEqual(db.get_multi(keys), [None] * len(keys))
