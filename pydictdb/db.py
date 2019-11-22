import copy
import datetime
from . import core


_database_in_use = core.Database()


class Attribute(object):
    _allowed_classes = []

    def __init__(self, choices=None, default=None, repeated=False, kept=True):
        self.repeated = repeated
        if repeated and not (
                isinstance(default, list) or isinstance(default, tuple)):
            msg = "arg. 'default' should be list or tuple in repeated attribute"
            raise TypeError(msg)

        if repeated:
            default = list(default)

        if choices is not None:
            choices = list(choices)
            for choice in choices:
                # NOTE: self attr. 'choices' is not yet set
                self._do_validate_value(choice)

        self.choices = choices
        self._do_validate_value(default)
        self.default = default
        self.kept = bool(kept)

    def get_default(self):
        return copy.deepcopy(self.default)

    def validate_value(self, value):
        # NOTE: always allow None as single value
        if value is None:
            return

        for _class in self._allowed_classes:
            if isinstance(value, _class):
                break
        else:
            msg = "value type '%s' is not allowed" % type(value).__name__
            raise TypeError(msg)

        # NOTE: self attribute 'choices' is not yet set when validating argument
        # 'choices' in '__init__'
        if getattr(self, 'choices', None) and value not in self.choices:
            msg = "value {} is not in the attribute choices".format(value)
            raise ValueError(msg)

    def _do_validate_value(self, value):
        if self.repeated:
            for val in value:
                self.validate_value(val)
        else:
            self.validate_value(value)

    def _post_decode(self, generic_value):
        return generic_value

    def decode(self, generic_value):
        if self.repeated:
            return [self._post_decode(val) for val in generic_value]
        else:
            return self._post_decode(generic_value)

    def _post_encode(self, value):
        return value

    def encode(self, value):
        if self.repeated:
            return [self._post_encode(val) for val in value]
        else:
            return self._post_encode(value)


class GenericAttribute(Attribute):
    _allowed_classes = [bool, int, float, str]


class BooleanAttribute(Attribute):
    _allowed_classes = [bool]


class IntegerAttribute(Attribute):
    _allowed_classes = [int]

    def validate_value(self, value):
        # FIXME: isinstance(bool(), int) returns True
        if isinstance(value, bool):
            raise TypeError("value type 'bool' is not allowed")

        super().validate_value(value)


class FloatAttribute(Attribute):
    _allowed_classes = [float]


class StringAttribute(Attribute):
    _allowed_classes = [str]


# NOTE: issubclass(datetime.datetime, datetime.date) returns True
class DateAttribute(Attribute):
    _allowed_classes = [datetime.date]

    def __init__(self, fmt='%Y-%m-%d', **kwargs):
        super().__init__(**kwargs)
        self.fmt = fmt

    def _post_decode(self, generic_value):
        if generic_value is None:
            return None
        return datetime.datetime.strptime(generic_value, self.fmt).date()

    def _post_encode(self, value):
        if value is None:
            return None
        return datetime.datetime.strftime(value, self.fmt)


class DatetimeAttribute(DateAttribute):
    def __init__(self, fmt='%Y-%m-%d %H:%M:%S.%f', **kwargs):
        super().__init__(fmt=fmt, **kwargs)

    def _post_decode(self, generic_value):
        if generic_value is None:
            return None
        return datetime.datetime.strptime(generic_value, self.fmt)


class BaseObject(object):
    def __init__(self, **kwargs):
        for kw in kwargs:
            setattr(self, kw, kwargs[kw])

    def __eq__(self, other):
        return type(self) == type(other) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return (not self.__eq__(other))

    def __repr__(self):
        cls = self.__class__
        cls_str = '%s.%s' % (cls.__module__, cls.__name__)

        self_dict = self.__dict__
        prop_list = []
        keys = sorted(self_dict.keys())
        for k in keys:
            v = self_dict[k]
            if isinstance(v, str):
                v = "'%s'" % (v)

            prop_list.append('%s=%s' % (str(k), str(v)))

        prop_str = ', '.join(prop_list)
        return '<%s(%s)>' % (cls_str, prop_str)

    def to_dict(self, include=None, exclude=None):
        self_dict = self.__dict__
        keywords = set(self_dict.keys())
        if include is not None:
            keywords = keywords & set(include)

        if exclude is not None:
            keywords = keywords - set(exclude)

        return {k: self_dict[k] for k in keywords}


class Model(BaseObject):
    def __init__(self, **kwargs):
        self.key = kwargs.pop('key', None)
        # set default value from Attribute
        attributes = self._get_cls_attributes(only_kept=False)
        for name, attr in attributes.items():
            if name not in kwargs:
                kwargs[name] = attr.get_default()

        super().__init__(**kwargs)

    def __setattr__(self, name, value):
        cls_dict = self.__class__.__dict__
        if name in cls_dict and isinstance(cls_dict[name], Attribute):
            try:
                cls_dict[name]._do_validate_value(value)
            except TypeError:
                # NOTE: more readable error message
                msg = "attribute '%s' type '%s' is not allowed" % (
                        name, type(value).__name__)
                raise TypeError(msg)

        return super().__setattr__(name, value)

    def __getattribute__(self, name):
        value = super().__getattribute__(name)
        if (isinstance(value, Attribute)
                and value == getattr(self.__class__, name)):
            return value.get_default()

        return value

    @classmethod
    def _get_cls_attributes(cls, only_kept=True):
        cls_dict = cls.__dict__
        attributes = {name: attr for name, attr in cls_dict.items()
                if isinstance(attr, Attribute) and (attr.kept or not only_kept)}
        return attributes

    def put(self):
        kind = self.__class__.__name__
        table = _database_in_use.table(kind)
        attributes = self._get_cls_attributes()
        obj = self.to_dict(include=tuple(attributes.keys()), exclude=('key',))
        for name, attr in attributes.items():
            if name in obj:
                obj[name] = attr.encode(obj[name])
            else:
                obj[name] = attr.get_default()

        if self.key:
            table.update_or_insert(self.key.object_id, obj)
        else:
            object_id = table.insert(obj)
            self.key = Key(kind, object_id)

        return self.key

    @classmethod
    def query(cls, test_func=lambda obj: True):
        return Query(cls.__name__, test_func)


class Key(BaseObject):
    _classes_dict = {}

    def __init__(self, kind, object_id):
        self.kind = kind
        self.object_id = object_id

    @classmethod
    def _get_class(cls, kind):
        if kind not in cls._classes_dict:
            # refresh _classes_dict
            cls._classes_dict['Model'] = Model
            queue = [Model]
            while queue:
                parent = queue.pop(0)
                for child in parent.__subclasses__():
                    if child.__name__ not in cls._classes_dict:
                        cls._classes_dict[child.__name__] = child
                        queue.append(child)

        _class = cls._classes_dict.get(kind, None)
        return _class

    def get(self):
        table = _database_in_use.table(self.kind)
        obj = table.get(self.object_id)
        if obj is None:
            return None

        cls = self._get_class(self.kind)
        if cls is None:
            return None

        attributes = cls._get_cls_attributes()
        for name, attr in attributes.items():
            if name in obj:
                obj[name] = attr.decode(obj[name])
            else:
                obj[name] = attr.get_default()

        return cls(key=self, **obj)

    def delete(self):
        table = _database_in_use.table(self.kind)
        table.delete(self.object_id, ignore_exception=True)


# NOTE: different interface from `core.Query`
class Query(object):
    def __init__(self, kind, test_func=lambda obj: True):
        self.kind = kind
        self.test_func = test_func

    def fetch(self, keys_only=False):
        table = _database_in_use.table(self.kind)
        keys = []
        for object_id in table.dictionary.keys():
            key = Key(self.kind, object_id)
            if self.test_func(key.get()):
                keys.append(key)

        if keys_only:
            return keys

        return get_multi(keys)


def put_multi(models):
    return [model.put() for model in models]


def get_multi(keys):
    return [key.get() for key in keys]


def delete_multi(keys):
    for key in keys:
        key.delete()


def register_database(database):
    global _database_in_use
    _database_in_use = database
