import copy
import datetime
from . import core


_database_in_use = core.Database()


class Attribute(object):
    _allowed_classes = (type(None),)

    def __init__(self, default=None, kept=True):
        self._check_value_class(default)
        self.default = default
        self.kept = bool(kept)

    def get_default(self):
        return copy.deepcopy(self.default)

    @classmethod
    def _check_value_class(cls, value):
        for _class in cls._allowed_classes:
            if isinstance(value, _class):
                break
        else:
            msg = "value type '%s' is not allowed" % type(value).__name__
            raise TypeError(msg)

    def decode(self, generic_value):
        return generic_value

    def encode(self, value):
        return value


class GenericAttribute(Attribute):
    _allowed_classes = (bool, int, type(None), float, str)


class BooleanAttribute(Attribute):
    _allowed_classes = (bool, type(None))


class IntegerAttribute(Attribute):
    _allowed_classes = (int, type(None))

    @classmethod
    def _check_value_class(cls, value):
        # FIXME: isinstance(bool(), int) returns True
        if isinstance(value, bool):
            raise TypeError("value type 'bool' is not allowed")

        super()._check_value_class(value)


class FloatAttribute(Attribute):
    _allowed_classes = (type(None), float)


class StringAttribute(Attribute):
    _allowed_classes = (type(None), str)


# NOTE: issubclass(datetime.datetime, datetime.date) returns True
class DateAttribute(Attribute):
    _allowed_classes = (type(None), datetime.date)

    def __init__(self, fmt='%Y-%m-%d', **kwargs):
        super().__init__(**kwargs)
        self.fmt = fmt

    def decode(self, generic_value):
        if generic_value is None:
            return None
        return datetime.datetime.strptime(generic_value, self.fmt).date()

    def encode(self, value):
        if value is None:
            return None
        return datetime.datetime.strftime(value, self.fmt)


class DatetimeAttribute(DateAttribute):
    def __init__(self, fmt='%Y-%m-%d %H:%M:%S.%f', **kwargs):
        super().__init__(fmt=fmt, **kwargs)

    def decode(self, generic_value):
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
        super().__init__(**kwargs)

    def __setattr__(self, name, value):
        cls_dict = self.__class__.__dict__
        if name in cls_dict and isinstance(cls_dict[name], Attribute):
            try:
                cls_dict[name]._check_value_class(value)
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
    def _get_kept_attributes(cls):
        cls_dict = cls.__dict__
        attributes = {name: attr for name, attr in cls_dict.items()
                if isinstance(attr, Attribute) and attr.kept}
        return attributes

    def put(self):
        kind = self.__class__.__name__
        table = _database_in_use.table(kind)
        attributes = self._get_kept_attributes()
        obj = self.to_dict(include=tuple(attributes.keys()), exclude=('key',))
        if self.key:
            table.update_or_insert(self.key.object_id, obj)
        else:
            object_id = table.insert(obj)
            self.key = Key(kind, object_id)

        return self.key


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

        return cls(key=self, **obj)
