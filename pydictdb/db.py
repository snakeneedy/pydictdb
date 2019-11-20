from . import core


_database_in_use = core.Database()


class Attribute(object):
    _allowed_classes = tuple()

    def __init__(self, kept=True):
        self.kept = bool(kept)

    @classmethod
    def _check_value_class(cls, value):
        for _class in cls._allowed_classes:
            if isinstance(value, _class):
                break
        else:
            msg = "value type '%s' is not allowed" % type(value).__name__
            raise TypeError(msg)


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

    def put(self):
        cls = self.__class__
        kind = cls.__name__
        table = _database_in_use.table(kind)
        cls_dict = cls.__dict__
        # NOTE: only put kept attributes to database
        attributes = {name: attr for name, attr in cls_dict.items()
                if isinstance(attr, Attribute) and attr.kept}
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
