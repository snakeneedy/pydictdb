from . import core


_database_in_use = core.Database()


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

    def put(self):
        kind = self.__class__.__name__
        table = _database_in_use.table(kind)
        obj = self.to_dict(exclude=('key',))
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
