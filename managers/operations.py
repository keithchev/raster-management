
import os
import json
import datetime
from . import datasets


class Operation(object):

    _serializable_attrs = ['method', 'kwargs', 'commit', 'timestamp']


    def __repr__(self):

        def _clean(path):
            return path.split(os.sep)[-1]

        return 'Operation(\n    method=\'%s\',\n    kwargs=%s,\n    source=%s,\n    destination=%s)' % \
            (self.method, self.kwargs, [_clean(d.path) for d in self._source], [_clean(d.path) for d in self._destination])


    def __init__(self, source, destination, method=None, kwargs=None, commit=None):

        # note: source is sometimes a single dataset and sometimes a list of datasets
        # for consistency, we force the internal _source and _destination attributes to lists
        # but allow the public attributes to be either single datasets or a list of datasets
        if not isinstance(source, list):
            source = [source]

        if not isinstance(destination, list):
            destination = [destination]

        for dataset in source + destination:
            assert isinstance(dataset, datasets.Dataset)

        self._source = source
        self._destination = destination

        self.method = method
        self.kwargs = kwargs
        self.commit = commit
        self.timestamp = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')


    @property
    def source(self):
        if len(self._source)==1:
            return self._source[0]
        return self._source


    @property
    def destination(self):
        if len(self._destination)==1:
            return self._destination[0]
        return self._destination
        

    @classmethod
    def deserialize(cls, props):

        source = [datasets.new_dataset(d['type'], d['path']) for d in props['source']]
        destination = [datasets.new_dataset(d['type'], d['path']) for d in props['destination']]

        instance = cls(source, destination)
        for attr in instance._serializable_attrs:
            setattr(instance, attr, props.get(attr))

        return instance


    def serialize(self):

        props = {}
        for attr in self._serializable_attrs:
            props[attr] = getattr(self, attr)

        props['source'] = [{'type': d.type, 'path': d.path} for d in self._source]
        props['destination'] = [{'type': d.type, 'path': d.path} for d in self._destination]
        
        return props