
import json
from . import datasets


class Operation(object):

    _serializable_attrs = ['method', 'kwargs', 'commit']


    def __repr__(self):
        return 'Operation(\n\tmethod=\'%s\', \n\tkwargs=%s, \n\tsource=\'%s\', \n\tdestination=\'%s\')' % \
            (self.method, self.kwargs, self._source[0].path, self._destination[0].path)


    def __init__(self, source, destination, method, kwargs, commit):

        # note: source is sometimes a single Dataset and sometimes a list of Datasets
        # for consistency, we force the internal _source and _destination attributes to lists
        # but allow the public attributes to be either single Datasets or a list of Datasets
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

        return cls(source, destination, props.get('method'), props.get('kwargs'), props.get('commit'))


    def serialize(self):

        props = {}
        for attr in self._serializable_attrs:
            props[attr] = getattr(self, attr)

        props['source'] = [{'type': d.type, 'path': d.path} for d in self._source]
        props['destination'] = [{'type': d.type, 'path': d.path} for d in self._destination]
        
        return props