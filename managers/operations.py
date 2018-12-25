
import json
from . import datasets


class Operation(object):

    _serializable_attrs = ['method', 'kwargs', 'commit']

    def __init__(self, exists=False):
        self.exists = exists


    def __repr__(self):
        return json.dumps(self.serialize())


    def create(self, source, destination, method, kwargs, commit):

        # force source to list
        if not isinstance(source, list):
            source = [source]

        for dataset in source + [destination]:
            assert isinstance(dataset, datasets.Dataset)

        self.source = source
        self.destination = destination

        self.method = method
        self.kwargs = kwargs
        self.commit = commit

        return self


    def deserialize(self, props):

        for attr in self._serializable_attrs:
            setattr(self, attr, props.get(attr))

        source = props['source']
        destination = props['destination']

        self.source = [datasets.new_dataset(d['type'], d['path'], exists=self.exists) for d in source]
        self.destination = datasets.new_dataset(destination['type'], destination['path'], exists=self.exists)

        return self


    def serialize(self):

        props = {}
        for attr in self._serializable_attrs:
            props[attr] = getattr(self, attr)

        props['source'] = [{'type': s.type, 'path': s.path} for s in self.source]
        props['destination'] = {'type': self.destination.type, 'path': self.destination.path}
        
        return props