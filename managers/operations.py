
import json
from . import datasets


class Operation(object):

    _serializable_attrs = ['method', 'kwargs', 'commit']


    def __repr__(self):
        return json.dumps(self.serialize())


    def __init__(self, source, destination, method, kwargs, commit):

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


    @classmethod
    def deserialize(cls, props):

        source = [datasets.new_dataset(d['type'], d['path']) for d in props['source']]
        destination = datasets.new_dataset(props['destination']['type'], props['destination']['path'])

        return cls(source, destination, props.get('method'), props.get('kwargs'), props.get('commit'))


    def serialize(self):

        props = {}
        for attr in self._serializable_attrs:
            props[attr] = getattr(self, attr)

        props['source'] = [{'type': s.type, 'path': s.path} for s in self.source]
        props['destination'] = {'type': self.destination.type, 'path': self.destination.path}
        
        return props