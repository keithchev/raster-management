
import os
import re
import sys
import glob
import json
import deepdiff
import datetime
import rasterio
import subprocess

import numpy as np

from . import utils
from . import datasets
from . import settings


class Operation(object):

    def __init__(self):
        pass

    def __repr__(self):
        return json.dumps(self.serialize())


    def from_json(self, op):

        for attr in ['method', 'kwargs', 'commit']:
            setattr(self, attr, op[attr])

        self.sources = [datasets.new_dataset(source['type'], source['path'], exists=True)
            for source in op['sources']]

        destination = op['destination']
        self.destination = datasets.new_dataset(destination['type'], destination['path'], exists=True)


    def from_args(self, sources, destination, method, kwargs, commit):

        self.sources = sources
        self.destination = destination

        self.method = method
        self.kwargs = kwargs
        self.commit = commit


    def serialize(self):

        sources = []
        for source in self.sources:
            sources.append({
                'type': source.type,
                'path': source.path
            })

        op = {
            'method': self.method,
            'kwargs': self.kwargs,
            'commit': self.commit,
            'sources': sources,
            'destination': {'type': self.destination.type, 'path': self.destination.path}
        }

        return op



def log_operation(method):

    def wrapper(self, source, **kwargs):

        destination = method(self, source, **kwargs)

        sources = source
        if not isinstance(sources, list):
            sources = [sources]
        
        operation = Operation()
        operation.from_args(
            destination=destination,
            sources=sources,
            kwargs=kwargs,
            method=method.__name__, 
            commit=utils.current_commit()
        )

        self.operations.append(operation)
        self.save_props()

    return wrapper



class RasterProject(object):
    
    # path to rasterio CLI
    rio = settings.RIO_CLI

    # hard-coded output options
    opts = '--overwrite --driver GTiff --co tiled=false'


    def __init__(self, project_root, raw_datasets=None, res=None, bounds=None, reset=False):
        '''
        project_root: path to the project directory
        raw_datasets: a list of paths to either TIF files or Landsat scene directories
        
        '''

        # raw dataset type must be hard-coded in subclasses
        assert hasattr(self, 'raw_dataset_type')

        if not os.path.isdir(project_root):
            os.makedirs(project_root)
        
        self.root = re.sub(r'%s*$' % os.sep, '', project_root)
        self.name = os.path.split(self.root)[-1]

        self.props_path = os.path.join(self.root, 'props.json')

        # initialize props object
        self.props = {
            'root': self.root,
            'operations': []
        }

        # if there are cached props and we are not resetting
        if os.path.exists(self.props_path) and not reset:
            print('Loading from existing project')

            if res is not None or bounds is not None:
                print('Warning: res and bounds are ignored when loading an existing dataset')

            with open(self.props_path, 'r') as file:
                cached_props = json.load(file)                        

            self.define_raw_datasets(cached_props['raw_datasets']['paths'])

            self.operations = self.deserialize_operations(cached_props['operations'])
            self.validate_operations()

            # check that the cached operations match the newly serialized operations
            serialized_operations = self.serialize_operations()
            diff = deepdiff.DeepDiff(cached_props['operations'], serialized_operations, report_repetition=True)
            if diff:
                print('WARNING: cached serialized operations are not reproducible')
                print(diff)

        else:
            if raw_datasets is None:
                raise ValueError('The argument `raw_datasets` must be provided')

            print('Generating new project')
            self.define_raw_datasets(raw_datasets)

            self.operations = []
            self.merge(self.raw_datasets, res=res, bounds=bounds)



    def serialize_operations(self):
        return [operation.serialize() for operation in self.operations]



    @staticmethod
    def deserialize_operations(serialized_operations):
        operations = []
        for op in serialized_operations:
            operation = Operation()
            operation.from_json(op)
            operations.append(operation)

        return operations



    def save_props(self):

        self.props['operations'] = self.serialize_operations()

        with open(self.props_path, 'w') as file:
            json.dump(self.props, file)



    def get_operation(self, index):

        if index=='last':
            pass

        if index=='first':
            pass

        if type(ind) is int:
            pass



    def _new_dataset(self, dataset_type=None, method=None):
        '''
        Generate a new output dataset given a method name
        
        Note that we use the timestamp as a primitive kind of hash to guarantee a unique filename
        '''

        timestamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d_%H%M%S')
        name = '%s_%s_%s' % (self.name, method, timestamp)
        path = os.path.join(self.root, name)
        return datasets.new_dataset(dataset_type, path, exists=False)



    def define_raw_datasets(self, paths, root=None):
        '''
        Define the raw dataset(s) from which we will generate the derived dataset
        
        path: path or list of paths to the raw datasets
        (either tifs, NED13 tile directories, or landsat scene directories)

        '''

        if type(paths) is str:
            paths = [paths]

        if root is not None:
            paths = [os.path.join(root, path) for path in paths]
    
        self.raw_datasets = [
            datasets.new_dataset(self.raw_dataset_type, path, is_raw=True, exists=True) for path in paths]

        self.props['raw_datasets'] = {
            'dataset_type': self.raw_dataset_type,
            'paths': [dataset.path for dataset in self.raw_datasets],
        }


    @log_operation
    def merge(self, source, res=None, bounds=None):
        '''
        Create the root dataset by merging and/or cropping the raw dataset(s).

        Note that this method is the only way to create a root derived dataset;
        that is, the source must be equal to self.raw_datasets,
        and the root/first operation in self.operations must be a call to this method.
        
        For now, we don't enforce these conventions. 
        '''

        assert isinstance(source, list)        
        sources = source

        destination = self._new_dataset(self.raw_dataset_type, method='merge')

        for band in destination.expected_bands:
    
            srs = ' '.join([source.bandpath(band) for source in sources])
            dst = destination.bandpath(band)
            
            command = '%s merge %s' % (self.rio, self.opts)

            if res:
                command += ' -r %s' % res

            if bounds:
                command += ' --bounds "%s"' % bounds

            command += ' %s %s' % (srs, dst)
            utils.shell(command, verbose=False)

        return destination



    def validate_operations(self):
        '''
        Validate operations in self.operations by checking that operation.kwargs are consistent
        with the geoTIFF metadata, to the extent that we can. 
        
        For now, we only check that the `res` and `bounds` of the initial merge operation
        are consistent with the root derived dataset.

        '''

        # the first operation must be a merge
        operation = self.operations[0]
        assert(operation.method=='merge')

        res, bounds = operation.kwargs['res'], operation.kwargs['bounds']

        with rasterio.open(operation.destination.bandpath(1)) as src:

            # tolerance for comparing actual to expected bounds
            tolerance = max(src.res)*2

            if res is not None and set(src.res)!=set([res]):
                raise ValueError(
                    'The resolution of the existing root dataset is %s but a resolution of %s was provided' % \
                        (src.res, res))

            if bounds is not None and np.any(np.abs(np.array(src.bounds) - bounds) > tolerance):
                raise ValueError(
                    'The bounds of the existing root dataset are %s but bounds of %s were provided' % \
                        (tuple(src.bounds), bounds))

            print('Found root dataset with res=%s and bounds=%s' % (src.res, tuple(src.bounds)))    



class LandsatProject(RasterProject):

    def __init__(self, *args, **kwargs):
        
        self.raw_dataset_type = 'landsat'
        super().__init__(*args, **kwargs)


    @log_operation
    def stack(self, source, bands=[4,3,2]):
        '''
        source: a Landsat dataset
        '''

        bands = list(map(str, bands))
        destination = self._new_dataset('tif', method='stack')

        utils.shell('%s stack --overwrite --rgb %s %s' % \
              (self.rio, ' '.join([source.bandpath(b) for b in bands]), destination.path))

        return destination


    @log_operation
    def autogain(self, source, percentile=None, each_band=True):
        '''
        Autogain an RGB image

        source: dataset object representing an RGB geoTIFF
        
        '''

        # hard-coded 'uint8' dtype for now
        dtype = 'uint8'
        dtype_max = 255

        # default to min/max
        if percentile is None:
            percentile = 100

        # destination dataset
        destination = self._new_dataset('tif', method='autogain')
                
        def _autogain(im, percentile):
            minn, maxx = np.percentile(im[:], [100 - percentile, percentile])
            im -= minn
            im /= (maxx - minn)
            im[im < 0] = 0
            im[im > 1] = 1
            return im

        with rasterio.open(source.path) as src:
            dst_profile = src.profile
            dst_profile['dtype'] = dtype

            with rasterio.open(destination.path, 'w', **dst_profile) as dst:
                if each_band:
                    im_dst = np.zeros((len(src.indexes),) + src.shape)
                    for ind, band in enumerate(src.indexes):
                        im = src.read(band).astype('float64')
                        im_dst[ind, :, :] = _autogain(im, percentile)
                else:
                    im = src.read().astype('float64')
                    im_dst = _autogain(im, percentile)

                im_dst *= dtype_max
                im_dst = im_dst.astype(dtype)
                dst.write(im_dst)

        return destination



class DEMProject(RasterProject):

    def __init__(self, *args, **kwargs):

        self.raw_dataset_type = 'tif'
        super().__init__(*args, **kwargs)


    @log_operation
    def hillshade(self, source):

        destination = self._new_dataset('tif', method='hillshade')
        utils.shell('gdaldem hillshade %s %s' % (source.path, destination.path))
        return destination




