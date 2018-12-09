
import os
import re
import sys
import glob
import json
import hashlib
import datetime
import rasterio
import subprocess

import numpy as np

from . import utils
from . import datasets


def log_operation(method):

    def wrapper(self, source, **kwargs):
        
        output = method(self, source, **kwargs)

        self.props['operations'].append({
            'commit': utils.current_commit(),
            'method': method.__name__, 
            'source': source.path,
            'output': output.path,
            'kwargs': kwargs,
        })

    return wrapper


class RasterProject(object):
    
    # hard-coded path to rasterio CLI
    rio = '/home/keith/anaconda3/envs/gdalenv/bin/rio'
    
    # hard-coded output options
    opts = '--overwrite --driver GTiff --co tiled=false'


    def __init__(self, project_root, raw_datasets=None, res=None, bounds=None, reset=False):

        if not os.path.isdir(project_root):
            os.makedirs(project_root)
            
        self.root = project_root 
        self.name = os.path.split(self.root)[-1]

        # path to the derived dataset
        self.derived_dataset_filepath = os.path.join(self.root, self.name)

        # placeholder for the derived dataset
        self.derived_dataset = datasets.new_dataset(self.dtype, self.derived_dataset_filepath)

        self.props_filepath = os.path.join(self.root, 'props.json')

        # if there are cached props and we are not resetting
        if os.path.exists(self.props_filepath) and not reset:
            print('Loading from existing project')

            if res is not None or bounds is not None:
                print('Warning: res and bounds are ignored when loading an existing dataset')

            with open(self.props_filepath, 'r') as file:
                self.props = json.load(file)                        

            self.define_raw_datasets(self.props['raw_datasets'])

            # validate the derived dataset
            self.validate_derived_dataset(
                res=self.props['derived_dataset']['res'], 
                bounds=self.props['derived_dataset']['bounds'])

        # assume raw_datasets, res, and bounds are defined
        else:
            print('Generating new project')

            self.props = {
                'root': self.root,
                'operations': []
            }

            self.define_raw_datasets(raw_datasets)
            self.generate_derived_dataset(res, bounds)
            self.save_props()


    def save_props(self):
        with open(self.props_filepath, 'w') as file:
            json.dump(self.props, file)


    def new_dataset(self, dtype=None, method=None):
        '''
        '''

        timestamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d_%H%M%S')
        name = '%s_%s_%s' % (self.name, method, timestamp)
        path = os.path.join(self.root, name)

        return datasets.new_dataset(dtype, path, exists=False)



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
            datasets.new_dataset(self.dtype, path, is_raw=True, exists=True) for path in paths]

        self.props['raw_datasets'] = [dataset.path for dataset in self.raw_datasets]



    def generate_derived_dataset(self, res=None, bounds=None):
        '''
        Initialize the derived dataset by merging the raw datasets

        '''

        for band in self.derived_dataset.expected_bands:
    
            srs = ' '.join([raw_dataset.bandpath(band) for raw_dataset in self.raw_datasets])
            dst = self.derived_dataset.bandpath(band)
            
            command = '%s merge %s' % (self.rio, self.opts)

            if res:
                command += ' -r %s' % res

            if bounds:
                command += ' --bounds "%s"' % bounds

            command += ' %s %s' % (srs, dst)
            utils.shell(command, verbose=False)

        # the derived dataset now exists
        self.derived_dataset = datasets.new_dataset(self.dtype, self.derived_dataset.path, exists=True)

        self.props['derived_dataset'] = {
            'res': res,
            'bounds': bounds,
            'path': self.derived_dataset.path,
            'commit': utils.current_commit(),
        }


    def validate_derived_dataset(self, res=None, bounds=None):
        '''
        validate an existing derived dataset by verifying that its resolution and bounds
        are equal to `res` and `bounds`
        '''

        with rasterio.open(self.derived_dataset.bandpath(1)) as src:

            if res is not None and set(src.res)!=set([res]):
                raise ValueError(
                    'The resolution of the existing dataset is %s but a resolution of %s was provided' % \
                        (src.res, res))

            if bounds is not None and (np.abs(np.array(src.bounds) - bounds).max() > 1000):
                raise ValueError(
                    'The bounds of the existing dataset are %s but bounds of %s were provided' % \
                        (tuple(src.bounds), bounds))

            print('Found existing dataset with res=%s and bounds=%s' % (src.res, tuple(src.bounds)))    



class LandsatProject(RasterProject):

    def __init__(self, *args, **kwargs):
        
        self.dtype = 'landsat'
        super().__init__(*args, **kwargs)


    @log_operation
    def stack(self, source, bands=[4,3,2]):

        bands = list(map(str, bands))
        destination = self.new_dataset(dtype='tif', method='stack')

        utils.shell('%s stack --overwrite --rgb %s %s' % \
              (self.rio, ' '.join([source.bandpath(b) for b in bands]), destination.path))

        return destination


    @log_operation
    def autogain(self, source, dtype='uint8', percentile=1):

        destination = self.new_dataset(dtype='tif', method='autogain')

        for band in source.bands:
            with rasterio.open(source.bandpath(band), 'r'):
                pass




class DEMProject(RasterProject):

    def __init__(self, *args, **kwargs):

        self.dtype = 'tif'
        super().__init__(*args, **kwargs)


    @log_operation
    def hillshade(self, source):

        destination = self.new_dataset(dtype='tif', method='hillshade')
        utils.shell('gdaldem hillshade %s %s' % (source.path, destination.path))
        return destination




