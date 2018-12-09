
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


    def __init__(self, root, reset=False):

        if not os.path.isdir(root):
            os.makedirs(root)
            
        self.root = root 
        self.name = os.path.split(self.root)[-1]

        self.props_filename = os.path.join(self.root, 'props.json')

        self.props = {
            'root': self.root,
            'operations': []
        }

        if os.path.exists(self.props_filename):
            if reset:
                os.remove(self.props_filename)
            else:
                self._load_from_props()


    def _load_from_props(self):

        with open(self.props_filename, 'r') as file:
            props = json.load(file)

        # define the raw datasets
        self.define_sources(**props['sources'])

        # generate the derived dataset
        self.initialize(props['initialization']['res'], props['initialization']['bounds'])


    def save_props(self):
        with open(self.props_filename, 'w') as file:
            json.dump(self.props, file)


    def new_dataset(self, dtype=None, method=None):
        '''
        '''

        timestamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d_%H%M%S')
        dataset_name = '%s_%s_%s' % (self.name, method, timestamp)
        dataset_path = os.path.join(self.root, dataset_name)

        return datasets.new_dataset(dtype)(dataset_path, exists=False)


    def define_sources(self, paths, source_type=None, source_root=None):
        '''
        Define the raw dataset(s) from which we will generate the derived dataset
        
        source_type: one of 'tif', 'ned', 'landsat'
        filepaths: path or list of paths to the raw datasets
        (either tifs, NED13 tile directories, or landsat scene directories)

        '''

        if type(paths) is str:
            paths = [paths]

        if source_root is not None:
            paths = [os.path.join(source_root, path) for path in paths]

        # attempt to infer the source_type (TODO: add logic for NED13)
        if source_type is None:
            if os.path.isdir(paths[0]):
                source_type = 'landsat'
            else:
                source_type = 'tif'
    
        self.source_type = source_type
        self.sources = [datasets.new_dataset(source_type)(path) for path in paths]

        self.props['sources'] = {
            'source_type': self.source_type,
            'paths': [source.path for source in self.sources],
        }


    def initialize(self, res=None, bounds=None):
        '''
        Initialize the derived dataset by merging the raw datasets

        '''

        # location of the derived dataset
        filepath = os.path.join(self.root, self.name)

        # placeholder for the derived dataset
        derived_dataset = datasets.new_dataset(self.source_type)(filepath, exists=False)    
    
        for band in derived_dataset.expected_bands:
    
            srs = ' '.join([source.bandpath(band) for source in self.sources])
            dst = derived_dataset.bandpath(band)
            
            command = '%s merge %s' % (self.rio, self.opts)

            if res:
                command += ' -r %s' % res

            if bounds:
                command += ' --bounds "%s"' % bounds

            command += ' %s %s' % (srs, dst)

            if not os.path.isfile(dst):
                utils.shell(command, verbose=False)

        self.props['initialization'] = {
            'res': res,
            'bounds': bounds,
            'commit': utils.current_commit(),
        }

        self.save_props()
        self.derived_dataset = datasets.new_dataset(self.source_type)(filepath, exists=True)



class LandsatProject(RasterProject):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    @log_operation
    def stack(self, source, bands=[4,3,2]):

        bands = list(map(str, bands))
        destination = self.new_dataset(dtype='tif', method='stack')

        utils.shell('%s stack --overwrite --rgb %s %s' % \
              (self.rio, ' '.join([source.bandpath(b) for b in bands]), destination.path))

        return destination


class DEMProject(RasterProject):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    @log_operation
    def hillshade(self, source):

        destination = self.new_dataset(dtype='tif', method='hillshade')
        utils.shell('gdaldem hillshade %s %s' % (source.path, destination.path))
        return destination




