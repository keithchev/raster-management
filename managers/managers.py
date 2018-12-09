
import os
import re
import sys
import glob
import json
import rasterio
import subprocess

import numpy as np

from . import utils
from . import datasets

    
class RasterProject(object):
    
    # hard-coded path to rasterio CLI
    rio = '/home/keith/anaconda3/envs/gdalenv/bin/rio'
    
    # hard-coded output options
    opts = '--overwrite --driver GTiff --co tiled=false'


    def __init__(self, location, reset=False):

        if not os.path.isdir(location):
            os.makedirs(location)
            
        self.location = location 
        self.name = os.path.split(self.location)[-1]

        self.props_filename = os.path.join(self.location, 'props.json')

        self.props = {
            'location': self.location
        }

        if os.path.exists(self.props_filename):
            if reset:
                os.remove(self.props_filename)
            else:
                self._load_from_props()


    def _load_from_props(self):

        with open(self.props_filename, 'r') as file:
            props = json.load(file)

        self.define_sources(**props['sources'])
        self.initialize(props['initialization']['res'], props['initialization']['bounds'])


    def save_props(self):

        with open(self.props_filename, 'w') as file:
            json.dump(self.props, file)


    def define_sources(self, locations, source_type=None, root=None):
        '''
        Define the raw dataset(s) from which we will generate the derived dataset
        
        source_type: one of 'tif', 'ned', 'landsat'
        locations: path or list of paths 
        (to either tifs, NED13 tile directories, or landsat scene directories)

        '''

        if type(locations) is str:
            locations = [locations]

        if root is not None:
            locations = [os.path.join(root, location) for location in locations]

        # attempt to infer the source_type (TODO: add logic for NED13)
        if source_type is None:
            if os.path.isdir(locations[0]):
                source_type = 'landsat'
            else:
                source_type = 'tif'
    
        self.source_type = source_type
        self.sources = [datasets.new_dataset(source_type)(location) for location in locations]

        self.props['sources'] = {
            'type': self.source_type,
            'locations': [s.location for s in self.sources],
        }

        if source_type=='landsat':
            location = os.path.join(self.location, self.name)
        else:
            location = os.path.join(self.location, '%s.TIF' % self.name)

        # placeholder for the derived dataset
        self.derived_dataset = datasets.new_dataset(source_type)(location, exists=False)    
    


    def initialize(self, res=None, bounds=None):
        '''
        Initialize the dataset by merging the raw datasets

        '''

        for band in self.derived_dataset.expected_bands:
    
            srs = ' '.join([source.filepath(band) for source in self.sources])
            dst = self.derived_dataset.filepath(band)
            
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



class DEMProject(RasterProject):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    def hillshade(self):

        source = self.derived_dataset.filepath()
        destination = source.replace('.TIF', '_HS.TIF')
        utils.shell('gdaldem hillshade %s %s' % (source, destination))




