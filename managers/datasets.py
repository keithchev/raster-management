
import os
import re
import sys
import glob
import json

import numpy as np


def new_dataset(dataset_type):

    dataset_types = {
        'landsat': 'LandsatScene',
        'ned': 'NED13Tile',
        'tif': 'GeoTIFF',
    }

    if dataset_type not in dataset_types:
        raise ValueError('%s is not a valid source_type' % dataset_type)

    return getattr(sys.modules[__name__], dataset_types[dataset_type])



class Dataset(object):
    '''
    Generic dataset

    Either a single TIF or a set of TIFs (bands) in a single directory
    '''

    def __init__(self, location, is_raw=True, exists=True):

        # remove any trailing slashes
        location = re.sub(r'%s*$' % os.sep, '', location)

        # location is either a filepath (for single TIFs) 
        # or a directory (for Landsat scenes)
        self.location = location

        # whether the dataset is raw
        self.is_raw = is_raw

        # whether the dataset already exists
        self.exists = exists

        # the bands we expect the dataset to have
        # (only not [None] for Landsat datasets)
        self.expected_bands = [None]



class GeoTIFF(Dataset):
    
    def __init__(self, location, is_raw=True, exists=True):
        super().__init__(location, is_raw, exists)

        path, ext = os.path.splitext(self.location)

        # the location doesn't need to include the extension
        # (if it doesn't, and exists=True, we assume that it's '.TIF')
        if not ext:
            ext = '.TIF'
            self.location += ext

        if ext.lower() not in ['.tif', '.tiff']:
            raise ValueError('%s is not a TIFF file' % self.location)

        if self.exists and not os.path.isfile(self.location):
            raise ValueError('%s is not a file' % self.location)

        # the dataset name is the filename itself
        self.name = path.split(os.sep)[-1]


    def filepath(self, *args):
        '''
        filepath to the TIFF file
        (*args ensures consistency with LandsatScene.filepath)
        '''
        return self.location



class NED13Tile(Dataset):
    pass



class LandsatScene(Dataset):
    
    def __init__(self, location, is_raw=True, exists=True, satellite=None):
        super().__init__(location, is_raw, exists)

        # satellite type: 'L8', 'L7', or 'L5'
        if satellite is None:
            satellite = 'L8'

        self.satellite = satellite

        satellite_bands = {
            'L8': range(1, 12),
            'L7': range(1, 9),
            'L5': range(1, 8),
        }

        self.expected_bands = set(map(str, satellite_bands[self.satellite]))
        
        if self.exists:         
            if not os.path.isdir(self.location):
                raise ValueError('%s is not a directory' % self.location)
        else:
            os.makedirs(self.location, exist_ok=True)

        # the dataset name is the directory name
        self.name = os.path.split(self.location)[-1]
        
        # find the filename for each band
        self._band_filepaths = {}
        if self.exists:    
            filepaths = glob.glob(os.path.join(location, '*.TIF'))
            for filepath in filepaths:
                result = re.search('_B([0-9]+).TIF$', filepath)
                if result:
                    band = result.groups()[0]
                    self._band_filepaths[band] = filepath
                else:
                    print('Warning: unexpected filename %s in scene %s' % \
                          (filepath.split(os.sep)[-1], self.name))
            
            self._validate()

    
    def _validate(self):
        
        # check for expected and unexpected bands
        existing_bands = set(self._band_filepaths.keys())
        missing_bands = self.expected_bands.difference(existing_bands)
        unexpected_bands = existing_bands.difference(self.expected_bands)
        
        if missing_bands:
            print('Warning: expected bands %s were not found' % \
                  sorted(list(missing_bands)))
        
        if unexpected_bands:
            print('Warning: found unexpected bands: %s' % \
                  sorted(list(unexpected_bands)))

        # verify that filenames are consistent
        filenames = [filename.replace('_B%s.TIF' % band, '') 
            for band, filename in self._band_filepaths.items()]
            
        if len(set(filenames))!=1:
            raise ValueError('Filename roots are not consistent')


    @property
    def bands(self):
        return sorted(list(self._band_filepaths.keys()))

    
    def filepath(self, band=None):
        
        if band is None:
            raise ValueError('band must be specified for Landsat datasets')

        if type(band) is int:
            band = str(band)
        
        if self.exists:
            filepath = self._band_filepaths.get(band)
            if filepath is None:
                raise ValueError('B%s does not exist for scene %s' % (band, self.name))
        else:
            filepath = os.path.join(self.location, '%s_B%s.TIF' % (self.name, band))
            
        return filepath
