
import os
import re
import sys
import glob
import json

import numpy as np


def new_dataset(dataset_type, path, **kwargs):

    dataset_types = {
        'landsat': 'LandsatScene',
        'ned': 'NED13Tile',
        'tif': 'GeoTIFF',
    }

    if dataset_type not in dataset_types:
        raise ValueError('%s is not a valid dataset type' % dataset_type)

    dataset = getattr(sys.modules[__name__], dataset_types[dataset_type])
    dataset = dataset(path, **kwargs)
    return dataset



class Dataset(object):
    '''
    Generic dataset

    Either a single TIF or a set of TIFs (bands) in a single directory
    '''

    def __init__(self, path, is_raw=False, exists=False):

        # type must be hard-coded in subclasses
        self.type = None

        # remove any trailing slashes
        path = re.sub(r'%s*$' % os.sep, '', path)

        # path is either the path to a TIFF *file* or to a Landsat/NED13 *directory*
        self.path = path

        # whether the dataset is raw (for reference/sanity checks only)
        self.is_raw = is_raw

        # whether the dataset already exists
        self.exists = exists

        # the bands we expect the dataset to have
        # (only not [None] for Landsat datasets)
        self.expected_bands = [None]

        # the panchromatic band (for Landsat datasets only)
        self.pan_band = None



class GeoTIFF(Dataset):
    
    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)

        self.type = 'tif'

        base, ext = os.path.splitext(self.path)

        # the dataset name is the filename itself
        self.name = base.split(os.sep)[-1]

        # placeholder for the single 'band'
        # (for consistency with Landsat dataset API)
        self.bands = self.expected_bands

        # the path doesn't need to include the extension
        # (if it doesn't, and exists=True, we assume that it's '.TIF')
        if not ext:
            ext = '.TIF'
            self.path += ext

        if ext.lower() not in ['.tif', '.tiff']:
            raise ValueError('%s is not a TIFF file' % self.path)

        if self.exists:
            self._validate()


    def _validate(self):

        if not os.path.isfile(self.path):
            raise FileNotFoundError('%s does not exist' % self.path)


    def bandpath(self, band=None):
        '''
        For TIFF files, there is only one 'band' (the TIFF file),
        and the path to it is the same as self.path
        (band=None ensures consistency with LandsatScene.bandpath)
        '''
        return self.path



class NED13Tile(Dataset):
    pass



class LandsatScene(Dataset):
    
    def __init__(self, path, satellite=None, **kwargs):
        super().__init__(path, **kwargs)

        self.type = 'landsat'

        # satellite type: 'L8', 'L7', or 'L5'
        if satellite is None:
            satellite = 'L8'

        self.satellite = satellite

        bands = {
            'L8': range(1, 12),
            'L7': range(1, 9),
            'L5': range(1, 8),
        }

        pan_bands = {
            'L8': '8',
            'L7': '7',
            'L5': None,
        }

        self.pan_band = pan_bands[self.satellite]
        self.expected_bands = set(map(str, bands[self.satellite]))

        if self.exists:         
            if not os.path.isdir(self.path):
                raise FileNotFoundError('%s is not a directory' % self.path)
        else:
            os.makedirs(self.path, exist_ok=True)

        # the dataset name is the directory name
        self.name = os.path.split(self.path)[-1]
        
        # find the filename for each band
        self._bandpaths = {}
        if self.exists:    
            bandpaths = glob.glob(os.path.join(self.path, '*.TIF'))
            for bandpath in bandpaths:
                result = re.search('_B([0-9]+).TIF$', bandpath)
                if result:
                    band = result.groups()[0]
                    self._bandpaths[band] = bandpath
                else:
                    print('Warning: ignoring unexpected filename %s' % bandpath.split(os.sep)[-1])
            
            self._validate()

    
    def _validate(self):
        
        # check for expected and unexpected bands
        existing_bands = set(self._bandpaths.keys())
        missing_bands = self.expected_bands.difference(existing_bands)
        unexpected_bands = existing_bands.difference(self.expected_bands)
        
        if missing_bands:
            print('Warning: expected bands %s were not found' % \
                  sorted(list(missing_bands)))
        
        if unexpected_bands:
            print('Warning: found unexpected bands: %s' % \
                  sorted(list(unexpected_bands)))

        # verify that filenames are consistent
        paths = [path.replace('_B%s.TIF' % band, '') for band, path in self._bandpaths.items()]
            
        if len(set(paths))!=1:
            raise ValueError('Filename roots are not consistent')


    @property
    def bands(self):
        return sorted(list(self._bandpaths.keys()))

    
    def bandpath(self, band=None):
        
        if band is None:
            raise ValueError('A band must be provided')

        if type(band) is int:
            band = str(band)
    
        if self.exists:
            bandpath = self._bandpaths.get(band)
            if bandpath is None:
                raise FileNotFoundError('B%s does not exist for scene %s' % (band, self.name))
        else:
            bandpath = os.path.join(self.path, '%s_B%s.TIF' % (self.name, band))
            
        return bandpath
