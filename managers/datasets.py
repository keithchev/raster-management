
import os
import re
import sys
import glob
import json

import numpy as np


def new_dataset(dataset_type, path, **kwargs):

    dataset_types = {
        'landsat': 'LandsatScene',
        'ned13': 'NED13Tile',
        'tif': 'GeoTIFF',
        'goes': 'GOESScene'
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
        # ([0] for band-less datasets like GeoTIFF and NED13Tile))
        self.expected_bands = [0]

        # the relative resolution of each band
        # (for Landsat panchromatic band and GOESR bands)
        self.rel_band_res = {}


class GeoTIFF(Dataset):
    
    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)

        self.type = 'tif'

        base, ext = os.path.splitext(self.path)

        # the dataset name is the filename itself
        self.name = base.split(os.sep)[-1]

        # the path doesn't need to include the extension
        # (if it doesn't, and exists=True, we assume that it's '.TIF')
        if not ext:
            ext = '.TIF'
            self.path += ext

        if ext.lower() not in ['.tif', '.tiff']:
            raise ValueError('%s is not a TIFF file' % self.path)

        if self.exists:
            if not os.path.isfile(self.path):
                raise FileNotFoundError('%s does not exist' % self.path)


    def filepath(self, band=None):
        '''
        For TIFF files, there is only one 'band' (the TIFF file),
        and the path to it is the same as self.path
        (band=None ensures consistency with LandsatScene.filepath)
        '''
        return self.path



class NED13Tile(Dataset):

    def __init__(self, path, exists=False):
        '''
        Note that NED13 tile datasets are always raw;
        that is, we cannot create them, only read them.

        However, though we never create these datasets, they may not exist,
        since we will sometimes deserialize an existing project 
        that was generated from NED13 tiles on a now-disconnected external/remote drive
        '''

        super().__init__(path, is_raw=True, exists=exists)

        self.type = 'ned13'

        # the dataset name is the tile directory name
        self.name = self.path.split(os.sep)[-1]

        # only attempt to find the .adf file if the dataset exists
        if self.exists:

            # the adf file is always in the subdirectory beginning with 'grd'
            subdir = [s for s in glob.glob(os.path.join(self.path, 'grd*')) if os.path.isdir(s)]

            if len(subdir)!=1:
                raise ValueError('No grdn subdir in %s' % self.path)

            # the adf file itself always has the same name
            self.adf_path = os.path.join(self.path, subdir[0], 'w001001.adf')

            # fingers crossed...
            if not os.path.isfile(self.adf_path):
                raise FileNotFoundError('%s does not exist' % self.adf_path)


    def filepath(self, band=None):
        return self.adf_path


class LandsatScene(Dataset):
    
    def __init__(self, path, satellite=None, **kwargs):
        super().__init__(path, **kwargs)

        self.type = 'landsat'

        # satellite type: 'L8', 'L7', or 'L5'
        if satellite is None:
            satellite = 'L8'
        self.satellite = satellite

        expected_bands = {
            'L8': range(1, 12),
            'L7': range(1, 9),
            'L5': range(1, 8),
        }

        pan_band = {
            'L8': 8,
            'L7': 7,
            'L5': None,
        }

        self.expected_bands = expected_bands[self.satellite]

        # set the relative res for the pan band
        pan_band = pan_band.get(self.satellite)
        if pan_band is not None:
            self.rel_band_res[pan_band] = .5

        if self.exists and not os.path.isdir(self.path):
            raise FileNotFoundError('%s is not a directory' % self.path)
        os.makedirs(self.path, exist_ok=True)

        # the dataset name is the directory name
        self.name = os.path.split(self.path)[-1]
        
        # find the existing bands
        self.extant_bands = []
        if self.exists:    
            filepaths = glob.glob(os.path.join(self.path, '*.TIF'))
            for filepath in filepaths:
                result = re.search('%s_B([0-9]+).TIF$' % self.name, filepath)
                if result:
                    band = int(result.groups()[0])
                    self.extant_bands.append(band)
                else:
                    print('Warning: ignoring unexpected filename %s' % \
                        filepath.split(os.sep)[-1])
 
            self._validate()
            self.extant_bands = sorted(self.extant_bands)

    
    def _validate(self):
        
        # check for expected and unexpected bands)
        missing_bands = set(self.expected_bands).difference(self.extant_bands)
        unexpected_bands = set(self.extant_bands).difference(self.expected_bands)
        
        if missing_bands:
            print('Warning: expected bands %s were not found' % \
                  sorted(list(missing_bands)))
        
        if unexpected_bands:
            print('Warning: found unexpected bands: %s' % \
                  sorted(list(unexpected_bands)))


    def filepath(self, band=None):
        '''
        Construct the filepath given a band (and the scene name)
        (Note that filepath construction is the same for raw data and derived data)
        '''
        
        if band is None:
            raise ValueError('A band must be provided')

        filepath = os.path.join(self.path, '%s_B%s.TIF' % (self.name, band))    
        if self.exists and not os.path.isfile(filepath):
            raise FileNotFoundError('B%s does not exist for scene %s' % (band, self.name))
        return filepath



class GOESScene(Dataset):
    '''
    Filename convention for GOES ABI L1b data
    (see https://www.goes-r.gov/users/docs/PUG-L1b-vol3.pdf)

    Example filename:
    'OR_ABI-L1b-RadC-M6C03_G17_s20192431401196_e20192431403569_c20192431404008.nc'

    OR       Operational real-time data
    ABI-L1b  Advanced Baseline Imager Level 1b
    Rad      ABI radiances
    C        Image type ('C' CONUS, 'M' Mesoscale, 'F' Full Disk)
    M6       Scan mode (M3, M4, or M6)
    C03      Band number
    G17      GOES-17

    Timestamps (year, day of year, hour, minute, second, tenth second)
    sYYYYJJJHHMMSSZ - Scan start
    eYYYYJJJHHMMSSZ - Scan end
    cYYYYJJJHHMMSSZ - File creation

    Band details (for visible and near-IR bands only)
    -------------------------------------------------
    Band  Res (km)   Wavelength   Spectrum    Name
    01	  1          0.47µm	      Visible	  Blue
    02	  0.5        0.64µm	      Visible	  Red
    03	  1	         0.86µm	      Near-IR	  Veggie
    04	  2	         1.37µm	      Near-IR	  Cirrus
    05	  1	         1.60µm	      Near-IR	  Snow/Ice
    06	  2	         2.24µm	      Near-IR	  Cloud Particle Size

    '''

    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)

        self.type = 'goes'

        if self.exists and not os.path.isdir(self.path):
            raise FileNotFoundError('%s is not a directory' % self.path)
        os.makedirs(self.path, exist_ok=True)

        # the dataset name is the directory name
        self.name = os.path.split(self.path)[-1]

        # find existing bands
        # note that not all bands may be present (or need to be)
        self.filepaths = {}
        self.extant_bands = []
        if self.exists:    
            filepaths = glob.glob(os.path.join(self.path, '*.nc.tif'))
            for filepath in filepaths:
                filename = filepath.split(os.sep)[-1]
                result = re.search('OR_ABI-L1b-RadC-M6C0([0-9])', filename)
                if result:
                    band = int(result.groups()[0])
                    self.extant_bands.append(band)
                    self.filepaths[band] = filepath
                else:
                    print('Warning: ignoring unexpected filename %s' % \
                        filepath.split(os.sep)[-1])
 
            self.extant_bands = sorted(self.extant_bands)


    def filepath(self, band):
        '''
        Note that, for raw datasets, we cannot determine the filepath from the band
        (as we can for Landsat scenes) because of the unknown and variable timestamps 
        present in the raw data filenames
        '''

        if self.exists and band not in self.filepaths:
            raise ValueError('No data for band %02d' % band)
    
        if band in self.filepaths:
            filepath = self.filepaths[band]
        else:
            filepath = os.path.join(self.path, 'OR_ABI-L1b-RadC-M6C%02d.tif' % band)
        return filepath
