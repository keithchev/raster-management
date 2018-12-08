
'''
Generate test 'raw' landsat scenes
 
Because raw Landsat 8 scenes are large,
here we downsample two raw scenes to serve as test datasets.

Obviously, this requires that the raw scenes exist locally.

'''

import os
import sys
import json
import glob
import rasterio
import numpy as np

sys.path.insert(0, '../')
from managers import managers

# local path to rasterio CLI
rio = '/home/keith/anaconda3/envs/gdalenv/bin/rio'

# options for rio merge 
options = '--overwrite --driver GTiff --co tiled=false'

# local path to the raw landsat scenes
landsat_dir = '/home/keith/landsat-data' 

# the test scenes we're using
# (Mono Lake and east of Mono Lake)
scenes = [
    'LC08_L1TP_042034_20180907_20180912_01_T1',
    'LC08_L1TP_041034_20180916_20180928_01_T1',
]

def filename(scene, band):
    return '%s_B%d.TIF' % (scene, band)

def source(scene, band):
    return os.path.join(landsat_dir, scene, filename(scene, band))

def dest(scene, band):
    dirpath = os.path.join('.', 'datasets', 'landsat', scene)
    os.makedirs(dirpath, exist_ok=True)
    return os.path.join(dirpath, filename(scene, band))


bands = range(1, 12)
res = {band: 200 for band in bands}

# pan band
res[8] = 100

for scene in scenes:
    print('Generating scene %s' % scene)
    for band in bands:
        if os.path.isfile(dest(scene, band)):
            continue
        managers.utils.shell('%s merge %s -r %s %s %s' % \
            (rio, options, res[band], source(scene, band), dest(scene, band)))

