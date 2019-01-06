
import os
import sys
import json
import subprocess
import numpy as np

from . import settings


def shell(command=None, verbose=True):

    output = subprocess.run(
        command, 
        shell=True, 
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True)

    if verbose:
        if output.stderr:
            print(output.stderr)
        if output.stdout:
            print(output.stdout)
            
    return output.stdout


def current_commit():

    return shell('git log', verbose=False).split(' ')[1].split('\n')[0][:7]



def transform(bounds, dst_crs):
    '''
    Transform lat/lon bounds to a given crs

    bounds : a list of [lon_min, lat_min, lon_max, lat_max]
    dst_crs : a path to a geoTIFF whose crs the bounds will be transformed to
    
    '''
    
    bounds = shell(
        f'echo "{bounds}" | {settings.RIO_CLI} transform - --dst-crs {dst_crs} --precision 2',
        verbose=False)

    return json.loads(bounds)


def autogain_image(im, percentile=None):
    '''
    Autogain an image

    im : a numpy array
    percentile : an integer between 0 and 100
       
    '''
    
    im = im.astype(float)

    # default to min/max
    if percentile is None:
        percentile = 100

    minn, maxx = np.percentile(im[:], [100 - percentile, percentile])

    im -= minn
    im /= (maxx - minn)
    im[im < 0] = 0
    im[im > 1] = 1
    return im