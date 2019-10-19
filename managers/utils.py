
import os
import sys
import json
import subprocess
import numpy as np

from . import settings


def run_command(command=None, verbose=True):

    result = subprocess.run(
        command, 
        stdin=subprocess.PIPE, 
        stderr=subprocess.PIPE, 
        stdout=subprocess.PIPE,
        env=dict(PATH=settings.PATH, PROJ_LIB='/home/keith/anaconda3/envs/rasmanenv/share/proj'))

    if verbose:
        if result.stderr:
            print(result.stderr)
        if result.stdout:
            print(result.stdout)
            
    return result


def current_commit():
    # TODO: reimplement this using gitpython
    return ''


def transform(bounds, dst_crs):
    '''
    Transform lat/lon bounds to a given crs

    bounds : a list of [lon_min, lat_min, lon_max, lat_max]
    dst_crs : a path to a geoTIFF whose crs the bounds will be transformed to

    TODO: it would be cleaner to use rasterio.warp.transform here instead of the CLI

    '''
    
    result = run_command(
        ['rio', 'transform', '--dst-crs', dst_crs, '--precision', '2', f'{bounds}'], 
        verbose=False)

    if result.stderr:
        raise Exception('Rio error: %s' % result.stderr)

    return json.loads(result.stdout)


def autoscale(im, percentile=None, minn=None, maxx=None, gamma=None, dtype=None):
    '''
    Autogain an image

    im : a numpy array
    percentile : an integer between 0 and 100
       
    '''
    
    max_vals = {'uint8': 255, 'uint16': 65535}
    im = im.astype(float)

    # default to min/max
    if percentile is None:
        percentile = 100
    pmin, pmax = np.percentile(im[:], [100 - percentile, percentile])

    if minn is None:
        minn = pmin
    if maxx is None:
        maxx = pmax

    im -= minn
    im /= (maxx - minn)
    im[im < 0] = 0
    im[im > 1] = 1

    if gamma:
        im = im**gamma

    if dtype is not None:
        im *= max_vals[dtype]
        im = im.astype(dtype)

    return im