
import os
import sys
import json
import subprocess
import numpy as np

from . import settings


def construct_rio_command(command, inputs, output, **kwargs):
    '''
    Construct a RIO CLI command from the given kwargs

    Note that we don't need double quotes around the list of bounds,
    despite their appearance in the rio merge documentation.
    That is, the bounds option requires no special handling;
    we can simply use `args.extend(['--bounds', str(bounds)])`
    '''

    # default options for commands that output raster data
    default_output_options = {
        'driver': 'GTiff',
        'co': 'tiled=false',
        'overwrite': True
    }
    
    if command in ['warp', 'merge', 'rasterize']:
        kwargs.update(default_output_options)

    valid_commands = [
        'clip', 'convert', 'info', 'mask', 'merge', 
        'rasterize', 'stack', 'transform', 'warp'
    ]

    if command not in valid_commands:
        raise ValueError('%s is not a supported command' % command)

    args = ['rio', command]

    # append the kwargs
    for kwarg, value in kwargs.items():
        if value is None:
            continue
        
        # format the value
        # (note special handling for dst-bounds option of 'warp')
        if kwarg == 'dst_bounds':
            value = list(map(str, value))
        elif value and isinstance(value, bool):
            value = []
        else:
            value = [str(value)]
        
        args.append(format_rio_option(kwarg))
        args.extend(value)

    # append the inputs and outputs
    if inputs is not None:
        if not isinstance(inputs, list):
            inputs = [inputs]
        args.extend(inputs)

    if output is not None:
        args.append(output)
    
    return args


def format_rio_option(option):
    '''
    'r' to '-r', 'res' to '--res', 'dst_crs' to '--dst-crs', etc
    '''
    option = option.replace('_', '-')
    if len(option) == 1:
        return '-' + option
    return '--' + option


def run_command(command=None, verbose=True):

    result = subprocess.run(
        command, 
        stdin=subprocess.PIPE, 
        stderr=subprocess.PIPE, 
        stdout=subprocess.PIPE,
        env=settings.RIO_ENV)

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
    Transform EPSG:4326 lat/lon bounds to a given CRS

    bounds : a list of [lon_min, lat_min, lon_max, lat_max]
    dst_crs : either a CRS like 'EPSG:3857' 
        or a path to a geoTIFF to whose CRS the bounds will be transformed

    TODO: it would be cleaner to use rasterio.warp.transform here instead of the CLI

    '''
    
    command = construct_rio_command(
        'transform',
        inputs=str(bounds),
        output=None,
        dst_crs=dst_crs,
        precision=2)

    result = run_command(command, verbose=False)
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