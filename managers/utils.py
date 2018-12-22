
import os
import sys
import json
import subprocess
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
    `bounds` is a list of [lon_min, lat_min, lon_max, lat_max]
    `dst_crs` is a path to a geoTIFF whose crs the bounds will be transformed to
    '''
    
    bounds = shell(
        f'echo "{bounds}" | {settings.RIO_CLI} transform - --dst-crs {dst_crs} --precision 2',
        verbose=False)

    return json.loads(bounds)