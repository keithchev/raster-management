
import os
import re
import sys
import glob
import json
import shutil
import deepdiff
import datetime
import rasterio
import subprocess

import numpy as np

from . import utils
from . import settings
from . import datasets
from .operations import Operation



def log_operation(method):

    def wrapper(self, source, **kwargs):

        if isinstance(source, list):
            source = [s.destination if isinstance(s, Operation) else s for s in source]
        
        if isinstance(source, Operation):
            source = source.destination

        destination = method(self, source, **kwargs)

        operation = Operation(
            destination=destination,
            source=source,
            kwargs=kwargs,
            method=method.__name__, 
            commit=utils.current_commit()
        )

        self.operations.append(operation)
        # self.save_props()

    return wrapper



class RasterProject(object):
    
    # path to rasterio CLI
    _rio = settings.RIO_CLI

    # hard-coded output options
    _opts = '--overwrite --driver GTiff --co tiled=false'

    # temp dir
    _tmp_dir = os.path.join(os.getenv('HOME'), 'tmp')

    _serializable_attrs = [
        'project_root', 
        'project_name', 
        'project_created_on', 
        'raw_dataset_type',
    ]


    def __init__(self, project_root, dataset_paths=None, res=None, bounds=None, reset=False, refresh=False):
        '''
        project_root:  path to the project directory
        dataset_paths: a list of paths to the raw/initial data files
                       (either TIFF files, NED13 tile directories, or Landsat scene directories)

        reset: when loading an existing project, whether to delete existing datasets and cached operations
        refresh: when loading an existing project, whether to re-run all of the existing operations

        TODO: implement re-running cached operations when refresh=True using self._run_operation
        TODO: clean up/simplify the initialization logic (_load_existing_project vs _create_new_project)

        '''

        # raw dataset type must be hard-coded in subclasses
        assert hasattr(self, 'raw_dataset_type')

        self.props_path = os.path.join(project_root, 'props.json')

        if not reset:
            if not os.path.exists(self.props_path):
                raise FileNotFoundError('No cached props found at %s' % self.props_path)
            if res is not None or bounds is not None:
                print('Warning: res and bounds are ignored when loading an existing dataset')

            print('Loading from existing project')
            self._load_existing_project(project_root, refresh)

        # we're resetting or starting from scratch
        else:
            if dataset_paths is None:
                raise ValueError('Raw datasets must be provided when creating a new project')

            print('Creating new project')
            if os.path.isdir(project_root):
                shutil.rmtree(project_root)
            os.makedirs(project_root)

            self._create_new_project(project_root, dataset_paths, res, bounds)



    def _load_existing_project(self, project_root, refresh):

        with open(self.props_path, 'r') as file:
            cached_props = json.load(file)                        

        self._deserialize(cached_props, refresh)
        self._validate_operations()

        # check that the cached operations match the newly serialized operations
        new_props = self._serialize()
        diff = deepdiff.DeepDiff(cached_props, new_props, report_repetition=True)

        if diff:
            print('WARNING: cached serialized operations are not reproducible')
            print(diff)



    def _create_new_project(self, project_root, dataset_paths, res, bounds):

        self.operations = []
        self.project_root = re.sub(r'%s*$' % os.sep, '', project_root)
        self.project_name = os.path.split(self.project_root)[-1]
        self.project_created_on = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')

        if type(dataset_paths) is str:
            dataset_paths = [dataset_paths]

        raw_datasets = [
            datasets.new_dataset(self.raw_dataset_type, path, is_raw=True, exists=True) 
            for path in dataset_paths]

        self.merge(raw_datasets, res=res, bounds=bounds)



    def _serialize(self):

        props = {}
        for attr in self._serializable_attrs:
            props[attr] = getattr(self, attr)

        props['operations'] = [operation.serialize() for operation in self.operations]
        return props


    def _deserialize(self, cached_props, refresh):

        for attr in self._serializable_attrs:
            setattr(self, attr, cached_props.get(attr))

        # de-serialize the cached operations
        self.operations = [Operation.deserialize(op) for op in cached_props['operations']]


    def _run_operation(self, operation):

        # TODO: finish implementing this
        getattr(self, operation.method)(operation.source, **operation.kwargs)


    def save_props(self):

        props = self._serialize()
        with open(self.props_path, 'w') as file:
            json.dump(props, file)



    def get_operation(self, index, method=None):

        ops = self.operations

        if method:
            ops = [op for op in ops if op.method==method]
            if not ops:
                print('No operations exist for method `%s`' % method)
                return None

        if index=='last':
            return ops[-1]
            
        if index=='first':
            return ops[0]

        if type(ind) is int:
            return ops[ind]



    def _new_dataset(self, dataset_type=None, method=None):
        '''
        Generate a new output dataset given a method name
        
        Note that we use the timestamp as a primitive kind of hash to guarantee a unique filename
        '''

        timestamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d-%H%M%S')
        filename = '%s_%s_%s' % (self.project_name, method, timestamp)
        path = os.path.join(self.project_root, filename)
        return datasets.new_dataset(dataset_type, path, exists=False)




    @log_operation
    def merge(self, source, res=None, bounds=None):
        '''
        Create the root dataset by merging and/or cropping the raw dataset(s).

        Note that this method is the only way to create the root derived dataset;
        that is, `source` should correspond to `dataset_paths` in __init__,
        and the zeroth operation in self.operations should always correspond to this method.
        
        For now, we don't enforce these conventions. 
        '''

        assert isinstance(source, list)     
        for dataset in source:
            assert(dataset.type==self.raw_dataset_type)   
        
        destination = self._new_dataset(self.raw_dataset_type, method='merge')

        for band in destination.expected_bands:
    
            srs = ' '.join([dataset.bandpath(band) for dataset in source])
            dst = destination.bandpath(band)

            command = '%s merge %s' % (self._rio, self._opts)

            if res:
                _res = res
                if destination.pan_band and destination.pan_band==band:
                    _res = res/2
                
                command += ' -r %s' % _res

            if bounds:
                command += ' --bounds "%s"' % bounds

            command += ' %s %s' % (srs, dst)
            utils.shell(command, verbose=False)

        return destination


    @log_operation
    def crop(self, source, bounds=None):
        '''
        Crop any single dataset given bounds
        '''
        pass


    @log_operation
    def warp(self, source, crs=None, res=None):

        if crs is None:
            raise ValueError('a crs must be provided')

        destination = self._new_dataset('tif', method='warp')
        command = '%s warp %s --resampling cubic --dst-crs %s' % (self._rio, self._opts, crs)

        if res:
            command += ' -r %s' % res

        utils.shell(command + ' %s %s' % (source.path, destination.path))
        
        return destination


    @log_operation
    def multiply(self, sources, gamma=None):
        '''
        Multiply an RGB tif by a black-and-white tif

        We assume that one of the datasets in sources is a single-channel (BW) tif,
        and the other is an RGB (color) tif

        '''
        images = []
        for source in sources:
            with rasterio.open(source.path) as src:
                images.append(src.read().astype('float'))

        if images[0].shape[0]==1 and images[1].shape[0]==3:
            im_bw, im_rgb = images
        elif images[1].shape[0]==1 and images[0].shape[0]==3:
            im_rgb, im_bw = images
        else:
            raise ValueError('Unexpected image dimensions: %s, %s' % (images[0].shape, images[1].shape))

        # rasterio loads the color channel into the first dimension
        im_bw = im_bw[0, :, :]
        im_rgb = np.swapaxes(im_rgb, 0, 1)
        im_rgb = np.swapaxes(im_rgb, 1, 2)

        # multiply-blend the BW image with the RGB image
        im_bw = utils.autogain_image(im_bw, percentile=100)
        im_rgb *= im_bw[:, :, None]
        im_rgb = utils.autogain_image(im_rgb, percentile=100)

        if gamma:
            im_rgb **= .9

        # hard-coded 'uint8' dtype for now
        dtype = 'uint8'
        dtype_max = 255
    
        im_rgb = (im_rgb*dtype_max).astype(dtype)

        # move the color channel into the first dimension
        im_rgb = np.concatenate(
            (im_rgb[None, :, :, 0], im_rgb[None, :, :, 1], im_rgb[None, :, :, 2]),
            axis=0)

        # destination dataset
        destination = self._new_dataset('tif', method='multiply')

        with rasterio.open(sources[0].path) as src:
            dst_profile = src.profile
            dst_profile['dtype'] = dtype
            dst_profile['count'] = 3
            with rasterio.open(destination.path, 'w', **dst_profile) as dst:
                dst.write(im_rgb)

        return destination


    def _validate_operations(self):
        '''
        Validate operations in self.operations by checking that operation.kwargs are consistent
        with the geoTIFF metadata, to the extent that we can. 
        
        For now, we only check that the `res` and `bounds` of the initial merge operation
        are consistent with the root derived dataset.
        
        TODO: add existence checks for all operations' datasets
        (once we add setter/getter for Dataset.exists):

        try:
            operation.exists = True
        except FileNotFoundError:
           print('WARNING: source and/or destination files are missing for operation %s' % operation)

        '''

        # the first operation must be a merge
        operation = self.operations[0]
        assert(operation.method=='merge')

        res, bounds = operation.kwargs['res'], operation.kwargs['bounds']

        with rasterio.open(operation.destination.bandpath(1)) as src:

            # tolerance for comparing actual to expected bounds
            tolerance = max(src.res)*2

            if res is not None and set(src.res)!=set([res]):
                raise ValueError(
                    'The resolution of the existing root dataset is %s but a resolution of %s was provided' % \
                        (src.res, res))

            if bounds is not None and np.any(np.abs(np.array(src.bounds) - bounds) > tolerance):
                raise ValueError(
                    'The bounds of the existing root dataset are %s but bounds of %s were provided' % \
                        (tuple(src.bounds), bounds))

            print('Found root dataset with res=%s and bounds=%s' % (src.res, tuple(src.bounds)))    



class LandsatProject(RasterProject):

    def __init__(self, *args, **kwargs):
        
        self.raw_dataset_type = 'landsat'
        super().__init__(*args, **kwargs)


    @log_operation
    def stack(self, source, bands=None):
        '''
        source : a Landsat dataset
        bands : a list of three Landsat bands
        '''

        bands = list(map(str, bands))
        destination = self._new_dataset('tif', method='stack')

        utils.shell('%s stack --overwrite --rgb %s %s' % \
              (self._rio, ' '.join([source.bandpath(b) for b in bands]), destination.path))

        return destination



    @log_operation
    def autogain(self, source, percentile=None, each_band=True):
        '''
        Autogain an RGB image
        
        Parameters
        ----------

        source : a dataset representing an RGB geoTIFF

        percentile : integer percentile with which to calculate min/max intensities
            if None, absolute min/max are used
        
        '''

        # hard-coded 'uint8' dtype for now
        dtype = 'uint8'
        dtype_max = 255

        # destination dataset
        destination = self._new_dataset('tif', method='autogain')

        with rasterio.open(source.path) as src:
            dst_profile = src.profile
            dst_profile['dtype'] = dtype

            with rasterio.open(destination.path, 'w', **dst_profile) as dst:
                if each_band:
                    im_dst = np.zeros((len(src.indexes),) + src.shape)
                    for ind, band in enumerate(src.indexes):
                        im = src.read(band).astype('float64')
                        im_dst[ind, :, :] = utils.autogain_image(im, percentile)
                else:
                    im = src.read().astype('float64')
                    im_dst = utils.autogain_image(im, percentile)

                im_dst *= dtype_max
                im_dst = im_dst.astype(dtype)
                dst.write(im_dst)

        return destination



class DEMProject(RasterProject):

    def __init__(self, *args, **kwargs):

        self.raw_dataset_type = 'tif'
        super().__init__(*args, **kwargs)


    @log_operation
    def hillshade(self, source):

        destination = self._new_dataset('tif', method='hillshade')
        utils.shell('gdaldem hillshade %s %s' % (source.path, destination.path))
        return destination


    @log_operation
    def color_relief(self, source, colormap=None):
        '''
        Wrapper for gdaldem color-relief command

        Parameters
        ---------
        source : a DEM as a single tif dataset

        colormap : a list of elevation-color dicts of the form 
            {'elevation': <elevation>, 'color': (float, float, float)}
        
        For now, we assume that the DEM elevations are in meters,
        and the colormap elevations are in feet.

        '''
        feet_per_meter = 3.28

        if colormap is None:
            raise ValueError('A colormap must be provided')

        # coerce colors to float for JSON-ability
        for row in colormap:
            row['color'] = tuple(np.array(row['color']).astype(float))

        # gdaldem requires the colormap be a file in which each line is of the form
        # '<elevation> <uint8> <uint8> <uint8>\n'
        colormap_filename = os.path.join(self._tmp_dir, 'colormap.txt')
        with open(colormap_filename, 'w') as file:
            for row in colormap:
                file.write('%d %d %d %d\n' % ((row['elevation']/feet_per_meter,) + row['color']))

        destination = self._new_dataset('tif', method='color_relief')

        utils.shell('gdaldem color-relief %s %s %s' % \
            (source.path, colormap_filename, destination.path))

        # os.remove(colormap_filename)
        return destination


