
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

    def wrapper(self, source, log=True, **kwargs):

        if isinstance(source, list):
            source = [s.destination if isinstance(s, Operation) else s for s in source]
        
        if isinstance(source, Operation):
            source = source.destination

        destination, command = method(self, source, **kwargs)
        if destination is None:
            raise ValueError('method %s must return a dataset object' % method)

        operation = Operation(
            destination=destination,
            source=source,
            kwargs=kwargs,
            method=method.__name__, 
            commit=utils.current_commit(),
            command=command
        )

        if log:
            self.operations.append(operation)
        # self.save_props()

    return wrapper



class RasterProject(object):
    
    # hard-coded output options
    _default_rio_args = ['--overwrite', '--driver', 'GTiff', '--co', 'tiled=false']

    # temp dir
    _tmp_dir = os.path.join(os.getenv('HOME'), 'tmp')

    _serializable_attrs = [
        'project_root', 
        'project_name', 
        'project_created_on', 
        'raw_dataset_type',
    ]


    def __init__(
        self, 
        project_root,
        dataset_paths=None,
        raw_dataset_type=None, 
        reset=False, 
        refresh=False):
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
        self.raw_dataset_type = raw_dataset_type

        self.props_path = os.path.join(project_root, 'props.json')

        if not reset:
            if not os.path.exists(self.props_path):
                raise FileNotFoundError('No cached props found at %s' % self.props_path)

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
            self._create_new_project(project_root, dataset_paths)


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


    def _create_new_project(self, project_root, dataset_paths):

        self.operations = []
        self.project_root = re.sub(r'%s*$' % os.sep, '', project_root)
        self.project_name = os.path.split(self.project_root)[-1]
        self.project_created_on = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')

        if type(dataset_paths) is str:
            dataset_paths = [dataset_paths]

        self.raw_datasets = [
            datasets.new_dataset(self.raw_dataset_type, path, exists=True) 
            for path in dataset_paths
        ]


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
        elif index=='first':
            return ops[0]
        elif type(index) is int:
            return ops[index]
        else:
            raise ValueError('%s is not a valid index value' % index)


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

        Note that this method and the `warp` method are intended to be the only ways
        to create the root derived dataset; that is, the first operation
        in self.operations should always correspond to either the `merge` or `warp` methods.
        
        Parameters
        ----------
        bounds : bounds of the merged dataset in lat/lon degrees
        res: resolution, in units of the source crs

        '''

        assert isinstance(source, list)     
        for dataset in source:
            assert(dataset.type==self.raw_dataset_type)   
    
        output_dataset_type = self.raw_dataset_type
        if self.raw_dataset_type=='ned13':
            output_dataset_type = 'tif'

        destination = self._new_dataset(output_dataset_type, method='merge')

        # transform lat/lon bounds to the source CRS
        # (using the filepath to the first band of the first source)
        if bounds:
            bounds = utils.transform(bounds, source[0].bandpath(destination.expected_bands[0]))

        for band in destination.expected_bands:
    
            src_filepaths = [dataset.bandpath(band) for dataset in source]
            dst_filepath = destination.bandpath(band)

            if res:
                _res = res
                if destination.pan_band and destination.pan_band==band:
                    _res = res/2
                
            command = utils.construct_rio_command(
                'merge', src_filepaths, dst_filepath,
                bounds=bounds,
                res=_res)

            utils.run_command(command, verbose=True)

        return destination, command


    @log_operation
    def crop(self, source, bounds=None):
        '''
        Crop any single dataset given bounds
        '''
        pass


    @log_operation
    def warp(self, source, crs=None, res=None, bounds=None):
        '''
        Reproject and possibly resample a tif dataset

        Parameters
        ----------
        crs : the CRS to which to reproject the dataset (e.g., 'EPSG:3857')
        bounds : bounds of the reprojected dataset in lat/lon degrees
        res: resolution, in units of the *destination* crs

        Note that resampling must be 'cubic' to avoid grid-like artifacts
        in hillshading of un-downsampled NED13-based datasets

        '''

        
        if isinstance(source, list):
            if len(source) > 1:
                raise ValueError('Only one source dataset can be provided to warp')
            source = source[0]

        if crs is None:
            raise ValueError('a crs must be provided')

        destination = self._new_dataset('tif', method='warp')

        # transform bounds from lat/lon to the destination CRS        
        if bounds:
            bounds = utils.transform(bounds, crs)

        command = utils.construct_rio_command(
            'warp', source.path, destination.path,
            dst_crs=crs,
            dst_nodata=0,
            dst_bounds=bounds,
            resampling='lanczos',
            res=res)

        utils.run_command(command)
        return destination, command


    @log_operation
    def multiply_rgb(self, sources, gamma=None, weight=None):
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
        im_bw = utils.autoscale(im_bw, percentile=100)
        if weight:
            im_bw *= weight

        im_rgb *= im_bw[:, :, None]
        im_rgb = utils.autoscale(im_rgb, percentile=100)

        if gamma:
            im_rgb **= gamma

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

        # we never used a CLI
        command = None

        return destination, command


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

        # the first operation must be a merge or a warp
        operation = self.operations[0]
        assert(operation.method in ['merge', 'warp'])

        res = operation.kwargs.get('res')
        bounds = operation.kwargs.get('bounds')

        with rasterio.open(operation.destination.bandpath(1)) as src:

            # tolerance for comparing actual to expected bounds
            tolerance = max(src.res)*2

            if res is not None and set(src.res)!=set([res]):
                print('Warning: the resolution of the existing root dataset is %s but a resolution of %s was provided' % \
                        (src.res, res))

            if bounds is not None and np.any(np.abs(np.array(src.bounds) - bounds) > tolerance):
                print('Warning: the bounds of the existing root dataset are %s but bounds of %s were provided' % \
                        (tuple(src.bounds), bounds))

            print('Found root dataset with res=%s and bounds=%s' % (src.res, tuple(src.bounds)))    


class GOESProject(RasterProject):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, raw_dataset_type='tif', **kwargs)


class LandsatProject(RasterProject):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, raw_dataset_type='landsat', **kwargs)


    @log_operation
    def stack(self, source, bands=None):
        '''
        source : a Landsat dataset
        bands : a list of three Landsat bands
        '''

        bands = list(map(str, bands))
        destination = self._new_dataset('tif', method='stack')

        command = ['rio', 'stack', '--overwrite', '--rgb']
        command += [source.bandpath(b) for b in bands] + [destination.path]

        utils.run_command(command)
        return destination, command



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
                        im_dst[ind, :, :] = utils.autoscale(im, percentile)
                else:
                    im = src.read().astype('float64')
                    im_dst = utils.autoscale(im, percentile)

                im_dst *= dtype_max
                im_dst = im_dst.astype(dtype)
                dst.write(im_dst)

        command = None
        return destination, command



class DEMProject(RasterProject):

    def __init__(self, *args, **kwargs):

        # assume raw NED13 tiles to start (i.e., .adf files)
        raw_dataset_type = 'ned13'

        # hackish way to determine whether the raw datasets are actually tifs instead of adfs
        # (note that if dataset_paths doesn't exist, self.raw_dataset_type 
        # will be overwritten during deserialization)
        paths = kwargs.get('dataset_paths')
        if paths:
            if not isinstance(paths, list):
                paths = [paths]
            try:
                datasets.new_dataset('ned13', paths[0], exists=True)
            except:
                raw_dataset_type = 'tif'

        print(raw_dataset_type)
        super().__init__(*args, raw_dataset_type=raw_dataset_type, **kwargs)


    @log_operation
    def hill_shade(self, source):

        destination = self._new_dataset('tif', method='hill_shade')
        command = ['gdaldem', 'hillshade', source.path, destination.path]
        utils.run_command(command)

        return destination, command


    @log_operation
    def slope_shade(self, source):
        '''
        slope shading
        Note: no kwargs
        '''

        destination = self._new_dataset('tif', method='slope_shade')
        slope_filepath = os.path.join(self._tmp_dir, 'temp.tif')
        colormap_filename = os.path.join(self._tmp_dir, 'colormap.txt')

        # calculate the slope angles
        utils.run_command(['gdaldem', 'slope', source.path, slope_filepath])

        with open(colormap_filename, 'w') as file:
            for row in [(0, 255, 255, 255), (90, 0, 0, 0)]:
                file.write('%d %d %d %d\n' % row)

        # map the angles to uint8 values
        utils.run_command(['gdaldem', 'color-relief', slope_filepath, colormap_filename, destination.path])

        os.remove(slope_filepath)

        # we used two GDAL CLI commands, so let's not worry about how to capture them
        command = None
        return destination, command


    @log_operation
    def texture_shade(self, source, detail=None, enhancement=None):

        texture_bin = os.path.join(settings.TEXTURE_SHADER_PATH, 'texture')
        texture_image_bin = os.path.join(settings.TEXTURE_SHADER_PATH, 'texture_image')

        if detail is None:
            detail = .66

        if enhancement is None:
            enhancement = 2

        flt_filepath = os.path.join(self._tmp_dir, 'temp.flt')
        texture_filepath = os.path.join(self._tmp_dir, 'temp_texture.flt')
        destination = self._new_dataset('tif', method='texture_shade')
        
        # texture shader requires the input DEM as an FLT file
        utils.run_command(['gdal_translate', '-of', 'EHdr', '-ot', 'Float32', source.path, flt_filepath])
        
        # create the texture intermediate
        utils.run_command([texture_bin, str(detail), flt_filepath, texture_filepath])

        # create the texture-shaded TIFF
        utils.run_command([texture_image_bin, str(enhancement), texture_filepath, destination.path])
        
        # remove intermediate files
        for path in [flt_filepath, texture_filepath, destination.path]:
            for ext in ['flt', 'hdr', 'prj', 'tfw', 'flt.aux.xml']:
                try:
                    os.remove(re.sub(r'(\w+)$', ext, path))
                except FileNotFoundError:
                    continue

        command = None
        return destination, command



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
        command = ['gdaldem', 'color-relief', source.path, colormap_filename, destination.path]
        utils.run_command(command)

        return destination, command


