    props = {

    	# top-level project directory
        'root': ''

        # raw datasets (Landsat scenes, NED13 tiles, or generic GeoTIFFs)
        # for Landsat, the locations are paths to scene directories;
        # for TIFFs, the locations are paths to tif files
        'raw_datasets': {
            'dataset_type': 'landsat' | 'ned' | 'tif',
            'paths': list
        },

        # log of operations, beginning with the merge command that generates the derived dataset
        'operations': [
            {
                'commit': str
                'method': str,
                'kwargs': {}
                'sources': [{
                    'type': 'landsat' | 'tif',
                    'path': str
                }],
                'destination': {
                    'type': 'landsat' | 'tif',
                    'path': str
                }
            },
        ],
    }
