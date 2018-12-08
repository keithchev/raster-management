    props = {

    	# top-level project directory
        'location': ''

        # raw datasets (Landsat scenes, NED13 tiles, or generic GeoTIFFs)
        # for Landsat, the locations are paths to scene directories;
        # for TIFFs, the locations are paths to tif files
        'sources': {
            'type': 'landsat' | 'ned' | 'tif',
            'locations': list
        },

        # arguments for the merge command that generates the derived dataset
        'initialization': {
            'res': float,
            'bounds': list,
        },

        # log of files created from the derived dataset
        'history': [
            {
                'command': '',
                'destination': {
                    'type': 'landsat' | 'tif',
                    'location': ''
                },
            },
        ],
    }
