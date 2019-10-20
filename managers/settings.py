
import os

ENV_NAME = 'rasterenv'
CONDA_ENV_ROOT = '/home/keith/anaconda3/envs'

RIO_ENV = {
    'PATH': os.path.join(CONDA_ENV_ROOT, ENV_NAME, 'bin'),
    'PROJ_LIB': os.path.join(CONDA_ENV_ROOT, ENV_NAME, 'share', 'proj'),
}

# local path to texture shading binaries
TEXTURE_SHADER_PATH = '/home/keith/Dropbox/texture-shading/bin/'
