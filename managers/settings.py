
import os

VENV_ROOT = '/home/keith/anaconda3/envs/rasmanenv'

RIO_ENV = {
    'PATH': os.path.join(VENV_ROOT, 'bin'),
    'PROJ_LIB': os.path.join(VENV_ROOT, 'share', 'proj'),
}

# local path to texture shading binaries
TEXTURE_SHADER_PATH = '/home/keith/Dropbox/texture-shading/bin/'
