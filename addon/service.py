import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'resources', 'site-packages'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'resources', 'lib'))
from elementum_py.kodi_service import run

if __name__ == '__main__':
    run()
