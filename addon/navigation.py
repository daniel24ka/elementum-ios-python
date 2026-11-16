"""Route Kodi subtitle actions separately from the video directory frontend."""
import os
import sys
from urllib.parse import parse_qs, urlsplit

root = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(root, 'resources', 'site-packages'))
sys.path.insert(0, os.path.join(root, 'resources', 'lib'))
from elementum_py.subtitle_dispatch import is_subtitle_request

if is_subtitle_request(sys.argv):
    from elementum_py.subtitle_bridge.service import main
    main()
else:
    from elementum import navigation
    navigation.run()
